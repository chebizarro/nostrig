package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	gonostr "fiatjaf.com/nostr"
	cascontextvm "git.sharegap.net/cascadia/cascadia-go/contextvm"
	casnostr "git.sharegap.net/cascadia/cascadia-go/nostr"
	beadspb "github.com/chebizarro/nostrig/gen/beads"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
	"github.com/chebizarro/nostrig/internal/taskfabric"
	"github.com/chebizarro/nostrig/internal/taskmodel"
	"github.com/spf13/cobra"
)

const (
	exitGeneral    = 1
	exitUsage      = 2
	exitConflict   = 3
	exitNotFound   = 4
	exitTimeout    = 5
	exitRejected   = 6
	exitIncomplete = 7
)

type commandExitError struct {
	code int
	err  error
}

func (e *commandExitError) Error() string { return e.err.Error() }
func (e *commandExitError) Unwrap() error { return e.err }

func exitError(code int, err error) error {
	if err == nil {
		return nil
	}
	return &commandExitError{code: code, err: err}
}

func commandErrorExitCode(err error) int {
	if err == nil {
		return 0
	}
	var coded *commandExitError
	if errors.As(err, &coded) {
		return coded.code
	}
	if errors.Is(err, context.Canceled) {
		return 130
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return exitTimeout
	}
	if nip34.IsQueryTruncated(err) || nip34.IsPartialFetch(err) {
		return exitIncomplete
	}
	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "unknown flag"), strings.Contains(message, "required"), strings.Contains(message, "invalid argument"):
		return exitUsage
	case strings.Contains(message, "conflict"), strings.Contains(message, "stale_revision"), strings.Contains(message, "already_claimed"):
		return exitConflict
	case strings.Contains(message, "not found"):
		return exitNotFound
	case strings.Contains(message, "deadline exceeded"), strings.Contains(message, "timed out"), strings.Contains(message, "timeout"):
		return exitTimeout
	default:
		return exitGeneral
	}
}

type taskQueryOptions struct {
	repoAddr  string
	taskIDs   []string
	authors   []string
	relays    []string
	pageSize  int
	maxPages  int
	maxEvents int
	timeout   time.Duration
}

type taskFilterOptions struct {
	statuses []string
	assignee string
	labels   []string
	queue    string
}

type mutationOptions struct {
	recipient string
	relays    []string
	timeout   time.Duration
	dryRun    bool
	noWait    bool
}

type taskRecordJSON struct {
	EventID     string                   `json:"event_id,omitempty"`
	Revision    string                   `json:"revision,omitempty"`
	EvidenceIDs []string                 `json:"evidence_ids"`
	Task        *taskmodel.IssueDocument `json:"task"`
}

type taskEnvelope struct {
	Schema string         `json:"schema"`
	Task   taskRecordJSON `json:"task"`
}

type taskListEnvelope struct {
	Schema string           `json:"schema"`
	Count  int              `json:"count"`
	Tasks  []taskRecordJSON `json:"tasks"`
}

type taskWatchEnvelope struct {
	Schema    string           `json:"schema"`
	Type      string           `json:"type"`
	EventID   string           `json:"event_id,omitempty"`
	CreatedAt string           `json:"created_at,omitempty"`
	TaskID    string           `json:"task_id,omitempty"`
	Reason    string           `json:"reason,omitempty"`
	Error     string           `json:"error,omitempty"`
	Task      *taskRecordJSON  `json:"task,omitempty"`
	Tasks     []taskRecordJSON `json:"tasks,omitempty"`
}

type mutationEnvelope struct {
	Schema               string          `json:"schema"`
	Operation            string          `json:"operation"`
	DryRun               bool            `json:"dry_run"`
	Acknowledged         bool            `json:"acknowledged"`
	Ambiguous            bool            `json:"ambiguous"`
	CorrelationID        string          `json:"correlation_id,omitempty"`
	RequestEventID       string          `json:"request_event_id,omitempty"`
	PublishedEventID     string          `json:"published_event_id,omitempty"`
	ResponseEventID      string          `json:"response_event_id,omitempty"`
	SubmittedEvidenceIDs []string        `json:"submitted_evidence_ids"`
	EvidenceIDs          []string        `json:"evidence_ids"`
	Result               json.RawMessage `json:"result,omitempty"`
	Event                *gonostr.Event  `json:"event,omitempty"`
	Error                string          `json:"error,omitempty"`
	ErrorCode            int             `json:"error_code,omitempty"`
	ErrorData            json.RawMessage `json:"error_data,omitempty"`
}

func newTaskCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "task",
		Short: "Stable agent-facing task workflow",
	}
	cmd.PersistentFlags().Bool("json", false, "Emit the stable machine-readable JSON schema")
	cmd.AddCommand(
		newTaskGetCmd(), newTaskListCmd(), newTaskReadyCmd(), newTaskCreateCmd(),
		newTaskAssignCmd(), newTaskClaimCmd(), newTaskUpdateCmd(), newTaskBlockCmd(),
		newTaskCloseCmd(), newTaskWatchCmd(),
	)
	return cmd
}

func newTaskGetCmd() *cobra.Command {
	var taskID string
	opts := defaultTaskQueryOptions()
	cmd := &cobra.Command{
		Use:   "get [task-id]",
		Short: "Get one authoritative task revision",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := taskIDFromArgs(taskID, args)
			if err != nil {
				return exitError(exitUsage, err)
			}
			opts.taskIDs = []string{id}
			export, err := listTaskSnapshot(cmd.Context(), opts)
			if err != nil {
				return err
			}
			if len(export.Issues) == 0 {
				return exitError(exitNotFound, fmt.Errorf("task %s not found", id))
			}
			record, err := taskJSONRecord(export.Issues[0])
			if err != nil {
				return err
			}
			if agentJSON(cmd) {
				return writeJSON(cmd.OutOrStdout(), taskEnvelope{Schema: "nostrig.task.v1", Task: record})
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\t%s\t%s\n", record.Task.ID, record.Task.Status, record.Revision, record.Task.Title)
			return err
		},
	}
	cmd.Flags().StringVar(&taskID, "task-id", "", "Task ID (alternative to positional argument)")
	addTaskQueryFlags(cmd, &opts)
	return cmd
}

func newTaskListCmd() *cobra.Command {
	opts := defaultTaskQueryOptions()
	var filters taskFilterOptions
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List a proven-complete authoritative task snapshot",
		RunE: func(cmd *cobra.Command, args []string) error {
			export, err := listTaskSnapshot(cmd.Context(), opts)
			if err != nil {
				return err
			}
			return emitTaskList(cmd, filterIssues(export.Issues, filters), "nostrig.task-list.v1")
		},
	}
	addTaskQueryFlags(cmd, &opts)
	addTaskFilterFlags(cmd, &filters)
	cmd.Flags().StringSliceVar(&opts.taskIDs, "task-id", nil, "Exact task ID selector (repeatable)")
	return cmd
}

func newTaskReadyCmd() *cobra.Command {
	opts := defaultTaskQueryOptions()
	var filters taskFilterOptions
	cmd := &cobra.Command{
		Use:   "ready",
		Short: "List open, unblocked tasks whose dependencies are closed",
		RunE: func(cmd *cobra.Command, args []string) error {
			export, err := listTaskSnapshot(cmd.Context(), opts)
			if err != nil {
				return err
			}
			issues := readyIssues(export.Issues)
			filters.statuses = []string{"open"}
			return emitTaskList(cmd, filterIssues(issues, filters), "nostrig.task-ready.v1")
		},
	}
	addTaskQueryFlags(cmd, &opts)
	cmd.Flags().StringSliceVar(&filters.labels, "label", nil, "Required label (repeatable)")
	cmd.Flags().StringVar(&filters.queue, "queue", "", "Only tasks in this queue")
	return cmd
}

func newTaskWatchCmd() *cobra.Command {
	opts := defaultTaskQueryOptions()
	opts.timeout = 5 * time.Minute
	cmd := &cobra.Command{
		Use:   "watch",
		Short: "Emit a complete initial snapshot followed by live task changes",
		RunE: func(cmd *cobra.Command, args []string) error {
			resolved, err := resolveTaskQueryOptions(opts)
			if err != nil {
				return exitError(exitUsage, err)
			}
			watchCtx, cancel := context.WithTimeout(cmd.Context(), resolved.timeout)
			defer cancel()
			watch, err := taskfabric.WatchTaskState(watchCtx, nil, taskfabric.TaskWatchOptions{
				Relays: resolved.relays, RepoAddr: resolved.repoAddr, TaskIDs: resolved.taskIDs, Authors: resolved.authors,
				Pagination: nip34.PaginationOptions{PageSize: resolved.pageSize, MaxPages: resolved.maxPages, MaxEvents: resolved.maxEvents},
			})
			if err != nil {
				return err
			}
			defer watch.Close()
			records, err := taskJSONRecords(watch.Snapshot.Issues)
			if err != nil {
				return err
			}
			if agentJSON(cmd) {
				if err := writeJSON(cmd.OutOrStdout(), taskWatchEnvelope{Schema: "nostrig.task-watch.v1", Type: "snapshot", Tasks: records}); err != nil {
					return err
				}
			} else if _, err := fmt.Fprintf(cmd.OutOrStdout(), "snapshot\t%d task(s)\n", len(records)); err != nil {
				return err
			}
			changes, errs := watch.Changes, watch.Errors
			for changes != nil || errs != nil {
				select {
				case <-watchCtx.Done():
					reason := "interrupted"
					if errors.Is(watchCtx.Err(), context.DeadlineExceeded) {
						reason = "timeout"
					}
					if agentJSON(cmd) {
						if err := writeJSON(cmd.OutOrStdout(), taskWatchEnvelope{Schema: "nostrig.task-watch.v1", Type: "end", Reason: reason}); err != nil {
							return err
						}
					}
					if reason == "timeout" {
						return nil
					}
					return watchCtx.Err()
				case err, ok := <-errs:
					if !ok {
						errs = nil
						continue
					}
					if err != nil {
						if agentJSON(cmd) {
							if writeErr := writeJSON(cmd.OutOrStdout(), taskWatchEnvelope{Schema: "nostrig.task-watch.v1", Type: "error", Error: err.Error()}); writeErr != nil {
								return writeErr
							}
						}
						return err
					}
				case change, ok := <-changes:
					if !ok {
						changes = nil
						continue
					}
					envelope := taskWatchEnvelope{Schema: "nostrig.task-watch.v1", Type: string(change.Kind), EventID: change.EventID, TaskID: change.TaskID}
					if !change.CreatedAt.IsZero() {
						envelope.CreatedAt = change.CreatedAt.UTC().Format(time.RFC3339Nano)
					}
					if change.Issue != nil {
						record, recordErr := taskJSONRecord(change.Issue)
						if recordErr != nil {
							return recordErr
						}
						envelope.Task = &record
					}
					if agentJSON(cmd) {
						if err := writeJSON(cmd.OutOrStdout(), envelope); err != nil {
							return err
						}
					} else if _, err := fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\t%s\n", change.Kind, change.TaskID, change.EventID); err != nil {
						return err
					}
				}
			}
			return nil
		},
	}
	addTaskQueryFlags(cmd, &opts)
	cmd.Flags().StringSliceVar(&opts.taskIDs, "task-id", nil, "Exact task ID selector (repeatable)")
	return cmd
}

func newTaskCreateCmd() *cobra.Command {
	var taskID, title, description, status, priority, epic, assignee, repoAddr string
	var labels, dependencies []string
	opts := defaultMutationOptions()
	cmd := &cobra.Command{Use: "create", Short: "Create a task and wait for its correlated authoritative ACK", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(taskID) == "" || strings.TrimSpace(title) == "" {
			return exitError(exitUsage, fmt.Errorf("--task-id and --title are required"))
		}
		params := map[string]any{"task_id": strings.TrimSpace(taskID), "title": strings.TrimSpace(title), "status": strings.TrimSpace(status)}
		for key, value := range map[string]string{"description": description, "priority": priority, "epic": epic, "assignee": assignee, "repo_addr": repoAddr} {
			if strings.TrimSpace(value) != "" {
				params[key] = strings.TrimSpace(value)
			}
		}
		if values := cleanStrings(labels); len(values) > 0 {
			params["labels"] = values
		}
		if values := cleanStrings(dependencies); len(values) > 0 {
			params["depends_on"] = values
		}
		event, err := buildAgentCommand("task/create", opts, params)
		if err != nil {
			return err
		}
		return runAgentCommand(cmd, "task.create", event, opts, nil)
	}}
	cmd.Flags().StringVar(&taskID, "task-id", "", "Task ID (required)")
	cmd.Flags().StringVar(&title, "title", "", "Task title (required)")
	cmd.Flags().StringVar(&description, "description", "", "Task description")
	cmd.Flags().StringVar(&status, "status", "open", "Initial status")
	cmd.Flags().StringVar(&priority, "priority", "", "Priority P0-P4 or P9")
	cmd.Flags().StringVar(&epic, "epic", "", "Parent epic ID")
	cmd.Flags().StringVar(&assignee, "assignee", "", "Initial assignee")
	cmd.Flags().StringVar(&repoAddr, "repo-addr", strings.TrimSpace(os.Getenv("NOSTRIG_REPO_ADDR")), "Canonical repository address")
	cmd.Flags().StringSliceVar(&labels, "label", nil, "Label (repeatable)")
	cmd.Flags().StringSliceVar(&dependencies, "depends-on", nil, "Dependency task ID (repeatable)")
	addMutationFlags(cmd, &opts)
	return cmd
}

func newTaskAssignCmd() *cobra.Command {
	var taskID, assignee, baseEventID string
	opts := defaultMutationOptions()
	cmd := &cobra.Command{Use: "assign", Short: "Assign a task at an exact revision", RunE: func(cmd *cobra.Command, args []string) error {
		if err := requireTaskRevision(cmd, taskID, baseEventID); err != nil {
			return err
		}
		if strings.TrimSpace(assignee) == "" {
			return exitError(exitUsage, fmt.Errorf("--assignee is required"))
		}
		event, err := buildAgentCommand("task/assign", opts, map[string]any{"task_id": taskID, "assignee": assignee, "base_event_id": baseEventID})
		if err != nil {
			return err
		}
		return runAgentCommand(cmd, "task.assign", event, opts, nil)
	}}
	addTaskRevisionFlags(cmd, &taskID, &baseEventID)
	cmd.Flags().StringVar(&assignee, "assignee", "", "Assignee identity (required)")
	addMutationFlags(cmd, &opts)
	return cmd
}

func newTaskClaimCmd() *cobra.Command {
	var taskID, claimer, baseEventID string
	opts := defaultMutationOptions()
	cmd := &cobra.Command{Use: "claim", Short: "Atomically claim an unassigned task revision", RunE: func(cmd *cobra.Command, args []string) error {
		if err := requireTaskRevision(cmd, taskID, baseEventID); err != nil {
			return err
		}
		params := map[string]any{"task_id": taskID, "base_event_id": baseEventID}
		if strings.TrimSpace(claimer) != "" {
			params["claimer"] = strings.TrimSpace(claimer)
		}
		event, err := buildAgentCommand("task/claim", opts, params)
		if err != nil {
			return err
		}
		return runAgentCommand(cmd, "task.claim", event, opts, nil)
	}}
	addTaskRevisionFlags(cmd, &taskID, &baseEventID)
	cmd.Flags().StringVar(&claimer, "claimer", "", "Claiming worker ID; server derives the authorized worker when omitted")
	addMutationFlags(cmd, &opts)
	return cmd
}

type taskUpdateFlags struct {
	taskID, baseEventID, status, assignee, title, description, priority, epic                string
	statusReason, notes, checkpoint, checkpointID, checkpointStatus, reviewer                string
	setLabels, addLabels, removeLabels, addDeps, removeDeps, evidenceIDs, reviewRequirements []string
	requestValidation, handoff                                                               bool
}

func newTaskUpdateCmd() *cobra.Command {
	var flags taskUpdateFlags
	opts := defaultMutationOptions()
	cmd := &cobra.Command{Use: "update", Short: "Update a task revision and optionally publish a checkpoint or handoff", RunE: func(cmd *cobra.Command, args []string) error {
		if err := requireTaskRevision(cmd, flags.taskID, flags.baseEventID); err != nil {
			return err
		}
		params, evidence, err := updateParams(cmd, flags)
		if err != nil {
			return err
		}
		event, err := buildAgentCommand("task/update", opts, params)
		if err != nil {
			return err
		}
		return runAgentCommand(cmd, "task.update", event, opts, evidence)
	}}
	addTaskUpdateFlags(cmd, &flags)
	addMutationFlags(cmd, &opts)
	return cmd
}

func newTaskBlockCmd() *cobra.Command {
	var taskID, baseEventID, reason, checkpoint string
	var evidenceIDs []string
	opts := defaultMutationOptions()
	cmd := &cobra.Command{Use: "block", Short: "Mark a task blocked with durable evidence", RunE: func(cmd *cobra.Command, args []string) error {
		if err := requireTaskRevision(cmd, taskID, baseEventID); err != nil {
			return err
		}
		if strings.TrimSpace(reason) == "" {
			return exitError(exitUsage, fmt.Errorf("--reason is required"))
		}
		evidence := cleanStrings(evidenceIDs)
		if len(evidence) == 0 {
			return exitError(exitUsage, fmt.Errorf("at least one --evidence-id is required when blocking"))
		}
		if strings.TrimSpace(checkpoint) == "" {
			checkpoint = "Blocked: " + strings.TrimSpace(reason)
		}
		params := map[string]any{
			"task_id": taskID, "base_event_id": baseEventID, "status": "blocked", "status_reason": reason,
			"blocker_description": reason, "evidence_ids": evidence, "checkpoint_summary": checkpoint,
			"checkpoint_status": "blocked", "checkpoint_evidence_ids": evidence,
		}
		event, err := buildAgentCommand("task/update", opts, params)
		if err != nil {
			return err
		}
		return runAgentCommand(cmd, "task.block", event, opts, evidence)
	}}
	addTaskRevisionFlags(cmd, &taskID, &baseEventID)
	cmd.Flags().StringVar(&reason, "reason", "", "Blocking reason (required)")
	cmd.Flags().StringVar(&checkpoint, "checkpoint", "", "Checkpoint summary; defaults to the blocking reason")
	cmd.Flags().StringSliceVar(&evidenceIDs, "evidence-id", nil, "Authoritative event, commit, URL, or external evidence ID (repeatable, required)")
	addMutationFlags(cmd, &opts)
	return cmd
}

func newTaskCloseCmd() *cobra.Command {
	var taskID, baseEventID, reason string
	var acceptanceEvidence []string
	opts := defaultMutationOptions()
	cmd := &cobra.Command{Use: "close", Short: "Close an accepted task revision", RunE: func(cmd *cobra.Command, args []string) error {
		if err := requireTaskRevision(cmd, taskID, baseEventID); err != nil {
			return err
		}
		evidence := cleanStrings(acceptanceEvidence)
		params := map[string]any{"task_id": taskID, "base_event_id": baseEventID}
		if strings.TrimSpace(reason) != "" {
			params["close_reason"] = strings.TrimSpace(reason)
		}
		if len(evidence) > 0 {
			params["acceptance_evidence_ids"] = evidence
		}
		event, err := buildAgentCommand("task/close", opts, params)
		if err != nil {
			return err
		}
		return runAgentCommand(cmd, "task.close", event, opts, evidence)
	}}
	addTaskRevisionFlags(cmd, &taskID, &baseEventID)
	cmd.Flags().StringVar(&reason, "reason", "", "Close reason")
	cmd.Flags().StringSliceVar(&acceptanceEvidence, "acceptance-evidence-id", nil, "Acceptance or validation evidence ID (repeatable)")
	addMutationFlags(cmd, &opts)
	return cmd
}

func newAgentQueueListCmd() *cobra.Command {
	var repoAddr, queue string
	opts := defaultMutationOptions()
	cmd := &cobra.Command{Use: "list", Short: "List an authoritative queue with a correlated response", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(repoAddr) == "" {
			return exitError(exitUsage, fmt.Errorf("--repo-addr is required"))
		}
		event, err := buildAgentCommand("queue/list", opts, map[string]any{"repo_addr": repoAddr, "queue": queue})
		if err != nil {
			return err
		}
		return runAgentCommand(cmd, "queue.list", event, opts, nil)
	}}
	cmd.Flags().StringVar(&repoAddr, "repo-addr", strings.TrimSpace(os.Getenv("NOSTRIG_REPO_ADDR")), "Canonical repository address (required)")
	cmd.Flags().StringVar(&queue, "queue", "backlog", "Queue name")
	cmd.Flags().Bool("json", false, "Emit the stable machine-readable JSON schema")
	addMutationFlags(cmd, &opts)
	return cmd
}

func newQueueClaimNextCmd() *cobra.Command {
	var repoAddr, queue, baseEventID string
	var leaseSeconds int
	opts := defaultMutationOptions()
	cmd := &cobra.Command{Use: "claim-next", Short: "Atomically reserve the next available queue task", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(repoAddr) == "" || !cmd.Flags().Changed("base-event-id") {
			return exitError(exitUsage, fmt.Errorf("--repo-addr and --base-event-id are required"))
		}
		if strings.TrimSpace(baseEventID) != "" {
			if _, err := gonostr.IDFromHex(strings.TrimSpace(baseEventID)); err != nil {
				return exitError(exitUsage, fmt.Errorf("invalid --base-event-id: %w", err))
			}
		}
		if leaseSeconds < 1 || leaseSeconds > 3600 {
			return exitError(exitUsage, fmt.Errorf("--lease-seconds must be between 1 and 3600"))
		}
		params := map[string]any{"repo_addr": repoAddr, "queue": queue, "base_event_id": baseEventID, "lease_seconds": int(leaseSeconds)}
		event, err := buildAgentCommand("queue/dequeue", opts, params)
		if err != nil {
			return err
		}
		return runAgentCommand(cmd, "queue.claim-next", event, opts, nil)
	}}
	cmd.Flags().StringVar(&repoAddr, "repo-addr", strings.TrimSpace(os.Getenv("NOSTRIG_REPO_ADDR")), "Canonical repository address (required)")
	cmd.Flags().StringVar(&queue, "queue", "backlog", "Queue name")
	cmd.Flags().StringVar(&baseEventID, "base-event-id", "", "Current queue event ID (required; pass an explicit empty value for an absent queue)")
	cmd.Flags().IntVar(&leaseSeconds, "lease-seconds", 300, "Reservation lease duration in seconds (1-3600)")
	cmd.Flags().Bool("json", false, "Emit the stable machine-readable JSON schema")
	addMutationFlags(cmd, &opts)
	return cmd
}

func defaultTaskQueryOptions() taskQueryOptions {
	return taskQueryOptions{pageSize: 500, maxPages: 1000, maxEvents: 100000, timeout: 30 * time.Second}
}

func addTaskQueryFlags(cmd *cobra.Command, opts *taskQueryOptions) {
	cmd.Flags().StringVar(&opts.repoAddr, "repo-addr", strings.TrimSpace(os.Getenv("NOSTRIG_REPO_ADDR")), "Canonical repository selector; defaults to NOSTRIG_REPO_ADDR")
	cmd.Flags().StringSliceVar(&opts.authors, "author", nil, "Trusted canonical author pubkey (repeatable; defaults to NOSTRIG_CANONICAL_AUTHORS)")
	cmd.Flags().StringSliceVar(&opts.relays, "relay", nil, "Relay WebSocket URL (repeatable; defaults to NOSTR_RELAY(S))")
	cmd.Flags().IntVar(&opts.pageSize, "page-size", opts.pageSize, "Relay pagination page size")
	cmd.Flags().IntVar(&opts.maxPages, "max-pages", opts.maxPages, "Maximum pages before an explicit incomplete-query error")
	cmd.Flags().IntVar(&opts.maxEvents, "max-events", opts.maxEvents, "Maximum events before an explicit incomplete-query error")
	cmd.Flags().DurationVar(&opts.timeout, "timeout", opts.timeout, "Bound for the complete query or watch")
}

func addTaskFilterFlags(cmd *cobra.Command, opts *taskFilterOptions) {
	cmd.Flags().StringSliceVar(&opts.statuses, "status", nil, "Status filter (repeatable)")
	cmd.Flags().StringVar(&opts.assignee, "assignee", "", "Assignee filter")
	cmd.Flags().StringSliceVar(&opts.labels, "label", nil, "Required label (repeatable)")
	cmd.Flags().StringVar(&opts.queue, "queue", "", "Queue filter")
}

func resolveTaskQueryOptions(opts taskQueryOptions) (taskQueryOptions, error) {
	opts.repoAddr = strings.TrimSpace(opts.repoAddr)
	opts.relays = relaysWithEnv(opts.relays)
	if len(opts.authors) == 0 {
		opts.authors = splitEnvList(os.Getenv("NOSTRIG_CANONICAL_AUTHORS"))
	}
	opts.authors = cleanStrings(opts.authors)
	opts.taskIDs = cleanStrings(opts.taskIDs)
	if opts.repoAddr == "" && len(opts.taskIDs) == 0 {
		return opts, fmt.Errorf("--repo-addr or at least one --task-id is required")
	}
	if len(opts.relays) == 0 {
		return opts, fmt.Errorf("at least one --relay or NOSTR_RELAY is required")
	}
	if len(opts.authors) == 0 {
		return opts, fmt.Errorf("at least one --author or NOSTRIG_CANONICAL_AUTHORS is required")
	}
	if opts.pageSize <= 0 || opts.maxPages <= 0 || opts.maxEvents <= 0 || opts.timeout <= 0 {
		return opts, fmt.Errorf("query bounds and --timeout must be positive")
	}
	return opts, nil
}

func listTaskSnapshot(parent context.Context, opts taskQueryOptions) (*beadspb.Export, error) {
	resolved, err := resolveTaskQueryOptions(opts)
	if err != nil {
		return nil, exitError(exitUsage, err)
	}
	ctx, cancel := context.WithTimeout(parent, resolved.timeout)
	defer cancel()
	return taskfabric.ListTaskState(ctx, nil, taskfabric.TaskListOptions{
		Relays: resolved.relays, RepoAddr: resolved.repoAddr, TaskIDs: resolved.taskIDs, Authors: resolved.authors,
		Pagination: nip34.PaginationOptions{PageSize: resolved.pageSize, MaxPages: resolved.maxPages, MaxEvents: resolved.maxEvents},
	})
}

func taskIDFromArgs(flag string, args []string) (string, error) {
	flag = strings.TrimPrefix(strings.TrimSpace(flag), "task:")
	if flag != "" && len(args) > 0 {
		return "", fmt.Errorf("provide a task ID either positionally or with --task-id, not both")
	}
	if flag != "" {
		return flag, nil
	}
	if len(args) == 1 && strings.TrimSpace(args[0]) != "" {
		return strings.TrimPrefix(strings.TrimSpace(args[0]), "task:"), nil
	}
	return "", fmt.Errorf("task ID is required")
}

func taskJSONRecord(issue *beadspb.Issue) (taskRecordJSON, error) {
	doc, err := taskmodel.FromProto(issue)
	if err != nil {
		return taskRecordJSON{}, err
	}
	eventID := ""
	if doc.Metadata != nil {
		eventID = strings.TrimSpace(doc.Metadata["nostr.id"])
	}
	return taskRecordJSON{EventID: eventID, Revision: eventID, EvidenceIDs: issueEvidenceIDs(issue), Task: doc}, nil
}

func taskJSONRecords(issues []*beadspb.Issue) ([]taskRecordJSON, error) {
	out := make([]taskRecordJSON, 0, len(issues))
	for _, issue := range issues {
		if issue == nil {
			continue
		}
		record, err := taskJSONRecord(issue)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Task.ID < out[j].Task.ID })
	return out, nil
}

func emitTaskList(cmd *cobra.Command, issues []*beadspb.Issue, schema string) error {
	records, err := taskJSONRecords(issues)
	if err != nil {
		return err
	}
	if agentJSON(cmd) {
		return writeJSON(cmd.OutOrStdout(), taskListEnvelope{Schema: schema, Count: len(records), Tasks: records})
	}
	for _, record := range records {
		if _, err := fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\t%s\t%s\n", record.Task.ID, record.Task.Status, record.Revision, record.Task.Title); err != nil {
			return err
		}
	}
	return nil
}

func filterIssues(issues []*beadspb.Issue, filters taskFilterOptions) []*beadspb.Issue {
	statuses, labels := cleanStrings(filters.statuses), cleanStrings(filters.labels)
	out := make([]*beadspb.Issue, 0, len(issues))
	for _, issue := range issues {
		if issue == nil {
			continue
		}
		if len(statuses) > 0 && !containsFold(statuses, nip34.StatusString(issue.Status)) {
			continue
		}
		if strings.TrimSpace(filters.assignee) != "" && issue.Assignee != strings.TrimSpace(filters.assignee) {
			continue
		}
		if strings.TrimSpace(filters.queue) != "" && issue.Queue != strings.TrimSpace(filters.queue) {
			continue
		}
		if !containsAll(issue.Labels, labels) {
			continue
		}
		out = append(out, issue)
	}
	return out
}

func readyIssues(issues []*beadspb.Issue) []*beadspb.Issue {
	byID := make(map[string]*beadspb.Issue, len(issues))
	for _, issue := range issues {
		if issue != nil {
			byID[issue.Id] = issue
		}
	}
	out := make([]*beadspb.Issue, 0, len(issues))
	for _, issue := range issues {
		if issue == nil || issue.Status != beadspb.Status_STATUS_OPEN || strings.TrimSpace(issue.Assignee) != "" {
			continue
		}
		ready := true
		for _, dependency := range blockingDependencyIDs(issue) {
			dep := byID[strings.TrimPrefix(strings.TrimSpace(dependency), "task:")]
			if dep == nil || dep.Status != beadspb.Status_STATUS_CLOSED {
				ready = false
				break
			}
		}
		if ready {
			out = append(out, issue)
		}
	}
	return out
}

func blockingDependencyIDs(issue *beadspb.Issue) []string {
	if issue == nil {
		return nil
	}
	if len(issue.Dependencies) == 0 {
		return cleanStrings(issue.DependsOn)
	}
	ids := make([]string, 0, len(issue.Dependencies))
	for _, dependency := range issue.Dependencies {
		if dependency == nil || (dependency.Type != "blocks" && dependency.Type != "blocked-by") {
			continue
		}
		ids = append(ids, dependency.DependsOnId)
	}
	return cleanStrings(ids)
}

func defaultMutationOptions() mutationOptions { return mutationOptions{timeout: 30 * time.Second} }

func addMutationFlags(cmd *cobra.Command, opts *mutationOptions) {
	cmd.Flags().StringVar(&opts.recipient, "recipient", strings.TrimSpace(os.Getenv("NOSTRIG_RECIPIENT")), "ContextVM server pubkey; defaults to NOSTRIG_RECIPIENT")
	cmd.Flags().StringSliceVar(&opts.relays, "relay", nil, "Relay WebSocket URL (repeatable; defaults to NOSTR_RELAY(S))")
	cmd.Flags().DurationVar(&opts.timeout, "response-timeout", opts.timeout, "Bound for publish and the correlated response")
	cmd.Flags().BoolVar(&opts.noWait, "no-wait", false, "Return after relay acceptance instead of waiting for the correlated authoritative response")
	cmd.Flags().BoolVar(&opts.dryRun, "dry-run", false, "Print the unsigned command plan without signing or publishing")
}

func requireTaskRevision(cmd *cobra.Command, taskID, baseEventID string) error {
	if strings.TrimSpace(taskID) == "" || !cmd.Flags().Changed("base-event-id") || strings.TrimSpace(baseEventID) == "" {
		return exitError(exitUsage, fmt.Errorf("--task-id and a non-empty --base-event-id are required"))
	}
	if _, err := gonostr.IDFromHex(strings.TrimSpace(baseEventID)); err != nil {
		return exitError(exitUsage, fmt.Errorf("invalid --base-event-id: %w", err))
	}
	return nil
}

func addTaskRevisionFlags(cmd *cobra.Command, taskID, baseEventID *string) {
	cmd.Flags().StringVar(taskID, "task-id", "", "Task ID (required)")
	cmd.Flags().StringVar(baseEventID, "base-event-id", "", "Exact canonical task event ID precondition (required)")
}

func addTaskUpdateFlags(cmd *cobra.Command, f *taskUpdateFlags) {
	addTaskRevisionFlags(cmd, &f.taskID, &f.baseEventID)
	cmd.Flags().StringVar(&f.status, "status", "", "New task status")
	cmd.Flags().StringVar(&f.assignee, "assignee", "", "New assignee; empty clears it")
	cmd.Flags().StringVar(&f.title, "title", "", "New title")
	cmd.Flags().StringVar(&f.description, "description", "", "New description")
	cmd.Flags().StringVar(&f.priority, "priority", "", "New priority")
	cmd.Flags().StringVar(&f.epic, "epic", "", "New parent epic; empty clears it")
	cmd.Flags().StringVar(&f.statusReason, "status-reason", "", "Durable status reason")
	cmd.Flags().StringVar(&f.notes, "notes", "", "Durable notes or handoff details")
	cmd.Flags().StringSliceVar(&f.setLabels, "set-label", nil, "Replace labels (repeatable)")
	cmd.Flags().StringSliceVar(&f.addLabels, "add-label", nil, "Add a label (repeatable)")
	cmd.Flags().StringSliceVar(&f.removeLabels, "remove-label", nil, "Remove a label (repeatable)")
	cmd.Flags().StringSliceVar(&f.addDeps, "add-dep", nil, "Add dependency task ID (repeatable)")
	cmd.Flags().StringSliceVar(&f.removeDeps, "remove-dep", nil, "Remove dependency task ID (repeatable)")
	cmd.Flags().StringVar(&f.checkpoint, "checkpoint", "", "Append a durable checkpoint summary")
	cmd.Flags().StringVar(&f.checkpointID, "checkpoint-id", "", "Checkpoint ID; server derives one from the command event when omitted")
	cmd.Flags().StringVar(&f.checkpointStatus, "checkpoint-status", "progress", "Checkpoint status")
	cmd.Flags().BoolVar(&f.handoff, "handoff", false, "Record the checkpoint as a durable session handoff")
	cmd.Flags().StringSliceVar(&f.evidenceIDs, "evidence-id", nil, "Authoritative event, commit, URL, or external evidence ID (repeatable)")
	cmd.Flags().BoolVar(&f.requestValidation, "request-validation", false, "Set review state to requested")
	cmd.Flags().StringVar(&f.reviewer, "reviewer", "", "Requested reviewer identity")
	cmd.Flags().StringSliceVar(&f.reviewRequirements, "review-requirement", nil, "Validation requirement (repeatable)")
}

func updateParams(cmd *cobra.Command, f taskUpdateFlags) (map[string]any, []string, error) {
	params := map[string]any{"task_id": f.taskID, "base_event_id": f.baseEventID}
	for flag, key := range map[string]string{"status": "status", "assignee": "assignee", "title": "title", "description": "description", "priority": "priority", "epic": "epic", "status-reason": "status_reason", "notes": "notes"} {
		if cmd.Flags().Changed(flag) {
			value, _ := cmd.Flags().GetString(flag)
			params[key] = value
		}
	}
	for flag, key := range map[string]string{"set-label": "set_labels", "add-label": "add_labels", "remove-label": "remove_labels", "add-dep": "add_dependencies", "remove-dep": "remove_dependencies"} {
		if cmd.Flags().Changed(flag) {
			values, _ := cmd.Flags().GetStringSlice(flag)
			params[key] = cleanStrings(values)
		}
	}
	evidence := cleanStrings(f.evidenceIDs)
	if len(evidence) > 0 {
		params["evidence_ids"] = evidence
	}
	if f.handoff && strings.TrimSpace(f.checkpoint) == "" {
		return nil, nil, exitError(exitUsage, fmt.Errorf("--handoff requires --checkpoint"))
	}
	if strings.TrimSpace(f.checkpoint) != "" {
		params["checkpoint_summary"] = strings.TrimSpace(f.checkpoint)
		params["checkpoint_status"] = strings.TrimSpace(f.checkpointStatus)
		if f.handoff {
			params["checkpoint_status"] = "handoff"
		}
		if strings.TrimSpace(f.checkpointID) != "" {
			params["checkpoint_id"] = strings.TrimSpace(f.checkpointID)
		}
		if len(evidence) > 0 {
			params["checkpoint_evidence_ids"] = evidence
		}
	}
	if f.requestValidation {
		params["request_validation"] = true
		if strings.TrimSpace(f.reviewer) != "" {
			params["reviewer"] = strings.TrimSpace(f.reviewer)
		}
		if values := cleanStrings(f.reviewRequirements); len(values) > 0 {
			params["review_requirements"] = values
		}
	}
	if len(params) == 2 {
		return nil, nil, exitError(exitUsage, fmt.Errorf("provide at least one update, checkpoint, evidence, or validation field"))
	}
	return params, evidence, nil
}

func buildAgentCommand(method string, opts mutationOptions, params map[string]any) (*gonostr.Event, error) {
	recipient := strings.ToLower(strings.TrimSpace(opts.recipient))
	if recipient == "" {
		return nil, exitError(exitUsage, fmt.Errorf("--recipient or NOSTRIG_RECIPIENT is required"))
	}
	pubkey, err := gonostr.PubKeyFromHex(recipient)
	if err != nil {
		return nil, exitError(exitUsage, fmt.Errorf("invalid ContextVM recipient pubkey: %w", err))
	}
	return nip34.BuildContextVMCommand(method, pubkey.Hex(), params, time.Now().UTC())
}

func runAgentCommand(cmd *cobra.Command, operation string, event *gonostr.Event, opts mutationOptions, evidenceIDs []string) error {
	evidenceIDs = cleanStrings(evidenceIDs)
	envelope := mutationEnvelope{
		Schema: "nostrig.mutation.v1", Operation: operation, DryRun: opts.dryRun,
		SubmittedEvidenceIDs: evidenceIDs, EvidenceIDs: []string{},
	}
	if opts.dryRun {
		envelope.Event = event
		return writeJSON(cmd.OutOrStdout(), envelope)
	}
	if opts.timeout <= 0 {
		return exitError(exitUsage, fmt.Errorf("--response-timeout must be positive"))
	}
	relays := relaysWithEnv(opts.relays)
	if len(relays) == 0 {
		return exitError(exitUsage, fmt.Errorf("at least one --relay or NOSTR_RELAY is required"))
	}
	ctx, cancel := context.WithTimeout(cmd.Context(), opts.timeout)
	defer cancel()
	signer, _, err := signerFromOptions(ctx, signingOptions{}, true)
	if err != nil {
		return err
	}
	contextSigner, err := commandContextVMSigner(signer)
	if err != nil {
		return err
	}
	recipient, _ := nip34.TagFirst(event, "p")
	var request cascontextvm.Request
	if err := json.Unmarshal([]byte(event.Content), &request); err != nil {
		return err
	}
	outer, inner, err := cascontextvm.Wrap(ctx, contextSigner, recipient, request)
	if err != nil {
		return err
	}
	envelope.RequestEventID = inner.ID.Hex()
	envelope.PublishedEventID = outer.ID.Hex()
	envelope.CorrelationID = strings.Trim(strings.TrimSpace(string(request.ID)), "\"")
	var waiter *taskfabric.ContextVMResponseWaiter
	if !opts.noWait {
		waiter, err = taskfabric.PrepareContextVMResponseWait(ctx, relays, (*gonostr.Event)(inner), recipient, opts.timeout)
		if err != nil {
			return err
		}
		defer waiter.Close()
	}
	accepted, err := casnostr.Publish(ctx, relays, *outer)
	if err != nil {
		envelope.Ambiguous = accepted > 0
		envelope.Error = err.Error()
		if agentJSON(cmd) {
			if writeErr := writeJSON(cmd.OutOrStdout(), envelope); writeErr != nil {
				return writeErr
			}
		}
		return err
	}
	if accepted == 0 {
		envelope.Error = "no relay accepted ContextVM gift wrap"
		if agentJSON(cmd) {
			if writeErr := writeJSON(cmd.OutOrStdout(), envelope); writeErr != nil {
				return writeErr
			}
		}
		return fmt.Errorf("no relay accepted ContextVM gift wrap")
	}
	if !opts.noWait {
		response, waitErr := waiter.Wait()
		if waitErr != nil {
			envelope.Ambiguous = true
			envelope.Error = waitErr.Error()
			if agentJSON(cmd) {
				if writeErr := writeJSON(cmd.OutOrStdout(), envelope); writeErr != nil {
					return writeErr
				}
			}
			if errors.Is(waitErr, context.DeadlineExceeded) || strings.Contains(strings.ToLower(waitErr.Error()), "deadline exceeded") {
				return exitError(exitTimeout, waitErr)
			}
			return waitErr
		}
		envelope.Acknowledged = true
		if response.Event != nil {
			envelope.ResponseEventID = response.Event.ID.Hex()
		}
		if response.Result != nil {
			envelope.Result = append(json.RawMessage(nil), (*response.Result)...)
			envelope.EvidenceIDs = mergeStrings(envelope.EvidenceIDs, evidenceIDsFromResult(envelope.Result))
		}
		if response.Error != "" {
			envelope.Error, envelope.ErrorCode = response.Error, response.ErrorCode
			envelope.ErrorData = append(json.RawMessage(nil), response.ErrorData...)
		} else if err := validateMutationACK(operation, envelope.Result); err != nil {
			envelope.Error = err.Error()
		}
	} else {
		envelope.Ambiguous = true
	}
	if agentJSON(cmd) {
		if err := writeJSON(cmd.OutOrStdout(), envelope); err != nil {
			return err
		}
	} else {
		if _, err := fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\n", operation, envelope.RequestEventID); err != nil {
			return err
		}
	}
	if envelope.Error != "" {
		code := exitRejected
		lower := strings.ToLower(envelope.Error)
		if envelope.ErrorCode == taskfabric.ConflictErrorCode || strings.Contains(lower, "conflict") || strings.Contains(lower, "stale") || strings.Contains(lower, "already_claimed") {
			code = exitConflict
		} else if strings.Contains(lower, "not found") {
			code = exitNotFound
		}
		return exitError(code, fmt.Errorf("remote command rejected: %s", envelope.Error))
	}
	return nil
}

func issueEvidenceIDs(issue *beadspb.Issue) []string {
	if issue == nil {
		return []string{}
	}
	var refs []*beadspb.ArtifactReference
	refs = append(refs, issue.Evidence...)
	for _, checkpoint := range issue.Checkpoints {
		if checkpoint != nil {
			refs = append(refs, checkpoint.Evidence...)
		}
	}
	if issue.QualityGate != nil {
		refs = append(refs, issue.QualityGate.Evidence...)
	}
	for _, attempt := range issue.ExecutionAttempts {
		if attempt != nil {
			refs = append(refs, attempt.Evidence...)
		}
	}
	ids := make([]string, 0, len(refs))
	for _, ref := range refs {
		if ref == nil {
			continue
		}
		for _, value := range []string{ref.Reference, ref.Sha256, ref.Url} {
			if strings.TrimSpace(value) != "" {
				ids = append(ids, strings.TrimSpace(value))
				break
			}
		}
	}
	ids = cleanStrings(ids)
	sort.Strings(ids)
	return ids
}

func validateMutationACK(operation string, raw json.RawMessage) error {
	if !strings.HasPrefix(operation, "task.") {
		return nil
	}
	var result struct {
		EventID  string `json:"event_id"`
		Revision string `json:"revision"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &result) != nil {
		return fmt.Errorf("malformed task ACK result")
	}
	if result.EventID == "" || result.Revision == "" || result.EventID != result.Revision {
		return fmt.Errorf("task ACK is missing a consistent event_id/revision")
	}
	if _, err := gonostr.IDFromHex(result.EventID); err != nil {
		return fmt.Errorf("task ACK contains an invalid revision: %w", err)
	}
	return nil
}

func evidenceIDsFromResult(raw json.RawMessage) []string {
	var body map[string]any
	if json.Unmarshal(raw, &body) != nil {
		return nil
	}
	var out []string
	for _, key := range []string{"evidence_id", "evidence_ids", "acceptance_evidence_ids"} {
		value, ok := body[key]
		if !ok {
			continue
		}
		switch typed := value.(type) {
		case string:
			out = append(out, typed)
		case []any:
			for _, item := range typed {
				out = append(out, fmt.Sprint(item))
			}
		}
	}
	return cleanStrings(out)
}

func agentJSON(cmd *cobra.Command) bool {
	value, err := cmd.Flags().GetBool("json")
	return err == nil && value
}

func writeJSON(w io.Writer, value any) error {
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(value)
}

func containsFold(values []string, target string) bool {
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), strings.TrimSpace(target)) {
			return true
		}
	}
	return false
}

func containsAll(values, required []string) bool {
	for _, item := range required {
		found := false
		for _, value := range values {
			if value == item {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func mergeStrings(left, right []string) []string {
	out := cleanStrings(append(append([]string(nil), left...), right...))
	sort.Strings(out)
	return out
}
