package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	gonostr "fiatjaf.com/nostr"
	"fiatjaf.com/nostr/nip19"
	cascontextvm "git.sharegap.net/cascadia/cascadia-go/contextvm"
	casnostr "git.sharegap.net/cascadia/cascadia-go/nostr"
	"github.com/chebizarro/nostrig/internal/converter"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
	"github.com/chebizarro/nostrig/internal/taskfabric"
	"github.com/spf13/cobra"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := newRootCmd().ExecuteContext(ctx); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "nostrig",
		Short: "Relay-backed NIP-34 to beads task-fabric bridge",
	}

	cmd.AddCommand(newFetchCmd(), newPublishCmd(), newSyncCmd(), newMigrateCmd(), newImportCmd(), newCreateCmd(), newClaimCmd(), newAssignCmd(), newUpdateCmd(), newCloseCmd(), newDeleteCmd(), newQueueCmd(), newServeCmd(), newOutboxCmd())
	return cmd
}

func newFetchCmd() *cobra.Command {
	var repoID string
	var owner string
	var relays []string
	var outDir string
	var idFormat string
	var idPrefix string

	fetchCmd := &cobra.Command{
		Use:   "fetch",
		Short: "Fetch a single repo's NIP-34 events and write .beads JSONL artifacts",
		RunE: func(cmd *cobra.Command, args []string) error {
			format, ownerHex, err := parseFetchInputs(repoID, owner, idFormat)
			if err != nil {
				return err
			}
			if outDir == "" {
				return fmt.Errorf("--out is required")
			}

			opts := converter.FetchOptions{RepoID: repoID, Owner: ownerHex, Relays: relays, OutDir: outDir, IDFormat: format, IDPrefix: idPrefix}
			return converter.NewPipeline().Run(cmd.Context(), opts)
		},
	}

	addFetchFlags(fetchCmd, &repoID, &owner, &relays, &idFormat, &idPrefix)
	fetchCmd.Flags().StringVar(&outDir, "out", ".", "Output directory (writes <out>/.beads/*.jsonl)")

	return fetchCmd
}

func newPublishCmd() *cobra.Command {
	var repoID string
	var owner string
	var relays []string
	var idFormat string
	var idPrefix string
	var canonicalAuthor string
	var signing signingOptions

	publishCmd := &cobra.Command{
		Use:   "publish",
		Short: "Publish converted NIP-34 work items as canonical task-fabric events",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			format, ownerHex, err := parseFetchInputs(repoID, owner, idFormat)
			if err != nil {
				return err
			}

			result, err := converter.NewPipeline().Export(ctx, converter.FetchOptions{RepoID: repoID, Owner: ownerHex, Relays: relays, IDFormat: format, IDPrefix: idPrefix})
			if err != nil {
				return err
			}
			signer, _, err := signerFromOptions(ctx, signing, !signing.dryRun)
			if err != nil {
				return err
			}
			author := strings.ToLower(strings.TrimSpace(canonicalAuthor))
			if signer != nil {
				signerAuthor, err := publicKeyFromSigner(ctx, signer)
				if err != nil {
					return err
				}
				if author != "" && author != strings.ToLower(signerAuthor) {
					return fmt.Errorf("--canonical-author does not match signer pubkey")
				}
				author = strings.ToLower(signerAuthor)
			}
			events, err := nip34.BuildCanonicalEvents(result.Export, author, time.Now().UTC())
			if err != nil {
				return err
			}
			if len(events) == 0 {
				return fmt.Errorf("no canonical events generated")
			}
			if signing.dryRun {
				if signer != nil {
					if err := signEvents(ctx, signer, events); err != nil {
						return err
					}
				}
				return writeEvents(cmd, events)
			}

			if err := nip34.NewPublisher().Publish(ctx, result.Relays, signer, events); err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "published %d event(s) to %d relay(s)\n", len(events), len(result.Relays))
			return err
		},
	}

	addFetchFlags(publishCmd, &repoID, &owner, &relays, &idFormat, &idPrefix)
	publishCmd.Flags().StringVar(&canonicalAuthor, "canonical-author", "", "Canonical ledger author pubkey; defaults to signer pubkey")
	addSigningFlags(publishCmd, &signing)
	return publishCmd
}

func newSyncCmd() *cobra.Command {
	var repoID string
	var owner string
	var repoAddr string
	var relays []string
	var authors []string
	var taskIDs []string
	var outDir string
	var cachePath string
	var limit int
	var failOnConflict bool
	var push bool
	var syncNIP34Status bool
	var signing signingOptions

	syncCmd := &cobra.Command{
		Use:   "sync",
		Short: "Pull canonical task-state events into the durable task cache and render .beads JSONL",
		RunE: func(cmd *cobra.Command, args []string) error {
			ownerHex, err := resolveOwner(owner)
			if err != nil {
				return fmt.Errorf("invalid owner: %w", err)
			}
			addr := strings.TrimSpace(repoAddr)
			if addr == "" && strings.TrimSpace(repoID) != "" && ownerHex != "" {
				addr = nip34.RepoAddress(ownerHex, repoID)
			}
			relays = relaysWithEnv(relays)
			signer, _, err := signerFromOptions(cmd.Context(), signing, push && !signing.dryRun)
			if err != nil {
				return err
			}
			result, err := taskfabric.Sync(cmd.Context(), nip34.NewClient(), taskfabric.SyncOptions{Relays: relays, RepoAddr: addr, TaskIDs: taskIDs, Authors: authors, OutDir: outDir, CachePath: cachePath, Limit: limit, FailOnConflict: failOnConflict, Push: push && !signing.dryRun, SyncNIP34Status: syncNIP34Status, Signer: signer})
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "synced %d task(s) from %d event(s) into %s/.beads via cache %s (%d conflict(s), %d published)\n", len(result.Export.Issues), result.EventCount, strings.TrimRight(outDir, "/"), result.CachePath, result.ConflictCount, result.PublishedCount)
			return err
		},
	}

	syncCmd.Flags().StringVar(&repoID, "repo-id", "", "Repository id (d tag); used with --owner to derive --repo-addr")
	syncCmd.Flags().StringVar(&owner, "owner", "", "Repository owner pubkey (hex or npub); used with --repo-id to derive --repo-addr")
	syncCmd.Flags().StringVar(&repoAddr, "repo-addr", "", "Canonical repo address tag to sync (30617:<owner>:<repo-id>)")
	syncCmd.Flags().StringSliceVar(&relays, "relay", nil, "Relay websocket URL(s) to use (repeatable); falls back to NOSTR_RELAY/NOSTR_RELAYS")
	syncCmd.Flags().StringSliceVar(&authors, "author", nil, "Optional canonical task-state event author pubkey filter (repeatable)")
	syncCmd.Flags().StringSliceVar(&taskIDs, "task-id", nil, "Task id(s) to sync by exact d tag (repeatable)")
	syncCmd.Flags().StringVar(&outDir, "out", ".", "Output directory (writes <out>/.beads/issues.jsonl)")
	syncCmd.Flags().StringVar(&cachePath, "cache", "", "Durable nostrig task cache path (default: <out>/.nostrig/task-cache.jsonl)")
	syncCmd.Flags().BoolVar(&failOnConflict, "fail-on-conflict", false, "Exit non-zero when local and relay changes conflict")
	syncCmd.Flags().BoolVar(&push, "push", false, "Publish local .beads changes back to relay after relay-source-of-truth reconciliation")
	syncCmd.Flags().BoolVar(&syncNIP34Status, "sync-nip34-status", false, "Opt-in: with --push, publish NIP-34 issue status events for linked tasks")
	syncCmd.Flags().IntVar(&limit, "limit", 500, "Maximum task-state events to request")
	addSigningFlags(syncCmd, &signing)
	return syncCmd
}

func newMigrateCmd() *cobra.Command {
	var outDir string
	var canonicalAuthor string
	var relays []string
	var signing signingOptions
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Publish existing .beads JSONL as canonical task-fabric events",
		RunE: func(cmd *cobra.Command, args []string) error {
			signer, _, err := signerFromOptions(cmd.Context(), signing, !signing.dryRun)
			if err != nil {
				return err
			}
			author := strings.ToLower(strings.TrimSpace(canonicalAuthor))
			if signer != nil {
				signerAuthor, err := publicKeyFromSigner(cmd.Context(), signer)
				if err != nil {
					return err
				}
				if author != "" && author != strings.ToLower(signerAuthor) {
					return fmt.Errorf("--canonical-author does not match signer pubkey")
				}
				author = strings.ToLower(signerAuthor)
			}
			result, err := taskfabric.Migrate(cmd.Context(), taskfabric.MigrateOptions{OutDir: outDir, CanonicalAuthor: author, Relays: relaysWithEnv(relays), Signer: signer, DryRun: signing.dryRun})
			if err != nil {
				return err
			}
			if signing.dryRun {
				return writeEvents(cmd, result.Events)
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "migrated %d issue(s), %d epic(s), published %d event(s)\n", len(result.Export.Issues), len(result.Export.Epics), result.PublishedCount)
			return err
		},
	}
	cmd.Flags().StringVar(&outDir, "out", ".", "Directory containing .beads/issues.jsonl and optional epics.jsonl")
	cmd.Flags().StringVar(&canonicalAuthor, "canonical-author", "", "Canonical ledger author pubkey; defaults to signer pubkey")
	cmd.Flags().StringSliceVar(&relays, "relay", nil, "Relay websocket URL(s) to publish to (repeatable); falls back to NOSTR_RELAY/NOSTR_RELAYS")
	addSigningFlags(cmd, &signing)
	return cmd
}

func newCreateCmd() *cobra.Command {
	var taskID, title, description, status, priority, epic, assignee, recipient string
	var repoAddr, repoID, owner string
	var labels, dependsOn, relays []string
	var signing signingOptions
	var response responseOptions

	cmd := &cobra.Command{
		Use:   "create",
		Short: "Publish a ContextVM task/create command",
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(taskID) == "" {
				return fmt.Errorf("--task-id is required")
			}
			if strings.TrimSpace(title) == "" {
				return fmt.Errorf("--title is required")
			}
			if strings.TrimSpace(recipient) == "" {
				return fmt.Errorf("--recipient is required")
			}
			params := map[string]any{"task_id": strings.TrimSpace(taskID), "title": strings.TrimSpace(title)}
			addr := strings.TrimSpace(repoAddr)
			if addr == "" && (strings.TrimSpace(repoID) != "" || strings.TrimSpace(owner) != "") {
				if strings.TrimSpace(repoID) == "" || strings.TrimSpace(owner) == "" {
					return fmt.Errorf("provide both --repo-id and --owner when --repo-addr is not set")
				}
				ownerHex, err := resolveOwner(owner)
				if err != nil {
					return fmt.Errorf("invalid owner: %w", err)
				}
				addr = nip34.RepoAddress(ownerHex, strings.TrimSpace(repoID))
			}
			if addr != "" {
				params["repo_addr"] = addr
			}
			if v := strings.TrimSpace(description); v != "" {
				params["description"] = v
			}
			if v := strings.TrimSpace(status); v != "" {
				params["status"] = v
			}
			if v := strings.TrimSpace(priority); v != "" {
				params["priority"] = v
			}
			if v := strings.TrimSpace(epic); v != "" {
				params["epic"] = v
			}
			if v := strings.TrimSpace(assignee); v != "" {
				params["assignee"] = v
			}
			if values := cleanStrings(labels); len(values) > 0 {
				params["labels"] = values
			}
			if values := cleanStrings(dependsOn); len(values) > 0 {
				params["depends_on"] = values
			}
			event, err := nip34.BuildContextVMCommand("task/create", recipient, params, time.Now().UTC())
			if err != nil {
				return err
			}
			return publishContextVMCommand(cmd, event, relays, signing, response, fmt.Sprintf("published create for %s", taskID))
		},
	}
	cmd.Flags().StringVar(&taskID, "task-id", "", "Task id to create (required)")
	cmd.Flags().StringVar(&title, "title", "", "Task title (required)")
	cmd.Flags().StringVar(&repoAddr, "repo-addr", "", "Canonical repository address (30617:<owner>:<repo-id>)")
	cmd.Flags().StringVar(&repoID, "repo-id", "", "Repository id used with --owner to derive --repo-addr")
	cmd.Flags().StringVar(&owner, "owner", "", "Repository owner hex/npub used with --repo-id to derive --repo-addr")
	cmd.Flags().StringVar(&description, "description", "", "Task description")
	cmd.Flags().StringVar(&status, "status", "open", "Initial task status")
	cmd.Flags().StringVar(&priority, "priority", "", "Task priority (P0-P4 or 0-4)")
	cmd.Flags().StringVar(&epic, "epic", "", "Parent epic id")
	cmd.Flags().StringVar(&assignee, "assignee", "", "Initial task assignee")
	cmd.Flags().StringSliceVar(&labels, "label", nil, "Task label(s), repeatable")
	cmd.Flags().StringSliceVar(&dependsOn, "depends-on", nil, "Dependency task id(s), repeatable")
	cmd.Flags().StringVar(&recipient, "recipient", "", "ContextVM recipient pubkey (required)")
	cmd.Flags().StringSliceVar(&relays, "relay", nil, "Relay websocket URL(s) to publish to (repeatable); falls back to NOSTR_RELAY/NOSTR_RELAYS")
	addSigningFlags(cmd, &signing)
	addResponseFlags(cmd, &response)
	return cmd
}

func newClaimCmd() *cobra.Command {
	var taskID string
	var claimer string
	var baseEventID string
	var recipient string
	var relays []string
	var signing signingOptions
	var response responseOptions

	claimCmd := &cobra.Command{
		Use:   "claim",
		Short: "Publish a ContextVM task/claim command for a worker/agent",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if strings.TrimSpace(taskID) == "" {
				return fmt.Errorf("--task-id is required")
			}
			if strings.TrimSpace(recipient) == "" {
				return fmt.Errorf("--recipient is required")
			}
			if !cmd.Flags().Changed("base-event-id") {
				return fmt.Errorf("--base-event-id is required")
			}
			signer, _, err := signerFromOptions(ctx, signing, !signing.dryRun)
			if err != nil {
				return err
			}
			if strings.TrimSpace(claimer) == "" && signer != nil {
				claimer, err = publicKeyFromSigner(ctx, signer)
				if err != nil {
					return err
				}
			}
			if strings.TrimSpace(claimer) == "" {
				return fmt.Errorf("--claimer is required when no signer public key is available")
			}

			event, err := nip34.BuildClaimDispatchAtRevision(taskID, claimer, baseEventID, recipient, time.Now().UTC())
			if err != nil {
				return err
			}
			events := []*gonostr.Event{event}
			if signing.dryRun {
				if signer != nil {
					if err := signEvents(ctx, signer, events); err != nil {
						return err
					}
				}
				return writeEvents(cmd, events)
			}

			relays = relaysWithEnv(relays)
			if len(relays) == 0 {
				return fmt.Errorf("at least one --relay or NOSTR_RELAY is required")
			}
			if err := nip34.NewPublisher().Publish(ctx, relays, signer, events); err != nil {
				return err
			}
			if _, err := fmt.Fprintf(cmd.OutOrStdout(), "published claim for %s to %d relay(s)\n", taskID, len(relays)); err != nil {
				return err
			}
			return maybeWaitForResponse(cmd, relays, event, response)
		},
	}

	claimCmd.Flags().StringVar(&taskID, "task-id", "", "Task id to claim (required)")
	claimCmd.Flags().StringVar(&claimer, "claimer", "", "Claiming agent/worker pubkey or stable id; defaults to signer pubkey when available")
	claimCmd.Flags().StringVar(&baseEventID, "base-event-id", "", "Canonical task event ID being claimed (required)")
	claimCmd.Flags().StringVar(&recipient, "recipient", "", "ContextVM recipient pubkey (required)")
	claimCmd.Flags().StringSliceVar(&relays, "relay", nil, "Relay websocket URL(s) to publish to (repeatable); falls back to NOSTR_RELAY/NOSTR_RELAYS")
	addSigningFlags(claimCmd, &signing)
	addResponseFlags(claimCmd, &response)
	return claimCmd
}

func newAssignCmd() *cobra.Command {
	var taskID string
	var assignee string
	var baseEventID string
	var recipient string
	var relays []string
	var signing signingOptions
	var response responseOptions

	assignCmd := &cobra.Command{
		Use:   "assign",
		Short: "Publish a ContextVM task/assign command",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if strings.TrimSpace(taskID) == "" {
				return fmt.Errorf("--task-id is required")
			}
			if strings.TrimSpace(assignee) == "" {
				return fmt.Errorf("--assignee is required")
			}
			if !cmd.Flags().Changed("base-event-id") {
				return fmt.Errorf("--base-event-id is required")
			}
			if strings.TrimSpace(recipient) == "" {
				return fmt.Errorf("--recipient is required")
			}
			signer, _, err := signerFromOptions(ctx, signing, !signing.dryRun)
			if err != nil {
				return err
			}
			event, err := nip34.BuildAssignCommandAtRevision(taskID, assignee, baseEventID, recipient, time.Now().UTC())
			if err != nil {
				return err
			}
			events := []*gonostr.Event{event}
			if signing.dryRun {
				if signer != nil {
					if err := signEvents(ctx, signer, events); err != nil {
						return err
					}
				}
				return writeEvents(cmd, events)
			}
			relays = relaysWithEnv(relays)
			if len(relays) == 0 {
				return fmt.Errorf("at least one --relay or NOSTR_RELAY is required")
			}
			if err := nip34.NewPublisher().Publish(ctx, relays, signer, events); err != nil {
				return err
			}
			if _, err := fmt.Fprintf(cmd.OutOrStdout(), "published assignment for %s to %d relay(s)\n", taskID, len(relays)); err != nil {
				return err
			}
			return maybeWaitForResponse(cmd, relays, event, response)
		},
	}

	assignCmd.Flags().StringVar(&taskID, "task-id", "", "Task id to assign (required)")
	assignCmd.Flags().StringVar(&assignee, "assignee", "", "Assignee agent/worker pubkey or stable id (required)")
	assignCmd.Flags().StringVar(&baseEventID, "base-event-id", "", "Canonical task event ID being assigned (required)")
	assignCmd.Flags().StringVar(&recipient, "recipient", "", "ContextVM recipient pubkey (required)")
	assignCmd.Flags().StringSliceVar(&relays, "relay", nil, "Relay websocket URL(s) to publish to (repeatable); falls back to NOSTR_RELAY/NOSTR_RELAYS")
	addSigningFlags(assignCmd, &signing)
	addResponseFlags(assignCmd, &response)
	return assignCmd
}

func newUpdateCmd() *cobra.Command {
	var taskID string
	var baseEventID string
	var recipient string
	var status string
	var assignee string
	var title string
	var description string
	var priority string
	var epic string
	var setLabels []string
	var addLabels []string
	var removeLabels []string
	var addDeps []string
	var removeDeps []string
	var relays []string
	var signing signingOptions
	var response responseOptions

	updateCmd := &cobra.Command{
		Use:   "update",
		Short: "Publish a ContextVM task/update command",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if strings.TrimSpace(taskID) == "" {
				return fmt.Errorf("--task-id is required")
			}
			if strings.TrimSpace(recipient) == "" {
				return fmt.Errorf("--recipient is required")
			}
			if !cmd.Flags().Changed("base-event-id") {
				return fmt.Errorf("--base-event-id is required")
			}
			params := map[string]any{"task_id": taskID, "base_event_id": baseEventID}
			if strings.TrimSpace(status) != "" {
				params["status"] = strings.TrimSpace(status)
			}
			if cmd.Flags().Changed("assignee") {
				params["assignee"] = strings.TrimSpace(assignee)
			}
			if cmd.Flags().Changed("title") {
				params["title"] = strings.TrimSpace(title)
			}
			if cmd.Flags().Changed("description") {
				params["description"] = description
			}
			if cmd.Flags().Changed("priority") {
				params["priority"] = strings.TrimSpace(priority)
			}
			if cmd.Flags().Changed("epic") {
				params["epic"] = strings.TrimSpace(epic)
			}
			if cmd.Flags().Changed("set-label") {
				params["set_labels"] = cleanStrings(setLabels)
			}
			if values := cleanStrings(addLabels); len(values) > 0 {
				params["add_labels"] = values
			}
			if values := cleanStrings(removeLabels); len(values) > 0 {
				params["remove_labels"] = values
			}
			if values := cleanStrings(addDeps); len(values) > 0 {
				params["add_dependencies"] = values
			}
			if values := cleanStrings(removeDeps); len(values) > 0 {
				params["remove_dependencies"] = values
			}
			if len(params) == 2 {
				return fmt.Errorf("provide at least one update field")
			}
			signer, _, err := signerFromOptions(ctx, signing, !signing.dryRun)
			if err != nil {
				return err
			}
			event, err := nip34.BuildContextVMCommand("task/update", recipient, params, time.Now().UTC())
			if err != nil {
				return err
			}
			events := []*gonostr.Event{event}
			if signing.dryRun {
				if signer != nil {
					if err := signEvents(ctx, signer, events); err != nil {
						return err
					}
				}
				return writeEvents(cmd, events)
			}
			relays = relaysWithEnv(relays)
			if len(relays) == 0 {
				return fmt.Errorf("at least one --relay or NOSTR_RELAY is required")
			}
			if err := nip34.NewPublisher().Publish(ctx, relays, signer, events); err != nil {
				return err
			}
			if _, err := fmt.Fprintf(cmd.OutOrStdout(), "published update for %s to %d relay(s)\n", taskID, len(relays)); err != nil {
				return err
			}
			return maybeWaitForResponse(cmd, relays, event, response)
		},
	}

	updateCmd.Flags().StringVar(&taskID, "task-id", "", "Task id to update (required)")
	updateCmd.Flags().StringVar(&baseEventID, "base-event-id", "", "Canonical task event ID being updated (required)")
	updateCmd.Flags().StringVar(&recipient, "recipient", "", "ContextVM recipient pubkey (required)")
	updateCmd.Flags().StringVar(&status, "status", "", "New task status")
	updateCmd.Flags().StringVar(&assignee, "assignee", "", "New task assignee")
	updateCmd.Flags().StringVar(&title, "title", "", "New task title")
	updateCmd.Flags().StringVar(&description, "description", "", "New task description")
	updateCmd.Flags().StringVar(&priority, "priority", "", "New task priority (P0-P4 or 0-4)")
	updateCmd.Flags().StringVar(&epic, "epic", "", "New parent epic id; empty clears it")
	updateCmd.Flags().StringSliceVar(&setLabels, "set-label", nil, "Replace task labels (repeatable)")
	updateCmd.Flags().StringSliceVar(&addLabels, "add-label", nil, "Add task label(s), repeatable")
	updateCmd.Flags().StringSliceVar(&removeLabels, "remove-label", nil, "Remove task label(s), repeatable")
	updateCmd.Flags().StringSliceVar(&addDeps, "add-dep", nil, "Add dependency task id(s), repeatable")
	updateCmd.Flags().StringSliceVar(&removeDeps, "remove-dep", nil, "Remove dependency task id(s), repeatable")
	updateCmd.Flags().StringSliceVar(&relays, "relay", nil, "Relay websocket URL(s) to publish to (repeatable); falls back to NOSTR_RELAY/NOSTR_RELAYS")
	addSigningFlags(updateCmd, &signing)
	addResponseFlags(updateCmd, &response)
	return updateCmd
}

func newCloseCmd() *cobra.Command {
	var taskID string
	var baseEventID string
	var recipient string
	var relays []string
	var signing signingOptions
	var response responseOptions

	closeCmd := &cobra.Command{
		Use:   "close",
		Short: "Publish a ContextVM task/close command",
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(taskID) == "" {
				return fmt.Errorf("--task-id is required")
			}
			if strings.TrimSpace(recipient) == "" {
				return fmt.Errorf("--recipient is required")
			}
			if !cmd.Flags().Changed("base-event-id") {
				return fmt.Errorf("--base-event-id is required")
			}
			event, err := nip34.BuildCloseCommandAtRevision(taskID, baseEventID, recipient, time.Now().UTC())
			if err != nil {
				return err
			}
			return publishContextVMCommand(cmd, event, relays, signing, response, fmt.Sprintf("published close for %s", taskID))
		},
	}
	closeCmd.Flags().StringVar(&taskID, "task-id", "", "Task id to close (required)")
	closeCmd.Flags().StringVar(&baseEventID, "base-event-id", "", "Canonical task event ID being closed (required)")
	closeCmd.Flags().StringVar(&recipient, "recipient", "", "ContextVM recipient pubkey (required)")
	closeCmd.Flags().StringSliceVar(&relays, "relay", nil, "Relay websocket URL(s) to publish to (repeatable); falls back to NOSTR_RELAY/NOSTR_RELAYS")
	addSigningFlags(closeCmd, &signing)
	addResponseFlags(closeCmd, &response)
	return closeCmd
}

func newDeleteCmd() *cobra.Command {
	var taskID, baseEventID, recipient string
	var relays []string
	var signing signingOptions
	var response responseOptions
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Publish a ContextVM task/delete command",
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(taskID) == "" {
				return fmt.Errorf("--task-id is required")
			}
			if strings.TrimSpace(recipient) == "" {
				return fmt.Errorf("--recipient is required")
			}
			if !cmd.Flags().Changed("base-event-id") {
				return fmt.Errorf("--base-event-id is required")
			}
			event, err := nip34.BuildContextVMCommand("task/delete", recipient, map[string]any{"task_id": strings.TrimSpace(taskID), "base_event_id": baseEventID}, time.Now().UTC())
			if err != nil {
				return err
			}
			return publishContextVMCommand(cmd, event, relays, signing, response, fmt.Sprintf("published delete for %s", taskID))
		},
	}
	cmd.Flags().StringVar(&taskID, "task-id", "", "Task id to delete (required)")
	cmd.Flags().StringVar(&baseEventID, "base-event-id", "", "Canonical task event ID being deleted (required)")
	cmd.Flags().StringVar(&recipient, "recipient", "", "ContextVM recipient pubkey (required)")
	cmd.Flags().StringSliceVar(&relays, "relay", nil, "Relay websocket URL(s) to publish to (repeatable); falls back to NOSTR_RELAY/NOSTR_RELAYS")
	addSigningFlags(cmd, &signing)
	addResponseFlags(cmd, &response)
	return cmd
}

func newQueueCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "queue", Short: "Publish ContextVM queue commands"}
	cmd.AddCommand(newQueueEnqueueCmd(), newQueueDequeueCmd(), newQueueListCmd())
	return cmd
}

func newQueueEnqueueCmd() *cobra.Command {
	var repoAddr, queue, taskID, baseEventID, recipient string
	var relays []string
	var signing signingOptions
	var response responseOptions
	cmd := &cobra.Command{Use: "enqueue", Short: "Publish a ContextVM queue/enqueue command", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(taskID) == "" {
			return fmt.Errorf("--task-id is required")
		}
		if strings.TrimSpace(recipient) == "" {
			return fmt.Errorf("--recipient is required")
		}
		if strings.TrimSpace(repoAddr) == "" {
			return fmt.Errorf("--repo-addr is required")
		}
		if !cmd.Flags().Changed("base-event-id") {
			return fmt.Errorf("--base-event-id is required")
		}
		ev, err := nip34.BuildQueueEnqueueCommandAtRevision(repoAddr, queue, taskID, baseEventID, recipient, time.Now().UTC())
		if err != nil {
			return err
		}
		return publishContextVMCommand(cmd, ev, relays, signing, response, fmt.Sprintf("published enqueue for %s", taskID))
	}}
	cmd.Flags().StringVar(&repoAddr, "repo-addr", "", "Canonical repository address (required)")
	cmd.Flags().StringVar(&queue, "queue", "backlog", "Queue name")
	cmd.Flags().StringVar(&taskID, "task-id", "", "Task id to enqueue (required)")
	cmd.Flags().StringVar(&baseEventID, "base-event-id", "", "Canonical queue event ID being updated (required; pass empty for an absent queue)")
	cmd.Flags().StringVar(&recipient, "recipient", "", "ContextVM recipient pubkey (required)")
	cmd.Flags().StringSliceVar(&relays, "relay", nil, "Relay websocket URL(s) to publish to (repeatable); falls back to NOSTR_RELAY/NOSTR_RELAYS")
	addSigningFlags(cmd, &signing)
	addResponseFlags(cmd, &response)
	return cmd
}

func newQueueDequeueCmd() *cobra.Command {
	var repoAddr, queue, baseEventID, recipient string
	var relays []string
	var signing signingOptions
	var response responseOptions
	cmd := &cobra.Command{Use: "dequeue", Short: "Publish a ContextVM queue/dequeue command", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(recipient) == "" {
			return fmt.Errorf("--recipient is required")
		}
		if strings.TrimSpace(repoAddr) == "" {
			return fmt.Errorf("--repo-addr is required")
		}
		if !cmd.Flags().Changed("base-event-id") {
			return fmt.Errorf("--base-event-id is required")
		}
		ev, err := nip34.BuildQueueDequeueCommandAtRevision(repoAddr, queue, baseEventID, recipient, time.Now().UTC())
		if err != nil {
			return err
		}
		return publishContextVMCommand(cmd, ev, relays, signing, response, fmt.Sprintf("published dequeue for %s", queue))
	}}
	cmd.Flags().StringVar(&repoAddr, "repo-addr", "", "Canonical repository address (required)")
	cmd.Flags().StringVar(&queue, "queue", "backlog", "Queue name")
	cmd.Flags().StringVar(&baseEventID, "base-event-id", "", "Canonical queue event ID being reserved (required)")
	cmd.Flags().StringVar(&recipient, "recipient", "", "ContextVM recipient pubkey (required)")
	cmd.Flags().StringSliceVar(&relays, "relay", nil, "Relay websocket URL(s) to publish to (repeatable); falls back to NOSTR_RELAY/NOSTR_RELAYS")
	addSigningFlags(cmd, &signing)
	addResponseFlags(cmd, &response)
	return cmd
}

func newQueueListCmd() *cobra.Command {
	var repoAddr, queue, recipient string
	var relays []string
	var signing signingOptions
	var response responseOptions
	cmd := &cobra.Command{Use: "list", Short: "Publish a ContextVM queue/list command", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(recipient) == "" {
			return fmt.Errorf("--recipient is required")
		}
		if strings.TrimSpace(repoAddr) == "" {
			return fmt.Errorf("--repo-addr is required")
		}
		ev, err := nip34.BuildQueueListCommandForRepo(repoAddr, queue, recipient, time.Now().UTC())
		if err != nil {
			return err
		}
		return publishContextVMCommand(cmd, ev, relays, signing, response, fmt.Sprintf("published list for %s", queue))
	}}
	cmd.Flags().StringVar(&repoAddr, "repo-addr", "", "Canonical repository address (required)")
	cmd.Flags().StringVar(&queue, "queue", "backlog", "Queue name")
	cmd.Flags().StringVar(&recipient, "recipient", "", "ContextVM recipient pubkey (required)")
	cmd.Flags().StringSliceVar(&relays, "relay", nil, "Relay websocket URL(s) to publish to (repeatable); falls back to NOSTR_RELAY/NOSTR_RELAYS")
	addSigningFlags(cmd, &signing)
	addResponseFlags(cmd, &response)
	return cmd
}

func newServeCmd() *cobra.Command {
	var relays []string
	var ledgerRelays []string
	var mirrorRelays []string
	var repoAddrs []string
	var signing signingOptions
	var pubkey string
	var syncNIP34Status bool
	var qualityProject string
	var observabilityAddr string
	var outboxCriticalThreshold int
	var aclFile string
	var ackQuorum int
	var publishTimeout time.Duration
	var retryBaseBackoff time.Duration
	var retryMaxBackoff time.Duration
	var retryMaxAttempts int
	var circuitFailureLimit int
	var circuitCooldown time.Duration
	var outboxDrainInterval time.Duration
	var instanceLockPath string
	var commandJournalPath string
	var commandRetention time.Duration
	outboxPath := defaultOutboxPath()
	cmd := &cobra.Command{Use: "serve", Short: "Serve incoming ContextVM task and queue intents", RunE: func(cmd *cobra.Command, args []string) error {
		instanceLock, err := acquireInstanceLock(instanceLockPath)
		if err != nil {
			return err
		}
		defer instanceLock.Close()
		signer, _, err := signerFromOptions(cmd.Context(), signing, true)
		if err != nil {
			return err
		}
		path := strings.TrimSpace(aclFile)
		if path == "" {
			path = strings.TrimSpace(os.Getenv("NOSTRIG_ACL_FILE"))
		}
		if path == "" {
			return fmt.Errorf("--acl-file or NOSTRIG_ACL_FILE is required")
		}
		authz, err := taskfabric.LoadAuthorizationConfig(path)
		if err != nil {
			return err
		}
		required := cleanStrings(ledgerRelays)
		if len(required) == 0 {
			required = append(required, splitEnvList(os.Getenv("NOSTRIG_LEDGER_RELAYS"))...)
		}
		if len(required) == 0 {
			required = relaysWithEnv(relays)
		}
		mirrors := cleanStrings(append(append([]string(nil), mirrorRelays...), splitEnvList(os.Getenv("NOSTRIG_MIRROR_RELAYS"))...))
		publication := nip34.ReliablePublisherOptions{
			RequiredRelays: required, MirrorRelays: mirrors, AckQuorum: ackQuorum, OutboxPath: outboxPath,
			PublishTimeout: publishTimeout, BaseBackoff: retryBaseBackoff, MaxBackoff: retryMaxBackoff,
			MaxAttempts: retryMaxAttempts, CircuitFailureLimit: circuitFailureLimit, CircuitCooldown: circuitCooldown,
			DrainInterval: outboxDrainInterval,
		}
		journalPath := strings.TrimSpace(commandJournalPath)
		if journalPath == "" {
			journalPath = defaultCommandJournalPath(outboxPath)
		}
		return taskfabric.Serve(cmd.Context(), taskfabric.ServeOptions{
			Relays: relaysWithEnv(relays), RepoAddrs: repoAddrsWithEnv(repoAddrs), Signer: signer, PubKey: pubkey,
			SyncNIP34Status: syncNIP34Status, QualityProject: qualityProject, ObservabilityAddr: observabilityAddr,
			OutboxCriticalThreshold: outboxCriticalThreshold, Authorization: authz, Publication: publication,
			CommandJournalPath: journalPath, CommandRetention: commandRetention,
		})
	}}
	cmd.Flags().StringSliceVar(&relays, "relay", nil, "Relay websocket URL(s) to subscribe/publish to (repeatable); falls back to NOSTR_RELAY/NOSTR_RELAYS")
	cmd.Flags().StringSliceVar(&ledgerRelays, "ledger-relay", nil, "Required ledger relay(s), repeatable; defaults to --relay or NOSTRIG_LEDGER_RELAYS")
	cmd.Flags().StringSliceVar(&mirrorRelays, "mirror-relay", nil, "Optional mirror relay(s), repeatable; also reads NOSTRIG_MIRROR_RELAYS")
	cmd.Flags().StringSliceVar(&repoAddrs, "repo-addr", nil, "Allowed canonical repository address(es), repeatable; falls back to NOSTRIG_REPO_ADDR/NOSTRIG_REPO_ADDRS")
	cmd.Flags().StringVar(&pubkey, "pubkey", "", "Server recipient pubkey; defaults to signer pubkey when available")
	cmd.Flags().BoolVar(&syncNIP34Status, "sync-nip34-status", false, "Opt-in: publish NIP-34 issue status events when linked tasks change")
	cmd.Flags().StringVar(&qualityProject, "quality-project", "", "Optional PSTF project tag used to scope quality status/audit events")
	cmd.Flags().StringVar(&observabilityAddr, "observability-addr", "127.0.0.1:8080", "HTTP address for /livez, /readyz, /metrics, and redacted /diagnostics")
	cmd.Flags().IntVar(&outboxCriticalThreshold, "outbox-critical-threshold", 1000, "Readiness fails at or above this durable outbox depth")
	cmd.Flags().StringVar(&aclFile, "acl-file", "", "Caller ACL JSON file; defaults to NOSTRIG_ACL_FILE")
	cmd.Flags().IntVar(&ackQuorum, "relay-ack-quorum", 0, "Required-relay acknowledgements needed; 0 means all required relays")
	cmd.Flags().StringVar(&outboxPath, "outbox-path", outboxPath, "Durable outbound spool; defaults to NOSTRIG_OUTBOX_PATH")
	cmd.Flags().StringVar(&commandJournalPath, "command-journal-path", strings.TrimSpace(os.Getenv("NOSTRIG_COMMAND_JOURNAL_PATH")), "Durable command ledger; defaults beside the outbox")
	cmd.Flags().DurationVar(&commandRetention, "command-retention", 30*24*time.Hour, "Completed-command response retention and replay age limit")
	cmd.Flags().StringVar(&instanceLockPath, "instance-lock", strings.TrimSpace(os.Getenv("NOSTRIG_INSTANCE_LOCK")), "Exclusive local lock file preventing duplicate active instances")
	cmd.Flags().DurationVar(&publishTimeout, "relay-publish-timeout", 10*time.Second, "Per-relay publication timeout")
	cmd.Flags().DurationVar(&retryBaseBackoff, "relay-retry-base", time.Second, "Initial relay retry backoff")
	cmd.Flags().DurationVar(&retryMaxBackoff, "relay-retry-max", time.Minute, "Maximum relay retry backoff")
	cmd.Flags().IntVar(&retryMaxAttempts, "relay-retry-attempts", 10, "Attempts per relay before dead-lettering")
	cmd.Flags().IntVar(&circuitFailureLimit, "relay-circuit-failures", 3, "Consecutive failures before opening a relay circuit")
	cmd.Flags().DurationVar(&circuitCooldown, "relay-circuit-cooldown", 30*time.Second, "Relay circuit-breaker cooldown")
	cmd.Flags().DurationVar(&outboxDrainInterval, "outbox-drain-interval", time.Second, "How often to drain due outbox deliveries")
	addSigningFlags(cmd, &signing)
	return cmd
}

func publishContextVMCommand(cmd *cobra.Command, event *gonostr.Event, relays []string, signing signingOptions, response responseOptions, msg string) error {
	ctx := cmd.Context()
	signer, _, err := signerFromOptions(ctx, signing, !signing.dryRun)
	if err != nil {
		return err
	}
	if signing.dryRun {
		if signer != nil {
			if err := signEvents(ctx, signer, []*gonostr.Event{event}); err != nil {
				return err
			}
		}
		return writeEvents(cmd, []*gonostr.Event{event})
	}
	relays = relaysWithEnv(relays)
	if len(relays) == 0 {
		return fmt.Errorf("at least one --relay or NOSTR_RELAY is required")
	}
	contextSigner, err := commandContextVMSigner(signer)
	if err != nil {
		return err
	}
	recipient, _ := nip34.TagFirst(event, "p")
	var req cascontextvm.Request
	if err := json.Unmarshal([]byte(event.Content), &req); err != nil {
		return err
	}
	outer, inner, err := cascontextvm.Wrap(ctx, contextSigner, recipient, req)
	if err != nil {
		return err
	}
	accepted, err := casnostr.Publish(ctx, relays, *outer)
	if err != nil {
		return err
	}
	if accepted == 0 {
		return fmt.Errorf("no relay accepted ContextVM gift wrap")
	}
	if _, err := fmt.Fprintf(cmd.OutOrStdout(), "%s to %d relay(s)\n", msg, len(relays)); err != nil {
		return err
	}
	return maybeWaitForResponse(cmd, relays, (*gonostr.Event)(inner), response)
}

func commandContextVMSigner(s nip34.Signer) (casnostr.Signer, error) {
	keyer, ok := s.(casnostr.Signer)
	if !ok {
		return nil, fmt.Errorf("signer does not support ContextVM NIP-59 encryption")
	}
	return keyer, nil
}
func addFetchFlags(cmd *cobra.Command, repoID *string, owner *string, relays *[]string, idFormat *string, idPrefix *string) {
	cmd.Flags().StringVar(repoID, "repo-id", "", "Repository id (d tag) to fetch (required)")
	cmd.Flags().StringVar(owner, "owner", "", "Repository owner pubkey (hex or npub). Recommended to disambiguate.")
	cmd.Flags().StringSliceVar(relays, "relay", nil, "Relay websocket URL(s) to use (repeatable)")
	cmd.Flags().StringVar(idFormat, "id-format", "spec", "ID format for beads ids: legacy|spec")
	cmd.Flags().StringVar(idPrefix, "id-prefix", "", "Identifier prefix override for spec format (optional)")
}

type signingOptions struct {
	privateKey      string
	bunkerURL       string
	clientSecretKey string
	dryRun          bool
}

type responseOptions struct {
	wait    bool
	timeout time.Duration
}

func addSigningFlags(cmd *cobra.Command, opts *signingOptions) {
	cmd.Flags().StringVar(&opts.bunkerURL, "signer-bunker-url", "", "Signet/NIP-46 bunker URL; defaults to NOSTRIG_SIGNER_BUNKER_URL")
	cmd.Flags().StringVar(&opts.clientSecretKey, "signer-client-secret-key", "", "NIP-46 client secret key (hex); defaults to NOSTRIG_SIGNER_CLIENT_SECRET_KEY and may be ephemeral if omitted")
	cmd.Flags().StringVar(&opts.privateKey, "private-key", "", "Local-dev Nostr private key (hex); defaults to NOSTR_PRIVATE_KEY and is forbidden when NOSTRIG_ENV=production")
	cmd.Flags().BoolVar(&opts.dryRun, "dry-run", false, "Print generated events as JSONL instead of publishing")
}

func addResponseFlags(cmd *cobra.Command, opts *responseOptions) {
	cmd.Flags().BoolVar(&opts.wait, "wait-response", false, "Wait for a correlated ContextVM JSON-RPC response after publishing")
	cmd.Flags().DurationVar(&opts.timeout, "response-timeout", 30*time.Second, "Maximum time to wait for --wait-response")
}

func parseFetchInputs(repoID, owner, idFormat string) (converter.IDFormat, string, error) {
	if strings.TrimSpace(repoID) == "" {
		return "", "", fmt.Errorf("--repo-id is required")
	}
	format, err := converter.ParseIDFormat(idFormat)
	if err != nil {
		return "", "", err
	}
	ownerHex, err := resolveOwner(owner)
	if err != nil {
		return "", "", fmt.Errorf("invalid owner: %w", err)
	}
	return format, ownerHex, nil
}

// resolveOwner converts an npub to hex, or returns hex as-is.
func resolveOwner(owner string) (string, error) {
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return "", nil
	}

	if strings.HasPrefix(owner, "npub1") {
		prefix, value, err := nip19.Decode(owner)
		if err != nil {
			return "", fmt.Errorf("failed to decode npub: %w", err)
		}
		if prefix != "npub" {
			return "", fmt.Errorf("expected npub, got %s", prefix)
		}
		hex, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("decoded value is not a string")
		}
		return hex, nil
	}

	return owner, nil
}

func resolvePrivateKey(privateKey string) (string, error) {
	key := strings.TrimSpace(privateKey)
	if key == "" {
		key = strings.TrimSpace(os.Getenv("NOSTR_PRIVATE_KEY"))
	}
	return normalizeSecretKey(key)
}

func resolveClientSecretKey(clientSecretKey string) (string, error) {
	key := strings.TrimSpace(clientSecretKey)
	if key == "" {
		key = strings.TrimSpace(os.Getenv("NOSTRIG_SIGNER_CLIENT_SECRET_KEY"))
	}
	if key == "" {
		key = strings.TrimSpace(os.Getenv("NOSTRIG_SIGNER_SECRET_KEY"))
	}
	if key == "" {
		path := strings.TrimSpace(os.Getenv("NOSTRIG_SIGNER_CLIENT_SECRET_KEY_FILE"))
		if path != "" {
			contents, err := os.ReadFile(path)
			if err != nil {
				return "", fmt.Errorf("read NOSTRIG_SIGNER_CLIENT_SECRET_KEY_FILE: %w", err)
			}
			key = strings.TrimSpace(string(contents))
		}
	}
	return normalizeSecretKey(key)
}

func normalizeSecretKey(key string) (string, error) {
	return strings.TrimSpace(key), nil
}

func signerFromOptions(ctx context.Context, opts signingOptions, required bool) (nip34.Signer, bool, error) {
	bunkerURL := strings.TrimSpace(opts.bunkerURL)
	if bunkerURL == "" {
		bunkerURL = strings.TrimSpace(os.Getenv("NOSTRIG_SIGNER_BUNKER_URL"))
	}
	key, err := resolvePrivateKey(opts.privateKey)
	if err != nil {
		return nil, false, err
	}
	production := isProductionEnv()
	if bunkerURL != "" && key != "" {
		return nil, false, fmt.Errorf("configure either Signet/NIP-46 (--signer-bunker-url) or local-dev --private-key, not both")
	}
	if key != "" {
		if production {
			return nil, false, fmt.Errorf("raw --private-key/NOSTR_PRIVATE_KEY is forbidden when NOSTRIG_ENV=production; use --signer-bunker-url")
		}
		signer, err := nip34.NewPrivateKeySigner(key)
		if err != nil {
			return nil, false, err
		}
		return signer, true, ctx.Err()
	}
	if bunkerURL != "" {
		clientSecretKey, err := resolveClientSecretKey(opts.clientSecretKey)
		if err != nil {
			return nil, false, err
		}
		signer, err := nip34.ConnectNIP46Signer(ctx, bunkerURL, clientSecretKey)
		if err != nil {
			return nil, false, err
		}
		return signer, true, ctx.Err()
	}
	if required {
		if production {
			return nil, false, fmt.Errorf("production signing requires --signer-bunker-url or NOSTRIG_SIGNER_BUNKER_URL")
		}
		return nil, false, fmt.Errorf("signing requires --signer-bunker-url/NOSTRIG_SIGNER_BUNKER_URL or local-dev --private-key/NOSTR_PRIVATE_KEY")
	}
	return nil, false, nil
}

func isProductionEnv() bool {
	env := strings.ToLower(strings.TrimSpace(os.Getenv("NOSTRIG_ENV")))
	return env == "production" || env == "prod"
}

func publicKeyFromSigner(ctx context.Context, signer nip34.Signer) (string, error) {
	provider, ok := signer.(nip34.PublicKeyProvider)
	if !ok {
		return "", fmt.Errorf("signer cannot provide a public key; set --claimer explicitly")
	}
	pub, err := provider.PublicKey(ctx)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(pub) == "" {
		return "", fmt.Errorf("signer returned empty public key")
	}
	return pub, nil
}

func signEvents(ctx context.Context, signer nip34.Signer, events []*gonostr.Event) error {
	for _, ev := range events {
		if ev == nil {
			continue
		}
		if err := signer.SignEvent(ctx, ev); err != nil {
			return err
		}
	}
	return nil
}

func writeEvents(cmd *cobra.Command, events []*gonostr.Event) error {
	enc := json.NewEncoder(cmd.OutOrStdout())
	for _, ev := range events {
		if ev == nil {
			continue
		}
		if err := enc.Encode(ev); err != nil {
			return err
		}
	}
	return nil
}

func maybeWaitForResponse(cmd *cobra.Command, relays []string, event *gonostr.Event, opts responseOptions) error {
	if !opts.wait {
		return nil
	}
	resp, err := taskfabric.WaitForContextVMResponse(cmd.Context(), relays, event, opts.timeout)
	if err != nil {
		return err
	}
	return json.NewEncoder(cmd.OutOrStdout()).Encode(resp)
}

func relaysWithEnv(relays []string) []string {
	out := make([]string, 0, len(relays)+2)
	out = append(out, relays...)
	if len(cleanStrings(out)) == 0 {
		out = append(out, splitEnvList(os.Getenv("NOSTR_RELAYS"))...)
		out = append(out, splitEnvList(os.Getenv("NOSTR_RELAY"))...)
	}
	return cleanStrings(out)
}

func repoAddrsWithEnv(repoAddrs []string) []string {
	out := append([]string(nil), repoAddrs...)
	if len(cleanStrings(out)) == 0 {
		out = append(out, splitEnvList(os.Getenv("NOSTRIG_REPO_ADDRS"))...)
		out = append(out, splitEnvList(os.Getenv("NOSTRIG_REPO_ADDR"))...)
	}
	return cleanStrings(out)
}

func splitEnvList(v string) []string {
	return strings.FieldsFunc(v, func(r rune) bool { return r == ',' || r == ' ' || r == '\n' || r == '\t' })
}

func cleanStrings(in []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
