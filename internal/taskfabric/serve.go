package taskfabric

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	gonostr "fiatjaf.com/nostr"
	cascontextvm "git.sharegap.net/cascadia/cascadia-go/contextvm"
	beadspb "github.com/chebizarro/nostrig/gen/beads"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
	"github.com/chebizarro/nostrig/internal/taskmodel"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Handler struct {
	Ledger      Ledger
	Quality     QualityLookup
	RepoAddrs   []string
	Recipient   string
	ACL         map[string]CallerPolicy
	ClosePolicy ClosePolicy
	Audit       AuthzAuditSink
}

func (h *Handler) HandleIntent(ctx context.Context, ev *gonostr.Event, now time.Time) (*gonostr.Event, error) {
	if h == nil || h.Ledger == nil {
		return nil, fmt.Errorf("handler ledger is nil")
	}
	if ev == nil || ev.Kind != nip34.KindContextVMIntent {
		return nil, fmt.Errorf("expected ContextVM intent")
	}
	var req cascontextvm.Request
	if err := json.Unmarshal([]byte(ev.Content), &req); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.Method) == "" {
		req.Method, _ = nip34.TagFirst(ev, "method")
	}
	if recipient := strings.TrimSpace(h.Recipient); recipient != "" {
		recipients := nip34.TagAll(ev, "p")
		if len(recipients) != 1 || recipients[0] != recipient {
			return nil, fmt.Errorf("intent recipient does not match this service")
		}
	}
	var resp cascontextvm.Response
	switch {
	case req.JSONRPC != cascontextvm.JSONRPCVersion || strings.TrimSpace(req.Method) == "":
		resp = cascontextvm.NewErrorResponse(req.ID, cascontextvm.InvalidRequestCode, "invalid request")
	case !supportedMethod(req.Method):
		resp = cascontextvm.NewErrorResponse(req.ID, cascontextvm.MethodNotFoundCode, "method not found")
	default:
		result, dispatchErr := h.dispatch(ctx, ev, req.Method, req.Params, ev.PubKey.Hex(), now)
		if dispatchErr == nil {
			resp = cascontextvm.NewResponse(req.ID, result)
		} else {
			var conflict *ConflictError
			if errors.As(dispatchErr, &conflict) {
				resp = cascontextvm.Response{JSONRPC: cascontextvm.JSONRPCVersion, ID: req.IDOrNull(), Error: &cascontextvm.Error{Code: conflictErrorCode, Message: conflict.Error(), Data: conflict.responseData()}}
			} else {
				resp = cascontextvm.NewErrorResponse(req.ID, cascontextvm.InternalErrorCode, dispatchErr.Error())
			}
		}
	}
	return newContextVMResponseEvent(ev, req, resp, now)
}

func newContextVMResponseEvent(ev *gonostr.Event, req cascontextvm.Request, resp cascontextvm.Response, now time.Time) (*gonostr.Event, error) {
	content, err := json.Marshal(resp)
	if err != nil {
		return nil, err
	}
	tags := gonostr.Tags{{"e", ev.ID.Hex()}, {"p", ev.PubKey.Hex()}, {"method", req.Method}, {"schema", nip34.TaskIntentSchema}}
	if id := rawIDString(req.ID); id != "" {
		tags = append(tags, gonostr.Tag{"correlation", id}, gonostr.Tag{"request", id})
	}
	return &gonostr.Event{Kind: gonostr.Kind(nip34.KindContextVMIntent), CreatedAt: gonostr.Timestamp(now.Unix()), Tags: tags, Content: string(content)}, nil
}

func (h *Handler) registry(ev *gonostr.Event, now time.Time) *cascontextvm.Registry {
	caller := ev.PubKey.Hex()
	r := cascontextvm.NewRegistry()
	for _, method := range []string{"task/create", "task/claim", "task/assign", "task/update", "task/close", "task/delete", "task/quality-status", "queue/enqueue", "queue/dequeue", "queue/list"} {
		parts := strings.Split(method, "/")
		r.Register(parts[0], parts[1], func(method string) cascontextvm.Handler {
			return func(ctx context.Context, req cascontextvm.Request) (any, error) {
				return h.dispatch(ctx, ev, method, req.Params, caller, now)
			}
		}(method))
	}
	return r
}

func (h *Handler) dispatch(ctx context.Context, ev *gonostr.Event, method string, raw json.RawMessage, caller string, now time.Time) (any, error) {
	var p map[string]any
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, err
		}
	}
	get := func(k string) string {
		if v, ok := p[k]; ok {
			return strings.TrimSpace(fmt.Sprint(v))
		}
		return ""
	}
	if err := h.authorize(ctx, ev, method, p, caller, now); err != nil {
		return nil, err
	}
	if strings.HasPrefix(method, "task/") && method != "task/quality-status" {
		if err := h.authorizeRepo(ctx, method, get("task_id"), get("repo_addr")); err != nil {
			return nil, err
		}
	}
	switch method {
	case "task/create":
		id, title := strings.TrimPrefix(get("task_id"), "task:"), get("title")
		if id == "" {
			return nil, fmt.Errorf("task_id is required")
		}
		if title == "" {
			return nil, fmt.Errorf("title is required")
		}
		status := nip34.ParseStatus(get("status"))
		if status == beadspb.Status_STATUS_UNSPECIFIED {
			status = beadspb.Status_STATUS_OPEN
		}
		metadata := &beadspb.Metadata{Custom: map[string]string{}}
		if repoAddr := get("repo_addr"); repoAddr != "" {
			metadata.Custom["nip34.repo_addr"] = repoAddr
		}
		issue := &beadspb.Issue{
			Id:          id,
			Title:       title,
			Description: get("description"),
			Status:      status,
			Priority:    parsePriorityParam(get("priority")),
			Epic:        get("epic"),
			Assignee:    get("assignee"),
			Labels:      cleanStrings(paramList(p, "labels")),
			DependsOn:   cleanTaskIDs(paramList(p, "depends_on")),
			Created:     timestamppb.New(now),
			Updated:     timestamppb.New(now),
			Metadata:    metadata,
		}
		record, err := h.Ledger.MutateTask(ctx, id, func(current *TaskRecord) (TaskMutationResult, error) {
			if current != nil {
				return TaskMutationResult{}, &ConflictError{Resource: "task", Reason: "already_exists", ActualEventID: current.EventID, Assignee: current.Issue.Assignee, Status: statusString(current.Issue)}
			}
			return TaskMutationResult{Issue: issue}, nil
		})
		if err != nil {
			return nil, err
		}
		r := taskResult(record.Issue, record.EventID)
		h.annotateQuality(ctx, r, []string{record.Issue.Id})
		return r, nil
	case "task/claim":
		id, claimer := strings.TrimPrefix(get("task_id"), "task:"), get("claimer")
		if policy, ok := h.ACL[strings.ToLower(strings.TrimSpace(caller))]; ok && containsRole(policy.Roles, RoleWorker) {
			claimer = strings.TrimSpace(policy.WorkerID)
		}
		if claimer == "" {
			claimer = caller
		}
		base, err := requireBaseParam(p)
		if err != nil {
			return nil, err
		}
		record, err := h.Ledger.MutateTask(ctx, id, func(current *TaskRecord) (TaskMutationResult, error) {
			if current == nil || current.Issue == nil {
				return TaskMutationResult{}, fmt.Errorf("task %s not found", id)
			}
			if current.Issue.Assignee == claimer && current.Issue.Status == beadspb.Status_STATUS_IN_PROGRESS {
				return TaskMutationResult{Unchanged: true}, nil
			}
			if current.Issue.Assignee != "" {
				return TaskMutationResult{}, &ConflictError{Resource: "task", Reason: "already_claimed", ExpectedEventID: base, ActualEventID: current.EventID, Assignee: current.Issue.Assignee, Status: statusString(current.Issue)}
			}
			if current.Issue.Status != beadspb.Status_STATUS_OPEN {
				return TaskMutationResult{}, &ConflictError{Resource: "task", Reason: "not_claimable", ExpectedEventID: base, ActualEventID: current.EventID, Status: statusString(current.Issue)}
			}
			if err := checkTaskBase(current, base); err != nil {
				return TaskMutationResult{}, err
			}
			issue := cloneIssue(current.Issue)
			issue.Assignee, issue.Status, issue.Updated, issue.ClaimedAt = claimer, beadspb.Status_STATUS_IN_PROGRESS, timestamppb.New(now), timestamppb.New(now)
			if issue.StartedAt == nil {
				issue.StartedAt = timestamppb.New(now)
			}
			return TaskMutationResult{Issue: issue}, nil
		})
		if err != nil {
			return nil, err
		}
		r := taskResult(record.Issue, record.EventID)
		h.annotateQuality(ctx, r, []string{record.Issue.Id})
		return r, nil
	case "task/assign":
		id, assignee := strings.TrimPrefix(get("task_id"), "task:"), get("assignee")
		if assignee == "" {
			return nil, fmt.Errorf("assignee is required")
		}
		base, err := requireBaseParam(p)
		if err != nil {
			return nil, err
		}
		record, err := h.Ledger.MutateTask(ctx, id, func(current *TaskRecord) (TaskMutationResult, error) {
			if current == nil || current.Issue == nil {
				return TaskMutationResult{}, fmt.Errorf("task %s not found", id)
			}
			if err := checkTaskBase(current, base); err != nil {
				return TaskMutationResult{}, err
			}
			issue := cloneIssue(current.Issue)
			issue.Assignee, issue.Updated = assignee, timestamppb.New(now)
			return TaskMutationResult{Issue: issue}, nil
		})
		if err != nil {
			return nil, err
		}
		r := taskResult(record.Issue, record.EventID)
		h.annotateQuality(ctx, r, []string{record.Issue.Id})
		return r, nil
	case "task/update":
		id := strings.TrimPrefix(get("task_id"), "task:")
		base, err := requireBaseParam(p)
		if err != nil {
			return nil, err
		}
		record, err := h.Ledger.MutateTask(ctx, id, func(current *TaskRecord) (TaskMutationResult, error) {
			if current == nil || current.Issue == nil {
				return TaskMutationResult{}, fmt.Errorf("task %s not found", id)
			}
			if err := checkTaskBase(current, base); err != nil {
				return TaskMutationResult{}, err
			}
			issue := cloneIssue(current.Issue)
			if v := get("status"); v != "" {
				issue.Status = nip34.ParseStatus(v)
				if issue.Status == beadspb.Status_STATUS_BLOCKED {
					issue.BlockedAt = timestamppb.New(now)
				}
			}
			if _, ok := p["assignee"]; ok {
				issue.Assignee = get("assignee")
			}
			if _, ok := p["title"]; ok {
				issue.Title = get("title")
			}
			if _, ok := p["description"]; ok {
				issue.Description = get("description")
			}
			if _, ok := p["priority"]; ok {
				issue.Priority = parsePriorityParam(get("priority"))
			}
			if _, ok := p["epic"]; ok {
				issue.Epic = get("epic")
			}
			if _, ok := p["set_labels"]; ok {
				issue.Labels = cleanStrings(paramList(p, "set_labels"))
			}
			issue.Labels = addStrings(issue.Labels, paramList(p, "add_labels"))
			issue.Labels = removeStrings(issue.Labels, paramList(p, "remove_labels"))
			issue.DependsOn = addStrings(issue.DependsOn, cleanTaskIDs(paramList(p, "add_dependencies")))
			issue.DependsOn = removeStrings(issue.DependsOn, cleanTaskIDs(paramList(p, "remove_dependencies")))
			if _, ok := p["status_reason"]; ok {
				issue.StatusReason = get("status_reason")
			}
			if _, ok := p["blocker_description"]; ok {
				if issue.Status != beadspb.Status_STATUS_BLOCKED {
					return TaskMutationResult{}, fmt.Errorf("blocker_description requires blocked status")
				}
				issue.BlockerDescription = get("blocker_description")
			}
			if _, ok := p["notes"]; ok {
				issue.Notes = get("notes")
			}
			evidence := artifactReferences(paramList(p, "evidence_ids"), "evidence")
			issue.Evidence = appendUniqueArtifacts(issue.Evidence, evidence...)
			if summary := get("checkpoint_summary"); summary != "" {
				checkpointID := get("checkpoint_id")
				if checkpointID == "" {
					checkpointID = "checkpoint:" + ev.ID.Hex()
				}
				checkpointEvidence := artifactReferences(paramList(p, "checkpoint_evidence_ids"), "evidence")
				issue.Checkpoints = append(issue.Checkpoints, &beadspb.Checkpoint{
					Id: checkpointID, Actor: caller, Status: get("checkpoint_status"), Summary: summary,
					CreatedAt: timestamppb.New(now), Evidence: checkpointEvidence,
				})
			}
			if strings.EqualFold(get("request_validation"), "true") {
				if issue.Review == nil {
					issue.Review = &beadspb.Review{}
				}
				issue.Review.Required = true
				issue.Review.State = "requested"
				issue.Review.Reviewer = get("reviewer")
				issue.Review.Requirements = cleanStrings(paramList(p, "review_requirements"))
			}
			issue.Updated = timestamppb.New(now)
			return TaskMutationResult{Issue: issue}, nil
		})
		if err != nil {
			return nil, err
		}
		r := taskResult(record.Issue, record.EventID)
		h.annotateQuality(ctx, r, []string{record.Issue.Id})
		return r, nil
	case "task/close":
		id := strings.TrimPrefix(get("task_id"), "task:")
		base, err := requireBaseParam(p)
		if err != nil {
			return nil, err
		}
		record, err := h.Ledger.MutateTask(ctx, id, func(current *TaskRecord) (TaskMutationResult, error) {
			if current == nil || current.Issue == nil {
				return TaskMutationResult{}, fmt.Errorf("task %s not found", id)
			}
			if err := checkTaskBase(current, base); err != nil {
				return TaskMutationResult{}, err
			}
			issue := cloneIssue(current.Issue)
			issue.Status, issue.Updated, issue.ClosedAt = beadspb.Status_STATUS_CLOSED, timestamppb.New(now), timestamppb.New(now)
			if _, ok := p["close_reason"]; ok {
				issue.CloseReason = get("close_reason")
			}
			issue.Evidence = appendUniqueArtifacts(issue.Evidence, artifactReferences(paramList(p, "acceptance_evidence_ids"), "acceptance")...)
			return TaskMutationResult{Issue: issue}, nil
		})
		if err != nil {
			return nil, err
		}
		r := taskResult(record.Issue, record.EventID)
		h.annotateQuality(ctx, r, []string{record.Issue.Id})
		return r, nil
	case "task/delete":
		id := strings.TrimPrefix(get("task_id"), "task:")
		if id == "" {
			return nil, fmt.Errorf("task_id is required")
		}
		base, err := requireBaseParam(p)
		if err != nil {
			return nil, err
		}
		record, err := h.Ledger.MutateTask(ctx, id, func(current *TaskRecord) (TaskMutationResult, error) {
			if err := checkTaskBase(current, base); err != nil {
				return TaskMutationResult{}, err
			}
			if current == nil {
				return TaskMutationResult{Unchanged: true}, nil
			}
			return TaskMutationResult{Delete: true}, nil
		})
		if err != nil {
			return nil, err
		}
		r := map[string]any{"task_id": id, "deleted": true}
		if record != nil && record.EventID != "" {
			r["event_id"] = record.EventID
		}
		return r, nil
	case "task/quality-status":
		ids := paramList(p, "task_ids")
		if id := get("task_id"); id != "" {
			ids = append(ids, id)
		}
		if q := get("queue"); q != "" && len(ids) == 0 {
			queueRecord, err := h.Ledger.GetQueue(ctx, get("repo_addr"), queueName(q))
			if err != nil {
				return nil, err
			}
			if queueRecord != nil {
				ids = append(ids, availableQueueIDs(queueRecord, now)...)
			}
		}
		ids = cleanStrings(ids)
		if len(ids) == 0 {
			return nil, fmt.Errorf("task_id, task_ids, or queue is required")
		}
		quality := pendingQuality(ids)
		if h.Quality != nil {
			var err error
			quality, err = h.Quality.GetQuality(ctx, ids)
			if err != nil {
				return nil, err
			}
		}
		return map[string]any{"quality": quality}, nil
	case "queue/enqueue":
		repo, q, id := get("repo_addr"), queueName(get("queue")), get("task_id")
		if id == "" {
			return nil, fmt.Errorf("task_id is required")
		}
		base, err := requireBaseParam(p)
		if err != nil {
			return nil, err
		}
		record, err := h.Ledger.MutateQueue(ctx, repo, q, func(current *QueueRecord) (QueueMutationResult, error) {
			if err := checkQueueBase(current, base); err != nil {
				return QueueMutationResult{}, err
			}
			if current == nil {
				current = &QueueRecord{}
			}
			if contains(current.TaskIDs, id) {
				return QueueMutationResult{Unchanged: true}, nil
			}
			current.TaskIDs = append(current.TaskIDs, id)
			return QueueMutationResult{Queue: current}, nil
		})
		if err != nil {
			return nil, err
		}
		r := queueResult(q, record, now)
		r["repo_addr"] = repo
		return r, nil
	case "queue/dequeue":
		repo, q := get("repo_addr"), queueName(get("queue"))
		base, err := requireBaseParam(p)
		if err != nil {
			return nil, err
		}
		worker := caller
		if policy, ok := h.ACL[strings.ToLower(strings.TrimSpace(caller))]; ok && strings.TrimSpace(policy.WorkerID) != "" {
			worker = strings.TrimSpace(policy.WorkerID)
		}
		leaseSeconds := 300
		if value := get("lease_seconds"); value != "" {
			parsed, parseErr := strconv.Atoi(value)
			if parseErr != nil || parsed < 1 || parsed > 3600 {
				return nil, fmt.Errorf("lease_seconds must be between 1 and 3600")
			}
			leaseSeconds = parsed
		}
		var reservation QueueLease
		record, err := h.Ledger.MutateQueue(ctx, repo, q, func(current *QueueRecord) (QueueMutationResult, error) {
			for _, lease := range activeQueueLeases(current, now) {
				if lease.Worker == worker {
					reservation = lease
					return QueueMutationResult{Unchanged: true}, nil
				}
			}
			if err := checkQueueBase(current, base); err != nil {
				return QueueMutationResult{}, err
			}
			if current == nil {
				current = &QueueRecord{}
			}
			available := availableQueueIDs(current, now)
			if len(available) == 0 {
				return QueueMutationResult{Unchanged: true}, nil
			}
			reservation = QueueLease{TaskID: available[0], Worker: worker, LeaseID: ev.ID.Hex(), ExpiresAt: now.Add(time.Duration(leaseSeconds) * time.Second)}
			current.Leases = append(activeQueueLeases(current, now), reservation)
			return QueueMutationResult{Queue: current}, nil
		})
		if err != nil {
			return nil, err
		}
		r := queueResult(q, record, now)
		r["repo_addr"] = repo
		r["task_id"] = reservation.TaskID
		if reservation.TaskID != "" {
			r["lease_id"] = reservation.LeaseID
			r["lease_expires_at"] = reservation.ExpiresAt.UTC().Format(time.RFC3339Nano)
		}
		return r, nil
	case "queue/list":
		repo, q := get("repo_addr"), queueName(get("queue"))
		record, err := h.Ledger.GetQueue(ctx, repo, q)
		if err != nil {
			return nil, err
		}
		r := queueResult(q, record, now)
		r["repo_addr"] = repo
		h.annotateQueueQuality(ctx, r, availableQueueIDs(record, now))
		return r, nil
	default:
		return nil, fmt.Errorf("unsupported method %q", method)
	}
}

func (h *Handler) task(ctx context.Context, id string) (*beadspb.Issue, error) {
	id = strings.TrimPrefix(strings.TrimSpace(id), "task:")
	if id == "" {
		return nil, fmt.Errorf("task_id is required")
	}
	issue, err := h.Ledger.GetTask(ctx, id)
	if err != nil {
		return nil, err
	}
	if issue == nil {
		return nil, fmt.Errorf("task %s not found", id)
	}
	return issue, nil
}

func taskResult(i *beadspb.Issue, eventID string) map[string]any {
	r := map[string]any{"task_id": i.Id, "status": nip34.StatusString(i.Status), "assignee": i.Assignee, "evidence_ids": artifactEvidenceIDs(i)}
	if task, err := taskmodel.FromProto(i); err == nil {
		r["task"] = task
	}
	if eventID != "" {
		r["event_id"] = eventID
		r["revision"] = eventID
	}
	return r
}

func (h *Handler) annotateQuality(ctx context.Context, r map[string]any, ids []string) {
	if h == nil || h.Quality == nil || len(ids) == 0 {
		return
	}
	quality, err := h.Quality.GetQuality(ctx, ids)
	if err != nil {
		r["quality_error"] = err.Error()
		return
	}
	if len(ids) == 1 {
		if q, ok := quality[ids[0]]; ok {
			r["quality"] = q
			return
		}
	}
	r["quality"] = quality
}

func (h *Handler) annotateQueueQuality(ctx context.Context, r map[string]any, ids []string) {
	if h == nil || h.Quality == nil || len(ids) == 0 {
		return
	}
	quality, err := h.Quality.GetQuality(ctx, ids)
	if err != nil {
		r["quality_error"] = err.Error()
		return
	}
	r["quality"] = quality
}

func queueResult(q string, record *QueueRecord, now time.Time) map[string]any {
	r := map[string]any{"queue": q, "task_ids": availableQueueIDs(record, now)}
	if record != nil {
		if record.EventID != "" {
			r["event_id"] = record.EventID
			r["revision"] = record.EventID
		}
		r["leases"] = activeQueueLeases(record, now)
	}
	return r
}

func paramList(p map[string]any, key string) []string {
	v, ok := p[key]
	if !ok || v == nil {
		return nil
	}
	switch x := v.(type) {
	case []any:
		out := make([]string, 0, len(x))
		for _, item := range x {
			out = append(out, strings.TrimSpace(fmt.Sprint(item)))
		}
		return out
	case []string:
		return append([]string(nil), x...)
	default:
		return []string{strings.TrimSpace(fmt.Sprint(x))}
	}
}

func artifactReferences(ids []string, kind string) []*beadspb.ArtifactReference {
	ids = cleanStrings(ids)
	out := make([]*beadspb.ArtifactReference, 0, len(ids))
	for _, id := range ids {
		out = append(out, &beadspb.ArtifactReference{Kind: kind, Reference: id})
	}
	return out
}

func appendUniqueArtifacts(base []*beadspb.ArtifactReference, values ...*beadspb.ArtifactReference) []*beadspb.ArtifactReference {
	seen := map[string]struct{}{}
	out := make([]*beadspb.ArtifactReference, 0, len(base)+len(values))
	for _, ref := range append(append([]*beadspb.ArtifactReference(nil), base...), values...) {
		if ref == nil {
			continue
		}
		key := strings.Join([]string{ref.Kind, ref.Reference, ref.Sha256, ref.Url}, "|")
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, ref)
	}
	return out
}

func artifactEvidenceIDs(issue *beadspb.Issue) []string {
	if issue == nil {
		return []string{}
	}
	ids := make([]string, 0, len(issue.Evidence))
	for _, ref := range issue.Evidence {
		if ref == nil {
			continue
		}
		for _, value := range []string{ref.Reference, ref.Sha256, ref.Url} {
			if value = strings.TrimSpace(value); value != "" {
				ids = append(ids, value)
				break
			}
		}
	}
	return cleanStrings(ids)
}

func (h *Handler) authorizeRepo(ctx context.Context, method, taskID, repoAddr string) error {
	allowed := cleanStrings(h.RepoAddrs)
	if len(allowed) == 0 {
		return nil
	}
	isAllowed := func(candidate string) bool {
		candidate = strings.TrimSpace(candidate)
		for _, addr := range allowed {
			if candidate == addr {
				return true
			}
		}
		return false
	}
	if method == "task/create" {
		if !isAllowed(repoAddr) {
			return fmt.Errorf("repo_addr is not served by this instance")
		}
		return nil
	}
	issue, err := h.Ledger.GetTask(ctx, strings.TrimPrefix(strings.TrimSpace(taskID), "task:"))
	if err != nil {
		return err
	}
	if issue == nil || !isAllowed(issue.GetMetadata().GetCustom()["nip34.repo_addr"]) {
		return fmt.Errorf("task is not served by this instance")
	}
	return nil
}

func parsePriorityParam(value string) beadspb.Priority {
	value = strings.ToUpper(strings.TrimSpace(value))
	if len(value) == 1 && value[0] >= '0' && value[0] <= '4' {
		value = "P" + value
	}
	return nip34.ParsePriority(value)
}

func addStrings(base, values []string) []string {
	return cleanStrings(append(append([]string(nil), base...), values...))
}

func removeStrings(base, values []string) []string {
	remove := map[string]struct{}{}
	for _, value := range values {
		remove[strings.TrimSpace(value)] = struct{}{}
	}
	out := make([]string, 0, len(base))
	for _, value := range cleanStrings(base) {
		if _, ok := remove[value]; !ok {
			out = append(out, value)
		}
	}
	return out
}

func cleanTaskIDs(ids []string) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if id = strings.TrimPrefix(strings.TrimSpace(id), "task:"); id != "" {
			out = append(out, id)
		}
	}
	return cleanStrings(out)
}

func queueName(q string) string {
	if strings.TrimSpace(q) == "" {
		return "backlog"
	}
	return strings.TrimSpace(q)
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func containsRole(roles []Role, role Role) bool {
	for _, candidate := range roles {
		if candidate == role {
			return true
		}
	}
	return false
}
