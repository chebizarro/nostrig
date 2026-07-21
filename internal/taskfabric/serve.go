package taskfabric

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	gonostr "fiatjaf.com/nostr"
	cascontextvm "git.sharegap.net/cascadia/cascadia-go/contextvm"
	beadspb "github.com/chebizarro/nostrig/gen/beads"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Ledger interface {
	GetTask(ctx context.Context, id string) (*beadspb.Issue, error)
	PutTask(ctx context.Context, issue *beadspb.Issue) (*gonostr.Event, error)
	DeleteTask(ctx context.Context, id string) (*gonostr.Event, error)
	GetQueue(ctx context.Context, repoAddr, queue string) ([]string, error)
	PutQueue(ctx context.Context, repoAddr, queue string, ids []string) (*gonostr.Event, error)
}

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
	resp := h.registry(ev, now).Dispatch(ctx, req)
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
		ev, err := h.Ledger.PutTask(ctx, issue)
		if err != nil {
			return nil, err
		}
		r := taskResult(issue, ev)
		h.annotateQuality(ctx, r, []string{issue.Id})
		return r, nil
	case "task/claim":
		id, claimer := get("task_id"), get("claimer")
		if policy, ok := h.ACL[strings.ToLower(strings.TrimSpace(caller))]; ok && containsRole(policy.Roles, RoleWorker) {
			claimer = strings.TrimSpace(policy.WorkerID)
		}
		if claimer == "" {
			claimer = caller
		}
		issue, err := h.task(ctx, id)
		if err != nil {
			return nil, err
		}
		issue.Assignee, issue.Status, issue.Updated = claimer, beadspb.Status_STATUS_IN_PROGRESS, timestamppb.New(now)
		ev, err := h.Ledger.PutTask(ctx, issue)
		if err != nil {
			return nil, err
		}
		r := taskResult(issue, ev)
		h.annotateQuality(ctx, r, []string{issue.Id})
		return r, nil
	case "task/assign":
		id, assignee := get("task_id"), get("assignee")
		if assignee == "" {
			return nil, fmt.Errorf("assignee is required")
		}
		issue, err := h.task(ctx, id)
		if err != nil {
			return nil, err
		}
		issue.Assignee, issue.Updated = assignee, timestamppb.New(now)
		ev, err := h.Ledger.PutTask(ctx, issue)
		if err != nil {
			return nil, err
		}
		r := taskResult(issue, ev)
		h.annotateQuality(ctx, r, []string{issue.Id})
		return r, nil
	case "task/update":
		issue, err := h.task(ctx, get("task_id"))
		if err != nil {
			return nil, err
		}
		if v := get("status"); v != "" {
			issue.Status = nip34.ParseStatus(v)
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
		issue.Updated = timestamppb.New(now)
		ev, err := h.Ledger.PutTask(ctx, issue)
		if err != nil {
			return nil, err
		}
		r := taskResult(issue, ev)
		h.annotateQuality(ctx, r, []string{issue.Id})
		return r, nil
	case "task/close":
		issue, err := h.task(ctx, get("task_id"))
		if err != nil {
			return nil, err
		}
		issue.Status, issue.Updated = beadspb.Status_STATUS_CLOSED, timestamppb.New(now)
		ev, err := h.Ledger.PutTask(ctx, issue)
		if err != nil {
			return nil, err
		}
		r := taskResult(issue, ev)
		h.annotateQuality(ctx, r, []string{issue.Id})
		return r, nil
	case "task/delete":
		id := strings.TrimPrefix(get("task_id"), "task:")
		if id == "" {
			return nil, fmt.Errorf("task_id is required")
		}
		ev, err := h.Ledger.DeleteTask(ctx, id)
		if err != nil {
			return nil, err
		}
		r := map[string]any{"task_id": id, "deleted": true}
		if ev != nil {
			r["event_id"] = ev.ID
		}
		return r, nil
	case "task/quality-status":
		ids := paramList(p, "task_ids")
		if id := get("task_id"); id != "" {
			ids = append(ids, id)
		}
		if q := get("queue"); q != "" && len(ids) == 0 {
			queueIDs, err := h.Ledger.GetQueue(ctx, get("repo_addr"), queueName(q))
			if err != nil {
				return nil, err
			}
			ids = append(ids, queueIDs...)
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
		ids, err := h.Ledger.GetQueue(ctx, repo, q)
		if err != nil {
			return nil, err
		}
		if !contains(ids, id) {
			ids = append(ids, id)
		}
		ev, err := h.Ledger.PutQueue(ctx, repo, q, ids)
		if err != nil {
			return nil, err
		}
		r := queueResult(q, ids, ev)
		r["repo_addr"] = repo
		return r, nil
	case "queue/dequeue":
		repo, q := get("repo_addr"), queueName(get("queue"))
		ids, err := h.Ledger.GetQueue(ctx, repo, q)
		if err != nil {
			return nil, err
		}
		var id string
		if len(ids) > 0 {
			id, ids = ids[0], ids[1:]
		}
		ev, err := h.Ledger.PutQueue(ctx, repo, q, ids)
		if err != nil {
			return nil, err
		}
		r := queueResult(q, ids, ev)
		r["repo_addr"] = repo
		r["task_id"] = id
		return r, nil
	case "queue/list":
		repo, q := get("repo_addr"), queueName(get("queue"))
		ids, err := h.Ledger.GetQueue(ctx, repo, q)
		if err != nil {
			return nil, err
		}
		r := queueResult(q, ids, nil)
		r["repo_addr"] = repo
		h.annotateQueueQuality(ctx, r, ids)
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

func taskResult(i *beadspb.Issue, ev *gonostr.Event) map[string]any {
	r := map[string]any{"task_id": i.Id, "status": nip34.StatusString(i.Status), "assignee": i.Assignee}
	if ev != nil {
		r["event_id"] = ev.ID
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

func queueResult(q string, ids []string, ev *gonostr.Event) map[string]any {
	r := map[string]any{"queue": q, "task_ids": append([]string(nil), ids...)}
	if ev != nil {
		r["event_id"] = ev.ID
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
