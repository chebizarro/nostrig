package taskfabric

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	gonostr "fiatjaf.com/nostr"
	beadspb "github.com/chebizarro/nostrig/gen/beads"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Ledger interface {
	GetTask(ctx context.Context, id string) (*beadspb.Issue, error)
	PutTask(ctx context.Context, issue *beadspb.Issue) (*gonostr.Event, error)
	GetQueue(ctx context.Context, queue string) ([]string, error)
	PutQueue(ctx context.Context, queue string, ids []string) (*gonostr.Event, error)
}

type Handler struct{ Ledger Ledger }

type rpcEnvelope struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
}

func (h *Handler) HandleIntent(ctx context.Context, ev *gonostr.Event, now time.Time) (*gonostr.Event, error) {
	if h == nil || h.Ledger == nil {
		return nil, fmt.Errorf("handler ledger is nil")
	}
	if ev == nil || ev.Kind != nip34.KindContextVMIntent {
		return nil, fmt.Errorf("expected ContextVM intent")
	}
	var req rpcEnvelope
	if err := json.Unmarshal([]byte(ev.Content), &req); err != nil {
		return nil, err
	}
	method := strings.TrimSpace(req.Method)
	if method == "" {
		method, _ = nip34.TagFirst(ev, "method")
	}
	result, err := h.dispatch(ctx, method, req.Params, ev.PubKey.Hex(), now)
	resp := map[string]any{"jsonrpc": "2.0"}
	if len(req.ID) > 0 {
		var id any
		_ = json.Unmarshal(req.ID, &id)
		resp["id"] = id
	}
	if err != nil {
		resp["error"] = map[string]any{"code": -32000, "message": err.Error()}
	} else {
		resp["result"] = result
	}
	content, err := json.Marshal(resp)
	if err != nil {
		return nil, err
	}
	tags := gonostr.Tags{{"e", ev.ID.Hex()}, {"p", ev.PubKey.Hex()}, {"method", method}, {"schema", nip34.TaskIntentSchema}}
	if id := rawIDString(req.ID); id != "" {
		tags = append(tags, gonostr.Tag{"correlation", id}, gonostr.Tag{"request", id})
	}
	return &gonostr.Event{Kind: gonostr.Kind(nip34.KindContextVMIntent), CreatedAt: gonostr.Timestamp(now.Unix()), Tags: tags, Content: string(content)}, nil
}

func (h *Handler) dispatch(ctx context.Context, method string, raw json.RawMessage, caller string, now time.Time) (any, error) {
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
	switch method {
	case "task/claim":
		id, claimer := get("task_id"), get("claimer")
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
		return taskResult(issue, ev), nil
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
		return taskResult(issue, ev), nil
	case "task/update":
		issue, err := h.task(ctx, get("task_id"))
		if err != nil {
			return nil, err
		}
		if v := get("status"); v != "" {
			issue.Status = nip34.ParseStatus(v)
		}
		if v := get("assignee"); v != "" {
			issue.Assignee = v
		}
		if v := get("title"); v != "" {
			issue.Title = v
		}
		if v := get("description"); v != "" {
			issue.Description = v
		}
		issue.Updated = timestamppb.New(now)
		ev, err := h.Ledger.PutTask(ctx, issue)
		if err != nil {
			return nil, err
		}
		return taskResult(issue, ev), nil
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
		return taskResult(issue, ev), nil
	case "queue/enqueue":
		q, id := queueName(get("queue")), get("task_id")
		if id == "" {
			return nil, fmt.Errorf("task_id is required")
		}
		ids, err := h.Ledger.GetQueue(ctx, q)
		if err != nil {
			return nil, err
		}
		if !contains(ids, id) {
			ids = append(ids, id)
		}
		ev, err := h.Ledger.PutQueue(ctx, q, ids)
		if err != nil {
			return nil, err
		}
		return queueResult(q, ids, ev), nil
	case "queue/dequeue":
		q := queueName(get("queue"))
		ids, err := h.Ledger.GetQueue(ctx, q)
		if err != nil {
			return nil, err
		}
		var id string
		if len(ids) > 0 {
			id, ids = ids[0], ids[1:]
		}
		ev, err := h.Ledger.PutQueue(ctx, q, ids)
		if err != nil {
			return nil, err
		}
		r := queueResult(q, ids, ev)
		r["task_id"] = id
		return r, nil
	case "queue/list":
		q := queueName(get("queue"))
		ids, err := h.Ledger.GetQueue(ctx, q)
		if err != nil {
			return nil, err
		}
		return queueResult(q, ids, nil), nil
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

func queueResult(q string, ids []string, ev *gonostr.Event) map[string]any {
	r := map[string]any{"queue": q, "task_ids": append([]string(nil), ids...)}
	if ev != nil {
		r["event_id"] = ev.ID
	}
	return r
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
