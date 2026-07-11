package taskfabric

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	gonostr "fiatjaf.com/nostr"
	beadspb "github.com/chebizarro/nostrig/gen/beads"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
)

func TestProjectQualityStateParsesPSTFGateAndTaskAuditEvents(t *testing.T) {
	projectEvent := &gonostr.Event{
		ID:        testID(30),
		Kind:      KindPSTFGateStatus,
		CreatedAt: gonostr.Timestamp(time.Unix(100, 0).Unix()),
		Tags:      gonostr.Tags{{"project", "nostrig"}, {"d", "pstf:gate:nostrig"}, {"status", "fail"}},
		Content: compactJSON(map[string]any{
			"schema_version": "pstf.status.gate.v1",
			"project":        "nostrig",
			"result":         "fail",
			"reason":         "1 blocking drift finding(s).",
			"blocks_merge":   true,
			"findings":       []map[string]any{{"id": "DRIFT-1", "title": "Spec drift", "blocks_merge": true}},
		}),
	}
	taskEvent := &gonostr.Event{
		ID:        testID(31),
		Kind:      KindPSTFAudit,
		CreatedAt: gonostr.Timestamp(time.Unix(200, 0).Unix()),
		Tags:      gonostr.Tags{{"project", "nostrig"}, {"task", "task-1"}, {"decision", "pass"}},
		Content: compactJSON(map[string]any{
			"schema_version": "pstf.audit.gate_decision.v1",
			"project":        "nostrig",
			"decision":       "pass",
			"reason":         "No blocking drift detected.",
			"blocks_merge":   false,
			"findings":       []map[string]any{},
		}),
	}

	state := ProjectQualityState([]*gonostr.Event{projectEvent, taskEvent})
	if state.Project.State != QualityFailing || !state.Project.BlocksMerge || len(state.Project.Findings) != 1 {
		t.Fatalf("unexpected project quality: %#v", state.Project)
	}
	if got := state.Tasks["task-1"]; got.State != QualityPassing || got.EventKind != KindPSTFAudit || got.Project != "nostrig" {
		t.Fatalf("unexpected task quality: %#v", got)
	}
}

func compactJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

type staticQuality struct{ values map[string]QualityResult }

func (s staticQuality) GetQuality(ctx context.Context, taskIDs []string) (map[string]QualityResult, error) {
	out := pendingQuality(taskIDs)
	for _, id := range taskIDs {
		if q, ok := s.values[id]; ok {
			out[id] = q
		}
	}
	return out, nil
}

func TestQueueListAnnotatesQuality(t *testing.T) {
	ledger := &memoryLedger{tasks: map[string]*beadspb.Issue{}, queues: map[string][]string{"backlog": []string{"task-1"}}}
	h := &Handler{Ledger: ledger, Quality: staticQuality{values: map[string]QualityResult{"task-1": {State: QualityFailing, Result: "fail", Reason: "drift"}}}}
	req, _ := nip34.BuildQueueListCommand("backlog", "server", time.Unix(1, 0))
	req.ID, req.PubKey = testID(32), testPubKey(1)
	resp, err := h.HandleIntent(context.Background(), req, time.Unix(2, 0))
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		Result map[string]any `json:"result"`
	}
	if err := json.Unmarshal([]byte(resp.Content), &body); err != nil {
		t.Fatal(err)
	}
	quality := body.Result["quality"].(map[string]any)
	q := quality["task-1"].(map[string]any)
	if q["state"] != QualityFailing || q["reason"] != "drift" {
		t.Fatalf("unexpected quality result: %s", resp.Content)
	}
}

func TestTaskQualityStatusIntentReturnsQualityForIDs(t *testing.T) {
	ledger := &memoryLedger{tasks: map[string]*beadspb.Issue{}, queues: map[string][]string{}}
	h := &Handler{Ledger: ledger, Quality: staticQuality{values: map[string]QualityResult{"task-1": {State: QualityPassing, Result: "pass"}}}}
	req, _ := nip34.BuildContextVMCommand("task/quality-status", "server", map[string]any{"task_ids": []string{"task-1", "task-2"}}, time.Unix(1, 0))
	req.ID, req.PubKey = testID(33), testPubKey(1)
	resp, err := h.HandleIntent(context.Background(), req, time.Unix(2, 0))
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		Result map[string]any `json:"result"`
	}
	if err := json.Unmarshal([]byte(resp.Content), &body); err != nil {
		t.Fatal(err)
	}
	quality := body.Result["quality"].(map[string]any)
	if quality["task-1"].(map[string]any)["state"] != QualityPassing {
		t.Fatalf("task-1 quality missing: %s", resp.Content)
	}
	if quality["task-2"].(map[string]any)["state"] != QualityPending {
		t.Fatalf("task-2 pending quality missing: %s", resp.Content)
	}
}
