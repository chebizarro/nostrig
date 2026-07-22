package taskfabric

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	gonostr "fiatjaf.com/nostr"
	beadspb "github.com/chebizarro/nostrig/gen/beads"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
)

func TestProjectQualityStateParsesPSTFGateAndTaskAuditEvents(t *testing.T) {
	trusted := gonostr.Generate()
	projectEvent := signedQualityEvent(t, trusted, KindPSTFGateStatus, 100,
		gonostr.Tags{{"project", "nostrig"}, {"d", "pstf:gate:nostrig"}, {"status", "fail"}},
		map[string]any{
			"schema_version": "pstf.status.gate.v1", "project": "nostrig", "result": "fail",
			"reason": "1 blocking drift finding(s).", "blocks_merge": true,
			"findings": []map[string]any{{"id": "DRIFT-1", "severity": "critical", "status": "confirmed", "title": "Spec drift", "evidence": []string{"diff"}, "blocks_merge": true}},
		})
	taskEvent := signedQualityEvent(t, trusted, KindPSTFAudit, 200,
		gonostr.Tags{{"project", "nostrig"}, {"d", "pstf:gate-decision:nostrig:200"}, {"audit_type", "CAS_AUDIT"}, {"task", "task-1"}, {"decision", "pass"}},
		map[string]any{
			"schema_version": "pstf.audit.gate_decision.v1", "project": "nostrig", "decision": "pass",
			"reason": "No blocking drift detected.", "blocks_merge": false, "findings": []map[string]any{},
		})

	state, err := ProjectQualityState([]*gonostr.Event{projectEvent, taskEvent}, []string{projectEvent.PubKey.Hex()}, "nostrig")
	if err != nil {
		t.Fatal(err)
	}
	if state.Project.State != QualityFailing || !state.Project.BlocksMerge || len(state.Project.Findings) != 1 {
		t.Fatalf("unexpected project quality: %#v", state.Project)
	}
	if got := state.Tasks["task-1"]; got.State != QualityPassing || got.EventKind != KindPSTFAudit || got.Project != "nostrig" {
		t.Fatalf("unexpected task quality: %#v", got)
	}
}

func TestProjectQualityStateRejectsUntrustedMalformedAndCrossProjectEvents(t *testing.T) {
	trusted, untrusted := gonostr.Generate(), gonostr.Generate()
	valid := signedQualityEvent(t, trusted, KindPSTFGateStatus, 40,
		gonostr.Tags{{"project", "nostrig"}, {"d", "pstf:gate:nostrig"}, {"status", "pass"}},
		map[string]any{"schema_version": "pstf.status.gate.v1", "project": "nostrig", "result": "pass", "reason": "ok", "blocks_merge": false, "findings": []any{}})
	untrustedFailure := signedQualityEvent(t, untrusted, KindPSTFGateStatus, 41,
		gonostr.Tags{{"project", "nostrig"}, {"d", "pstf:gate:nostrig"}, {"status", "fail"}},
		map[string]any{"schema_version": "pstf.status.gate.v1", "project": "nostrig", "result": "fail", "reason": "forged", "blocks_merge": true, "findings": []any{}})
	wrongSchema := signedQualityEvent(t, trusted, KindPSTFGateStatus, 42,
		gonostr.Tags{{"project", "nostrig"}, {"d", "pstf:gate:nostrig"}, {"status", "fail"}},
		map[string]any{"schema_version": "bahia.status.continuity-heartbeat.v1", "project": "nostrig", "result": "fail", "reason": "unrelated", "blocks_merge": true, "findings": []any{}})
	missingRequired := signedQualityEvent(t, trusted, KindPSTFGateStatus, 43,
		gonostr.Tags{{"project", "nostrig"}, {"d", "pstf:gate:nostrig"}, {"status", "pass"}},
		map[string]any{"schema_version": "pstf.status.gate.v1", "project": "nostrig"})
	conflictingTag := signedQualityEvent(t, trusted, KindPSTFGateStatus, 44,
		gonostr.Tags{{"project", "nostrig"}, {"d", "pstf:gate:nostrig"}, {"status", "fail"}},
		map[string]any{"schema_version": "pstf.status.gate.v1", "project": "nostrig", "result": "pass", "reason": "conflict", "blocks_merge": false, "findings": []any{}})
	nullFindings := signedQualityEvent(t, trusted, KindPSTFGateStatus, 45,
		gonostr.Tags{{"project", "nostrig"}, {"d", "pstf:gate:nostrig"}, {"status", "fail"}},
		map[string]any{"schema_version": "pstf.status.gate.v1", "project": "nostrig", "result": "fail", "reason": "null findings", "blocks_merge": true, "findings": nil})
	otherProject := signedQualityEvent(t, trusted, KindPSTFGateStatus, 46,
		gonostr.Tags{{"project", "other"}, {"status", "fail"}},
		map[string]any{"schema_version": "pstf.status.gate.v1", "project": "other", "result": "fail", "reason": "other", "blocks_merge": true, "findings": []any{}})

	state, err := ProjectQualityState([]*gonostr.Event{valid, untrustedFailure, wrongSchema, missingRequired, conflictingTag, nullFindings, otherProject}, []string{valid.PubKey.Hex()}, "nostrig")
	if err != nil {
		t.Fatal(err)
	}
	if state.Project.State != QualityPassing || state.Project.Author != valid.PubKey.Hex() || state.Project.EventID != valid.ID.Hex() {
		t.Fatalf("invalid event affected quality: %#v", state.Project)
	}
}

func TestProjectQualityStateUsesDeterministicEventIDTieBreak(t *testing.T) {
	trusted := gonostr.Generate()
	pass := signedQualityEvent(t, trusted, KindPSTFGateStatus, 70,
		gonostr.Tags{{"project", "nostrig"}, {"d", "pstf:gate:nostrig"}, {"status", "pass"}},
		map[string]any{"schema_version": "pstf.status.gate.v1", "project": "nostrig", "result": "pass", "reason": "pass", "blocks_merge": false, "findings": []any{}})
	fail := signedQualityEvent(t, trusted, KindPSTFGateStatus, 70,
		gonostr.Tags{{"project", "nostrig"}, {"d", "pstf:gate:nostrig"}, {"status", "fail"}},
		map[string]any{"schema_version": "pstf.status.gate.v1", "project": "nostrig", "result": "fail", "reason": "fail", "blocks_merge": true, "findings": []any{}})
	want := pass
	if fail.ID.Hex() > pass.ID.Hex() {
		want = fail
	}
	for _, events := range [][]*gonostr.Event{{pass, fail}, {fail, pass}} {
		state, err := ProjectQualityState(events, []string{pass.PubKey.Hex()}, "nostrig")
		if err != nil {
			t.Fatal(err)
		}
		if state.Project.EventID != want.ID.Hex() {
			t.Fatalf("tie break selected %s, want %s", state.Project.EventID, want.ID.Hex())
		}
	}
}

func TestFilterTrustedQualityEventsRequiresTrustedValidSignature(t *testing.T) {
	trustedEvent := signedQualityEvent(t, gonostr.Generate(), KindPSTFGateStatus, 60,
		gonostr.Tags{{"project", "nostrig"}, {"d", "pstf:gate:nostrig"}, {"status", "pass"}},
		map[string]any{"schema_version": "pstf.status.gate.v1", "project": "nostrig", "result": "pass", "reason": "ok", "blocks_merge": false, "findings": []any{}})
	untrustedEvent := signedQualityEvent(t, gonostr.Generate(), KindPSTFGateStatus, 61,
		gonostr.Tags{{"project", "nostrig"}, {"d", "pstf:gate:nostrig"}, {"status", "pass"}},
		map[string]any{"schema_version": "pstf.status.gate.v1", "project": "nostrig", "result": "pass", "reason": "ok", "blocks_merge": false, "findings": []any{}})
	tampered := *trustedEvent
	tampered.Content = `{"schema_version":"pstf.status.gate.v1","project":"nostrig","result":"fail"}`
	filtered := filterTrustedQualityEvents([]*gonostr.Event{untrustedEvent, &tampered, trustedEvent}, map[string]struct{}{trustedEvent.PubKey.Hex(): {}})
	if len(filtered) != 1 || filtered[0].ID != trustedEvent.ID {
		t.Fatalf("trusted signature filter returned %#v", filtered)
	}
}

func signedQualityEvent(t *testing.T, key gonostr.SecretKey, kind int, createdAt int64, tags gonostr.Tags, body map[string]any) *gonostr.Event {
	t.Helper()
	event := &gonostr.Event{Kind: gonostr.Kind(kind), CreatedAt: gonostr.Timestamp(createdAt), Tags: tags, Content: compactJSON(body)}
	if err := event.Sign(key); err != nil {
		t.Fatal(err)
	}
	return event
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
	ledger := &memoryLedger{tasks: map[string]*beadspb.Issue{}, queues: map[string][]string{"30617:owner:repo|backlog": {"task-1"}}}
	h := testHandler(ledger)
	h.Quality = staticQuality{values: map[string]QualityResult{"task-1": {State: QualityFailing, Result: "fail", Reason: "drift"}}}
	req, _ := nip34.BuildQueueListCommandForRepo("30617:owner:repo", "backlog", "server", time.Unix(1, 0))
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

func TestQualityGatedCloseBlockedUntilTrustedQualityPasses(t *testing.T) {
	ledger := &memoryLedger{
		tasks: map[string]*beadspb.Issue{"task-1": {
			Id: "task-1", Title: "gated", Status: beadspb.Status_STATUS_IN_PROGRESS,
			Metadata: &beadspb.Metadata{Custom: map[string]string{"nip34.repo_addr": "30617:owner:repo"}},
		}},
		taskEventIDs: map[string]string{"task-1": testID(50).Hex()}, queues: map[string][]string{},
	}
	quality := staticQuality{values: map[string]QualityResult{"task-1": {State: QualityPending, Result: "pending"}}}
	h := testHandler(ledger)
	h.ClosePolicy.RequireQuality = true
	h.Quality = quality
	closeRequest := func(id byte) *gonostr.Event {
		req, _ := nip34.BuildCloseCommandAtRevision("task-1", ledger.taskRevision("task-1"), "server", time.Unix(int64(id), 0))
		req.ID, req.PubKey = testID(id), testPubKey(1)
		return req
	}
	resp, err := h.HandleIntent(context.Background(), closeRequest(51), time.Unix(51, 0))
	if err != nil {
		t.Fatal(err)
	}
	if ledger.tasks["task-1"].Status == beadspb.Status_STATUS_CLOSED || !strings.Contains(resp.Content, "quality_required") {
		t.Fatalf("pending quality did not block close: task=%#v response=%s", ledger.tasks["task-1"], resp.Content)
	}
	quality.values["task-1"] = QualityResult{State: QualityPassing, Result: "pass"}
	if _, err := h.HandleIntent(context.Background(), closeRequest(52), time.Unix(52, 0)); err != nil {
		t.Fatal(err)
	}
	if ledger.tasks["task-1"].Status != beadspb.Status_STATUS_CLOSED {
		t.Fatalf("passing quality did not allow close: %#v", ledger.tasks["task-1"])
	}
}

func TestTaskQualityStatusIntentReturnsQualityForIDs(t *testing.T) {
	repoMeta := &beadspb.Metadata{Custom: map[string]string{"nip34.repo_addr": "30617:owner:repo"}}
	ledger := &memoryLedger{tasks: map[string]*beadspb.Issue{"task-1": {Id: "task-1", Metadata: repoMeta}, "task-2": {Id: "task-2", Metadata: repoMeta}}, queues: map[string][]string{}}
	h := testHandler(ledger)
	h.Quality = staticQuality{values: map[string]QualityResult{"task-1": {State: QualityPassing, Result: "pass"}}}
	req, _ := nip34.BuildContextVMCommand("task/quality-status", "server", map[string]any{"repo_addr": "30617:owner:repo", "task_ids": []string{"task-1", "task-2"}}, time.Unix(1, 0))
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
