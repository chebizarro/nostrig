package taskfabric

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	gonostr "fiatjaf.com/nostr"
	beadspb "github.com/chebizarro/nostrig/gen/beads"
)

func TestAgentUpdatePersistsCheckpointBlockerEvidenceAndReviewRequest(t *testing.T) {
	ledger := &memoryLedger{
		tasks: map[string]*beadspb.Issue{
			"task-1": {Id: "task-1", Title: "work", Status: beadspb.Status_STATUS_IN_PROGRESS, Assignee: "worker"},
		},
		taskEventIDs: map[string]string{"task-1": "base-event"},
		queues:       map[string][]string{},
	}
	h := testHandler(ledger)
	ev := &gonostr.Event{ID: testID(44), PubKey: testPubKey(1)}
	params, err := json.Marshal(map[string]any{
		"task_id": "task-1", "base_event_id": "base-event", "status": "blocked",
		"status_reason": "relay is unavailable", "blocker_description": "relay is unavailable",
		"notes": "handoff details", "evidence_ids": []string{"event:abc"},
		"checkpoint_summary": "blocked after retry", "checkpoint_status": "blocked",
		"checkpoint_evidence_ids": []string{"event:abc"}, "request_validation": true,
		"reviewer": "reviewer-1", "review_requirements": []string{"go test ./..."},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := h.dispatch(context.Background(), ev, "task/update", params, testPubKey(1).Hex(), time.Unix(20, 0))
	if err != nil {
		t.Fatal(err)
	}
	issue := ledger.tasks["task-1"]
	if issue.Status != beadspb.Status_STATUS_BLOCKED || issue.BlockedAt == nil || issue.BlockerDescription != "relay is unavailable" {
		t.Fatalf("block state not persisted: %#v", issue)
	}
	if issue.Notes != "handoff details" || issue.StatusReason != "relay is unavailable" {
		t.Fatalf("durable notes/reason not persisted: %#v", issue)
	}
	if len(issue.Checkpoints) != 1 || issue.Checkpoints[0].Id != "checkpoint:"+testID(44).Hex() || issue.Checkpoints[0].Summary != "blocked after retry" {
		t.Fatalf("checkpoint not persisted: %#v", issue.Checkpoints)
	}
	if issue.Review == nil || !issue.Review.Required || issue.Review.State != "requested" || issue.Review.Reviewer != "reviewer-1" {
		t.Fatalf("review request not persisted: %#v", issue.Review)
	}
	response := result.(map[string]any)
	if got := response["evidence_ids"]; !reflect.DeepEqual(got, []string{"event:abc"}) {
		t.Fatalf("evidence_ids=%#v", got)
	}
}

func TestAgentClosePersistsAcceptanceEvidenceAndReturnsItsID(t *testing.T) {
	ledger := &memoryLedger{
		tasks: map[string]*beadspb.Issue{
			"task-1": {Id: "task-1", Title: "work", Status: beadspb.Status_STATUS_IN_PROGRESS, Assignee: "worker"},
		},
		taskEventIDs: map[string]string{"task-1": "base-event"},
		queues:       map[string][]string{},
	}
	h := testHandler(ledger)
	params, _ := json.Marshal(map[string]any{
		"task_id": "task-1", "base_event_id": "base-event", "close_reason": "accepted",
		"acceptance_evidence_ids": []string{"review:event-1"},
	})
	ev := &gonostr.Event{ID: testID(45), PubKey: testPubKey(1)}
	result, err := h.dispatch(context.Background(), ev, "task/close", params, testPubKey(1).Hex(), time.Unix(30, 0))
	if err != nil {
		t.Fatal(err)
	}
	issue := ledger.tasks["task-1"]
	if issue.Status != beadspb.Status_STATUS_CLOSED || issue.ClosedAt == nil || issue.CloseReason != "accepted" {
		t.Fatalf("close state not persisted: %#v", issue)
	}
	response := result.(map[string]any)
	if got := response["evidence_ids"]; !reflect.DeepEqual(got, []string{"review:event-1"}) {
		t.Fatalf("evidence_ids=%#v", got)
	}
}
