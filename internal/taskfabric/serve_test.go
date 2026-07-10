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

func testID(n byte) gonostr.ID {
	var id gonostr.ID
	id[31] = n
	return id
}

func testPubKey(n byte) gonostr.PubKey {
	var pk gonostr.PubKey
	pk[31] = n
	return pk
}

type memoryLedger struct {
	tasks  map[string]*beadspb.Issue
	queues map[string][]string
}

func (m *memoryLedger) GetTask(ctx context.Context, id string) (*beadspb.Issue, error) {
	return m.tasks[id], nil
}
func (m *memoryLedger) PutTask(ctx context.Context, issue *beadspb.Issue) (*gonostr.Event, error) {
	m.tasks[issue.Id] = issue
	return &gonostr.Event{ID: testID(10)}, nil
}
func (m *memoryLedger) GetQueue(ctx context.Context, queue string) ([]string, error) {
	return append([]string(nil), m.queues[queue]...), nil
}
func (m *memoryLedger) PutQueue(ctx context.Context, queue string, ids []string) (*gonostr.Event, error) {
	m.queues[queue] = append([]string(nil), ids...)
	return &gonostr.Event{ID: testID(11)}, nil
}

func TestTaskClaimIntentUpdatesTaskStateAndReturnsResult(t *testing.T) {
	ledger := &memoryLedger{tasks: map[string]*beadspb.Issue{"task-1": {Id: "task-1", Title: "claim me", Status: beadspb.Status_STATUS_OPEN}}, queues: map[string][]string{}}
	h := &Handler{Ledger: ledger}
	req, _ := nip34.BuildContextVMCommand("task/claim", "server", map[string]string{"task_id": "task-1", "claimer": "agent-a"}, time.Unix(1, 0))
	req.ID, req.PubKey = testID(1), testPubKey(1)
	resp, err := h.HandleIntent(context.Background(), req, time.Unix(2, 0))
	if err != nil {
		t.Fatal(err)
	}
	if got := ledger.tasks["task-1"]; got.Status != beadspb.Status_STATUS_IN_PROGRESS || got.Assignee != "agent-a" {
		t.Fatalf("task not claimed: %#v", got)
	}
	var body struct {
		Result map[string]any `json:"result"`
	}
	if err := json.Unmarshal([]byte(resp.Content), &body); err != nil {
		t.Fatal(err)
	}
	if body.Result["status"] != "in_progress" || body.Result["assignee"] != "agent-a" {
		t.Fatalf("unexpected result: %s", resp.Content)
	}
}

func TestTaskCloseIntentClosesTask(t *testing.T) {
	ledger := &memoryLedger{tasks: map[string]*beadspb.Issue{"task-1": {Id: "task-1", Title: "close me", Status: beadspb.Status_STATUS_IN_PROGRESS}}, queues: map[string][]string{}}
	h := &Handler{Ledger: ledger}
	req, _ := nip34.BuildCloseCommand("task-1", "server", time.Unix(1, 0))
	req.ID, req.PubKey = testID(1), testPubKey(1)
	if _, err := h.HandleIntent(context.Background(), req, time.Unix(2, 0)); err != nil {
		t.Fatal(err)
	}
	if got := ledger.tasks["task-1"]; got.Status != beadspb.Status_STATUS_CLOSED {
		t.Fatalf("task not closed: %#v", got)
	}
}

func TestQueueEnqueueDequeueRoundTrip(t *testing.T) {
	ledger := &memoryLedger{tasks: map[string]*beadspb.Issue{}, queues: map[string][]string{}}
	h := &Handler{Ledger: ledger}
	enq, _ := nip34.BuildQueueEnqueueCommand("backlog", "task-1", "server", time.Unix(1, 0))
	enq.ID, enq.PubKey = testID(2), testPubKey(1)
	if _, err := h.HandleIntent(context.Background(), enq, time.Unix(2, 0)); err != nil {
		t.Fatal(err)
	}
	deq, _ := nip34.BuildQueueDequeueCommand("backlog", "server", time.Unix(3, 0))
	deq.ID, deq.PubKey = testID(3), testPubKey(1)
	resp, err := h.HandleIntent(context.Background(), deq, time.Unix(4, 0))
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		Result map[string]any `json:"result"`
	}
	if err := json.Unmarshal([]byte(resp.Content), &body); err != nil {
		t.Fatal(err)
	}
	if body.Result["task_id"] != "task-1" || len(ledger.queues["backlog"]) != 0 {
		t.Fatalf("unexpected dequeue result=%s queue=%#v", resp.Content, ledger.queues["backlog"])
	}
}
