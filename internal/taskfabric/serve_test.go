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
	tasks         map[string]*beadspb.Issue
	queues        map[string][]string
	lastTaskEvent *gonostr.Event
}

func (m *memoryLedger) GetTask(ctx context.Context, id string) (*beadspb.Issue, error) {
	return m.tasks[id], nil
}
func (m *memoryLedger) PutTask(ctx context.Context, issue *beadspb.Issue) (*gonostr.Event, error) {
	m.tasks[issue.Id] = issue
	ev, err := nip34.BuildTaskStateEvent(issue, time.Unix(10, 0))
	if err != nil {
		return nil, err
	}
	m.lastTaskEvent = ev
	return ev, nil
}
func (m *memoryLedger) DeleteTask(ctx context.Context, id string) (*gonostr.Event, error) {
	delete(m.tasks, id)
	return &gonostr.Event{ID: testID(12), Kind: gonostr.Kind(5)}, nil
}
func (m *memoryLedger) GetQueue(ctx context.Context, queue string) ([]string, error) {
	return append([]string(nil), m.queues[queue]...), nil
}
func (m *memoryLedger) PutQueue(ctx context.Context, queue string, ids []string) (*gonostr.Event, error) {
	m.queues[queue] = append([]string(nil), ids...)
	return &gonostr.Event{ID: testID(11)}, nil
}

func TestTaskCreateUpdateDeleteIntentRoundTrip(t *testing.T) {
	ledger := &memoryLedger{tasks: map[string]*beadspb.Issue{}, queues: map[string][]string{}}
	h := &Handler{Ledger: ledger}
	now := time.Unix(2, 0)

	create, _ := nip34.BuildContextVMCommand("task/create", "server", map[string]any{
		"task_id": "task-1", "title": "created", "description": "body", "priority": "1", "repo_addr": "30617:owner:repo",
		"epic": "epic-1", "labels": []string{"bug", "backend"}, "depends_on": []string{"task-0"},
	}, time.Unix(1, 0))
	create.ID, create.PubKey = testID(1), testPubKey(1)
	if _, err := h.HandleIntent(context.Background(), create, now); err != nil {
		t.Fatal(err)
	}
	got := ledger.tasks["task-1"]
	if got == nil || got.Title != "created" || got.Priority != beadspb.Priority_PRIORITY_P1 || got.Epic != "epic-1" || len(got.Labels) != 2 || len(got.DependsOn) != 1 || got.GetMetadata().GetCustom()["nip34.repo_addr"] != "30617:owner:repo" {
		t.Fatalf("task not created with full state: %#v", got)
	}
	if ledger.lastTaskEvent == nil || ledger.lastTaskEvent.Kind != gonostr.Kind(nip34.KindCanonicalState) {
		t.Fatalf("create intent did not materialize canonical 30900 state: %#v", ledger.lastTaskEvent)
	}
	roundTrip, err := nip34.ParseTaskStateEvent(ledger.lastTaskEvent)
	if err != nil || roundTrip.GetId() != "task-1" || roundTrip.GetPriority() != beadspb.Priority_PRIORITY_P1 || roundTrip.GetMetadata().GetCustom()["nip34.repo_addr"] != "30617:owner:repo" {
		t.Fatalf("canonical state round trip failed: issue=%#v err=%v", roundTrip, err)
	}

	update, _ := nip34.BuildContextVMCommand("task/update", "server", map[string]any{
		"task_id": "task-1", "priority": "P0", "set_labels": []string{"urgent"},
		"add_dependencies": []string{"task-2"}, "remove_dependencies": []string{"task-0"}, "epic": "",
	}, time.Unix(3, 0))
	update.ID, update.PubKey = testID(2), testPubKey(1)
	if _, err := h.HandleIntent(context.Background(), update, time.Unix(4, 0)); err != nil {
		t.Fatal(err)
	}
	got = ledger.tasks["task-1"]
	if got.Priority != beadspb.Priority_PRIORITY_P0 || got.Epic != "" || len(got.Labels) != 1 || got.Labels[0] != "urgent" || len(got.DependsOn) != 1 || got.DependsOn[0] != "task-2" {
		t.Fatalf("task not fully updated: %#v", got)
	}

	remove, _ := nip34.BuildContextVMCommand("task/delete", "server", map[string]string{"task_id": "task-1"}, time.Unix(5, 0))
	remove.ID, remove.PubKey = testID(3), testPubKey(1)
	if _, err := h.HandleIntent(context.Background(), remove, time.Unix(6, 0)); err != nil {
		t.Fatal(err)
	}
	if ledger.tasks["task-1"] != nil {
		t.Fatalf("task was not deleted: %#v", ledger.tasks["task-1"])
	}
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
