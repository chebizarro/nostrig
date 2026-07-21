package taskfabric

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	gonostr "fiatjaf.com/nostr"
	cascadia "git.sharegap.net/cascadia/cascadia-go"
	casnostr "git.sharegap.net/cascadia/cascadia-go/nostr"
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
	mu            sync.Mutex
	tasks         map[string]*beadspb.Issue
	taskEventIDs  map[string]string
	queues        map[string][]string
	queueRecords  map[string]*QueueRecord
	lastTaskEvent *gonostr.Event
	getTaskErr    error
	deleteCalls   int
	nextEvent     byte
}

type testServeSigner struct{}

func (testServeSigner) SignEvent(_ context.Context, event *gonostr.Event) error {
	if event != nil && event.ID == gonostr.ZeroID {
		event.ID = testID(99)
	}
	return nil
}
func (testServeSigner) Encrypt(context.Context, string, gonostr.PubKey) (string, error) {
	return "", nil
}
func (testServeSigner) Decrypt(context.Context, string, gonostr.PubKey) (string, error) {
	return "", nil
}
func (testServeSigner) Nip04Encrypt(context.Context, string, gonostr.PubKey) (string, error) {
	return "", nil
}
func (testServeSigner) Nip04Decrypt(context.Context, string, gonostr.PubKey) (string, error) {
	return "", nil
}
func (testServeSigner) GetPublicKey(context.Context) (gonostr.PubKey, error) {
	return testPubKey(1), nil
}
func (testServeSigner) PublicKey(context.Context) (string, error) {
	return fmt.Sprintf("%064x", 1), nil
}

type errPublisher struct{ err error }

func (p errPublisher) Publish(ctx context.Context, relays []string, signer nip34.Signer, events []*gonostr.Event) error {
	return p.err
}

func (m *memoryLedger) nextIDLocked() gonostr.ID {
	m.nextEvent++
	if m.nextEvent == 0 {
		m.nextEvent++
	}
	return testID(m.nextEvent)
}

func (m *memoryLedger) GetTask(ctx context.Context, id string) (*beadspb.Issue, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getTaskErr != nil {
		return nil, m.getTaskErr
	}
	return cloneIssue(m.tasks[id]), nil
}

func (m *memoryLedger) MutateTask(ctx context.Context, id string, mutate TaskMutation) (*TaskRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getTaskErr != nil {
		return nil, m.getTaskErr
	}
	if m.taskEventIDs == nil {
		m.taskEventIDs = map[string]string{}
	}
	var current *TaskRecord
	if issue := m.tasks[id]; issue != nil {
		current = &TaskRecord{Issue: cloneIssue(issue), EventID: m.taskEventIDs[id]}
	}
	decision, err := mutate(cloneTaskRecord(current))
	if err != nil {
		return nil, err
	}
	if decision.Unchanged {
		return cloneTaskRecord(current), nil
	}
	eventID := m.nextIDLocked()
	if decision.Delete {
		m.deleteCalls++
		delete(m.tasks, id)
		delete(m.taskEventIDs, id)
		return &TaskRecord{EventID: eventID.Hex()}, nil
	}
	issue := cloneIssue(decision.Issue)
	m.tasks[id] = issue
	m.taskEventIDs[id] = eventID.Hex()
	ev, err := nip34.BuildTaskStateEvent(issue, testPubKey(1).Hex(), time.Unix(10, 0))
	if err != nil {
		return nil, err
	}
	ev.ID, ev.PubKey = eventID, testPubKey(1)
	m.lastTaskEvent = ev
	return &TaskRecord{Issue: cloneIssue(issue), EventID: eventID.Hex(), event: ev}, nil
}

func (m *memoryLedger) GetQueue(ctx context.Context, repoAddr, queue string) (*QueueRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := repoAddr + "|" + queue
	if record := m.queueRecords[key]; record != nil {
		return cloneQueueRecord(record), nil
	}
	if ids, ok := m.queues[key]; ok {
		return &QueueRecord{TaskIDs: append([]string(nil), ids...)}, nil
	}
	return nil, nil
}

func (m *memoryLedger) taskRevision(id string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.taskEventIDs[id]
}

func (m *memoryLedger) queueRevision(repoAddr, queue string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if record := m.queueRecords[repoAddr+"|"+queue]; record != nil {
		return record.EventID
	}
	return ""
}

func (m *memoryLedger) MutateQueue(ctx context.Context, repoAddr, queue string, mutate QueueMutation) (*QueueRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.queueRecords == nil {
		m.queueRecords = map[string]*QueueRecord{}
	}
	key := repoAddr + "|" + queue
	current := cloneQueueRecord(m.queueRecords[key])
	if current == nil {
		if ids, ok := m.queues[key]; ok {
			current = &QueueRecord{TaskIDs: append([]string(nil), ids...)}
		}
	}
	decision, err := mutate(cloneQueueRecord(current))
	if err != nil {
		return nil, err
	}
	if decision.Unchanged {
		return cloneQueueRecord(current), nil
	}
	out := cloneQueueRecord(decision.Queue)
	out.EventID = m.nextIDLocked().Hex()
	m.queueRecords[key] = cloneQueueRecord(out)
	m.queues[key] = append([]string(nil), out.TaskIDs...)
	return out, nil
}

type discardAudit struct{}

func (discardAudit) Record(context.Context, AuthzAuditRecord) error { return nil }

func testHandler(ledger Ledger) *Handler {
	caller := testPubKey(1).Hex()
	return &Handler{
		Ledger: ledger,
		ACL:    map[string]CallerPolicy{caller: {Roles: []Role{RoleAdmin}, Repositories: []string{"*"}}},
		Audit:  discardAudit{},
	}
}

func testAuthorization() AuthorizationConfig {
	caller := testPubKey(1).Hex()
	return AuthorizationConfig{Callers: map[string]CallerPolicy{
		caller: {Roles: []Role{RoleAdmin}, Repositories: []string{"30617:owner:repo"}},
	}}
}

func TestTaskCreateUpdateDeleteIntentRoundTrip(t *testing.T) {
	ledger := &memoryLedger{tasks: map[string]*beadspb.Issue{}, queues: map[string][]string{}}
	h := testHandler(ledger)
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
		"task_id": "task-1", "base_event_id": ledger.taskRevision("task-1"), "priority": "P0", "set_labels": []string{"urgent"},
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

	remove, _ := nip34.BuildContextVMCommand("task/delete", "server", map[string]string{"task_id": "task-1", "base_event_id": ledger.taskRevision("task-1")}, time.Unix(5, 0))
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
	h := testHandler(ledger)
	req, _ := nip34.BuildContextVMCommand("task/claim", "server", map[string]string{"task_id": "task-1", "claimer": "agent-a", "base_event_id": ""}, time.Unix(1, 0))
	req.ID, req.PubKey = testID(1), testPubKey(1)
	resp, err := h.HandleIntent(context.Background(), req, time.Unix(2, 0))
	if err != nil {
		t.Fatal(err)
	}
	if got := ledger.tasks["task-1"]; got.Status != beadspb.Status_STATUS_IN_PROGRESS || got.Assignee != "agent-a" {
		t.Fatalf("task not claimed: %#v response=%s", got, resp.Content)
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
	h := testHandler(ledger)
	req, _ := nip34.BuildCloseCommandAtRevision("task-1", "", "server", time.Unix(1, 0))
	req.ID, req.PubKey = testID(1), testPubKey(1)
	if _, err := h.HandleIntent(context.Background(), req, time.Unix(2, 0)); err != nil {
		t.Fatal(err)
	}
	if got := ledger.tasks["task-1"]; got.Status != beadspb.Status_STATUS_CLOSED {
		t.Fatalf("task not closed: %#v", got)
	}
}

func TestQueueEnqueueDequeueRoundTrip(t *testing.T) {
	ledger := &memoryLedger{tasks: map[string]*beadspb.Issue{"task-1": {Id: "task-1", Title: "queued", Metadata: &beadspb.Metadata{Custom: map[string]string{"nip34.repo_addr": "30617:owner:repo"}}}}, queues: map[string][]string{}}
	h := testHandler(ledger)
	enq, _ := nip34.BuildQueueEnqueueCommandAtRevision("30617:owner:repo", "backlog", "task-1", "", "server", time.Unix(1, 0))
	enq.ID, enq.PubKey = testID(2), testPubKey(1)
	if _, err := h.HandleIntent(context.Background(), enq, time.Unix(2, 0)); err != nil {
		t.Fatal(err)
	}
	deq, _ := nip34.BuildQueueDequeueCommandAtRevision("30617:owner:repo", "backlog", ledger.queueRevision("30617:owner:repo", "backlog"), "server", time.Unix(3, 0))
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
	if body.Result["task_id"] != "task-1" || len(ledger.queueRecords["30617:owner:repo|backlog"].Leases) != 1 {
		t.Fatalf("unexpected reservation result=%s queue=%#v", resp.Content, ledger.queueRecords["30617:owner:repo|backlog"])
	}
}

func TestTaskDeleteRequiresSuccessfulRepoLookupWhenScoped(t *testing.T) {
	lookupErr := errors.New("relay lookup failed")
	ledger := &memoryLedger{tasks: map[string]*beadspb.Issue{}, queues: map[string][]string{}, getTaskErr: lookupErr}
	h := testHandler(ledger)
	h.RepoAddrs = []string{"30617:owner:repo"}
	req, _ := nip34.BuildContextVMCommand("task/delete", "server", map[string]string{"task_id": "task-1"}, time.Unix(1, 0))
	req.ID, req.PubKey = testID(4), testPubKey(1)
	resp, err := h.HandleIntent(context.Background(), req, time.Unix(2, 0))
	if err != nil {
		t.Fatalf("expected JSON-RPC error response, got transport error %v", err)
	}
	if ledger.deleteCalls != 0 {
		t.Fatalf("delete should not reach ledger when scoped lookup fails; deleteCalls=%d", ledger.deleteCalls)
	}
	var body struct {
		Error any `json:"error"`
	}
	if err := json.Unmarshal([]byte(resp.Content), &body); err != nil {
		t.Fatal(err)
	}
	if body.Error == nil {
		t.Fatalf("expected JSON-RPC error response, got %s", resp.Content)
	}
}

func TestValidateIntentRejectsSignatureRecipientAndMethodMismatch(t *testing.T) {
	recipient := testPubKey(1).Hex()
	ev, err := nip34.BuildContextVMCommand("task/create", recipient, map[string]any{"repo_addr": "30617:owner:repo", "task_id": "task-1", "title": "new"}, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := validateIntent(ev, recipient, func(*gonostr.Event) bool { return false }); err == nil {
		t.Fatal("expected invalid signature rejection")
	}
	if err := validateIntent(ev, testPubKey(8).Hex(), func(*gonostr.Event) bool { return true }); err == nil {
		t.Fatal("expected recipient mismatch rejection")
	}
	for _, tag := range ev.Tags {
		if len(tag) >= 2 && tag[0] == "method" {
			tag[1] = "task/delete"
		}
	}
	if err := validateIntent(ev, recipient, func(*gonostr.Event) bool { return true }); err == nil {
		t.Fatal("expected method tag mismatch rejection")
	}
}

func TestServeRequiresRepoAddrInProduction(t *testing.T) {
	t.Setenv("NOSTRIG_ENV", "production")
	err := Serve(context.Background(), ServeOptions{Relays: []string{"wss://relay.example"}})
	if err == nil || err.Error() != "at least one repo addr is required in production serve mode" {
		t.Fatalf("expected production repo-addr guard, got %v", err)
	}
}

func TestServeReportsUnwrapFailureAndContinuesUntilCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan gonostr.RelayEvent, 1)
	ch <- gonostr.RelayEvent{Event: gonostr.Event{ID: testID(20), Kind: gonostr.Kind(cascadia.NIP59_GIFT_WRAP)}}
	var stages []string
	err := Serve(ctx, ServeOptions{
		Relays:        []string{"wss://relay.example"},
		Signer:        testServeSigner{},
		PubKey:        fmt.Sprintf("%064x", 1),
		Authorization: testAuthorization(),
		verify:        func(*gonostr.Event) bool { return true },
		subscribe:     func(ctx context.Context, relays []string, filter gonostr.Filter) <-chan gonostr.RelayEvent { return ch },
		unwrap: func(ctx context.Context, signer casnostr.Signer, outer *gonostr.Event) (*gonostr.Event, error) {
			cancel()
			return nil, errors.New("boom")
		},
		reportError: func(stage string, err error, event *gonostr.Event) { stages = append(stages, stage) },
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation after unwrap failure, got %v", err)
	}
	if len(stages) != 1 || stages[0] != "unwrap" {
		t.Fatalf("expected unwrap error report, got %#v", stages)
	}
}

func TestServeReturnsPlainPublishError(t *testing.T) {
	ch := make(chan gonostr.RelayEvent, 1)
	req, _ := nip34.BuildContextVMCommand("task/create", fmt.Sprintf("%064x", 1), map[string]any{"task_id": "task-1", "title": "created", "repo_addr": "30617:owner:repo"}, time.Unix(1, 0))
	req.ID, req.PubKey = testID(21), testPubKey(1)
	ch <- gonostr.RelayEvent{Event: *req}
	publishErr := errors.New("publish failed")
	var stages []string
	err := Serve(context.Background(), ServeOptions{
		Relays:            []string{"wss://relay.example"},
		RepoAddrs:         []string{"30617:owner:repo"},
		Signer:            testServeSigner{},
		PubKey:            fmt.Sprintf("%064x", 1),
		Authorization:     testAuthorization(),
		verify:            func(*gonostr.Event) bool { return true },
		subscribe:         func(ctx context.Context, relays []string, filter gonostr.Filter) <-chan gonostr.RelayEvent { return ch },
		responsePublisher: errPublisher{err: publishErr},
		reportError:       func(stage string, err error, event *gonostr.Event) { stages = append(stages, stage) },
	})
	if !errors.Is(err, publishErr) {
		t.Fatalf("expected publish error, got %v", err)
	}
	if len(stages) != 1 || stages[0] != "publish_response" {
		t.Fatalf("expected publish_response report, got %#v", stages)
	}
}

func TestServeReturnsWrappedPublishError(t *testing.T) {
	ch := make(chan gonostr.RelayEvent, 1)
	ch <- gonostr.RelayEvent{Event: gonostr.Event{ID: testID(22), Kind: gonostr.Kind(cascadia.NIP59_GIFT_WRAP)}}
	inner, _ := nip34.BuildContextVMCommand("task/create", fmt.Sprintf("%064x", 1), map[string]any{"task_id": "task-1", "title": "created", "repo_addr": "30617:owner:repo"}, time.Unix(1, 0))
	inner.PubKey = testPubKey(1)
	publishErr := errors.New("wrapped publish failed")
	var stages []string
	err := Serve(context.Background(), ServeOptions{
		Relays:        []string{"wss://relay.example"},
		RepoAddrs:     []string{"30617:owner:repo"},
		Signer:        testServeSigner{},
		PubKey:        fmt.Sprintf("%064x", 1),
		Authorization: testAuthorization(),
		verify:        func(*gonostr.Event) bool { return true },
		subscribe:     func(ctx context.Context, relays []string, filter gonostr.Filter) <-chan gonostr.RelayEvent { return ch },
		unwrap: func(ctx context.Context, signer casnostr.Signer, outer *gonostr.Event) (*gonostr.Event, error) {
			return inner, nil
		},
		wrap: func(ctx context.Context, signer casnostr.Signer, recipientPubkey string, payload json.RawMessage) (*gonostr.Event, error) {
			return &gonostr.Event{ID: testID(23), Kind: gonostr.Kind(cascadia.NIP59_GIFT_WRAP)}, nil
		},
		publishWrapped: func(ctx context.Context, relays []string, outer *gonostr.Event) error { return publishErr },
		reportError:    func(stage string, err error, event *gonostr.Event) { stages = append(stages, stage) },
	})
	if !errors.Is(err, publishErr) {
		t.Fatalf("expected wrapped publish error, got %v", err)
	}
	if len(stages) != 1 || stages[0] != "publish_wrapped_response" {
		t.Fatalf("expected publish_wrapped_response report, got %#v", stages)
	}
}
