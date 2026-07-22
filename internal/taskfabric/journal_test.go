package taskfabric

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	gonostr "fiatjaf.com/nostr"
	beadspb "github.com/chebizarro/nostrig/gen/beads"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
)

type responseCapture struct {
	mu       sync.Mutex
	events   []*gonostr.Event
	failNext int
}

func (c *responseCapture) publish(_ context.Context, event *gonostr.Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	copy := cloneNostrEvent(*event)
	c.events = append(c.events, &copy)
	if c.failNext > 0 {
		c.failNext--
		return errors.New("response publication interrupted")
	}
	return nil
}

func (c *responseCapture) contents() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.events))
	for _, event := range c.events {
		out = append(out, event.Content)
	}
	return out
}

func openTestJournal(t *testing.T, path string, retention time.Duration) *CommandJournal {
	t.Helper()
	journal, err := OpenCommandJournal(path, retention)
	if err != nil {
		t.Fatal(err)
	}
	return journal
}

func testCommandProcessor(journal *CommandJournal, ledger Ledger, capture *responseCapture, now time.Time) *commandProcessor {
	recipient := testPubKey(9).Hex()
	handler := testHandler(ledger)
	handler.Recipient = recipient
	return &commandProcessor{
		journal: journal,
		handler: handler,
		signer:  testServeSigner{},
		relays:  []string{"wss://relay.example"},
		publishPlain: func(ctx context.Context, event *gonostr.Event) error {
			return capture.publish(ctx, event)
		},
		reportError: func(string, error, *gonostr.Event) {},
		verify:      func(*gonostr.Event) bool { return true },
		now:         func() time.Time { return now },
	}
}

func buildJournalCommand(t *testing.T, method string, params any, created time.Time, id byte) *gonostr.Event {
	t.Helper()
	command, err := nip34.BuildContextVMCommand(method, testPubKey(9).Hex(), params, created)
	if err != nil {
		t.Fatal(err)
	}
	command.ID, command.PubKey = testID(id), testPubKey(1)
	return command
}

func TestCommandJournalDetectsRequestIDContentConflict(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	journal := openTestJournal(t, filepath.Join(t.TempDir(), "commands.json"), time.Hour)
	first := buildJournalCommand(t, "task/create", map[string]any{"task_id": "a", "title": "one"}, now, 1)
	if _, created, err := journal.Begin(first, first, false, now); err != nil || !created {
		t.Fatalf("begin first: created=%v err=%v", created, err)
	}
	conflict := buildJournalCommand(t, "task/create", map[string]any{"task_id": "b", "title": "different"}, now, 2)
	_, _, err := journal.Begin(conflict, conflict, false, now)
	var requestConflict *RequestIDConflictError
	if !errors.As(err, &requestConflict) {
		t.Fatalf("expected request-id conflict, got %v", err)
	}
}

func TestDuplicateRequestIDWithSameContentReturnsCachedResponse(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	journal := openTestJournal(t, filepath.Join(t.TempDir(), "commands.json"), 24*time.Hour)
	ledger := &memoryLedger{tasks: map[string]*beadspb.Issue{}, queues: map[string][]string{}}
	capture := &responseCapture{}
	processor := testCommandProcessor(journal, ledger, capture, now)
	first := buildJournalCommand(t, "task/create", map[string]any{
		"task_id": "task-1", "title": "created", "repo_addr": "30617:owner:repo",
	}, now, 3)
	duplicate := cloneNostrEvent(*first)
	duplicate.ID = testID(4)
	if err := processor.Process(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if err := processor.Process(context.Background(), &duplicate); err != nil {
		t.Fatal(err)
	}
	if ledger.nextEvent != 1 || len(capture.contents()) != 2 {
		t.Fatalf("same request id was not cached: mutations=%d responses=%d", ledger.nextEvent, len(capture.contents()))
	}
	record, created, err := journal.Begin(&duplicate, &duplicate, false, now)
	if err != nil || created || len(record.CommandEventIDs) != 2 {
		t.Fatalf("processed event aliases were not durable: record=%#v created=%v err=%v", record, created, err)
	}
}

func TestConflictingRequestIDPublishesErrorWithoutMutation(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	journal := openTestJournal(t, filepath.Join(t.TempDir(), "commands.json"), 24*time.Hour)
	ledger := &memoryLedger{tasks: map[string]*beadspb.Issue{}, queues: map[string][]string{}}
	capture := &responseCapture{}
	processor := testCommandProcessor(journal, ledger, capture, now)
	first := buildJournalCommand(t, "task/create", map[string]any{
		"task_id": "task-1", "title": "first", "repo_addr": "30617:owner:repo",
	}, now, 5)
	conflict := buildJournalCommand(t, "task/create", map[string]any{
		"task_id": "task-2", "title": "conflict", "repo_addr": "30617:owner:repo",
	}, now, 6)
	if err := processor.Process(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if err := processor.Process(context.Background(), conflict); err != nil {
		t.Fatal(err)
	}
	if ledger.nextEvent != 1 || ledger.tasks["task-2"] != nil {
		t.Fatalf("conflicting request mutated state: mutations=%d task2=%#v", ledger.nextEvent, ledger.tasks["task-2"])
	}
	contents := capture.contents()
	var response struct {
		Error *struct {
			Code int `json:"code"`
		} `json:"error"`
	}
	if len(contents) != 2 || json.Unmarshal([]byte(contents[1]), &response) != nil || response.Error == nil || response.Error.Code != requestIDConflictCode {
		t.Fatalf("missing request-id conflict response: %#v", contents)
	}
}

func TestCommandJournalRetentionRejectsExpiredReplay(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	path := filepath.Join(t.TempDir(), "commands.json")
	journal := openTestJournal(t, path, time.Hour)
	old := buildJournalCommand(t, "task/create", map[string]any{"task_id": "old", "title": "old"}, now, 1)
	record, _, err := journal.Begin(old, old, false, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Complete(record.EventID, now); err != nil {
		t.Fatal(err)
	}
	later := now.Add(2 * time.Hour)
	fresh := buildJournalCommand(t, "task/create", map[string]any{"task_id": "fresh", "title": "fresh"}, later, 2)
	if _, _, err := journal.Begin(fresh, fresh, false, later); err != nil {
		t.Fatal(err)
	}
	if _, _, err := journal.Begin(old, old, false, later); !errors.Is(err, ErrReplayExpired) {
		t.Fatalf("expired event was replayable: %v", err)
	}
}

func TestReplayCreateUpdateDeleteHundredTimesMutatesOnce(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	journal := openTestJournal(t, filepath.Join(t.TempDir(), "commands.json"), 24*time.Hour)
	ledger := &memoryLedger{tasks: map[string]*beadspb.Issue{}, queues: map[string][]string{}}
	capture := &responseCapture{}
	processor := testCommandProcessor(journal, ledger, capture, now)

	create := buildJournalCommand(t, "task/create", map[string]any{
		"task_id": "task-1", "title": "created", "repo_addr": "30617:owner:repo",
	}, now, 10)
	for range 100 {
		if err := processor.Process(context.Background(), create); err != nil {
			t.Fatal(err)
		}
	}
	if ledger.nextEvent != 1 || ledger.tasks["task-1"] == nil {
		t.Fatalf("create replay mutated %d times", ledger.nextEvent)
	}

	update := buildJournalCommand(t, "task/update", map[string]any{
		"task_id": "task-1", "base_event_id": ledger.taskRevision("task-1"), "title": "updated",
	}, now.Add(time.Second), 11)
	for range 100 {
		if err := processor.Process(context.Background(), update); err != nil {
			t.Fatal(err)
		}
	}
	if ledger.nextEvent != 2 || ledger.tasks["task-1"].Title != "updated" {
		t.Fatalf("update replay mutated state %d times: %#v", ledger.nextEvent, ledger.tasks["task-1"])
	}

	remove := buildJournalCommand(t, "task/delete", map[string]any{
		"task_id": "task-1", "base_event_id": ledger.taskRevision("task-1"),
	}, now.Add(2*time.Second), 12)
	for range 100 {
		if err := processor.Process(context.Background(), remove); err != nil {
			t.Fatal(err)
		}
	}
	if ledger.nextEvent != 3 || ledger.deleteCalls != 1 || ledger.tasks["task-1"] != nil {
		t.Fatalf("delete replay mutated state %d times (delete calls %d)", ledger.nextEvent, ledger.deleteCalls)
	}
	if got := len(capture.contents()); got != 300 {
		t.Fatalf("duplicates did not receive cached responses: got %d", got)
	}
}

func TestRestartAfterStateBeforeResponseRepairsCachedResponse(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	path := filepath.Join(t.TempDir(), "commands.json")
	ledger := &memoryLedger{tasks: map[string]*beadspb.Issue{}, queues: map[string][]string{}}
	command := buildJournalCommand(t, "task/create", map[string]any{
		"task_id": "task-1", "title": "created", "repo_addr": "30617:owner:repo",
	}, now, 20)
	firstCapture := &responseCapture{failNext: 1}
	first := testCommandProcessor(openTestJournal(t, path, 24*time.Hour), ledger, firstCapture, now)
	if err := first.Process(context.Background(), command); err == nil {
		t.Fatal("expected interrupted response publication")
	}
	if ledger.nextEvent != 1 {
		t.Fatalf("state publication repeated before restart: %d", ledger.nextEvent)
	}
	firstContents := firstCapture.contents()
	if len(firstContents) != 1 {
		t.Fatalf("expected one attempted response, got %d", len(firstContents))
	}

	secondCapture := &responseCapture{}
	secondJournal := openTestJournal(t, path, 24*time.Hour)
	second := testCommandProcessor(secondJournal, ledger, secondCapture, now.Add(time.Second))
	pending, err := secondJournal.Pending()
	if err != nil || len(pending) != 1 || pending[0].Phase != CommandResponsePending {
		t.Fatalf("response was not durably pending: %#v err=%v", pending, err)
	}
	if err := repairPendingCommands(context.Background(), second, secondJournal); err != nil {
		t.Fatal(err)
	}
	if ledger.nextEvent != 1 {
		t.Fatalf("restart duplicated canonical state: %d mutations", ledger.nextEvent)
	}
	secondContents := secondCapture.contents()
	if len(secondContents) != 1 || secondContents[0] != firstContents[0] {
		t.Fatalf("restart did not publish exact cached response: first=%q second=%q", firstContents, secondContents)
	}
	pending, err = secondJournal.Pending()
	if err != nil || len(pending) != 0 {
		t.Fatalf("repaired command remained pending: %#v err=%v", pending, err)
	}
}

func TestRestartAtEveryCommandPhase(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	path := filepath.Join(t.TempDir(), "commands.json")
	ledger := &memoryLedger{tasks: map[string]*beadspb.Issue{}, queues: map[string][]string{}}
	command := buildJournalCommand(t, "task/create", map[string]any{
		"task_id": "task-1", "title": "created", "repo_addr": "30617:owner:repo",
	}, now, 30)

	received := openTestJournal(t, path, 24*time.Hour)
	if _, created, err := received.Begin(command, command, false, now); err != nil || !created {
		t.Fatalf("persist received phase: created=%v err=%v", created, err)
	}
	capture := &responseCapture{}
	afterReceived := testCommandProcessor(openTestJournal(t, path, 24*time.Hour), ledger, capture, now.Add(time.Second))
	if err := afterReceived.Process(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	if ledger.nextEvent != 1 {
		t.Fatalf("received-phase restart mutation count = %d", ledger.nextEvent)
	}

	afterComplete := testCommandProcessor(openTestJournal(t, path, 24*time.Hour), ledger, capture, now.Add(2*time.Second))
	if err := afterComplete.Process(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	if ledger.nextEvent != 1 {
		t.Fatalf("complete-phase restart mutation count = %d", ledger.nextEvent)
	}
}

func TestHistoricalBackfillCatchesUpExactlyOnce(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	path := filepath.Join(t.TempDir(), "commands.json")
	ledger := &memoryLedger{tasks: map[string]*beadspb.Issue{}, queues: map[string][]string{}}
	capture := &responseCapture{}
	journal := openTestJournal(t, path, 24*time.Hour)
	processor := testCommandProcessor(journal, ledger, capture, now.Add(10*time.Second))

	var events []*gonostr.Event
	for i := 1; i <= 3; i++ {
		events = append(events, buildJournalCommand(t, "task/create", map[string]any{
			"task_id": fmt.Sprintf("task-%d", i), "title": fmt.Sprintf("task %d", i), "repo_addr": "30617:owner:repo",
		}, now.Add(time.Duration(i)*time.Second), byte(40+i)))
	}
	unsortedWithDuplicates := []*gonostr.Event{events[2], events[0], events[1], events[0], events[2]}
	if err := processCommandBackfill(context.Background(), processor, journal, unsortedWithDuplicates); err != nil {
		t.Fatal(err)
	}
	if ledger.nextEvent != 3 {
		t.Fatalf("initial catch-up mutations = %d", ledger.nextEvent)
	}

	restartedJournal := openTestJournal(t, path, 24*time.Hour)
	restarted := testCommandProcessor(restartedJournal, ledger, capture, now.Add(20*time.Second))
	if err := processCommandBackfill(context.Background(), restarted, restartedJournal, unsortedWithDuplicates); err != nil {
		t.Fatal(err)
	}
	if ledger.nextEvent != 3 {
		t.Fatalf("restart replayed historical commands: %d", ledger.nextEvent)
	}
	fourth := buildJournalCommand(t, "task/create", map[string]any{
		"task_id": "task-4", "title": "task 4", "repo_addr": "30617:owner:repo",
	}, now.Add(4*time.Second), 44)
	if err := processCommandBackfill(context.Background(), restarted, restartedJournal, []*gonostr.Event{events[2], fourth}); err != nil {
		t.Fatal(err)
	}
	if ledger.nextEvent != 4 || ledger.tasks["task-4"] == nil {
		t.Fatalf("downtime catch-up did not process exactly one new command: %d", ledger.nextEvent)
	}
}
