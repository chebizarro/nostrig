package taskfabric

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	gonostr "fiatjaf.com/nostr"
	cascontextvm "git.sharegap.net/cascadia/cascadia-go/contextvm"
	beadspb "github.com/chebizarro/nostrig/gen/beads"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
)

type atomicRPCResponse struct {
	Result map[string]any      `json:"result"`
	Error  *cascontextvm.Error `json:"error"`
}

func decodeAtomicResponse(t *testing.T, ev *gonostr.Event) atomicRPCResponse {
	t.Helper()
	var response atomicRPCResponse
	if ev == nil {
		t.Fatal("nil response event")
	}
	if err := json.Unmarshal([]byte(ev.Content), &response); err != nil {
		t.Fatalf("decode response %q: %v", ev.Content, err)
	}
	return response
}

func TestAtomicClaimHundredWayRaceAndWinnerRetry(t *testing.T) {
	ledger := &memoryLedger{
		tasks:  map[string]*beadspb.Issue{"task-1": {Id: "task-1", Title: "claim me", Status: beadspb.Status_STATUS_OPEN}},
		queues: map[string][]string{},
	}
	handler := testHandler(ledger)
	start := make(chan struct{})
	responses := make(chan atomicRPCResponse, 100)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			request, err := nip34.BuildContextVMCommand("task/claim", "server", map[string]string{
				"task_id": "task-1", "claimer": fmt.Sprintf("agent-%03d", i), "base_event_id": "",
			}, time.Unix(1, int64(i)))
			if err != nil {
				responses <- atomicRPCResponse{Error: &cascontextvm.Error{Message: err.Error()}}
				return
			}
			request.ID, request.PubKey = testID(byte(i+1)), testPubKey(1)
			response, err := handler.HandleIntent(context.Background(), request, time.Unix(2, int64(i)))
			if err != nil {
				responses <- atomicRPCResponse{Error: &cascontextvm.Error{Message: err.Error()}}
				return
			}
			var decoded atomicRPCResponse
			if err := json.Unmarshal([]byte(response.Content), &decoded); err != nil {
				responses <- atomicRPCResponse{Error: &cascontextvm.Error{Message: err.Error()}}
				return
			}
			responses <- decoded
		}()
	}
	close(start)
	wg.Wait()
	close(responses)

	successes, conflicts := 0, 0
	var winner, winningEventID string
	for response := range responses {
		switch {
		case response.Error == nil:
			successes++
			winner, _ = response.Result["assignee"].(string)
			winningEventID, _ = response.Result["event_id"].(string)
		case response.Error.Code == conflictErrorCode:
			conflicts++
			var data ConflictError
			if err := json.Unmarshal(response.Error.Data, &data); err != nil {
				t.Fatalf("decode conflict data: %v", err)
			}
			if data.Assignee == "" || data.ActualEventID == "" {
				t.Fatalf("competitor conflict omitted winner: %#v", data)
			}
		default:
			t.Fatalf("unexpected claim response: %#v", response.Error)
		}
	}
	if successes != 1 || conflicts != 99 {
		t.Fatalf("successes=%d conflicts=%d; want 1/99", successes, conflicts)
	}
	if winner == "" || winningEventID == "" {
		t.Fatalf("winner response missing assignee/event: winner=%q event=%q", winner, winningEventID)
	}

	retry, _ := nip34.BuildContextVMCommand("task/claim", "server", map[string]string{
		"task_id": "task-1", "claimer": winner, "base_event_id": "",
	}, time.Unix(4, 0))
	retry.ID, retry.PubKey = testID(120), testPubKey(1)
	retryEvent, err := handler.HandleIntent(context.Background(), retry, time.Unix(5, 0))
	if err != nil {
		t.Fatal(err)
	}
	retryResponse := decodeAtomicResponse(t, retryEvent)
	if retryResponse.Error != nil || retryResponse.Result["event_id"] != winningEventID {
		t.Fatalf("winner retry did not return original success: %s", retryEvent.Content)
	}
}

func TestStaleTaskRevisionReturnsStructuredConflict(t *testing.T) {
	ledger := &memoryLedger{
		tasks:        map[string]*beadspb.Issue{"task-1": {Id: "task-1", Title: "base", Status: beadspb.Status_STATUS_OPEN}},
		taskEventIDs: map[string]string{"task-1": "base-event"},
		queues:       map[string][]string{},
	}
	handler := testHandler(ledger)
	first, _ := nip34.BuildContextVMCommand("task/update", "server", map[string]string{
		"task_id": "task-1", "base_event_id": "base-event", "title": "first",
	}, time.Unix(1, 0))
	first.ID, first.PubKey = testID(1), testPubKey(1)
	firstResponse, err := handler.HandleIntent(context.Background(), first, time.Unix(2, 0))
	if err != nil {
		t.Fatal(err)
	}
	firstBody := decodeAtomicResponse(t, firstResponse)
	if firstBody.Error != nil {
		t.Fatalf("first update failed: %s", firstResponse.Content)
	}
	winnerEventID, _ := firstBody.Result["event_id"].(string)

	stale, _ := nip34.BuildContextVMCommand("task/update", "server", map[string]string{
		"task_id": "task-1", "base_event_id": "base-event", "title": "stale",
	}, time.Unix(3, 0))
	stale.ID, stale.PubKey = testID(2), testPubKey(1)
	staleEvent, err := handler.HandleIntent(context.Background(), stale, time.Unix(4, 0))
	if err != nil {
		t.Fatal(err)
	}
	staleBody := decodeAtomicResponse(t, staleEvent)
	if staleBody.Error == nil || staleBody.Error.Code != conflictErrorCode {
		t.Fatalf("expected explicit conflict, got %s", staleEvent.Content)
	}
	var data ConflictError
	if err := json.Unmarshal(staleBody.Error.Data, &data); err != nil {
		t.Fatal(err)
	}
	if data.Reason != "stale_revision" || data.ActualEventID != winnerEventID || data.ExpectedEventID != "base-event" {
		t.Fatalf("unexpected conflict data: %#v", data)
	}
	if got := ledger.tasks["task-1"].Title; got != "first" {
		t.Fatalf("stale update overwrote winner: %q", got)
	}
}

func TestQueueReservationHundredWayAcrossHandlers(t *testing.T) {
	const repo = "30617:owner:repo"
	ledger := &memoryLedger{
		tasks: map[string]*beadspb.Issue{},
		queues: map[string][]string{
			repo + "|backlog": {"task-1"},
		},
		queueRecords: map[string]*QueueRecord{
			repo + "|backlog": {TaskIDs: []string{"task-1"}, EventID: "queue-base"},
		},
	}
	handlers := make([]*Handler, 100)
	requests := make([]*gonostr.Event, 100)
	for i := range handlers {
		caller := testPubKey(byte(i + 1)).Hex()
		handlers[i] = &Handler{
			Ledger: ledger,
			ACL: map[string]CallerPolicy{
				caller: {Roles: []Role{RoleWorker}, Repositories: []string{repo}, WorkerID: fmt.Sprintf("worker-%03d", i)},
			},
			Audit: discardAudit{},
		}
		request, _ := nip34.BuildContextVMCommand("queue/dequeue", "server", map[string]string{
			"repo_addr": repo, "queue": "backlog", "base_event_id": "queue-base",
		}, time.Unix(1, int64(i)))
		request.ID, request.PubKey = testID(byte(i+1)), testPubKey(byte(i+1))
		requests[i] = request
	}

	start := make(chan struct{})
	responses := make(chan atomicRPCResponse, 100)
	var wg sync.WaitGroup
	for i := range handlers {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			response, err := handlers[i].HandleIntent(context.Background(), requests[i], time.Unix(2, int64(i)))
			if err != nil {
				responses <- atomicRPCResponse{Error: &cascontextvm.Error{Message: err.Error()}}
				return
			}
			var decoded atomicRPCResponse
			if err := json.Unmarshal([]byte(response.Content), &decoded); err != nil {
				responses <- atomicRPCResponse{Error: &cascontextvm.Error{Message: err.Error()}}
				return
			}
			responses <- decoded
		}()
	}
	close(start)
	wg.Wait()
	close(responses)

	successes, conflicts := 0, 0
	var winningWorker, winningLeaseID string
	for response := range responses {
		if response.Error == nil {
			successes++
			winningLeaseID, _ = response.Result["lease_id"].(string)
			continue
		}
		if response.Error.Code != conflictErrorCode {
			t.Fatalf("unexpected reservation error: %#v", response.Error)
		}
		conflicts++
	}
	if successes != 1 || conflicts != 99 {
		t.Fatalf("reservation successes=%d conflicts=%d; want 1/99", successes, conflicts)
	}
	record := ledger.queueRecords[repo+"|backlog"]
	active := activeQueueLeases(record, time.Unix(3, 0))
	if len(active) != 1 || active[0].TaskID != "task-1" || active[0].LeaseID != winningLeaseID {
		t.Fatalf("queue item was not singly reserved: %#v", active)
	}
	winningWorker = active[0].Worker

	var winnerIndex int
	for i := range handlers {
		if handlers[i].ACL[testPubKey(byte(i+1)).Hex()].WorkerID == winningWorker {
			winnerIndex = i
			break
		}
	}
	retry := requests[winnerIndex]
	retryCopy := *retry
	retryCopy.ID = testID(125)
	retryEvent, err := handlers[winnerIndex].HandleIntent(context.Background(), &retryCopy, time.Unix(3, 0))
	if err != nil {
		t.Fatal(err)
	}
	retryBody := decodeAtomicResponse(t, retryEvent)
	if retryBody.Error != nil || retryBody.Result["lease_id"] != winningLeaseID {
		t.Fatalf("winner reservation retry changed result: %s", retryEvent.Content)
	}
}

func TestEventAfterUsesEventIDForEqualTimestamp(t *testing.T) {
	at := gonostr.Timestamp(time.Unix(10, 0).Unix())
	first := &gonostr.Event{ID: testID(1), CreatedAt: at}
	second := &gonostr.Event{ID: testID(2), CreatedAt: at}
	if !eventAfter(second, first) || eventAfter(first, second) {
		t.Fatal("equal-timestamp events were not ordered by event ID")
	}
}
