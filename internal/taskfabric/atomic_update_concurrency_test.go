package taskfabric

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	cascontextvm "git.sharegap.net/cascadia/cascadia-go/contextvm"
	beadspb "github.com/chebizarro/nostrig/gen/beads"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
)

func TestAtomicUpdateHundredWayRace(t *testing.T) {
	const competitors = 100
	ledger := &memoryLedger{
		tasks: map[string]*beadspb.Issue{
			"task-1": {Id: "task-1", Title: "base", Status: beadspb.Status_STATUS_OPEN},
		},
		taskEventIDs: map[string]string{"task-1": "base-event"},
		queues:       map[string][]string{},
	}
	handler := testHandler(ledger)

	type result struct {
		title string
		body  atomicRPCResponse
	}
	results := make(chan result, competitors)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < competitors; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			title := fmt.Sprintf("winner-%03d", i)
			request, err := nip34.BuildContextVMCommand("task/update", "server", map[string]string{
				"task_id": "task-1", "base_event_id": "base-event", "title": title,
			}, time.Unix(1, int64(i)))
			if err != nil {
				results <- result{title: title, body: atomicRPCResponse{Error: &cascontextvm.Error{Message: err.Error()}}}
				return
			}
			request.ID, request.PubKey = testID(byte(i+1)), testPubKey(1)
			response, err := handler.HandleIntent(context.Background(), request, time.Unix(2, int64(i)))
			if err != nil {
				results <- result{title: title, body: atomicRPCResponse{Error: &cascontextvm.Error{Message: err.Error()}}}
				return
			}
			var body atomicRPCResponse
			if err := json.Unmarshal([]byte(response.Content), &body); err != nil {
				results <- result{title: title, body: atomicRPCResponse{Error: &cascontextvm.Error{Message: err.Error()}}}
				return
			}
			results <- result{title: title, body: body}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	successes, conflicts := 0, 0
	var winningTitle, winningEventID string
	var conflictData []ConflictError
	for got := range results {
		if got.body.Error == nil {
			successes++
			winningTitle = got.title
			winningEventID, _ = got.body.Result["event_id"].(string)
			continue
		}
		if got.body.Error.Code != ConflictErrorCode {
			t.Fatalf("unexpected update error: %#v", got.body.Error)
		}
		conflicts++
		var data ConflictError
		if err := json.Unmarshal(got.body.Error.Data, &data); err != nil {
			t.Fatalf("decode conflict data: %v", err)
		}
		if data.Reason != "stale_revision" || data.ExpectedEventID != "base-event" {
			t.Fatalf("unexpected conflict: %#v", data)
		}
		conflictData = append(conflictData, data)
	}
	if successes != 1 || conflicts != competitors-1 {
		t.Fatalf("successes=%d conflicts=%d; want 1/%d", successes, conflicts, competitors-1)
	}
	if winningEventID == "" {
		t.Fatal("winning update omitted event_id")
	}
	for _, data := range conflictData {
		if data.ActualEventID != winningEventID {
			t.Fatalf("conflict actual event=%q, want winner %q", data.ActualEventID, winningEventID)
		}
	}
	final, err := ledger.GetTask(context.Background(), "task-1")
	if err != nil {
		t.Fatal(err)
	}
	if final.Title != winningTitle {
		t.Fatalf("final title=%q, want winning title %q", final.Title, winningTitle)
	}
}
