package taskfabric

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	gonostr "fiatjaf.com/nostr"
	beadspb "github.com/chebizarro/nostrig/gen/beads"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestExportFromTaskStateEventsLatestWins(t *testing.T) {
	older := time.Unix(100, 0).UTC()
	newer := time.Unix(200, 0).UTC()
	oldIssue := &beadspb.Issue{Id: "repo-task1", Title: "old", Status: beadspb.Status_STATUS_OPEN, Updated: timestamppb.New(older)}
	newIssue := &beadspb.Issue{Id: "repo-task1", Title: "new", Status: beadspb.Status_STATUS_IN_PROGRESS, Updated: timestamppb.New(newer)}
	author := testPubKey(20).Hex()
	oldEvent, err := nip34.BuildTaskStateEvent(oldIssue, author, older)
	if err != nil {
		t.Fatal(err)
	}
	newEvent, err := nip34.BuildTaskStateEvent(newIssue, author, newer)
	if err != nil {
		t.Fatal(err)
	}
	oldEvent.ID = testID(20)
	oldEvent.PubKey = testPubKey(20)
	newEvent.ID = testID(21)
	newEvent.PubKey = testPubKey(20)

	export, err := ExportFromTaskStateEvents([]*gonostr.Event{oldEvent, newEvent})
	if err != nil {
		t.Fatal(err)
	}
	if len(export.Issues) != 1 {
		t.Fatalf("issues=%d", len(export.Issues))
	}
	issue := export.Issues[0]
	if issue.Title != "new" || issue.Status != beadspb.Status_STATUS_IN_PROGRESS {
		t.Fatalf("latest issue not selected: %#v", issue)
	}
	if issue.Metadata.Custom["nostr.id"] != testID(21).Hex() {
		t.Fatalf("metadata not updated: %#v", issue.Metadata.Custom)
	}
}

func TestFetchTaskStateEventsRequiresBoundedSelector(t *testing.T) {
	_, err := FetchTaskStateEvents(context.Background(), nil, SyncOptions{Relays: []string{"wss://relay.example"}})
	if err == nil {
		t.Fatal("expected bounded selector error")
	}
}

func TestRelayAuthoritativeMergeReportsDriftButRendersRelayAndDeletesLocalOnly(t *testing.T) {
	relayIssue := &beadspb.Issue{
		Id: "repo-task1", Title: "relay", Status: beadspb.Status_STATUS_IN_PROGRESS,
		ExecutionAttempts: []*beadspb.ExecutionAttempt{{Id: "attempt-1", Agent: "worker", Status: "running"}},
		Metadata:          &beadspb.Metadata{Custom: map[string]string{"nostr.id": "relay-event"}},
	}
	local := []*TaskSnapshot{
		{ID: "repo-task1", Title: "local edit", Status: "open"},
		{ID: "local-only", Title: "must disappear", Status: "open"},
	}
	merged, err := MergeTaskStateWithOptions(&beadspb.Export{Issues: []*beadspb.Issue{relayIssue}}, local, nil, MergeOptions{RelayAuthoritative: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(merged.Conflicts) != 1 || merged.Conflicts[0].Conflict.Reason != "local_projection_drift" {
		t.Fatalf("unexpected conflicts: %#v", merged.Conflicts)
	}
	if len(merged.Export.Issues) != 1 || merged.Export.Issues[0].Title != "relay" || len(merged.Export.Issues[0].ExecutionAttempts) != 1 {
		t.Fatalf("relay projection was not authoritative/full-fidelity: %#v", merged.Export.Issues)
	}
	for _, issue := range merged.Export.Issues {
		if issue.Id == "local-only" {
			t.Fatal("local-only task leaked into authoritative projection")
		}
	}
}

func TestRelayAuthoritativeExactTaskMergeRejectsUnknownOutOfScopeLocalState(t *testing.T) {
	selectedIssue := &beadspb.Issue{Id: "task-1", Title: "selected", Status: beadspb.Status_STATUS_OPEN}
	_, err := MergeTaskStateWithOptions(
		&beadspb.Export{Issues: []*beadspb.Issue{selectedIssue}},
		[]*TaskSnapshot{{ID: "unqueried", Title: "unknown local state", Status: "open"}},
		nil,
		MergeOptions{RelayAuthoritative: true, AuthoritativeTaskIDs: []string{"task-1"}},
	)
	if err == nil {
		t.Fatal("exact-task merge rewrote an unqueried local record without prior relay provenance")
	}
}

func TestRelayAuthoritativeExactTaskMergeRetainsOnlyPreviouslyObservedOutOfScopeRelayState(t *testing.T) {
	cachedIssue := &beadspb.Issue{
		Id: "task-2", Title: "cached relay task", Status: beadspb.Status_STATUS_OPEN,
		Metadata: &beadspb.Metadata{Custom: map[string]string{"nostr.id": "relay-task-2"}},
	}
	cached := SnapshotFromIssue(cachedIssue)
	previous := []*CacheRecord{{
		ID: "task-2", Resolved: cached, Local: cached, Relay: cached,
		LocalRevision: SnapshotRevision(cached), RelayEventID: "relay-task-2", Resolution: ResolutionClean,
	}}
	selectedIssue := &beadspb.Issue{Id: "task-1", Title: "selected", Status: beadspb.Status_STATUS_OPEN}
	merged, err := MergeTaskStateWithOptions(
		&beadspb.Export{Issues: []*beadspb.Issue{selectedIssue}},
		[]*TaskSnapshot{cached},
		previous,
		MergeOptions{RelayAuthoritative: true, AuthoritativeTaskIDs: []string{"task-1"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(merged.Export.Issues) != 2 {
		t.Fatalf("partial sync discarded out-of-scope relay projection: %#v", merged.Export.Issues)
	}
}

type taskQueryFake struct {
	events                    []*gonostr.Event
	err                       error
	filters                   []gonostr.Filter
	subscriptions             []chan gonostr.RelayEvent
	fetchedAfterSubscriptions bool
	fetchCalls                int
}

func (f *taskQueryFake) FetchManyPaginated(ctx context.Context, relays []string, filters []gonostr.Filter, opts nip34.PaginationOptions) ([]*gonostr.Event, error) {
	f.filters = append([]gonostr.Filter(nil), filters...)
	f.fetchedAfterSubscriptions = len(f.subscriptions) == len(filters)
	f.fetchCalls++
	return f.events, f.err
}

func (f *taskQueryFake) Subscribe(ctx context.Context, relays []string, filter gonostr.Filter) (<-chan gonostr.RelayEvent, error) {
	ch := make(chan gonostr.RelayEvent, 8)
	f.subscriptions = append(f.subscriptions, ch)
	return ch, nil
}

func TestListTaskStateReturnsMoreThanFiveHundredTasks(t *testing.T) {
	author := testPubKey(20).Hex()
	repo := "30617:owner:large-repo"
	events := make([]*gonostr.Event, 0, 601)
	for i := 0; i < 601; i++ {
		issue := &beadspb.Issue{
			Id: fmt.Sprintf("task-%04d", i), Title: fmt.Sprintf("Task %d", i),
			Status:   beadspb.Status_STATUS_OPEN,
			Metadata: &beadspb.Metadata{Custom: map[string]string{"nip34.repo_addr": repo}},
		}
		ev, err := nip34.BuildTaskStateEvent(issue, author, time.Unix(int64(10_000-i), 0))
		if err != nil {
			t.Fatal(err)
		}
		ev.ID, ev.PubKey = queryTestID(i+1), testPubKey(20)
		events = append(events, ev)
	}
	source := &taskQueryFake{events: events}
	export, err := ListTaskState(context.Background(), source, TaskListOptions{
		Relays: []string{"wss://relay.example"}, RepoAddr: repo, Authors: []string{author},
		Pagination: nip34.PaginationOptions{PageSize: 500, MaxEvents: 1000},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(export.Issues) != 601 {
		t.Fatalf("issues=%d, want 601", len(export.Issues))
	}
	if len(source.filters) != 2 {
		t.Fatalf("filters=%d, want 2", len(source.filters))
	}
	for i, filter := range source.filters {
		if len(filter.Authors) != 1 || filter.Authors[0].Hex() != author {
			t.Fatalf("filter %d missing canonical author: %#v", i, filter)
		}
		if got := filter.Tags["a"]; len(got) != 1 || got[0] != repo {
			t.Fatalf("filter %d missing repository selector: %#v", i, filter.Tags)
		}
	}
}

func TestListTaskStatePropagatesTruncation(t *testing.T) {
	source := &taskQueryFake{err: &nip34.QueryTruncatedError{Reason: nip34.TruncatedByEventLimit}}
	export, err := ListTaskState(context.Background(), source, TaskListOptions{
		Relays: []string{"wss://relay.example"}, RepoAddr: "30617:owner:repo",
		Authors: []string{testPubKey(20).Hex()},
	})
	if export != nil || !nip34.IsQueryTruncated(err) {
		t.Fatalf("export=%#v err=%v", export, err)
	}
}

func TestWatchTaskStateSubscribesBeforeSnapshotAndStreamsChanges(t *testing.T) {
	author := testPubKey(20).Hex()
	repo := "30617:owner:repo"
	oldIssue := &beadspb.Issue{Id: "task-1", Title: "old", Status: beadspb.Status_STATUS_OPEN, Metadata: &beadspb.Metadata{Custom: map[string]string{"nip34.repo_addr": repo}}}
	oldState, _ := nip34.BuildTaskStateEvent(oldIssue, author, time.Unix(10, 0))
	oldState.ID, oldState.PubKey = queryTestID(1), testPubKey(20)
	source := &taskQueryFake{events: []*gonostr.Event{oldState}}

	watch, err := WatchTaskState(context.Background(), source, TaskWatchOptions{
		Relays: []string{"wss://relay.example"}, RepoAddr: repo, Authors: []string{author},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close()
	if !source.fetchedAfterSubscriptions {
		t.Fatal("snapshot fetch started before both live subscriptions")
	}
	if source.fetchCalls != 2 {
		t.Fatalf("snapshot fetch calls=%d, want initial plus reconciliation", source.fetchCalls)
	}
	if len(watch.Snapshot.Issues) != 1 || watch.Snapshot.Issues[0].Title != "old" {
		t.Fatalf("snapshot=%#v", watch.Snapshot.Issues)
	}

	newIssue := &beadspb.Issue{Id: "task-1", Title: "new", Status: beadspb.Status_STATUS_IN_PROGRESS, Metadata: &beadspb.Metadata{Custom: map[string]string{"nip34.repo_addr": repo}}}
	newState, _ := nip34.BuildTaskStateEvent(newIssue, author, time.Unix(20, 0))
	newState.ID, newState.PubKey = queryTestID(2), testPubKey(20)
	source.subscriptions[0] <- gonostr.RelayEvent{Event: *newState}
	change := receiveTaskChange(t, watch.Changes)
	if change.Kind != TaskChangeUpsert || change.Issue == nil || change.Issue.Title != "new" {
		t.Fatalf("upsert=%#v", change)
	}

	// A duplicate already represented by the projection must not generate a
	// second consumer update.
	source.subscriptions[0] <- gonostr.RelayEvent{Event: *newState}
	select {
	case duplicate := <-watch.Changes:
		t.Fatalf("duplicate change=%#v", duplicate)
	case <-time.After(50 * time.Millisecond):
	}

	tombstone, err := nip34.BuildTaskTombstone(newState, repo, author, time.Unix(30, 0))
	if err != nil {
		t.Fatal(err)
	}
	tombstone.ID, tombstone.PubKey = queryTestID(3), testPubKey(20)
	source.subscriptions[1] <- gonostr.RelayEvent{Event: *tombstone}
	change = receiveTaskChange(t, watch.Changes)
	if change.Kind != TaskChangeDelete || change.Issue != nil || change.TaskID != "task-1" {
		t.Fatalf("delete=%#v", change)
	}
}

func receiveTaskChange(t *testing.T, changes <-chan TaskChange) TaskChange {
	t.Helper()
	select {
	case change, ok := <-changes:
		if !ok {
			t.Fatal("task changes closed")
		}
		return change
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for task change")
		return TaskChange{}
	}
}

func queryTestID(n int) gonostr.ID {
	var id gonostr.ID
	binary.BigEndian.PutUint64(id[24:], uint64(n))
	return id
}

type statusCapturePublisher struct{ events []*gonostr.Event }

func (p *statusCapturePublisher) Publish(ctx context.Context, relays []string, signer nip34.Signer, events []*gonostr.Event) error {
	p.events = append(p.events, events...)
	return nil
}
