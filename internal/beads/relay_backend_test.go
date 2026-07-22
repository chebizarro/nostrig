package beads

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
	"time"

	gonostr "fiatjaf.com/nostr"
	pb "github.com/chebizarro/nostrig/gen/beads"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeFetcher struct {
	events     []*gonostr.Event
	tombstones []*gonostr.Event
	filter     gonostr.Filter
	relays     []string
}

func (f *fakeFetcher) Fetch(ctx context.Context, relays []string, filter gonostr.Filter) ([]*gonostr.Event, error) {
	f.relays = append([]string(nil), relays...)
	if len(filter.Kinds) == 1 && int(filter.Kinds[0]) == 5 {
		return f.tombstones, nil
	}
	f.filter = filter
	return f.events, nil
}

type fakePaginatedFetcher struct {
	fakeFetcher
	filters    []gonostr.Filter
	pagination nip34.PaginationOptions
}

func (f *fakePaginatedFetcher) FetchManyPaginated(ctx context.Context, relays []string, filters []gonostr.Filter, opts nip34.PaginationOptions) ([]*gonostr.Event, error) {
	f.filters = append([]gonostr.Filter(nil), filters...)
	f.pagination = opts
	return f.events, nil
}

type fakePublisher struct{ events []*gonostr.Event }

func (p *fakePublisher) Publish(ctx context.Context, relays []string, signer nip34.Signer, events []*gonostr.Event) error {
	p.events = append(p.events, events...)
	return nil
}

type noopSigner struct{}

func (noopSigner) SignEvent(ctx context.Context, ev *gonostr.Event) error { return nil }

func backendTestID(n byte) gonostr.ID {
	var id gonostr.ID
	id[31] = n
	return id
}

func backendTestPubKey(n byte) gonostr.PubKey {
	var pk gonostr.PubKey
	pk[31] = n
	return pk
}

func TestRelayBackendLoadExportFetchesBoundedTaskStateAndLatestWins(t *testing.T) {
	oldTime := time.Unix(10, 0).UTC()
	newTime := time.Unix(20, 0).UTC()
	author := backendTestPubKey(1).Hex()
	repoMeta := &pb.Metadata{Custom: map[string]string{"nip34.repo_addr": "30617:owner:repo"}}
	oldEvent, err := nip34.BuildTaskStateEvent(&pb.Issue{Id: "task-1", Title: "old", Status: pb.Status_STATUS_OPEN, Updated: timestamppb.New(oldTime), Metadata: repoMeta}, author, oldTime)
	if err != nil {
		t.Fatal(err)
	}
	newEvent, err := nip34.BuildTaskStateEvent(&pb.Issue{Id: "task-1", Title: "new", Status: pb.Status_STATUS_IN_PROGRESS, Updated: timestamppb.New(newTime), Metadata: repoMeta}, author, newTime)
	if err != nil {
		t.Fatal(err)
	}
	oldEvent.ID, oldEvent.PubKey = backendTestID(1), backendTestPubKey(1)
	newEvent.ID, newEvent.PubKey = backendTestID(2), backendTestPubKey(1)
	fetcher := &fakeFetcher{events: []*gonostr.Event{oldEvent, newEvent}}
	backend := NewRelayBackend(RelayBackendOptions{Relays: []string{"wss://relay.example"}, RepoAddr: "30617:owner:repo", TaskIDs: []string{"task-1"}, Authors: []string{author}, Fetcher: fetcher})

	export, err := backend.LoadExport(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(export.Issues) != 1 || export.Issues[0].Title != "new" {
		t.Fatalf("latest issue not selected: %#v", export.Issues)
	}
	if got := fetcher.filter.Tags["a"]; len(got) != 1 || got[0] != "30617:owner:repo" {
		t.Fatalf("repo tag filter=%#v", fetcher.filter.Tags)
	}
	if got := fetcher.filter.Tags["d"]; len(got) != 1 || got[0] != "task:task-1" {
		t.Fatalf("task tag filter=%#v", fetcher.filter.Tags)
	}
	if export.Issues[0].Metadata.Custom["nostr.id"] != backendTestID(2).Hex() {
		t.Fatalf("nostr provenance missing: %#v", export.Issues[0].Metadata.Custom)
	}
}

func TestRelayBackendRejectsReturnedStateFromUnknownAuthor(t *testing.T) {
	trusted := backendTestPubKey(1).Hex()
	attacker := backendTestPubKey(2).Hex()
	ev, err := nip34.BuildTaskStateEvent(&pb.Issue{Id: "task-1", Title: "forged", Status: pb.Status_STATUS_OPEN, Metadata: &pb.Metadata{Custom: map[string]string{"nip34.repo_addr": "30617:owner:repo"}}}, attacker, time.Unix(20, 0))
	if err != nil {
		t.Fatal(err)
	}
	ev.ID, ev.PubKey = backendTestID(9), backendTestPubKey(2)
	backend := NewRelayBackend(RelayBackendOptions{
		Relays: []string{"wss://relay.example"}, RepoAddr: "30617:owner:repo",
		TaskIDs: []string{"task-1"}, Authors: []string{trusted}, Fetcher: &fakeFetcher{events: []*gonostr.Event{ev}},
	})
	if _, err := backend.LoadExport(context.Background()); err == nil {
		t.Fatal("expected unknown canonical author rejection")
	}
}

func TestExportEqualTimestampUsesEventIDTieBreak(t *testing.T) {
	author := backendTestPubKey(1).Hex()
	at := time.Unix(20, 0)
	first, _ := nip34.BuildTaskStateEvent(&pb.Issue{Id: "task-1", Title: "first", Status: pb.Status_STATUS_OPEN}, author, at)
	second, _ := nip34.BuildTaskStateEvent(&pb.Issue{Id: "task-1", Title: "second", Status: pb.Status_STATUS_OPEN}, author, at)
	first.ID, first.PubKey = backendTestID(1), backendTestPubKey(1)
	second.ID, second.PubKey = backendTestID(2), backendTestPubKey(1)
	export, err := ExportFromTaskStateEvents([]*gonostr.Event{second, first})
	if err != nil {
		t.Fatal(err)
	}
	if len(export.Issues) != 1 || export.Issues[0].Title != "second" {
		t.Fatalf("equal timestamp selection was input-order dependent: %#v", export.Issues)
	}
}

func TestExportPrefersV2OverNewerV1ForSameCoordinate(t *testing.T) {
	author := backendTestPubKey(1).Hex()
	repo := "30617:owner:repo"
	v2, err := nip34.BuildTaskStateEvent(&pb.Issue{Id: "task-1", Title: "v2", Owner: "Stew", Status: pb.Status_STATUS_OPEN, Repository: repo}, author, time.Unix(10, 0))
	if err != nil {
		t.Fatal(err)
	}
	v1, err := nip34.BuildTaskStateEventV1(&pb.Issue{Id: "task-1", Title: "newer but lossy", Status: pb.Status_STATUS_OPEN, Metadata: &pb.Metadata{Custom: map[string]string{"nip34.repo_addr": repo}}}, author, time.Unix(20, 0))
	if err != nil {
		t.Fatal(err)
	}
	v2.ID, v2.PubKey = backendTestID(1), backendTestPubKey(1)
	v1.ID, v1.PubKey = backendTestID(2), backendTestPubKey(1)
	export, err := ExportFromTaskStateEvents([]*gonostr.Event{v1, v2})
	if err != nil {
		t.Fatal(err)
	}
	if len(export.Issues) != 1 || export.Issues[0].Title != "v2" || export.Issues[0].Owner != "Stew" {
		t.Fatalf("v1 downgraded v2 state: %#v", export.Issues)
	}
}

func TestCanonicalTombstoneSuppressesStateUntilLaterRecreation(t *testing.T) {
	author := backendTestPubKey(1).Hex()
	repo := "30617:owner:repo"
	issue := &pb.Issue{Id: "task-1", Title: "old", Status: pb.Status_STATUS_OPEN, Metadata: &pb.Metadata{Custom: map[string]string{"nip34.repo_addr": repo}}}
	state, _ := nip34.BuildTaskStateEvent(issue, author, time.Unix(10, 0))
	state.ID, state.PubKey = backendTestID(1), backendTestPubKey(1)
	tombstone, err := nip34.BuildTaskTombstone(state, repo, author, time.Unix(20, 0))
	if err != nil {
		t.Fatal(err)
	}
	tombstone.ID, tombstone.PubKey = backendTestID(2), backendTestPubKey(1)
	export, err := ExportFromTaskStateEvents([]*gonostr.Event{state, tombstone})
	if err != nil {
		t.Fatal(err)
	}
	if len(export.Issues) != 0 {
		t.Fatalf("tombstoned task remained visible: %#v", export.Issues)
	}
	issue.Title = "recreated"
	recreated, _ := nip34.BuildTaskStateEvent(issue, author, time.Unix(30, 0))
	recreated.ID, recreated.PubKey = backendTestID(3), backendTestPubKey(1)
	export, err = ExportFromTaskStateEvents([]*gonostr.Event{tombstone, state, recreated})
	if err != nil {
		t.Fatal(err)
	}
	if len(export.Issues) != 1 || export.Issues[0].Title != "recreated" {
		t.Fatalf("later recreation was not selected: %#v", export.Issues)
	}
}

func TestRelayBackendLoadExportReturnsMoreThanFiveHundredTasks(t *testing.T) {
	author := backendTestPubKey(1).Hex()
	repo := "30617:owner:large-repo"
	events := make([]*gonostr.Event, 0, 601)
	for i := 0; i < 601; i++ {
		at := time.Unix(int64(10_000-i), 0)
		issue := &pb.Issue{
			Id: fmt.Sprintf("task-%04d", i), Title: fmt.Sprintf("Task %d", i),
			Status:   pb.Status_STATUS_OPEN,
			Metadata: &pb.Metadata{Custom: map[string]string{"nip34.repo_addr": repo}},
		}
		ev, err := nip34.BuildTaskStateEvent(issue, author, at)
		if err != nil {
			t.Fatal(err)
		}
		ev.ID, ev.PubKey = backendNumberedID(i+1), backendTestPubKey(1)
		events = append(events, ev)
	}
	fetcher := &fakePaginatedFetcher{fakeFetcher: fakeFetcher{events: events}}
	backend := NewRelayBackend(RelayBackendOptions{
		Relays: []string{"wss://relay.example"}, RepoAddr: repo, Authors: []string{author},
		Fetcher: fetcher, Pagination: nip34.PaginationOptions{PageSize: 500, MaxEvents: 1000},
	})
	export, err := backend.LoadExport(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(export.Issues) != 601 {
		t.Fatalf("issues=%d, want 601", len(export.Issues))
	}
	if len(fetcher.filters) != 2 {
		t.Fatalf("filters=%d, want state+tombstone", len(fetcher.filters))
	}
	for i, filter := range fetcher.filters {
		if len(filter.Authors) != 1 || filter.Authors[0].Hex() != author {
			t.Fatalf("filter %d missing canonical author: %#v", i, filter)
		}
		if got := filter.Tags["a"]; len(got) == 0 || got[len(got)-1] != repo {
			t.Fatalf("filter %d missing repository selector: %#v", i, filter.Tags)
		}
	}
}

func TestCanonicalTaskQueryBuildsEfficientExactSelectors(t *testing.T) {
	author := backendTestPubKey(1).Hex()
	repo := "30617:owner:repo"
	query, err := NewCanonicalTaskQuery(repo, []string{"task-1"}, []string{author}, 100)
	if err != nil {
		t.Fatal(err)
	}
	filters := query.Filters()
	if len(filters) != 2 {
		t.Fatalf("filters=%d", len(filters))
	}
	state, tombstone := filters[0], filters[1]
	if got := state.Tags["a"]; len(got) != 1 || got[0] != repo {
		t.Fatalf("state repo selector=%#v", got)
	}
	if got := state.Tags["d"]; len(got) != 1 || got[0] != "task:task-1" {
		t.Fatalf("state exact selector=%#v", got)
	}
	wantCoordinate := nip34.Address(nip34.KindCanonicalState, author, "task:task-1")
	if got := tombstone.Tags["a"]; len(got) != 1 || got[0] != wantCoordinate {
		t.Fatalf("tombstone exact selector=%#v want %q", got, wantCoordinate)
	}
	for i, filter := range filters {
		if len(filter.Authors) != 1 || filter.Authors[0].Hex() != author {
			t.Fatalf("filter %d authors=%#v", i, filter.Authors)
		}
	}
}

func TestRelayBackendLegacyFetcherCannotSilentlySaturateLimit(t *testing.T) {
	author := backendTestPubKey(1).Hex()
	repo := "30617:owner:repo"
	events := make([]*gonostr.Event, 0, 2)
	for i := 0; i < 2; i++ {
		ev, err := nip34.BuildTaskStateEvent(&pb.Issue{
			Id: fmt.Sprintf("task-%d", i), Title: "task", Status: pb.Status_STATUS_OPEN,
			Metadata: &pb.Metadata{Custom: map[string]string{"nip34.repo_addr": repo}},
		}, author, time.Unix(int64(i+1), 0))
		if err != nil {
			t.Fatal(err)
		}
		ev.ID, ev.PubKey = backendNumberedID(i+1), backendTestPubKey(1)
		events = append(events, ev)
	}
	backend := NewRelayBackend(RelayBackendOptions{
		Relays: []string{"wss://relay.example"}, RepoAddr: repo, Authors: []string{author},
		Limit: 2, Fetcher: &fakeFetcher{events: events},
	})
	_, err := backend.LoadExport(context.Background())
	var truncated *nip34.QueryTruncatedError
	if !errors.As(err, &truncated) || truncated.Reason != nip34.TruncatedByLegacyFetcherLimit {
		t.Fatalf("expected explicit legacy fetcher truncation, got %v", err)
	}
}

func TestTaskStateProjectionAppliesDeleteAndRecreationIncrementally(t *testing.T) {
	author := backendTestPubKey(1).Hex()
	repo := "30617:owner:repo"
	issue := &pb.Issue{Id: "task-1", Title: "first", Status: pb.Status_STATUS_OPEN, Metadata: &pb.Metadata{Custom: map[string]string{"nip34.repo_addr": repo}}}
	state, _ := nip34.BuildTaskStateEvent(issue, author, time.Unix(10, 0))
	state.ID, state.PubKey = backendNumberedID(1), backendTestPubKey(1)
	projection := NewTaskStateProjection()
	if id, changed, err := projection.Apply(state); err != nil || id != "task-1" || !changed {
		t.Fatalf("apply state id=%q changed=%v err=%v", id, changed, err)
	}
	if _, changed, err := projection.Apply(state); err != nil || changed {
		t.Fatalf("duplicate state changed=%v err=%v", changed, err)
	}
	tombstone, _ := nip34.BuildTaskTombstone(state, repo, author, time.Unix(20, 0))
	tombstone.ID, tombstone.PubKey = backendNumberedID(2), backendTestPubKey(1)
	if _, changed, err := projection.Apply(tombstone); err != nil || !changed || projection.Issue("task-1") != nil {
		t.Fatalf("tombstone changed=%v issue=%#v err=%v", changed, projection.Issue("task-1"), err)
	}
	issue.Title = "recreated"
	recreated, _ := nip34.BuildTaskStateEvent(issue, author, time.Unix(30, 0))
	recreated.ID, recreated.PubKey = backendNumberedID(3), backendTestPubKey(1)
	if _, changed, err := projection.Apply(recreated); err != nil || !changed {
		t.Fatalf("recreate changed=%v err=%v", changed, err)
	}
	if got := projection.Issue("task-1"); got == nil || got.Title != "recreated" {
		t.Fatalf("recreated issue=%#v", got)
	}
}

func backendNumberedID(n int) gonostr.ID {
	var id gonostr.ID
	binary.BigEndian.PutUint64(id[24:], uint64(n))
	return id
}

func TestRelayBackendPutIssuePublishesAppendOnlyTaskState(t *testing.T) {

	pub := &fakePublisher{}
	now := time.Unix(123, 0).UTC()
	backend := NewRelayBackend(RelayBackendOptions{Relays: []string{"wss://relay.example"}, RepoAddr: "30617:owner:repo", Authors: []string{backendTestPubKey(1).Hex()}, Signer: noopSigner{}, Publisher: pub, Now: func() time.Time { return now }})
	issue := &pb.Issue{Id: "task-2", Title: "write me", Status: pb.Status_STATUS_OPEN}

	ev, err := backend.PutIssue(context.Background(), issue)
	if err != nil {
		t.Fatal(err)
	}
	if ev == nil || len(pub.events) != 1 || pub.events[0] != ev {
		t.Fatalf("publish mismatch event=%#v published=%d", ev, len(pub.events))
	}
	if ev.Kind != gonostr.Kind(nip34.KindCanonicalState) {
		t.Fatalf("kind=%d", ev.Kind)
	}
	if got, _ := nip34.TagFirst(ev, "d"); got != "task:task-2" {
		t.Fatalf("d tag=%q", got)
	}
	body, err := nip34.ParseTaskStateEvent(ev)
	if err != nil {
		t.Fatal(err)
	}
	if body.Id != "task-2" || body.Metadata.Custom["nip34.repo_addr"] != "30617:owner:repo" || body.Updated.AsTime() != now {
		t.Fatalf("unexpected task state: %#v", body)
	}
	if issue.Metadata != nil {
		t.Fatalf("PutIssue mutated caller issue metadata: %#v", issue.Metadata)
	}
}
