package beads

import (
	"context"
	"fmt"
	"strings"

	gonostr "fiatjaf.com/nostr"
	pb "github.com/chebizarro/nostrig/gen/beads"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
	"google.golang.org/protobuf/proto"
)

// PaginatedEventFetcher is an additive capability implemented by nostr.Client.
// EventFetcher remains unchanged so existing stores and backfill consumers stay
// source-compatible.
type PaginatedEventFetcher interface {
	FetchManyPaginated(ctx context.Context, relays []string, filters []gonostr.Filter, opts nip34.PaginationOptions) ([]*gonostr.Event, error)
}

// CanonicalTaskQuery owns the relay filters and validation rules for canonical
// task state. Construction requires trusted authors and either a repository or
// exact task selector, so production callers cannot accidentally issue an
// unbounded task query.
type CanonicalTaskQuery struct {
	repoAddr string
	taskIDs  []string
	trusted  map[string]struct{}
	filters  []gonostr.Filter
	pageSize int
}

func NewCanonicalTaskQuery(repoAddr string, taskIDs, authors []string, pageSize int) (*CanonicalTaskQuery, error) {
	repoAddr = strings.TrimSpace(repoAddr)
	taskIDs = cleanTaskIDs(taskIDs)
	if repoAddr == "" && len(taskIDs) == 0 {
		return nil, fmt.Errorf("canonical task query requires a bounded selector: repo addr or task ids")
	}
	authors = cleanStrings(authors)
	if len(authors) == 0 {
		return nil, fmt.Errorf("at least one canonical author is required")
	}
	if pageSize <= 0 {
		pageSize = nip34.DefaultQueryPageSize
	}

	trusted := make(map[string]struct{}, len(authors))
	pubkeys := make([]gonostr.PubKey, 0, len(authors))
	normalizedAuthors := make([]string, 0, len(authors))
	for _, author := range authors {
		pk, err := gonostr.PubKeyFromHex(author)
		if err != nil {
			return nil, fmt.Errorf("invalid canonical author %q", author)
		}
		normalized := pk.Hex()
		if _, ok := trusted[normalized]; ok {
			continue
		}
		trusted[normalized] = struct{}{}
		pubkeys = append(pubkeys, pk)
		normalizedAuthors = append(normalizedAuthors, normalized)
	}

	stateTags := gonostr.TagMap{}
	if repoAddr != "" {
		stateTags["a"] = []string{repoAddr}
	}
	if len(taskIDs) > 0 {
		ds := make([]string, 0, len(taskIDs))
		for _, id := range taskIDs {
			ds = append(ds, "task:"+id)
		}
		stateTags["d"] = ds
	}
	state := gonostr.Filter{
		Kinds:   []gonostr.Kind{gonostr.Kind(nip34.KindCanonicalState)},
		Authors: append([]gonostr.PubKey(nil), pubkeys...),
		Tags:    stateTags,
		Limit:   pageSize,
	}

	tombstoneTags := gonostr.TagMap{}
	if len(taskIDs) > 0 {
		coords := make([]string, 0, len(taskIDs)*len(normalizedAuthors))
		for _, author := range normalizedAuthors {
			for _, id := range taskIDs {
				coords = append(coords, nip34.Address(nip34.KindCanonicalState, author, "task:"+id))
			}
		}
		tombstoneTags["a"] = coords
	} else {
		tombstoneTags["a"] = []string{repoAddr}
	}
	tombstone := gonostr.Filter{
		Kinds:   []gonostr.Kind{gonostr.Kind(5)},
		Authors: append([]gonostr.PubKey(nil), pubkeys...),
		Tags:    tombstoneTags,
		Limit:   pageSize,
	}

	return &CanonicalTaskQuery{
		repoAddr: repoAddr,
		taskIDs:  taskIDs,
		trusted:  trusted,
		filters:  []gonostr.Filter{state, tombstone},
		pageSize: pageSize,
	}, nil
}

func (q *CanonicalTaskQuery) Filters() []gonostr.Filter {
	if q == nil {
		return nil
	}
	out := make([]gonostr.Filter, len(q.filters))
	for i := range q.filters {
		out[i] = q.filters[i].Clone()
	}
	return out
}

func (q *CanonicalTaskQuery) PageSize() int {
	if q == nil {
		return 0
	}
	return q.pageSize
}

// ValidateEvent verifies returned relay data even when the relay claims it
// matched the requested filters.
func (q *CanonicalTaskQuery) ValidateEvent(ev *gonostr.Event) error {
	if q == nil {
		return fmt.Errorf("canonical task query is nil")
	}
	if ev == nil {
		return fmt.Errorf("relay returned nil canonical state")
	}
	if _, ok := q.trusted[ev.PubKey.Hex()]; !ok {
		return fmt.Errorf("relay returned state from untrusted author")
	}

	var taskID, eventRepo string
	switch int(ev.Kind) {
	case nip34.KindCanonicalState:
		issue, err := nip34.ParseTaskStateEvent(ev)
		if err != nil {
			return fmt.Errorf("invalid canonical state %s: %w", ev.ID.Hex(), err)
		}
		taskID = cleanTaskID(issue.Id)
		eventRepo = issue.GetMetadata().GetCustom()["nip34.repo_addr"]
	case 5:
		var err error
		taskID, err = nip34.ValidateTaskTombstone(ev, ev.PubKey.Hex())
		if err != nil {
			return fmt.Errorf("invalid canonical tombstone %s: %w", ev.ID.Hex(), err)
		}
		taskID = cleanTaskID(taskID)
		eventRepo = nip34.TaskTombstoneRepo(ev)
	default:
		return fmt.Errorf("relay returned unexpected canonical event kind")
	}
	if taskID == "" {
		return fmt.Errorf("relay returned canonical event without task id")
	}
	if q.repoAddr != "" && eventRepo != q.repoAddr {
		return fmt.Errorf("relay returned state outside repository selector")
	}
	if len(q.taskIDs) > 0 && !stringContains(q.taskIDs, taskID) {
		return fmt.Errorf("relay returned state outside task selector")
	}
	return nil
}

// TaskStateProjection keeps only the latest state and tombstone per canonical
// author/task coordinate. It is suitable for large fleets because memory grows
// with selected tasks and trusted authors, not relay history.
type TaskStateProjection struct {
	states     map[string]*gonostr.Event
	tombstones map[string]*gonostr.Event
}

func NewTaskStateProjection() *TaskStateProjection {
	return &TaskStateProjection{
		states:     map[string]*gonostr.Event{},
		tombstones: map[string]*gonostr.Event{},
	}
}

// Apply incrementally updates the projection. visibleChanged reports whether
// the selected visible task changed after applying the event.
func (p *TaskStateProjection) Apply(ev *gonostr.Event) (taskID string, visibleChanged bool, err error) {
	if p == nil {
		return "", false, fmt.Errorf("task state projection is nil")
	}
	if ev == nil {
		return "", false, fmt.Errorf("task state event is nil")
	}
	author := ev.PubKey.Hex()
	switch int(ev.Kind) {
	case nip34.KindCanonicalState:
		issue, parseErr := nip34.ParseTaskStateEvent(ev)
		if parseErr != nil {
			return "", false, parseErr
		}
		taskID = cleanTaskID(issue.Id)
	case 5:
		taskID, err = nip34.ValidateTaskTombstone(ev, author)
		if err != nil {
			return "", false, err
		}
		taskID = cleanTaskID(taskID)
	default:
		return "", false, fmt.Errorf("unsupported task projection event kind %d", ev.Kind)
	}
	if taskID == "" {
		return "", false, fmt.Errorf("task id is required")
	}

	before := p.issue(taskID)
	key := author + "|" + taskID
	target := p.states
	if int(ev.Kind) == 5 {
		target = p.tombstones
	}
	if previous := target[key]; previous == nil || eventAfter(previous, ev) {
		copyEvent := *ev
		target[key] = &copyEvent
	}
	after := p.issue(taskID)
	return taskID, !proto.Equal(before, after), nil
}

// Issue returns the current visible task with caller-owned protobuf state.
func (p *TaskStateProjection) Issue(taskID string) *pb.Issue {
	return cloneProjectedIssue(p.issue(cleanTaskID(taskID)))
}

func (p *TaskStateProjection) issue(taskID string) *pb.Issue {
	if p == nil || taskID == "" {
		return nil
	}
	events := p.eventsForTask(taskID)
	export, err := ExportFromTaskStateEvents(events)
	if err != nil || len(export.Issues) == 0 {
		return nil
	}
	return export.Issues[0]
}

// Export returns a stable, task-ID-sorted snapshot.
func (p *TaskStateProjection) Export() (*pb.Export, error) {
	if p == nil {
		return &pb.Export{}, nil
	}
	events := make([]*gonostr.Event, 0, len(p.states)+len(p.tombstones))
	for _, ev := range p.states {
		events = append(events, ev)
	}
	for _, ev := range p.tombstones {
		events = append(events, ev)
	}
	return ExportFromTaskStateEvents(events)
}

func (p *TaskStateProjection) eventsForTask(taskID string) []*gonostr.Event {
	events := make([]*gonostr.Event, 0, 4)
	suffix := "|" + taskID
	for key, ev := range p.states {
		if strings.HasSuffix(key, suffix) {
			events = append(events, ev)
		}
	}
	for key, ev := range p.tombstones {
		if strings.HasSuffix(key, suffix) {
			events = append(events, ev)
		}
	}
	return events
}

func cloneProjectedIssue(issue *pb.Issue) *pb.Issue {
	if issue == nil {
		return nil
	}
	return proto.Clone(issue).(*pb.Issue)
}
