package beads

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	gonostr "fiatjaf.com/nostr"
	pb "github.com/chebizarro/nostrig/gen/beads"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Store is the storage seam nostrig uses for beads-compatible state.
// JSONL rendering is one implementation; RelayBackend makes 30900 task:* events
// the canonical, append-only store and leaves local .beads files as projections.
type Store interface {
	LoadExport(ctx context.Context) (*pb.Export, error)
	SaveExport(ctx context.Context, export *pb.Export) error
	GetIssue(ctx context.Context, id string) (*pb.Issue, error)
	PutIssue(ctx context.Context, issue *pb.Issue) (*gonostr.Event, error)
}

// EventFetcher is satisfied by internal/nostr.Client and by tests.
type EventFetcher interface {
	Fetch(ctx context.Context, relays []string, filter gonostr.Filter) ([]*gonostr.Event, error)
}

// EventPublisher is satisfied by internal/nostr.Publisher and by tests.
type EventPublisher interface {
	Publish(ctx context.Context, relays []string, signer nip34.Signer, events []*gonostr.Event) error
}

// RelayBackendOptions configure the relay-as-source-of-truth beads store.
type RelayBackendOptions struct {
	Relays    []string
	RepoAddr  string
	TaskIDs   []string
	Authors   []string
	Limit     int
	Signer    nip34.Signer
	Fetcher   EventFetcher
	Publisher EventPublisher
	Now       func() time.Time
}

// RelayBackend reads and writes beads issues directly as Nostr 30900 task:* state
// events. Writes are append-only: each mutation publishes a new canonical state
// event; reads collapse event history to the latest state per task id.
type RelayBackend struct{ opts RelayBackendOptions }

// NewRelayBackend creates a relay-backed beads store.
func NewRelayBackend(opts RelayBackendOptions) *RelayBackend { return &RelayBackend{opts: opts} }

// LoadExport fetches canonical 30900 task:* events and returns the latest issue
// state for each task.
func (b *RelayBackend) LoadExport(ctx context.Context) (*pb.Export, error) {
	events, err := b.fetch(ctx, b.opts.TaskIDs)
	if err != nil {
		return nil, err
	}
	return ExportFromTaskStateEvents(events)
}

// SaveExport publishes one new 30900 task:* state event for each issue in export.
func (b *RelayBackend) SaveExport(ctx context.Context, export *pb.Export) error {
	if export == nil {
		return fmt.Errorf("export is nil")
	}
	events := make([]*gonostr.Event, 0, len(export.Issues))
	for _, issue := range export.Issues {
		if issue == nil {
			continue
		}
		ev, err := b.buildEvent(issue)
		if err != nil {
			return err
		}
		events = append(events, ev)
	}
	return b.publish(ctx, events)
}

// GetIssue loads the latest state for one task id.
func (b *RelayBackend) GetIssue(ctx context.Context, id string) (*pb.Issue, error) {
	id = cleanTaskID(id)
	if id == "" {
		return nil, fmt.Errorf("issue id is required")
	}
	events, err := b.fetch(ctx, []string{id})
	if err != nil {
		return nil, err
	}
	export, err := ExportFromTaskStateEvents(events)
	if err != nil {
		return nil, err
	}
	for _, issue := range export.Issues {
		if issue.GetId() == id {
			return issue, nil
		}
	}
	return nil, fmt.Errorf("issue %s not found", id)
}

// PutIssue publishes a new canonical 30900 task:* state event for issue.
func (b *RelayBackend) PutIssue(ctx context.Context, issue *pb.Issue) (*gonostr.Event, error) {
	ev, err := b.buildEvent(issue)
	if err != nil {
		return nil, err
	}
	if err := b.publish(ctx, []*gonostr.Event{ev}); err != nil {
		return nil, err
	}
	return ev, nil
}

func (b *RelayBackend) fetch(ctx context.Context, taskIDs []string) ([]*gonostr.Event, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is nil")
	}
	relays := cleanStrings(b.opts.Relays)
	if len(relays) == 0 {
		return nil, fmt.Errorf("at least one relay is required")
	}
	limit := b.opts.Limit
	if limit <= 0 {
		limit = 500
	}
	authors := cleanStrings(b.opts.Authors)
	if len(authors) == 0 {
		return nil, fmt.Errorf("at least one canonical author is required")
	}
	trusted := map[string]struct{}{}
	filter := gonostr.Filter{Kinds: []gonostr.Kind{gonostr.Kind(nip34.KindCanonicalState)}, Limit: limit}
	for _, author := range authors {
		pk, err := gonostr.PubKeyFromHex(author)
		if err != nil {
			return nil, fmt.Errorf("invalid canonical author %q", author)
		}
		filter.Authors = append(filter.Authors, pk)
		trusted[pk.Hex()] = struct{}{}
	}
	tags := gonostr.TagMap{}
	if repoAddr := strings.TrimSpace(b.opts.RepoAddr); repoAddr != "" {
		tags["a"] = []string{repoAddr}
	}
	if ids := cleanTaskIDs(taskIDs); len(ids) > 0 {
		ds := make([]string, 0, len(ids))
		for _, id := range ids {
			ds = append(ds, "task:"+id)
		}
		tags["d"] = ds
	}
	if len(tags) > 0 {
		filter.Tags = tags
	}
	if strings.TrimSpace(b.opts.RepoAddr) == "" && len(cleanTaskIDs(taskIDs)) == 0 {
		return nil, fmt.Errorf("relay backend requires a bounded selector: repo addr or task ids")
	}
	fetcher := b.opts.Fetcher
	if fetcher == nil {
		fetcher = nip34.NewClient()
	}
	events, err := fetcher.Fetch(ctx, relays, filter)
	if err != nil {
		return nil, err
	}
	tombstoneFilter := gonostr.Filter{Kinds: []gonostr.Kind{gonostr.Kind(5)}, Authors: append([]gonostr.PubKey(nil), filter.Authors...), Limit: limit}
	if ids := cleanTaskIDs(taskIDs); len(ids) > 0 {
		coords := make([]string, 0, len(ids)*len(authors))
		for _, author := range authors {
			for _, id := range ids {
				coords = append(coords, nip34.Address(nip34.KindCanonicalState, strings.ToLower(author), "task:"+id))
			}
		}
		tombstoneFilter.Tags = gonostr.TagMap{"a": coords}
	} else {
		tombstoneFilter.Tags = gonostr.TagMap{"a": []string{strings.TrimSpace(b.opts.RepoAddr)}}
	}
	tombstones, err := fetcher.Fetch(ctx, relays, tombstoneFilter)
	if err != nil {
		return nil, err
	}
	events = append(events, tombstones...)
	requested := cleanTaskIDs(taskIDs)
	for _, ev := range events {
		if ev == nil {
			return nil, fmt.Errorf("relay returned nil canonical state")
		}
		if _, ok := trusted[ev.PubKey.Hex()]; !ok {
			return nil, fmt.Errorf("relay returned state from untrusted author")
		}
		var taskID, eventRepo string
		switch int(ev.Kind) {
		case nip34.KindCanonicalState:
			issue, err := nip34.ParseTaskStateEvent(ev)
			if err != nil {
				return nil, fmt.Errorf("invalid canonical state %s: %w", ev.ID.Hex(), err)
			}
			taskID = issue.Id
			eventRepo = issue.GetMetadata().GetCustom()["nip34.repo_addr"]
		case 5:
			taskID, err = nip34.ValidateTaskTombstone(ev, ev.PubKey.Hex())
			if err != nil {
				return nil, fmt.Errorf("invalid canonical tombstone %s: %w", ev.ID.Hex(), err)
			}
			eventRepo = nip34.TaskTombstoneRepo(ev)
		default:
			return nil, fmt.Errorf("relay returned unexpected canonical event kind")
		}
		if repo := strings.TrimSpace(b.opts.RepoAddr); repo != "" && eventRepo != repo {
			return nil, fmt.Errorf("relay returned state outside repository selector")
		}
		if len(requested) > 0 && !stringContains(requested, taskID) {
			return nil, fmt.Errorf("relay returned state outside task selector")
		}
	}
	return events, nil
}

func (b *RelayBackend) buildEvent(issue *pb.Issue) (*gonostr.Event, error) {
	if issue == nil {
		return nil, fmt.Errorf("issue is nil")
	}
	if strings.TrimSpace(issue.Id) == "" {
		return nil, fmt.Errorf("issue missing id")
	}
	issue = cloneIssueForRelay(issue)
	ensureMetadata(issue)
	if repoAddr := strings.TrimSpace(b.opts.RepoAddr); repoAddr != "" && issue.Metadata.Custom["nip34.repo_addr"] == "" {
		issue.Metadata.Custom["nip34.repo_addr"] = repoAddr
	}
	now := b.now()
	if issue.Created == nil {
		issue.Created = timestamppb.New(now)
	}
	issue.Updated = timestamppb.New(now)
	author, err := canonicalBackendAuthor(b.opts.Authors)
	if err != nil {
		return nil, err
	}
	return nip34.BuildTaskStateEvent(issue, author, now)
}

func (b *RelayBackend) publish(ctx context.Context, events []*gonostr.Event) error {
	if len(events) == 0 {
		return nil
	}
	relays := cleanStrings(b.opts.Relays)
	if len(relays) == 0 {
		return fmt.Errorf("at least one relay is required")
	}
	if b.opts.Signer == nil {
		return fmt.Errorf("signer is required")
	}
	publisher := b.opts.Publisher
	if publisher == nil {
		publisher = nip34.NewPublisher()
	}
	return publisher.Publish(ctx, relays, b.opts.Signer, events)
}

func (b *RelayBackend) now() time.Time {
	if b != nil && b.opts.Now != nil {
		return b.opts.Now().UTC()
	}
	return time.Now().UTC()
}

func ExportFromTaskStateEvents(events []*gonostr.Event) (*pb.Export, error) {
	type stateRecord struct {
		issue *pb.Issue
		event *gonostr.Event
	}
	states := map[string]stateRecord{}
	tombstones := map[string]*gonostr.Event{}
	for _, ev := range events {
		if ev == nil {
			continue
		}
		author := ev.PubKey.Hex()
		if int(ev.Kind) == 5 {
			id, err := nip34.ValidateTaskTombstone(ev, author)
			if err != nil {
				continue
			}
			key := author + "|" + id
			if tombstones[key] == nil || eventAfter(tombstones[key], ev) {
				tombstones[key] = ev
			}
			continue
		}
		issue, err := nip34.ParseTaskStateEvent(ev)
		if err != nil {
			continue
		}
		id := strings.TrimSpace(issue.GetId())
		if id == "" {
			continue
		}
		createdAt := nip34.EventTime(ev)
		ensureMetadata(issue)
		issue.Metadata.Custom["nostr.id"] = ev.ID.Hex()
		issue.Metadata.Custom["nostr.pubkey"] = author
		issue.Metadata.Custom["nostr.kind"] = fmt.Sprintf("%d", ev.Kind)
		issue.Metadata.Custom["nostrig.source"] = "canonical-task-state"
		if issue.Updated == nil && !createdAt.IsZero() {
			issue.Updated = timestamppb.New(createdAt)
		}
		if issue.Created == nil && !createdAt.IsZero() {
			issue.Created = timestamppb.New(createdAt)
		}
		key := author + "|" + id
		if previous, ok := states[key]; !ok || eventAfter(previous.event, ev) {
			states[key] = stateRecord{issue: issue, event: ev}
		}
	}
	latest := map[string]stateRecord{}
	for key, record := range states {
		if tombstone := tombstones[key]; tombstone != nil && !eventAfter(tombstone, record.event) {
			continue
		}
		id := record.issue.Id
		if previous, ok := latest[id]; !ok || eventAfter(previous.event, record.event) {
			latest[id] = record
		}
	}
	issues := make([]*pb.Issue, 0, len(latest))
	for _, record := range latest {
		issues = append(issues, record.issue)
	}
	sort.Slice(issues, func(i, j int) bool { return issues[i].Id < issues[j].Id })
	return &pb.Export{Issues: issues}, nil
}

func eventAfter(previous, candidate *gonostr.Event) bool {
	previousTime, candidateTime := nip34.EventTime(previous), nip34.EventTime(candidate)
	if !candidateTime.Equal(previousTime) {
		return candidateTime.After(previousTime)
	}
	return candidate.ID.Hex() > previous.ID.Hex()
}

func ensureMetadata(issue *pb.Issue) {
	if issue.Metadata == nil {
		issue.Metadata = &pb.Metadata{}
	}
	if issue.Metadata.Custom == nil {
		issue.Metadata.Custom = map[string]string{}
	}
}

func cloneIssueForRelay(issue *pb.Issue) *pb.Issue {
	if issue == nil {
		return nil
	}
	return proto.Clone(issue).(*pb.Issue)
}

func cleanTaskIDs(in []string) []string {
	out := make([]string, 0, len(in))
	seen := map[string]struct{}{}
	for _, id := range in {
		id = cleanTaskID(id)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func cleanTaskID(id string) string {
	return strings.TrimPrefix(strings.TrimSpace(id), "task:")
}

func canonicalBackendAuthor(authors []string) (string, error) {
	authors = cleanStrings(authors)
	if len(authors) == 0 {
		return "", fmt.Errorf("at least one canonical author is required")
	}
	author := strings.ToLower(authors[0])
	if _, err := gonostr.PubKeyFromHex(author); err != nil {
		return "", fmt.Errorf("invalid canonical author %q", authors[0])
	}
	return author, nil
}

func stringContains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func cleanStrings(in []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
