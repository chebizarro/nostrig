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
	GetIssue(ctx context.Context, id string) (*pb.Issue, error)
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
	Relays   []string
	RepoAddr string
	TaskIDs  []string
	Authors  []string

	// Limit is retained for compatibility and now controls page size rather
	// than the total result count. Pagination supplies explicit safety bounds.
	Limit      int
	Pagination nip34.PaginationOptions

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

// SaveExport is retained as a source-compatible hard failure for old callers.
// Canonical writes are accepted only through the ContextVM task API.
func (b *RelayBackend) SaveExport(context.Context, *pb.Export) error {
	return fmt.Errorf("direct relay mutations are disabled; use the ContextVM task API")
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

// PutIssue is retained as a source-compatible hard failure for old callers.
// Canonical writes are accepted only through the ContextVM task API.
func (b *RelayBackend) PutIssue(context.Context, *pb.Issue) (*gonostr.Event, error) {
	return nil, fmt.Errorf("direct relay mutations are disabled; use the ContextVM task API")
}

func (b *RelayBackend) fetch(ctx context.Context, taskIDs []string) ([]*gonostr.Event, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is nil")
	}
	relays := cleanStrings(b.opts.Relays)
	if len(relays) == 0 {
		return nil, fmt.Errorf("at least one relay is required")
	}

	pagination := b.opts.Pagination
	if pagination.PageSize == 0 {
		pagination.PageSize = b.opts.Limit
	}
	query, err := NewCanonicalTaskQuery(b.opts.RepoAddr, taskIDs, b.opts.Authors, pagination.PageSize)
	if err != nil {
		return nil, err
	}
	pagination.PageSize = query.PageSize()

	fetcher := b.opts.Fetcher
	if fetcher == nil {
		fetcher = nip34.NewClient()
	}

	var events []*gonostr.Event
	if paginated, ok := fetcher.(PaginatedEventFetcher); ok {
		events, err = paginated.FetchManyPaginated(ctx, relays, query.Filters(), pagination)
		if err != nil {
			return nil, err
		}
	} else {
		// Keep EventFetcher source-compatible. A legacy fetcher may only claim
		// completeness when neither selected filter saturates one page.
		maxEvents := pagination.MaxEvents
		if maxEvents == 0 {
			maxEvents = nip34.DefaultQueryMaxEvents
		}
		for filterIndex, filter := range query.Filters() {
			page, fetchErr := fetcher.Fetch(ctx, relays, filter)
			if fetchErr != nil {
				return nil, fetchErr
			}
			events = append(events, page...)
			if len(page) >= query.PageSize() {
				return nil, &nip34.QueryTruncatedError{
					Reason: nip34.TruncatedByLegacyFetcherLimit, FilterIndex: filterIndex,
					PageSize: query.PageSize(), Pages: 1, EventCount: len(events),
				}
			}
			if len(events) > maxEvents {
				return nil, &nip34.QueryTruncatedError{
					Reason: nip34.TruncatedByEventLimit, FilterIndex: filterIndex,
					PageSize: query.PageSize(), Pages: 1, EventCount: len(events),
				}
			}
		}
	}

	for _, ev := range events {
		if err := query.ValidateEvent(ev); err != nil {
			return nil, err
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
		return fmt.Errorf("direct relay writes require an explicit publisher; use the ContextVM task API for production mutations")
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
		issue   *pb.Issue
		event   *gonostr.Event
		version nip34.TaskStateSchemaVersion
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
		issue, version, err := nip34.ParseTaskStateEventVersioned(ev)
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
		if previous, ok := states[key]; !ok || version > previous.version || (version == previous.version && eventAfter(previous.event, ev)) {
			states[key] = stateRecord{issue: issue, event: ev, version: version}
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
