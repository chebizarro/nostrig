package taskfabric

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	gonostr "fiatjaf.com/nostr"
	beadspb "github.com/chebizarro/nostrig/gen/beads"
	"github.com/chebizarro/nostrig/internal/beads"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
)

// SyncOptions configure relay-backed canonical task-state synchronization.
type EventPublisher interface {
	Publish(ctx context.Context, relays []string, signer nip34.Signer, events []*gonostr.Event) error
}

type SyncOptions struct {
	Relays    []string
	RepoAddr  string
	TaskIDs   []string
	Authors   []string
	OutDir    string
	CachePath string
	// Limit is retained for compatibility and is now the query page size,
	// not a total-result ceiling. Pagination supplies explicit safety bounds.
	Limit      int
	Pagination nip34.PaginationOptions

	FailOnConflict      bool
	Push                bool
	SyncNIP34Status     bool
	Signer              nip34.Signer
	Publisher           EventPublisher
	RelayWinsOnConflict bool
}

type SyncResult struct {
	Export         *beadspb.Export
	EventCount     int
	CachePath      string
	ConflictCount  int
	PublishedCount int
}

// Sync fetches canonical task-state events and renders their authoritative
// state into the durable cache and local .beads projection. Local edits are
// retained only as drift diagnostics and never become canonical relay state.
func Sync(ctx context.Context, client *nip34.Client, opts SyncOptions) (*SyncResult, error) {
	if strings.TrimSpace(opts.OutDir) == "" {
		return nil, fmt.Errorf("out dir is required")
	}
	if opts.Push {
		return nil, fmt.Errorf("sync --push is deprecated: local .beads files are projections; mutate tasks through ContextVM or use migrate for one-time import")
	}
	events, err := FetchTaskStateEvents(ctx, client, opts)
	if err != nil {
		return nil, err
	}
	relayExport, err := ExportFromTaskStateEvents(events)
	if err != nil {
		return nil, err
	}
	cachePath := strings.TrimSpace(opts.CachePath)
	if cachePath == "" {
		cachePath = DefaultCachePath(opts.OutDir)
	}
	previous, err := LoadCache(cachePath)
	if err != nil {
		return nil, fmt.Errorf("load cache: %w", err)
	}
	local, err := LoadLocalIssues(opts.OutDir)
	if err != nil {
		return nil, fmt.Errorf("load local beads issues: %w", err)
	}
	merged, err := MergeTaskStateWithOptions(relayExport, local, previous, MergeOptions{RelayAuthoritative: true, AuthoritativeTaskIDs: opts.TaskIDs})
	if err != nil {
		return nil, err
	}
	published := 0
	if err := WriteCache(cachePath, merged.Records); err != nil {
		return nil, fmt.Errorf("write cache: %w", err)
	}
	if err := beads.NewRenderer(opts.OutDir).RenderExport(merged.Export); err != nil {
		return nil, fmt.Errorf("render failed: %w", err)
	}
	if opts.FailOnConflict && len(merged.Conflicts) > 0 {
		return nil, fmt.Errorf("sync detected %d task conflict(s); inspect %s", len(merged.Conflicts), cachePath)
	}
	return &SyncResult{Export: merged.Export, EventCount: len(events), CachePath: cachePath, ConflictCount: len(merged.Conflicts), PublishedCount: published}, nil
}

// publishWriteBack remains only to return an explicit error to older callers.
// Canonical mutations must traverse the ContextVM command ledger and durable
// publisher; local Beads JSONL is never a write source.
func publishWriteBack(_ context.Context, opts SyncOptions, _ *MergeResult) (int, error) {
	if !opts.Push {
		return 0, nil
	}
	return 0, fmt.Errorf("sync write-back is deprecated: local .beads files are projections; mutate tasks through ContextVM")
}

// FetchTaskStateEvents queries relays to completion for selected canonical
// task state and tombstones. A successful return is never a partial first page.
func FetchTaskStateEvents(ctx context.Context, client *nip34.Client, opts SyncOptions) ([]*gonostr.Event, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is nil")
	}
	if client == nil {
		client = nip34.NewClient()
	}
	relays := cleanStrings(opts.Relays)
	if len(relays) == 0 {
		return nil, fmt.Errorf("at least one relay is required")
	}
	pagination := opts.Pagination
	if pagination.PageSize == 0 {
		pagination.PageSize = opts.Limit
	}
	query, err := beads.NewCanonicalTaskQuery(opts.RepoAddr, opts.TaskIDs, opts.Authors, pagination.PageSize)
	if err != nil {
		return nil, err
	}
	pagination.PageSize = query.PageSize()
	events, err := client.FetchManyPaginated(ctx, relays, query.Filters(), pagination)
	if err != nil {
		return nil, err
	}
	for _, ev := range events {
		if err := query.ValidateEvent(ev); err != nil {
			return nil, err
		}
	}
	return events, nil
}

// TaskQuerySource is the complete-query capability implemented by nostr.Client.
type TaskQuerySource interface {
	FetchManyPaginated(ctx context.Context, relays []string, filters []gonostr.Filter, opts nip34.PaginationOptions) ([]*gonostr.Event, error)
}

// TaskEventSource extends complete snapshots with live relay updates. It is
// intentionally independent of command handling and durable cursors.
type TaskEventSource interface {
	TaskQuerySource
	Subscribe(ctx context.Context, relays []string, filter gonostr.Filter) (<-chan gonostr.RelayEvent, error)
}

type TaskListOptions struct {
	Relays     []string
	RepoAddr   string
	TaskIDs    []string
	Authors    []string
	Pagination nip34.PaginationOptions
}

// ListTaskState returns a proven-complete selected snapshot. Query safety or
// timestamp ambiguity is returned as an error rather than a partial export.
func ListTaskState(ctx context.Context, source TaskQuerySource, opts TaskListOptions) (*beadspb.Export, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is nil")
	}
	if source == nil {
		source = nip34.NewClient()
	}
	relays := cleanStrings(opts.Relays)
	if len(relays) == 0 {
		return nil, fmt.Errorf("at least one relay is required")
	}
	query, err := beads.NewCanonicalTaskQuery(opts.RepoAddr, opts.TaskIDs, opts.Authors, opts.Pagination.PageSize)
	if err != nil {
		return nil, err
	}
	pagination := opts.Pagination
	pagination.PageSize = query.PageSize()
	events, err := source.FetchManyPaginated(ctx, relays, query.Filters(), pagination)
	if err != nil {
		return nil, err
	}
	projection := beads.NewTaskStateProjection()
	for _, ev := range events {
		if err := query.ValidateEvent(ev); err != nil {
			return nil, err
		}
		if _, _, err := projection.Apply(ev); err != nil {
			return nil, err
		}
	}
	return projection.Export()
}

type TaskWatchOptions struct {
	Relays     []string
	RepoAddr   string
	TaskIDs    []string
	Authors    []string
	Pagination nip34.PaginationOptions

	// ChangeBuffer bounds in-process delivery buffering. Slow consumers apply
	// backpressure; changes are never silently dropped.
	ChangeBuffer int
}

type TaskChangeKind string

const (
	TaskChangeUpsert TaskChangeKind = "upsert"
	TaskChangeDelete TaskChangeKind = "delete"
)

type TaskChange struct {
	Kind      TaskChangeKind
	TaskID    string
	Issue     *beadspb.Issue
	EventID   string
	CreatedAt time.Time
}

type TaskWatch struct {
	Snapshot *beadspb.Export
	Changes  <-chan TaskChange
	Errors   <-chan error

	cancel context.CancelFunc
	once   sync.Once
}

func (w *TaskWatch) Close() {
	if w == nil {
		return
	}
	w.once.Do(func() {
		if w.cancel != nil {
			w.cancel()
		}
	})
}

// WatchTaskState subscribes before fetching the complete initial snapshot,
// closing the fetch-then-subscribe gap. Duplicate or stale subscription events
// already represented by Snapshot are absorbed by the incremental projection.
func WatchTaskState(ctx context.Context, source TaskEventSource, opts TaskWatchOptions) (*TaskWatch, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is nil")
	}
	if source == nil {
		source = nip34.NewClient()
	}
	relays := cleanStrings(opts.Relays)
	if len(relays) == 0 {
		return nil, fmt.Errorf("at least one relay is required")
	}
	query, err := beads.NewCanonicalTaskQuery(opts.RepoAddr, opts.TaskIDs, opts.Authors, opts.Pagination.PageSize)
	if err != nil {
		return nil, err
	}
	pagination := opts.Pagination
	pagination.PageSize = query.PageSize()
	buffer := opts.ChangeBuffer
	if buffer <= 0 {
		buffer = 256
	}

	watchCtx, cancel := context.WithCancel(ctx)
	filters := query.Filters()
	watchStart := gonostr.Timestamp(time.Now().Add(-time.Second).Unix())
	subscriptions := make([]<-chan gonostr.RelayEvent, 0, len(filters))
	for _, filter := range filters {
		filter.Limit = 0
		filter.LimitZero = false
		filter.Since = watchStart
		ch, err := source.Subscribe(watchCtx, relays, filter)
		if err != nil {
			cancel()
			return nil, err
		}
		subscriptions = append(subscriptions, ch)
	}
	live, subscriptionClosed := mergeTaskSubscriptions(watchCtx, buffer, subscriptions...)

	snapshotEvents, err := source.FetchManyPaginated(watchCtx, relays, filters, pagination)
	if err != nil {
		cancel()
		return nil, err
	}
	// Reconcile once more after subscriptions have been requested. The
	// subscription API does not expose an EOSE/readiness barrier, so this
	// second complete read closes the practical setup window; any overlap is
	// absorbed by event-ID/order deduplication in the projection.
	reconciledEvents, err := source.FetchManyPaginated(watchCtx, relays, filters, pagination)
	if err != nil {
		cancel()
		return nil, err
	}
	snapshotEvents = append(snapshotEvents, reconciledEvents...)
	select {
	case <-subscriptionClosed:
		cancel()
		return nil, fmt.Errorf("task watch subscription closed during initial snapshot")
	default:
	}

	projection := beads.NewTaskStateProjection()
	for _, ev := range snapshotEvents {
		if err := query.ValidateEvent(ev); err != nil {
			cancel()
			return nil, err
		}
		if _, _, err := projection.Apply(ev); err != nil {
			cancel()
			return nil, err
		}
	}
	snapshot, err := projection.Export()
	if err != nil {
		cancel()
		return nil, err
	}

	changes := make(chan TaskChange, buffer)
	errs := make(chan error, 1)
	watch := &TaskWatch{Snapshot: snapshot, Changes: changes, Errors: errs, cancel: cancel}
	go func() {
		defer close(changes)
		defer close(errs)
		defer cancel()
		for {
			select {
			case <-watchCtx.Done():
				return
			case <-subscriptionClosed:
				select {
				case errs <- fmt.Errorf("task watch subscription closed"):
				default:
				}
				return
			case relayEvent, ok := <-live:
				if !ok {
					if watchCtx.Err() == nil {
						select {
						case errs <- fmt.Errorf("task watch subscription closed"):
						default:
						}
					}
					return
				}
				ev := relayEvent.Event
				if err := query.ValidateEvent(&ev); err != nil {
					select {
					case errs <- err:
					default:
					}
					return
				}
				taskID, changed, err := projection.Apply(&ev)
				if err != nil {
					select {
					case errs <- err:
					default:
					}
					return
				}
				if !changed {
					continue
				}
				change := TaskChange{
					Kind: TaskChangeUpsert, TaskID: taskID, Issue: projection.Issue(taskID),
					EventID: ev.ID.Hex(), CreatedAt: nip34.EventTime(&ev),
				}
				if change.Issue == nil {
					change.Kind = TaskChangeDelete
				}
				select {
				case changes <- change:
				case <-watchCtx.Done():
					return
				}
			}
		}
	}()
	return watch, nil
}

func mergeTaskSubscriptions(ctx context.Context, buffer int, inputs ...<-chan gonostr.RelayEvent) (<-chan gonostr.RelayEvent, <-chan struct{}) {
	out := make(chan gonostr.RelayEvent, buffer)
	closed := make(chan struct{}, 1)
	var wg sync.WaitGroup
	wg.Add(len(inputs))
	for _, input := range inputs {
		go func(ch <-chan gonostr.RelayEvent) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case ev, ok := <-ch:
					if !ok {
						select {
						case closed <- struct{}{}:
						default:
						}
						return
					}
					select {
					case out <- ev:
					case <-ctx.Done():
						return
					}
				}
			}
		}(input)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out, closed
}

func canonicalAuthor(authors []string) (string, error) {
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

// ExportFromTaskStateEvents converts canonical 30900 task-state events into a beads export.
func ExportFromTaskStateEvents(events []*gonostr.Event) (*beadspb.Export, error) {
	return beads.ExportFromTaskStateEvents(events)
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
