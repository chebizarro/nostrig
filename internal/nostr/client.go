package nostr

import (
	"context"
	"errors"
	"fmt"
	"time"

	gonostr "fiatjaf.com/nostr"
)

// Client wraps a go-nostr Pool and provides higher-level fetch helpers.
type fetchManyFunc func(ctx context.Context, relays []string, filter gonostr.Filter, opts gonostr.SubscriptionOptions) <-chan gonostr.RelayEvent
type subscribeManyFunc func(ctx context.Context, relays []string, filter gonostr.Filter, opts gonostr.SubscriptionOptions) <-chan gonostr.RelayEvent

type Client struct {
	pool          *gonostr.Pool
	fetchMany     fetchManyFunc
	subscribeMany subscribeManyFunc
	queryTimeout  time.Duration
}

// NewClient creates a new nostr client using a Pool.
func NewClient() *Client {
	pool := gonostr.NewPool()
	return &Client{
		pool:         pool,
		queryTimeout: 30 * time.Second,
		fetchMany: func(ctx context.Context, relays []string, filter gonostr.Filter, opts gonostr.SubscriptionOptions) <-chan gonostr.RelayEvent {
			return pool.FetchMany(ctx, relays, filter, opts)
		},
		subscribeMany: func(ctx context.Context, relays []string, filter gonostr.Filter, opts gonostr.SubscriptionOptions) <-chan gonostr.RelayEvent {
			return pool.SubscribeMany(ctx, relays, filter, opts)
		},
	}
}

// Fetch queries relays for events matching a single filter.
func (c *Client) Fetch(ctx context.Context, relays []string, f gonostr.Filter) ([]*gonostr.Event, error) {
	return c.FetchMany(ctx, relays, []gonostr.Filter{f})
}

// PartialFetchError reports that a fetch ended before full completion and the
// returned events may be incomplete.
type PartialFetchError struct {
	Cause      error
	EventCount int
}

func (e *PartialFetchError) Error() string {
	if e == nil {
		return ""
	}
	if e.EventCount > 0 {
		return fmt.Sprintf("nostr fetch incomplete after %d event(s): %v", e.EventCount, e.Cause)
	}
	return fmt.Sprintf("nostr fetch incomplete: %v", e.Cause)
}

func (e *PartialFetchError) Unwrap() error { return e.Cause }

func IsPartialFetch(err error) bool {
	var target *PartialFetchError
	return errors.As(err, &target)
}

// PaginationOptions controls complete relay traversal. Zero values select safe
// defaults. Limit on a caller filter is treated as a legacy page-size hint, not
// as a total-result limit.
type PaginationOptions struct {
	PageSize  int
	MaxPages  int
	MaxEvents int
}

const (
	DefaultQueryPageSize  = 500
	DefaultQueryMaxPages  = 1000
	DefaultQueryMaxEvents = 100000
)

// QueryTruncationReason identifies why a query could not prove completeness.
type QueryTruncationReason string

const (
	TruncatedByPageLimit          QueryTruncationReason = "page_limit"
	TruncatedByEventLimit         QueryTruncationReason = "event_limit"
	TruncatedAtTimestampBoundary  QueryTruncationReason = "timestamp_boundary"
	TruncatedByLegacyFetcherLimit QueryTruncationReason = "legacy_fetcher_limit"
)

// QueryTruncatedError reports an explicitly incomplete result. Callers must not
// persist or project the returned events as a complete selected result set.
type QueryTruncatedError struct {
	Reason      QueryTruncationReason
	Relay       string
	FilterIndex int
	PageSize    int
	Pages       int
	EventCount  int
	Cursor      gonostr.Timestamp
}

func (e *QueryTruncatedError) Error() string {
	if e == nil {
		return ""
	}
	where := ""
	if e.Relay != "" {
		where = fmt.Sprintf(" from %s", e.Relay)
	}
	cursor := ""
	if e.Cursor != 0 {
		cursor = fmt.Sprintf(" at created_at=%d", e.Cursor)
	}
	return fmt.Sprintf("nostr query truncated%s%s: reason=%s filter=%d pages=%d events=%d page_size=%d",
		where, cursor, e.Reason, e.FilterIndex, e.Pages, e.EventCount, e.PageSize)
}

// IsQueryTruncated reports whether err indicates an explicit safety-bound or
// timestamp-cursor truncation.
func IsQueryTruncated(err error) bool {
	var target *QueryTruncatedError
	return errors.As(err, &target)
}

func normalizePagination(opts PaginationOptions, legacyPageSize int) (PaginationOptions, error) {
	if opts.PageSize < 0 || opts.MaxPages < 0 || opts.MaxEvents < 0 {
		return PaginationOptions{}, fmt.Errorf("pagination bounds cannot be negative")
	}
	if opts.PageSize == 0 {
		opts.PageSize = legacyPageSize
	}
	if opts.PageSize <= 0 {
		opts.PageSize = DefaultQueryPageSize
	}
	if opts.MaxPages == 0 {
		opts.MaxPages = DefaultQueryMaxPages
	}
	if opts.MaxEvents == 0 {
		opts.MaxEvents = DefaultQueryMaxEvents
	}
	return opts, nil
}

// FetchManyPaginated queries every relay/filter stream until each selected
// result set is proven complete. Relays are traversed independently so one
// relay's page saturation cannot be hidden by cross-relay deduplication.
//
// Nostr timestamp cursors are inclusive and second-resolution. After a full
// page, this method queries the oldest timestamp exactly. It advances to the
// preceding second only when that boundary query returns fewer than PageSize.
// A saturated one-second boundary is therefore an explicit truncation error,
// never a silent omission.
func (c *Client) FetchManyPaginated(ctx context.Context, relays []string, filters []gonostr.Filter, opts PaginationOptions) ([]*gonostr.Event, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is nil")
	}
	if c == nil || c.pool == nil {
		return nil, fmt.Errorf("nostr client is not initialized")
	}
	if len(relays) == 0 {
		return nil, fmt.Errorf("no relays provided")
	}
	if len(filters) == 0 {
		return nil, fmt.Errorf("no filters provided")
	}

	seen := make(map[string]struct{}, 1024)
	out := make([]*gonostr.Event, 0, 256)
	addEvents := func(events []*gonostr.Event, p PaginationOptions, relay string, filterIndex, pages int, cursor gonostr.Timestamp) error {
		for _, ev := range events {
			if ev == nil || ev.ID == gonostr.ZeroID {
				continue
			}
			id := ev.ID.Hex()
			if _, ok := seen[id]; ok {
				continue
			}
			if len(seen) >= p.MaxEvents {
				return &QueryTruncatedError{
					Reason: TruncatedByEventLimit, Relay: relay, FilterIndex: filterIndex,
					PageSize: p.PageSize, Pages: pages, EventCount: len(seen), Cursor: cursor,
				}
			}
			seen[id] = struct{}{}
			evCopy := *ev
			out = append(out, &evCopy)
		}
		return nil
	}

	for filterIndex, original := range filters {
		pagination, err := normalizePagination(opts, original.Limit)
		if err != nil {
			return out, err
		}
		base := original.Clone()
		base.Limit = pagination.PageSize

		for _, relay := range relays {
			cursor := base.Until
			pages := 0
			fetchPage := func(filter gonostr.Filter) ([]*gonostr.Event, error) {
				if pages >= pagination.MaxPages {
					return nil, &QueryTruncatedError{
						Reason: TruncatedByPageLimit, Relay: relay, FilterIndex: filterIndex,
						PageSize: pagination.PageSize, Pages: pages, EventCount: len(seen), Cursor: cursor,
					}
				}
				pages++
				return c.Fetch(ctx, []string{relay}, filter)
			}

			for {
				pageFilter := base.Clone()
				pageFilter.Limit = pagination.PageSize
				pageFilter.Until = cursor
				page, err := fetchPage(pageFilter)
				if err != nil {
					return out, err
				}
				if err := addEvents(page, pagination, relay, filterIndex, pages, cursor); err != nil {
					return out, err
				}
				if len(page) < pagination.PageSize {
					break
				}

				oldest := page[0].CreatedAt
				for _, ev := range page[1:] {
					if ev != nil && ev.CreatedAt < oldest {
						oldest = ev.CreatedAt
					}
				}

				// Prove that every event at the inclusive boundary fits in one
				// page before skipping to the preceding second.
				boundary := base.Clone()
				boundary.Limit = pagination.PageSize
				boundary.Since = oldest
				boundary.Until = oldest
				cursor = oldest
				boundaryEvents, err := fetchPage(boundary)
				if err != nil {
					return out, err
				}
				if err := addEvents(boundaryEvents, pagination, relay, filterIndex, pages, cursor); err != nil {
					return out, err
				}
				if len(boundaryEvents) >= pagination.PageSize {
					return out, &QueryTruncatedError{
						Reason: TruncatedAtTimestampBoundary, Relay: relay, FilterIndex: filterIndex,
						PageSize: pagination.PageSize, Pages: pages, EventCount: len(seen), Cursor: oldest,
					}
				}

				if oldest == 0 || (base.Since != 0 && oldest <= base.Since) {
					break
				}
				cursor = oldest - 1
			}
		}
	}
	return out, nil
}

// Subscribe starts a live subscription using the same relay/filter semantics as
// Fetch. The caller owns cancellation through ctx.
func (c *Client) Subscribe(ctx context.Context, relays []string, filter gonostr.Filter) (<-chan gonostr.RelayEvent, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is nil")
	}
	if c == nil || (c.pool == nil && c.subscribeMany == nil) {
		return nil, fmt.Errorf("nostr client is not initialized")
	}
	if len(relays) == 0 {
		return nil, fmt.Errorf("no relays provided")
	}
	subscribe := c.subscribeMany
	if subscribe == nil {
		subscribe = func(ctx context.Context, relays []string, filter gonostr.Filter, opts gonostr.SubscriptionOptions) <-chan gonostr.RelayEvent {
			return c.pool.SubscribeMany(ctx, relays, filter, opts)
		}
	}
	ch := subscribe(ctx, relays, filter, gonostr.SubscriptionOptions{})
	if ch == nil {
		return nil, fmt.Errorf("failed to create subscription")
	}
	return ch, nil
}

// FetchMany queries relays for events matching multiple filters.
//
// It deduplicates events by ID across relays and filters.
// If the caller context is cancelled, it returns any collected events plus
// ctx.Err(). If the internal deadline expires first, it returns any collected
// events plus a PartialFetchError so callers can reject incomplete projections.
func (c *Client) FetchMany(ctx context.Context, relays []string, filters []gonostr.Filter) ([]*gonostr.Event, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is nil")
	}
	if c == nil || c.pool == nil {
		return nil, fmt.Errorf("nostr client is not initialized")
	}
	if len(relays) == 0 {
		return nil, fmt.Errorf("no relays provided")
	}
	if len(filters) == 0 {
		return nil, fmt.Errorf("no filters provided")
	}

	// Use FetchMany which closes the channel after receiving EOSE from all relays.
	// Add a timeout to avoid hanging if relays don't respond.
	timeout := c.queryTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	seen := make(map[string]struct{}, 1024)
	out := make([]*gonostr.Event, 0, 256)

	fetchMany := c.fetchMany
	if fetchMany == nil {
		fetchMany = func(ctx context.Context, relays []string, filter gonostr.Filter, opts gonostr.SubscriptionOptions) <-chan gonostr.RelayEvent {
			return c.pool.FetchMany(ctx, relays, filter, opts)
		}
	}
	for _, filter := range filters {
		ch := fetchMany(queryCtx, relays, filter, gonostr.SubscriptionOptions{})
		if ch == nil {
			return nil, fmt.Errorf("failed to create subscription")
		}
		for ie := range ch {
			ev := ie.Event
			if ev.ID == gonostr.ZeroID {
				continue
			}
			id := ev.ID.Hex()
			if _, exists := seen[id]; exists {
				continue
			}
			seen[id] = struct{}{}
			evCopy := ev
			out = append(out, &evCopy)
		}
	}

	if ctx.Err() != nil {
		return out, ctx.Err()
	}
	if err := queryCtx.Err(); err != nil {
		return out, &PartialFetchError{Cause: err, EventCount: len(out)}
	}
	return out, nil
}
