package nostr

import (
	"context"
	"errors"
	"fmt"
	"time"

	gonostr "fiatjaf.com/nostr"
)

// Client wraps a go-nostr SimplePool and provides higher-level fetch helpers.
type fetchManyFunc func(ctx context.Context, relays []string, filter gonostr.Filter, opts gonostr.SubscriptionOptions) <-chan gonostr.RelayEvent

type Client struct {
	pool         *gonostr.Pool
	fetchMany    fetchManyFunc
	queryTimeout time.Duration
}

// NewClient creates a new nostr client using a SimplePool.
func NewClient() *Client {
	pool := gonostr.NewPool()
	return &Client{
		pool:         pool,
		queryTimeout: 30 * time.Second,
		fetchMany: func(ctx context.Context, relays []string, filter gonostr.Filter, opts gonostr.SubscriptionOptions) <-chan gonostr.RelayEvent {
			return pool.FetchMany(ctx, relays, filter, opts)
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

	// Use SubManyEose which closes the channel after receiving EOSE from all relays
	// Add a timeout to avoid hanging if relays don't respond
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
