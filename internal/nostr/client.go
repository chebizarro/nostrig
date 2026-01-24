package nostr

import (
	"context"
	"fmt"
	"time"

	gonostr "github.com/nbd-wtf/go-nostr"
)

// Client wraps a go-nostr SimplePool and provides higher-level fetch helpers.
type Client struct {
	pool *gonostr.SimplePool
}

// NewClient creates a new nostr client using a SimplePool.
func NewClient() *Client {
	return &Client{
		pool: gonostr.NewSimplePool(context.Background()),
	}
}

// Fetch queries relays for events matching a single filter.
func (c *Client) Fetch(ctx context.Context, relays []string, f gonostr.Filter) ([]*gonostr.Event, error) {
	return c.FetchMany(ctx, relays, []gonostr.Filter{f})
}

// FetchMany queries relays for events matching multiple filters.
//
// It deduplicates events by ID across relays and filters.
// If ctx is cancelled, it returns any collected events plus ctx.Err().
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
	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	ch := c.pool.SubManyEose(queryCtx, relays, gonostr.Filters(filters))
	if ch == nil {
		return nil, fmt.Errorf("failed to create subscription")
	}

	seen := make(map[string]struct{}, 1024)
	out := make([]*gonostr.Event, 0, 256)

	for ie := range ch {
		ev := ie.Event
		if ev == nil || ev.ID == "" {
			continue
		}
		if _, exists := seen[ev.ID]; exists {
			continue
		}
		seen[ev.ID] = struct{}{}
		out = append(out, ev)
	}

	// Don't treat timeout as error if we got some results
	if ctx.Err() != nil && len(out) == 0 {
		return out, ctx.Err()
	}
	return out, nil
}