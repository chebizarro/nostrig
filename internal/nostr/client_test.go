package nostr

import (
	"context"
	"encoding/binary"
	"errors"
	"sort"
	"testing"
	"time"

	gonostr "fiatjaf.com/nostr"
)

func TestFetchManyReturnsPartialFetchErrorOnInternalTimeout(t *testing.T) {
	client := &Client{
		pool:         gonostr.NewPool(),
		queryTimeout: time.Millisecond,
		fetchMany: func(ctx context.Context, relays []string, filter gonostr.Filter, opts gonostr.SubscriptionOptions) <-chan gonostr.RelayEvent {
			ch := make(chan gonostr.RelayEvent)
			go func() {
				<-ctx.Done()
				close(ch)
			}()
			return ch
		},
	}
	events, err := client.FetchMany(context.Background(), []string{"wss://relay.example"}, []gonostr.Filter{{Kinds: []gonostr.Kind{1}}})
	if len(events) != 0 {
		t.Fatalf("expected no events, got %d", len(events))
	}
	if !IsPartialFetch(err) {
		t.Fatalf("expected partial fetch error, got %v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded cause, got %v", err)
	}
}

func TestFetchManyDeduplicatesEventsAcrossFilters(t *testing.T) {
	shared := gonostr.Event{ID: mustID(1)}
	client := &Client{
		pool: gonostr.NewPool(),
		fetchMany: func(ctx context.Context, relays []string, filter gonostr.Filter, opts gonostr.SubscriptionOptions) <-chan gonostr.RelayEvent {
			ch := make(chan gonostr.RelayEvent, 2)
			ch <- gonostr.RelayEvent{Event: shared}
			ch <- gonostr.RelayEvent{Event: shared}
			close(ch)
			return ch
		},
	}
	events, err := client.FetchMany(context.Background(), []string{"wss://relay.example"}, []gonostr.Filter{{Kinds: []gonostr.Kind{1}}, {Kinds: []gonostr.Kind{2}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("expected deduped single event, got %d", len(events))
	}
}

func TestFetchManyPaginatedReturnsMoreThanFiveHundredEvents(t *testing.T) {
	events := make([]gonostr.Event, 1201)
	for i := range events {
		events[i] = gonostr.Event{ID: numberedID(i + 1), Kind: 1, CreatedAt: gonostr.Timestamp(10_000 - i)}
	}
	client := paginatedTestClient(events)

	filter := gonostr.Filter{Kinds: []gonostr.Kind{1}, Limit: 500}
	got, err := client.FetchManyPaginated(context.Background(), []string{"wss://relay.example"}, []gonostr.Filter{filter}, PaginationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(events) {
		t.Fatalf("events=%d, want %d", len(got), len(events))
	}
	if filter.Limit != 500 || filter.Until != 0 || filter.Since != 0 {
		t.Fatalf("caller filter was mutated: %#v", filter)
	}
}

func TestFetchManyPaginatedReturnsExplicitTimestampTruncation(t *testing.T) {
	events := make([]gonostr.Event, 501)
	for i := range events {
		events[i] = gonostr.Event{ID: numberedID(i + 1), Kind: 1, CreatedAt: 42}
	}
	client := paginatedTestClient(events)
	_, err := client.FetchManyPaginated(context.Background(), []string{"wss://relay.example"}, []gonostr.Filter{{Kinds: []gonostr.Kind{1}}}, PaginationOptions{PageSize: 500})
	if !IsQueryTruncated(err) {
		t.Fatalf("expected explicit truncation, got %v", err)
	}
	var truncated *QueryTruncatedError
	if !errors.As(err, &truncated) || truncated.Reason != TruncatedAtTimestampBoundary {
		t.Fatalf("unexpected truncation: %#v", truncated)
	}
}

func TestFetchManyPaginatedEnforcesSafetyBounds(t *testing.T) {
	events := make([]gonostr.Event, 700)
	for i := range events {
		events[i] = gonostr.Event{ID: numberedID(i + 1), Kind: 1, CreatedAt: gonostr.Timestamp(10_000 - i)}
	}
	client := paginatedTestClient(events)
	_, err := client.FetchManyPaginated(context.Background(), []string{"wss://relay.example"}, []gonostr.Filter{{Kinds: []gonostr.Kind{1}}}, PaginationOptions{PageSize: 500, MaxEvents: 600})
	var truncated *QueryTruncatedError
	if !errors.As(err, &truncated) || truncated.Reason != TruncatedByEventLimit {
		t.Fatalf("expected event-limit truncation, got %v", err)
	}

	_, err = client.FetchManyPaginated(context.Background(), []string{"wss://relay.example"}, []gonostr.Filter{{Kinds: []gonostr.Kind{1}}}, PaginationOptions{PageSize: 500, MaxPages: 1})
	if !errors.As(err, &truncated) || truncated.Reason != TruncatedByPageLimit {
		t.Fatalf("expected page-limit truncation, got %v", err)
	}
}

func TestFetchManyPaginatedDeduplicatesAcrossRelays(t *testing.T) {
	events := []gonostr.Event{
		{ID: numberedID(1), Kind: 1, CreatedAt: 3},
		{ID: numberedID(2), Kind: 1, CreatedAt: 2},
		{ID: numberedID(3), Kind: 1, CreatedAt: 1},
	}
	client := paginatedTestClient(events)
	got, err := client.FetchManyPaginated(context.Background(), []string{"wss://one.example", "wss://two.example"}, []gonostr.Filter{{Kinds: []gonostr.Kind{1}}}, PaginationOptions{PageSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(events) {
		t.Fatalf("events=%d, want %d", len(got), len(events))
	}
}

func paginatedTestClient(events []gonostr.Event) *Client {
	pool := gonostr.NewPool()
	return &Client{
		pool: pool,
		fetchMany: func(ctx context.Context, relays []string, filter gonostr.Filter, opts gonostr.SubscriptionOptions) <-chan gonostr.RelayEvent {
			ch := make(chan gonostr.RelayEvent, len(events))
			matches := make([]gonostr.Event, 0, len(events))
			for _, ev := range events {
				if ev.Kind != 1 {
					continue
				}
				if filter.Since != 0 && ev.CreatedAt < filter.Since {
					continue
				}
				if filter.Until != 0 && ev.CreatedAt > filter.Until {
					continue
				}
				matches = append(matches, ev)
			}
			sort.Slice(matches, func(i, j int) bool {
				if matches[i].CreatedAt != matches[j].CreatedAt {
					return matches[i].CreatedAt > matches[j].CreatedAt
				}
				return matches[i].ID.Hex() > matches[j].ID.Hex()
			})
			if filter.Limit > 0 && len(matches) > filter.Limit {
				matches = matches[:filter.Limit]
			}
			for _, ev := range matches {
				ch <- gonostr.RelayEvent{Event: ev}
			}
			close(ch)
			return ch
		},
	}
}

func numberedID(n int) gonostr.ID {
	var id gonostr.ID
	binary.BigEndian.PutUint64(id[24:], uint64(n))
	return id
}

func mustID(n byte) gonostr.ID {
	var id gonostr.ID
	id[31] = n
	return id
}
