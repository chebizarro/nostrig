package nostr

import (
	"context"
	"errors"
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

func mustID(n byte) gonostr.ID {
	var id gonostr.ID
	id[31] = n
	return id
}
