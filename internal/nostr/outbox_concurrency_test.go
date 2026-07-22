package nostr

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	gonostr "fiatjaf.com/nostr"
)

func TestConcurrentOutboxPublishSnapshotAndRecovery(t *testing.T) {
	const eventCount = 24
	now := time.Unix(500, 0).UTC()
	transport := &outboxTestTransport{failures: map[string]error{
		"wss://mirror": fmt.Errorf("mirror unavailable"),
	}}
	publisher, err := NewReliablePublisher(ReliablePublisherOptions{
		RequiredRelays:      []string{"wss://ledger-1", "wss://ledger-2"},
		MirrorRelays:        []string{"wss://mirror"},
		AckQuorum:           2,
		OutboxPath:          filepath.Join(t.TempDir(), "outbox.json"),
		PublishTimeout:      time.Second,
		BaseBackoff:         time.Second,
		MaxBackoff:          time.Minute,
		MaxAttempts:         3,
		CircuitFailureLimit: eventCount + 1,
		transport:           transport,
		now:                 func() time.Time { return now },
		jitter:              func() float64 { return 1 },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(publisher.Close)

	stopSnapshots := make(chan struct{})
	snapshotErrors := make(chan error, 1)
	var snapshots sync.WaitGroup
	snapshots.Add(1)
	go func() {
		defer snapshots.Done()
		for {
			select {
			case <-stopSnapshots:
				return
			default:
				if _, err := publisher.Snapshot(); err != nil {
					select {
					case snapshotErrors <- err:
					default:
					}
					return
				}
			}
		}
	}()

	start := make(chan struct{})
	publishErrors := make(chan error, eventCount)
	var publishes sync.WaitGroup
	for i := 0; i < eventCount; i++ {
		i := i
		publishes.Add(1)
		go func() {
			defer publishes.Done()
			<-start
			event := &gonostr.Event{
				ID:   numberedID(10_000 + i),
				Kind: 30900, CreatedAt: gonostr.Timestamp(now.Unix()),
				Content: fmt.Sprintf("event-%d", i),
			}
			if _, err := publisher.PublishSigned(context.Background(), event); err != nil {
				publishErrors <- err
			}
		}()
	}
	close(start)
	publishes.Wait()
	close(publishErrors)
	close(stopSnapshots)
	snapshots.Wait()

	for err := range publishErrors {
		t.Errorf("publish: %v", err)
	}
	select {
	case err := <-snapshotErrors:
		t.Fatalf("snapshot: %v", err)
	default:
	}

	entries, err := publisher.store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != eventCount {
		t.Fatalf("queued entries=%d, want %d", len(entries), eventCount)
	}
	for _, relay := range []string{"wss://ledger-1", "wss://ledger-2", "wss://mirror"} {
		if got := transport.callCount(relay); got != eventCount {
			t.Fatalf("%s initial calls=%d, want %d", relay, got, eventCount)
		}
	}

	transport.setFailure("wss://mirror", nil)
	now = now.Add(2 * time.Second)
	if _, err := publisher.DrainOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	entries, err = publisher.store.List()
	if err != nil || len(entries) != 0 {
		t.Fatalf("recovery left entries=%d err=%v", len(entries), err)
	}
	for _, relay := range []string{"wss://ledger-1", "wss://ledger-2"} {
		if got := transport.callCount(relay); got != eventCount {
			t.Fatalf("acknowledged relay %s retried: calls=%d", relay, got)
		}
	}
	if got := transport.callCount("wss://mirror"); got != eventCount*2 {
		t.Fatalf("mirror calls=%d, want %d", got, eventCount*2)
	}
}
