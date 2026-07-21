package nostr

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	gonostr "fiatjaf.com/nostr"
)

type outboxTestSigner struct{}

func (outboxTestSigner) SignEvent(_ context.Context, event *gonostr.Event) error {
	event.ID = event.GetID()
	return nil
}

type outboxTestTransport struct {
	mu       sync.Mutex
	failures map[string]error
	calls    map[string]int
}

type timeoutTransport struct{}

func (timeoutTransport) Publish(ctx context.Context, _ string, _ gonostr.Event) error {
	<-ctx.Done()
	return ctx.Err()
}

func (t *outboxTestTransport) Publish(_ context.Context, relayURL string, _ gonostr.Event) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.calls == nil {
		t.calls = make(map[string]int)
	}
	t.calls[relayURL]++
	return t.failures[relayURL]
}

func (t *outboxTestTransport) setFailure(relayURL string, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.failures == nil {
		t.failures = make(map[string]error)
	}
	t.failures[relayURL] = err
}

func (t *outboxTestTransport) callCount(relayURL string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.calls[relayURL]
}

func newTestReliablePublisher(t *testing.T, path string, clock *time.Time, transport RelayTransport, required, mirrors []string, quorum int) *ReliablePublisher {
	t.Helper()
	publisher, err := NewReliablePublisher(ReliablePublisherOptions{
		RequiredRelays:      required,
		MirrorRelays:        mirrors,
		AckQuorum:           quorum,
		OutboxPath:          path,
		PublishTimeout:      time.Second,
		BaseBackoff:         time.Second,
		MaxBackoff:          time.Minute,
		MaxAttempts:         3,
		CircuitFailureLimit: 10,
		transport:           transport,
		now:                 func() time.Time { return *clock },
		jitter:              func() float64 { return 1 },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(publisher.Close)
	return publisher
}

func TestReliablePublisherMirrorFailureDoesNotBlockQuorumAndRecoverySkipsAcknowledgedRelays(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	path := filepath.Join(t.TempDir(), "state", "outbox.json")
	transport := &outboxTestTransport{failures: map[string]error{"wss://mirror": errors.New("mirror unavailable")}}
	publisher := newTestReliablePublisher(t, path, &now, transport, []string{"wss://ledger-1", "wss://ledger-2"}, []string{"wss://mirror"}, 2)
	event := &gonostr.Event{Kind: 30900, CreatedAt: 100, Content: "state"}

	reports, err := publisher.PublishWithReport(context.Background(), outboxTestSigner{}, []*gonostr.Event{event})
	if err != nil {
		t.Fatalf("mirror failure blocked required quorum: %v", err)
	}
	if len(reports) != 1 || !reports[0].QuorumReached || !reports[0].Queued || reports[0].RequiredAcks != 2 {
		t.Fatalf("unexpected publication report: %#v", reports)
	}
	entries, err := publisher.store.List()
	if err != nil || len(entries) != 1 {
		t.Fatalf("failed mirror should remain queued: entries=%#v err=%v", entries, err)
	}

	transport.setFailure("wss://mirror", nil)
	now = now.Add(2 * time.Second)
	if _, err := publisher.DrainOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	entries, err = publisher.store.List()
	if err != nil || len(entries) != 0 {
		t.Fatalf("recovered mirror should drain outbox: entries=%#v err=%v", entries, err)
	}
	if got := transport.callCount("wss://ledger-1"); got != 1 {
		t.Fatalf("acknowledged ledger relay retried %d times", got)
	}
	if got := transport.callCount("wss://ledger-2"); got != 1 {
		t.Fatalf("acknowledged ledger relay retried %d times", got)
	}
	if got := transport.callCount("wss://mirror"); got != 2 {
		t.Fatalf("mirror attempts=%d, want 2", got)
	}
}

func TestReliablePublisherSubQuorumRemainsQueued(t *testing.T) {
	now := time.Unix(200, 0).UTC()
	path := filepath.Join(t.TempDir(), "outbox.json")
	transport := &outboxTestTransport{failures: map[string]error{"wss://ledger-2": errors.New("down")}}
	publisher := newTestReliablePublisher(t, path, &now, transport, []string{"wss://ledger-1", "wss://ledger-2"}, nil, 2)

	reports, err := publisher.PublishWithReport(context.Background(), outboxTestSigner{}, []*gonostr.Event{{Kind: 30900, CreatedAt: 200, Content: "state"}})
	var quorumErr *QuorumError
	if !errors.As(err, &quorumErr) {
		t.Fatalf("expected quorum error, got reports=%#v err=%v", reports, err)
	}
	if len(reports) != 1 || reports[0].QuorumReached || reports[0].RequiredAcks != 1 {
		t.Fatalf("unexpected sub-quorum report: %#v", reports)
	}
	entries, err := publisher.store.List()
	if err != nil || len(entries) != 1 || entries[0].QuorumReached {
		t.Fatalf("sub-quorum event was not durably queued: entries=%#v err=%v", entries, err)
	}
}

func TestReliablePublisherRestartDrainsOnlyMissingRelay(t *testing.T) {
	now := time.Unix(300, 0).UTC()
	path := filepath.Join(t.TempDir(), "outbox.json")
	firstTransport := &outboxTestTransport{failures: map[string]error{"wss://ledger-2": errors.New("down")}}
	first := newTestReliablePublisher(t, path, &now, firstTransport, []string{"wss://ledger-1", "wss://ledger-2"}, nil, 2)
	event := &gonostr.Event{Kind: 30900, CreatedAt: 300, Content: "restart"}
	if _, err := first.PublishWithReport(context.Background(), outboxTestSigner{}, []*gonostr.Event{event}); err == nil {
		t.Fatal("expected initial sub-quorum error")
	}
	eventID := event.ID.Hex()
	first.Close()

	now = now.Add(2 * time.Second)
	secondTransport := &outboxTestTransport{failures: map[string]error{}}
	second := newTestReliablePublisher(t, path, &now, secondTransport, []string{"wss://ledger-1", "wss://ledger-2"}, nil, 2)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- second.Run(ctx) }()
	deadline := time.Now().Add(time.Second)
	for secondTransport.callCount("wss://ledger-2") == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("outbox worker stopped with %v", err)
	}
	entries, err := second.store.List()
	if err != nil || len(entries) != 0 {
		t.Fatalf("restart did not drain outbox: entries=%#v err=%v", entries, err)
	}
	if got := secondTransport.callCount("wss://ledger-1"); got != 0 {
		t.Fatalf("restart retried already acknowledged relay %d times", got)
	}
	if got := secondTransport.callCount("wss://ledger-2"); got != 1 {
		t.Fatalf("restart missing-relay attempts=%d, want 1", got)
	}
	if event.ID.Hex() != eventID {
		t.Fatalf("logical event id changed across recovery: got %s want %s", event.ID.Hex(), eventID)
	}
}

func TestReliablePublisherTimeoutDeadLettersAndOperatorRetryResetsState(t *testing.T) {
	now := time.Now().UTC()
	path := filepath.Join(t.TempDir(), "outbox.json")
	publisher, err := NewReliablePublisher(ReliablePublisherOptions{
		RequiredRelays:      []string{"wss://slow"},
		AckQuorum:           1,
		OutboxPath:          path,
		PublishTimeout:      5 * time.Millisecond,
		BaseBackoff:         time.Second,
		MaxBackoff:          time.Second,
		MaxAttempts:         1,
		CircuitFailureLimit: 1,
		transport:           timeoutTransport{},
		now:                 func() time.Time { return now },
		jitter:              func() float64 { return 1 },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer publisher.Close()
	event := &gonostr.Event{Kind: 30900, CreatedAt: gonostr.Timestamp(now.Unix()), Content: "timeout"}
	reports, err := publisher.PublishWithReport(context.Background(), outboxTestSigner{}, []*gonostr.Event{event})
	if err == nil || len(reports) != 1 || !reports[0].DeadLetter || len(reports[0].Results) != 1 {
		t.Fatalf("timeout was not reported as dead letter: reports=%#v err=%v", reports, err)
	}
	if !strings.Contains(reports[0].Results[0].Error, "deadline exceeded") {
		t.Fatalf("timeout result missing deadline: %#v", reports[0].Results[0])
	}
	dead, err := publisher.store.DeadLetters()
	if err != nil || len(dead) != 1 {
		t.Fatalf("dead-letter list=%#v err=%v", dead, err)
	}
	count, err := publisher.store.Retry(event.ID.Hex())
	if err != nil || count != 1 {
		t.Fatalf("retry count=%d err=%v", count, err)
	}
	entries, err := publisher.store.List()
	if err != nil || len(entries) != 1 || entries[0].State != OutboxPending || entries[0].Deliveries[0].Attempts != 0 || entries[0].Deliveries[0].DeadLetter {
		t.Fatalf("retry did not reset dead letter: entries=%#v err=%v", entries, err)
	}
}

func TestReliablePublisherCircuitBreakerSkipsUnhealthyRelayUntilCooldown(t *testing.T) {
	now := time.Unix(400, 0).UTC()
	path := filepath.Join(t.TempDir(), "outbox.json")
	transport := &outboxTestTransport{failures: map[string]error{"wss://ledger": errors.New("down")}}
	publisher, err := NewReliablePublisher(ReliablePublisherOptions{
		RequiredRelays:      []string{"wss://ledger"},
		AckQuorum:           1,
		OutboxPath:          path,
		PublishTimeout:      time.Second,
		BaseBackoff:         time.Second,
		MaxBackoff:          time.Second,
		MaxAttempts:         3,
		CircuitFailureLimit: 1,
		CircuitCooldown:     30 * time.Second,
		transport:           transport,
		now:                 func() time.Time { return now },
		jitter:              func() float64 { return 1 },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer publisher.Close()
	if _, err := publisher.PublishWithReport(context.Background(), outboxTestSigner{}, []*gonostr.Event{{Kind: 30900, CreatedAt: 400}}); err == nil {
		t.Fatal("expected initial quorum failure")
	}
	now = now.Add(2 * time.Second)
	reports, err := publisher.DrainOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if transport.callCount("wss://ledger") != 1 || len(reports) != 1 || !reports[0].Results[0].CircuitOpen {
		t.Fatalf("open circuit did not suppress relay attempt: calls=%d reports=%#v", transport.callCount("wss://ledger"), reports)
	}
	now = now.Add(30 * time.Second)
	if _, err := publisher.DrainOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if transport.callCount("wss://ledger") != 2 {
		t.Fatalf("relay was not retried after cooldown: calls=%d", transport.callCount("wss://ledger"))
	}
}
