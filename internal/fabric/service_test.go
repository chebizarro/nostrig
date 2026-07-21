package fabric

import (
	"context"
	"strings"
	"testing"
	"time"

	beadspb "github.com/chebizarro/nostrig/gen/beads"
	gonostr "github.com/nbd-wtf/go-nostr"
	"google.golang.org/protobuf/proto"
)

type memoryStore struct {
	ledger *beadspb.Export
	seen   map[string]bool
	digest string
}

func (s *memoryStore) Load(context.Context) (*beadspb.Export, error) {
	return proto.Clone(s.ledger).(*beadspb.Export), nil
}
func (s *memoryStore) Save(_ context.Context, v *beadspb.Export) error {
	s.ledger = proto.Clone(v).(*beadspb.Export)
	return nil
}
func (s *memoryStore) Seen(_ context.Context, id string) (bool, error)     { return s.seen[id], nil }
func (s *memoryStore) MarkSeen(_ context.Context, id string) error         { s.seen[id] = true; return nil }
func (s *memoryStore) OutboundDigest(context.Context) (string, error)      { return s.digest, nil }
func (s *memoryStore) SetOutboundDigest(_ context.Context, v string) error { s.digest = v; return nil }

type memorySource struct{ state, intents []*gonostr.Event }

func (s *memorySource) Snapshot(context.Context, string) ([]*gonostr.Event, error) {
	return s.state, nil
}
func (s *memorySource) Intents(context.Context, string) ([]*gonostr.Event, error) {
	return s.intents, nil
}

func TestServiceBidirectionalCatchupAndLoopPrevention(t *testing.T) {
	secret := gonostr.GeneratePrivateKey()
	pub, _ := gonostr.GetPublicKey(secret)
	actor := gonostr.GeneratePrivateKey()
	store := &memoryStore{ledger: &beadspb.Export{Issues: []*beadspb.Issue{{Id: "fp-50", Status: beadspb.Status_STATUS_OPEN}}}, seen: map[string]bool{}}
	relay := new(recordingRelay)
	source := &memorySource{intents: []*gonostr.Event{signedIntent(t, actor, pub, "task/update", map[string]any{"id": "fp-50", "status": "in_progress", "title": "caught up"})}}
	service := &Service{Store: store, Source: source, Publisher: &Publisher{Signer: testSigner{key: secret}, Relays: []Relay{relay}}, PubKey: pub}

	if err := service.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.ledger.Issues[0].Title != "caught up" || store.ledger.Issues[0].Status != beadspb.Status_STATUS_IN_PROGRESS {
		t.Fatalf("relay intent not materialized locally: %v", store.ledger.Issues[0])
	}
	if len(relay.events) != 2 {
		t.Fatalf("want local outbound + intent projection, got %d", len(relay.events))
	}
	if err := service.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(relay.events) != 2 {
		t.Fatalf("replay loop/duplicate publication: got %d events", len(relay.events))
	}

	// A new process with the same durable Store replays relay history but skips
	// the already recorded intent and unchanged outbound state.
	restarted := &Service{Store: store, Source: source, Publisher: service.Publisher, PubKey: pub, Interval: time.Millisecond}
	if err := restarted.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(relay.events) != 2 {
		t.Fatalf("restart catch-up duplicated publication: got %d", len(relay.events))
	}
}

func TestServiceHydratesRelayBeforePublishingLocalBootstrap(t *testing.T) {
	secret := gonostr.GeneratePrivateKey()
	pub, _ := gonostr.GetPublicKey(secret)
	remote := &beadspb.Export{Issues: []*beadspb.Issue{{Id: "fp-remote", Title: "relay wins bootstrap"}}}
	state, err := Encode(remote, pub, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range state {
		if err := ev.Sign(secret); err != nil {
			t.Fatal(err)
		}
	}
	store := &memoryStore{ledger: &beadspb.Export{}, seen: map[string]bool{}}
	relay := new(recordingRelay)
	service := &Service{
		Store: store, Source: &memorySource{state: state},
		Publisher: &Publisher{Signer: testSigner{key: secret}, Relays: []Relay{relay}}, PubKey: pub,
	}
	if err := service.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(relay.events) != 0 {
		t.Fatalf("bootstrap echoed relay state: %d publishes", len(relay.events))
	}
	if len(store.ledger.Issues) != 1 || store.ledger.Issues[0].Id != "fp-remote" {
		t.Fatalf("relay state not hydrated: %v", store.ledger)
	}
}

func TestServiceFailsClosedOnConcurrentLocalAndRelayChanges(t *testing.T) {
	secret := gonostr.GeneratePrivateKey()
	pub, _ := gonostr.GetPublicKey(secret)
	base := &beadspb.Export{Issues: []*beadspb.Issue{{Id: "fp-conflict", Title: "base"}}}
	local := &beadspb.Export{Issues: []*beadspb.Issue{{Id: "fp-conflict", Title: "local"}}}
	remote := &beadspb.Export{Issues: []*beadspb.Issue{{Id: "fp-conflict", Title: "remote"}}}
	state, err := Encode(remote, pub, time.Unix(101, 0))
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range state {
		if err := ev.Sign(secret); err != nil {
			t.Fatal(err)
		}
	}
	store := &memoryStore{ledger: local, seen: map[string]bool{}, digest: modelDigest(base)}
	relay := new(recordingRelay)
	service := &Service{
		Store: store, Source: &memorySource{state: state},
		Publisher: &Publisher{Signer: testSigner{key: secret}, Relays: []Relay{relay}}, PubKey: pub,
	}
	err = service.SyncOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("expected fail-closed conflict, got %v", err)
	}
	if len(relay.events) != 0 {
		t.Fatalf("conflict published stale state: %d events", len(relay.events))
	}
}
