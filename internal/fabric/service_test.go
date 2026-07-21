package fabric

import (
	"context"
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
	service := &Service{Store: store, Source: source, Publisher: &Publisher{Signer: testSigner{secret: secret, pub: pub}, Relays: []Relay{relay}}, PubKey: pub}

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
