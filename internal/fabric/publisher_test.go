package fabric

import (
	"context"
	"errors"
	"testing"
	"time"

	gonostr "github.com/nbd-wtf/go-nostr"
)

type testSigner struct {
	secret string
	pub    string
	tamper bool
}

func (s testSigner) PublicKey(context.Context) (string, error) { return s.pub, nil }
func (s testSigner) SignEvent(_ context.Context, ev *gonostr.Event) (*gonostr.Event, error) {
	copy := *ev
	if err := copy.Sign(s.secret); err != nil {
		return nil, err
	}
	if s.tamper {
		copy.Content += "tampered"
	}
	return &copy, nil
}

type recordingRelay struct {
	events []gonostr.Event
	err    error
}

func (r *recordingRelay) Publish(_ context.Context, ev gonostr.Event) error {
	if r.err != nil {
		return r.err
	}
	r.events = append(r.events, ev)
	return nil
}

func TestPublisherVerifiesSignetAndPublishes(t *testing.T) {
	secret := gonostr.GeneratePrivateKey()
	pub, _ := gonostr.GetPublicKey(secret)
	event := &gonostr.Event{PubKey: pub, CreatedAt: gonostr.Timestamp(time.Now().Unix()), Kind: 30900,
		Tags: gonostr.Tags{{"d", "task:fp-50"}}, Content: `{}`}
	relay := new(recordingRelay)
	got, err := (&Publisher{Signer: testSigner{secret: secret, pub: pub}, Relays: []Relay{relay}}).Publish(context.Background(), []*gonostr.Event{event})
	if err != nil || len(got) != 1 || len(relay.events) != 1 {
		t.Fatalf("publish failed: signed=%d relayed=%d err=%v", len(got), len(relay.events), err)
	}
	ok, _ := got[0].CheckSignature()
	if !ok {
		t.Fatal("published event signature is invalid")
	}
}

func TestPublisherRejectsTamperedSignetResultAndRelayFailure(t *testing.T) {
	secret := gonostr.GeneratePrivateKey()
	pub, _ := gonostr.GetPublicKey(secret)
	event := &gonostr.Event{PubKey: pub, Kind: 30900, Tags: gonostr.Tags{{"d", "task:fp-50"}}}
	if _, err := (&Publisher{Signer: testSigner{secret: secret, pub: pub, tamper: true}, Relays: []Relay{new(recordingRelay)}}).Publish(context.Background(), []*gonostr.Event{event}); err == nil {
		t.Fatal("expected invalid Signet signature rejection")
	}
	if _, err := (&Publisher{Signer: testSigner{secret: secret, pub: pub}, Relays: []Relay{&recordingRelay{err: errors.New("down")}}}).Publish(context.Background(), []*gonostr.Event{event}); err == nil {
		t.Fatal("expected relay failure")
	}
}
