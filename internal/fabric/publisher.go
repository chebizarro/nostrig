package fabric

import (
	"context"
	"fmt"
	"reflect"

	gonostr "github.com/nbd-wtf/go-nostr"
)

// Signer is the Signet boundary. Implementations use a NIP-46 bunker (or a
// Signet local socket); nostrig intentionally provides no raw-key signer.
type Signer interface {
	PublicKey(ctx context.Context) (string, error)
	SignEvent(ctx context.Context, unsigned *gonostr.Event) (*gonostr.Event, error)
}

type Relay interface {
	Publish(ctx context.Context, event gonostr.Event) error
}

type Publisher struct {
	Signer Signer
	Relays []Relay
}

func (p *Publisher) Publish(ctx context.Context, events []*gonostr.Event) ([]*gonostr.Event, error) {
	if p == nil || p.Signer == nil {
		return nil, fmt.Errorf("Signet signer is required")
	}
	if len(p.Relays) == 0 {
		return nil, fmt.Errorf("at least one relay is required")
	}
	pubkey, err := p.Signer.PublicKey(ctx)
	if err != nil {
		return nil, fmt.Errorf("Signet public key: %w", err)
	}
	signed := make([]*gonostr.Event, 0, len(events))
	for _, unsigned := range events {
		if unsigned == nil {
			continue
		}
		if unsigned.PubKey != pubkey {
			return nil, fmt.Errorf("event pubkey %q does not match Signet identity %q", unsigned.PubKey, pubkey)
		}
		ev, err := p.Signer.SignEvent(ctx, unsigned)
		if err != nil {
			return nil, fmt.Errorf("Signet sign %s: %w", eventD(unsigned), err)
		}
		if ev == nil || ev.ID == "" || ev.Sig == "" {
			return nil, fmt.Errorf("Signet returned incomplete signed event %s", eventD(unsigned))
		}
		if ev.PubKey != unsigned.PubKey || ev.CreatedAt != unsigned.CreatedAt || ev.Kind != unsigned.Kind || ev.Content != unsigned.Content || !reflect.DeepEqual(ev.Tags, unsigned.Tags) {
			return nil, fmt.Errorf("Signet changed unsigned event %s", eventD(unsigned))
		}
		if err := ValidateSignedEvent(ev, pubkey); err != nil {
			return nil, fmt.Errorf("Signet returned invalid event %s: %w", eventD(unsigned), err)
		}
		for _, relay := range p.Relays {
			if err := relay.Publish(ctx, *ev); err != nil {
				return nil, fmt.Errorf("publish %s: %w", eventD(ev), err)
			}
		}
		signed = append(signed, ev)
	}
	return signed, nil
}

func eventD(ev *gonostr.Event) string {
	if d, ok := fnTagD(ev); ok {
		return d
	}
	return "<unaddressed>"
}
func fnTagD(ev *gonostr.Event) (string, bool) {
	for _, t := range ev.Tags {
		if len(t) > 1 && t[0] == "d" {
			return t[1], true
		}
	}
	return "", false
}
