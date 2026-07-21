package nostr

import (
	"context"
	"fmt"
	"time"

	gonostr "fiatjaf.com/nostr"
)

// Event aliases fiatjaf.com's nostr event type for Cascadia callers.
type Event = gonostr.Event

type Filter = gonostr.Filter
type Relay = gonostr.Relay
type Subscription = gonostr.Subscription
type Pool = gonostr.Pool
type Keyer = gonostr.Keyer
type PubKey = gonostr.PubKey
type SecretKey = gonostr.SecretKey
type ID = gonostr.ID
type Kind = gonostr.Kind
type Tags = gonostr.Tags
type Tag = gonostr.Tag
type RelayEvent = gonostr.RelayEvent
type SubscriptionOptions = gonostr.SubscriptionOptions

// Signer is the minimal signing/encryption contract used by Cascadia transports.
type Signer interface {
	gonostr.Keyer
}

func NewPool() *Pool { return gonostr.NewPool() }

func PubKeyFromHex(s string) (PubKey, error)       { return gonostr.PubKeyFromHex(s) }
func SecretKeyFromHex(s string) (SecretKey, error) { return gonostr.SecretKeyFromHex(s) }
func GenerateSecretKey() SecretKey                 { return gonostr.Generate() }

// BuildEvent creates and signs a nostr event.
func BuildEvent(ctx context.Context, signer Signer, kind int, tags gonostr.Tags, content string) (*Event, error) {
	if signer == nil {
		return nil, fmt.Errorf("nostr signer is nil")
	}
	pubkey, err := signer.GetPublicKey(ctx)
	if err != nil {
		return nil, err
	}
	event := &Event{Kind: gonostr.Kind(kind), CreatedAt: gonostr.Timestamp(time.Now().Unix()), Tags: tags, Content: content, PubKey: pubkey}
	if err := signer.SignEvent(ctx, event); err != nil {
		return nil, err
	}
	return event, nil
}

// EventID returns the canonical event id for an unsigned/signed event.
func EventID(event *Event) ID {
	if event == nil {
		return gonostr.ZeroID
	}
	return event.GetID()
}

// VerifyEvent checks event id and signature.
func VerifyEvent(event *Event) bool {
	if event == nil || event.ID == gonostr.ZeroID || event.ID != event.GetID() {
		return false
	}
	return event.VerifySignature()
}

// Publish sends an event to all relay URLs using a fiatjaf relay pool and returns the accepted count.
func Publish(ctx context.Context, relayURLs []string, event Event) (int, error) {
	pool := gonostr.NewPool()
	defer pool.Close("cascadia publish complete")
	return PublishWithPool(ctx, pool, relayURLs, event)
}

// PublishWithPool sends an event through an existing fiatjaf relay pool and returns the accepted count.
func PublishWithPool(ctx context.Context, pool *Pool, relayURLs []string, event Event) (int, error) {
	if pool == nil {
		return 0, fmt.Errorf("nostr pool is nil")
	}
	accepted := 0
	var lastErr error
	for result := range pool.PublishMany(ctx, relayURLs, event) {
		if result.Error != nil {
			lastErr = result.Error
			continue
		}
		accepted++
	}
	if accepted == 0 && lastErr != nil {
		return 0, lastErr
	}
	return accepted, nil
}

// Subscribe opens subscriptions against every relay URL using a fiatjaf relay pool.
func Subscribe(ctx context.Context, relayURLs []string, filter Filter, opts SubscriptionOptions) (*Pool, chan RelayEvent) {
	pool := gonostr.NewPool()
	return pool, pool.SubscribeMany(ctx, relayURLs, filter, opts)
}

// SubscribeWithPool opens subscriptions against every relay URL through an existing fiatjaf relay pool.
func SubscribeWithPool(ctx context.Context, pool *Pool, relayURLs []string, filter Filter, opts SubscriptionOptions) (chan RelayEvent, error) {
	if pool == nil {
		return nil, fmt.Errorf("nostr pool is nil")
	}
	return pool.SubscribeMany(ctx, relayURLs, filter, opts), nil
}
