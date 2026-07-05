package nostr

import (
	"context"
	"fmt"
	"strings"

	gonostr "github.com/nbd-wtf/go-nostr"
)

// PrivateKeySigner signs events with a raw Nostr private key. It is intended as
// an explicit local-development fallback; production deployments should use
// Signet/NIP-46 signing.
type PrivateKeySigner struct {
	PrivateKey string
}

func NewPrivateKeySigner(privateKey string) (*PrivateKeySigner, error) {
	privateKey = strings.TrimSpace(privateKey)
	if privateKey == "" {
		return nil, fmt.Errorf("private key is required")
	}
	return &PrivateKeySigner{PrivateKey: privateKey}, nil
}

func (s *PrivateKeySigner) SignEvent(ctx context.Context, ev *gonostr.Event) error {
	if ctx == nil {
		return fmt.Errorf("context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil || strings.TrimSpace(s.PrivateKey) == "" {
		return fmt.Errorf("private key signer is not configured")
	}
	if ev == nil {
		return fmt.Errorf("event is nil")
	}
	return ev.Sign(s.PrivateKey)
}
