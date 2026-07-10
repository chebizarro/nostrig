package nostr

import (
	"context"
	"fmt"
	"strings"

	gonostr "fiatjaf.com/nostr"
	"fiatjaf.com/nostr/nip46"
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
	sk, err := gonostr.SecretKeyFromHex(s.PrivateKey)
	if err != nil {
		return fmt.Errorf("parse private key: %w", err)
	}
	return ev.Sign(sk)
}

func (s *PrivateKeySigner) PublicKey(ctx context.Context) (string, error) {
	if ctx == nil {
		return "", fmt.Errorf("context is nil")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if s == nil || strings.TrimSpace(s.PrivateKey) == "" {
		return "", fmt.Errorf("private key signer is not configured")
	}
	sk, err := gonostr.SecretKeyFromHex(s.PrivateKey)
	if err != nil {
		return "", fmt.Errorf("parse private key: %w", err)
	}
	return sk.Public().Hex(), nil
}

// PublicKeyProvider is implemented by signers that can report the Nostr public
// key that will ultimately sign events.
type PublicKeyProvider interface {
	PublicKey(ctx context.Context) (string, error)
}

// NIP46Signer wraps a NIP-46/Signet bunker client behind the local Signer
// interface used by publishers and ContextVM command dispatchers.
type NIP46Signer struct {
	client *nip46.BunkerClient
}

func NewNIP46Signer(client *nip46.BunkerClient) (*NIP46Signer, error) {
	if client == nil {
		return nil, fmt.Errorf("nip46 bunker client is required")
	}
	return &NIP46Signer{client: client}, nil
}

func ConnectNIP46Signer(ctx context.Context, bunkerURL, clientSecretKey string) (*NIP46Signer, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is nil")
	}
	bunkerURL = strings.TrimSpace(bunkerURL)
	if bunkerURL == "" {
		return nil, fmt.Errorf("signer bunker url is required")
	}
	clientSecretKey = strings.TrimSpace(clientSecretKey)
	if clientSecretKey == "" {
		clientSecretKey = gonostr.Generate().Hex()
	}
	clientSK, err := gonostr.SecretKeyFromHex(clientSecretKey)
	if err != nil {
		return nil, fmt.Errorf("parse nip46 client secret key: %w", err)
	}
	client, err := nip46.ConnectBunker(ctx, clientSK, bunkerURL, nil, func(string) {})
	if err != nil {
		return nil, fmt.Errorf("connect nip46 signer: %w", err)
	}
	return NewNIP46Signer(client)
}

func (s *NIP46Signer) SignEvent(ctx context.Context, ev *gonostr.Event) error {
	if ctx == nil {
		return fmt.Errorf("context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil || s.client == nil {
		return fmt.Errorf("nip46 signer is not configured")
	}
	if ev == nil {
		return fmt.Errorf("event is nil")
	}
	return s.client.SignEvent(ctx, ev)
}

func (s *NIP46Signer) PublicKey(ctx context.Context) (string, error) {
	if ctx == nil {
		return "", fmt.Errorf("context is nil")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if s == nil || s.client == nil {
		return "", fmt.Errorf("nip46 signer is not configured")
	}
	pub, err := s.client.GetPublicKey(ctx)
	if err != nil {
		return "", err
	}
	return pub.Hex(), nil
}
