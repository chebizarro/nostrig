package fabric

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	beadspb "github.com/chebizarro/nostrig/gen/beads"
	fn "github.com/chebizarro/nostrig/internal/nostr"
	gonostr "github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/nip46"
	"google.golang.org/protobuf/encoding/protojson"
)

type RelaySource struct {
	Client *fn.Client
	Relays []string
}

func (s *RelaySource) Snapshot(ctx context.Context, author string) ([]*gonostr.Event, error) {
	if s == nil || len(s.Relays) == 0 {
		return nil, fmt.Errorf("relay source is not configured")
	}
	client := s.Client
	if client == nil {
		client = fn.NewClient()
	}
	return client.Fetch(ctx, s.Relays, gonostr.Filter{Kinds: []int{fn.KindTaskState, fn.KindNIP51Set, gonostr.KindDeletion}, Authors: []string{author}})
}

func (s *RelaySource) Intents(ctx context.Context, recipient string) ([]*gonostr.Event, error) {
	if s == nil || len(s.Relays) == 0 {
		return nil, fmt.Errorf("relay source is not configured")
	}
	client := s.Client
	if client == nil {
		client = fn.NewClient()
	}
	return client.Fetch(ctx, s.Relays, gonostr.Filter{Kinds: []int{fn.KindIntent}, Tags: gonostr.TagMap{"p": []string{recipient}}})
}

func (s *RelaySource) SubscribeIntents(ctx context.Context, recipient string) (<-chan *gonostr.Event, error) {
	if s == nil || len(s.Relays) == 0 {
		return nil, fmt.Errorf("relay source is not configured")
	}
	pool := gonostr.NewSimplePool(ctx)
	now := gonostr.Now()
	stream := pool.SubMany(ctx, s.Relays, gonostr.Filters{{Kinds: []int{fn.KindIntent}, Tags: gonostr.TagMap{"p": []string{recipient}}, Since: &now}})
	out := make(chan *gonostr.Event)
	go func() {
		defer close(out)
		for item := range stream {
			if item.Event == nil {
				continue
			}
			select {
			case out <- item.Event:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

type WebsocketRelay struct{ URL string }

func (r WebsocketRelay) Publish(ctx context.Context, event gonostr.Event) error {
	if r.URL == "" {
		return fmt.Errorf("relay URL is required")
	}
	relay, err := gonostr.RelayConnect(ctx, r.URL)
	if err != nil {
		return err
	}
	defer relay.Close()
	return relay.Publish(ctx, event)
}

type BunkerSigner struct{ client *nip46.BunkerClient }

func NewBunkerSigner(ctx context.Context, bunkerURL string) (*BunkerSigner, error) {
	if bunkerURL == "" {
		return nil, fmt.Errorf("Signet bunker URL is required")
	}
	clientKey := gonostr.GeneratePrivateKey()
	client, err := nip46.ConnectBunker(ctx, clientKey, bunkerURL, nil, func(string) {})
	if err != nil {
		return nil, fmt.Errorf("connect Signet bunker: %w", err)
	}
	return &BunkerSigner{client: client}, nil
}
func (s *BunkerSigner) PublicKey(ctx context.Context) (string, error) {
	if s == nil || s.client == nil {
		return "", fmt.Errorf("Signet bunker is not connected")
	}
	return s.client.GetPublicKey(ctx)
}
func (s *BunkerSigner) SignEvent(ctx context.Context, unsigned *gonostr.Event) (*gonostr.Event, error) {
	if s == nil || s.client == nil || unsigned == nil {
		return nil, fmt.Errorf("Signet bunker and event are required")
	}
	copy := *unsigned
	if err := s.client.SignEvent(ctx, &copy); err != nil {
		return nil, err
	}
	return &copy, nil
}

type storeMetadata struct {
	Seen           map[string]bool `json:"seen"`
	OutboundDigest string          `json:"outbound_digest"`
}

// JSONStore is a crash-safe durable adapter store. State is protobuf JSON and
// delivery metadata is kept beside it; writes use rename within one directory.
type JSONStore struct {
	Path string
	mu   sync.Mutex
}

func (s *JSONStore) Load(context.Context) (*beadspb.Export, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.Path)
	if os.IsNotExist(err) {
		return &beadspb.Export{}, nil
	}
	if err != nil {
		return nil, err
	}
	out := new(beadspb.Export)
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(data, out); err != nil {
		return nil, err
	}
	return out, nil
}
func (s *JSONStore) Save(_ context.Context, value *beadspb.Export) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := (protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}).Marshal(value)
	if err != nil {
		return err
	}
	return atomicWrite(s.Path, data, 0600)
}
func (s *JSONStore) Seen(_ context.Context, id string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	meta, err := s.loadMeta()
	return meta.Seen[id], err
}
func (s *JSONStore) MarkSeen(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	meta, err := s.loadMeta()
	if err != nil {
		return err
	}
	meta.Seen[id] = true
	return s.saveMeta(meta)
}
func (s *JSONStore) OutboundDigest(context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	meta, err := s.loadMeta()
	return meta.OutboundDigest, err
}
func (s *JSONStore) SetOutboundDigest(_ context.Context, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	meta, err := s.loadMeta()
	if err != nil {
		return err
	}
	meta.OutboundDigest = value
	return s.saveMeta(meta)
}
func (s *JSONStore) metadataPath() string { return s.Path + ".fabric-meta.json" }
func (s *JSONStore) loadMeta() (storeMetadata, error) {
	meta := storeMetadata{Seen: map[string]bool{}}
	data, err := os.ReadFile(s.metadataPath())
	if os.IsNotExist(err) {
		return meta, nil
	}
	if err != nil {
		return meta, err
	}
	if err := json.Unmarshal(data, &meta); err != nil {
		return meta, err
	}
	if meta.Seen == nil {
		meta.Seen = map[string]bool{}
	}
	return meta, nil
}
func (s *JSONStore) saveMeta(meta storeMetadata) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return atomicWrite(s.metadataPath(), data, 0600)
}

func atomicWrite(path string, data []byte, mode os.FileMode) error {
	if path == "" {
		return fmt.Errorf("store path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".nostrig-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}
