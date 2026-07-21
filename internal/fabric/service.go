package fabric

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"time"

	beadspb "github.com/chebizarro/nostrig/gen/beads"
	gonostr "github.com/nbd-wtf/go-nostr"
	"google.golang.org/protobuf/proto"
)

// Store is the durable bd-facing side of the adapter. Implementations persist
// the protobuf ledger, processed intent IDs, and the last outbound model hash.
type Store interface {
	Load(context.Context) (*beadspb.Export, error)
	Save(context.Context, *beadspb.Export) error
	Seen(context.Context, string) (bool, error)
	MarkSeen(context.Context, string) error
	OutboundDigest(context.Context) (string, error)
	SetOutboundDigest(context.Context, string) error
}

// Source is the relay-facing side. Snapshot returns 30900/NIP-51 state and
// Intents returns addressed 25910 commands. Implementations should query from
// relay history so restart performs catch-up rather than only live subscribe.
type Source interface {
	Snapshot(context.Context, string) ([]*gonostr.Event, error)
	Intents(context.Context, string) ([]*gonostr.Event, error)
}

type Service struct {
	Store     Store
	Source    Source
	Publisher *Publisher
	PubKey    string
	Interval  time.Duration
}

// SyncOnce performs both directions: changed local bd state is published,
// relay state is materialized locally, then unseen ContextVM mutations are
// applied and projected back to the relay.
func (s *Service) SyncOnce(ctx context.Context) error {
	if s == nil || s.Store == nil || s.Source == nil || s.Publisher == nil || s.PubKey == "" {
		return fmt.Errorf("fabric service is not configured")
	}
	local, err := s.Store.Load(ctx)
	if err != nil {
		return fmt.Errorf("load local ledger: %w", err)
	}
	if local == nil {
		local = &beadspb.Export{}
	}
	localDigest := modelDigest(local)
	previous, err := s.Store.OutboundDigest(ctx)
	if err != nil {
		return err
	}

	// Fetch relay history before publishing. The durable digest is the last
	// common model: it lets us distinguish a local edit from relay catch-up and
	// fail closed instead of overwriting concurrent remote work with stale local
	// state on restart.
	stateEvents, err := s.Source.Snapshot(ctx, s.PubKey)
	if err != nil {
		return fmt.Errorf("fetch relay snapshot: %w", err)
	}
	var remote *beadspb.Export
	if len(stateEvents) > 0 {
		remote, err = DecodeVerified(stateEvents, s.PubKey)
		if err != nil {
			return err
		}
	}
	remoteDigest := ""
	if remote != nil {
		remoteDigest = modelDigest(remote)
	}
	localChanged := localDigest != previous
	remoteChanged := remote != nil && remoteDigest != previous
	if previous == "" && remote != nil && emptyExport(local) {
		local = remote
		localDigest = remoteDigest
		localChanged = false
		remoteChanged = false
	} else if previous == "" && remote != nil && localDigest == remoteDigest {
		localChanged = false
		remoteChanged = false
	} else if localChanged && remoteChanged && localDigest != remoteDigest {
		return fmt.Errorf("task-fabric conflict: local and relay changed since last common digest")
	} else if remoteChanged {
		local = remote
		localDigest = remoteDigest
	} else if localChanged {
		if err := s.publishExport(ctx, local); err != nil {
			return err
		}
	}

	intents, err := s.Source.Intents(ctx, s.PubKey)
	if err != nil {
		return fmt.Errorf("fetch intents: %w", err)
	}
	sort.Slice(intents, func(i, j int) bool {
		if intents[i].CreatedAt != intents[j].CreatedAt {
			return intents[i].CreatedAt < intents[j].CreatedAt
		}
		return intents[i].ID < intents[j].ID
	})
	for _, intent := range intents {
		if intent == nil || intent.ID == "" {
			continue
		}
		seen, err := s.Store.Seen(ctx, intent.ID)
		if err != nil {
			return err
		}
		if seen {
			continue
		}
		var taskID string
		local, taskID, err = ApplyIntent(local, intent, s.PubKey)
		if err != nil {
			return fmt.Errorf("apply intent %s: %w", intent.ID, err)
		}
		issue := findIssue(local, taskID)
		projection, err := Encode(&beadspb.Export{Issues: []*beadspb.Issue{issue}}, s.PubKey, time.Now().UTC())
		if err != nil {
			return err
		}
		if _, err := s.Publisher.Publish(ctx, projection); err != nil {
			return err
		}
		if err := s.Store.MarkSeen(ctx, intent.ID); err != nil {
			return err
		}
	}
	if err := s.Store.Save(ctx, local); err != nil {
		return fmt.Errorf("save local ledger: %w", err)
	}
	if err := s.Store.SetOutboundDigest(ctx, modelDigest(local)); err != nil {
		return err
	}
	return nil
}

func (s *Service) publishExport(ctx context.Context, export *beadspb.Export) error {
	events, err := Encode(export, s.PubKey, time.Now().UTC())
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}
	_, err = s.Publisher.Publish(ctx, events)
	return err
}

func emptyExport(export *beadspb.Export) bool {
	return export == nil || len(export.Issues) == 0 && len(export.Epics) == 0
}

func (s *Service) Run(ctx context.Context) error {
	interval := s.Interval
	if interval <= 0 {
		interval = 15 * time.Second
	}
	if err := s.SyncOnce(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.SyncOnce(ctx); err != nil {
				return err
			}
		}
	}
}

func modelDigest(export *beadspb.Export) string {
	b, _ := proto.MarshalOptions{Deterministic: true}.Marshal(export)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
