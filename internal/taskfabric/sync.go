package taskfabric

import (
	"context"
	"fmt"
	"strings"
	"time"

	gonostr "fiatjaf.com/nostr"
	beadspb "github.com/chebizarro/nostrig/gen/beads"
	"github.com/chebizarro/nostrig/internal/beads"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
)

// SyncOptions configure relay-backed canonical task-state synchronization.
type EventPublisher interface {
	Publish(ctx context.Context, relays []string, signer nip34.Signer, events []*gonostr.Event) error
}

type SyncOptions struct {
	Relays              []string
	RepoAddr            string
	TaskIDs             []string
	Authors             []string
	OutDir              string
	CachePath           string
	Limit               int
	FailOnConflict      bool
	Push                bool
	SyncNIP34Status     bool
	Signer              nip34.Signer
	Publisher           EventPublisher
	RelayWinsOnConflict bool
}

type SyncResult struct {
	Export         *beadspb.Export
	EventCount     int
	CachePath      string
	ConflictCount  int
	PublishedCount int
}

// Sync fetches canonical task-state events, merges them with the durable local
// cache and current local beads projection, then renders the resolved view back
// to .beads JSONL.
func Sync(ctx context.Context, client *nip34.Client, opts SyncOptions) (*SyncResult, error) {
	if strings.TrimSpace(opts.OutDir) == "" {
		return nil, fmt.Errorf("out dir is required")
	}
	events, err := FetchTaskStateEvents(ctx, client, opts)
	if err != nil {
		return nil, err
	}
	relayExport, err := ExportFromTaskStateEvents(events)
	if err != nil {
		return nil, err
	}
	cachePath := strings.TrimSpace(opts.CachePath)
	if cachePath == "" {
		cachePath = DefaultCachePath(opts.OutDir)
	}
	previous, err := LoadCache(cachePath)
	if err != nil {
		return nil, fmt.Errorf("load cache: %w", err)
	}
	local, err := LoadLocalIssues(opts.OutDir)
	if err != nil {
		return nil, fmt.Errorf("load local beads issues: %w", err)
	}
	merged, err := MergeTaskStateWithOptions(relayExport, local, previous, MergeOptions{RelayWinsOnConflict: opts.RelayWinsOnConflict || opts.Push})
	if err != nil {
		return nil, err
	}
	published, err := publishWriteBack(ctx, opts, merged)
	if err != nil {
		return nil, err
	}
	if err := WriteCache(cachePath, merged.Records); err != nil {
		return nil, fmt.Errorf("write cache: %w", err)
	}
	if err := beads.NewRenderer(opts.OutDir).RenderExport(merged.Export); err != nil {
		return nil, fmt.Errorf("render failed: %w", err)
	}
	if opts.FailOnConflict && len(merged.Conflicts) > 0 {
		return nil, fmt.Errorf("sync detected %d task conflict(s); inspect %s", len(merged.Conflicts), cachePath)
	}
	return &SyncResult{Export: merged.Export, EventCount: len(events), CachePath: cachePath, ConflictCount: len(merged.Conflicts), PublishedCount: published}, nil
}

func publishWriteBack(ctx context.Context, opts SyncOptions, merged *MergeResult) (int, error) {
	if !opts.Push {
		return 0, nil
	}
	if opts.Signer == nil {
		return 0, fmt.Errorf("push requires signer")
	}
	publisher := opts.Publisher
	if publisher == nil {
		publisher = nip34.NewPublisher()
	}
	relays := cleanStrings(opts.Relays)
	if len(relays) == 0 {
		return 0, fmt.Errorf("push requires at least one relay")
	}
	var events []*gonostr.Event
	for _, rec := range merged.Records {
		if shouldPublishRecord(rec) {
			issue := rec.Resolved.ToIssue()
			now := time.Now().UTC()
			author, err := canonicalAuthor(opts.Authors)
			if err != nil {
				return 0, err
			}
			if provider, ok := opts.Signer.(nip34.PublicKeyProvider); ok {
				signerAuthor, err := provider.PublicKey(ctx)
				if err != nil {
					return 0, err
				}
				if strings.ToLower(strings.TrimSpace(signerAuthor)) != author {
					return 0, fmt.Errorf("canonical author does not match signer pubkey")
				}
			}
			ev, err := nip34.BuildTaskStateEvent(issue, author, now)
			if err != nil {
				return 0, err
			}
			events = append(events, ev)
			if opts.SyncNIP34Status {
				if status := nip34.BuildNIP34IssueStatusEvent(issue, now); status != nil {
					events = append(events, status)
				}
			}
		}
	}
	if len(events) == 0 {
		return 0, nil
	}
	if err := publisher.Publish(ctx, relays, opts.Signer, events); err != nil {
		return 0, err
	}
	return len(events), nil
}

func shouldPublishRecord(rec *CacheRecord) bool {
	if rec == nil || rec.Resolved == nil || rec.Resolution == ResolutionConflict {
		return false
	}
	return rec.Resolution == ResolutionLocalOnly || (rec.Local != nil && snapshotsEqual(rec.Resolved, rec.Local) && rec.LocalRevision != "")
}

// FetchTaskStateEvents queries relays for bounded canonical 30900 task states.
func FetchTaskStateEvents(ctx context.Context, client *nip34.Client, opts SyncOptions) ([]*gonostr.Event, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is nil")
	}
	if client == nil {
		client = nip34.NewClient()
	}
	relays := cleanStrings(opts.Relays)
	if len(relays) == 0 {
		return nil, fmt.Errorf("at least one relay is required")
	}
	repoAddr := strings.TrimSpace(opts.RepoAddr)
	taskIDs := cleanStrings(opts.TaskIDs)
	if repoAddr == "" && len(taskIDs) == 0 {
		return nil, fmt.Errorf("sync requires a bounded selector: provide --repo-addr or at least one --task-id")
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = 500
	}

	authors := cleanStrings(opts.Authors)
	if len(authors) == 0 {
		return nil, fmt.Errorf("at least one canonical author is required")
	}
	trusted := map[string]struct{}{}
	f := gonostr.Filter{Kinds: []gonostr.Kind{gonostr.Kind(nip34.KindCanonicalState)}, Limit: limit}
	for _, author := range authors {
		pk, err := gonostr.PubKeyFromHex(author)
		if err != nil {
			return nil, fmt.Errorf("invalid canonical author %q", author)
		}
		f.Authors = append(f.Authors, pk)
		trusted[pk.Hex()] = struct{}{}
	}
	tags := gonostr.TagMap{}
	if repoAddr != "" {
		tags["a"] = []string{repoAddr}
	}
	if len(taskIDs) > 0 {
		ds := make([]string, 0, len(taskIDs))
		for _, id := range taskIDs {
			ds = append(ds, "task:"+strings.TrimPrefix(id, "task:"))
		}
		tags["d"] = ds
	}
	if len(tags) > 0 {
		f.Tags = tags
	}

	tombstone := gonostr.Filter{Kinds: []gonostr.Kind{gonostr.Kind(5)}, Authors: append([]gonostr.PubKey(nil), f.Authors...), Limit: limit}
	if len(taskIDs) > 0 {
		coords := make([]string, 0, len(taskIDs)*len(authors))
		for _, author := range authors {
			for _, id := range taskIDs {
				coords = append(coords, nip34.Address(nip34.KindCanonicalState, strings.ToLower(author), "task:"+strings.TrimPrefix(id, "task:")))
			}
		}
		tombstone.Tags = gonostr.TagMap{"a": coords}
	} else {
		tombstone.Tags = gonostr.TagMap{"a": []string{repoAddr}}
	}
	events, err := client.FetchMany(ctx, relays, []gonostr.Filter{f, tombstone})
	if err != nil {
		return nil, err
	}
	for _, ev := range events {
		if ev == nil {
			return nil, fmt.Errorf("relay returned nil canonical state")
		}
		if _, ok := trusted[ev.PubKey.Hex()]; !ok {
			return nil, fmt.Errorf("relay returned state from untrusted author")
		}
		var taskID, eventRepo string
		switch int(ev.Kind) {
		case nip34.KindCanonicalState:
			issue, err := nip34.ParseTaskStateEvent(ev)
			if err != nil {
				return nil, fmt.Errorf("invalid canonical state %s: %w", ev.ID.Hex(), err)
			}
			taskID = issue.Id
			eventRepo = issue.GetMetadata().GetCustom()["nip34.repo_addr"]
		case 5:
			taskID, err = nip34.ValidateTaskTombstone(ev, ev.PubKey.Hex())
			if err != nil {
				return nil, fmt.Errorf("invalid canonical tombstone %s: %w", ev.ID.Hex(), err)
			}
			eventRepo = nip34.TaskTombstoneRepo(ev)
		default:
			return nil, fmt.Errorf("relay returned unexpected canonical event kind")
		}
		if repoAddr != "" && eventRepo != repoAddr {
			return nil, fmt.Errorf("relay returned state outside repository selector")
		}
		if len(taskIDs) > 0 && !contains(cleanTaskIDs(taskIDs), taskID) {
			return nil, fmt.Errorf("relay returned state outside task selector")
		}
	}
	return events, nil
}

func canonicalAuthor(authors []string) (string, error) {
	authors = cleanStrings(authors)
	if len(authors) == 0 {
		return "", fmt.Errorf("at least one canonical author is required")
	}
	author := strings.ToLower(authors[0])
	if _, err := gonostr.PubKeyFromHex(author); err != nil {
		return "", fmt.Errorf("invalid canonical author %q", authors[0])
	}
	return author, nil
}

// ExportFromTaskStateEvents converts canonical 30900 task-state events into a beads export.
func ExportFromTaskStateEvents(events []*gonostr.Event) (*beadspb.Export, error) {
	return beads.ExportFromTaskStateEvents(events)
}

func cleanStrings(in []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
