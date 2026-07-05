package taskfabric

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	beadspb "github.com/chebizarro/nostrig/gen/beads"
	"github.com/chebizarro/nostrig/internal/beads"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
	gonostr "github.com/nbd-wtf/go-nostr"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// SyncOptions configure relay-backed canonical task-state synchronization.
type SyncOptions struct {
	Relays   []string
	RepoAddr string
	TaskIDs  []string
	Authors  []string
	OutDir   string
	Limit    int
}

type SyncResult struct {
	Export     *beadspb.Export
	EventCount int
}

// Sync fetches canonical task-state events and renders them as beads JSONL.
func Sync(ctx context.Context, client *nip34.Client, opts SyncOptions) (*SyncResult, error) {
	if strings.TrimSpace(opts.OutDir) == "" {
		return nil, fmt.Errorf("out dir is required")
	}
	events, err := FetchTaskStateEvents(ctx, client, opts)
	if err != nil {
		return nil, err
	}
	export, err := ExportFromTaskStateEvents(events)
	if err != nil {
		return nil, err
	}
	if err := beads.NewRenderer(opts.OutDir).RenderExport(export); err != nil {
		return nil, fmt.Errorf("render failed: %w", err)
	}
	return &SyncResult{Export: export, EventCount: len(events)}, nil
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

	f := gonostr.Filter{Kinds: []int{nip34.KindCanonicalState}, Limit: limit}
	if len(opts.Authors) > 0 {
		f.Authors = cleanStrings(opts.Authors)
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

	return client.Fetch(ctx, relays, f)
}

// ExportFromTaskStateEvents converts canonical 30900 task-state events into a beads export.
func ExportFromTaskStateEvents(events []*gonostr.Event) (*beadspb.Export, error) {
	latest := map[string]*beadspb.Issue{}
	latestTime := map[string]time.Time{}

	for _, ev := range events {
		issue, err := nip34.ParseTaskStateEvent(ev)
		if err != nil {
			continue
		}
		id := strings.TrimSpace(issue.Id)
		if id == "" {
			continue
		}
		createdAt := nip34.EventTime(ev)
		ensureMetadata(issue)
		issue.Metadata.Custom["nostr.id"] = ev.ID
		issue.Metadata.Custom["nostr.pubkey"] = ev.PubKey
		issue.Metadata.Custom["nostr.kind"] = fmt.Sprintf("%d", ev.Kind)
		issue.Metadata.Custom["nostrig.source"] = "canonical-task-state"
		if issue.Updated == nil && !createdAt.IsZero() {
			issue.Updated = timestamppb.New(createdAt)
		}
		if issue.Created == nil && !createdAt.IsZero() {
			issue.Created = timestamppb.New(createdAt)
		}

		prevTime, ok := latestTime[id]
		if !ok || createdAt.After(prevTime) {
			latest[id] = issue
			latestTime[id] = createdAt
		}
	}

	issues := make([]*beadspb.Issue, 0, len(latest))
	for _, issue := range latest {
		issues = append(issues, issue)
	}
	sort.Slice(issues, func(i, j int) bool { return issues[i].Id < issues[j].Id })
	return &beadspb.Export{Issues: issues}, nil
}

func ensureMetadata(issue *beadspb.Issue) {
	if issue.Metadata == nil {
		issue.Metadata = &beadspb.Metadata{}
	}
	if issue.Metadata.Custom == nil {
		issue.Metadata.Custom = map[string]string{}
	}
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
