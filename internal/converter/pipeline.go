package converter

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/bizarro/nostrig/internal/beads"
	nip34 "github.com/bizarro/nostrig/internal/nostr"
	gonostr "github.com/nbd-wtf/go-nostr"
)

// FetchOptions configure a single `nostrig fetch` run for one repository.
type FetchOptions struct {
	RepoID string
	Owner  string   // optional hex pubkey (filters 30617/30618 authors when provided)
	Relays []string // seed relays to query; merged with repo announcement relays
	OutDir string   // output directory where .beads/ will be written
}

// Pipeline orchestrates fetch → aggregate → convert → render.
type Pipeline struct {
	client    *nip34.Client
	converter *Converter
}

// NewPipeline creates a new pipeline.
func NewPipeline() *Pipeline {
	return &Pipeline{
		client:    nip34.NewClient(),
		converter: NewConverter(),
	}
}

// Run executes the full pipeline and writes JSONL output.
func (p *Pipeline) Run(ctx context.Context, opts FetchOptions) error {
	if ctx == nil {
		return fmt.Errorf("context is nil")
	}
	if strings.TrimSpace(opts.RepoID) == "" {
		return fmt.Errorf("repo id is required")
	}
	if strings.TrimSpace(opts.OutDir) == "" {
		return fmt.Errorf("out dir is required")
	}

	seedRelays := dedupeStrings(opts.Relays)
	if len(seedRelays) == 0 {
		seedRelays = defaultRelays()
	}

	// Phase A: find repo announcement (30617) by d tag (and optional owner)
	repo, err := p.findRepoAnnouncement(ctx, seedRelays, opts.RepoID, opts.Owner)
	if err != nil {
		return err
	}

	owner := repo.PubKey
	if strings.TrimSpace(opts.Owner) != "" {
		owner = strings.TrimSpace(opts.Owner)
	}

	// Merge relays (CLI relays + announcement relays)
	relays := dedupeStrings(append(seedRelays, repo.Relays...))
	if len(relays) == 0 {
		return fmt.Errorf("no relays available (provide --relay or ensure repo announcement has relays tags)")
	}

	repoAddr := nip34.RepoAddress(owner, repo.RepoID)

	// Phase B: fetch items and repo state
	state, roots, err := p.fetchRepoData(ctx, relays, repo.RepoID, owner, repoAddr)
	if err != nil {
		return err
	}

	// Phase C: fetch statuses targeting the root IDs
	statusByRoot, err := p.fetchStatuses(ctx, relays, roots)
	if err != nil {
		return err
	}

	// Phase D: build aggregate
	agg := &Aggregate{
		Repo:         repo,
		State:        state,
		Items:        make([]*AggregateItem, 0, len(roots)),
		StatusByRoot: statusByRoot,
	}

	for _, root := range roots {
		if root == nil {
			continue
		}
		agg.Items = append(agg.Items, &AggregateItem{
			Root:   root,
			Status: statusByRoot[root.EventID],
		})
	}

	// Convert to beads proto
	export, err := p.converter.Convert(agg)
	if err != nil {
		return fmt.Errorf("convert failed: %w", err)
	}

	// Render JSONL
	renderer := beads.NewRenderer(opts.OutDir)
	if err := renderer.RenderExport(export); err != nil {
		return fmt.Errorf("render failed: %w", err)
	}

	return nil
}

func (p *Pipeline) findRepoAnnouncement(ctx context.Context, relays []string, repoID string, owner string) (*nip34.RepoAnnouncement, error) {
	f := gonostr.Filter{
		Kinds: []int{nip34.KindRepositoryAnnouncement},
		Tags:  gonostr.TagMap{"d": []string{repoID}},
	}

	owner = strings.TrimSpace(owner)
	if owner != "" {
		f.Authors = []string{owner}
	}

	events, err := p.client.Fetch(ctx, relays, f)
	if err != nil {
		return nil, fmt.Errorf("failed fetching repository announcement: %w", err)
	}
	if len(events) == 0 {
		return nil, fmt.Errorf("no repository announcement found for repo-id %q", repoID)
	}

	// Parse all candidates and pick latest by created_at
	candidates := make([]*nip34.RepoAnnouncement, 0, len(events))
	for _, ev := range events {
		ra, err := nip34.ParseRepoAnnouncement(ev)
		if err != nil {
			continue
		}
		candidates = append(candidates, ra)
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no valid repository announcement events found for repo-id %q", repoID)
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].CreatedAt.Before(candidates[j].CreatedAt)
	})

	return candidates[len(candidates)-1], nil
}

func (p *Pipeline) fetchRepoData(ctx context.Context, relays []string, repoID string, owner string, repoAddr string) (*nip34.RepoState, []*nip34.RootItem, error) {
	// Repo state (30618) is optional.
	var state *nip34.RepoState
	{
		f := gonostr.Filter{
			Kinds:   []int{nip34.KindRepositoryState},
			Authors: []string{owner},
			Tags:    gonostr.TagMap{"d": []string{repoID}},
		}

		events, err := p.client.Fetch(ctx, relays, f)
		if err != nil {
			return nil, nil, fmt.Errorf("failed fetching repository state: %w", err)
		}

		var candidates []*nip34.RepoState
		for _, ev := range events {
			rs, err := nip34.ParseRepoState(ev)
			if err != nil {
				continue
			}
			candidates = append(candidates, rs)
		}

		if len(candidates) > 0 {
			sort.Slice(candidates, func(i, j int) bool {
				return candidates[i].CreatedAt.Before(candidates[j].CreatedAt)
			})
			state = candidates[len(candidates)-1]
		}
	}

	// Root items + PR updates (we fetch 1619 now, but only convert 1617/1618/1621 for now).
	roots := make([]*nip34.RootItem, 0, 256)
	{
		f := gonostr.Filter{
			Kinds: []int{
				nip34.KindPatch,
				nip34.KindPullRequest,
				nip34.KindPRUpdate,
				nip34.KindIssue,
			},
			Tags: gonostr.TagMap{"a": []string{repoAddr}},
		}

		events, err := p.client.Fetch(ctx, relays, f)
		if err != nil {
			return nil, nil, fmt.Errorf("failed fetching repository items: %w", err)
		}

		for _, ev := range events {
			item, err := nip34.ParseRootItem(ev)
			if err != nil {
				// Includes KindPRUpdate which is not a root item; ignore.
				continue
			}
			roots = append(roots, item)
		}
	}

	// Ensure stable-ish order (created time asc) for output determinism.
	sort.Slice(roots, func(i, j int) bool {
		return roots[i].CreatedAt.Before(roots[j].CreatedAt)
	})

	return state, roots, nil
}

func (p *Pipeline) fetchStatuses(ctx context.Context, relays []string, roots []*nip34.RootItem) (map[string]*nip34.StatusEvent, error) {
	rootIDs := make([]string, 0, len(roots))
	for _, r := range roots {
		if r == nil {
			continue
		}
		if strings.TrimSpace(r.EventID) == "" {
			continue
		}
		rootIDs = append(rootIDs, r.EventID)
	}
	rootIDs = dedupeStrings(rootIDs)

	out := make(map[string]*nip34.StatusEvent, len(rootIDs))
	if len(rootIDs) == 0 {
		return out, nil
	}

	const chunkSize = 200
	chunks := chunkStrings(rootIDs, chunkSize)

	filters := make([]gonostr.Filter, 0, len(chunks))
	for _, ch := range chunks {
		filters = append(filters, gonostr.Filter{
			Kinds: []int{
				nip34.KindStatusOpen,
				nip34.KindStatusApplied,
				nip34.KindStatusClosed,
				nip34.KindStatusDraft,
			},
			Tags: gonostr.TagMap{"e": ch},
		})
	}

	events, err := p.client.FetchMany(ctx, relays, filters)
	if err != nil {
		// Return partial results only if context canceled; otherwise fail fast.
		if ctx.Err() != nil {
			return out, err
		}
		return nil, fmt.Errorf("failed fetching statuses: %w", err)
	}

	for _, ev := range events {
		st, err := nip34.ParseStatusEvent(ev)
		if err != nil {
			continue
		}

		prev, ok := out[st.RootEventID]
		if !ok {
			out[st.RootEventID] = st
			continue
		}
		if st.CreatedAt.After(prev.CreatedAt) {
			out[st.RootEventID] = st
		}
	}

	return out, nil
}

func defaultRelays() []string {
	// Sensible defaults; users can override/extend with --relay.
	return []string{
		"wss://relay.damus.io",
		"wss://nos.lol",
		"wss://relay.nostr.band",
	}
}

func dedupeStrings(in []string) []string {
	seen := make(map[string]struct{}, len(in))
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

func chunkStrings(in []string, chunkSize int) [][]string {
	if chunkSize <= 0 {
		chunkSize = 1
	}
	if len(in) == 0 {
		return nil
	}
	out := make([][]string, 0, (len(in)+chunkSize-1)/chunkSize)
	for i := 0; i < len(in); i += chunkSize {
		j := i + chunkSize
		if j > len(in) {
			j = len(in)
		}
		ch := make([]string, 0, j-i)
		ch = append(ch, in[i:j]...)
		out = append(out, ch)
	}
	return out
}