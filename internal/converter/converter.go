package converter

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	beadspb "github.com/chebizarro/nostrig/gen/beads"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
	"github.com/chebizarro/nostrig/internal/taskmodel"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Converter converts a NIP-34 aggregate into a beads protobuf Export.
type Converter struct{}

// NewConverter creates a new Converter.
func NewConverter() *Converter {
	return &Converter{}
}

// Convert converts a validated Aggregate into beads Export.
// It always emits exactly one Epic (the repository) and many Issues (issues/PRs/patches).
func (c *Converter) Convert(agg *Aggregate) (*beadspb.Export, error) {
	if err := agg.Validate(); err != nil {
		return nil, err
	}

	format := agg.IDFormat

	legacyEpicID := nip34.RepoEpicID(agg.Repo.RepoID)

	repoEpicID := legacyEpicID
	if format.IsSpec() {
		specID, err := nip34.BeadsEpicID(agg.IDPrefix, agg.Repo.PubKey, agg.Repo.RepoID)
		if err != nil {
			return nil, fmt.Errorf("failed generating spec epic id: %w", err)
		}
		repoEpicID = specID
	}

	out := &beadspb.Export{
		Issues: make([]*beadspb.Issue, 0),
		Epics:  make([]*beadspb.Epic, 0, 1),
	}

	out.Epics = append(out.Epics, c.convertRepoEpic(agg.Repo, agg.State, repoEpicID, format, legacyEpicID))

	for _, item := range agg.Items {
		if item == nil || item.Root == nil {
			continue
		}

		status := item.Status
		if status == nil {
			status = agg.StatusFor(item.Root.EventID)
		}

		beadsIssue, err := c.convertRootItem(item.Root, status, agg.Repo, repoEpicID, format, agg.IDPrefix, item.Root.EventID)
		if err != nil {
			return nil, fmt.Errorf("failed converting root item %s: %w", item.Root.EventID, err)
		}
		out.Issues = append(out.Issues, beadsIssue)
	}

	return out, nil
}

func (c *Converter) convertRepoEpic(repo *nip34.RepoAnnouncement, state *nip34.RepoState, repoEpicID string, format IDFormat, legacyID string) *beadspb.Epic {
	name := strings.TrimSpace(repo.Name)
	if name == "" {
		name = repo.RepoID
	}

	updated := repo.CreatedAt
	if state != nil && state.CreatedAt.After(updated) {
		updated = state.CreatedAt
	}

	custom := map[string]string{
		"nostr.id":          repo.EventID,
		"nostr.pubkey":      repo.PubKey,
		"nostr.kind":        strconv.Itoa(nip34.KindRepositoryAnnouncement),
		"nip34.repo_id":     repo.RepoID,
		"nip34.repo_addr":   nip34.RepoAddress(repo.PubKey, repo.RepoID),
		"nip34.euc":         repo.EUC,
		"nip34.web":         strings.Join(repo.Web, ","),
		"nip34.clone":       strings.Join(repo.Clone, ","),
		"nip34.relays":      strings.Join(repo.Relays, ","),
		"nip34.maintainers": strings.Join(repo.Maintainers, ","),
		"nip34.topics":      strings.Join(repo.Topics, ","),
		"nostrig.id_format": format.String(),
		"nostrig.beads_id":  repoEpicID,
	}

	if format.IsSpec() {
		custom["nostrig.legacy_id"] = legacyID
	}

	if state != nil {
		custom["nip34.state.id"] = state.EventID
		custom["nip34.state.pubkey"] = state.PubKey
		custom["nip34.state.kind"] = strconv.Itoa(nip34.KindRepositoryState)
		if strings.TrimSpace(state.HEAD) != "" {
			custom["nip34.state.head"] = state.HEAD
		}
		for refName, commit := range state.Refs {
			// Store as separate keys; refName contains slashes but is fine in map keys.
			custom["nip34.state.ref."+refName] = commit
		}
	}

	epic := &beadspb.Epic{
		Id:          repoEpicID,
		Name:        name,
		Description: repo.Description,
		Status:      beadspb.Status_STATUS_OPEN,
		Created:     timestamppb.New(repo.CreatedAt),
		Updated:     timestamppb.New(updated),
		Metadata: &beadspb.Metadata{
			Custom: custom,
		},
	}

	return epic
}

func (c *Converter) convertRootItem(root *nip34.RootItem, status *nip34.StatusEvent, repo *nip34.RepoAnnouncement, repoEpicID string, format IDFormat, prefix string, legacyID string) (*beadspb.Issue, error) {
	if root == nil {
		return nil, fmt.Errorf("root item is nil")
	}
	if strings.TrimSpace(root.EventID) == "" {
		return nil, fmt.Errorf("root item missing event id")
	}

	repoAddr := strings.TrimSpace(root.RepoAddr)
	if repoAddr == "" && repo != nil {
		repoAddr = nip34.RepoAddress(repo.PubKey, repo.RepoID)
	}

	issueID := root.EventID
	if format.IsSpec() {
		id, err := nip34.BeadsIssueID(prefix, repoAddr, root.EventID)
		if err != nil {
			return nil, fmt.Errorf("failed generating spec issue id: %w", err)
		}
		issueID = id
	}

	issueStatus, draft := mapBeadsStatus(status)

	title := strings.TrimSpace(root.Subject)
	if title == "" {
		title = titleFromContent(root.Content)
	}
	if title == "" {
		title = fmt.Sprintf("nostr-%s", root.EventID)
	}

	labels := make([]string, 0, len(root.Labels)+2)
	switch root.Kind {
	case nip34.KindIssue:
		labels = append(labels, "issue")
	case nip34.KindPullRequest:
		labels = append(labels, "pr")
	case nip34.KindPatch:
		labels = append(labels, "patch")
	default:
		labels = append(labels, "item")
	}

	for _, l := range root.Labels {
		l = strings.TrimSpace(l)
		if l == "" {
			continue
		}
		if !contains(labels, l) {
			labels = append(labels, l)
		}
	}

	if draft && !contains(labels, "draft") {
		labels = append(labels, "draft")
	}

	updatedAt := root.CreatedAt
	if status != nil && status.CreatedAt.After(updatedAt) {
		updatedAt = status.CreatedAt
	}

	custom := map[string]string{
		"nostr.id":          root.EventID,
		"nostr.pubkey":      root.PubKey,
		"nostr.kind":        strconv.Itoa(root.Kind),
		"nip34.repo_addr":   repoAddr,
		"nostrig.id_format": format.String(),
		"nostrig.beads_id":  issueID,
	}

	if format.IsSpec() {
		custom["nostrig.legacy_id"] = legacyID
	} else {
		// In legacy mode beads_id == id; keep legacy_id equal to the rendered id for clarity.
		custom["nostrig.legacy_id"] = issueID
	}

	if strings.TrimSpace(root.Subject) != "" {
		custom["nip34.subject"] = root.Subject
	}

	if status != nil {
		custom["nip34.status.id"] = status.EventID
		custom["nip34.status.pubkey"] = status.PubKey
		custom["nip34.status.kind"] = strconv.Itoa(status.Kind)
	}

	switch root.Kind {
	case nip34.KindPullRequest:
		if strings.TrimSpace(root.Commit) != "" {
			custom["nip34.pr.commit"] = root.Commit
		}
		if strings.TrimSpace(root.MergeBase) != "" {
			custom["nip34.pr.merge_base"] = root.MergeBase
		}
		if strings.TrimSpace(root.BranchName) != "" {
			custom["nip34.pr.branch_name"] = root.BranchName
		}
		if len(root.Clone) > 0 {
			custom["nip34.clone"] = strings.Join(root.Clone, ",")
		}

	case nip34.KindPatch:
		if strings.TrimSpace(root.CommitID) != "" {
			custom["nip34.patch.commit"] = root.CommitID
		}
		if strings.TrimSpace(root.ParentCommit) != "" {
			custom["nip34.patch.parent_commit"] = root.ParentCommit
		}
	}

	issue := &beadspb.Issue{
		Id:          issueID,
		Title:       title,
		Description: root.Content,
		Status:      issueStatus,
		Epic:        repoEpicID,
		Labels:      labels,
		DependsOn:   []string{},
		Created:     timestamppb.New(root.CreatedAt),
		Updated:     timestamppb.New(updatedAt),
		Metadata: &beadspb.Metadata{
			Custom: custom,
		},
	}
	statusKind := "missing"
	if status != nil {
		statusKind = strconv.Itoa(status.Kind)
	}
	nipRevision := taskmodel.StableRevision(map[string]string{
		"root_id": root.EventID, "root_kind": strconv.Itoa(root.Kind), "repo_addr": repoAddr,
		"subject": root.Subject, "content": root.Content, "status_kind": statusKind,
	})
	doc, err := taskmodel.FromProto(issue)
	if err != nil {
		return nil, err
	}
	taskRevision := taskmodel.MaterialRevision(doc)
	custom["sync.origin"] = "nip34"
	custom["sync.origin_revision"] = nipRevision
	custom["sync.nip34.source_revision"] = nipRevision
	custom["sync.nip34.last_sync_revision"] = nipRevision
	custom["sync.nostrig.source_revision"] = taskRevision
	custom["sync.nostrig.last_sync_revision"] = taskRevision

	return issue, nil
}

func mapBeadsStatus(status *nip34.StatusEvent) (beadspb.Status, bool) {
	if status == nil {
		return beadspb.Status_STATUS_OPEN, false
	}

	switch status.Kind {
	case nip34.KindStatusOpen:
		return beadspb.Status_STATUS_OPEN, false
	case nip34.KindStatusApplied:
		return beadspb.Status_STATUS_CLOSED, false
	case nip34.KindStatusClosed:
		return beadspb.Status_STATUS_CLOSED, false
	case nip34.KindStatusDraft:
		return beadspb.Status_STATUS_OPEN, true
	default:
		return beadspb.Status_STATUS_OPEN, false
	}
}

func titleFromContent(content string) string {
	content = strings.TrimSpace(content)
	if content == "" {
		return ""
	}
	lines := strings.Split(content, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		// Keep titles reasonably short.
		if len(line) > 120 {
			return strings.TrimSpace(line[:120])
		}
		return line
	}
	return ""
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}
