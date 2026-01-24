package converter

import (
	"testing"
	"time"

	beadspb "github.com/bizarro/nostrig/gen/beads"
	nip34 "github.com/bizarro/nostrig/internal/nostr"
)

func TestConverter_StatusMappingAndLabels(t *testing.T) {
	repo := &nip34.RepoAnnouncement{
		EventID:     "repoev",
		PubKey:      "pub1",
		RepoID:      "My Repo",
		Name:        "My Repo",
		Description: "desc",
		Relays:      []string{"wss://relay.example.com"},
		CreatedAt:   time.Unix(10, 0).UTC(),
	}

	repoEpicID := nip34.RepoEpicID(repo.RepoID)

	tests := []struct {
		name         string
		statusKind   int
		wantStatus   beadspb.Status
		wantHasDraft bool
	}{
		{"open", nip34.KindStatusOpen, beadspb.Status_STATUS_OPEN, false},
		{"applied", nip34.KindStatusApplied, beadspb.Status_STATUS_CLOSED, false},
		{"closed", nip34.KindStatusClosed, beadspb.Status_STATUS_CLOSED, false},
		{"draft", nip34.KindStatusDraft, beadspb.Status_STATUS_OPEN, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := &nip34.RootItem{
				EventID:   "root-" + tt.name,
				PubKey:    "pubA",
				Kind:      nip34.KindIssue,
				RepoAddr:  nip34.RepoAddress(repo.PubKey, repo.RepoID),
				Subject:   "",
				Content:   "Title line\nbody",
				Labels:    []string{"bug", "bug"}, // duplicate should dedupe
				CreatedAt: time.Unix(100, 0).UTC(),
			}

			status := &nip34.StatusEvent{
				EventID:     "status-" + tt.name,
				PubKey:      "pubB",
				Kind:        tt.statusKind,
				RootEventID: root.EventID,
				Content:     "status content",
				CreatedAt:   time.Unix(200, 0).UTC(),
			}

			agg := &Aggregate{
				Repo: repo,
				Items: []*AggregateItem{
					{Root: root, Status: status},
				},
				StatusByRoot: map[string]*nip34.StatusEvent{
					root.EventID: status,
				},
			}

			c := NewConverter()
			out, err := c.Convert(agg)
			if err != nil {
				t.Fatalf("Convert error: %v", err)
			}

			if len(out.Epics) != 1 {
				t.Fatalf("Epics len=%d, want 1", len(out.Epics))
			}
			if out.Epics[0].Id != repoEpicID {
				t.Fatalf("Epic.Id=%q, want %q", out.Epics[0].Id, repoEpicID)
			}
			if out.Epics[0].Metadata == nil || out.Epics[0].Metadata.Custom == nil {
				t.Fatalf("Epic.Metadata.Custom is nil, expected non-nil")
			}
			if out.Epics[0].Metadata.Custom["nostr.id"] != repo.EventID {
				t.Fatalf("Epic.Metadata.Custom[nostr.id]=%q, want %q", out.Epics[0].Metadata.Custom["nostr.id"], repo.EventID)
			}

			if len(out.Issues) != 1 {
				t.Fatalf("Issues len=%d, want 1", len(out.Issues))
			}

			iss := out.Issues[0]
			if iss.Id != root.EventID {
				t.Fatalf("Issue.Id=%q, want %q", iss.Id, root.EventID)
			}
			if iss.Epic != repoEpicID {
				t.Fatalf("Issue.Epic=%q, want %q", iss.Epic, repoEpicID)
			}
			if iss.Status != tt.wantStatus {
				t.Fatalf("Issue.Status=%v, want %v", iss.Status, tt.wantStatus)
			}

			// Title should be derived from content if subject empty.
			if iss.Title != "Title line" {
				t.Fatalf("Issue.Title=%q, want %q", iss.Title, "Title line")
			}

			// Should include type label "issue" plus deduped labels.
			if !contains(iss.Labels, "issue") {
				t.Fatalf("Issue.Labels=%#v, want to contain %q", iss.Labels, "issue")
			}
			if !contains(iss.Labels, "bug") {
				t.Fatalf("Issue.Labels=%#v, want to contain %q", iss.Labels, "bug")
			}

			if tt.wantHasDraft && !contains(iss.Labels, "draft") {
				t.Fatalf("Issue.Labels=%#v, want to contain %q for draft status", iss.Labels, "draft")
			}
			if !tt.wantHasDraft && contains(iss.Labels, "draft") {
				t.Fatalf("Issue.Labels=%#v, did not want draft label", iss.Labels)
			}

			if iss.Metadata == nil || iss.Metadata.Custom == nil {
				t.Fatalf("Issue.Metadata.Custom is nil, expected non-nil")
			}
			if iss.Metadata.Custom["nostr.id"] != root.EventID {
				t.Fatalf("Issue.Metadata.Custom[nostr.id]=%q, want %q", iss.Metadata.Custom["nostr.id"], root.EventID)
			}
			if iss.Metadata.Custom["nip34.repo_addr"] == "" {
				t.Fatalf("Issue.Metadata.Custom[nip34.repo_addr] is empty, expected non-empty")
			}
		})
	}
}

func TestConverter_PRAndPatchLabels(t *testing.T) {
	repo := &nip34.RepoAnnouncement{
		EventID:   "repoev",
		PubKey:    "pub1",
		RepoID:    "my-repo",
		CreatedAt: time.Unix(10, 0).UTC(),
	}

	agg := &Aggregate{
		Repo: repo,
		Items: []*AggregateItem{
			{
				Root: &nip34.RootItem{
					EventID:   "pr1",
					PubKey:    "pubA",
					Kind:      nip34.KindPullRequest,
					RepoAddr:  nip34.RepoAddress(repo.PubKey, repo.RepoID),
					Subject:   "PR subject",
					Content:   "PR content",
					Labels:    []string{"enhancement"},
					Commit:    "deadbeef",
					Clone:     []string{"https://example.com/repo.git"},
					CreatedAt: time.Unix(100, 0).UTC(),
				},
				Status: &nip34.StatusEvent{
					EventID:     "st-pr1",
					PubKey:      "pubB",
					Kind:        nip34.KindStatusOpen,
					RootEventID: "pr1",
					CreatedAt:   time.Unix(101, 0).UTC(),
				},
			},
			{
				Root: &nip34.RootItem{
					EventID:     "patch1",
					PubKey:      "pubC",
					Kind:        nip34.KindPatch,
					RepoAddr:    nip34.RepoAddress(repo.PubKey, repo.RepoID),
					Subject:     "Patch subject",
					Content:     "patch text",
					Labels:      []string{"fix"},
					CommitID:    "c0ffee",
					ParentCommit: "bada55",
					CreatedAt:   time.Unix(200, 0).UTC(),
				},
			},
		},
	}

	c := NewConverter()
	out, err := c.Convert(agg)
	if err != nil {
		t.Fatalf("Convert error: %v", err)
	}

	if len(out.Issues) != 2 {
		t.Fatalf("Issues len=%d, want 2", len(out.Issues))
	}

	var pr, patch *beadspb.Issue
	for _, is := range out.Issues {
		switch is.Id {
		case "pr1":
			pr = is
		case "patch1":
			patch = is
		}
	}

	if pr == nil {
		t.Fatalf("Expected PR issue with id pr1")
	}
	if !contains(pr.Labels, "pr") {
		t.Fatalf("PR Labels=%#v, want contain %q", pr.Labels, "pr")
	}
	if pr.Metadata == nil || pr.Metadata.Custom == nil {
		t.Fatalf("PR metadata missing")
	}
	if pr.Metadata.Custom["nip34.pr.commit"] != "deadbeef" {
		t.Fatalf("PR nip34.pr.commit=%q, want %q", pr.Metadata.Custom["nip34.pr.commit"], "deadbeef")
	}
	if pr.Metadata.Custom["nip34.clone"] == "" {
		t.Fatalf("PR nip34.clone is empty, expected non-empty")
	}

	if patch == nil {
		t.Fatalf("Expected patch issue with id patch1")
	}
	if !contains(patch.Labels, "patch") {
		t.Fatalf("Patch Labels=%#v, want contain %q", patch.Labels, "patch")
	}
	if patch.Metadata == nil || patch.Metadata.Custom == nil {
		t.Fatalf("Patch metadata missing")
	}
	if patch.Metadata.Custom["nip34.patch.commit"] != "c0ffee" {
		t.Fatalf("Patch nip34.patch.commit=%q, want %q", patch.Metadata.Custom["nip34.patch.commit"], "c0ffee")
	}
	if patch.Metadata.Custom["nip34.patch.parent_commit"] != "bada55" {
		t.Fatalf("Patch nip34.patch.parent_commit=%q, want %q", patch.Metadata.Custom["nip34.patch.parent_commit"], "bada55")
	}
}