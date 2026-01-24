package beads

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/chebizarro/nostrig/gen/beads"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRenderer_RenderExport_WritesJSONL(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "nostrig-render-*")
	if err != nil {
		t.Fatalf("MkdirTemp error: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	now := time.Unix(1700000000, 0).UTC()

	export := &pb.Export{
		Epics: []*pb.Epic{
			{
				Id:          "repo-my-repo",
				Name:        "My Repo",
				Description: "desc",
				Status:      pb.Status_STATUS_OPEN,
				Created:     timestamppb.New(now),
				Updated:     timestamppb.New(now.Add(5 * time.Minute)),
				Metadata: &pb.Metadata{
					Custom: map[string]string{
						"nostr.id":     "repoev",
						"nip34.repo_id": "my-repo",
					},
				},
			},
		},
		Issues: []*pb.Issue{
			{
				Id:          "issue1",
				Title:       "Test issue",
				Description: "Body",
				Status:      pb.Status_STATUS_CLOSED,
				Priority:    pb.Priority_PRIORITY_UNSPECIFIED,
				Epic:        "repo-my-repo",
				Labels:      []string{"issue", "bug"},
				DependsOn:   []string{},
				Created:     timestamppb.New(now),
				Updated:     timestamppb.New(now.Add(1 * time.Hour)),
				Metadata: &pb.Metadata{
					Custom: map[string]string{
						"nostr.id":        "issue1",
						"nip34.repo_addr": "30617:pub:my-repo",
					},
				},
			},
		},
	}

	r := NewRenderer(tmpDir)
	if err := r.RenderExport(export); err != nil {
		t.Fatalf("RenderExport error: %v", err)
	}

	issuesPath := filepath.Join(tmpDir, ".beads", "issues.jsonl")
	epicsPath := filepath.Join(tmpDir, ".beads", "epics.jsonl")

	if _, err := os.Stat(issuesPath); err != nil {
		t.Fatalf("issues.jsonl stat error: %v", err)
	}
	if _, err := os.Stat(epicsPath); err != nil {
		t.Fatalf("epics.jsonl stat error: %v", err)
	}

	// Read first (and only) issue line and validate a few fields.
	{
		f, err := os.Open(issuesPath)
		if err != nil {
			t.Fatalf("open issues.jsonl: %v", err)
		}
		defer func() { _ = f.Close() }()

		sc := bufio.NewScanner(f)
		if !sc.Scan() {
			t.Fatalf("expected at least one line in issues.jsonl")
		}

		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("unmarshal issue json: %v", err)
		}

		if m["id"] != "issue1" {
			t.Fatalf("issue id=%v, want %v", m["id"], "issue1")
		}
		if m["status"] != "closed" {
			t.Fatalf("issue status=%v, want %v", m["status"], "closed")
		}
		// Priority omitted when unspecified (omitempty); allow either missing or empty.
		if v, ok := m["priority"]; ok && v != "" {
			t.Fatalf("issue priority=%v, want omitted or empty", v)
		}

		if err := sc.Err(); err != nil {
			t.Fatalf("scanner error reading issues.jsonl: %v", err)
		}
	}

	// Read first (and only) epic line and validate a few fields.
	{
		f, err := os.Open(epicsPath)
		if err != nil {
			t.Fatalf("open epics.jsonl: %v", err)
		}
		defer func() { _ = f.Close() }()

		sc := bufio.NewScanner(f)
		if !sc.Scan() {
			t.Fatalf("expected at least one line in epics.jsonl")
		}

		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("unmarshal epic json: %v", err)
		}

		if m["id"] != "repo-my-repo" {
			t.Fatalf("epic id=%v, want %v", m["id"], "repo-my-repo")
		}
		if m["status"] != "open" {
			t.Fatalf("epic status=%v, want %v", m["status"], "open")
		}

		if err := sc.Err(); err != nil {
			t.Fatalf("scanner error reading epics.jsonl: %v", err)
		}
	}
}