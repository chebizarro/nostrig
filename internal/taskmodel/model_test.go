package taskmodel

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	pb "github.com/chebizarro/nostrig/gen/beads"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestBeadsV103FixtureDecodesLosslessly(t *testing.T) {
	fixture := []byte(`{
		"_type":"issue","id":"nostrig-2mv","title":"Complete task model",
		"description":"Lossless model","status":"in_progress","priority":0,
		"issue_type":"feature","assignee":"Biz","owner":"owner@example.com",
		"created_at":"2026-07-21T22:13:23Z","created_by":"Biz",
		"updated_at":"2026-07-22T00:15:33Z","started_at":"2026-07-22T00:15:33Z",
		"acceptance_criteria":"all fields round trip","notes":"handoff",
		"labels":["p0","schema"],
		"dependencies":[
			{"issue_id":"nostrig-2mv","depends_on_id":"nostrig-6ng","type":"blocks","created_at":"2026-07-21T15:14:40Z","created_by":"Biz","metadata":"{}"},
			{"issue_id":"nostrig-2mv","depends_on_id":"nostrig-epic","type":"parent-child","metadata":"{}"}
		],
		"dependency_count":1,"dependent_count":4,
		"comments":[{"id":"7","issue_id":"nostrig-2mv","author":"Gus","text":"reviewed","created_at":"2026-07-22T01:00:00Z"}],
		"comment_count":1
	}`)
	doc, err := DecodeBeads(fixture)
	if err != nil {
		t.Fatal(err)
	}
	if doc.Priority != "P0" || doc.Owner != "owner@example.com" || doc.CreatedBy != "Biz" || doc.IssueType != "feature" {
		t.Fatalf("identity/type/priority lost: %#v", doc)
	}
	if len(doc.Dependencies) != 2 || doc.Dependencies[1].Type != "parent-child" || *doc.DependencyCount != 1 {
		t.Fatalf("typed dependencies/count lost: %#v", doc.Dependencies)
	}
	if len(doc.Comments) != 1 || doc.Comments[0].Text != "reviewed" {
		t.Fatalf("comments lost: %#v", doc.Comments)
	}
	raw, err := EncodeBeads(doc)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got["priority"] != float64(0) || got["created_at"] == nil || got["dependencies"] == nil {
		t.Fatalf("noncanonical Beads rendering: %s", raw)
	}
}

func TestCompleteProtoCanonicalRoundTrip(t *testing.T) {
	now := time.Date(2026, 7, 22, 1, 2, 3, 4, time.UTC)
	hash := strings.Repeat("a", 64)
	ref := &pb.ArtifactReference{Kind: "evidence", Url: "https://blossom.example/" + hash, Sha256: hash, MediaType: "application/json", SizeBytes: 42}
	depCount, dependentCount, commentCount := int32(1), int32(2), int32(1)
	issue := &pb.Issue{
		Id: "task-1", Title: "complete", Description: "body", Status: pb.Status_STATUS_BLOCKED,
		Priority: pb.Priority_PRIORITY_P9, Epic: "epic-1", Assignee: "worker", Labels: []string{"fleet"}, DependsOn: []string{"task-0"},
		Created: timestamppb.New(now), Updated: timestamppb.New(now.Add(time.Hour)),
		Metadata:  &pb.Metadata{Custom: map[string]string{"nip34.repo_addr": "30617:owner:repo"}},
		IssueType: "feature", Owner: "owner", CreatedBy: "creator", StartedAt: timestamppb.New(now.Add(time.Minute)),
		CloseReason: "later", StatusReason: "dependency unavailable", ClosedAt: timestamppb.New(now.Add(2 * time.Hour)),
		AcceptanceCriteria: "green", Notes: "handoff",
		Dependencies:    []*pb.Dependency{{IssueId: "task-1", DependsOnId: "task-0", Type: "blocked-by", CreatedAt: timestamppb.New(now), CreatedBy: "creator", Metadata: "{\"source\":\"bd\"}"}},
		Comments:        []*pb.Comment{{Id: "c1", IssueId: "task-1", Author: "Gus", Text: "note", CreatedAt: timestamppb.New(now)}},
		DependencyCount: &depCount, DependentCount: &dependentCount, CommentCount: &commentCount,
		ClaimedAt: timestamppb.New(now.Add(time.Minute)), BlockedAt: timestamppb.New(now.Add(2 * time.Minute)), ReviewedAt: timestamppb.New(now.Add(3 * time.Minute)),
		BlockerDescription: "waiting", Checkpoints: []*pb.Checkpoint{{Id: "cp1", Actor: "worker", Status: "blocked", Summary: "checkpoint", CreatedAt: timestamppb.New(now), Evidence: []*pb.ArtifactReference{ref}}},
		Branch: "feature/task-1", Commits: []string{"abcdef1"}, Patches: []*pb.ArtifactReference{ref}, PullRequests: []string{"https://forge/pr/1"}, Evidence: []*pb.ArtifactReference{ref, {Kind: "nostr-event", Reference: hash}},
		Review:      &pb.Review{Required: true, Requirements: []string{"maintainer"}, Reviewer: "Gus", State: "approved"},
		QualityGate: &pb.QualityGate{Required: true, State: "pass", Source: "pstf", CheckedAt: timestamppb.New(now), Evidence: []*pb.ArtifactReference{ref}},
		Project:     "nostrig", Queue: "p0", Repository: "30617:owner:repo",
		ExecutionAttempts: []*pb.ExecutionAttempt{{Id: "attempt-1", Agent: "Netward", AgentSession: "session-1", Status: "blocked", StartedAt: timestamppb.New(now), EndedAt: timestamppb.New(now.Add(time.Minute)), Branch: "feature/task-1", Commits: []string{"abcdef1"}, Patches: []*pb.ArtifactReference{ref}, PullRequests: []string{"https://forge/pr/1"}, Evidence: []*pb.ArtifactReference{ref}, StatusReason: "blocked"}},
		AgentSessions:     []*pb.AgentSessionReference{{Id: "session-1", Agent: "Netward", Status: "complete", StartedAt: timestamppb.New(now), EndedAt: timestamppb.New(now.Add(time.Minute)), Transcript: ref}},
	}
	doc, err := FromProto(issue)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeCanonical(doc)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"priority":9`) || strings.Contains(string(raw), "transcript_text") {
		t.Fatalf("canonical encoding wrong: %s", raw)
	}
	decoded, err := DecodeCanonical(raw)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ToProto(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got, issue) {
		t.Fatalf("complete round trip mismatch:\ngot  %#v\nwant %#v", got, issue)
	}
}

func TestCanonicalRejectsUnknownStringPriorityAndNonBlossomArtifact(t *testing.T) {
	cases := []string{
		`{"schema_version":"cascadia.task-state.v2","id":"t","title":"t","status":"open","priority":"P1"}`,
		`{"schema_version":"cascadia.task-state.v2","id":"t","title":"t","status":"open","unknown":true}`,
		`{"schema_version":"cascadia.task-state.v2","id":"t","title":"t","status":"open","patches":[{"url":"https://example/x","sha256":"abc"}]}`,
	}
	for _, input := range cases {
		if _, err := DecodeCanonical([]byte(input)); err == nil {
			t.Fatalf("expected rejection for %s", input)
		}
	}
}

func TestGiteaLinkStableAndSyncMetadataNonMaterial(t *testing.T) {
	issue := &pb.Issue{Id: "task-1", Title: "task", Status: pb.Status_STATUS_OPEN, Metadata: &pb.Metadata{Custom: map[string]string{}}}
	if err := SetGiteaLink(issue.Metadata.Custom, GiteaLink{BaseURL: "https://gitea.example/", Owner: "acme", Repo: "repo", IssueNumber: 42}); err != nil {
		t.Fatal(err)
	}
	link, linked, err := ParseGiteaLink(issue.Metadata.Custom)
	if err != nil || !linked || link.IssueURL != "https://gitea.example/acme/repo/issues/42" {
		t.Fatalf("link = %#v,%v,%v", link, linked, err)
	}
	doc, err := FromProto(issue)
	if err != nil {
		t.Fatal(err)
	}
	before := MaterialRevision(doc)
	doc.Metadata["sync.origin"] = "gitea"
	doc.Metadata["sync.gitea.source_revision"] = StableRevision(map[string]string{"state": "open"})
	if after := MaterialRevision(doc); after != before {
		t.Fatalf("sync metadata changed material revision: %s != %s", after, before)
	}
}
