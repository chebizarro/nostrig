package reconcile

import (
	"context"
	"testing"

	pb "github.com/chebizarro/nostrig/gen/beads"
	"github.com/chebizarro/nostrig/internal/gitea"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
	"google.golang.org/protobuf/proto"
)

type fakeRepairSides struct {
	task         *pb.Issue
	status       *nip34.StatusEvent
	issue        *gitea.Issue
	giteaWrites  int
	statusWrites int
	taskWrites   int
}

func (f *fakeRepairSides) UpdateIssueState(_ context.Context, _ gitea.IssueKey, state, _ string) (*gitea.Issue, error) {
	f.giteaWrites++
	copyIssue := *f.issue
	copyIssue.State = state
	f.issue = &copyIssue
	return f.issue, nil
}

func (f *fakeRepairSides) UpdateNIP34Status(_ context.Context, _ *pb.Issue, kind int, _ string) error {
	f.statusWrites++
	f.status = &nip34.StatusEvent{Kind: kind, RootEventID: f.task.Metadata.Custom["nostr.id"]}
	return nil
}

func (f *fakeRepairSides) UpdateTaskState(_ context.Context, issue *pb.Issue, _ string) error {
	f.taskWrites++
	f.task = proto.Clone(issue).(*pb.Issue)
	return nil
}

func TestReconcileDetectsAndRepairsDriftThenConverges(t *testing.T) {
	task := linkedTask(t)
	sides := &fakeRepairSides{
		task:   task,
		status: &nip34.StatusEvent{Kind: nip34.KindStatusOpen, RootEventID: task.Metadata.Custom["nostr.id"]},
		issue:  &gitea.Issue{Number: 42, Title: "Gitea title", Body: "Gitea body", State: "open", ETag: `"one"`},
	}
	engine := &Reconciler{Gitea: sides, NIP34: sides, Tasks: sides}

	report, err := engine.Reconcile(context.Background(), []Item{{
		Task: sides.task, TaskEventID: "task-rev-1", Status: sides.status, Gitea: sides.issue,
	}}, false)
	if err != nil {
		t.Fatal(err)
	}
	if report.Summary.Drifted != 1 || sides.giteaWrites+sides.statusWrites+sides.taskWrites != 0 {
		t.Fatalf("report-only result = %#v writes=%d/%d/%d", report.Summary, sides.giteaWrites, sides.statusWrites, sides.taskWrites)
	}

	report, err = engine.Reconcile(context.Background(), []Item{{
		Task: sides.task, TaskEventID: "task-rev-1", Status: sides.status, Gitea: sides.issue,
	}}, true)
	if err != nil {
		t.Fatal(err)
	}
	if report.Summary.Repaired != 1 || sides.giteaWrites != 1 || sides.statusWrites != 1 || sides.taskWrites != 1 {
		t.Fatalf("repair result = %#v writes=%d/%d/%d", report.Summary, sides.giteaWrites, sides.statusWrites, sides.taskWrites)
	}
	if sides.task.Title != "Gitea title" || sides.task.Description != "Gitea body" || sides.issue.State != "closed" || sides.status.Kind != nip34.KindStatusClosed {
		t.Fatalf("repair did not apply authoritative fields: task=%#v issue=%#v status=%#v", sides.task, sides.issue, sides.status)
	}

	report, err = engine.Reconcile(context.Background(), []Item{{
		Task: sides.task, TaskEventID: "task-rev-2", Status: sides.status, Gitea: sides.issue,
	}}, true)
	if err != nil {
		t.Fatal(err)
	}
	if report.Summary.InSync != 1 || sides.giteaWrites != 1 || sides.statusWrites != 1 || sides.taskWrites != 1 {
		t.Fatalf("loop did not converge: %#v writes=%d/%d/%d", report.Summary, sides.giteaWrites, sides.statusWrites, sides.taskWrites)
	}
}

func TestReconcileReportsUntrustedStatusWithoutTreatingItAsState(t *testing.T) {
	task := linkedTask(t)
	task.Status = pb.Status_STATUS_OPEN
	status := &nip34.StatusEvent{Kind: nip34.KindStatusOpen, RootEventID: task.Metadata.Custom["nostr.id"]}
	issue := &gitea.Issue{Number: 42, Title: task.Title, Body: task.Description, State: "open"}
	report, err := (&Reconciler{}).Reconcile(context.Background(), []Item{{
		Task: task, Status: status, UntrustedStatusCount: 1, Gitea: issue,
	}}, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Items) != 1 || len(report.Items[0].Warnings) != 1 || report.Items[0].Warnings[0] != "untrusted_nip34_status" {
		t.Fatalf("report = %#v", report.Items)
	}
	for _, drift := range report.Items[0].Drift {
		if drift.Target == "nip34" && drift.Field == "status" {
			t.Fatalf("untrusted event altered trusted status comparison: %#v", drift)
		}
	}
}

func linkedTask(t *testing.T) *pb.Issue {
	t.Helper()
	task := &pb.Issue{
		Id: "task-1", Title: "old", Description: "old body", Status: pb.Status_STATUS_CLOSED,
		Metadata: &pb.Metadata{Custom: map[string]string{
			"nostr.id":        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"nostr.kind":      "1621",
			"nip34.repo_addr": "30617:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:repo",
		}},
	}
	if err := LinkIssue(task, "https://gitea.example", "acme", "repo", 42); err != nil {
		t.Fatal(err)
	}
	return task
}
