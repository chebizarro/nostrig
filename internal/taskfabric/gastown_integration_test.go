package taskfabric

import (
	"context"
	"strings"
	"testing"
	"time"

	gonostr "fiatjaf.com/nostr"
	beadspb "github.com/chebizarro/nostrig/gen/beads"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
)

const gastownTestRepo = "30617:owner:gastown"

func gastownCommand(t *testing.T, h *Handler, method string, params map[string]any, caller byte, commandID byte, now time.Time) *gonostr.Event {
	t.Helper()
	event, err := nip34.BuildContextVMCommand(method, "server", params, now.Add(-time.Second))
	if err != nil {
		t.Fatal(err)
	}
	event.ID, event.PubKey = testID(commandID), testPubKey(caller)
	response, err := h.HandleIntent(context.Background(), event, now)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func TestGastownTypedDependencyMutationRoundTripsCanonicalState(t *testing.T) {
	ledger := &memoryLedger{
		tasks: map[string]*beadspb.Issue{"task-1": {
			Id: "task-1", Title: "typed dependencies", Status: beadspb.Status_STATUS_OPEN,
			DependsOn: []string{"task-a"},
			Dependencies: []*beadspb.Dependency{
				{IssueId: "task-1", DependsOnId: "task-a", Type: "blocks"},
				{IssueId: "task-1", DependsOnId: "task-a", Type: "parent-child"},
			},
			Metadata: &beadspb.Metadata{Custom: map[string]string{"nip34.repo_addr": gastownTestRepo}},
		}},
		taskEventIDs: map[string]string{"task-1": testID(10).Hex()},
		queues:       map[string][]string{},
	}
	h := testHandler(ledger)
	response := gastownCommand(t, h, "task/update", map[string]any{
		"task_id":                   "task-1",
		"base_event_id":             ledger.taskRevision("task-1"),
		"remove_typed_dependencies": []map[string]any{{"task_id": "task-a", "type": "blocks"}},
		"add_typed_dependencies":    []map[string]any{{"task_id": "task-b", "type": "discovered-from", "metadata": "found while executing"}},
	}, 1, 11, time.Unix(11, 0))
	if strings.Contains(response.Content, "\"error\"") {
		t.Fatalf("dependency mutation failed: %s", response.Content)
	}

	got := ledger.tasks["task-1"]
	if len(got.Dependencies) != 2 {
		t.Fatalf("dependencies=%#v", got.Dependencies)
	}
	relations := map[string]*beadspb.Dependency{}
	for _, dependency := range got.Dependencies {
		relations[dependency.DependsOnId+"|"+dependency.Type] = dependency
	}
	if relations["task-a|blocks"] != nil || relations["task-a|parent-child"] == nil {
		t.Fatalf("exact typed removal did not preserve sibling relation: %#v", relations)
	}
	if relations["task-a|parent-child"].Metadata != "" {
		t.Fatalf("omitted dependency metadata was not empty: %#v", relations["task-a|parent-child"])
	}
	if dep := relations["task-b|discovered-from"]; dep == nil || dep.Metadata != "found while executing" || dep.CreatedAt == nil {
		t.Fatalf("typed add did not preserve v2 fields: %#v", dep)
	}
	if len(got.DependsOn) != 2 {
		t.Fatalf("legacy dependency projection was not derived: %#v", got.DependsOn)
	}

	roundTrip, err := nip34.ParseTaskStateEvent(ledger.lastTaskEvent)
	if err != nil {
		t.Fatal(err)
	}
	if len(roundTrip.Dependencies) != 2 || len(roundTrip.DependsOn) != 2 {
		t.Fatalf("canonical dependency round trip lost state: %#v", roundTrip)
	}
}

func TestGastownContextVMDispatchCompletionAndPerTaskCloseGates(t *testing.T) {
	ledger := &memoryLedger{
		tasks: map[string]*beadspb.Issue{"task-1": {
			Id: "task-1", Title: "dispatch me", Status: beadspb.Status_STATUS_OPEN,
			Review:      &beadspb.Review{Required: true, State: "requested", Reviewer: testPubKey(3).Hex()},
			QualityGate: &beadspb.QualityGate{Required: true, State: "pending"},
			Metadata:    &beadspb.Metadata{Custom: map[string]string{"nip34.repo_addr": gastownTestRepo}},
		}},
		taskEventIDs: map[string]string{"task-1": testID(20).Hex()},
		queues:       map[string][]string{},
	}
	quality := staticQuality{values: map[string]QualityResult{"task-1": {State: QualityPending, Result: "pending"}}}
	h := testHandler(ledger)
	h.Quality = quality
	h.ACL[testPubKey(2).Hex()] = CallerPolicy{Roles: []Role{RoleWorker}, WorkerID: "worker-a", Repositories: []string{gastownTestRepo}}
	h.ACL[testPubKey(3).Hex()] = CallerPolicy{Roles: []Role{RoleReviewer}, Repositories: []string{gastownTestRepo}}
	h.ACL[testPubKey(4).Hex()] = CallerPolicy{Roles: []Role{RoleReviewer}, Repositories: []string{gastownTestRepo}}

	response := gastownCommand(t, h, "task/assign", map[string]any{
		"task_id": "task-1", "base_event_id": ledger.taskRevision("task-1"), "assignee": "worker-a",
		"execution_attempt_id": "attempt-1", "agent_session_id": "session-1", "branch": "worker/task-1",
	}, 1, 21, time.Unix(21, 0))
	if strings.Contains(response.Content, "\"error\"") {
		t.Fatalf("dispatch failed: %s", response.Content)
	}
	dispatched := ledger.tasks["task-1"]
	if len(dispatched.ExecutionAttempts) != 1 || dispatched.ExecutionAttempts[0].Status != "dispatched" ||
		len(dispatched.AgentSessions) != 1 || dispatched.AgentSessions[0].Status != "active" {
		t.Fatalf("dispatch lifecycle was not recorded atomically: %#v", dispatched)
	}

	response = gastownCommand(t, h, "task/update", map[string]any{
		"task_id": "task-1", "base_event_id": ledger.taskRevision("task-1"), "status": "in_progress",
		"execution_attempt_id": "attempt-1", "attempt_status": "completed", "attempt_status_reason": "worker done",
		"attempt_commits": []string{"abc123"}, "attempt_pull_requests": []string{"https://example.test/pr/1"},
		"attempt_evidence_ids": []string{"evidence:event-1"},
	}, 2, 22, time.Unix(22, 0))
	if strings.Contains(response.Content, "\"error\"") {
		t.Fatalf("worker completion failed: %s", response.Content)
	}
	completed := ledger.tasks["task-1"]
	attempt := completed.ExecutionAttempts[0]
	if completed.Status == beadspb.Status_STATUS_CLOSED || attempt.Status != "completed" || attempt.EndedAt == nil ||
		len(attempt.Commits) != 1 || len(attempt.PullRequests) != 1 || len(attempt.Evidence) != 1 {
		t.Fatalf("completion lifecycle was not persisted: %#v", completed)
	}
	if completed.Review == nil || !completed.Review.Required || completed.Review.State != "requested" {
		t.Fatalf("worker completion did not require review: %#v", completed.Review)
	}
	if completed.AgentSessions[0].Status != "completed" || completed.AgentSessions[0].EndedAt == nil {
		t.Fatalf("session was not completed with its attempt: %#v", completed.AgentSessions[0])
	}
	roundTrip, err := nip34.ParseTaskStateEvent(ledger.lastTaskEvent)
	if err != nil || len(roundTrip.ExecutionAttempts) != 1 || len(roundTrip.AgentSessions) != 1 {
		t.Fatalf("execution lifecycle canonical round trip failed: %#v err=%v", roundTrip, err)
	}

	response = gastownCommand(t, h, "task/update", map[string]any{
		"task_id": "task-1", "base_event_id": ledger.taskRevision("task-1"), "status": "closed",
	}, 1, 23, time.Unix(23, 0))
	if !strings.Contains(response.Content, "use task/close") || ledger.tasks["task-1"].Status == beadspb.Status_STATUS_CLOSED {
		t.Fatalf("generic update bypassed close policy: task=%#v response=%s", ledger.tasks["task-1"], response.Content)
	}

	response = gastownCommand(t, h, "task/close", map[string]any{
		"task_id": "task-1", "base_event_id": ledger.taskRevision("task-1"),
	}, 2, 24, time.Unix(24, 0))
	if !strings.Contains(response.Content, "reviewer_required") || ledger.tasks["task-1"].Status == beadspb.Status_STATUS_CLOSED {
		t.Fatalf("worker bypassed per-task review gate: task=%#v response=%s", ledger.tasks["task-1"], response.Content)
	}

	response = gastownCommand(t, h, "task/close", map[string]any{
		"task_id": "task-1", "base_event_id": ledger.taskRevision("task-1"), "acceptance_evidence_ids": []string{"review:event-wrong"},
	}, 4, 25, time.Unix(25, 0))
	if !strings.Contains(response.Content, "designated_reviewer_required") || ledger.tasks["task-1"].Status == beadspb.Status_STATUS_CLOSED {
		t.Fatalf("wrong reviewer bypassed designated review: task=%#v response=%s", ledger.tasks["task-1"], response.Content)
	}

	response = gastownCommand(t, h, "task/close", map[string]any{
		"task_id": "task-1", "base_event_id": ledger.taskRevision("task-1"), "acceptance_evidence_ids": []string{"review:event-pending"},
	}, 3, 26, time.Unix(26, 0))
	if !strings.Contains(response.Content, "quality_required") || ledger.tasks["task-1"].Status == beadspb.Status_STATUS_CLOSED {
		t.Fatalf("reviewer bypassed per-task quality gate: task=%#v response=%s", ledger.tasks["task-1"], response.Content)
	}

	quality.values["task-1"] = QualityResult{State: QualityPassing, Result: "pass"}
	response = gastownCommand(t, h, "task/close", map[string]any{
		"task_id": "task-1", "base_event_id": ledger.taskRevision("task-1"), "acceptance_evidence_ids": []string{"review:event-2"},
	}, 3, 27, time.Unix(27, 0))
	if strings.Contains(response.Content, "\"error\"") {
		t.Fatalf("gated reviewer close failed: %s", response.Content)
	}
	closed := ledger.tasks["task-1"]
	if closed.Status != beadspb.Status_STATUS_CLOSED || closed.Review.State != "approved" || closed.Review.Reviewer != testPubKey(3).Hex() || closed.ReviewedAt == nil {
		t.Fatalf("reviewed close did not persist policy state: %#v", closed)
	}
}

func TestGastownRejectsInvalidUpdatesAndActiveAttemptReassignmentOrClose(t *testing.T) {
	ledger := &memoryLedger{
		tasks: map[string]*beadspb.Issue{"task-1": {
			Id: "task-1", Title: "active", Status: beadspb.Status_STATUS_IN_PROGRESS, Priority: beadspb.Priority_PRIORITY_P1, Assignee: "worker-a",
			ExecutionAttempts: []*beadspb.ExecutionAttempt{{Id: "attempt-active", Agent: "worker-a", Status: "running"}},
			Metadata:          &beadspb.Metadata{Custom: map[string]string{"nip34.repo_addr": gastownTestRepo}},
		}},
		taskEventIDs: map[string]string{"task-1": testID(30).Hex()},
		queues:       map[string][]string{},
	}
	h := testHandler(ledger)

	response := gastownCommand(t, h, "task/update", map[string]any{
		"task_id": "task-1", "base_event_id": ledger.taskRevision("task-1"), "status": "typo",
	}, 1, 31, time.Unix(31, 0))
	if !strings.Contains(response.Content, "invalid task status") || ledger.tasks["task-1"].Status != beadspb.Status_STATUS_IN_PROGRESS {
		t.Fatalf("invalid status mutated task: task=%#v response=%s", ledger.tasks["task-1"], response.Content)
	}
	response = gastownCommand(t, h, "task/update", map[string]any{
		"task_id": "task-1", "base_event_id": ledger.taskRevision("task-1"), "priority": "urgent",
	}, 1, 32, time.Unix(32, 0))
	if !strings.Contains(response.Content, "invalid task priority") || ledger.tasks["task-1"].Priority != beadspb.Priority_PRIORITY_P1 {
		t.Fatalf("invalid priority mutated task: task=%#v response=%s", ledger.tasks["task-1"], response.Content)
	}

	response = gastownCommand(t, h, "task/assign", map[string]any{
		"task_id": "task-1", "base_event_id": ledger.taskRevision("task-1"), "assignee": "worker-b",
		"execution_attempt_id": "attempt-new",
	}, 1, 33, time.Unix(33, 0))
	if !strings.Contains(response.Content, "still active") || ledger.tasks["task-1"].Assignee != "worker-a" {
		t.Fatalf("reassignment orphaned active attempt: task=%#v response=%s", ledger.tasks["task-1"], response.Content)
	}

	response = gastownCommand(t, h, "task/close", map[string]any{
		"task_id": "task-1", "base_event_id": ledger.taskRevision("task-1"),
	}, 1, 34, time.Unix(34, 0))
	if !strings.Contains(response.Content, "still active") || ledger.tasks["task-1"].Status == beadspb.Status_STATUS_CLOSED {
		t.Fatalf("close orphaned active attempt: task=%#v response=%s", ledger.tasks["task-1"], response.Content)
	}
}
