package taskfabric

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	gonostr "fiatjaf.com/nostr"
	beadspb "github.com/chebizarro/nostrig/gen/beads"
)

type captureAudit struct {
	records []AuthzAuditRecord
	err     error
}

func (a *captureAudit) Record(_ context.Context, record AuthzAuditRecord) error {
	a.records = append(a.records, record)
	return a.err
}

func authzTestIssue(id, repo, assignee string) *beadspb.Issue {
	return &beadspb.Issue{
		Id:       id,
		Title:    id,
		Assignee: assignee,
		Metadata: &beadspb.Metadata{Custom: map[string]string{"nip34.repo_addr": repo}},
	}
}

func TestAuthorizationMatrix(t *testing.T) {
	const repo = "30617:owner:repo-a"
	caller := testPubKey(1).Hex()
	other := testPubKey(2).Hex()
	cases := []struct {
		name    string
		role    Role
		method  string
		params  map[string]any
		task    *beadspb.Issue
		allowed bool
		reason  string
	}{
		{"admin create", RoleAdmin, "task/create", map[string]any{"repo_addr": repo, "task_id": "new", "title": "new"}, nil, true, ""},
		{"maintainer delete", RoleMaintainer, "task/delete", map[string]any{"task_id": "task-1"}, authzTestIssue("task-1", repo, ""), true, ""},
		{"dispatcher assign", RoleDispatcher, "task/assign", map[string]any{"task_id": "task-1", "assignee": other}, authzTestIssue("task-1", repo, ""), true, ""},
		{"dispatcher reprioritize", RoleDispatcher, "task/update", map[string]any{"task_id": "task-1", "priority": "P0"}, authzTestIssue("task-1", repo, ""), true, ""},
		{"dispatcher field denied", RoleDispatcher, "task/update", map[string]any{"task_id": "task-1", "title": "rewrite"}, authzTestIssue("task-1", repo, ""), false, "field_denied"},
		{"worker self claim", RoleWorker, "task/claim", map[string]any{"task_id": "task-1", "claimer": caller}, authzTestIssue("task-1", repo, ""), true, ""},
		{"worker impersonation", RoleWorker, "task/claim", map[string]any{"task_id": "task-1", "claimer": other}, authzTestIssue("task-1", repo, ""), false, "worker_identity_mismatch"},
		{"worker assigned update", RoleWorker, "task/update", map[string]any{"task_id": "task-1", "status": "blocked"}, authzTestIssue("task-1", repo, caller), true, ""},
		{"worker other task", RoleWorker, "task/update", map[string]any{"task_id": "task-1", "status": "blocked"}, authzTestIssue("task-1", repo, other), false, "worker_not_assignee"},
		{"worker cannot reprioritize", RoleWorker, "task/update", map[string]any{"task_id": "task-1", "priority": "P0"}, authzTestIssue("task-1", repo, caller), false, "field_denied"},
		{"reviewer quality read", RoleReviewer, "task/quality-status", map[string]any{"repo_addr": repo, "task_id": "task-1"}, authzTestIssue("task-1", repo, ""), true, ""},
		{"read only queue", RoleReadOnly, "queue/list", map[string]any{"repo_addr": repo, "queue": "backlog"}, nil, true, ""},
		{"read only create denied", RoleReadOnly, "task/create", map[string]any{"repo_addr": repo, "task_id": "new", "title": "new"}, nil, false, "method_denied"},
		{"worker enqueue denied", RoleWorker, "queue/enqueue", map[string]any{"repo_addr": repo, "queue": "backlog", "task_id": "task-1"}, authzTestIssue("task-1", repo, caller), false, "method_denied"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tasks := map[string]*beadspb.Issue{}
			if tc.task != nil {
				tasks[tc.task.Id] = tc.task
			}
			audit := &captureAudit{}
			h := &Handler{
				Ledger: &memoryLedger{tasks: tasks, queues: map[string][]string{}},
				ACL:    map[string]CallerPolicy{caller: {Roles: []Role{tc.role}, Repositories: []string{repo}}},
				Audit:  audit,
			}
			ev := &gonostr.Event{ID: testID(1), PubKey: testPubKey(1)}
			err := h.authorize(context.Background(), ev, tc.method, tc.params, caller, time.Unix(1, 0))
			if tc.allowed && err != nil {
				t.Fatalf("expected allow, got %v", err)
			}
			if !tc.allowed {
				var denied *AuthorizationError
				if !errors.As(err, &denied) || denied.Reason != tc.reason {
					t.Fatalf("denial=%v, want %s", err, tc.reason)
				}
			}
			if len(audit.records) != 1 {
				t.Fatalf("audit records=%d, want 1", len(audit.records))
			}
			wantDecision := "deny"
			if tc.allowed {
				wantDecision = "allow"
			}
			if audit.records[0].Decision != wantDecision {
				t.Fatalf("audit decision=%q, want %q", audit.records[0].Decision, wantDecision)
			}
		})
	}
}

func TestAuthorizationFailsClosedForUnknownCallerAndCrossRepo(t *testing.T) {
	const repoA = "30617:owner:repo-a"
	const repoB = "30617:owner:repo-b"
	caller := testPubKey(1).Hex()
	h := &Handler{
		Ledger: &memoryLedger{tasks: map[string]*beadspb.Issue{"task-1": authzTestIssue("task-1", repoB, "")}, queues: map[string][]string{}},
		ACL:    map[string]CallerPolicy{caller: {Roles: []Role{RoleAdmin}, Repositories: []string{repoA}}},
		Audit:  &captureAudit{},
	}
	ev := &gonostr.Event{ID: testID(1), PubKey: testPubKey(1)}
	cases := []struct {
		name, method string
		params       map[string]any
		caller       string
		want         string
	}{
		{"unknown", "task/create", map[string]any{"repo_addr": repoA}, testPubKey(2).Hex(), "unknown_caller"},
		{"cross task", "task/update", map[string]any{"task_id": "task-1", "status": "blocked"}, caller, "repo_denied"},
		{"cross queue", "queue/list", map[string]any{"repo_addr": repoB, "queue": "backlog"}, caller, "repo_denied"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := h.authorize(context.Background(), ev, tc.method, tc.params, tc.caller, time.Unix(1, 0))
			var denied *AuthorizationError
			if !errors.As(err, &denied) || denied.Reason != tc.want {
				t.Fatalf("denial=%v, want %s", err, tc.want)
			}
		})
	}
}

func TestAuthorizationAuditFailurePreventsMutation(t *testing.T) {
	const repo = "30617:owner:repo"
	caller := testPubKey(1).Hex()
	ledger := &memoryLedger{tasks: map[string]*beadspb.Issue{}, queues: map[string][]string{}}
	h := &Handler{
		Ledger: ledger,
		ACL:    map[string]CallerPolicy{caller: {Roles: []Role{RoleAdmin}, Repositories: []string{repo}}},
		Audit:  &captureAudit{err: errors.New("audit unavailable")},
	}
	ev := &gonostr.Event{ID: testID(1), PubKey: testPubKey(1)}
	params := map[string]any{"repo_addr": repo, "task_id": "task-1", "title": "blocked"}
	if _, err := h.dispatch(context.Background(), ev, "task/create", mustRawJSON(t, params), caller, time.Unix(1, 0)); err == nil {
		t.Fatal("expected audit failure")
	}
	if ledger.tasks["task-1"] != nil {
		t.Fatal("mutation occurred before allow audit completed")
	}
}

func mustRawJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
