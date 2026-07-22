package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"testing"

	beadspb "github.com/chebizarro/nostrig/gen/beads"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func TestAgentWorkflowCommandsAreRegistered(t *testing.T) {
	root := newRootCmd()
	task, _, err := root.Find([]string{"task"})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, child := range task.Commands() {
		got = append(got, child.Name())
	}
	sort.Strings(got)
	want := []string{"assign", "block", "claim", "close", "create", "get", "list", "ready", "update", "watch"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("task commands=%v, want %v", got, want)
	}
	for _, path := range [][]string{{"queue", "list"}, {"queue", "claim-next"}} {
		if _, _, err := root.Find(path); err != nil {
			t.Fatalf("missing %v: %v", path, err)
		}
	}
}

func TestAgentCommandsExposeJSONWithoutSecretBearingFlags(t *testing.T) {
	root := newRootCmd()
	for _, path := range [][]string{
		{"task", "get"}, {"task", "list"}, {"task", "ready"}, {"task", "create"},
		{"task", "assign"}, {"task", "claim"}, {"task", "update"}, {"task", "block"},
		{"task", "close"}, {"task", "watch"}, {"queue", "list"}, {"queue", "claim-next"},
	} {
		cmd, _, err := root.Find(path)
		if err != nil {
			t.Fatalf("find %v: %v", path, err)
		}
		if lookupFlag(cmd, "json") == nil {
			t.Errorf("%v does not expose --json", path)
		}
		for _, secret := range []string{"private-key", "signer-client-secret-key", "signer-bunker-url"} {
			if lookupFlag(cmd, secret) != nil {
				t.Errorf("%v exposes secret-bearing --%s", path, secret)
			}
		}
	}
}

func TestTaskBlockDryRunHasStableEnvelopeAndEvidence(t *testing.T) {
	cmd := newRootCmd()
	cmd.SetArgs([]string{"task", "block", "--json", "--task-id", "task-1", "--base-event-id", fmt.Sprintf("%064x", 1), "--reason", "relay unavailable", "--evidence-id", "event:abc", "--recipient", fmt.Sprintf("%064x", 2), "--dry-run"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	var envelope mutationEnvelope
	if err := json.Unmarshal(out.Bytes(), &envelope); err != nil {
		t.Fatalf("decode envelope: %v\n%s", err, out.String())
	}
	if envelope.Schema != "nostrig.mutation.v1" || envelope.Operation != "task.block" || !envelope.DryRun {
		t.Fatalf("unexpected envelope: %#v", envelope)
	}
	if !reflect.DeepEqual(envelope.SubmittedEvidenceIDs, []string{"event:abc"}) || len(envelope.EvidenceIDs) != 0 || envelope.Event == nil {
		t.Fatalf("evidence/event missing: %#v", envelope)
	}
	var request struct {
		Method string         `json:"method"`
		Params map[string]any `json:"params"`
	}
	if err := json.Unmarshal([]byte(envelope.Event.Content), &request); err != nil {
		t.Fatal(err)
	}
	if request.Method != "task/update" || request.Params["status"] != "blocked" || request.Params["blocker_description"] != "relay unavailable" {
		t.Fatalf("request=%#v", request)
	}
}

func TestTaskExecutionAndTypedDependencyDryRunUsesContextVMParams(t *testing.T) {
	cmd := newRootCmd()
	cmd.SetArgs([]string{
		"task", "update", "--json", "--task-id", "task-1",
		"--base-event-id", fmt.Sprintf("%064x", 1),
		"--execution-attempt-id", "attempt-1", "--attempt-status", "completed",
		"--attempt-commit", "abc123", "--attempt-pr", "https://example.test/pr/1",
		"--attempt-evidence-id", "event:evidence",
		"--add-typed-dep", "parent-child:task-0",
		"--recipient", fmt.Sprintf("%064x", 2), "--dry-run",
	})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	var envelope mutationEnvelope
	if err := json.Unmarshal(out.Bytes(), &envelope); err != nil {
		t.Fatalf("decode envelope: %v\n%s", err, out.String())
	}
	var request struct {
		Method string         `json:"method"`
		Params map[string]any `json:"params"`
	}
	if err := json.Unmarshal([]byte(envelope.Event.Content), &request); err != nil {
		t.Fatal(err)
	}
	if request.Method != "task/update" || request.Params["execution_attempt_id"] != "attempt-1" || request.Params["attempt_status"] != "completed" {
		t.Fatalf("request=%#v", request)
	}
	if values, ok := request.Params["attempt_commits"].([]any); !ok || len(values) != 1 || values[0] != "abc123" {
		t.Fatalf("attempt commits=%#v", request.Params["attempt_commits"])
	}
	deps, ok := request.Params["add_typed_dependencies"].([]any)
	if !ok || len(deps) != 1 {
		t.Fatalf("typed dependencies=%#v", request.Params["add_typed_dependencies"])
	}
	dep, ok := deps[0].(map[string]any)
	if !ok || dep["type"] != "parent-child" || dep["task_id"] != "task-0" {
		t.Fatalf("typed dependency=%#v", deps[0])
	}
}

func TestQueueClaimNextDryRunUsesRevisionAndLease(t *testing.T) {
	cmd := newRootCmd()
	queueRevision := fmt.Sprintf("%064x", 3)
	cmd.SetArgs([]string{"queue", "claim-next", "--json", "--repo-addr", "30617:owner:repo", "--base-event-id", queueRevision, "--lease-seconds", "90", "--recipient", fmt.Sprintf("%064x", 2), "--dry-run"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	var envelope mutationEnvelope
	if err := json.Unmarshal(out.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Operation != "queue.claim-next" || envelope.Event == nil {
		t.Fatalf("envelope=%#v", envelope)
	}
	var request struct {
		Params map[string]any `json:"params"`
	}
	if err := json.Unmarshal([]byte(envelope.Event.Content), &request); err != nil {
		t.Fatal(err)
	}
	if request.Params["base_event_id"] != queueRevision || request.Params["lease_seconds"] != float64(90) {
		t.Fatalf("params=%#v", request.Params)
	}
}

func TestReadyIssuesRequiresClosedDependenciesAndNoAssignee(t *testing.T) {
	issues := []*beadspb.Issue{
		{Id: "closed", Status: beadspb.Status_STATUS_CLOSED},
		{Id: "ready", Status: beadspb.Status_STATUS_OPEN, DependsOn: []string{"closed"}},
		{Id: "blocked-by-open", Status: beadspb.Status_STATUS_OPEN, DependsOn: []string{"missing"}},
		{Id: "typed-blocked", Status: beadspb.Status_STATUS_OPEN, Dependencies: []*beadspb.Dependency{{IssueId: "typed-blocked", DependsOnId: "missing", Type: "blocked-by"}}},
		{Id: "nonblocking-relation", Status: beadspb.Status_STATUS_OPEN, Dependencies: []*beadspb.Dependency{{IssueId: "nonblocking-relation", DependsOnId: "missing", Type: "parent-child"}}},
		{Id: "assigned", Status: beadspb.Status_STATUS_OPEN, Assignee: "agent"},
	}
	got := readyIssues(issues)
	if len(got) != 2 || got[0].Id != "ready" || got[1].Id != "nonblocking-relation" {
		t.Fatalf("ready=%#v", got)
	}
}

func TestTaskMutationRejectsEmptyRevisionLocally(t *testing.T) {
	cmd := newRootCmd()
	cmd.SetArgs([]string{"task", "claim", "--json", "--task-id", "task-1", "--base-event-id=", "--recipient", fmt.Sprintf("%064x", 2), "--dry-run"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	err := cmd.Execute()
	if err == nil || commandErrorExitCode(err) != exitUsage {
		t.Fatalf("err=%v exit=%d", err, commandErrorExitCode(err))
	}
	if out.Len() != 0 {
		t.Fatalf("invalid command emitted a mutation: %s", out.String())
	}
}

func TestCommandErrorExitCodesAreStable(t *testing.T) {
	if got := commandErrorExitCode(exitError(exitConflict, assertError("conflict"))); got != exitConflict {
		t.Fatalf("conflict exit=%d", got)
	}
	if got := commandErrorExitCode(assertError("task x not found")); got != exitNotFound {
		t.Fatalf("not-found exit=%d", got)
	}
}

type assertError string

func (e assertError) Error() string { return string(e) }

func lookupFlag(cmd *cobra.Command, name string) *pflag.Flag {
	if flag := cmd.Flags().Lookup(name); flag != nil {
		return flag
	}
	return cmd.InheritedFlags().Lookup(name)
}
