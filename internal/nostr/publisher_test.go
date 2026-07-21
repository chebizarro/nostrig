package nostr

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	gonostr "fiatjaf.com/nostr"
	beadspb "github.com/chebizarro/nostrig/gen/beads"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTaskStateRoundTripLossless(t *testing.T) {
	now := time.Unix(1234, 0).UTC()
	issue := &beadspb.Issue{Id: "repo-abc12345", Title: "Implement fabric", Description: "full model", Status: beadspb.Status_STATUS_IN_PROGRESS, Priority: beadspb.Priority_PRIORITY_P1, Epic: "repo-epic1234", Assignee: "agent-a", Labels: []string{"issue", "fabric"}, DependsOn: []string{"repo-dep00001"}, Created: timestamppb.New(now), Updated: timestamppb.New(now.Add(time.Minute)), Metadata: &beadspb.Metadata{Custom: map[string]string{"nostr.id": "root-event", "nip34.repo_addr": "30617:pub:repo", "nostrig.beads_id": "repo-abc12345", "nostrig.id_format": "spec"}}}
	author := fmt.Sprintf("%064x", 1)
	ev, err := BuildTaskStateEvent(issue, author, now)
	if err != nil {
		t.Fatal(err)
	}
	pk, _ := gonostr.PubKeyFromHex(author)
	ev.PubKey = pk
	if ev.Kind != KindCanonicalState {
		t.Fatalf("kind=%d", ev.Kind)
	}
	d, ok := TagD(ev)
	if !ok || d != "task:repo-abc12345" {
		t.Fatalf("d tag=%q ok=%v", d, ok)
	}
	got, err := ParseTaskStateEvent(ev)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got, issue) {
		t.Fatalf("round trip mismatch:\ngot  %#v\nwant %#v", got, issue)
	}
}

func TestCanonicalExportRoundTripIncludesEpicCollection(t *testing.T) {
	now := time.Unix(1234, 0).UTC()
	export := &beadspb.Export{
		Issues: []*beadspb.Issue{{Id: "repo-abc12345", Title: "Implement fabric", Description: "full model", Status: beadspb.Status_STATUS_IN_PROGRESS, Priority: beadspb.Priority_PRIORITY_P1, Epic: "repo-epic1234", Assignee: "agent-a", Labels: []string{"issue", "fabric"}, DependsOn: []string{"repo-dep00001"}, Created: timestamppb.New(now), Updated: timestamppb.New(now.Add(time.Minute)), Metadata: &beadspb.Metadata{Custom: map[string]string{"nostr.id": "root-event", "nip34.repo_addr": "30617:pub:repo", "nostrig.beads_id": "repo-abc12345", "nostrig.id_format": "spec"}}}},
		Epics:  []*beadspb.Epic{{Id: "repo-epic1234", Name: "Fabric epic"}},
	}
	author := fmt.Sprintf("%064x", 1)
	events, err := BuildCanonicalEvents(export, author, now)
	if err != nil {
		t.Fatal(err)
	}
	var state *gonostr.Event
	var epic *gonostr.Event
	pk, _ := gonostr.PubKeyFromHex(author)
	for _, ev := range events {
		ev.PubKey = pk
		if ev.Kind == KindCanonicalState {
			state = ev
		}
		if ev.Kind == KindNamedList {
			epic = ev
		}
	}
	if state == nil {
		t.Fatal("missing 30900 task-state event")
	}
	got, err := ParseTaskStateEvent(state)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got, export.Issues[0]) {
		t.Fatalf("export task state was not lossless:\ngot  %#v\nwant %#v", got, export.Issues[0])
	}
	if epic == nil {
		t.Fatal("missing NIP-51 epic collection")
	}
	if d, _ := TagD(epic); d != "epic:repo-epic1234" {
		t.Fatalf("epic d tag=%q", d)
	}
	if !hasTag(epic, "a", "30900:"+author+":task:repo-abc12345") {
		t.Fatalf("epic collection missing task member: %#v", epic.Tags)
	}
}

func hasTag(ev *gonostr.Event, key, value string) bool {
	for _, tag := range ev.Tags {
		if len(tag) >= 2 && tag[0] == key && tag[1] == value {
			return true
		}
	}
	return false
}

func TestParseTaskStateRejectsMalformedTagContentCombinations(t *testing.T) {
	author := fmt.Sprintf("%064x", 1)
	pk, _ := gonostr.PubKeyFromHex(author)
	issue := &beadspb.Issue{
		Id: "task-1", Title: "strict", Status: beadspb.Status_STATUS_OPEN,
		Assignee: "worker-a", DependsOn: []string{"task-0"},
		Metadata: &beadspb.Metadata{Custom: map[string]string{"nip34.repo_addr": "30617:owner:repo"}},
	}
	base, err := BuildTaskStateEvent(issue, author, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	base.PubKey = pk
	cases := []struct {
		name   string
		mutate func(*gonostr.Event)
	}{
		{"content id", func(ev *gonostr.Event) {
			var state TaskState
			_ = json.Unmarshal([]byte(ev.Content), &state)
			state.ID = "attacker-task"
			raw, _ := json.Marshal(state)
			ev.Content = string(raw)
		}},
		{"assignee", func(ev *gonostr.Event) {
			for _, tag := range ev.Tags {
				if len(tag) >= 2 && tag[0] == "assignee" {
					tag[1] = "other"
				}
			}
		}},
		{"dependency coordinate", func(ev *gonostr.Event) {
			for _, tag := range ev.Tags {
				if len(tag) >= 2 && tag[0] == "depends-on" {
					tag[1] = "task:task-0"
				}
			}
		}},
		{"schema version", func(ev *gonostr.Event) {
			for _, tag := range ev.Tags {
				if len(tag) >= 2 && tag[0] == "schema" {
					tag[1] = "cascadia.task-state.v2"
				}
			}
		}},
		{"repo tag", func(ev *gonostr.Event) {
			for _, tag := range ev.Tags {
				if len(tag) >= 4 && tag[0] == "a" && tag[3] == "nip34-repo" {
					tag[1] = "30617:owner:other"
				}
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, _ := json.Marshal(base)
			var ev gonostr.Event
			_ = json.Unmarshal(raw, &ev)
			tc.mutate(&ev)
			if _, err := ParseTaskStateEvent(&ev); err == nil {
				t.Fatal("expected malformed state rejection")
			}
		})
	}
}

func TestCanonicalTombstoneRequiresMatchingAuthorAndCoordinate(t *testing.T) {
	author := fmt.Sprintf("%064x", 1)
	pk, _ := gonostr.PubKeyFromHex(author)
	target, err := BuildTaskStateEvent(&beadspb.Issue{Id: "task-1", Title: "delete", Status: beadspb.Status_STATUS_OPEN, Metadata: &beadspb.Metadata{Custom: map[string]string{"nip34.repo_addr": "30617:owner:repo"}}}, author, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	target.ID[31], target.PubKey = 1, pk
	tombstone, err := BuildTaskTombstone(target, "30617:owner:repo", author, time.Unix(2, 0))
	if err != nil {
		t.Fatal(err)
	}
	tombstone.PubKey = pk
	if id, err := ValidateTaskTombstone(tombstone, author); err != nil || id != "task-1" {
		t.Fatalf("valid tombstone id=%q err=%v", id, err)
	}
	tombstone.PubKey[31] = 2
	if _, err := ValidateTaskTombstone(tombstone, author); err == nil {
		t.Fatal("expected attacker-authored tombstone rejection")
	}
}

func TestQueueCollectionPersistsCanonicalReservation(t *testing.T) {
	author := fmt.Sprintf("%064x", 1)
	expires := time.Unix(100, 123).UTC()
	ev := BuildQueueCollectionEventWithReservations(
		"30617:owner:repo",
		"backlog",
		[]string{"task-1"},
		[]QueueReservation{{TaskID: "task-1", Worker: "worker-a", LeaseID: "command-event", ExpiresAt: expires}},
		author,
		time.Unix(10, 0),
	)
	if !hasExactTag(ev, "a", Address(KindCanonicalState, author, "task:task-1")) {
		t.Fatalf("queue omitted canonical task coordinate: %#v", ev.Tags)
	}
	foundLease := false
	for _, tag := range ev.Tags {
		if len(tag) == 5 && tag[0] == "lease" && tag[1] == "task-1" && tag[2] == "worker-a" && tag[3] == expires.Format(time.RFC3339Nano) && tag[4] == "command-event" {
			foundLease = true
		}
	}
	if !foundLease {
		t.Fatalf("queue omitted durable reservation: %#v", ev.Tags)
	}
}

func TestBuildContextVMCommand(t *testing.T) {
	ev, err := BuildClaimDispatch("task-1", "agent-a", "recipient-pubkey", time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if ev.Kind != KindContextVMIntent {
		t.Fatalf("kind=%d", ev.Kind)
	}
	if method, _ := TagFirst(ev, "method"); method != "task/claim" {
		t.Fatalf("method=%q", method)
	}

	ev, err = BuildAssignCommand("task-1", "agent-b", "recipient-pubkey", time.Unix(2, 0))
	if err != nil {
		t.Fatal(err)
	}
	if ev.Kind != KindContextVMIntent {
		t.Fatalf("assign kind=%d", ev.Kind)
	}
	if method, _ := TagFirst(ev, "method"); method != "task/assign" {
		t.Fatalf("assign method=%q", method)
	}
}
