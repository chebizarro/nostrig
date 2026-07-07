package nostr

import (
	"testing"
	"time"

	beadspb "github.com/chebizarro/nostrig/gen/beads"
	gonostr "github.com/nbd-wtf/go-nostr"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTaskStateRoundTripLossless(t *testing.T) {
	now := time.Unix(1234, 0).UTC()
	issue := &beadspb.Issue{Id: "repo-abc12345", Title: "Implement fabric", Description: "full model", Status: beadspb.Status_STATUS_IN_PROGRESS, Priority: beadspb.Priority_PRIORITY_P1, Epic: "repo-epic1234", Assignee: "agent-a", Labels: []string{"issue", "fabric"}, DependsOn: []string{"repo-dep00001"}, Created: timestamppb.New(now), Updated: timestamppb.New(now.Add(time.Minute)), Metadata: &beadspb.Metadata{Custom: map[string]string{"nostr.id": "root-event", "nip34.repo_addr": "30617:pub:repo", "nostrig.beads_id": "repo-abc12345", "nostrig.id_format": "spec"}}}
	ev, err := BuildTaskStateEvent(issue, now)
	if err != nil {
		t.Fatal(err)
	}
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
	events, err := BuildCanonicalEvents(export, now)
	if err != nil {
		t.Fatal(err)
	}
	var state *gonostr.Event
	var epic *gonostr.Event
	for _, ev := range events {
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
	if !hasTag(epic, "a", "30900::task:repo-abc12345") {
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
