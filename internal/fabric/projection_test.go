package fabric

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	beadspb "github.com/chebizarro/nostrig/gen/beads"
	fn "github.com/chebizarro/nostrig/internal/nostr"
	gonostr "github.com/nbd-wtf/go-nostr"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestAllBeadsStatusPriorityAndRelationshipIndexes(t *testing.T) {
	key := gonostr.GeneratePrivateKey()
	pubkey, err := gonostr.GetPublicKey(key)
	if err != nil {
		t.Fatal(err)
	}
	statuses := []beadspb.Status{
		beadspb.Status_STATUS_UNSPECIFIED,
		beadspb.Status_STATUS_OPEN,
		beadspb.Status_STATUS_IN_PROGRESS,
		beadspb.Status_STATUS_BLOCKED,
		beadspb.Status_STATUS_CLOSED,
	}
	priorities := []beadspb.Priority{
		beadspb.Priority_PRIORITY_UNSPECIFIED,
		beadspb.Priority_PRIORITY_P0,
		beadspb.Priority_PRIORITY_P1,
		beadspb.Priority_PRIORITY_P2,
		beadspb.Priority_PRIORITY_P3,
		beadspb.Priority_PRIORITY_P4,
	}
	now := timestamppb.New(time.Unix(1_700_000_000, 0))
	export := &beadspb.Export{Epics: []*beadspb.Epic{{
		Id: "fp-parent", Name: "Parent", Status: beadspb.Status_STATUS_OPEN, Created: now, Updated: now,
	}}}
	for i, status := range statuses {
		export.Issues = append(export.Issues, &beadspb.Issue{
			Id: fmt.Sprintf("fp-%d", i), Title: status.String(), Description: "closure semantics",
			Status: status, Priority: priorities[i%len(priorities)], Epic: "fp-parent", Assignee: "Strand",
			Labels: []string{"task", "label-a", "label-b"}, DependsOn: []string{"fp-dep-a", "fp-dep-b"},
			Created: now, Updated: now, Metadata: &beadspb.Metadata{Custom: map[string]string{"close_reason": "verified"}},
		})
	}
	events, err := Encode(export, pubkey, time.Unix(1_700_000_001, 0))
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range events {
		if err := ev.Sign(key); err != nil {
			t.Fatal(err)
		}
		if err := ValidateSignedEvent(ev, pubkey); err != nil {
			t.Fatalf("%s: %v", eventD(ev), err)
		}
	}
	projection, err := ProjectVerified(events, pubkey)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(export, projection.Export) {
		t.Fatalf("mapping changed beads fields\nwant: %v\n got: %v", export, projection.Export)
	}
}

func TestReducerConflictIdempotencyLoopGuardAndRestart(t *testing.T) {
	key := gonostr.GeneratePrivateKey()
	pubkey, _ := gonostr.GetPublicKey(key)
	at := time.Unix(1_700_000_010, 0)
	makeEvent := func(title string) *gonostr.Event {
		events, err := Encode(&beadspb.Export{Issues: []*beadspb.Issue{{Id: "fp-conflict", Title: title, Status: beadspb.Status_STATUS_OPEN}}}, pubkey, at)
		if err != nil {
			t.Fatal(err)
		}
		if err := events[0].Sign(key); err != nil {
			t.Fatal(err)
		}
		return events[0]
	}
	a := makeEvent("alpha")
	b := makeEvent("beta")
	winner := a
	loser := b
	if b.ID < a.ID {
		winner, loser = b, a
	}

	r, err := NewReducer(pubkey)
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := r.Apply(loser); err != nil || !changed {
		t.Fatalf("apply loser first: changed=%v err=%v", changed, err)
	}
	if changed, err := r.Apply(winner); err != nil || !changed {
		t.Fatalf("lower-id tie winner: changed=%v err=%v", changed, err)
	}
	if changed, err := r.Apply(winner); err != nil || changed {
		t.Fatalf("replay must be idempotent: changed=%v err=%v", changed, err)
	}

	local, err := Encode(&beadspb.Export{Issues: []*beadspb.Issue{{Id: "fp-conflict", Title: taskTitle(winner), Status: beadspb.Status_STATUS_OPEN}}}, pubkey, at.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if needed, err := r.NeedsPublish(local[0]); err != nil || needed {
		t.Fatalf("unchanged relay echo must not republish: needed=%v err=%v", needed, err)
	}
	local[0].Content = strings.Replace(local[0].Content, taskTitle(winner), "material change", 1)
	if needed, err := r.NeedsPublish(local[0]); err != nil || !needed {
		t.Fatalf("material local change must publish: needed=%v err=%v", needed, err)
	}

	restarted, _ := NewReducer(pubkey)
	for _, ev := range []*gonostr.Event{winner, loser, winner} {
		if _, err := restarted.Apply(ev); err != nil {
			t.Fatal(err)
		}
	}
	want, _ := r.Snapshot()
	got, _ := restarted.Snapshot()
	if !proto.Equal(want.Export, got.Export) {
		t.Fatalf("restart catch-up diverged: want=%v got=%v", want.Export, got.Export)
	}
}

func TestDeletionTombstoneAndNewerRecreation(t *testing.T) {
	key := gonostr.GeneratePrivateKey()
	pubkey, _ := gonostr.GetPublicKey(key)
	encodeSigned := func(title string, at time.Time) *gonostr.Event {
		events, err := Encode(&beadspb.Export{Issues: []*beadspb.Issue{{Id: "fp-delete", Title: title}}}, pubkey, at)
		if err != nil {
			t.Fatal(err)
		}
		if err := events[0].Sign(key); err != nil {
			t.Fatal(err)
		}
		return events[0]
	}
	old := encodeSigned("old", time.Unix(100, 0))
	coord, _, _ := stateCoordinate(old)
	deletion, err := EncodeDeletion([]string{coord}, pubkey, time.Unix(101, 0), "acceptance cleanup")
	if err != nil {
		t.Fatal(err)
	}
	if err := deletion.Sign(key); err != nil {
		t.Fatal(err)
	}
	newer := encodeSigned("recreated", time.Unix(102, 0))

	r, _ := NewReducer(pubkey)
	if changed, err := r.Apply(old); err != nil || !changed {
		t.Fatalf("old: changed=%v err=%v", changed, err)
	}
	if changed, err := r.Apply(deletion); err != nil || !changed {
		t.Fatalf("delete: changed=%v err=%v", changed, err)
	}
	snapshot, err := r.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Export.Issues) != 0 || len(snapshot.Tombstones) != 1 {
		t.Fatalf("deletion projection: %+v", snapshot)
	}
	if changed, err := r.Apply(old); err != nil || changed {
		t.Fatalf("deleted replay must stay hidden: changed=%v err=%v", changed, err)
	}
	if changed, err := r.Apply(newer); err != nil || !changed {
		t.Fatalf("newer recreation: changed=%v err=%v", changed, err)
	}
	snapshot, _ = r.Snapshot()
	if len(snapshot.Export.Issues) != 1 || snapshot.Export.Issues[0].Title != "recreated" {
		t.Fatalf("newer state did not recreate task: %+v", snapshot.Export)
	}
}

func TestValidationRejectsMalformedEvents(t *testing.T) {
	key := gonostr.GeneratePrivateKey()
	pubkey, _ := gonostr.GetPublicKey(key)
	baseEvents, err := Encode(&beadspb.Export{Issues: []*beadspb.Issue{{Id: "fp-bad", DependsOn: []string{"fp-dep"}}}}, pubkey, time.Unix(200, 0))
	if err != nil {
		t.Fatal(err)
	}
	base := baseEvents[0]

	tests := map[string]func(*gonostr.Event){
		"address mismatch": func(ev *gonostr.Event) { ev.Tags[0][1] = "task:other" },
		"duplicate d":      func(ev *gonostr.Event) { ev.Tags = append(ev.Tags, gonostr.Tag{"d", "task:fp-bad"}) },
		"missing schema": func(ev *gonostr.Event) {
			ev.Tags = append(gonostr.Tags(nil), ev.Tags[:2]...)
		},
		"dependency conflict": func(ev *gonostr.Event) {
			for _, tag := range ev.Tags {
				if len(tag) > 2 && tag[2] == "depends_on" {
					tag[1] = fmt.Sprintf("%d:%s:task:wrong", fn.KindTaskState, pubkey)
				}
			}
		},
		"malformed content": func(ev *gonostr.Event) { ev.Content = "{" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			ev := cloneEvent(base)
			mutate(ev)
			if err := ev.Sign(key); err != nil {
				t.Fatal(err)
			}
			if err := ValidateSignedEvent(ev, pubkey); err == nil {
				t.Fatal("expected validation failure")
			}
		})
	}

	valid := cloneEvent(base)
	if err := valid.Sign(key); err != nil {
		t.Fatal(err)
	}
	valid.Content += " "
	if err := ValidateSignedEvent(valid, pubkey); err == nil || !strings.Contains(err.Error(), "id") {
		t.Fatalf("expected id mismatch, got %v", err)
	}
	valid = cloneEvent(base)
	if err := valid.Sign(key); err != nil {
		t.Fatal(err)
	}
	valid.Sig = strings.Repeat("0", len(valid.Sig))
	if err := ValidateSignedEvent(valid, pubkey); err == nil || !strings.Contains(err.Error(), "signature") {
		t.Fatalf("expected signature failure, got %v", err)
	}

	otherKey := gonostr.GeneratePrivateKey()
	otherPubkey, _ := gonostr.GetPublicKey(otherKey)
	coord := fmt.Sprintf("%d:%s:task:fp-bad", fn.KindTaskState, pubkey)
	deletion, err := EncodeDeletion([]string{strings.Replace(coord, pubkey, otherPubkey, 1)}, otherPubkey, time.Unix(201, 0), "wrong author")
	if err != nil {
		t.Fatal(err)
	}
	if err := deletion.Sign(otherKey); err != nil {
		t.Fatal(err)
	}
	if err := ValidateSignedEvent(deletion, pubkey); err == nil {
		t.Fatal("expected deletion author rejection")
	}
}

type testSigner struct {
	key    string
	mutate func(*gonostr.Event)
}

func (s testSigner) PublicKey(context.Context) (string, error) { return gonostr.GetPublicKey(s.key) }
func (s testSigner) SignEvent(_ context.Context, unsigned *gonostr.Event) (*gonostr.Event, error) {
	ev := cloneEvent(unsigned)
	if s.mutate != nil {
		s.mutate(ev)
	}
	return ev, ev.Sign(s.key)
}

type countingRelay struct{ count int }

func (r *countingRelay) Publish(context.Context, gonostr.Event) error { r.count++; return nil }

func TestPublisherValidatesSignetOutputBeforeRelay(t *testing.T) {
	key := gonostr.GeneratePrivateKey()
	pubkey, _ := gonostr.GetPublicKey(key)
	events, err := Encode(&beadspb.Export{Issues: []*beadspb.Issue{{Id: "fp-publish"}}}, pubkey, time.Unix(300, 0))
	if err != nil {
		t.Fatal(err)
	}
	relay := &countingRelay{}
	publisher := &Publisher{Signer: testSigner{key: key}, Relays: []Relay{relay}}
	if _, err := publisher.Publish(context.Background(), events); err != nil {
		t.Fatal(err)
	}
	if relay.count != 1 {
		t.Fatalf("relay publish count=%d", relay.count)
	}

	relay.count = 0
	publisher.Signer = testSigner{key: key, mutate: func(ev *gonostr.Event) { ev.Content += "mutated" }}
	if _, err := publisher.Publish(context.Background(), events); err == nil {
		t.Fatal("expected Signet mutation rejection")
	}
	if relay.count != 0 {
		t.Fatal("invalid Signet output reached relay")
	}
}

func TestLiveRelayWS0WS1Acceptance(t *testing.T) {
	relayURL := os.Getenv("NOSTRIG_LIVE_RELAY")
	if relayURL == "" {
		t.Skip("set NOSTRIG_LIVE_RELAY for reversible live acceptance")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	relay, err := gonostr.RelayConnect(ctx, relayURL)
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()

	key := gonostr.GeneratePrivateKey() // ephemeral acceptance identity; never persisted
	pubkey, _ := gonostr.GetPublicKey(key)
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	taskID := "fp-50-acceptance-" + suffix
	epicID := "fp-50-epic-" + suffix
	base := time.Now().UTC().Add(-10 * time.Second).Truncate(time.Second)
	ws0 := &beadspb.Export{
		Epics: []*beadspb.Epic{{Id: epicID, Name: "fp-50 reversible acceptance", Status: beadspb.Status_STATUS_OPEN}},
		Issues: []*beadspb.Issue{{
			Id: taskID, Title: "WS0 created", Description: "reversible live task-fabric acceptance",
			Status: beadspb.Status_STATUS_OPEN, Priority: beadspb.Priority_PRIORITY_P1, Epic: epicID,
			Assignee: pubkey, Labels: []string{"fp-50", "acceptance"}, DependsOn: []string{"fp-2", "fp-4"},
		}},
	}
	ws0Events, err := Encode(ws0, pubkey, base)
	if err != nil {
		t.Fatal(err)
	}
	signAll(t, key, ws0Events)
	for _, ev := range ws0Events {
		if err := relay.Publish(ctx, *ev); err != nil {
			t.Fatalf("WS0 publish %s: %v", eventD(ev), err)
		}
	}

	ws1Read := queryAcceptance(t, ctx, relay, pubkey, taskID, epicID)
	ws1Projection, err := ProjectVerified(ws1Read, pubkey)
	if err != nil {
		t.Fatal(err)
	}
	if len(ws1Projection.Export.Issues) != 1 || ws1Projection.Export.Issues[0].Title != "WS0 created" {
		t.Fatalf("WS1 did not receive WS0 state: %v", ws1Projection.Export)
	}
	ws1Reducer, _ := NewReducer(pubkey)
	for _, ev := range ws1Read {
		if _, err := ws1Reducer.Apply(ev); err != nil {
			t.Fatal(err)
		}
	}
	if changed, err := ws1Reducer.Apply(ws0Events[0]); err != nil || changed {
		t.Fatalf("WS1 relay echo was not idempotent: changed=%v err=%v", changed, err)
	}

	ws1 := proto.Clone(ws0).(*beadspb.Export)
	ws1.Issues[0].Title = "WS1 closed"
	ws1.Issues[0].Status = beadspb.Status_STATUS_CLOSED
	ws1.Issues[0].Metadata = &beadspb.Metadata{Custom: map[string]string{"close_reason": "live acceptance"}}
	ws1Events, err := Encode(ws1, pubkey, base.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	signAll(t, key, ws1Events)
	for _, ev := range ws1Events {
		if err := relay.Publish(ctx, *ev); err != nil {
			t.Fatalf("WS1 publish %s: %v", eventD(ev), err)
		}
	}

	ws0Catchup := queryAcceptance(t, ctx, relay, pubkey, taskID, epicID)
	ws0Restart, _ := NewReducer(pubkey)
	for _, ev := range ws0Catchup {
		if _, err := ws0Restart.Apply(ev); err != nil {
			t.Fatal(err)
		}
	}
	ws0Projection, err := ws0Restart.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(ws0Projection.Export.Issues) != 1 || ws0Projection.Export.Issues[0].Status != beadspb.Status_STATUS_CLOSED {
		t.Fatalf("WS0 restart/catch-up did not receive WS1 closure: %v", ws0Projection.Export)
	}
	if needed, err := ws0Restart.NeedsPublish(ws1Events[0]); err != nil || needed {
		t.Fatalf("WS0 loop guard failed: needed=%v err=%v", needed, err)
	}

	coordinates := make([]string, 0, len(ws1Events))
	for _, ev := range ws1Events {
		coord, recognized, err := stateCoordinate(ev)
		if err != nil || !recognized {
			t.Fatalf("cleanup coordinate: recognized=%v err=%v", recognized, err)
		}
		coordinates = append(coordinates, coord)
	}
	deletion, err := EncodeDeletion(coordinates, pubkey, base.Add(2*time.Second), "fp-50 reversible acceptance cleanup")
	if err != nil {
		t.Fatal(err)
	}
	if err := deletion.Sign(key); err != nil {
		t.Fatal(err)
	}
	if err := relay.Publish(ctx, *deletion); err != nil {
		t.Fatalf("cleanup deletion: %v", err)
	}
	deleteRead, err := relay.QuerySync(ctx, gonostr.Filter{IDs: []string{deletion.ID}, Authors: []string{pubkey}, Kinds: []int{kindDeletion}})
	if err != nil {
		t.Fatal(err)
	}
	if len(deleteRead) != 1 {
		t.Fatalf("cleanup deletion not readable: %d events", len(deleteRead))
	}
	postDeleteState := queryAcceptance(t, ctx, relay, pubkey, taskID, epicID)
	for _, ev := range deleteRead {
		if _, err := ws0Restart.Apply(ev); err != nil {
			t.Fatal(err)
		}
	}
	clean, _ := ws0Restart.Snapshot()
	if len(clean.Export.Issues) != 0 || len(clean.Export.Epics) != 0 || len(clean.Tombstones) != 2 {
		t.Fatalf("cleanup projection incomplete: %+v", clean)
	}

	t.Logf("RELAY=%s PUBKEY=%s TASK_ID=%s EPIC_ID=%s", relayURL, pubkey, taskID, epicID)
	t.Logf("WS0_TASK_EVENT=%s WS0_EPIC_EVENT=%s", ws0Events[0].ID, ws0Events[1].ID)
	t.Logf("WS1_TASK_EVENT=%s WS1_EPIC_EVENT=%s", ws1Events[0].ID, ws1Events[1].ID)
	t.Logf("TOMBSTONE_EVENT=%s POST_DELETE_STATE_EVENTS=%d", deletion.ID, len(postDeleteState))
}

func signAll(t *testing.T, key string, events []*gonostr.Event) {
	t.Helper()
	for _, ev := range events {
		if err := ev.Sign(key); err != nil {
			t.Fatal(err)
		}
	}
}

func queryAcceptance(t *testing.T, ctx context.Context, relay *gonostr.Relay, pubkey, taskID, epicID string) []*gonostr.Event {
	t.Helper()
	taskEvents, err := relay.QuerySync(ctx, gonostr.Filter{
		Authors: []string{pubkey}, Kinds: []int{fn.KindTaskState}, Tags: gonostr.TagMap{"d": {"task:" + taskID}},
	})
	if err != nil {
		t.Fatal(err)
	}
	epicEvents, err := relay.QuerySync(ctx, gonostr.Filter{
		Authors: []string{pubkey}, Kinds: []int{fn.KindNIP51Set}, Tags: gonostr.TagMap{"d": {"epic:" + epicID}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return append(taskEvents, epicEvents...)
}

func taskTitle(ev *gonostr.Event) string {
	issue, _, _, err := decodeStateEvent(ev)
	if err != nil || issue == nil {
		return ""
	}
	return issue.Title
}
