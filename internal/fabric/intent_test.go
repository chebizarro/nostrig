package fabric

import (
	"encoding/json"
	"testing"

	beadspb "github.com/chebizarro/nostrig/gen/beads"
	gonostr "github.com/nbd-wtf/go-nostr"
)

func signedIntent(t *testing.T, secret, recipient, method string, params map[string]any) *gonostr.Event {
	t.Helper()
	pub, _ := gonostr.GetPublicKey(secret)
	content, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": method + "-1", "method": method, "params": params})
	ev := &gonostr.Event{PubKey: pub, CreatedAt: 100, Kind: 25910,
		Tags: gonostr.Tags{{"p", recipient}, {"method", method}, {"schema", "cascadia.task.v1"}}, Content: string(content)}
	if err := ev.Sign(secret); err != nil {
		t.Fatal(err)
	}
	return ev
}

func TestApplyIntentClaimAssignUpdateClose(t *testing.T) {
	recipientSecret := gonostr.GeneratePrivateKey()
	recipient, _ := gonostr.GetPublicKey(recipientSecret)
	actor := gonostr.GeneratePrivateKey()
	actorPub, _ := gonostr.GetPublicKey(actor)
	ledger := &beadspb.Export{Issues: []*beadspb.Issue{{Id: "fp-50", Status: beadspb.Status_STATUS_OPEN}}}

	var err error
	ledger, _, err = ApplyIntent(ledger, signedIntent(t, actor, recipient, "task/claim", map[string]any{"id": "fp-50"}), recipient)
	if err != nil || ledger.Issues[0].Assignee != actorPub || ledger.Issues[0].Status != beadspb.Status_STATUS_IN_PROGRESS {
		t.Fatalf("claim failed: issue=%v err=%v", ledger.Issues[0], err)
	}
	ledger, _, err = ApplyIntent(ledger, signedIntent(t, actor, recipient, "task/assign", map[string]any{"id": "fp-50", "assignee": "gus"}), recipient)
	if err != nil || ledger.Issues[0].Assignee != "gus" {
		t.Fatalf("assign failed: %v %v", ledger.Issues[0], err)
	}
	ledger, _, err = ApplyIntent(ledger, signedIntent(t, actor, recipient, "task/update", map[string]any{
		"id": "fp-50", "title": "bidirectional", "status": "blocked", "priority": "P0", "labels": []string{"fabric"}, "depends_on": []string{"fp-2"},
	}), recipient)
	if err != nil || ledger.Issues[0].Title != "bidirectional" || ledger.Issues[0].Status != beadspb.Status_STATUS_BLOCKED || ledger.Issues[0].Priority != beadspb.Priority_PRIORITY_P0 {
		t.Fatalf("update failed: %v %v", ledger.Issues[0], err)
	}
	ledger, _, err = ApplyIntent(ledger, signedIntent(t, actor, recipient, "task/close", map[string]any{"id": "fp-50"}), recipient)
	if err != nil || ledger.Issues[0].Status != beadspb.Status_STATUS_CLOSED {
		t.Fatalf("close failed: %v %v", ledger.Issues[0], err)
	}
}

func TestApplyIntentRejectsWrongRecipientTamperAndConflict(t *testing.T) {
	secret := gonostr.GeneratePrivateKey()
	recipient, _ := gonostr.GetPublicKey(gonostr.GeneratePrivateKey())
	ledger := &beadspb.Export{Issues: []*beadspb.Issue{{Id: "fp-50", Assignee: "someone-else"}}}
	ev := signedIntent(t, secret, recipient, "task/claim", map[string]any{"id": "fp-50"})
	if _, _, err := ApplyIntent(ledger, ev, "wrong"); err == nil {
		t.Fatal("expected recipient rejection")
	}
	if _, _, err := ApplyIntent(ledger, ev, recipient); err == nil {
		t.Fatal("expected claim conflict")
	}
	ev.Content += " "
	if _, _, err := ApplyIntent(ledger, ev, recipient); err == nil {
		t.Fatal("expected signature rejection")
	}
}
