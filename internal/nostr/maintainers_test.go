package nostr

import (
	"testing"

	gonostr "fiatjaf.com/nostr"
)

func TestTrustedMaintainersIncludesOwnerAndAllTagValues(t *testing.T) {
	owner, maintainer := gonostr.Generate(), gonostr.Generate()
	event := &gonostr.Event{
		Kind: KindRepositoryAnnouncement,
		Tags: gonostr.Tags{
			{"d", "repo"},
			{"maintainers", maintainer.Public().Hex(), "invalid", maintainer.Public().Hex()},
		},
	}
	if err := event.Sign(owner); err != nil {
		t.Fatal(err)
	}
	repo, err := ParseRepoAnnouncement(event)
	if err != nil {
		t.Fatal(err)
	}
	trusted, err := TrustedMaintainers(repo)
	if err != nil {
		t.Fatal(err)
	}
	if len(trusted) != 2 || !IsTrustedMaintainer(repo, owner.Public().Hex()) || !IsTrustedMaintainer(repo, maintainer.Public().Hex()) {
		t.Fatalf("trusted maintainers = %#v", trusted)
	}
}

func TestResolveStatusesRejectsNonMaintainerStatus(t *testing.T) {
	maintainer, attacker := gonostr.Generate(), gonostr.Generate()
	rootID := testID(42).Hex()
	trusted := signedStatus(t, maintainer, KindStatusOpen, 10, rootID)
	untrusted := signedStatus(t, attacker, KindStatusClosed, 20, rootID)

	resolution := ResolveStatuses(
		[]*gonostr.Event{trusted, untrusted},
		[]string{rootID},
		[]string{maintainer.Public().Hex()},
	)
	if got := resolution.Trusted[rootID]; got == nil || got.EventID != trusted.ID.Hex() || got.Kind != KindStatusOpen {
		t.Fatalf("trusted winner = %#v, want open event %s", got, trusted.ID.Hex())
	}
	if len(resolution.Untrusted[rootID]) != 1 || resolution.Untrusted[rootID][0].EventID != untrusted.ID.Hex() {
		t.Fatalf("untrusted statuses = %#v", resolution.Untrusted[rootID])
	}
}

func TestResolveStatusesUsesEventIDTieBreakAndRejectsTampering(t *testing.T) {
	maintainer := gonostr.Generate()
	rootID := testID(43).Hex()
	open := signedStatus(t, maintainer, KindStatusOpen, 10, rootID)
	closed := signedStatus(t, maintainer, KindStatusClosed, 10, rootID)
	want := open
	if closed.ID.Hex() > open.ID.Hex() {
		want = closed
	}
	tampered := *signedStatus(t, maintainer, KindStatusDraft, 11, rootID)
	tampered.Content = "tampered"

	for _, events := range [][]*gonostr.Event{{open, closed, &tampered}, {closed, open, &tampered}} {
		resolution := ResolveStatuses(events, []string{rootID}, []string{maintainer.Public().Hex()})
		if got := resolution.Trusted[rootID]; got == nil || got.EventID != want.ID.Hex() {
			t.Fatalf("winner = %#v, want %s", got, want.ID.Hex())
		}
		if len(resolution.Untrusted[rootID]) != 1 {
			t.Fatalf("tampered event was not rejected: %#v", resolution.Untrusted[rootID])
		}
	}
}

func signedStatus(t *testing.T, key gonostr.SecretKey, kind int, createdAt int64, rootID string) *gonostr.Event {
	t.Helper()
	event := &gonostr.Event{
		Kind:      gonostr.Kind(kind),
		CreatedAt: gonostr.Timestamp(createdAt),
		Tags:      gonostr.Tags{{"e", rootID, "", "root"}},
	}
	if err := event.Sign(key); err != nil {
		t.Fatal(err)
	}
	return event
}
