package nostr

import (
	"testing"
	"time"

	gonostr "fiatjaf.com/nostr"
)

func testID(n byte) gonostr.ID {
	var id gonostr.ID
	id[31] = n
	return id
}

func testPubKey(n byte) gonostr.PubKey {
	var pk gonostr.PubKey
	pk[31] = n
	return pk
}

func TestTagHelpers(t *testing.T) {
	ev := &gonostr.Event{
		Kind:      KindRepositoryAnnouncement,
		CreatedAt: 100,
		Tags: gonostr.Tags{
			{"d", "my-repo"},
			{"t", "root"},
			{"t", "bug"},
			{"clone", "https://example.com/repo.git"},
			{"clone", "ssh://example.com/repo.git"},
			{"refs/heads/main", "abc123"},
		},
	}

	if got, ok := TagFirst(ev, "d"); !ok || got != "my-repo" {
		t.Fatalf("TagFirst(d) = (%q,%v), want (%q,true)", got, ok, "my-repo")
	}

	allT := TagAll(ev, "t")
	if len(allT) != 2 || allT[0] != "root" || allT[1] != "bug" {
		t.Fatalf("TagAll(t) = %#v, want [root bug]", allT)
	}

	allClone := TagAll(ev, "clone")
	if len(allClone) != 2 {
		t.Fatalf("TagAll(clone) len=%d, want 2", len(allClone))
	}

	pref := TagsWithNamePrefix(ev, "refs/")
	if len(pref) != 1 || len(pref[0]) < 2 || pref[0][0] != "refs/heads/main" || pref[0][1] != "abc123" {
		t.Fatalf("TagsWithNamePrefix(refs/) = %#v, want one refs/heads/main tag", pref)
	}

	if got, ok := TagD(ev); !ok || got != "my-repo" {
		t.Fatalf("TagD = (%q,%v), want (%q,true)", got, ok, "my-repo")
	}
}

func TestSanitizeSlug(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"My Repo", "my-repo"},
		{"  Hello, world!  ", "hello-world"},
		{"UPPER_case__and--dashes", "upper-case-and-dashes"},
		{"---already--sluggy---", "already-sluggy"},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got := SanitizeSlug(tt.in)
			if got != tt.want {
				t.Fatalf("SanitizeSlug(%q)=%q, want %q", tt.in, got, tt.want)
			}
		})
	}

	if got := RepoEpicID("My Repo"); got != "repo-my-repo" {
		t.Fatalf("RepoEpicID()=%q, want %q", got, "repo-my-repo")
	}
}

func TestParseRepoAnnouncement(t *testing.T) {
	ev := &gonostr.Event{
		ID:        testID(1),
		PubKey:    testPubKey(1),
		Kind:      KindRepositoryAnnouncement,
		CreatedAt: 123,
		Tags: gonostr.Tags{
			{"d", "my-repo"},
			{"name", "My Repo"},
			{"description", "desc"},
			{"web", "https://example.com"},
			{"clone", "https://example.com/repo.git"},
			{"relays", "wss://relay.example.com"},
			{"maintainers", "pub2"},
			{"t", "go"},
			{"r", "rootcommit", "euc"},
		},
	}

	ra, err := ParseRepoAnnouncement(ev)
	if err != nil {
		t.Fatalf("ParseRepoAnnouncement error: %v", err)
	}

	if ra.RepoID != "my-repo" {
		t.Fatalf("RepoID=%q, want %q", ra.RepoID, "my-repo")
	}
	if ra.Name != "My Repo" {
		t.Fatalf("Name=%q, want %q", ra.Name, "My Repo")
	}
	if ra.Description != "desc" {
		t.Fatalf("Description=%q, want %q", ra.Description, "desc")
	}
	if len(ra.Web) != 1 || ra.Web[0] != "https://example.com" {
		t.Fatalf("Web=%#v, want one entry", ra.Web)
	}
	if len(ra.Clone) != 1 || ra.Clone[0] != "https://example.com/repo.git" {
		t.Fatalf("Clone=%#v, want one entry", ra.Clone)
	}
	if len(ra.Relays) != 1 || ra.Relays[0] != "wss://relay.example.com" {
		t.Fatalf("Relays=%#v, want one entry", ra.Relays)
	}
	if len(ra.Maintainers) != 1 || ra.Maintainers[0] != "pub2" {
		t.Fatalf("Maintainers=%#v, want one entry", ra.Maintainers)
	}
	if len(ra.Topics) != 1 || ra.Topics[0] != "go" {
		t.Fatalf("Topics=%#v, want one entry", ra.Topics)
	}
	if ra.EUC != "rootcommit" {
		t.Fatalf("EUC=%q, want %q", ra.EUC, "rootcommit")
	}
	if ra.CreatedAt.IsZero() {
		t.Fatalf("CreatedAt is zero, expected non-zero")
	}
}

func TestParseRepoState(t *testing.T) {
	ev := &gonostr.Event{
		ID:        testID(2),
		PubKey:    testPubKey(1),
		Kind:      KindRepositoryState,
		CreatedAt: 200,
		Tags: gonostr.Tags{
			{"d", "my-repo"},
			{"refs/heads/main", "abc123"},
			{"refs/tags/v1.0.0", "def456"},
			{"HEAD", "ref: refs/heads/main"},
		},
	}

	rs, err := ParseRepoState(ev)
	if err != nil {
		t.Fatalf("ParseRepoState error: %v", err)
	}

	if rs.RepoID != "my-repo" {
		t.Fatalf("RepoID=%q, want %q", rs.RepoID, "my-repo")
	}
	if rs.HEAD != "ref: refs/heads/main" {
		t.Fatalf("HEAD=%q, want %q", rs.HEAD, "ref: refs/heads/main")
	}
	if rs.Refs["refs/heads/main"] != "abc123" {
		t.Fatalf("Refs[refs/heads/main]=%q, want %q", rs.Refs["refs/heads/main"], "abc123")
	}
	if rs.Refs["refs/tags/v1.0.0"] != "def456" {
		t.Fatalf("Refs[refs/tags/v1.0.0]=%q, want %q", rs.Refs["refs/tags/v1.0.0"], "def456")
	}
}

func TestParseRootItemAndStatusEvent(t *testing.T) {
	rootEv := &gonostr.Event{
		ID:        testID(3),
		PubKey:    testPubKey(2),
		Kind:      KindIssue,
		CreatedAt: 300,
		Content:   "Hello world\nMore text",
		Tags: gonostr.Tags{
			{"a", RepoAddress("pub1", "my-repo")},
			{"subject", "Test issue"},
			{"t", "bug"},
		},
	}

	root, err := ParseRootItem(rootEv)
	if err != nil {
		t.Fatalf("ParseRootItem error: %v", err)
	}
	if root.EventID != testID(3).Hex() {
		t.Fatalf("Root.EventID=%q, want %q", root.EventID, testID(3).Hex())
	}
	if root.Subject != "Test issue" {
		t.Fatalf("Root.Subject=%q, want %q", root.Subject, "Test issue")
	}
	if len(root.Labels) != 1 || root.Labels[0] != "bug" {
		t.Fatalf("Root.Labels=%#v, want [bug]", root.Labels)
	}
	if root.CreatedAt.IsZero() {
		t.Fatalf("Root.CreatedAt is zero, expected non-zero")
	}

	statusEv := &gonostr.Event{
		ID:        testID(4),
		PubKey:    testPubKey(3),
		Kind:      KindStatusDraft,
		CreatedAt: 400,
		Content:   "drafting",
		Tags: gonostr.Tags{
			{"e", "root1", "", "root"},
		},
	}

	st, err := ParseStatusEvent(statusEv)
	if err != nil {
		t.Fatalf("ParseStatusEvent error: %v", err)
	}
	if st.RootEventID != "root1" {
		t.Fatalf("Status.RootEventID=%q, want %q", st.RootEventID, "root1")
	}
	if st.Kind != KindStatusDraft {
		t.Fatalf("Status.Kind=%d, want %d", st.Kind, KindStatusDraft)
	}

	// quick sanity: EventTime should match created_at
	gotTime := EventTime(rootEv)
	wantTime := time.Unix(int64(rootEv.CreatedAt), 0).UTC()
	if !gotTime.Equal(wantTime) {
		t.Fatalf("EventTime=%v, want %v", gotTime, wantTime)
	}
}
