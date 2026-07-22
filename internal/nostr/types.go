package nostr

import (
	"fmt"
	"strings"
	"time"
	"unicode"

	gonostr "fiatjaf.com/nostr"
)

// TagFirst returns the first value for tags with the given name.
func TagFirst(ev *gonostr.Event, name string) (string, bool) {
	if ev == nil {
		return "", false
	}
	for _, t := range ev.Tags {
		if len(t) >= 2 && t[0] == name {
			return t[1], true
		}
	}
	return "", false
}

// TagAll returns all values for tags with the given name.
func TagAll(ev *gonostr.Event, name string) []string {
	if ev == nil {
		return nil
	}
	out := make([]string, 0)
	for _, t := range ev.Tags {
		if len(t) >= 2 && t[0] == name {
			out = append(out, t[1])
		}
	}
	return out
}

// tagValues returns every value after the tag name. NIP-34 repository
// announcements permit maintainers to be expressed in repeated or multi-value
// tags, unlike indexable one-value tags.
func tagValues(ev *gonostr.Event, name string) []string {
	if ev == nil {
		return nil
	}
	var out []string
	for _, tag := range ev.Tags {
		if len(tag) >= 2 && tag[0] == name {
			out = append(out, tag[1:]...)
		}
	}
	return out
}

// TagsWithNamePrefix returns all tags whose name begins with prefix (e.g. "refs/").
func TagsWithNamePrefix(ev *gonostr.Event, prefix string) gonostr.Tags {
	if ev == nil {
		return nil
	}
	out := make(gonostr.Tags, 0)
	for _, t := range ev.Tags {
		if len(t) >= 1 && strings.HasPrefix(t[0], prefix) {
			out = append(out, t)
		}
	}
	return out
}

// TagD returns the first "d" tag value.
func TagD(ev *gonostr.Event) (string, bool) {
	return TagFirst(ev, "d")
}

// Address formats a Nostr "a" address value: "<kind>:<pubkey>:<d>".
func Address(kind int, pubkey, d string) string {
	return fmt.Sprintf("%d:%s:%s", kind, pubkey, d)
}

// RepoAddress returns the repository announcement address (30617:<pubkey>:<repoID>).
func RepoAddress(repoOwnerPubKey, repoID string) string {
	return Address(KindRepositoryAnnouncement, repoOwnerPubKey, repoID)
}

// SanitizeSlug lowercases s and replaces non [a-z0-9-] characters with '-'.
// It also collapses repeated '-' and trims leading/trailing '-'.
func SanitizeSlug(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))

	var b strings.Builder
	b.Grow(len(s))

	lastWasDash := false
	for _, r := range s {
		isAllowed := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-'
		if isAllowed {
			if r == '-' {
				if lastWasDash {
					continue
				}
				lastWasDash = true
			} else {
				lastWasDash = false
			}
			b.WriteRune(r)
			continue
		}

		// Treat all other characters (including unicode letters) as separators.
		if unicode.IsSpace(r) || unicode.IsPunct(r) || unicode.IsSymbol(r) || !isAllowed {
			if lastWasDash {
				continue
			}
			lastWasDash = true
			b.WriteRune('-')
			continue
		}
	}

	out := b.String()
	out = strings.Trim(out, "-")
	for strings.Contains(out, "--") {
		out = strings.ReplaceAll(out, "--", "-")
	}
	return out
}

// RepoEpicID returns the beads epic ID for the repository: "repo-<d-tag>", sanitized/lowercased.
func RepoEpicID(repoID string) string {
	slug := SanitizeSlug(repoID)
	if slug == "" {
		slug = "unknown"
	}
	return "repo-" + slug
}

// EventTime converts a go-nostr event CreatedAt to time.Time in UTC.
func EventTime(ev *gonostr.Event) time.Time {
	if ev == nil {
		return time.Time{}
	}
	return time.Unix(int64(ev.CreatedAt), 0).UTC()
}

// RepoAnnouncement is a parsed view of a NIP-34 repository announcement (kind 30617).
type RepoAnnouncement struct {
	EventID     string
	PubKey      string
	RepoID      string
	Name        string
	Description string
	Web         []string
	Clone       []string
	Relays      []string
	Maintainers []string
	Topics      []string
	EUC         string
	CreatedAt   time.Time
	Raw         *gonostr.Event
}

// ParseRepoAnnouncement parses a kind:30617 event into a RepoAnnouncement.
func ParseRepoAnnouncement(ev *gonostr.Event) (*RepoAnnouncement, error) {
	if ev == nil {
		return nil, fmt.Errorf("event is nil")
	}
	if ev.Kind != KindRepositoryAnnouncement {
		return nil, fmt.Errorf("unexpected kind %d (expected %d)", ev.Kind, KindRepositoryAnnouncement)
	}

	repoID, ok := TagD(ev)
	if !ok || strings.TrimSpace(repoID) == "" {
		return nil, fmt.Errorf("repository announcement missing required d tag")
	}

	name, _ := TagFirst(ev, "name")
	desc, _ := TagFirst(ev, "description")

	ra := &RepoAnnouncement{
		EventID:     ev.ID.Hex(),
		PubKey:      ev.PubKey.Hex(),
		RepoID:      repoID,
		Name:        name,
		Description: desc,
		Web:         TagAll(ev, "web"),
		Clone:       TagAll(ev, "clone"),
		Relays:      TagAll(ev, "relays"),
		Maintainers: tagValues(ev, "maintainers"),
		Topics:      TagAll(ev, "t"),
		CreatedAt:   EventTime(ev),
		Raw:         ev,
	}

	// Extract earliest unique commit id: ["r", "<commit-id>", "euc"]
	for _, t := range ev.Tags {
		if len(t) >= 3 && t[0] == "r" && t[2] == "euc" {
			ra.EUC = t[1]
			break
		}
	}

	return ra, nil
}

// RepoState is a parsed view of a NIP-34 repository state announcement (kind 30618).
type RepoState struct {
	EventID   string
	PubKey    string
	RepoID    string
	Refs      map[string]string // key: "refs/heads/main" or "refs/tags/v1.0.0" => commit id
	HEAD      string            // e.g. "ref: refs/heads/main"
	CreatedAt time.Time
	Raw       *gonostr.Event
}

// ParseRepoState parses a kind:30618 event into a RepoState.
func ParseRepoState(ev *gonostr.Event) (*RepoState, error) {
	if ev == nil {
		return nil, fmt.Errorf("event is nil")
	}
	if ev.Kind != KindRepositoryState {
		return nil, fmt.Errorf("unexpected kind %d (expected %d)", ev.Kind, KindRepositoryState)
	}

	repoID, ok := TagD(ev)
	if !ok || strings.TrimSpace(repoID) == "" {
		return nil, fmt.Errorf("repository state missing required d tag")
	}

	rs := &RepoState{
		EventID:   ev.ID.Hex(),
		PubKey:    ev.PubKey.Hex(),
		RepoID:    repoID,
		Refs:      make(map[string]string),
		CreatedAt: EventTime(ev),
		Raw:       ev,
	}

	for _, t := range ev.Tags {
		if len(t) >= 2 && t[0] == "HEAD" {
			rs.HEAD = t[1]
			continue
		}
		if len(t) >= 2 && strings.HasPrefix(t[0], "refs/") {
			rs.Refs[t[0]] = t[1]
			continue
		}
	}

	return rs, nil
}

// RootItem is a parsed view over NIP-34 root items (issues, PRs, patches).
// It is intentionally permissive: it can represent kind 1617/1618/1621.
type RootItem struct {
	EventID   string
	PubKey    string
	Kind      int
	RepoAddr  string // value of the "a" tag if present
	Subject   string // subject tag if present
	Content   string
	Labels    []string // all "t" tags
	CreatedAt time.Time
	Raw       *gonostr.Event

	// PR-specific (kind 1618) and PR-update related fields
	Commit     string   // tag "c"
	Clone      []string // tag "clone"
	MergeBase  string   // tag "merge-base"
	BranchName string   // tag "branch-name"

	// Patch-specific (kind 1617) fields
	CommitID     string // tag "commit"
	ParentCommit string // tag "parent-commit"
}

// ParseRootItem parses a kind 1617/1618/1621 event into a RootItem.
func ParseRootItem(ev *gonostr.Event) (*RootItem, error) {
	if ev == nil {
		return nil, fmt.Errorf("event is nil")
	}
	switch ev.Kind {
	case KindPatch, KindPullRequest, KindIssue:
	default:
		return nil, fmt.Errorf("unsupported root kind %d", ev.Kind)
	}

	repoAddr, _ := TagFirst(ev, "a")
	subject, _ := TagFirst(ev, "subject")

	item := &RootItem{
		EventID:   ev.ID.Hex(),
		PubKey:    ev.PubKey.Hex(),
		Kind:      int(ev.Kind),
		RepoAddr:  repoAddr,
		Subject:   subject,
		Content:   ev.Content,
		Labels:    TagAll(ev, "t"),
		CreatedAt: EventTime(ev),
		Raw:       ev,
	}

	// Common PR tags (only meaningful for PR/PR updates, but harmless to parse here).
	item.Commit, _ = TagFirst(ev, "c")
	item.Clone = TagAll(ev, "clone")
	item.MergeBase, _ = TagFirst(ev, "merge-base")
	item.BranchName, _ = TagFirst(ev, "branch-name")

	// Patch tags.
	item.CommitID, _ = TagFirst(ev, "commit")
	item.ParentCommit, _ = TagFirst(ev, "parent-commit")

	return item, nil
}

// StatusEvent is a parsed view of NIP-34 status events (1630-1633).
type StatusEvent struct {
	EventID     string
	PubKey      string
	Kind        int
	RootEventID string // extracted from ["e", "<id>", "", "root"] when present, else first e tag
	Content     string
	CreatedAt   time.Time
	Raw         *gonostr.Event
}

// ParseStatusEvent parses a status kind (1630-1633) into a StatusEvent.
func ParseStatusEvent(ev *gonostr.Event) (*StatusEvent, error) {
	if ev == nil {
		return nil, fmt.Errorf("event is nil")
	}
	switch ev.Kind {
	case KindStatusOpen, KindStatusApplied, KindStatusClosed, KindStatusDraft:
	default:
		return nil, fmt.Errorf("unsupported status kind %d", ev.Kind)
	}

	rootID := ""
	for _, t := range ev.Tags {
		// Preferred: ["e", "<id>", "", "root"]
		if len(t) >= 4 && t[0] == "e" && t[3] == "root" {
			rootID = t[1]
			break
		}
	}
	if rootID == "" {
		// Fallback: first "e" tag.
		for _, t := range ev.Tags {
			if len(t) >= 2 && t[0] == "e" {
				rootID = t[1]
				break
			}
		}
	}
	if strings.TrimSpace(rootID) == "" {
		return nil, fmt.Errorf("status event missing target root e tag")
	}

	return &StatusEvent{
		EventID:     ev.ID.Hex(),
		PubKey:      ev.PubKey.Hex(),
		Kind:        int(ev.Kind),
		RootEventID: rootID,
		Content:     ev.Content,
		CreatedAt:   EventTime(ev),
		Raw:         ev,
	}, nil
}
