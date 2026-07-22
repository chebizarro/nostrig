package nostr

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	gonostr "fiatjaf.com/nostr"
	casnostr "git.sharegap.net/cascadia/cascadia-go/nostr"
)

// RepositoryCoordinate is the parsed form of a NIP-34 repository address.
type RepositoryCoordinate struct {
	Owner  string
	RepoID string
}

// ParseRepositoryAddress validates a 30617:<owner>:<repo-id> coordinate.
func ParseRepositoryAddress(value string) (RepositoryCoordinate, error) {
	parts := strings.SplitN(strings.TrimSpace(value), ":", 3)
	if len(parts) != 3 || parts[0] != strconv.Itoa(KindRepositoryAnnouncement) || strings.TrimSpace(parts[2]) == "" {
		return RepositoryCoordinate{}, fmt.Errorf("invalid NIP-34 repository address %q", value)
	}
	owner, err := gonostr.PubKeyFromHex(strings.TrimSpace(parts[1]))
	if err != nil {
		return RepositoryCoordinate{}, fmt.Errorf("invalid NIP-34 repository owner: %w", err)
	}
	return RepositoryCoordinate{Owner: owner.Hex(), RepoID: strings.TrimSpace(parts[2])}, nil
}

// TrustedMaintainers returns the repository owner plus every valid announced
// maintainer. Invalid individual maintainer values are ignored so a malformed
// optional entry cannot invalidate an otherwise valid owner announcement.
func TrustedMaintainers(repo *RepoAnnouncement) ([]string, error) {
	if repo == nil {
		return nil, fmt.Errorf("repository announcement is required")
	}
	owner, err := gonostr.PubKeyFromHex(strings.TrimSpace(repo.PubKey))
	if err != nil {
		return nil, fmt.Errorf("invalid repository owner: %w", err)
	}
	trusted := map[string]struct{}{owner.Hex(): {}}
	for _, value := range repo.Maintainers {
		key, err := gonostr.PubKeyFromHex(strings.TrimSpace(value))
		if err == nil {
			trusted[key.Hex()] = struct{}{}
		}
	}
	out := make([]string, 0, len(trusted))
	for value := range trusted {
		out = append(out, value)
	}
	sort.Strings(out)
	return out, nil
}

// IsTrustedMaintainer reports whether author is the repository owner or an
// announced maintainer.
func IsTrustedMaintainer(repo *RepoAnnouncement, author string) bool {
	trusted, err := TrustedMaintainers(repo)
	if err != nil {
		return false
	}
	key, err := gonostr.PubKeyFromHex(strings.TrimSpace(author))
	if err != nil {
		return false
	}
	needle := key.Hex()
	for _, candidate := range trusted {
		if candidate == needle {
			return true
		}
	}
	return false
}

// ResolveRepositoryAnnouncement fetches and verifies the current replaceable
// repository announcement for an exact NIP-34 coordinate.
func ResolveRepositoryAnnouncement(ctx context.Context, client *Client, relays []string, repoAddr string) (*RepoAnnouncement, error) {
	coordinate, err := ParseRepositoryAddress(repoAddr)
	if err != nil {
		return nil, err
	}
	if client == nil {
		client = NewClient()
	}
	owner, _ := gonostr.PubKeyFromHex(coordinate.Owner)
	events, err := client.Fetch(ctx, relays, gonostr.Filter{
		Kinds:   []gonostr.Kind{gonostr.Kind(KindRepositoryAnnouncement)},
		Authors: []gonostr.PubKey{owner},
		Tags:    gonostr.TagMap{"d": []string{coordinate.RepoID}},
	})
	if err != nil {
		return nil, fmt.Errorf("fetch repository announcement: %w", err)
	}
	var latest *RepoAnnouncement
	for _, event := range events {
		if event == nil || event.PubKey.Hex() != coordinate.Owner || !casnostr.VerifyEvent((*casnostr.Event)(event)) {
			continue
		}
		repo, parseErr := ParseRepoAnnouncement(event)
		if parseErr != nil || RepoAddress(repo.PubKey, repo.RepoID) != repoAddr {
			continue
		}
		if latest == nil || repo.CreatedAt.After(latest.CreatedAt) ||
			(repo.CreatedAt.Equal(latest.CreatedAt) && repo.EventID > latest.EventID) {
			latest = repo
		}
	}
	if latest == nil {
		return nil, fmt.Errorf("no valid repository announcement found for %s", repoAddr)
	}
	return latest, nil
}

// StatusResolution contains trusted winners plus parseable events that were
// rejected because their author or signature was not trusted.
type StatusResolution struct {
	Trusted   map[string]*StatusEvent
	Untrusted map[string][]*StatusEvent
	Malformed int
}

// ResolveStatuses verifies status events and selects the latest trusted status
// per requested root using (created_at,event_id) as a deterministic order.
func ResolveStatuses(events []*gonostr.Event, rootIDs []string, trustedAuthors []string) StatusResolution {
	roots := make(map[string]struct{}, len(rootIDs))
	for _, id := range rootIDs {
		if id = strings.TrimSpace(id); id != "" {
			roots[id] = struct{}{}
		}
	}
	trusted := make(map[string]struct{}, len(trustedAuthors))
	for _, value := range trustedAuthors {
		if key, err := gonostr.PubKeyFromHex(strings.TrimSpace(value)); err == nil {
			trusted[key.Hex()] = struct{}{}
		}
	}
	out := StatusResolution{
		Trusted:   make(map[string]*StatusEvent, len(roots)),
		Untrusted: make(map[string][]*StatusEvent),
	}
	seen := map[string]struct{}{}
	for _, event := range events {
		status, err := ParseStatusEvent(event)
		if err != nil {
			out.Malformed++
			continue
		}
		if _, ok := roots[status.RootEventID]; !ok {
			continue
		}
		if _, duplicate := seen[status.EventID]; duplicate {
			continue
		}
		seen[status.EventID] = struct{}{}
		_, authorTrusted := trusted[status.PubKey]
		if !authorTrusted || !casnostr.VerifyEvent((*casnostr.Event)(event)) {
			out.Untrusted[status.RootEventID] = append(out.Untrusted[status.RootEventID], status)
			continue
		}
		previous := out.Trusted[status.RootEventID]
		if previous == nil || status.CreatedAt.After(previous.CreatedAt) ||
			(status.CreatedAt.Equal(previous.CreatedAt) && status.EventID > previous.EventID) {
			out.Trusted[status.RootEventID] = status
		}
	}
	for rootID := range out.Untrusted {
		sort.Slice(out.Untrusted[rootID], func(i, j int) bool {
			left, right := out.Untrusted[rootID][i], out.Untrusted[rootID][j]
			if left.CreatedAt.Equal(right.CreatedAt) {
				return left.EventID < right.EventID
			}
			return left.CreatedAt.Before(right.CreatedAt)
		})
	}
	return out
}
