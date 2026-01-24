package converter

import (
	"fmt"

	nip34 "github.com/bizarro/nostrig/internal/nostr"
)

// Aggregate is the normalized, single-repository dataset fetched from Nostr.
// It is the input to the beads conversion step.
type Aggregate struct {
	Repo  *nip34.RepoAnnouncement
	State *nip34.RepoState

	// Items are root work items (issues, PRs, patches).
	Items []*AggregateItem

	// StatusByRoot holds resolved/latest statuses by root event ID.
	// Converter uses "latest status wins" semantics.
	StatusByRoot map[string]*nip34.StatusEvent
}

// AggregateItem represents a single NIP-34 root item and its associated resolved status.
type AggregateItem struct {
	Root   *nip34.RootItem
	Status *nip34.StatusEvent
}

// Validate checks the aggregate has the required minimum fields for conversion.
func (a *Aggregate) Validate() error {
	if a == nil {
		return fmt.Errorf("aggregate is nil")
	}
	if a.Repo == nil {
		return fmt.Errorf("aggregate repo is nil")
	}
	if a.Repo.RepoID == "" {
		return fmt.Errorf("aggregate repo is missing repo id")
	}
	return nil
}

// StatusFor returns the resolved status for a root event id, if present.
func (a *Aggregate) StatusFor(rootEventID string) *nip34.StatusEvent {
	if a == nil {
		return nil
	}
	if a.StatusByRoot != nil {
		if st, ok := a.StatusByRoot[rootEventID]; ok {
			return st
		}
	}
	for _, it := range a.Items {
		if it == nil || it.Root == nil {
			continue
		}
		if it.Root.EventID == rootEventID {
			return it.Status
		}
	}
	return nil
}