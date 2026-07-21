package fabric

import (
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	beadspb "github.com/chebizarro/nostrig/gen/beads"
	fn "github.com/chebizarro/nostrig/internal/nostr"
	gonostr "github.com/nbd-wtf/go-nostr"
)

const kindDeletion = 5

// Tombstone records a valid NIP-09 coordinate deletion. NIP-09 deletion is a
// request, not an erasure guarantee, so callers retain this evidence and hide
// state versions at or before DeletedAt.
type Tombstone struct {
	Coordinate string
	Reason     string
	EventID    string
	DeletedAt  gonostr.Timestamp
}

// Projection is the deterministic current task-fabric view for one author.
type Projection struct {
	Export     *beadspb.Export
	Tombstones []Tombstone
}

// Reducer consumes relay replay and live events. It verifies IDs, signatures,
// author and coordinates; deduplicates event IDs; and applies NIP-01
// addressable latest-wins rules (lowest ID wins an equal-timestamp tie).
type Reducer struct {
	author     string
	verify     bool
	seen       map[string]struct{}
	current    map[string]*gonostr.Event
	tombstones map[string]Tombstone
}

// NewReducer creates a verified reducer restricted to one 32-byte hex author.
func NewReducer(author string) (*Reducer, error) {
	if !validPubkey(author) {
		return nil, fmt.Errorf("author must be a 32-byte lowercase hex pubkey")
	}
	return &Reducer{
		author:     author,
		verify:     true,
		seen:       make(map[string]struct{}),
		current:    make(map[string]*gonostr.Event),
		tombstones: make(map[string]Tombstone),
	}, nil
}

func newUncheckedReducer() *Reducer {
	return &Reducer{
		seen:       make(map[string]struct{}),
		current:    make(map[string]*gonostr.Event),
		tombstones: make(map[string]Tombstone),
	}
}

// Apply returns true only when the materialized projection changed. Replayed
// events and relay echoes return false, which is the sync loop guard.
func (r *Reducer) Apply(ev *gonostr.Event) (bool, error) {
	if r == nil || ev == nil {
		return false, nil
	}
	if r.verify {
		if err := ValidateSignedEvent(ev, r.author); err != nil {
			return false, err
		}
	}
	if ev.ID != "" {
		if _, ok := r.seen[ev.ID]; ok {
			return false, nil
		}
		r.seen[ev.ID] = struct{}{}
	}

	if ev.Kind == kindDeletion {
		return r.applyDeletion(ev)
	}
	if ev.Kind == fn.KindTaskState || ev.Kind == fn.KindNIP51Set {
		if _, ok := fn.TagD(ev); !ok {
			return false, fmt.Errorf("kind %d event %s missing d tag", ev.Kind, ev.ID)
		}
	}
	coord, recognized, err := stateCoordinate(ev)
	if err != nil {
		return false, err
	}
	if !recognized {
		return false, nil
	}
	if _, _, _, err := decodeStateEvent(ev); err != nil {
		return false, err
	}
	if tomb, ok := r.tombstones[coord]; ok && ev.CreatedAt <= tomb.DeletedAt {
		return false, nil
	}
	if prior := r.current[coord]; prior != nil && !newer(ev, prior) {
		return false, nil
	}
	changed := priorMateriallyDifferent(r.current[coord], ev)
	r.current[coord] = cloneEvent(ev)
	return changed, nil
}

func (r *Reducer) applyDeletion(ev *gonostr.Event) (bool, error) {
	changed := false
	for _, tag := range ev.Tags {
		if len(tag) < 2 || tag[0] != "a" {
			continue
		}
		coord := tag[1]
		kind, author, d, ok := parseCoordinate(coord)
		if !ok || author != ev.PubKey || !supportedCoordinate(kind, d) {
			continue
		}
		prior, exists := r.tombstones[coord]
		candidate := Tombstone{Coordinate: coord, Reason: ev.Content, EventID: ev.ID, DeletedAt: ev.CreatedAt}
		if exists && !newerTombstone(candidate, prior) {
			continue
		}
		r.tombstones[coord] = candidate
		if current := r.current[coord]; current != nil && current.CreatedAt <= ev.CreatedAt {
			delete(r.current, coord)
			changed = true
		}
	}
	return changed, nil
}

// NeedsPublish reports whether an encoded local state differs materially from
// the relay-derived projection. A false result prevents receive/apply/export
// echo loops even when the local encoder would assign a newer timestamp.
func (r *Reducer) NeedsPublish(candidate *gonostr.Event) (bool, error) {
	coord, recognized, err := stateCoordinate(candidate)
	if err != nil || !recognized {
		return false, err
	}
	if _, _, _, err := decodeStateEvent(candidate); err != nil {
		return false, err
	}
	current := r.current[coord]
	if current == nil {
		return true, nil
	}
	return priorMateriallyDifferent(current, candidate), nil
}

// Snapshot returns a stable, sorted Beads view and retained tombstone evidence.
func (r *Reducer) Snapshot() (*Projection, error) {
	events := make([]*gonostr.Event, 0, len(r.current))
	for _, ev := range r.current {
		events = append(events, cloneEvent(ev))
	}
	export, err := decodeCurrent(events)
	if err != nil {
		return nil, err
	}
	tombstones := make([]Tombstone, 0, len(r.tombstones))
	for _, tomb := range r.tombstones {
		tombstones = append(tombstones, tomb)
	}
	sort.Slice(tombstones, func(i, j int) bool { return tombstones[i].Coordinate < tombstones[j].Coordinate })
	return &Projection{Export: export, Tombstones: tombstones}, nil
}

// ProjectVerified reconstructs relay state for an exact signer identity.
func ProjectVerified(events []*gonostr.Event, author string) (*Projection, error) {
	r, err := NewReducer(author)
	if err != nil {
		return nil, err
	}
	for _, ev := range events {
		if ev != nil && ev.PubKey != author {
			continue
		}
		if _, err := r.Apply(ev); err != nil {
			return nil, err
		}
	}
	return r.Snapshot()
}

// ValidateSignedEvent fail-closes on malformed IDs, signatures, authors,
// task/epic addresses, relationship indexes, and deletion coordinates.
func ValidateSignedEvent(ev *gonostr.Event, author string) error {
	if ev == nil {
		return fmt.Errorf("event is nil")
	}
	if author != "" && ev.PubKey != author {
		return fmt.Errorf("event author %q does not match expected author %q", ev.PubKey, author)
	}
	if !validPubkey(ev.PubKey) {
		return fmt.Errorf("event pubkey must be 32-byte lowercase hex")
	}
	if ev.ID == "" || ev.ID != ev.GetID() {
		return fmt.Errorf("event id does not match serialized event")
	}
	ok, err := ev.CheckSignature()
	if err != nil {
		return fmt.Errorf("event signature: %w", err)
	}
	if !ok {
		return fmt.Errorf("event signature is invalid")
	}
	if ev.Kind == kindDeletion {
		valid := 0
		for _, tag := range ev.Tags {
			if len(tag) < 2 || tag[0] != "a" {
				continue
			}
			kind, pubkey, d, parsed := parseCoordinate(tag[1])
			if !parsed || pubkey != ev.PubKey || !supportedCoordinate(kind, d) {
				return fmt.Errorf("invalid deletion coordinate %q", tag[1])
			}
			valid++
		}
		if valid == 0 {
			return fmt.Errorf("deletion event has no supported a coordinate")
		}
		return nil
	}
	issue, epic, recognized, err := decodeStateEvent(ev)
	if err != nil {
		return err
	}
	if !recognized {
		return fmt.Errorf("unsupported task-fabric event kind/address")
	}
	if err := validateSingleD(ev); err != nil {
		return err
	}
	if !hasTag(ev.Tags, "schema", schema) {
		return fmt.Errorf("event is missing schema tag %q", schema)
	}
	if issue != nil {
		return validateIssueIndexes(ev, issue)
	}
	return validateEpicIndexes(ev, epic)
}

// EncodeDeletion creates an unsigned NIP-09 deletion request for task/epic
// coordinates. It is signed through the same Signet-only Publisher boundary.
func EncodeDeletion(coordinates []string, pubkey string, at time.Time, reason string) (*gonostr.Event, error) {
	if len(coordinates) == 0 {
		return nil, fmt.Errorf("at least one coordinate is required")
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	tags := make(gonostr.Tags, 0, len(coordinates)*2)
	seenKinds := make(map[int]struct{})
	for _, coord := range coordinates {
		kind, author, d, ok := parseCoordinate(coord)
		if !ok || author != pubkey || !supportedCoordinate(kind, d) {
			return nil, fmt.Errorf("invalid deletion coordinate %q", coord)
		}
		tags = append(tags, gonostr.Tag{"a", coord})
		seenKinds[kind] = struct{}{}
	}
	kinds := make([]int, 0, len(seenKinds))
	for kind := range seenKinds {
		kinds = append(kinds, kind)
	}
	sort.Ints(kinds)
	for _, kind := range kinds {
		tags = append(tags, gonostr.Tag{"k", strconv.Itoa(kind)})
	}
	return &gonostr.Event{PubKey: pubkey, CreatedAt: gonostr.Timestamp(at.Unix()), Kind: kindDeletion, Tags: tags, Content: reason}, nil
}

func stateCoordinate(ev *gonostr.Event) (string, bool, error) {
	if ev == nil {
		return "", false, nil
	}
	d, ok := fn.TagD(ev)
	if !ok {
		return "", false, nil
	}
	if !supportedCoordinate(ev.Kind, d) {
		return "", false, nil
	}
	if err := validateSingleD(ev); err != nil {
		return "", true, err
	}
	return fmt.Sprintf("%d:%s:%s", ev.Kind, ev.PubKey, d), true, nil
}

func supportedCoordinate(kind int, d string) bool {
	return (kind == fn.KindTaskState && strings.HasPrefix(d, "task:") && len(d) > len("task:")) ||
		(kind == fn.KindNIP51Set && strings.HasPrefix(d, "epic:") && len(d) > len("epic:"))
}

func validPubkey(pubkey string) bool {
	if len(pubkey) != 64 || pubkey != strings.ToLower(pubkey) {
		return false
	}
	decoded, err := hex.DecodeString(pubkey)
	return err == nil && len(decoded) == 32
}

func parseCoordinate(coord string) (int, string, string, bool) {
	parts := strings.SplitN(coord, ":", 3)
	if len(parts) != 3 {
		return 0, "", "", false
	}
	kind, err := strconv.Atoi(parts[0])
	if err != nil || parts[1] == "" || parts[2] == "" {
		return 0, "", "", false
	}
	return kind, parts[1], parts[2], true
}

func validateSingleD(ev *gonostr.Event) error {
	count := 0
	for _, tag := range ev.Tags {
		if len(tag) > 1 && tag[0] == "d" {
			count++
		}
	}
	if count != 1 {
		return fmt.Errorf("event must contain exactly one d tag, got %d", count)
	}
	return nil
}

func validateIssueIndexes(ev *gonostr.Event, issue *beadspb.Issue) error {
	if !hasTag(ev.Tags, "t", "task") {
		return fmt.Errorf("task %q is missing task index tag", issue.Id)
	}
	wantEpic := ""
	if issue.Epic != "" {
		wantEpic = fmt.Sprintf("%d:%s:epic:%s", fn.KindNIP51Set, ev.PubKey, issue.Epic)
	}
	gotEpic := markedValues(ev.Tags, "a", "epic")
	if (wantEpic == "" && len(gotEpic) != 0) || (wantEpic != "" && !reflect.DeepEqual(gotEpic, []string{wantEpic})) {
		return fmt.Errorf("task %q epic index does not match content", issue.Id)
	}
	wantDeps := make([]string, 0, len(issue.DependsOn))
	for _, dep := range issue.DependsOn {
		wantDeps = append(wantDeps, fmt.Sprintf("%d:%s:task:%s", fn.KindTaskState, ev.PubKey, dep))
	}
	if !sameStrings(wantDeps, markedValues(ev.Tags, "a", "depends_on")) {
		return fmt.Errorf("task %q dependency indexes do not match content", issue.Id)
	}
	wantAssignee := []string(nil)
	if issue.Assignee != "" {
		wantAssignee = []string{issue.Assignee}
	}
	pubkeyAssignees := markedValues(ev.Tags, "p", "assignee")
	namedAssignees := tagValues(ev.Tags, "assignee")
	gotAssignee := pubkeyAssignees
	if issue.Assignee != "" && !validPubkey(issue.Assignee) {
		if len(pubkeyAssignees) != 0 {
			return fmt.Errorf("task %q has a non-pubkey assignee in a p tag", issue.Id)
		}
		gotAssignee = namedAssignees
	} else if len(namedAssignees) != 0 {
		return fmt.Errorf("task %q has conflicting assignee indexes", issue.Id)
	}
	if !sameStrings(wantAssignee, gotAssignee) {
		return fmt.Errorf("task %q assignee index does not match content", issue.Id)
	}
	gotLabels := tagValues(ev.Tags, "t")
	removedTask := false
	filtered := gotLabels[:0]
	for _, value := range gotLabels {
		if value == "task" && !removedTask {
			removedTask = true
			continue
		}
		filtered = append(filtered, value)
	}
	if !sameStrings(issue.Labels, filtered) {
		return fmt.Errorf("task %q label indexes do not match content", issue.Id)
	}
	return nil
}

func validateEpicIndexes(ev *gonostr.Event, epic *beadspb.Epic) error {
	if epic == nil || !hasTag(ev.Tags, "t", "queue") || !hasTag(ev.Tags, "t", "epic") {
		return fmt.Errorf("epic event is missing queue/epic index tags")
	}
	for _, tag := range ev.Tags {
		if len(tag) < 2 || tag[0] != "a" {
			continue
		}
		kind, author, d, ok := parseCoordinate(tag[1])
		if !ok || kind != fn.KindTaskState || author != ev.PubKey || !strings.HasPrefix(d, "task:") {
			return fmt.Errorf("epic %q has invalid task member coordinate %q", epic.Id, tag[1])
		}
	}
	return nil
}

func hasTag(tags gonostr.Tags, name, value string) bool {
	for _, tag := range tags {
		if len(tag) > 1 && tag[0] == name && tag[1] == value {
			return true
		}
	}
	return false
}

func markedValues(tags gonostr.Tags, name, marker string) []string {
	values := make([]string, 0)
	for _, tag := range tags {
		if len(tag) > 2 && tag[0] == name && tag[2] == marker {
			values = append(values, tag[1])
		}
	}
	return values
}

func tagValues(tags gonostr.Tags, name string) []string {
	values := make([]string, 0)
	for _, tag := range tags {
		if len(tag) > 1 && tag[0] == name {
			values = append(values, tag[1])
		}
	}
	return values
}

func sameStrings(a, b []string) bool {
	a = append([]string(nil), a...)
	b = append([]string(nil), b...)
	sort.Strings(a)
	sort.Strings(b)
	return reflect.DeepEqual(a, b)
}

func newer(candidate, current *gonostr.Event) bool {
	if candidate.CreatedAt != current.CreatedAt {
		return candidate.CreatedAt > current.CreatedAt
	}
	if candidate.ID == current.ID {
		return false
	}
	if current.ID == "" {
		return candidate.ID != ""
	}
	if candidate.ID == "" {
		return false
	}
	return candidate.ID < current.ID
}

func newerTombstone(candidate, current Tombstone) bool {
	if candidate.DeletedAt != current.DeletedAt {
		return candidate.DeletedAt > current.DeletedAt
	}
	if candidate.EventID == current.EventID {
		return false
	}
	return current.EventID == "" || candidate.EventID != "" && candidate.EventID < current.EventID
}

func priorMateriallyDifferent(a, b *gonostr.Event) bool {
	if a == nil || b == nil {
		return a != b
	}
	return a.PubKey != b.PubKey || a.Kind != b.Kind || a.Content != b.Content || !reflect.DeepEqual(a.Tags, b.Tags)
}

func cloneEvent(ev *gonostr.Event) *gonostr.Event {
	if ev == nil {
		return nil
	}
	clone := *ev
	clone.Tags = make(gonostr.Tags, len(ev.Tags))
	for i, tag := range ev.Tags {
		clone.Tags[i] = append(gonostr.Tag(nil), tag...)
	}
	return &clone
}
