package taskfabric

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	beadspb "github.com/chebizarro/nostrig/gen/beads"
	"github.com/chebizarro/nostrig/internal/taskmodel"
)

const (
	DefaultCacheRelPath = ".nostrig/task-cache.jsonl"

	ResolutionClean      = "clean"
	ResolutionRelayOnly  = "relay_only"
	ResolutionLocalOnly  = "local_only"
	ResolutionLatestWins = "latest_wins"
	ResolutionConflict   = "conflict"
)

type TaskSnapshot taskmodel.IssueDocument

func (s TaskSnapshot) MarshalJSON() ([]byte, error) {
	doc := taskmodel.IssueDocument(s)
	return json.Marshal(doc)
}

func (s *TaskSnapshot) UnmarshalJSON(data []byte) error {
	doc, err := taskmodel.DecodeBeads(data)
	if err != nil {
		return err
	}
	doc.RecordType = ""
	*s = TaskSnapshot(*doc)
	return nil
}

type ConflictMetadata struct {
	Reason        string   `json:"reason"`
	ChangedFields []string `json:"changed_fields,omitempty"`
	LocalRevision string   `json:"local_revision,omitempty"`
	RelayEventID  string   `json:"relay_event_id,omitempty"`
}

type CacheRecord struct {
	SchemaVersion int               `json:"schema_version"`
	Type          string            `json:"_type"`
	ID            string            `json:"id"`
	Resolved      *TaskSnapshot     `json:"resolved,omitempty"`
	Local         *TaskSnapshot     `json:"local,omitempty"`
	Relay         *TaskSnapshot     `json:"relay,omitempty"`
	LocalRevision string            `json:"local_revision,omitempty"`
	RelayEventID  string            `json:"relay_event_id,omitempty"`
	LocalUpdated  string            `json:"local_updated,omitempty"`
	RelayUpdated  string            `json:"relay_updated,omitempty"`
	Resolution    string            `json:"resolution"`
	Conflict      *ConflictMetadata `json:"conflict,omitempty"`
}

type MergeOptions struct {
	RelayWinsOnConflict bool
	// RelayAuthoritative treats local Beads JSONL as a projection only. Local
	// drift is reported but never selected as resolved canonical state.
	RelayAuthoritative bool
	// AuthoritativeTaskIDs narrows an authoritative merge. Cached relay state
	// outside this exact selector is retained because it was not refetched.
	AuthoritativeTaskIDs []string
}

type MergeResult struct {
	Records   []*CacheRecord
	Export    *beadspb.Export
	Conflicts []*CacheRecord
}

func DefaultCachePath(outDir string) string {
	outDir = strings.TrimSpace(outDir)
	if outDir == "" {
		outDir = "."
	}
	return filepath.Join(outDir, DefaultCacheRelPath)
}

func LoadCache(path string) ([]*CacheRecord, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var out []*CacheRecord
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" {
			continue
		}
		var rec CacheRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			return nil, fmt.Errorf("decode cache %s: %w", path, err)
		}
		if rec.ID == "" && rec.Resolved != nil {
			rec.ID = rec.Resolved.ID
		}
		if rec.ID == "" {
			continue
		}
		rec.SchemaVersion = 2
		rec.Resolved = normalizeSnapshot(rec.Resolved)
		rec.Local = normalizeSnapshot(rec.Local)
		rec.Relay = normalizeSnapshot(rec.Relay)
		out = append(out, &rec)
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func WriteCache(path string, records []*CacheRecord) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("cache path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	records = append([]*CacheRecord(nil), records...)
	sort.Slice(records, func(i, j int) bool { return records[i].ID < records[j].ID })
	for _, rec := range records {
		if rec == nil {
			continue
		}
		rec.SchemaVersion = 2
		if rec.Type == "" {
			rec.Type = "nostrig_task_cache"
		}
		if err := enc.Encode(rec); err != nil {
			return err
		}
	}
	return nil
}

func LoadLocalIssues(outDir string) ([]*TaskSnapshot, error) {
	path := filepath.Join(strings.TrimSpace(outDir), ".beads", "issues.jsonl")
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var out []*TaskSnapshot
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" {
			continue
		}
		var raw map[string]json.RawMessage
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			return nil, fmt.Errorf("decode local issue %s: %w", path, err)
		}
		if typ := rawString(raw, "_type"); typ != "" && typ != "issue" {
			continue
		}
		encoded, err := json.Marshal(raw)
		if err != nil {
			return nil, fmt.Errorf("encode local issue %s: %w", path, err)
		}
		doc, err := taskmodel.DecodeBeads(encoded)
		if err != nil {
			return nil, fmt.Errorf("decode local issue %s: %w", path, err)
		}
		if strings.TrimSpace(doc.ID) == "" {
			continue
		}
		snap := TaskSnapshot(*doc)
		out = append(out, &snap)
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func MergeTaskState(relayExport *beadspb.Export, local []*TaskSnapshot, previous []*CacheRecord) (*MergeResult, error) {
	return MergeTaskStateWithOptions(relayExport, local, previous, MergeOptions{})
}

func MergeTaskStateWithOptions(relayExport *beadspb.Export, local []*TaskSnapshot, previous []*CacheRecord, opts MergeOptions) (*MergeResult, error) {
	authoritativeScope := map[string]struct{}{}
	for _, id := range cleanTaskIDs(opts.AuthoritativeTaskIDs) {
		authoritativeScope[id] = struct{}{}
	}
	prevByID := map[string]*CacheRecord{}
	for _, rec := range previous {
		if rec != nil && strings.TrimSpace(rec.ID) != "" {
			prevByID[rec.ID] = rec
		}
	}
	localByID := map[string]*TaskSnapshot{}
	for _, snap := range local {
		if snap != nil && strings.TrimSpace(snap.ID) != "" {
			localByID[snap.ID] = normalizeSnapshot(snap)
		}
	}
	relayByID := map[string]*TaskSnapshot{}
	if relayExport != nil {
		for _, issue := range relayExport.Issues {
			snap := SnapshotFromIssue(issue)
			if snap != nil && snap.ID != "" {
				relayByID[snap.ID] = snap
			}
		}
	}

	ids := map[string]struct{}{}
	for id := range prevByID {
		ids[id] = struct{}{}
	}
	for id := range localByID {
		ids[id] = struct{}{}
	}
	for id := range relayByID {
		ids[id] = struct{}{}
	}

	records := make([]*CacheRecord, 0, len(ids))
	conflicts := make([]*CacheRecord, 0)
	for id := range ids {
		prev := prevByID[id]
		localSnap := localByID[id]
		relaySnap := relayByID[id]
		if opts.RelayAuthoritative && relaySnap == nil && len(authoritativeScope) > 0 {
			if _, selected := authoritativeScope[id]; !selected {
				if prev == nil || prev.Relay == nil {
					return nil, fmt.Errorf("exact-task sync cannot safely rewrite unqueried task %s without a prior relay projection", id)
				}
				// This task was outside the exact relay query. Preserve only its
				// previously observed relay state, never an unverified local value.
				relaySnap = prev.Relay
			}
		}
		localRev := SnapshotRevision(localSnap)
		relayEventID := metadataValue(relaySnap, "nostr.id")
		localChanged := localSnap != nil && (prev == nil || prev.LocalRevision != localRev)
		relayChanged := relaySnap != nil && (prev == nil || prev.RelayEventID != relayEventID)

		resolved := (*TaskSnapshot)(nil)
		resolution := ResolutionClean
		var conflict *ConflictMetadata

		if opts.RelayAuthoritative {
			switch {
			case relaySnap == nil:
				// Absence from the authoritative relay projection means deletion.
				// Retain local/previous state only in the cache for diagnostics.
				resolution = ResolutionLocalOnly
			case localSnap == nil:
				resolved = relaySnap
				resolution = ResolutionRelayOnly
			case snapshotsEqual(localSnap, relaySnap):
				resolved = relaySnap
				if localChanged || relayChanged {
					resolution = ResolutionLatestWins
				}
			default:
				resolved = relaySnap
				resolution = ResolutionConflict
				conflict = &ConflictMetadata{
					Reason: "local_projection_drift", ChangedFields: materialChangedFields(localSnap, relaySnap),
					LocalRevision: localRev, RelayEventID: relayEventID,
				}
			}
		} else {
			switch {
			case localSnap == nil && relaySnap != nil:
				resolved = relaySnap
				resolution = ResolutionRelayOnly
			case relaySnap == nil && localSnap != nil:
				resolved = localSnap
				resolution = ResolutionLocalOnly
			case localSnap != nil && relaySnap != nil && snapshotsEqual(localSnap, relaySnap):
				resolved = latestSnapshot(localSnap, relaySnap)
				if localChanged || relayChanged {
					resolution = ResolutionLatestWins
				}
			case localSnap != nil && relaySnap != nil && localChanged && relayChanged:
				changed := materialChangedFields(localSnap, relaySnap)
				if compatibleAutoMerge(localSnap, relaySnap, changed) {
					resolved = latestSnapshot(localSnap, relaySnap)
					resolution = ResolutionLatestWins
				} else {
					if opts.RelayWinsOnConflict {
						resolved = relaySnap
					} else {
						resolved = previousResolved(prev, latestSnapshot(localSnap, relaySnap))
					}
					resolution = ResolutionConflict
					conflict = &ConflictMetadata{Reason: "local_and_relay_changed", ChangedFields: changed, LocalRevision: localRev, RelayEventID: relayEventID}
				}
			case localSnap != nil && relaySnap != nil && relayChanged:
				resolved = relaySnap
				resolution = ResolutionRelayOnly
			case localSnap != nil && relaySnap != nil && localChanged:
				resolved = localSnap
				resolution = ResolutionLocalOnly
			case prev != nil && prev.Resolved != nil:
				resolved = prev.Resolved
			case relaySnap != nil:
				resolved = relaySnap
			default:
				resolved = localSnap
			}
		}

		rec := &CacheRecord{SchemaVersion: 2, Type: "nostrig_task_cache", ID: id, Resolved: normalizeSnapshot(resolved), Local: normalizeSnapshot(localSnap), Relay: normalizeSnapshot(relaySnap), LocalRevision: localRev, RelayEventID: relayEventID, LocalUpdated: snapshotUpdated(localSnap), RelayUpdated: snapshotUpdated(relaySnap), Resolution: resolution, Conflict: conflict}
		records = append(records, rec)
		if conflict != nil {
			conflicts = append(conflicts, rec)
		}
	}
	sort.Slice(records, func(i, j int) bool { return records[i].ID < records[j].ID })
	issues := make([]*beadspb.Issue, 0, len(records))
	for _, rec := range records {
		if rec.Resolved == nil {
			continue
		}
		issue := rec.Resolved.ToIssue()
		ensureIssueMetadata(issue)
		issue.Metadata.Custom["nostrig.cache_resolution"] = rec.Resolution
		if rec.Conflict != nil {
			issue.Metadata.Custom["nostrig.conflict"] = rec.Conflict.Reason
			issue.Metadata.Custom["nostrig.conflict_fields"] = strings.Join(rec.Conflict.ChangedFields, ",")
		}
		issues = append(issues, issue)
	}
	return &MergeResult{Records: records, Export: &beadspb.Export{Issues: issues}, Conflicts: conflicts}, nil
}

func SnapshotFromIssue(issue *beadspb.Issue) *TaskSnapshot {
	doc, err := taskmodel.FromProto(issue)
	if err != nil {
		return nil
	}
	snap := TaskSnapshot(*doc)
	return &snap
}

func (s *TaskSnapshot) ToIssue() *beadspb.Issue {
	if s == nil {
		return nil
	}
	doc := taskmodel.IssueDocument(*s)
	issue, _ := taskmodel.ToProto(&doc)
	return issue
}

func ExportFromCache(records []*CacheRecord) *beadspb.Export {
	issues := make([]*beadspb.Issue, 0, len(records))
	for _, rec := range records {
		if rec == nil || rec.Resolved == nil || rec.Resolution == ResolutionConflict {
			continue
		}
		issues = append(issues, rec.Resolved.ToIssue())
	}
	sort.Slice(issues, func(i, j int) bool { return issues[i].Id < issues[j].Id })
	return &beadspb.Export{Issues: issues}
}

func SnapshotRevision(s *TaskSnapshot) string {
	if s == nil {
		return ""
	}
	doc := taskmodel.IssueDocument(*s)
	return taskmodel.MaterialRevision(&doc)
}

func normalizeSnapshot(s *TaskSnapshot) *TaskSnapshot {
	if s == nil {
		return nil
	}
	doc := taskmodel.IssueDocument(*s)
	normalized, err := taskmodel.Normalize(&doc)
	if err != nil {
		return nil
	}
	out := TaskSnapshot(*normalized)
	return &out
}

func snapshotsEqual(a, b *TaskSnapshot) bool {
	if a == nil || b == nil {
		return a == b
	}
	aa, bb := taskmodel.IssueDocument(*a), taskmodel.IssueDocument(*b)
	return taskmodel.MaterialEqual(&aa, &bb)
}

func materialChangedFields(a, b *TaskSnapshot) []string {
	if a == nil || b == nil {
		return []string{"task"}
	}
	aa, bb := taskmodel.IssueDocument(*a), taskmodel.IssueDocument(*b)
	return taskmodel.MaterialChangedFields(&aa, &bb)
}

func compatibleAutoMerge(local, relay *TaskSnapshot, changed []string) bool {
	allowed := map[string]bool{"status": true, "assignee": true}
	for _, f := range changed {
		if !allowed[f] {
			return false
		}
	}
	return !snapshotTime(local).IsZero() && !snapshotTime(relay).IsZero() && !snapshotTime(local).Equal(snapshotTime(relay))
}

func latestSnapshot(a, b *TaskSnapshot) *TaskSnapshot {
	at := snapshotTime(a)
	bt := snapshotTime(b)
	if bt.After(at) {
		return b
	}
	return a
}

func previousResolved(prev *CacheRecord, fallback *TaskSnapshot) *TaskSnapshot {
	if prev != nil && prev.Resolved != nil {
		return prev.Resolved
	}
	return fallback
}

func snapshotUpdated(s *TaskSnapshot) string {
	if s == nil {
		return ""
	}
	return s.Updated
}

func snapshotTime(s *TaskSnapshot) time.Time {
	if s == nil {
		return time.Time{}
	}
	if t, err := parseTime(s.Updated); err == nil && !t.IsZero() {
		return t
	}
	if t, err := parseTime(s.Created); err == nil {
		return t
	}
	return time.Time{}
}

func parseTime(v string) (time.Time, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return time.Time{}, nil
	}
	layouts := []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05Z07:00", "2006-01-02 15:04:05"}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, v); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported time %q", v)
}

func ensureIssueMetadata(issue *beadspb.Issue) {
	if issue.Metadata == nil {
		issue.Metadata = &beadspb.Metadata{}
	}
	if issue.Metadata.Custom == nil {
		issue.Metadata.Custom = map[string]string{}
	}
}

func metadataValue(s *TaskSnapshot, key string) string {
	if s == nil || s.Metadata == nil {
		return ""
	}
	return strings.TrimSpace(s.Metadata[key])
}

func copyStringMap(in map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range in {
		out[k] = v
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func rawString(raw map[string]json.RawMessage, key string) string {
	v, ok := raw[key]
	if !ok {
		return ""
	}
	var s string
	if err := json.Unmarshal(v, &s); err == nil {
		return s
	}
	var n float64
	if err := json.Unmarshal(v, &n); err == nil {
		return strconv.FormatFloat(n, 'f', -1, 64)
	}
	return ""
}

func firstRawString(raw map[string]json.RawMessage, keys ...string) string {
	for _, key := range keys {
		if v := rawString(raw, key); v != "" {
			return v
		}
	}
	return ""
}

func rawStrings(raw map[string]json.RawMessage, key string) []string {
	v, ok := raw[key]
	if !ok {
		return nil
	}
	var ss []string
	if err := json.Unmarshal(v, &ss); err == nil {
		return ss
	}
	return nil
}

func firstRawStrings(raw map[string]json.RawMessage, keys ...string) []string {
	for _, key := range keys {
		if v := rawStrings(raw, key); len(v) > 0 {
			return v
		}
	}
	return nil
}

func rawMap(raw map[string]json.RawMessage, key string) map[string]any {
	v, ok := raw[key]
	if !ok {
		return nil
	}
	var out map[string]any
	_ = json.Unmarshal(v, &out)
	return out
}

func priorityFromNumber(raw map[string]json.RawMessage, key string) string {
	v, ok := raw[key]
	if !ok {
		return ""
	}
	var n int
	if err := json.Unmarshal(v, &n); err == nil && n >= 0 && n <= 4 {
		return fmt.Sprintf("P%d", n)
	}
	return ""
}
