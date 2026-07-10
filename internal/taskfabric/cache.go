package taskfabric

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	beadspb "github.com/chebizarro/nostrig/gen/beads"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	DefaultCacheRelPath = ".nostrig/task-cache.jsonl"

	ResolutionClean      = "clean"
	ResolutionRelayOnly  = "relay_only"
	ResolutionLocalOnly  = "local_only"
	ResolutionLatestWins = "latest_wins"
	ResolutionConflict   = "conflict"
)

type TaskSnapshot struct {
	ID          string            `json:"id"`
	Title       string            `json:"title"`
	Description string            `json:"description,omitempty"`
	Status      string            `json:"status"`
	Priority    string            `json:"priority,omitempty"`
	Epic        string            `json:"epic,omitempty"`
	Assignee    string            `json:"assignee,omitempty"`
	Labels      []string          `json:"labels,omitempty"`
	DependsOn   []string          `json:"depends_on,omitempty"`
	Created     string            `json:"created,omitempty"`
	Updated     string            `json:"updated,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

type ConflictMetadata struct {
	Reason        string   `json:"reason"`
	ChangedFields []string `json:"changed_fields,omitempty"`
	LocalRevision string   `json:"local_revision,omitempty"`
	RelayEventID  string   `json:"relay_event_id,omitempty"`
}

type CacheRecord struct {
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
		snap := snapshotFromRaw(raw)
		if strings.TrimSpace(snap.ID) == "" {
			continue
		}
		out = append(out, snap)
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
		localRev := SnapshotRevision(localSnap)
		relayEventID := metadataValue(relaySnap, "nostr.id")
		localChanged := localSnap != nil && (prev == nil || prev.LocalRevision != localRev)
		relayChanged := relaySnap != nil && (prev == nil || prev.RelayEventID != relayEventID)

		resolved := (*TaskSnapshot)(nil)
		resolution := ResolutionClean
		var conflict *ConflictMetadata

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

		rec := &CacheRecord{Type: "nostrig_task_cache", ID: id, Resolved: normalizeSnapshot(resolved), Local: normalizeSnapshot(localSnap), Relay: normalizeSnapshot(relaySnap), LocalRevision: localRev, RelayEventID: relayEventID, LocalUpdated: snapshotUpdated(localSnap), RelayUpdated: snapshotUpdated(relaySnap), Resolution: resolution, Conflict: conflict}
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
	if issue == nil {
		return nil
	}
	snap := &TaskSnapshot{ID: issue.Id, Title: issue.Title, Description: issue.Description, Status: nip34.StatusString(issue.Status), Priority: nip34.PriorityString(issue.Priority), Epic: issue.Epic, Assignee: issue.Assignee, Labels: append([]string(nil), issue.Labels...), DependsOn: append([]string(nil), issue.DependsOn...), Metadata: map[string]string{}}
	if issue.Created != nil {
		snap.Created = issue.Created.AsTime().UTC().Format(time.RFC3339)
	}
	if issue.Updated != nil {
		snap.Updated = issue.Updated.AsTime().UTC().Format(time.RFC3339)
	}
	if issue.Metadata != nil {
		for k, v := range issue.Metadata.Custom {
			snap.Metadata[k] = v
		}
		if issue.Metadata.JiraKey != "" {
			snap.Metadata["jiraKey"] = issue.Metadata.JiraKey
		}
	}
	return normalizeSnapshot(snap)
}

func (s *TaskSnapshot) ToIssue() *beadspb.Issue {
	if s == nil {
		return nil
	}
	issue := &beadspb.Issue{Id: s.ID, Title: s.Title, Description: s.Description, Status: nip34.ParseStatus(s.Status), Priority: nip34.ParsePriority(s.Priority), Epic: s.Epic, Assignee: s.Assignee, Labels: append([]string(nil), s.Labels...), DependsOn: append([]string(nil), s.DependsOn...), Metadata: &beadspb.Metadata{Custom: map[string]string{}}}
	if created, err := parseTime(s.Created); err == nil && !created.IsZero() {
		issue.Created = timestamppb.New(created)
	}
	if updated, err := parseTime(s.Updated); err == nil && !updated.IsZero() {
		issue.Updated = timestamppb.New(updated)
	}
	for k, v := range s.Metadata {
		issue.Metadata.Custom[k] = v
	}
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
	canonical, _ := json.Marshal(normalizeSnapshot(s))
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}

func snapshotFromRaw(raw map[string]json.RawMessage) *TaskSnapshot {
	meta := map[string]string{}
	if m := rawMap(raw, "metadata"); len(m) > 0 {
		for k, v := range m {
			meta[k] = fmt.Sprint(v)
		}
	}
	status := rawString(raw, "status")
	if status == "" {
		status = "open"
	}
	priority := rawString(raw, "priority")
	if priority == "" {
		priority = priorityFromNumber(raw, "priority")
	}
	return normalizeSnapshot(&TaskSnapshot{ID: rawString(raw, "id"), Title: rawString(raw, "title"), Description: firstRawString(raw, "description", "body"), Status: status, Priority: priority, Epic: rawString(raw, "epic"), Assignee: rawString(raw, "assignee"), Labels: rawStrings(raw, "labels"), DependsOn: firstRawStrings(raw, "depends_on", "dependsOn"), Created: firstRawString(raw, "created", "created_at"), Updated: firstRawString(raw, "updated", "updated_at"), Metadata: meta})
}

func normalizeSnapshot(s *TaskSnapshot) *TaskSnapshot {
	if s == nil {
		return nil
	}
	out := *s
	out.ID = strings.TrimSpace(out.ID)
	out.Title = strings.TrimSpace(out.Title)
	out.Status = strings.TrimSpace(strings.ToLower(out.Status))
	if out.Status == "" {
		out.Status = "open"
	}
	out.Priority = strings.ToUpper(strings.TrimSpace(out.Priority))
	if out.Priority != "" && !strings.HasPrefix(out.Priority, "P") {
		out.Priority = "P" + out.Priority
	}
	out.Labels = cleanStrings(out.Labels)
	out.DependsOn = cleanStrings(out.DependsOn)
	out.Metadata = copyStringMap(out.Metadata)
	return &out
}

func snapshotsEqual(a, b *TaskSnapshot) bool {
	return reflect.DeepEqual(snapshotComparable(a), snapshotComparable(b))
}

func snapshotComparable(s *TaskSnapshot) TaskSnapshot {
	if s == nil {
		return TaskSnapshot{}
	}
	n := normalizeSnapshot(s)
	n.Metadata = nil
	return *n
}

func materialChangedFields(a, b *TaskSnapshot) []string {
	av := snapshotComparable(a)
	bv := snapshotComparable(b)
	fields := []struct {
		name string
		eq   bool
	}{
		{"title", av.Title == bv.Title},
		{"description", av.Description == bv.Description},
		{"status", av.Status == bv.Status},
		{"priority", av.Priority == bv.Priority},
		{"epic", av.Epic == bv.Epic},
		{"assignee", av.Assignee == bv.Assignee},
		{"labels", reflect.DeepEqual(av.Labels, bv.Labels)},
		{"depends_on", reflect.DeepEqual(av.DependsOn, bv.DependsOn)},
	}
	var out []string
	for _, f := range fields {
		if !f.eq {
			out = append(out, f.name)
		}
	}
	return out
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
