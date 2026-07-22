package taskfabric

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	gonostr "fiatjaf.com/nostr"
	casnostr "git.sharegap.net/cascadia/cascadia-go/nostr"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
)

const (
	KindPSTFGateStatus = 30315
	KindPSTFAudit      = 4903

	QualityPassing = "passing"
	QualityFailing = "failing"
	QualityPending = "pending"
)

// QualityResult is the task-fabric view of PSTF/Harbormaster gate state.
type QualityResult struct {
	State       string   `json:"state"`
	Result      string   `json:"result,omitempty"`
	Reason      string   `json:"reason,omitempty"`
	BlocksMerge bool     `json:"blocks_merge,omitempty"`
	Project     string   `json:"project,omitempty"`
	EventID     string   `json:"event_id,omitempty"`
	EventKind   int      `json:"event_kind,omitempty"`
	CheckedAt   string   `json:"checked_at,omitempty"`
	Findings    []string `json:"findings,omitempty"`
	Author      string   `json:"author,omitempty"`
}

// QualityLookup supplies task quality annotations for ContextVM responses.
type QualityLookup interface {
	GetQuality(ctx context.Context, taskIDs []string) (map[string]QualityResult, error)
}

// RelayQualitySource projects PSTF 30315 operational status and 4903 audit
// events from the relay into per-task quality annotations.
type RelayQualitySource struct {
	Relays  []string
	Project string
	Authors []string
	Limit   int
	Client  *nip34.Client
}

func (s *RelayQualitySource) GetQuality(ctx context.Context, taskIDs []string) (map[string]QualityResult, error) {
	ids := cleanStrings(taskIDs)
	out := pendingQuality(ids)
	events, err := s.FetchQualityEvents(ctx)
	if err != nil {
		return nil, err
	}
	state, err := ProjectQualityState(events, s.Authors, s.Project)
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		if q, ok := state.Tasks[id]; ok {
			out[id] = q
			continue
		}
		if state.Project.State != "" {
			out[id] = state.Project
		}
	}
	return out, nil
}

// FetchQualityEvents fetches PSTF quality/audit events for the configured trusted
// authors. Project scoping is applied locally by ProjectQualityState because
// NIP-01 relay tag filters are interoperable only for single-letter tag names;
// relying on a non-standard #project filter silently omits events on strict relays.
// It is a pull projection used by command handlers; relays remain the source of truth.
func (s *RelayQualitySource) FetchQualityEvents(ctx context.Context) ([]*gonostr.Event, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is nil")
	}
	relays := cleanStrings(s.Relays)
	if len(relays) == 0 {
		return nil, fmt.Errorf("at least one relay is required")
	}
	project := strings.TrimSpace(s.Project)
	if project == "" {
		return nil, fmt.Errorf("quality project is required")
	}
	limit := s.Limit
	if limit <= 0 {
		limit = nip34.DefaultQueryPageSize
	}
	authors, trusted, err := trustedQualityAuthors(s.Authors)
	if err != nil {
		return nil, err
	}
	filter := gonostr.Filter{Kinds: []gonostr.Kind{KindPSTFGateStatus, KindPSTFAudit}, Authors: authors, Limit: limit}
	client := s.Client
	if client == nil {
		client = nip34.NewClient()
	}
	events, err := client.FetchManyPaginated(ctx, relays, []gonostr.Filter{filter}, nip34.PaginationOptions{
		PageSize: limit, MaxPages: nip34.DefaultQueryMaxPages, MaxEvents: nip34.DefaultQueryMaxEvents,
	})
	if err != nil {
		return nil, err
	}
	return filterTrustedQualityEvents(events, trusted), nil
}

// QualityState is the latest PSTF gate state, split between project-wide state
// and task-specific overrides when PSTF events include task tags.
type QualityState struct {
	Project QualityResult            `json:"project,omitempty"`
	Tasks   map[string]QualityResult `json:"tasks,omitempty"`
}

// ProjectQualityState collapses PSTF gate/audit history to latest quality state.
func ProjectQualityState(events []*gonostr.Event, authors []string, project string) (QualityState, error) {
	_, trusted, err := trustedQualityAuthors(authors)
	if err != nil {
		return QualityState{}, err
	}
	project = strings.TrimSpace(project)
	if project == "" {
		return QualityState{}, fmt.Errorf("quality project is required")
	}
	state := QualityState{Tasks: map[string]QualityResult{}}
	var projectEvent *gonostr.Event
	taskEvents := map[string]*gonostr.Event{}
	for _, ev := range events {
		if ev == nil {
			continue
		}
		if _, ok := trusted[ev.PubKey.Hex()]; !ok || !casnostr.VerifyEvent((*casnostr.Event)(ev)) {
			continue
		}
		q, taskIDs, _, ok := parseQualityEvent(ev)
		if !ok || q.Project != project {
			continue
		}
		if len(taskIDs) == 0 {
			if projectEvent == nil || qualityEventAfter(ev, projectEvent) {
				state.Project = q
				projectEvent = ev
			}
			continue
		}
		for _, id := range taskIDs {
			if previous := taskEvents[id]; previous == nil || qualityEventAfter(ev, previous) {
				state.Tasks[id] = q
				taskEvents[id] = ev
			}
		}
	}
	return state, nil
}

func parseQualityEvent(ev *gonostr.Event) (QualityResult, []string, time.Time, bool) {
	if ev == nil || (int(ev.Kind) != KindPSTFGateStatus && int(ev.Kind) != KindPSTFAudit) {
		return QualityResult{}, nil, time.Time{}, false
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(ev.Content), &fields); err != nil {
		return QualityResult{}, nil, time.Time{}, false
	}
	schema, ok := requiredJSONString(fields, "schema_version")
	if !ok {
		return QualityResult{}, nil, time.Time{}, false
	}
	project, ok := requiredJSONString(fields, "project")
	if !ok || !exactQualityTag(ev, "project", project) {
		return QualityResult{}, nil, time.Time{}, false
	}
	reason, ok := requiredJSONString(fields, "reason")
	if !ok {
		return QualityResult{}, nil, time.Time{}, false
	}
	blocksMerge, ok := requiredJSONBool(fields, "blocks_merge")
	if !ok {
		return QualityResult{}, nil, time.Time{}, false
	}
	if _, present := fields["findings"]; !present {
		return QualityResult{}, nil, time.Time{}, false
	}
	var findings []struct {
		ID          string   `json:"id"`
		Severity    string   `json:"severity"`
		Status      string   `json:"status"`
		Title       string   `json:"title"`
		Evidence    []string `json:"evidence"`
		BlocksMerge *bool    `json:"blocks_merge"`
	}
	if err := json.Unmarshal(fields["findings"], &findings); err != nil || findings == nil {
		return QualityResult{}, nil, time.Time{}, false
	}

	resultField, resultTag, wantSchema := "result", "status", "pstf.status.gate.v1"
	if int(ev.Kind) == KindPSTFAudit {
		resultField, resultTag, wantSchema = "decision", "decision", "pstf.audit.gate_decision.v1"
		if !exactQualityTag(ev, "audit_type", "CAS_AUDIT") || !hasQualityTagPrefix(ev, "d", "pstf:gate-decision:"+project+":") {
			return QualityResult{}, nil, time.Time{}, false
		}
	} else if !exactQualityTag(ev, "d", "pstf:gate:"+project) {
		return QualityResult{}, nil, time.Time{}, false
	}
	result, ok := requiredJSONString(fields, resultField)
	if !ok || schema != wantSchema || !exactQualityTag(ev, resultTag, result) {
		return QualityResult{}, nil, time.Time{}, false
	}
	state := qualityState(result)
	if state == QualityPending {
		return QualityResult{}, nil, time.Time{}, false
	}

	eventTime := nip34.EventTime(ev)
	q := QualityResult{
		State: state, Result: result, Reason: reason, BlocksMerge: blocksMerge, Project: project,
		EventID: ev.ID.Hex(), EventKind: int(ev.Kind), CheckedAt: eventTime.UTC().Format(time.RFC3339), Author: ev.PubKey.Hex(),
	}
	for _, finding := range findings {
		if !validQualityFinding(finding.ID, finding.Severity, finding.Status, finding.Title, finding.Evidence, finding.BlocksMerge) {
			return QualityResult{}, nil, time.Time{}, false
		}
		label := strings.TrimSpace(finding.ID)
		if finding.Title != "" {
			if label != "" {
				label += ": "
			}
			label += finding.Title
		}
		if label != "" {
			q.Findings = append(q.Findings, label)
		}
		if *finding.BlocksMerge {
			q.BlocksMerge = true
		}
	}
	return q, qualityTaskIDs(ev), eventTime, true
}

func requiredJSONString(fields map[string]json.RawMessage, name string) (string, bool) {
	raw, ok := fields[name]
	if !ok {
		return "", false
	}
	var value string
	if json.Unmarshal(raw, &value) != nil {
		return "", false
	}
	value = strings.TrimSpace(value)
	return value, value != ""
}

func requiredJSONBool(fields map[string]json.RawMessage, name string) (bool, bool) {
	raw, ok := fields[name]
	if !ok {
		return false, false
	}
	var value bool
	if json.Unmarshal(raw, &value) != nil {
		return false, false
	}
	return value, true
}

func exactQualityTag(ev *gonostr.Event, name, value string) bool {
	values := nip34.TagAll(ev, name)
	return len(values) == 1 && strings.TrimSpace(values[0]) == strings.TrimSpace(value)
}

func hasQualityTagPrefix(ev *gonostr.Event, name, prefix string) bool {
	values := nip34.TagAll(ev, name)
	return len(values) == 1 && strings.HasPrefix(strings.TrimSpace(values[0]), prefix) &&
		len(strings.TrimSpace(values[0])) > len(prefix)
}

func validQualityFinding(id, severity, status, title string, evidence []string, blocksMerge *bool) bool {
	if strings.TrimSpace(id) == "" || strings.TrimSpace(title) == "" || evidence == nil || blocksMerge == nil {
		return false
	}
	switch strings.TrimSpace(severity) {
	case "info", "minor", "major", "critical":
	default:
		return false
	}
	switch strings.TrimSpace(status) {
	case "suspected", "confirmed":
		return true
	default:
		return false
	}
}

func qualityEventAfter(a, b *gonostr.Event) bool {
	at, bt := nip34.EventTime(a), nip34.EventTime(b)
	if !at.Equal(bt) {
		return at.After(bt)
	}
	return a.ID.Hex() > b.ID.Hex()
}

func qualityState(result string) string {
	switch strings.ToLower(strings.TrimSpace(result)) {
	case "pass", "passed", "passing", "ok", "success":
		return QualityPassing
	case "fail", "failed", "failing", "error":
		return QualityFailing
	default:
		return QualityPending
	}
}

func qualityTaskIDs(ev *gonostr.Event) []string {
	seen := map[string]struct{}{}
	var ids []string
	add := func(id string) {
		id = strings.TrimPrefix(strings.TrimSpace(id), "task:")
		if id == "" {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	for _, tag := range ev.Tags {
		if len(tag) < 2 {
			continue
		}
		switch tag[0] {
		case "task":
			add(tag[1])
		case "a":
			if strings.Contains(tag[1], "task:") {
				parts := strings.Split(tag[1], "task:")
				add(parts[len(parts)-1])
			}
		}
	}
	sort.Strings(ids)
	return ids
}

func filterTrustedQualityEvents(events []*gonostr.Event, trusted map[string]struct{}) []*gonostr.Event {
	filtered := make([]*gonostr.Event, 0, len(events))
	for _, event := range events {
		if event == nil {
			continue
		}
		if _, ok := trusted[event.PubKey.Hex()]; !ok || !casnostr.VerifyEvent((*casnostr.Event)(event)) {
			continue
		}
		filtered = append(filtered, event)
	}
	return filtered
}

func trustedQualityAuthors(values []string) ([]gonostr.PubKey, map[string]struct{}, error) {
	values = cleanStrings(values)
	if len(values) == 0 {
		return nil, nil, fmt.Errorf("at least one trusted quality author is required")
	}
	authors := make([]gonostr.PubKey, 0, len(values))
	trusted := make(map[string]struct{}, len(values))
	for _, value := range values {
		pubkey, err := gonostr.PubKeyFromHex(strings.TrimSpace(value))
		if err != nil {
			return nil, nil, fmt.Errorf("invalid trusted quality author %q", value)
		}
		hex := pubkey.Hex()
		if _, exists := trusted[hex]; exists {
			continue
		}
		trusted[hex] = struct{}{}
		authors = append(authors, pubkey)
	}
	return authors, trusted, nil
}

func pendingQuality(ids []string) map[string]QualityResult {
	out := map[string]QualityResult{}
	for _, id := range cleanStrings(ids) {
		out[id] = QualityResult{State: QualityPending, Result: "pending"}
	}
	return out
}
