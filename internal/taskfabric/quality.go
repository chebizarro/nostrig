package taskfabric

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	gonostr "fiatjaf.com/nostr"
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
	state := ProjectQualityState(events)
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

// FetchQualityEvents fetches PSTF quality/audit events, optionally scoped to a
// project tag. It is a pull projection used by command handlers; relays remain
// the source of truth.
func (s *RelayQualitySource) FetchQualityEvents(ctx context.Context) ([]*gonostr.Event, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is nil")
	}
	relays := cleanStrings(s.Relays)
	if len(relays) == 0 {
		return nil, fmt.Errorf("at least one relay is required")
	}
	limit := s.Limit
	if limit <= 0 {
		limit = 50
	}
	filter := gonostr.Filter{Kinds: []gonostr.Kind{KindPSTFGateStatus, KindPSTFAudit}, Limit: limit}
	if project := strings.TrimSpace(s.Project); project != "" {
		filter.Tags = gonostr.TagMap{"project": []string{project}}
	}
	client := s.Client
	if client == nil {
		client = nip34.NewClient()
	}
	return client.Fetch(ctx, relays, filter)
}

// QualityState is the latest PSTF gate state, split between project-wide state
// and task-specific overrides when PSTF events include task tags.
type QualityState struct {
	Project QualityResult            `json:"project,omitempty"`
	Tasks   map[string]QualityResult `json:"tasks,omitempty"`
}

// ProjectQualityState collapses PSTF gate/audit history to latest quality state.
func ProjectQualityState(events []*gonostr.Event) QualityState {
	state := QualityState{Tasks: map[string]QualityResult{}}
	var projectTime time.Time
	taskTimes := map[string]time.Time{}
	for _, ev := range events {
		q, taskIDs, eventTime, ok := parseQualityEvent(ev)
		if !ok {
			continue
		}
		if len(taskIDs) == 0 {
			if state.Project.State == "" || eventTime.After(projectTime) {
				state.Project = q
				projectTime = eventTime
			}
			continue
		}
		for _, id := range taskIDs {
			if prev, ok := taskTimes[id]; !ok || eventTime.After(prev) {
				state.Tasks[id] = q
				taskTimes[id] = eventTime
			}
		}
	}
	return state
}

func parseQualityEvent(ev *gonostr.Event) (QualityResult, []string, time.Time, bool) {
	if ev == nil || (int(ev.Kind) != KindPSTFGateStatus && int(ev.Kind) != KindPSTFAudit) {
		return QualityResult{}, nil, time.Time{}, false
	}
	eventTime := nip34.EventTime(ev)
	var body struct {
		SchemaVersion string `json:"schema_version"`
		Project       string `json:"project"`
		Result        string `json:"result"`
		Decision      string `json:"decision"`
		Reason        string `json:"reason"`
		BlocksMerge   bool   `json:"blocks_merge"`
		Findings      []struct {
			ID          string `json:"id"`
			Severity    string `json:"severity"`
			Status      string `json:"status"`
			Title       string `json:"title"`
			BlocksMerge bool   `json:"blocks_merge"`
		} `json:"findings"`
	}
	_ = json.Unmarshal([]byte(ev.Content), &body)
	result := strings.TrimSpace(body.Result)
	if result == "" {
		result = strings.TrimSpace(body.Decision)
	}
	if result == "" {
		result, _ = nip34.TagFirst(ev, "status")
	}
	if result == "" {
		result, _ = nip34.TagFirst(ev, "decision")
	}
	project := strings.TrimSpace(body.Project)
	if project == "" {
		project, _ = nip34.TagFirst(ev, "project")
	}
	q := QualityResult{State: qualityState(result), Result: result, Reason: body.Reason, BlocksMerge: body.BlocksMerge, Project: project, EventID: ev.ID.Hex(), EventKind: int(ev.Kind), CheckedAt: eventTime.UTC().Format(time.RFC3339)}
	if q.State == QualityPending {
		q.Result = "pending"
	}
	for _, f := range body.Findings {
		label := strings.TrimSpace(f.ID)
		if f.Title != "" {
			if label != "" {
				label += ": "
			}
			label += f.Title
		}
		if label != "" {
			q.Findings = append(q.Findings, label)
		}
		if f.BlocksMerge {
			q.BlocksMerge = true
		}
	}
	return q, qualityTaskIDs(ev), eventTime, true
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

func pendingQuality(ids []string) map[string]QualityResult {
	out := map[string]QualityResult{}
	for _, id := range cleanStrings(ids) {
		out[id] = QualityResult{State: QualityPending, Result: "pending"}
	}
	return out
}
