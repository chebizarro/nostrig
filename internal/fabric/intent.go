package fabric

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	beadspb "github.com/chebizarro/nostrig/gen/beads"
	fn "github.com/chebizarro/nostrig/internal/nostr"
	gonostr "github.com/nbd-wtf/go-nostr"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type intentEnvelope struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  intentParams    `json:"params"`
}

type intentParams struct {
	ID          string   `json:"id"`
	Title       *string  `json:"title,omitempty"`
	Description *string  `json:"description,omitempty"`
	Status      *string  `json:"status,omitempty"`
	Priority    *string  `json:"priority,omitempty"`
	Assignee    *string  `json:"assignee,omitempty"`
	Epic        *string  `json:"epic,omitempty"`
	Labels      []string `json:"labels,omitempty"`
	DependsOn   []string `json:"depends_on,omitempty"`
}

// ApplyIntent verifies and applies a canonical ContextVM task mutation. The
// caller persists/publishes the returned model as a new 30900 projection.
func ApplyIntent(export *beadspb.Export, ev *gonostr.Event, recipient string) (*beadspb.Export, string, error) {
	if export == nil || ev == nil {
		return nil, "", fmt.Errorf("ledger and intent are required")
	}
	if ev.Kind != fn.KindIntent {
		return nil, "", fmt.Errorf("unexpected intent kind %d", ev.Kind)
	}
	ok, err := ev.CheckSignature()
	if err != nil || !ok {
		return nil, "", fmt.Errorf("invalid intent signature")
	}
	if recipient != "" && !hasTag(ev, "p", recipient) {
		return nil, "", fmt.Errorf("intent is not addressed to recipient")
	}
	var env intentEnvelope
	if err := json.Unmarshal([]byte(ev.Content), &env); err != nil {
		return nil, "", fmt.Errorf("decode intent: %w", err)
	}
	if env.JSONRPC != "2.0" || len(env.ID) == 0 || env.Method == "" || env.Params.ID == "" {
		return nil, "", fmt.Errorf("invalid ContextVM envelope")
	}
	if method, ok := tagValue(ev, "method"); !ok || method != env.Method {
		return nil, "", fmt.Errorf("intent method tag/content mismatch")
	}
	switch env.Method {
	case "task/claim", "task/assign", "task/update", "task/close":
	default:
		return nil, "", fmt.Errorf("unsupported task method %q", env.Method)
	}

	issue := findIssue(export, env.Params.ID)
	if issue == nil {
		return nil, "", fmt.Errorf("task %q not found", env.Params.ID)
	}
	switch env.Method {
	case "task/claim":
		if issue.Assignee != "" && issue.Assignee != ev.PubKey {
			return nil, "", fmt.Errorf("task %q is already assigned", issue.Id)
		}
		issue.Assignee = ev.PubKey
		issue.Status = beadspb.Status_STATUS_IN_PROGRESS
	case "task/assign":
		if env.Params.Assignee == nil || strings.TrimSpace(*env.Params.Assignee) == "" {
			return nil, "", fmt.Errorf("task/assign requires assignee")
		}
		issue.Assignee = *env.Params.Assignee
	case "task/update":
		applyUpdate(issue, env.Params)
	case "task/close":
		issue.Status = beadspb.Status_STATUS_CLOSED
	}
	issue.Updated = timestamppb.New(time.Unix(int64(ev.CreatedAt), 0).UTC())
	return export, issue.Id, nil
}

func applyUpdate(issue *beadspb.Issue, p intentParams) {
	if p.Title != nil {
		issue.Title = *p.Title
	}
	if p.Description != nil {
		issue.Description = *p.Description
	}
	if p.Assignee != nil {
		issue.Assignee = *p.Assignee
	}
	if p.Epic != nil {
		issue.Epic = *p.Epic
	}
	if p.Labels != nil {
		issue.Labels = append([]string(nil), p.Labels...)
	}
	if p.DependsOn != nil {
		issue.DependsOn = append([]string(nil), p.DependsOn...)
	}
	if p.Status != nil {
		issue.Status = parseStatus(*p.Status)
	}
	if p.Priority != nil {
		issue.Priority = parsePriority(*p.Priority)
	}
}

func parseStatus(v string) beadspb.Status {
	switch strings.ToLower(strings.TrimPrefix(v, "STATUS_")) {
	case "open":
		return beadspb.Status_STATUS_OPEN
	case "in_progress", "in-progress":
		return beadspb.Status_STATUS_IN_PROGRESS
	case "blocked":
		return beadspb.Status_STATUS_BLOCKED
	case "closed":
		return beadspb.Status_STATUS_CLOSED
	default:
		return beadspb.Status_STATUS_UNSPECIFIED
	}
}

func parsePriority(v string) beadspb.Priority {
	switch strings.ToUpper(strings.TrimPrefix(v, "PRIORITY_")) {
	case "P0":
		return beadspb.Priority_PRIORITY_P0
	case "P1":
		return beadspb.Priority_PRIORITY_P1
	case "P2":
		return beadspb.Priority_PRIORITY_P2
	case "P3":
		return beadspb.Priority_PRIORITY_P3
	case "P4":
		return beadspb.Priority_PRIORITY_P4
	default:
		return beadspb.Priority_PRIORITY_UNSPECIFIED
	}
}

func findIssue(export *beadspb.Export, id string) *beadspb.Issue {
	for _, issue := range export.Issues {
		if issue != nil && issue.Id == id {
			return issue
		}
	}
	return nil
}

func hasTag(ev *gonostr.Event, key, value string) bool {
	got, ok := tagValue(ev, key)
	return ok && got == value
}

func tagValue(ev *gonostr.Event, key string) (string, bool) {
	for _, tag := range ev.Tags {
		if len(tag) > 1 && tag[0] == key {
			return tag[1], true
		}
	}
	return "", false
}
