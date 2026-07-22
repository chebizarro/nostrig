package taskmodel

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"path"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	pb "github.com/chebizarro/nostrig/gen/beads"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const CurrentSchema = "cascadia.task-state.v2"

type IssueDocument struct {
	SchemaVersion      string                 `json:"schema_version,omitempty"`
	RecordType         string                 `json:"_type,omitempty"`
	ID                 string                 `json:"id"`
	Title              string                 `json:"title"`
	Description        string                 `json:"description,omitempty"`
	Status             string                 `json:"status"`
	Priority           string                 `json:"-"`
	IssueType          string                 `json:"issue_type,omitempty"`
	Assignee           string                 `json:"assignee,omitempty"`
	Owner              string                 `json:"owner,omitempty"`
	Created            string                 `json:"created_at,omitempty"`
	CreatedBy          string                 `json:"created_by,omitempty"`
	Updated            string                 `json:"updated_at,omitempty"`
	Started            string                 `json:"started_at,omitempty"`
	Claimed            string                 `json:"claimed_at,omitempty"`
	Blocked            string                 `json:"blocked_at,omitempty"`
	Reviewed           string                 `json:"reviewed_at,omitempty"`
	Closed             string                 `json:"closed_at,omitempty"`
	CloseReason        string                 `json:"close_reason,omitempty"`
	StatusReason       string                 `json:"status_reason,omitempty"`
	BlockerDescription string                 `json:"blocker_description,omitempty"`
	AcceptanceCriteria string                 `json:"acceptance_criteria,omitempty"`
	Notes              string                 `json:"notes,omitempty"`
	Labels             []string               `json:"labels,omitempty"`
	DependsOn          []string               `json:"depends_on,omitempty"`
	Dependencies       []DependencyDocument   `json:"dependencies,omitempty"`
	Comments           []CommentDocument      `json:"comments,omitempty"`
	DependencyCount    *int32                 `json:"dependency_count,omitempty"`
	DependentCount     *int32                 `json:"dependent_count,omitempty"`
	CommentCount       *int32                 `json:"comment_count,omitempty"`
	Checkpoints        []CheckpointDocument   `json:"checkpoints,omitempty"`
	Branch             string                 `json:"branch,omitempty"`
	Commits            []string               `json:"commits,omitempty"`
	Patches            []ArtifactDocument     `json:"patches,omitempty"`
	PullRequests       []string               `json:"pull_requests,omitempty"`
	Evidence           []ArtifactDocument     `json:"evidence,omitempty"`
	Review             *ReviewDocument        `json:"review,omitempty"`
	QualityGate        *QualityGateDocument   `json:"quality_gate,omitempty"`
	Project            string                 `json:"project,omitempty"`
	Epic               string                 `json:"epic,omitempty"`
	Queue              string                 `json:"queue,omitempty"`
	Repository         string                 `json:"repository,omitempty"`
	ExecutionAttempts  []ExecutionDocument    `json:"execution_attempts,omitempty"`
	AgentSessions      []AgentSessionDocument `json:"agent_sessions,omitempty"`
	Metadata           map[string]string      `json:"metadata,omitempty"`
}

type DependencyDocument struct {
	IssueID     string `json:"issue_id"`
	DependsOnID string `json:"depends_on_id"`
	Type        string `json:"type"`
	Created     string `json:"created_at,omitempty"`
	CreatedBy   string `json:"created_by,omitempty"`
	Metadata    string `json:"metadata,omitempty"`
}

type CommentDocument struct {
	ID      string `json:"id"`
	IssueID string `json:"issue_id"`
	Author  string `json:"author,omitempty"`
	Text    string `json:"text"`
	Created string `json:"created_at,omitempty"`
}

type ArtifactDocument struct {
	Kind      string `json:"kind,omitempty"`
	URL       string `json:"url"`
	SHA256    string `json:"sha256"`
	MediaType string `json:"media_type,omitempty"`
	SizeBytes uint64 `json:"size_bytes,omitempty"`
	Name      string `json:"name,omitempty"`
	Reference string `json:"reference,omitempty"`
}

type CheckpointDocument struct {
	ID       string             `json:"id"`
	Actor    string             `json:"actor,omitempty"`
	Status   string             `json:"status,omitempty"`
	Summary  string             `json:"summary"`
	Created  string             `json:"created_at,omitempty"`
	Evidence []ArtifactDocument `json:"evidence,omitempty"`
}

type ReviewDocument struct {
	Required     bool     `json:"required,omitempty"`
	Requirements []string `json:"requirements,omitempty"`
	Reviewer     string   `json:"reviewer,omitempty"`
	State        string   `json:"state,omitempty"`
}

type QualityGateDocument struct {
	Required bool               `json:"required,omitempty"`
	State    string             `json:"state,omitempty"`
	Source   string             `json:"source,omitempty"`
	Reason   string             `json:"reason,omitempty"`
	Checked  string             `json:"checked_at,omitempty"`
	Evidence []ArtifactDocument `json:"evidence,omitempty"`
}

type ExecutionDocument struct {
	ID           string             `json:"id"`
	Agent        string             `json:"agent,omitempty"`
	AgentSession string             `json:"agent_session,omitempty"`
	Status       string             `json:"status,omitempty"`
	Started      string             `json:"started_at,omitempty"`
	Ended        string             `json:"ended_at,omitempty"`
	Branch       string             `json:"branch,omitempty"`
	Commits      []string           `json:"commits,omitempty"`
	Patches      []ArtifactDocument `json:"patches,omitempty"`
	PullRequests []string           `json:"pull_requests,omitempty"`
	Evidence     []ArtifactDocument `json:"evidence,omitempty"`
	StatusReason string             `json:"status_reason,omitempty"`
}

type AgentSessionDocument struct {
	ID         string            `json:"id"`
	Agent      string            `json:"agent,omitempty"`
	Status     string            `json:"status,omitempty"`
	Started    string            `json:"started_at,omitempty"`
	Ended      string            `json:"ended_at,omitempty"`
	Transcript *ArtifactDocument `json:"transcript,omitempty"`
}

type issueAlias IssueDocument

func (d IssueDocument) MarshalJSON() ([]byte, error) {
	var priority *int32
	if d.Priority != "" {
		n, err := PriorityNumber(d.Priority)
		if err != nil {
			return nil, err
		}
		priority = &n
	}
	return json.Marshal(struct {
		*issueAlias
		Priority *int32 `json:"priority,omitempty"`
	}{issueAlias: (*issueAlias)(&d), Priority: priority})
}

func (d *IssueDocument) UnmarshalJSON(data []byte) error {
	if d == nil {
		return fmt.Errorf("nil issue document")
	}
	wire := struct {
		*issueAlias
		Priority *json.RawMessage `json:"priority,omitempty"`
	}{issueAlias: (*issueAlias)(d)}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&wire); err != nil {
		return err
	}
	if wire.Priority != nil {
		var n int32
		if err := json.Unmarshal(*wire.Priority, &n); err == nil {
			d.Priority = fmt.Sprintf("P%d", n)
		} else {
			var s string
			if err := json.Unmarshal(*wire.Priority, &s); err != nil {
				return fmt.Errorf("priority must be an integer or P-level string")
			}
			d.Priority = strings.ToUpper(strings.TrimSpace(s))
		}
	}
	return nil
}

func DecodeCanonical(data []byte) (*IssueDocument, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	allowed := map[string]struct{}{
		"schema_version": {}, "id": {}, "title": {}, "description": {}, "status": {}, "priority": {},
		"issue_type": {}, "assignee": {}, "owner": {}, "created_at": {}, "created_by": {}, "updated_at": {},
		"started_at": {}, "claimed_at": {}, "blocked_at": {}, "reviewed_at": {}, "closed_at": {},
		"close_reason": {}, "status_reason": {}, "blocker_description": {}, "acceptance_criteria": {}, "notes": {}, "labels": {},
		"dependencies": {}, "comments": {}, "dependency_count": {}, "dependent_count": {}, "comment_count": {},
		"checkpoints": {}, "branch": {}, "commits": {}, "patches": {}, "pull_requests": {}, "evidence": {},
		"review": {}, "quality_gate": {}, "project": {}, "epic": {}, "queue": {}, "repository": {},
		"execution_attempts": {}, "agent_sessions": {}, "metadata": {},
	}
	for key := range raw {
		if _, ok := allowed[key]; !ok {
			return nil, fmt.Errorf("unknown canonical task field %q", key)
		}
	}
	if p, ok := raw["priority"]; ok {
		var n int32
		if err := json.Unmarshal(p, &n); err != nil {
			return nil, fmt.Errorf("canonical priority must be numeric")
		}
	}
	var doc IssueDocument
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&doc); err != nil {
		return nil, err
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf("trailing JSON")
	}
	if doc.SchemaVersion != CurrentSchema {
		return nil, fmt.Errorf("unsupported content schema_version %q", doc.SchemaVersion)
	}
	doc.RecordType = ""
	return Normalize(&doc)
}

func DecodeBeads(data []byte) (*IssueDocument, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	alias(raw, "description", "body")
	alias(raw, "created_at", "created")
	alias(raw, "updated_at", "updated")
	alias(raw, "claimed_at", "claimed")
	alias(raw, "blocked_at", "blocked")
	alias(raw, "reviewed_at", "reviewed")
	alias(raw, "closed_at", "closed")
	if _, ok := raw["depends_on"]; !ok {
		if v, exists := raw["dependsOn"]; exists {
			raw["depends_on"] = v
		}
	}
	delete(raw, "body")
	delete(raw, "created")
	delete(raw, "updated")
	delete(raw, "claimed")
	delete(raw, "blocked")
	delete(raw, "reviewed")
	delete(raw, "closed")
	delete(raw, "dependsOn")
	canonical, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}
	var doc IssueDocument
	if err := json.Unmarshal(canonical, &doc); err != nil {
		return nil, err
	}
	doc.SchemaVersion = ""
	doc.RecordType = "issue"
	return Normalize(&doc)
}

func alias(raw map[string]json.RawMessage, canonical, legacy string) {
	if _, ok := raw[canonical]; ok {
		return
	}
	if v, ok := raw[legacy]; ok {
		raw[canonical] = v
	}
}

func EncodeCanonical(doc *IssueDocument) ([]byte, error) {
	n, err := Normalize(doc)
	if err != nil {
		return nil, err
	}
	n.SchemaVersion = CurrentSchema
	n.RecordType = ""
	n.DependsOn = nil
	return json.Marshal(n)
}

func EncodeBeads(doc *IssueDocument) ([]byte, error) {
	n, err := Normalize(doc)
	if err != nil {
		return nil, err
	}
	n.SchemaVersion = ""
	n.RecordType = "issue"
	n.DependsOn = nil
	return json.Marshal(n)
}

func Normalize(in *IssueDocument) (*IssueDocument, error) {
	if in == nil {
		return nil, fmt.Errorf("issue is nil")
	}
	raw, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	var d IssueDocument
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, err
	}
	d.ID = strings.TrimSpace(d.ID)
	d.Title = strings.TrimSpace(d.Title)
	d.Status = strings.ToLower(strings.TrimSpace(d.Status))
	d.Priority = strings.ToUpper(strings.TrimSpace(d.Priority))
	if d.Status == "" {
		d.Status = "open"
	}
	if d.ID == "" || strings.HasPrefix(d.ID, "task:") {
		return nil, fmt.Errorf("invalid task id")
	}
	if d.Title == "" {
		return nil, fmt.Errorf("task title is required")
	}
	switch d.Status {
	case "open", "in_progress", "blocked", "closed", "deferred":
	default:
		return nil, fmt.Errorf("invalid task status %q", d.Status)
	}
	if d.Priority != "" {
		if _, err := PriorityNumber(d.Priority); err != nil {
			return nil, err
		}
	}
	var errList error
	d.Labels, errList = cleanUnique("labels", d.Labels)
	if errList != nil {
		return nil, errList
	}
	d.Commits, errList = cleanUnique("commits", d.Commits)
	if errList != nil {
		return nil, errList
	}
	d.PullRequests, errList = cleanUnique("pull_requests", d.PullRequests)
	if errList != nil {
		return nil, errList
	}
	d.DependsOn, errList = cleanUnique("depends_on", d.DependsOn)
	if errList != nil {
		return nil, errList
	}
	if len(d.Dependencies) == 0 && len(d.DependsOn) > 0 {
		for _, id := range d.DependsOn {
			d.Dependencies = append(d.Dependencies, DependencyDocument{IssueID: d.ID, DependsOnID: id, Type: "blocks"})
		}
	}
	targets := make([]string, 0, len(d.Dependencies))
	targetSeen := map[string]struct{}{}
	relationSeen := map[string]struct{}{}
	for i := range d.Dependencies {
		dep := &d.Dependencies[i]
		dep.IssueID = strings.TrimSpace(dep.IssueID)
		dep.DependsOnID = strings.TrimSpace(dep.DependsOnID)
		dep.Type = strings.ToLower(strings.TrimSpace(dep.Type))
		if dep.IssueID == "" {
			dep.IssueID = d.ID
		}
		if dep.IssueID != d.ID || dep.DependsOnID == "" {
			return nil, fmt.Errorf("invalid dependency for task %s", d.ID)
		}
		switch dep.Type {
		case "blocks", "blocked-by", "parent-child", "discovered-from":
		default:
			return nil, fmt.Errorf("invalid dependency type %q", dep.Type)
		}
		if dep.Created, err = normalizeTime(dep.Created); err != nil {
			return nil, fmt.Errorf("dependency created_at: %w", err)
		}
		relationKey := dep.DependsOnID + "|" + dep.Type
		if _, exists := relationSeen[relationKey]; exists {
			return nil, fmt.Errorf("duplicate typed dependency %q", relationKey)
		}
		relationSeen[relationKey] = struct{}{}
		if _, exists := targetSeen[dep.DependsOnID]; !exists {
			targetSeen[dep.DependsOnID] = struct{}{}
			targets = append(targets, dep.DependsOnID)
		}
	}
	if len(d.DependsOn) > 0 && !sameSet(d.DependsOn, targets) {
		return nil, fmt.Errorf("legacy depends_on disagrees with dependencies")
	}
	d.DependsOn = targets
	for name, count := range map[string]*int32{
		"dependency_count": d.DependencyCount,
		"dependent_count":  d.DependentCount,
		"comment_count":    d.CommentCount,
	} {
		if count != nil && *count < 0 {
			return nil, fmt.Errorf("%s cannot be negative", name)
		}
	}
	commentIDs := map[string]struct{}{}
	for i := range d.Comments {
		c := &d.Comments[i]
		c.ID, c.IssueID = strings.TrimSpace(c.ID), strings.TrimSpace(c.IssueID)
		if c.IssueID == "" {
			c.IssueID = d.ID
		}
		if c.ID == "" || c.IssueID != d.ID {
			return nil, fmt.Errorf("invalid comment")
		}
		if _, exists := commentIDs[c.ID]; exists {
			return nil, fmt.Errorf("duplicate comment id %q", c.ID)
		}
		commentIDs[c.ID] = struct{}{}
		if c.Created, err = normalizeTime(c.Created); err != nil {
			return nil, fmt.Errorf("comment created_at: %w", err)
		}
	}
	for _, pair := range []struct {
		name string
		ptr  *string
	}{
		{"created_at", &d.Created}, {"updated_at", &d.Updated}, {"started_at", &d.Started},
		{"claimed_at", &d.Claimed}, {"blocked_at", &d.Blocked}, {"reviewed_at", &d.Reviewed}, {"closed_at", &d.Closed},
	} {
		if *pair.ptr, err = normalizeTime(*pair.ptr); err != nil {
			return nil, fmt.Errorf("%s: %w", pair.name, err)
		}
	}
	for i := range d.Checkpoints {
		c := &d.Checkpoints[i]
		c.ID = strings.TrimSpace(c.ID)
		if c.ID == "" || strings.TrimSpace(c.Summary) == "" {
			return nil, fmt.Errorf("checkpoint id and summary are required")
		}
		if c.Created, err = normalizeTime(c.Created); err != nil {
			return nil, fmt.Errorf("checkpoint created_at: %w", err)
		}
		if err := validateArtifacts(c.Evidence, false); err != nil {
			return nil, err
		}
	}
	if err := validateArtifacts(d.Patches, true); err != nil {
		return nil, err
	}
	if err := validateArtifacts(d.Evidence, false); err != nil {
		return nil, err
	}
	if d.QualityGate != nil {
		if d.QualityGate.Checked, err = normalizeTime(d.QualityGate.Checked); err != nil {
			return nil, fmt.Errorf("quality_gate checked_at: %w", err)
		}
		if err := validateArtifacts(d.QualityGate.Evidence, false); err != nil {
			return nil, err
		}
	}
	for i := range d.ExecutionAttempts {
		a := &d.ExecutionAttempts[i]
		if strings.TrimSpace(a.ID) == "" {
			return nil, fmt.Errorf("execution attempt id is required")
		}
		if a.Started, err = normalizeTime(a.Started); err != nil {
			return nil, err
		}
		if a.Ended, err = normalizeTime(a.Ended); err != nil {
			return nil, err
		}
		if err := validateArtifacts(a.Patches, true); err != nil {
			return nil, err
		}
		if err := validateArtifacts(a.Evidence, false); err != nil {
			return nil, err
		}
	}
	for i := range d.AgentSessions {
		s := &d.AgentSessions[i]
		if strings.TrimSpace(s.ID) == "" {
			return nil, fmt.Errorf("agent session id is required")
		}
		if s.Started, err = normalizeTime(s.Started); err != nil {
			return nil, err
		}
		if s.Ended, err = normalizeTime(s.Ended); err != nil {
			return nil, err
		}
		if s.Transcript != nil {
			if err := validateArtifacts([]ArtifactDocument{*s.Transcript}, true); err != nil {
				return nil, err
			}
		}
	}
	if d.Repository == "" && d.Metadata != nil {
		d.Repository = strings.TrimSpace(d.Metadata["nip34.repo_addr"])
	}
	if d.Repository != "" {
		if d.Metadata == nil {
			d.Metadata = map[string]string{}
		}
		if legacy := strings.TrimSpace(d.Metadata["nip34.repo_addr"]); legacy != "" && legacy != d.Repository {
			return nil, fmt.Errorf("repository disagrees with nip34.repo_addr")
		}
		d.Metadata["nip34.repo_addr"] = d.Repository
	}
	return &d, nil
}

func validateArtifacts(refs []ArtifactDocument, requireBlossom bool) error {
	for _, ref := range refs {
		reference := strings.TrimSpace(ref.Reference)
		if reference != "" {
			if requireBlossom {
				return fmt.Errorf("large artifacts must use content-addressed Blossom references")
			}
			if strings.TrimSpace(ref.URL) != "" || strings.TrimSpace(ref.SHA256) != "" {
				return fmt.Errorf("artifact reference and Blossom fields are mutually exclusive")
			}
			continue
		}
		hash := strings.ToLower(strings.TrimSpace(ref.SHA256))
		if len(hash) != 64 {
			return fmt.Errorf("artifact sha256 must be 64 lowercase hex characters")
		}
		if _, err := hex.DecodeString(hash); err != nil || hash != ref.SHA256 {
			return fmt.Errorf("artifact sha256 must be 64 lowercase hex characters")
		}
		u, err := url.Parse(strings.TrimSpace(ref.URL))
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || path.Base(u.Path) != hash {
			return fmt.Errorf("artifact must use a content-addressed Blossom URL ending in its sha256")
		}
	}
	return nil
}

func cleanUnique(name string, values []string) ([]string, error) {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return nil, fmt.Errorf("%s contain an empty value", name)
		}
		if _, ok := seen[value]; ok {
			return nil, fmt.Errorf("%s contain duplicate %q", name, value)
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out, nil
}

func sameSet(a, b []string) bool {
	a, b = append([]string{}, a...), append([]string{}, b...)
	sort.Strings(a)
	sort.Strings(b)
	return reflect.DeepEqual(a, b)
}

func normalizeTime(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	t, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return "", fmt.Errorf("invalid RFC3339 timestamp %q", value)
	}
	return t.UTC().Format(time.RFC3339Nano), nil
}

func PriorityNumber(priority string) (int32, error) {
	switch strings.ToUpper(strings.TrimSpace(priority)) {
	case "P0":
		return 0, nil
	case "P1":
		return 1, nil
	case "P2":
		return 2, nil
	case "P3":
		return 3, nil
	case "P4":
		return 4, nil
	case "P9":
		return 9, nil
	default:
		return 0, fmt.Errorf("invalid task priority %q", priority)
	}
}

func FromProto(issue *pb.Issue) (*IssueDocument, error) {
	if issue == nil {
		return nil, fmt.Errorf("issue is nil")
	}
	d := &IssueDocument{
		ID: issue.Id, Title: issue.Title, Description: issue.Description,
		Status: StatusString(issue.Status), Priority: PriorityString(issue.Priority),
		IssueType: issue.IssueType, Assignee: issue.Assignee, Owner: issue.Owner,
		Created: ts(issue.Created), CreatedBy: issue.CreatedBy, Updated: ts(issue.Updated),
		Started: ts(issue.StartedAt), Claimed: ts(issue.ClaimedAt), Blocked: ts(issue.BlockedAt),
		Reviewed: ts(issue.ReviewedAt), Closed: ts(issue.ClosedAt), CloseReason: issue.CloseReason, StatusReason: issue.StatusReason,
		BlockerDescription: issue.BlockerDescription, AcceptanceCriteria: issue.AcceptanceCriteria,
		Notes: issue.Notes, Labels: append([]string{}, issue.Labels...), DependsOn: append([]string{}, issue.DependsOn...),
		DependencyCount: issue.DependencyCount, DependentCount: issue.DependentCount, CommentCount: issue.CommentCount,
		Branch: issue.Branch, Commits: append([]string{}, issue.Commits...), PullRequests: append([]string{}, issue.PullRequests...),
		Project: issue.Project, Epic: issue.Epic, Queue: issue.Queue, Repository: issue.Repository,
	}
	if issue.Metadata != nil {
		d.Metadata = map[string]string{}
		for k, v := range issue.Metadata.Custom {
			d.Metadata[k] = v
		}
		if issue.Metadata.JiraKey != "" {
			d.Metadata["jiraKey"] = issue.Metadata.JiraKey
		}
		if issue.Metadata.JiraId != "" {
			d.Metadata["jiraId"] = issue.Metadata.JiraId
		}
		if issue.Metadata.JiraIssueType != "" {
			d.Metadata["jiraIssueType"] = issue.Metadata.JiraIssueType
		}
		if len(issue.Metadata.Repositories) > 0 {
			d.Metadata["repositories"] = strings.Join(issue.Metadata.Repositories, ",")
		}
	}
	for _, dep := range issue.Dependencies {
		if dep != nil {
			d.Dependencies = append(d.Dependencies, DependencyDocument{IssueID: dep.IssueId, DependsOnID: dep.DependsOnId, Type: dep.Type, Created: ts(dep.CreatedAt), CreatedBy: dep.CreatedBy, Metadata: dep.Metadata})
		}
	}
	for _, c := range issue.Comments {
		if c != nil {
			d.Comments = append(d.Comments, CommentDocument{ID: c.Id, IssueID: c.IssueId, Author: c.Author, Text: c.Text, Created: ts(c.CreatedAt)})
		}
	}
	for _, c := range issue.Checkpoints {
		if c != nil {
			d.Checkpoints = append(d.Checkpoints, CheckpointDocument{ID: c.Id, Actor: c.Actor, Status: c.Status, Summary: c.Summary, Created: ts(c.CreatedAt), Evidence: artifactsFromProto(c.Evidence)})
		}
	}
	d.Patches, d.Evidence = artifactsFromProto(issue.Patches), artifactsFromProto(issue.Evidence)
	if issue.Review != nil {
		d.Review = &ReviewDocument{Required: issue.Review.Required, Requirements: append([]string{}, issue.Review.Requirements...), Reviewer: issue.Review.Reviewer, State: issue.Review.State}
	}
	if q := issue.QualityGate; q != nil {
		d.QualityGate = &QualityGateDocument{Required: q.Required, State: q.State, Source: q.Source, Reason: q.Reason, Checked: ts(q.CheckedAt), Evidence: artifactsFromProto(q.Evidence)}
	}
	for _, a := range issue.ExecutionAttempts {
		if a != nil {
			d.ExecutionAttempts = append(d.ExecutionAttempts, ExecutionDocument{ID: a.Id, Agent: a.Agent, AgentSession: a.AgentSession, Status: a.Status, Started: ts(a.StartedAt), Ended: ts(a.EndedAt), Branch: a.Branch, Commits: append([]string{}, a.Commits...), Patches: artifactsFromProto(a.Patches), PullRequests: append([]string{}, a.PullRequests...), Evidence: artifactsFromProto(a.Evidence), StatusReason: a.StatusReason})
		}
	}
	for _, s := range issue.AgentSessions {
		if s != nil {
			item := AgentSessionDocument{ID: s.Id, Agent: s.Agent, Status: s.Status, Started: ts(s.StartedAt), Ended: ts(s.EndedAt)}
			if s.Transcript != nil {
				ref := artifactFromProto(s.Transcript)
				item.Transcript = &ref
			}
			d.AgentSessions = append(d.AgentSessions, item)
		}
	}
	return Normalize(d)
}

func ToProto(doc *IssueDocument) (*pb.Issue, error) {
	d, err := Normalize(doc)
	if err != nil {
		return nil, err
	}
	issue := &pb.Issue{
		Id: d.ID, Title: d.Title, Description: d.Description, Status: ParseStatus(d.Status), Priority: ParsePriority(d.Priority),
		Epic: d.Epic, Assignee: d.Assignee, Labels: append([]string{}, d.Labels...), DependsOn: append([]string{}, d.DependsOn...),
		Created: pbts(d.Created), Updated: pbts(d.Updated), IssueType: d.IssueType, Owner: d.Owner, CreatedBy: d.CreatedBy,
		StartedAt: pbts(d.Started), CloseReason: d.CloseReason, StatusReason: d.StatusReason, ClosedAt: pbts(d.Closed), AcceptanceCriteria: d.AcceptanceCriteria,
		Notes: d.Notes, DependencyCount: d.DependencyCount, DependentCount: d.DependentCount, CommentCount: d.CommentCount,
		ClaimedAt: pbts(d.Claimed), BlockedAt: pbts(d.Blocked), ReviewedAt: pbts(d.Reviewed),
		BlockerDescription: d.BlockerDescription, Branch: d.Branch, Commits: append([]string{}, d.Commits...),
		PullRequests: append([]string{}, d.PullRequests...), Project: d.Project, Queue: d.Queue, Repository: d.Repository,
		Metadata: &pb.Metadata{Custom: map[string]string{}},
	}
	for k, v := range d.Metadata {
		switch k {
		case "jiraKey":
			issue.Metadata.JiraKey = v
		case "jiraId":
			issue.Metadata.JiraId = v
		case "jiraIssueType":
			issue.Metadata.JiraIssueType = v
		case "repositories":
			if v != "" {
				issue.Metadata.Repositories = strings.Split(v, ",")
			}
		default:
			issue.Metadata.Custom[k] = v
		}
	}
	for _, dep := range d.Dependencies {
		issue.Dependencies = append(issue.Dependencies, &pb.Dependency{IssueId: dep.IssueID, DependsOnId: dep.DependsOnID, Type: dep.Type, CreatedAt: pbts(dep.Created), CreatedBy: dep.CreatedBy, Metadata: dep.Metadata})
	}
	for _, c := range d.Comments {
		issue.Comments = append(issue.Comments, &pb.Comment{Id: c.ID, IssueId: c.IssueID, Author: c.Author, Text: c.Text, CreatedAt: pbts(c.Created)})
	}
	for _, c := range d.Checkpoints {
		issue.Checkpoints = append(issue.Checkpoints, &pb.Checkpoint{Id: c.ID, Actor: c.Actor, Status: c.Status, Summary: c.Summary, CreatedAt: pbts(c.Created), Evidence: artifactsToProto(c.Evidence)})
	}
	issue.Patches, issue.Evidence = artifactsToProto(d.Patches), artifactsToProto(d.Evidence)
	if d.Review != nil {
		issue.Review = &pb.Review{Required: d.Review.Required, Requirements: append([]string{}, d.Review.Requirements...), Reviewer: d.Review.Reviewer, State: d.Review.State}
	}
	if q := d.QualityGate; q != nil {
		issue.QualityGate = &pb.QualityGate{Required: q.Required, State: q.State, Source: q.Source, Reason: q.Reason, CheckedAt: pbts(q.Checked), Evidence: artifactsToProto(q.Evidence)}
	}
	for _, a := range d.ExecutionAttempts {
		issue.ExecutionAttempts = append(issue.ExecutionAttempts, &pb.ExecutionAttempt{Id: a.ID, Agent: a.Agent, AgentSession: a.AgentSession, Status: a.Status, StartedAt: pbts(a.Started), EndedAt: pbts(a.Ended), Branch: a.Branch, Commits: append([]string{}, a.Commits...), Patches: artifactsToProto(a.Patches), PullRequests: append([]string{}, a.PullRequests...), Evidence: artifactsToProto(a.Evidence), StatusReason: a.StatusReason})
	}
	for _, s := range d.AgentSessions {
		item := &pb.AgentSessionReference{Id: s.ID, Agent: s.Agent, Status: s.Status, StartedAt: pbts(s.Started), EndedAt: pbts(s.Ended)}
		if s.Transcript != nil {
			item.Transcript = artifactToProto(*s.Transcript)
		}
		issue.AgentSessions = append(issue.AgentSessions, item)
	}
	return issue, nil
}

func artifactsFromProto(in []*pb.ArtifactReference) []ArtifactDocument {
	out := make([]ArtifactDocument, 0, len(in))
	for _, ref := range in {
		if ref != nil {
			out = append(out, artifactFromProto(ref))
		}
	}
	return out
}
func artifactFromProto(ref *pb.ArtifactReference) ArtifactDocument {
	return ArtifactDocument{Kind: ref.Kind, URL: ref.Url, SHA256: ref.Sha256, MediaType: ref.MediaType, SizeBytes: ref.SizeBytes, Name: ref.Name, Reference: ref.Reference}
}
func artifactsToProto(in []ArtifactDocument) []*pb.ArtifactReference {
	out := make([]*pb.ArtifactReference, 0, len(in))
	for _, ref := range in {
		out = append(out, artifactToProto(ref))
	}
	return out
}
func artifactToProto(ref ArtifactDocument) *pb.ArtifactReference {
	return &pb.ArtifactReference{Kind: ref.Kind, Url: ref.URL, Sha256: ref.SHA256, MediaType: ref.MediaType, SizeBytes: ref.SizeBytes, Name: ref.Name, Reference: ref.Reference}
}

func ts(value *timestamppb.Timestamp) string {
	if value == nil {
		return ""
	}
	return value.AsTime().UTC().Format(time.RFC3339Nano)
}
func pbts(value string) *timestamppb.Timestamp {
	if value == "" {
		return nil
	}
	t, _ := time.Parse(time.RFC3339Nano, value)
	return timestamppb.New(t)
}

func StatusString(s pb.Status) string {
	switch s {
	case pb.Status_STATUS_IN_PROGRESS:
		return "in_progress"
	case pb.Status_STATUS_BLOCKED:
		return "blocked"
	case pb.Status_STATUS_CLOSED:
		return "closed"
	case pb.Status_STATUS_DEFERRED:
		return "deferred"
	default:
		return "open"
	}
}
func ParseStatus(s string) pb.Status {
	switch strings.ToLower(s) {
	case "in_progress", "in-progress":
		return pb.Status_STATUS_IN_PROGRESS
	case "blocked":
		return pb.Status_STATUS_BLOCKED
	case "closed":
		return pb.Status_STATUS_CLOSED
	case "deferred":
		return pb.Status_STATUS_DEFERRED
	default:
		return pb.Status_STATUS_OPEN
	}
}
func PriorityString(p pb.Priority) string {
	switch p {
	case pb.Priority_PRIORITY_P0:
		return "P0"
	case pb.Priority_PRIORITY_P1:
		return "P1"
	case pb.Priority_PRIORITY_P2:
		return "P2"
	case pb.Priority_PRIORITY_P3:
		return "P3"
	case pb.Priority_PRIORITY_P4:
		return "P4"
	case pb.Priority_PRIORITY_P9:
		return "P9"
	default:
		return ""
	}
}
func ParsePriority(s string) pb.Priority {
	switch strings.ToUpper(s) {
	case "P0":
		return pb.Priority_PRIORITY_P0
	case "P1":
		return pb.Priority_PRIORITY_P1
	case "P2":
		return pb.Priority_PRIORITY_P2
	case "P3":
		return pb.Priority_PRIORITY_P3
	case "P4":
		return pb.Priority_PRIORITY_P4
	case "P9":
		return pb.Priority_PRIORITY_P9
	default:
		return pb.Priority_PRIORITY_UNSPECIFIED
	}
}

func MaterialRevision(doc *IssueDocument) string {
	n, err := materialDocument(doc)
	if err != nil {
		return ""
	}
	raw, _ := json.Marshal(n)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func MaterialEqual(a, b *IssueDocument) bool {
	aa, errA := materialDocument(a)
	bb, errB := materialDocument(b)
	return errA == nil && errB == nil && reflect.DeepEqual(aa, bb)
}

func MaterialChangedFields(a, b *IssueDocument) []string {
	aa, errA := materialDocument(a)
	bb, errB := materialDocument(b)
	if errA != nil || errB != nil {
		return []string{"invalid"}
	}
	left, _ := json.Marshal(aa)
	right, _ := json.Marshal(bb)
	var lm, rm map[string]json.RawMessage
	_ = json.Unmarshal(left, &lm)
	_ = json.Unmarshal(right, &rm)
	keys := map[string]struct{}{}
	for k := range lm {
		keys[k] = struct{}{}
	}
	for k := range rm {
		keys[k] = struct{}{}
	}
	var changed []string
	for k := range keys {
		if k == "created_at" || k == "updated_at" {
			continue
		}
		if !bytes.Equal(lm[k], rm[k]) {
			changed = append(changed, k)
		}
	}
	sort.Strings(changed)
	return changed
}

func materialDocument(doc *IssueDocument) (*IssueDocument, error) {
	n, err := Normalize(doc)
	if err != nil {
		return nil, err
	}
	n.SchemaVersion, n.RecordType = "", ""
	if n.Metadata != nil {
		for key := range n.Metadata {
			if strings.HasPrefix(key, "nostr.") || strings.HasPrefix(key, "nostrig.") {
				delete(n.Metadata, key)
			}
		}
		if len(n.Metadata) == 0 {
			n.Metadata = nil
		}
	}
	return n, nil
}

// PriorityFromRaw accepts the integer representation emitted by Beads.
func PriorityFromRaw(raw json.RawMessage) string {
	var n int
	if json.Unmarshal(raw, &n) == nil && (n >= 0 && n <= 4 || n == 9) {
		return "P" + strconv.Itoa(n)
	}
	return ""
}
