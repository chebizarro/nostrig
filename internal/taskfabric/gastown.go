package taskfabric

import (
	"fmt"
	"strings"
	"time"

	gonostr "fiatjaf.com/nostr"
	beadspb "github.com/chebizarro/nostrig/gen/beads"
	"github.com/chebizarro/nostrig/internal/taskmodel"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const defaultDependencyType = "blocks"

type typedDependencyMutation struct {
	TaskID   string
	Type     string
	Metadata string
}

func normalizeTaskIssue(issue *beadspb.Issue) (*beadspb.Issue, error) {
	doc, err := taskmodel.FromProto(issue)
	if err != nil {
		return nil, err
	}
	return taskmodel.ToProto(doc)
}

// applyDependencyMutations keeps typed v2 dependencies authoritative. Legacy
// add/remove parameters remain compatible and map only to the "blocks" type.
func applyDependencyMutations(issue *beadspb.Issue, params map[string]any, actor string, now time.Time) error {
	legacyAdds := cleanTaskIDs(paramList(params, "add_dependencies"))
	legacyRemoves := cleanTaskIDs(paramList(params, "remove_dependencies"))
	typedAdds, err := typedDependencyParams(params, "add_typed_dependencies", true)
	if err != nil {
		return err
	}
	typedRemoves, err := typedDependencyParams(params, "remove_typed_dependencies", false)
	if err != nil {
		return err
	}
	if len(legacyAdds)+len(legacyRemoves)+len(typedAdds)+len(typedRemoves) == 0 {
		return nil
	}
	doc, err := taskmodel.FromProto(issue)
	if err != nil {
		return err
	}
	for _, id := range legacyAdds {
		typedAdds = append(typedAdds, typedDependencyMutation{TaskID: id, Type: defaultDependencyType})
	}
	for _, id := range legacyRemoves {
		typedRemoves = append(typedRemoves, typedDependencyMutation{TaskID: id, Type: defaultDependencyType})
	}
	remove := map[string]struct{}{}
	for _, dep := range typedRemoves {
		remove[dependencyKey(dep.TaskID, dep.Type)] = struct{}{}
	}
	kept := doc.Dependencies[:0]
	seen := map[string]struct{}{}
	for _, dep := range doc.Dependencies {
		key := dependencyKey(dep.DependsOnID, dep.Type)
		if _, drop := remove[key]; drop {
			continue
		}
		kept = append(kept, dep)
		seen[key] = struct{}{}
	}
	doc.Dependencies = kept
	for _, dep := range typedAdds {
		if dep.TaskID == doc.ID {
			return fmt.Errorf("task cannot depend on itself")
		}
		key := dependencyKey(dep.TaskID, dep.Type)
		if _, exists := seen[key]; exists {
			continue
		}
		doc.Dependencies = append(doc.Dependencies, taskmodel.DependencyDocument{
			IssueID: doc.ID, DependsOnID: dep.TaskID, Type: dep.Type,
			Created: now.UTC().Format(time.RFC3339Nano), CreatedBy: actor, Metadata: dep.Metadata,
		})
		seen[key] = struct{}{}
	}
	// Normalize derives the compatibility target list from typed relations.
	doc.DependsOn = nil
	normalized, err := taskmodel.ToProto(doc)
	if err != nil {
		return err
	}
	proto.Reset(issue)
	proto.Merge(issue, normalized)
	return nil
}

func typedDependencyParams(params map[string]any, key string, allowMetadata bool) ([]typedDependencyMutation, error) {
	raw, ok := params[key]
	if !ok || raw == nil {
		return nil, nil
	}
	items, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("%s must be an array", key)
	}
	out := make([]typedDependencyMutation, 0, len(items))
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s entries must be objects", key)
		}
		for field := range m {
			if field != "task_id" && field != "depends_on_id" && field != "type" && (field != "metadata" || !allowMetadata) {
				return nil, fmt.Errorf("unknown %s field %q", key, field)
			}
		}
		id := strings.TrimPrefix(mapString(m, "task_id"), "task:")
		if id == "" {
			id = strings.TrimPrefix(mapString(m, "depends_on_id"), "task:")
		}
		typ := strings.ToLower(mapString(m, "type"))
		if id == "" || typ == "" {
			return nil, fmt.Errorf("%s entries require task_id and type", key)
		}
		switch typ {
		case "blocks", "blocked-by", "parent-child", "discovered-from":
		default:
			return nil, fmt.Errorf("invalid dependency type %q", typ)
		}
		out = append(out, typedDependencyMutation{TaskID: id, Type: typ, Metadata: mapString(m, "metadata")})
	}
	return out, nil
}

func mapString(values map[string]any, key string) string {
	value, ok := values[key]
	if !ok || value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func dependencyKey(id, typ string) string {
	return strings.TrimPrefix(strings.TrimSpace(id), "task:") + "|" + strings.ToLower(strings.TrimSpace(typ))
}

// recordDispatch creates the execution-attempt and optional session reference
// in the same CAS mutation as assignment/claim.
func recordDispatch(issue *beadspb.Issue, commandID gonostr.ID, agent, status string, params map[string]any, now time.Time) (string, string, error) {
	attemptID := stringParam(params, "execution_attempt_id")
	if attemptID == "" {
		attemptID = "attempt:" + commandID.Hex()
	}
	sessionID := stringParam(params, "agent_session_id")
	branch := stringParam(params, "branch")
	for _, attempt := range issue.ExecutionAttempts {
		if attempt == nil || attempt.Id != attemptID {
			continue
		}
		if attempt.Agent != agent || attempt.AgentSession != sessionID || attempt.Branch != branch || attempt.Status != status {
			return "", "", fmt.Errorf("execution attempt %s already exists with different dispatch data", attemptID)
		}
		if isTerminalAttemptStatus(attempt.Status) {
			return "", "", fmt.Errorf("execution attempt %s is already terminal", attemptID)
		}
		return attemptID, sessionID, nil
	}
	if active := activeExecutionAttempt(issue); active != nil {
		return "", "", fmt.Errorf("execution attempt %s is still active", active.Id)
	}
	if active := activeAgentSession(issue); active != nil && active.Id != sessionID {
		return "", "", fmt.Errorf("agent session %s is still active", active.Id)
	}
	issue.ExecutionAttempts = append(issue.ExecutionAttempts, &beadspb.ExecutionAttempt{
		Id: attemptID, Agent: agent, AgentSession: sessionID, Status: status,
		StartedAt: timestamppb.New(now), Branch: branch,
	})
	if sessionID != "" {
		found := false
		for _, session := range issue.AgentSessions {
			if session != nil && session.Id == sessionID {
				if isTerminalSessionStatus(session.Status) {
					return "", "", fmt.Errorf("agent session %s is already terminal", sessionID)
				}
				if session.Agent != agent {
					return "", "", fmt.Errorf("agent session %s belongs to another agent", sessionID)
				}
				found = true
			}
		}
		if !found {
			issue.AgentSessions = append(issue.AgentSessions, &beadspb.AgentSessionReference{
				Id: sessionID, Agent: agent, Status: "active", StartedAt: timestamppb.New(now),
			})
		}
	}
	return attemptID, sessionID, nil
}

func applyExecutionUpdate(issue *beadspb.Issue, params map[string]any, actor string, restrictToActor bool, now time.Time) error {
	keys := []string{"execution_attempt_id", "attempt_status", "attempt_status_reason", "attempt_branch", "attempt_commits", "attempt_pull_requests", "attempt_evidence_ids", "agent_session_status"}
	changed := false
	for _, key := range keys {
		if _, ok := params[key]; ok {
			changed = true
		}
	}
	if !changed {
		return nil
	}
	id := stringParam(params, "execution_attempt_id")
	var attempt *beadspb.ExecutionAttempt
	for i := len(issue.ExecutionAttempts) - 1; i >= 0; i-- {
		candidate := issue.ExecutionAttempts[i]
		if candidate != nil && (candidate.Id == id || id == "") && (!restrictToActor || candidate.Agent == actor) {
			attempt = candidate
			break
		}
	}
	if attempt == nil {
		return fmt.Errorf("execution attempt %q not found", id)
	}
	if restrictToActor && attempt.Agent != actor {
		return fmt.Errorf("worker cannot update another agent's execution attempt")
	}
	status := strings.ToLower(stringParam(params, "attempt_status"))
	if status == "" {
		status = attempt.Status
	}
	if !validAttemptTransition(attempt.Status, status) {
		return fmt.Errorf("invalid execution attempt transition %q to %q", attempt.Status, status)
	}
	attempt.Status = status
	if _, ok := params["attempt_status_reason"]; ok {
		attempt.StatusReason = stringParam(params, "attempt_status_reason")
	}
	if _, ok := params["attempt_branch"]; ok {
		attempt.Branch = stringParam(params, "attempt_branch")
	}
	attempt.Commits = addStrings(attempt.Commits, paramList(params, "attempt_commits"))
	attempt.PullRequests = addStrings(attempt.PullRequests, paramList(params, "attempt_pull_requests"))
	attempt.Evidence = appendUniqueArtifacts(attempt.Evidence, artifactReferences(paramList(params, "attempt_evidence_ids"), "execution")...)
	if attempt.StartedAt == nil {
		attempt.StartedAt = timestamppb.New(now)
	}
	terminal := status == "completed" || status == "failed" || status == "cancelled"
	if terminal && attempt.EndedAt == nil {
		attempt.EndedAt = timestamppb.New(now)
	}
	if status == "blocked" {
		issue.Status = beadspb.Status_STATUS_BLOCKED
		issue.BlockedAt = timestamppb.New(now)
	} else if status == "running" && issue.Status != beadspb.Status_STATUS_CLOSED {
		issue.Status = beadspb.Status_STATUS_IN_PROGRESS
	}
	if status == "completed" {
		if issue.Review == nil {
			issue.Review = &beadspb.Review{}
		}
		issue.Review.Required = true
		issue.Review.State = "requested"
	}
	if attempt.AgentSession != "" {
		for _, session := range issue.AgentSessions {
			if session == nil || session.Id != attempt.AgentSession {
				continue
			}
			sessionStatus := strings.ToLower(stringParam(params, "agent_session_status"))
			if sessionStatus == "" && terminal {
				sessionStatus = status
			}
			if sessionStatus != "" {
				if !validSessionStatus(sessionStatus) {
					return fmt.Errorf("invalid agent session status %q", sessionStatus)
				}
				if terminal != isTerminalSessionStatus(sessionStatus) {
					return fmt.Errorf("agent session and execution attempt terminal state must agree")
				}
				if isTerminalSessionStatus(session.Status) && session.Status != sessionStatus {
					return fmt.Errorf("agent session %s is already terminal", session.Id)
				}
				session.Status = sessionStatus
				if isTerminalSessionStatus(sessionStatus) && session.EndedAt == nil {
					session.EndedAt = timestamppb.New(now)
				}
			}
		}
	}
	return nil
}

func activeExecutionAttempt(issue *beadspb.Issue) *beadspb.ExecutionAttempt {
	if issue == nil {
		return nil
	}
	for _, attempt := range issue.ExecutionAttempts {
		if attempt != nil && !isTerminalAttemptStatus(attempt.Status) {
			return attempt
		}
	}
	return nil
}

func activeAgentSession(issue *beadspb.Issue) *beadspb.AgentSessionReference {
	if issue == nil {
		return nil
	}
	for _, session := range issue.AgentSessions {
		if session != nil && !isTerminalSessionStatus(session.Status) {
			return session
		}
	}
	return nil
}

func isTerminalAttemptStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "completed", "failed", "cancelled":
		return true
	default:
		return false
	}
}

func validAttemptTransition(from, to string) bool {
	from, to = strings.ToLower(strings.TrimSpace(from)), strings.ToLower(strings.TrimSpace(to))
	if from == to {
		return true
	}
	if to != "running" && to != "blocked" && to != "completed" && to != "failed" && to != "cancelled" {
		return false
	}
	switch from {
	case "dispatched":
		return to == "running" || to == "blocked" || to == "completed" || to == "cancelled"
	case "running":
		return to == "blocked" || to == "completed" || to == "failed" || to == "cancelled"
	case "blocked":
		return to == "running" || to == "completed" || to == "failed" || to == "cancelled"
	default:
		return false
	}
}

func validSessionStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "active", "running", "blocked", "completed", "failed", "cancelled":
		return true
	default:
		return false
	}
}

func isTerminalSessionStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "completed", "failed", "cancelled":
		return true
	default:
		return false
	}
}

func (h *Handler) mutationActor(caller string) (string, bool) {
	policy, ok := h.ACL[strings.ToLower(strings.TrimSpace(caller))]
	if !ok {
		return caller, false
	}
	actor := strings.TrimSpace(policy.WorkerID)
	if actor == "" {
		actor = caller
	}
	restricted := containsRole(policy.Roles, RoleWorker) && !containsRole(policy.Roles, RoleAdmin) && !containsRole(policy.Roles, RoleOperator) && !containsRole(policy.Roles, RoleMaintainer)
	return actor, restricted
}
