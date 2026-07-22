package taskfabric

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	gonostr "fiatjaf.com/nostr"
	beadspb "github.com/chebizarro/nostrig/gen/beads"
)

type Role string

const (
	RoleAdmin      Role = "admin"
	RoleOperator   Role = "operator"
	RoleMaintainer Role = "maintainer"
	RoleDispatcher Role = "dispatcher"
	RoleWorker     Role = "worker"
	RoleReviewer   Role = "reviewer"
	RoleReadOnly   Role = "read-only"
)

type CallerPolicy struct {
	Roles        []Role   `json:"roles"`
	Repositories []string `json:"repositories"`
	Methods      []string `json:"methods,omitempty"`
	WorkerID     string   `json:"worker_id,omitempty"`
}

type ClosePolicy struct {
	RequireQuality  bool `json:"require_quality,omitempty"`
	RequireReviewer bool `json:"require_reviewer,omitempty"`
}

type AuthorizationConfig struct {
	Callers     map[string]CallerPolicy `json:"callers"`
	ClosePolicy ClosePolicy             `json:"close_policy,omitempty"`
}

func LoadAuthorizationConfig(path string) (AuthorizationConfig, error) {
	f, err := os.Open(strings.TrimSpace(path))
	if err != nil {
		return AuthorizationConfig{}, err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	var cfg AuthorizationConfig
	if err := dec.Decode(&cfg); err != nil {
		return AuthorizationConfig{}, fmt.Errorf("decode caller ACL: %w", err)
	}
	if err := ensureJSONEOF(dec); err != nil {
		return AuthorizationConfig{}, err
	}
	normalized := make(map[string]CallerPolicy, len(cfg.Callers))
	for caller, policy := range cfg.Callers {
		key := strings.ToLower(strings.TrimSpace(caller))
		if _, exists := normalized[key]; exists {
			return AuthorizationConfig{}, fmt.Errorf("caller ACL contains duplicate pubkey %q", caller)
		}
		normalized[key] = policy
	}
	cfg.Callers = normalized
	if err := cfg.Validate(); err != nil {
		return AuthorizationConfig{}, err
	}
	return cfg, nil
}

func ensureJSONEOF(dec *json.Decoder) error {
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("decode caller ACL: trailing JSON value")
		}
		return fmt.Errorf("decode caller ACL: %w", err)
	}
	return nil
}

func (c AuthorizationConfig) Validate() error {
	if len(c.Callers) == 0 {
		return fmt.Errorf("caller ACL must contain at least one caller")
	}
	for caller, policy := range c.Callers {
		if _, err := gonostr.PubKeyFromHex(strings.TrimSpace(caller)); err != nil {
			return fmt.Errorf("caller ACL contains invalid pubkey %q", caller)
		}
		if len(policy.Roles) == 0 {
			return fmt.Errorf("caller %s has no roles", caller)
		}
		for _, role := range policy.Roles {
			if !validRole(role) {
				return fmt.Errorf("caller %s has unknown role %q", caller, role)
			}
		}
		if len(cleanStrings(policy.Repositories)) == 0 {
			return fmt.Errorf("caller %s has no repositories", caller)
		}
		for _, method := range cleanStrings(policy.Methods) {
			if !supportedMethod(method) {
				return fmt.Errorf("caller %s has unknown method %q", caller, method)
			}
		}
	}
	return nil
}

func validRole(role Role) bool {
	switch role {
	case RoleAdmin, RoleOperator, RoleMaintainer, RoleDispatcher, RoleWorker, RoleReviewer, RoleReadOnly:
		return true
	default:
		return false
	}
}

func supportedMethod(method string) bool {
	switch method {
	case "task/create", "task/claim", "task/assign", "task/update", "task/close", "task/delete", "task/quality-status", "queue/enqueue", "queue/dequeue", "queue/list":
		return true
	default:
		return false
	}
}

type AuthzAuditRecord struct {
	Time      time.Time `json:"time"`
	EventID   string    `json:"event_id,omitempty"`
	Caller    string    `json:"caller"`
	Recipient string    `json:"recipient,omitempty"`
	Method    string    `json:"method"`
	RepoAddr  string    `json:"repo_addr,omitempty"`
	Role      Role      `json:"role,omitempty"`
	Fields    []string  `json:"fields,omitempty"`
	Decision  string    `json:"decision"`
	Reason    string    `json:"reason"`
}

type AuthzAuditSink interface {
	Record(context.Context, AuthzAuditRecord) error
}

type JSONAuditSink struct {
	mu sync.Mutex
	w  io.Writer
}

func NewJSONAuditSink(w io.Writer) *JSONAuditSink {
	if w == nil {
		w = os.Stderr
	}
	return &JSONAuditSink{w: w}
}

func (s *JSONAuditSink) Record(_ context.Context, record AuthzAuditRecord) error {
	if s == nil || s.w == nil {
		return fmt.Errorf("authorization audit sink is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.NewEncoder(s.w).Encode(record)
}

type AuthorizationError struct {
	Reason string
}

func (e *AuthorizationError) Error() string {
	return "authorization denied: " + e.Reason
}

type authorizationDecision struct {
	repo string
	role Role
}

func (h *Handler) authorize(ctx context.Context, ev *gonostr.Event, method string, p map[string]any, caller string, now time.Time) error {
	fields := make([]string, 0, len(p))
	for field := range p {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	record := AuthzAuditRecord{Time: now.UTC(), Caller: caller, Method: method, Fields: fields, Decision: "deny"}
	if ev != nil {
		record.EventID = ev.ID.Hex()
	}
	if h != nil {
		record.Recipient = strings.TrimSpace(h.Recipient)
	}
	deny := func(reason string) error {
		record.Reason = reason
		if h != nil && h.Audit != nil {
			_ = h.Audit.Record(ctx, record)
		}
		return &AuthorizationError{Reason: reason}
	}
	if h == nil || len(h.ACL) == 0 {
		return deny("acl_unconfigured")
	}
	if h.Audit == nil {
		return deny("audit_unconfigured")
	}
	policy, ok := h.ACL[strings.ToLower(strings.TrimSpace(caller))]
	if !ok {
		return deny("unknown_caller")
	}
	if !supportedMethod(method) {
		return deny("method_denied")
	}
	if len(policy.Methods) > 0 && !contains(cleanStrings(policy.Methods), method) {
		return deny("method_denied")
	}

	repo, issue, err := h.resolveAuthorizationRepo(ctx, method, p)
	if err != nil {
		return deny("repo_unresolved")
	}
	record.RepoAddr = repo
	if allowed := cleanStrings(h.RepoAddrs); len(allowed) > 0 && !contains(allowed, repo) {
		return deny("repo_not_served")
	}
	if !contains(cleanStrings(policy.Repositories), repo) && !contains(cleanStrings(policy.Repositories), "*") {
		return deny("repo_denied")
	}
	role, ok := allowedRole(policy.Roles, method)
	if !ok {
		return deny("method_denied")
	}
	record.Role = role
	if reason := authorizeFields(role, policy, method, p, issue, caller); reason != "" {
		return deny(reason)
	}
	if method == "task/close" {
		if h.ClosePolicy.RequireReviewer && role != RoleReviewer && role != RoleAdmin && role != RoleOperator {
			return deny("reviewer_required")
		}
		if h.ClosePolicy.RequireQuality {
			if h.Quality == nil || issue == nil {
				return deny("quality_required")
			}
			quality, err := h.Quality.GetQuality(ctx, []string{issue.Id})
			if err != nil || quality[issue.Id].State != QualityPassing || quality[issue.Id].BlocksMerge {
				return deny("quality_required")
			}
		}
	}
	record.Decision, record.Reason = "allow", "authorized"
	if err := h.Audit.Record(ctx, record); err != nil {
		return &AuthorizationError{Reason: "audit_failed"}
	}
	return nil
}

func (h *Handler) resolveAuthorizationRepo(ctx context.Context, method string, p map[string]any) (string, *beadIssue, error) {
	requestedRepo := stringParam(p, "repo_addr")
	if strings.HasPrefix(method, "queue/") {
		if requestedRepo == "" {
			return "", nil, fmt.Errorf("repo_addr is required")
		}
		if method == "queue/enqueue" {
			issue, err := h.lookupAuthzTask(ctx, stringParam(p, "task_id"))
			if err != nil {
				return "", nil, err
			}
			if issue.RepoAddr != requestedRepo {
				return "", nil, fmt.Errorf("task repository mismatch")
			}
		}
		return requestedRepo, nil, nil
	}
	if method == "task/create" {
		if requestedRepo == "" {
			return "", nil, fmt.Errorf("repo_addr is required")
		}
		return requestedRepo, nil, nil
	}
	if method == "task/quality-status" {
		if requestedRepo == "" {
			return "", nil, fmt.Errorf("repo_addr is required")
		}
		ids := paramList(p, "task_ids")
		if id := stringParam(p, "task_id"); id != "" {
			ids = append(ids, id)
		}
		for _, id := range cleanTaskIDs(ids) {
			issue, err := h.lookupAuthzTask(ctx, id)
			if err != nil || issue.RepoAddr != requestedRepo {
				return "", nil, fmt.Errorf("task repository mismatch")
			}
		}
		return requestedRepo, nil, nil
	}
	issue, err := h.lookupAuthzTask(ctx, stringParam(p, "task_id"))
	if err != nil {
		return "", nil, err
	}
	if requestedRepo != "" && requestedRepo != issue.RepoAddr {
		return "", nil, fmt.Errorf("task repository mismatch")
	}
	return issue.RepoAddr, issue, nil
}

func (h *Handler) lookupAuthzTask(ctx context.Context, id string) (*beadIssue, error) {
	if h == nil || h.Ledger == nil {
		return nil, fmt.Errorf("handler ledger is nil")
	}
	id = strings.TrimPrefix(strings.TrimSpace(id), "task:")
	if id == "" {
		return nil, fmt.Errorf("task_id is required")
	}
	issue, err := h.Ledger.GetTask(ctx, id)
	if err != nil {
		return nil, err
	}
	if issue == nil {
		return nil, fmt.Errorf("task not found")
	}
	return authzIssue(issue), nil
}

func authzIssue(issue *beadspb.Issue) *beadIssue {
	if issue == nil {
		return nil
	}
	return &beadIssue{Id: issue.Id, Assignee: issue.Assignee, RepoAddr: issue.GetMetadata().GetCustom()["nip34.repo_addr"]}
}

type beadIssue struct {
	Id       string
	Assignee string
	RepoAddr string
}

func allowedRole(roles []Role, method string) (Role, bool) {
	for _, role := range []Role{RoleAdmin, RoleOperator, RoleMaintainer, RoleDispatcher, RoleWorker, RoleReviewer, RoleReadOnly} {
		if containsRole(roles, role) && roleAllows(role, method) {
			return role, true
		}
	}
	return "", false
}

func roleAllows(role Role, method string) bool {
	switch role {
	case RoleAdmin, RoleOperator:
		return true
	case RoleMaintainer:
		return true
	case RoleDispatcher:
		switch method {
		case "task/create", "task/claim", "task/assign", "task/update", "task/close", "task/delete", "task/quality-status", "queue/enqueue", "queue/dequeue", "queue/list":
			return true
		}
	case RoleWorker:
		switch method {
		case "task/claim", "task/update", "task/close", "task/quality-status", "queue/dequeue", "queue/list":
			return true
		}
	case RoleReviewer:
		return method == "task/quality-status" || method == "task/close" || method == "queue/list"
	case RoleReadOnly:
		return method == "task/quality-status" || method == "queue/list"
	}
	return false
}

func authorizeFields(role Role, policy CallerPolicy, method string, p map[string]any, issue *beadIssue, caller string) string {
	if role == RoleAdmin || role == RoleOperator || role == RoleMaintainer {
		return ""
	}
	self := strings.TrimSpace(policy.WorkerID)
	if self == "" {
		self = caller
	}
	if role == RoleWorker {
		switch method {
		case "task/claim":
			if claimer := stringParam(p, "claimer"); claimer != "" && claimer != self && claimer != caller {
				return "worker_identity_mismatch"
			}
			if issue != nil && issue.Assignee != "" && issue.Assignee != self && issue.Assignee != caller {
				return "worker_not_assignee"
			}
		case "task/update":
			if issue == nil || (issue.Assignee != self && issue.Assignee != caller) {
				return "worker_not_assignee"
			}
			for _, field := range []string{"assignee", "priority", "title", "epic", "feature_id", "nip34_event_id", "nip34_kind", "add_dependencies", "remove_dependencies"} {
				if _, ok := p[field]; ok {
					return "field_denied"
				}
			}
			if strings.EqualFold(stringParam(p, "status"), "closed") {
				return "field_denied"
			}
		case "task/close":
			if issue == nil || (issue.Assignee != self && issue.Assignee != caller) {
				return "worker_not_assignee"
			}
		}
	}
	if role == RoleDispatcher && method == "task/update" {
		for field := range p {
			if field != "task_id" && field != "repo_addr" && field != "priority" {
				return "field_denied"
			}
		}
	}
	return ""
}

func stringParam(p map[string]any, key string) string {
	if p == nil {
		return ""
	}
	value, ok := p[key]
	if !ok || value == nil {
		return ""
	}
	s, ok := value.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(s)
}
