//go:build nostrig_acceptance

package acceptance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	gonostr "fiatjaf.com/nostr"
	beadspb "github.com/chebizarro/nostrig/gen/beads"
	nostrigNostr "github.com/chebizarro/nostrig/internal/nostr"
	"github.com/chebizarro/nostrig/internal/taskfabric"
	"github.com/chebizarro/nostrig/internal/taskmodel"
)

const (
	acceptanceRepo    = "30617:nostrig-acceptance:three-agent"
	acceptanceQueue   = "acceptance"
	acceptanceProject = "nostrig-crm"
)

type acceptanceActor struct {
	name   string
	pubkey string
	signer *nostrigNostr.PrivateKeySigner
}

func newAcceptanceActor(t *testing.T, name string) acceptanceActor {
	t.Helper()
	secret := gonostr.Generate()
	signer, err := nostrigNostr.NewPrivateKeySigner(secret.Hex())
	if err != nil {
		t.Fatal(err)
	}
	return acceptanceActor{name: name, pubkey: secret.Public().Hex(), signer: signer}
}

type acceptanceAudit struct {
	mu      sync.Mutex
	records []taskfabric.AuthzAuditRecord
}

func (a *acceptanceAudit) Record(_ context.Context, record taskfabric.AuthzAuditRecord) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.records = append(a.records, record)
	return nil
}

func (a *acceptanceAudit) snapshot() []taskfabric.AuthzAuditRecord {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]taskfabric.AuthzAuditRecord(nil), a.records...)
}

type runningAcceptanceService struct {
	cancel  context.CancelFunc
	done    chan error
	stopped bool
}

func startAcceptanceService(t *testing.T, opts taskfabric.ServeOptions, readyAddr string) *runningAcceptanceService {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- taskfabric.Serve(ctx, opts)
	}()
	service := &runningAcceptanceService{cancel: cancel, done: done}
	waitAcceptanceReady(t, service, readyAddr, 45*time.Second)
	return service
}

func (s *runningAcceptanceService) stop(t *testing.T) {
	t.Helper()
	if s == nil || s.stopped {
		return
	}
	s.stopped = true
	s.cancel()
	select {
	case err := <-s.done:
		if err != nil && !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "subscription closed") {
			t.Fatalf("stop Nostrig service: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Nostrig service did not stop before deadline")
	}
}

func waitAcceptanceReady(t *testing.T, service *runningAcceptanceService, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	url := "http://" + addr + "/readyz"
	for time.Now().Before(deadline) {
		select {
		case err := <-service.done:
			t.Fatalf("Nostrig service exited before readiness: %v", err)
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		resp, err := http.DefaultClient.Do(req)
		cancel()
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("Nostrig service did not become ready at %s", url)
}

func waitAcceptanceRelay(t *testing.T, relayURL string, signer nostrigNostr.Signer, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		probe := &gonostr.Event{
			Kind: gonostr.Kind(1), CreatedAt: gonostr.Timestamp(time.Now().Unix()),
			Content: "nostrig-crm relay readiness probe",
		}
		lastErr = nostrigNostr.NewPublisher().Publish(ctx, []string{relayURL}, signer, []*gonostr.Event{probe})
		cancel()
		if lastErr == nil {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("relay %s did not acknowledge a readiness event: %v", relayURL, lastErr)
}

func freeAcceptanceAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

type acceptanceCommand struct {
	step          string
	actor         string
	method        string
	signer        nostrigNostr.Signer
	event         *gonostr.Event
	response      *taskfabric.ContextVMResponse
	responseEvent string
}

type acceptanceCommandRunner struct {
	t             *testing.T
	serverPubkey  string
	allRelays     []string
	publisher     *nostrigNostr.Publisher
	journalPath   string
	commands      []acceptanceCommand
	lastTimestamp int64
}

func (r *acceptanceCommandRunner) nextTime() time.Time {
	r.t.Helper()
	for {
		now := time.Now().UTC()
		if now.Unix() > r.lastTimestamp {
			r.lastTimestamp = now.Unix()
			return now
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (r *acceptanceCommandRunner) run(step string, actor acceptanceActor, method string, params any, relays []string) acceptanceCommand {
	r.t.Helper()
	if len(relays) == 0 {
		relays = r.allRelays
	}
	command, err := nostrigNostr.BuildContextVMCommand(method, r.serverPubkey, params, r.nextTime())
	if err != nil {
		r.t.Fatalf("%s build %s: %v", step, method, err)
	}
	if err := actor.signer.SignEvent(context.Background(), command); err != nil {
		r.t.Fatalf("%s sign %s: %v", step, method, err)
	}
	waiter, err := taskfabric.PrepareContextVMResponseWait(context.Background(), relays, command, r.serverPubkey, 30*time.Second)
	if err != nil {
		r.t.Fatalf("%s prepare response wait: %v", step, err)
	}
	if err := r.publisher.Publish(context.Background(), relays, actor.signer, []*gonostr.Event{command}); err != nil {
		waiter.Close()
		r.t.Fatalf("%s publish %s: %v", step, method, err)
	}
	response, err := waiter.Wait()
	if err != nil {
		r.t.Fatalf("%s wait for %s response: %v", step, method, err)
	}
	record := acceptanceCommand{step: step, actor: actor.name, method: method, signer: actor.signer, event: command, response: response}
	if response.Event != nil {
		record.responseEvent = response.Event.ID.Hex()
	}
	waitAcceptanceCommandComplete(r.t, r.journalPath, command.ID.Hex(), 15*time.Second)
	r.commands = append(r.commands, record)
	r.t.Logf("%s actor=%s method=%s command=%s response=%s error_code=%d", step, actor.name, method, command.ID.Hex(), record.responseEvent, response.ErrorCode)
	return record
}

func commandResult(t *testing.T, command acceptanceCommand) map[string]any {
	t.Helper()
	if command.response == nil {
		t.Fatalf("%s has no response", command.step)
	}
	if command.response.Error != "" {
		t.Fatalf("%s failed: code=%d error=%s data=%s", command.step, command.response.ErrorCode, command.response.Error, command.response.ErrorData)
	}
	if command.response.Result == nil {
		t.Fatalf("%s has no result", command.step)
	}
	var result map[string]any
	if err := json.Unmarshal(*command.response.Result, &result); err != nil {
		t.Fatalf("%s decode result: %v", command.step, err)
	}
	return result
}

func resultString(t *testing.T, result map[string]any, field string) string {
	t.Helper()
	value := strings.TrimSpace(fmt.Sprint(result[field]))
	if value == "" || value == "<nil>" {
		t.Fatalf("result lacks %s: %#v", field, result)
	}
	return value
}

type agentTaskView struct {
	eventID string
	issue   *beadspb.Issue
	digest  string
}

func fetchAgentTaskView(t *testing.T, actor string, relays []string, serverPubkey, taskID string) agentTaskView {
	t.Helper()
	client := nostrigNostr.NewClient()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	events, err := taskfabric.FetchTaskStateEvents(ctx, client, taskfabric.SyncOptions{
		Relays: relays, TaskIDs: []string{taskID}, Authors: []string{serverPubkey},
	})
	if err != nil {
		t.Fatalf("%s list task: %v", actor, err)
	}
	export, err := taskfabric.ExportFromTaskStateEvents(events)
	if err != nil {
		t.Fatalf("%s project task list: %v", actor, err)
	}
	if len(export.Issues) != 1 || export.Issues[0].Id != taskID {
		t.Fatalf("%s list returned %#v", actor, export.Issues)
	}
	var latest *gonostr.Event
	for _, event := range events {
		if event == nil || event.Kind != gonostr.Kind(nostrigNostr.KindCanonicalState) {
			continue
		}
		d, _ := nostrigNostr.TagD(event)
		if d != "task:"+taskID {
			continue
		}
		if latest == nil || event.CreatedAt > latest.CreatedAt ||
			(event.CreatedAt == latest.CreatedAt && event.ID.Hex() > latest.ID.Hex()) {
			latest = event
		}
	}
	if latest == nil {
		t.Fatalf("%s list returned no canonical revision for %s", actor, taskID)
	}
	eventID := latest.ID.Hex()
	doc, err := taskmodel.FromProto(export.Issues[0])
	if err != nil {
		t.Fatalf("%s normalize task: %v", actor, err)
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	view := agentTaskView{eventID: eventID, issue: export.Issues[0], digest: hex.EncodeToString(sum[:])}
	t.Logf("independent-list actor=%s revision=%s digest=%s", actor, view.eventID, view.digest)
	return view
}

func waitAgentTaskRevision(t *testing.T, actor string, relays []string, serverPubkey, taskID, revision string, timeout time.Duration) agentTaskView {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last agentTaskView
	for time.Now().Before(deadline) {
		last = fetchAgentTaskView(t, actor, relays, serverPubkey, taskID)
		if last.eventID == revision {
			return last
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("%s did not observe revision %s before timeout; last=%#v", actor, revision, last)
	return agentTaskView{}
}

func buildPassingQualityEvent(t *testing.T, actor acceptanceActor, project, taskID string, now time.Time) *gonostr.Event {
	t.Helper()
	content, err := json.Marshal(map[string]any{
		"schema_version": "pstf.audit.gate_decision.v1",
		"project":        project,
		"decision":       "pass",
		"reason":         "Gus approved review with PSTF evidence",
		"blocks_merge":   false,
		"findings": []map[string]any{{
			"id": "review-approved", "severity": "info", "status": "confirmed",
			"title":    "Three-agent acceptance review approved",
			"evidence": []string{"review:nostrig-crm:gus"}, "blocks_merge": false,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	event := &gonostr.Event{
		Kind:      gonostr.Kind(taskfabric.KindPSTFAudit),
		CreatedAt: gonostr.Timestamp(now.Unix()),
		Tags: gonostr.Tags{
			{"d", "pstf:gate-decision:" + project + ":" + taskID},
			{"project", project},
			{"audit_type", "CAS_AUDIT"},
			{"decision", "pass"},
			{"task", taskID},
		},
		Content: string(content),
	}
	if err := actor.signer.SignEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	return event
}

func waitAcceptanceQuality(t *testing.T, relays []string, author, taskID string, timeout time.Duration) taskfabric.QualityResult {
	t.Helper()
	source := &taskfabric.RelayQualitySource{
		Relays: relays, Project: acceptanceProject, Authors: []string{author},
	}
	deadline := time.Now().Add(timeout)
	var last taskfabric.QualityResult
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		var quality map[string]taskfabric.QualityResult
		quality, lastErr = source.GetQuality(ctx, []string{taskID})
		cancel()
		last = quality[taskID]
		if lastErr == nil && last.State == taskfabric.QualityPassing {
			return last
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("PSTF quality event did not become relay-visible: result=%#v err=%v", last, lastErr)
	return taskfabric.QualityResult{}
}

func TestPassingQualityAcceptanceFixture(t *testing.T) {
	gus := newAcceptanceActor(t, "Gus")
	event := buildPassingQualityEvent(t, gus, acceptanceProject, "fixture-task", time.Now().UTC())
	state, err := taskfabric.ProjectQualityState([]*gonostr.Event{event}, []string{gus.pubkey}, acceptanceProject)
	if err != nil {
		t.Fatal(err)
	}
	if state.Tasks["fixture-task"].State != taskfabric.QualityPassing {
		t.Fatalf("quality fixture did not project: %#v event=%#v", state, event)
	}
}

func resetAcceptanceJournalCursor(t *testing.T, path string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var disk map[string]any
	if err := json.Unmarshal(raw, &disk); err != nil {
		t.Fatal(err)
	}
	disk["cursor"] = map[string]any{"created_at": 0, "event_id": ""}
	raw, err = json.MarshalIndent(disk, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(raw, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

type journalEvidence struct {
	Cursor struct {
		CreatedAt int64  `json:"created_at"`
		EventID   string `json:"event_id"`
	} `json:"cursor"`
	Commands []struct {
		EventID      string `json:"event_id"`
		ResponseJSON string `json:"response_json"`
		Phase        string `json:"phase"`
	} `json:"commands"`
}

func waitAcceptanceCommandComplete(t *testing.T, path, eventID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(path)
		if err == nil {
			var evidence journalEvidence
			if json.Unmarshal(raw, &evidence) == nil {
				for _, record := range evidence.Commands {
					if record.EventID == eventID && record.Phase == string(taskfabric.CommandComplete) {
						return
					}
				}
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("command %s did not reach durable complete phase", eventID)
}

func loadJournalEvidence(t *testing.T, path string) journalEvidence {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var evidence journalEvidence
	if err := json.Unmarshal(raw, &evidence); err != nil {
		t.Fatal(err)
	}
	return evidence
}

func artifactIDs(issue *beadspb.Issue) []string {
	var ids []string
	for _, ref := range issue.GetEvidence() {
		if ref != nil && strings.TrimSpace(ref.Reference) != "" {
			ids = append(ids, strings.TrimSpace(ref.Reference))
		}
	}
	sort.Strings(ids)
	return ids
}

func TestFinalThreeAgentLiveAcceptance(t *testing.T) {
	if os.Getenv("NOSTRIG_ACCEPTANCE_CONTROL_DOCKER") != "1" {
		t.Skip("set NOSTRIG_ACCEPTANCE_CONTROL_DOCKER=1 to permit the disposable relay restart")
	}
	relays := cleanAcceptanceRelays(os.Getenv("NOSTRIG_ACCEPTANCE_RELAYS"))
	composeFile := strings.TrimSpace(os.Getenv("NOSTRIG_ACCEPTANCE_COMPOSE_FILE"))
	if len(relays) < 3 || composeFile == "" {
		t.Skip("three-agent acceptance requires three relay URLs and NOSTRIG_ACCEPTANCE_COMPOSE_FILE")
	}
	relays = relays[:3]

	server := newAcceptanceActor(t, "Nostrig")
	stew := newAcceptanceActor(t, "Stew")
	netward := newAcceptanceActor(t, "Netward")
	gus := newAcceptanceActor(t, "Gus")
	taskID := fmt.Sprintf("nostrig-crm-%d", time.Now().UnixNano())
	attemptID := "attempt:" + taskID
	sessionID := "session:" + taskID
	branch := "acceptance/" + taskID
	reviewEvidenceID := "review:" + taskID + ":gus"

	tempDir := t.TempDir()
	journalPath := filepath.Join(tempDir, "commands.json")
	outboxPath := filepath.Join(tempDir, "outbox.json")
	readyAddr := freeAcceptanceAddr(t)
	audit := &acceptanceAudit{}
	opts := taskfabric.ServeOptions{
		Relays:            relays,
		RepoAddrs:         []string{acceptanceRepo},
		Signer:            server.signer,
		PubKey:            server.pubkey,
		QualityProject:    acceptanceProject,
		QualityAuthors:    []string{gus.pubkey},
		ObservabilityAddr: readyAddr,
		Authorization: taskfabric.AuthorizationConfig{
			Callers: map[string]taskfabric.CallerPolicy{
				stew.pubkey:    {Roles: []taskfabric.Role{taskfabric.RoleMaintainer}, Repositories: []string{acceptanceRepo}, WorkerID: stew.pubkey},
				netward.pubkey: {Roles: []taskfabric.Role{taskfabric.RoleWorker}, Repositories: []string{acceptanceRepo}, WorkerID: netward.pubkey},
				gus.pubkey:     {Roles: []taskfabric.Role{taskfabric.RoleAdmin, taskfabric.RoleReviewer}, Repositories: []string{acceptanceRepo}, WorkerID: gus.pubkey},
			},
			ClosePolicy: taskfabric.ClosePolicy{RequireQuality: true, RequireReviewer: true},
		},
		Audit: audit,
		Publication: nostrigNostr.ReliablePublisherOptions{
			RequiredRelays:      relays,
			AckQuorum:           2,
			OutboxPath:          outboxPath,
			PublishTimeout:      5 * time.Second,
			BaseBackoff:         200 * time.Millisecond,
			MaxBackoff:          time.Second,
			MaxAttempts:         3,
			CircuitFailureLimit: 1,
		},
		CommandJournalPath: journalPath,
		CommandRetention:   24 * time.Hour,
	}
	service := startAcceptanceService(t, opts, readyAddr)
	serviceRunning := true
	t.Cleanup(func() {
		if serviceRunning {
			service.stop(t)
		}
	})

	runner := &acceptanceCommandRunner{
		t: t, serverPubkey: server.pubkey, allRelays: relays,
		publisher: nostrigNostr.NewPublisher(), journalPath: journalPath,
	}
	t.Logf("infrastructure relays=%s compose=%s signer_mode=ephemeral-local-test service=in-process journal=%s outbox=%s", strings.Join(relays, ","), composeFile, journalPath, outboxPath)

	// 1. Stew creates, assigns, and queues the task.
	create := runner.run("step-1-create", stew, "task/create", map[string]any{
		"task_id": taskID, "title": "Final Stew/Netward/Gus live acceptance",
		"description": "Disposable three-relay acceptance task", "repo_addr": acceptanceRepo,
		"queue": acceptanceQueue, "review_required": true, "quality_required": true,
		"review_requirements": []string{"Gus approval", "passing PSTF quality"},
	}, nil)
	createRevision := resultString(t, commandResult(t, create), "revision")
	assign := runner.run("step-1-assign", stew, "task/assign", map[string]any{
		"task_id": taskID, "assignee": netward.pubkey, "base_event_id": createRevision,
		"execution_attempt_id": attemptID, "agent_session_id": sessionID, "branch": branch,
	}, nil)
	assignRevision := resultString(t, commandResult(t, assign), "revision")
	enqueue := runner.run("step-1-enqueue", stew, "queue/enqueue", map[string]any{
		"repo_addr": acceptanceRepo, "queue": acceptanceQueue, "task_id": taskID, "base_event_id": "",
	}, nil)
	commandResult(t, enqueue)

	// 2. Netward independently receives the assignment and records a durable ACK.
	received := waitAgentTaskRevision(t, "Netward-receive", relays, server.pubkey, taskID, assignRevision, 20*time.Second)
	if received.issue.Assignee != netward.pubkey || received.eventID != assignRevision {
		t.Fatalf("Netward did not receive assigned revision: %#v", received)
	}
	ack := runner.run("step-2-ack", netward, "task/update", map[string]any{
		"task_id": taskID, "base_event_id": assignRevision,
		"checkpoint_id": "ack:" + taskID, "checkpoint_status": "acknowledged",
		"checkpoint_summary":      "Netward received and acknowledged the Nostrig dispatch",
		"checkpoint_evidence_ids": []string{assign.responseEvent},
	}, nil)
	ackRevision := resultString(t, commandResult(t, ack), "revision")

	// 3. Netward atomically claims the assigned dispatch.
	claim := runner.run("step-3-atomic-claim", netward, "task/claim", map[string]any{
		"task_id": taskID, "claimer": netward.pubkey, "base_event_id": ackRevision,
		"execution_attempt_id": attemptID, "agent_session_id": sessionID, "branch": branch,
	}, nil)
	claimResult := commandResult(t, claim)
	claimRevision := resultString(t, claimResult, "revision")
	if claimResult["status"] != "in_progress" || claimResult["assignee"] != netward.pubkey {
		t.Fatalf("unexpected winning claim: %#v", claimResult)
	}

	// 4. Gus's authorized competing attempt reaches CAS and loses.
	competing := runner.run("step-4-competing-claim", gus, "task/claim", map[string]any{
		"task_id": taskID, "claimer": gus.pubkey, "base_event_id": claimRevision,
	}, nil)
	if competing.response.ErrorCode != taskfabric.ConflictErrorCode || !strings.Contains(competing.response.Error, "already_claimed") ||
		!strings.Contains(string(competing.response.ErrorData), netward.pubkey) {
		t.Fatalf("competing claim was not a structured conflict: %#v", competing.response)
	}

	// 5. Netward publishes progress, then blocks with evidence.
	progress := runner.run("step-5-checkpoint", netward, "task/update", map[string]any{
		"task_id": taskID, "base_event_id": claimRevision,
		"checkpoint_id": "checkpoint:progress:" + taskID, "checkpoint_status": "running",
		"checkpoint_summary":      "Implementation checkpoint before dependency discovery",
		"checkpoint_evidence_ids": []string{"commit:acceptance-progress"},
	}, nil)
	progressRevision := resultString(t, commandResult(t, progress), "revision")
	blocked := runner.run("step-5-block", netward, "task/update", map[string]any{
		"task_id": taskID, "base_event_id": progressRevision, "status": "blocked",
		"status_reason":       "relay fixture dependency requires operator action",
		"blocker_description": "relay-3 restart is required to validate recovery",
		"evidence_ids":        []string{"evidence:blocker:relay-3"},
		"checkpoint_id":       "checkpoint:blocked:" + taskID, "checkpoint_status": "blocked",
		"checkpoint_summary":      "Blocked pending disposable relay recovery",
		"checkpoint_evidence_ids": []string{"evidence:blocker:relay-3"},
		"execution_attempt_id":    attemptID, "attempt_status": "blocked",
		"attempt_status_reason": "waiting for relay restart evidence",
		"attempt_evidence_ids":  []string{"evidence:blocker:relay-3"},
	}, nil)
	blockedRevision := resultString(t, commandResult(t, blocked), "revision")

	// 12 (performed mid-flow). Restart Nostrig, then stop one relay while work continues.
	service.stop(t)
	serviceRunning = false
	service = startAcceptanceService(t, opts, readyAddr)
	serviceRunning = true
	t.Log("step-12 service restart completed with durable journal and outbox")
	runCompose(t, composeFile, "stop", "relay-3")
	relayStopped := true
	defer func() {
		if relayStopped {
			_ = runComposeCommand(composeFile, "start", "relay-3")
		}
	}()
	t.Log("step-12 relay-3 stopped mid-flow; quorum remains two")

	// 6. Stew resolves the blocker while relay-3 is down; quorum/outbox must carry it.
	unblocked := runner.run("step-6-unblock", stew, "task/update", map[string]any{
		"task_id": taskID, "base_event_id": blockedRevision, "status": "in_progress",
		"status_reason": "Stew resolved the relay dependency; Netward may resume",
	}, relays[:2])
	unblockedRevision := resultString(t, commandResult(t, unblocked), "revision")
	runCompose(t, composeFile, "start", "relay-3")
	relayStopped = false
	waitAcceptanceRelay(t, relays[2], server.signer, 45*time.Second)
	waitAcceptanceReady(t, service, readyAddr, 45*time.Second)
	// The agent publisher intentionally observed a severed socket. Rotate that
	// disposable client pool; the service retains and drains its durable outbox.
	runner.publisher = nostrigNostr.NewPublisher()
	t.Log("step-12 relay-3 restarted and service returned to ready")

	resumed := runner.run("step-6-resume", netward, "task/update", map[string]any{
		"task_id": taskID, "base_event_id": unblockedRevision,
		"checkpoint_id": "checkpoint:resumed:" + taskID, "checkpoint_status": "running",
		"checkpoint_summary":      "Netward resumed after Stew resolved the blocker",
		"checkpoint_evidence_ids": []string{"evidence:relay-3:recovered"},
		"execution_attempt_id":    attemptID, "attempt_status": "running",
		"attempt_status_reason": "blocker resolved",
	}, nil)
	resumedRevision := resultString(t, commandResult(t, resumed), "revision")

	// 7. Netward completes implementation and requests Gus review.
	completed := runner.run("step-7-review-request", netward, "task/update", map[string]any{
		"task_id": taskID, "base_event_id": resumedRevision,
		"checkpoint_id": "checkpoint:completed:" + taskID, "checkpoint_status": "completed",
		"checkpoint_summary":      "Implementation complete; requesting Gus review",
		"checkpoint_evidence_ids": []string{"commit:acceptance-final"},
		"execution_attempt_id":    attemptID, "attempt_status": "completed",
		"agent_session_status": "completed", "attempt_commits": []string{"aacb65a", "acceptance-final"},
		"attempt_evidence_ids": []string{"test:three-agent:pre-review"},
		"request_validation":   true, "reviewer": gus.pubkey,
		"review_requirements": []string{"PSTF pass", "review evidence"},
	}, nil)
	completedRevision := resultString(t, commandResult(t, completed), "revision")

	// 8. Gus publishes trusted review/PSTF quality evidence and observes it through Nostrig.
	qualityEvent := buildPassingQualityEvent(t, gus, acceptanceProject, taskID, runner.nextTime())
	if err := runner.publisher.Publish(context.Background(), relays, gus.signer, []*gonostr.Event{qualityEvent}); err != nil {
		t.Fatalf("step-8 publish PSTF evidence: %v", err)
	}
	t.Logf("step-8 quality_event=%s review_evidence=%s", qualityEvent.ID.Hex(), reviewEvidenceID)
	projectedQuality := waitAcceptanceQuality(t, relays, gus.pubkey, taskID, 20*time.Second)
	if projectedQuality.EventID != qualityEvent.ID.Hex() {
		t.Fatalf("relay quality projection selected %s, want %s", projectedQuality.EventID, qualityEvent.ID.Hex())
	}
	quality := runner.run("step-8-quality-status", gus, "task/quality-status", map[string]any{
		"repo_addr": acceptanceRepo, "task_id": taskID,
	}, nil)
	qualityResult := commandResult(t, quality)
	qualityJSON, _ := json.Marshal(qualityResult)
	if !strings.Contains(string(qualityJSON), taskfabric.QualityPassing) || !strings.Contains(string(qualityJSON), qualityEvent.ID.Hex()) {
		t.Fatalf("trusted PSTF quality was not projected: %s", qualityJSON)
	}

	// 9. Netward cannot close a reviewer-gated task; canonical revision must not change.
	unauthorized := runner.run("step-9-unauthorized-close", netward, "task/close", map[string]any{
		"task_id": taskID, "base_event_id": completedRevision,
		"acceptance_evidence_ids": []string{"worker-self-approval"},
	}, nil)
	if unauthorized.response.ErrorCode == 0 || !strings.Contains(unauthorized.response.Error, "reviewer_required") {
		t.Fatalf("unauthorized close was not rejected by reviewer policy: %#v", unauthorized.response)
	}
	afterDenied := waitAgentTaskRevision(t, "after-denied-close", relays, server.pubkey, taskID, completedRevision, 20*time.Second)
	if afterDenied.eventID != completedRevision || afterDenied.issue.Status == beadspb.Status_STATUS_CLOSED {
		t.Fatalf("unauthorized close changed canonical state: %#v", afterDenied)
	}

	// 10. Gus closes after both review and quality gates pass.
	authorized := runner.run("step-10-authorized-close", gus, "task/close", map[string]any{
		"task_id": taskID, "base_event_id": completedRevision,
		"close_reason":            "Gus approved live acceptance after PSTF passed",
		"acceptance_evidence_ids": []string{reviewEvidenceID, qualityEvent.ID.Hex()},
	}, nil)
	authorizedResult := commandResult(t, authorized)
	finalRevision := resultString(t, authorizedResult, "revision")
	if authorizedResult["status"] != "closed" {
		t.Fatalf("authorized close did not close task: %#v", authorizedResult)
	}

	// 11. Three fresh clients independently list exactly the same canonical state.
	views := []agentTaskView{
		waitAgentTaskRevision(t, stew.name, relays, server.pubkey, taskID, finalRevision, 20*time.Second),
		waitAgentTaskRevision(t, netward.name, relays, server.pubkey, taskID, finalRevision, 20*time.Second),
		waitAgentTaskRevision(t, gus.name, relays, server.pubkey, taskID, finalRevision, 20*time.Second),
	}
	for _, view := range views {
		if view.eventID != finalRevision || view.digest != views[0].digest {
			t.Fatalf("independent list divergence: %#v", views)
		}
	}

	// 13. Stop the service, restart with the retained journal, and republish every
	// original signed ephemeral command. The restarted processor must return cached
	// responses without re-authorizing or mutating canonical state.
	auditBeforeReplay := len(audit.snapshot())
	service.stop(t)
	serviceRunning = false

	// CAS intent responses are ephemeral kind 25910 events. Subscribe before
	// replay so every cached response is observed and authenticated live rather
	// than incorrectly expecting a relay to retain ephemeral events.
	replayWaiters := make([]*taskfabric.ContextVMResponseWaiter, 0, len(runner.commands))
	for _, command := range runner.commands {
		waiter, err := taskfabric.PrepareContextVMResponseWait(
			context.Background(), relays, command.event, server.pubkey, 60*time.Second,
		)
		if err != nil {
			t.Fatalf("prepare replay response wait for %s: %v", command.step, err)
		}
		defer waiter.Close()
		replayWaiters = append(replayWaiters, waiter)
	}
	resetAcceptanceJournalCursor(t, journalPath)
	service = startAcceptanceService(t, opts, readyAddr)
	serviceRunning = true
	for _, command := range runner.commands {
		if err := runner.publisher.Publish(context.Background(), relays, command.signer, []*gonostr.Event{command.event}); err != nil {
			t.Fatalf("republish signed command for %s: %v", command.step, err)
		}
	}
	for i, waiter := range replayWaiters {
		replayed, err := waiter.Wait()
		if err != nil {
			t.Fatalf("observe replayed response for %s: %v", runner.commands[i].step, err)
		}
		if replayed.Event == nil || replayed.Event.ID.Hex() != runner.commands[i].responseEvent {
			t.Fatalf("%s cached response changed: live=%s replay=%#v", runner.commands[i].step, runner.commands[i].responseEvent, replayed.Event)
		}
	}
	t.Logf("step-13 replayed %d signed commands and observed identical cached responses", len(runner.commands))
	if got := len(audit.snapshot()); got != auditBeforeReplay {
		t.Fatalf("command replay duplicated authorization audit: before=%d after=%d", auditBeforeReplay, got)
	}

	// 14. Verify final state, history, queue, responses, journal, and audit.
	final := waitAgentTaskRevision(t, "post-replay", relays, server.pubkey, taskID, finalRevision, 20*time.Second)
	if final.eventID != finalRevision || final.digest != views[0].digest {
		t.Fatalf("replay changed final task: before=%#v after=%#v", views[0], final)
	}
	issue := final.issue
	if issue.Status != beadspb.Status_STATUS_CLOSED || issue.Assignee != netward.pubkey ||
		issue.GetReview().GetState() != "approved" || issue.GetReview().GetReviewer() != gus.pubkey {
		t.Fatalf("incorrect final task state: %#v", issue)
	}
	if len(issue.Checkpoints) != 5 {
		t.Fatalf("checkpoint history count=%d, want 5: %#v", len(issue.Checkpoints), issue.Checkpoints)
	}
	checkpointIDs := map[string]struct{}{}
	for _, checkpoint := range issue.Checkpoints {
		if checkpoint == nil {
			t.Fatal("nil checkpoint")
		}
		if _, duplicate := checkpointIDs[checkpoint.Id]; duplicate {
			t.Fatalf("duplicate checkpoint %s", checkpoint.Id)
		}
		checkpointIDs[checkpoint.Id] = struct{}{}
	}
	if len(issue.ExecutionAttempts) != 1 || issue.ExecutionAttempts[0].Id != attemptID ||
		issue.ExecutionAttempts[0].Status != "completed" {
		t.Fatalf("execution history is incorrect or duplicated: %#v", issue.ExecutionAttempts)
	}
	if len(issue.AgentSessions) != 1 || issue.AgentSessions[0].Id != sessionID ||
		issue.AgentSessions[0].Status != "completed" {
		t.Fatalf("agent-session history is incorrect or duplicated: %#v", issue.AgentSessions)
	}
	finalEvidence := artifactIDs(issue)
	for _, want := range []string{"evidence:blocker:relay-3", reviewEvidenceID, qualityEvent.ID.Hex()} {
		if !containsAcceptanceString(finalEvidence, want) {
			t.Fatalf("final audit evidence omits %s: %v", want, finalEvidence)
		}
	}

	queueLedger := &taskfabric.RelayLedger{Relays: relays, CanonicalAuthor: server.pubkey}
	queueRecord, err := queueLedger.GetQueue(context.Background(), acceptanceRepo, acceptanceQueue)
	if err != nil {
		t.Fatal(err)
	}
	if queueRecord == nil || len(queueRecord.TaskIDs) != 1 || queueRecord.TaskIDs[0] != taskID || len(queueRecord.Leases) != 0 {
		t.Fatalf("incorrect final queue membership: %#v", queueRecord)
	}

	journal := loadJournalEvidence(t, journalPath)
	if len(journal.Commands) != len(runner.commands) {
		t.Fatalf("journal commands=%d, want %d", len(journal.Commands), len(runner.commands))
	}
	journalIDs := map[string]struct{}{}
	for _, record := range journal.Commands {
		if record.Phase != string(taskfabric.CommandComplete) || record.ResponseJSON == "" {
			t.Fatalf("incomplete journal record: %#v", record)
		}
		if _, duplicate := journalIDs[record.EventID]; duplicate {
			t.Fatalf("duplicate journal command %s", record.EventID)
		}
		journalIDs[record.EventID] = struct{}{}
	}
	if len(journalIDs) != len(runner.commands) {
		t.Fatalf("journal uniqueness=%d, want %d", len(journalIDs), len(runner.commands))
	}

	auditRecords := audit.snapshot()
	if len(auditRecords) != len(runner.commands) {
		t.Fatalf("audit records=%d, want %d", len(auditRecords), len(runner.commands))
	}
	auditIDs := map[string]int{}
	denied := 0
	for _, record := range auditRecords {
		auditIDs[record.EventID]++
		if record.Decision == "deny" {
			denied++
		}
	}
	for _, command := range runner.commands {
		if auditIDs[command.event.ID.Hex()] != 1 {
			t.Fatalf("%s audit count=%d, want 1", command.step, auditIDs[command.event.ID.Hex()])
		}
	}
	if denied != 1 {
		t.Fatalf("audit denial count=%d, want unauthorized close only", denied)
	}

	t.Logf("PASS nostrig-crm task=%s revision=%s digest=%s commands=%d responses=%d audit=%d denied=%d checkpoints=%d queue_members=%d quality_event=%s",
		taskID, finalRevision, final.digest, len(runner.commands), len(runner.commands), len(auditRecords), denied,
		len(issue.Checkpoints), len(queueRecord.TaskIDs), qualityEvent.ID.Hex())
}

func cleanAcceptanceRelays(raw string) []string {
	var relays []string
	for _, relay := range strings.Split(raw, ",") {
		if relay = strings.TrimSpace(relay); relay != "" {
			relays = append(relays, relay)
		}
	}
	return relays
}

func containsAcceptanceString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func runComposeCommand(composeFile string, args ...string) error {
	commandArgs := append([]string{"compose", "-f", composeFile}, args...)
	output, err := exec.Command("docker", commandArgs...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker %s: %w\n%s", strings.Join(commandArgs, " "), err, output)
	}
	return nil
}
