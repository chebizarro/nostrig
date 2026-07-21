package taskfabric

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	gonostr "fiatjaf.com/nostr"
	cascadia "git.sharegap.net/cascadia/cascadia-go"
	cascontextvm "git.sharegap.net/cascadia/cascadia-go/contextvm"
	casnostr "git.sharegap.net/cascadia/cascadia-go/nostr"
	beadspb "github.com/chebizarro/nostrig/gen/beads"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
)

type RelayLedger struct {
	Relays          []string
	Signer          nip34.Signer
	Client          *nip34.Client
	CanonicalAuthor string
	SyncNIP34Status bool
}

func (l *RelayLedger) GetTask(ctx context.Context, id string) (*beadspb.Issue, error) {
	id = strings.TrimPrefix(strings.TrimSpace(id), "task:")
	if id == "" {
		return nil, fmt.Errorf("task id is required")
	}
	events, err := FetchTaskStateEvents(ctx, l.client(), SyncOptions{Relays: l.Relays, TaskIDs: []string{id}, Authors: []string{l.CanonicalAuthor}, Limit: 10})
	if err != nil {
		return nil, err
	}
	export, err := ExportFromTaskStateEvents(events)
	if err != nil {
		return nil, err
	}
	for _, issue := range export.Issues {
		if issue.Id == id {
			return issue, nil
		}
	}
	return nil, fmt.Errorf("task %s not found", id)
}

func (l *RelayLedger) PutTask(ctx context.Context, issue *beadspb.Issue) (*gonostr.Event, error) {
	now := time.Now().UTC()
	ev, err := nip34.BuildTaskStateEvent(issue, l.CanonicalAuthor, now)
	if err != nil {
		return nil, err
	}
	events := []*gonostr.Event{ev}
	if l.SyncNIP34Status {
		if status := nip34.BuildNIP34IssueStatusEvent(issue, now); status != nil {
			events = append(events, status)
		}
	}
	return l.publishEvents(ctx, events)
}

func (l *RelayLedger) DeleteTask(ctx context.Context, id string) (*gonostr.Event, error) {
	id = strings.TrimPrefix(strings.TrimSpace(id), "task:")
	if id == "" {
		return nil, fmt.Errorf("task id is required")
	}
	events, err := FetchTaskStateEvents(ctx, l.client(), SyncOptions{Relays: l.Relays, TaskIDs: []string{id}, Authors: []string{l.CanonicalAuthor}, Limit: 10})
	if err != nil {
		return nil, err
	}
	var latest *gonostr.Event
	for _, candidate := range events {
		d, _ := nip34.TagD(candidate)
		if d != "task:"+id {
			continue
		}
		if candidate.PubKey.Hex() != strings.ToLower(strings.TrimSpace(l.CanonicalAuthor)) {
			continue
		}
		if latest == nil || eventAfter(candidate, latest) {
			latest = candidate
		}
	}
	if latest == nil {
		// NIP-09 deletion is idempotent. A relay may stop returning the target
		// immediately after accepting the first request, so at-least-once
		// command replay must treat an already-absent task as success.
		return nil, nil
	}
	issue, err := nip34.ParseTaskStateEvent(latest)
	if err != nil {
		return nil, err
	}
	repoAddr := issue.GetMetadata().GetCustom()["nip34.repo_addr"]
	ev, err := nip34.BuildTaskTombstone(latest, repoAddr, l.CanonicalAuthor, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	return l.publishOne(ctx, ev)
}

func (l *RelayLedger) GetQueue(ctx context.Context, repoAddr, queue string) ([]string, error) {
	repoAddr = strings.TrimSpace(repoAddr)
	if repoAddr == "" {
		return nil, fmt.Errorf("repo addr is required")
	}
	queue = queueName(queue)
	relays := cleanStrings(l.Relays)
	if len(relays) == 0 {
		return nil, fmt.Errorf("at least one relay is required")
	}
	author, err := gonostr.PubKeyFromHex(strings.TrimSpace(l.CanonicalAuthor))
	if err != nil {
		return nil, fmt.Errorf("canonical author is required")
	}
	f := gonostr.Filter{Kinds: []gonostr.Kind{gonostr.Kind(nip34.KindNamedList)}, Authors: []gonostr.PubKey{author}, Tags: gonostr.TagMap{"d": []string{queueIdentifier(repoAddr, queue)}, "a": []string{repoAddr}}, Limit: 20}
	events, err := l.client().Fetch(ctx, relays, f)
	if err != nil {
		return nil, err
	}
	var latest *gonostr.Event
	expectedD := queueIdentifier(repoAddr, queue)
	for _, ev := range events {
		if ev == nil || ev.PubKey.Hex() != strings.ToLower(strings.TrimSpace(l.CanonicalAuthor)) {
			continue
		}
		d, _ := nip34.TagD(ev)
		schema, _ := nip34.TagFirst(ev, "schema")
		if d != expectedD || schema != "cascadia.task-collection.v1" || !hasMarkedTag(ev, "a", repoAddr, "nip34-repo") {
			return nil, fmt.Errorf("relay returned malformed queue state")
		}
		if latest == nil || eventAfter(ev, latest) {
			latest = ev
		}
	}
	if latest == nil {
		return nil, nil
	}
	prefix := nip34.Address(nip34.KindCanonicalState, strings.ToLower(strings.TrimSpace(l.CanonicalAuthor)), "task:")
	ids := []string{}
	for _, tag := range latest.Tags {
		if len(tag) >= 2 && tag[0] == "a" && strings.HasPrefix(tag[1], prefix) {
			id := strings.TrimPrefix(tag[1], prefix)
			if id == "" {
				return nil, fmt.Errorf("relay returned malformed queue task coordinate")
			}
			ids = append(ids, id)
		}
	}
	return ids, nil
}

func (l *RelayLedger) PutQueue(ctx context.Context, repoAddr, queue string, ids []string) (*gonostr.Event, error) {
	ev := nip34.BuildQueueCollectionEventForAuthor(repoAddr, queue, ids, l.CanonicalAuthor, time.Now().UTC())
	return l.publishOne(ctx, ev)
}

func (l *RelayLedger) publishOne(ctx context.Context, ev *gonostr.Event) (*gonostr.Event, error) {
	return l.publishEvents(ctx, []*gonostr.Event{ev})
}

func (l *RelayLedger) publishEvents(ctx context.Context, events []*gonostr.Event) (*gonostr.Event, error) {
	if l.Signer == nil {
		return nil, fmt.Errorf("signer is required")
	}
	if err := nip34.NewPublisher().Publish(ctx, cleanStrings(l.Relays), l.Signer, events); err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, nil
	}
	return events[0], nil
}

func (l *RelayLedger) client() *nip34.Client {
	if l != nil && l.Client != nil {
		return l.Client
	}
	return nip34.NewClient()
}

type serveSubscriber func(ctx context.Context, relays []string, filter gonostr.Filter) <-chan gonostr.RelayEvent

type serveUnwrapper func(ctx context.Context, signer casnostr.Signer, outer *gonostr.Event) (*gonostr.Event, error)

type serveWrapper func(ctx context.Context, signer casnostr.Signer, recipientPubkey string, payload json.RawMessage) (*gonostr.Event, error)

type serveWrappedPublisher func(ctx context.Context, relays []string, outer *gonostr.Event) error

type serveErrorReporter func(stage string, err error, event *gonostr.Event)

type serveVerifier func(event *gonostr.Event) bool

type ServeOptions struct {
	Relays          []string
	RepoAddrs       []string
	Signer          nip34.Signer
	PubKey          string
	SyncNIP34Status bool
	QualityProject  string
	HealthFile      string
	Authorization   AuthorizationConfig
	Audit           AuthzAuditSink

	subscribe         serveSubscriber
	unwrap            serveUnwrapper
	wrap              serveWrapper
	publishWrapped    serveWrappedPublisher
	responsePublisher EventPublisher
	reportError       serveErrorReporter
	verify            serveVerifier
}

func Serve(ctx context.Context, opts ServeOptions) error {
	relays := cleanStrings(opts.Relays)
	if len(relays) == 0 {
		return fmt.Errorf("at least one relay is required")
	}
	repoAddrs := cleanStrings(opts.RepoAddrs)
	if len(repoAddrs) == 0 && serveProductionMode() {
		return fmt.Errorf("at least one repo addr is required in production serve mode")
	}
	if opts.Signer == nil {
		return fmt.Errorf("signer is required")
	}
	if err := opts.Authorization.Validate(); err != nil {
		return fmt.Errorf("invalid caller ACL: %w", err)
	}
	pubkey := strings.ToLower(strings.TrimSpace(opts.PubKey))
	if provider, ok := opts.Signer.(nip34.PublicKeyProvider); ok {
		signerPubKey, err := provider.PublicKey(ctx)
		if err != nil {
			return err
		}
		signerPubKey = strings.ToLower(strings.TrimSpace(signerPubKey))
		if pubkey != "" && pubkey != signerPubKey {
			return fmt.Errorf("serve pubkey does not match signer pubkey")
		}
		pubkey = signerPubKey
	}
	if pubkey == "" {
		return fmt.Errorf("serve requires --pubkey when signer cannot provide one")
	}
	parsedPubKey, err := gonostr.PubKeyFromHex(pubkey)
	if err != nil {
		return fmt.Errorf("serve pubkey must be valid hex")
	}
	pubkey = parsedPubKey.Hex()
	contextSigner, err := contextVMSigner(opts.Signer)
	if err != nil {
		return err
	}
	ledger := &RelayLedger{Relays: relays, Signer: opts.Signer, CanonicalAuthor: strings.ToLower(pubkey), SyncNIP34Status: opts.SyncNIP34Status}
	quality := &RelayQualitySource{Relays: relays, Project: opts.QualityProject}
	audit := opts.Audit
	if audit == nil {
		audit = NewJSONAuditSink(os.Stderr)
	}
	handler := &Handler{Ledger: ledger, Quality: quality, RepoAddrs: repoAddrs, Recipient: pubkey, ACL: opts.Authorization.Callers, ClosePolicy: opts.Authorization.ClosePolicy, Audit: audit}
	subscribe := opts.subscribe
	if subscribe == nil {
		pool := gonostr.NewPool()
		defer pool.Close("nostrig serve complete")
		subscribe = func(ctx context.Context, relays []string, filter gonostr.Filter) <-chan gonostr.RelayEvent {
			return pool.SubscribeMany(ctx, relays, filter, gonostr.SubscriptionOptions{})
		}
	}
	unwrap := opts.unwrap
	if unwrap == nil {
		unwrap = func(ctx context.Context, signer casnostr.Signer, outer *gonostr.Event) (*gonostr.Event, error) {
			inner, err := cascontextvm.Unwrap(ctx, signer, (*casnostr.Event)(outer))
			if err != nil {
				return nil, err
			}
			return (*gonostr.Event)(inner), nil
		}
	}
	wrap := opts.wrap
	if wrap == nil {
		wrap = func(ctx context.Context, signer casnostr.Signer, recipientPubkey string, payload json.RawMessage) (*gonostr.Event, error) {
			outer, _, err := cascontextvm.Wrap(ctx, signer, recipientPubkey, payload)
			if err != nil {
				return nil, err
			}
			return (*gonostr.Event)(outer), nil
		}
	}
	publishWrapped := opts.publishWrapped
	if publishWrapped == nil {
		publishWrapped = func(ctx context.Context, relays []string, outer *gonostr.Event) error {
			accepted, err := casnostr.Publish(ctx, relays, *(*casnostr.Event)(outer))
			if err != nil {
				return err
			}
			if accepted == 0 {
				return fmt.Errorf("no relay accepted wrapped response")
			}
			return nil
		}
	}
	responsePublisher := opts.responsePublisher
	if responsePublisher == nil {
		responsePublisher = nip34.NewPublisher()
	}
	reportError := opts.reportError
	if reportError == nil {
		reportError = defaultServeErrorReporter
	}
	verify := opts.verify
	if verify == nil {
		verify = func(event *gonostr.Event) bool { return casnostr.VerifyEvent((*casnostr.Event)(event)) }
	}
	filter := gonostr.Filter{Kinds: []gonostr.Kind{gonostr.Kind(nip34.KindContextVMIntent), gonostr.Kind(cascadia.NIP59_GIFT_WRAP)}, Tags: gonostr.TagMap{"p": []string{pubkey}}}
	ch := subscribe(ctx, relays, filter)
	stopHealth, err := startHealthFile(ctx, opts.HealthFile)
	if err != nil {
		return err
	}
	defer stopHealth()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ie, ok := <-ch:
			if !ok {
				return fmt.Errorf("subscription closed")
			}
			ev := ie.Event
			if err := serveRelayEvent(ctx, relays, opts.Signer, contextSigner, handler, unwrap, wrap, publishWrapped, responsePublisher, reportError, verify, &ev); err != nil {
				return err
			}
		}
	}
}

func startHealthFile(ctx context.Context, path string) (func(), error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return func() {}, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create health file directory: %w", err)
	}
	_ = os.Remove(path)
	write := func() error {
		return os.WriteFile(path, []byte(time.Now().UTC().Format(time.RFC3339)+"\n"), 0o644)
	}
	if err := write(); err != nil {
		return nil, fmt.Errorf("write health file: %w", err)
	}
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case <-ticker.C:
				_ = write()
			}
		}
	}()
	return func() {
		close(stop)
		_ = os.Remove(path)
	}, nil
}

func allowedMethod(ev *gonostr.Event) bool {
	method, _ := nip34.TagFirst(ev, "method")
	if method == "" {
		var req cascontextvm.Request
		_ = json.Unmarshal([]byte(ev.Content), &req)
		method = req.Method
	}
	switch method {
	case "task/create", "task/claim", "task/assign", "task/update", "task/close", "task/delete", "task/quality-status", "queue/enqueue", "queue/dequeue", "queue/list":
		return true
	default:
		return false
	}
}

func serveRelayEvent(ctx context.Context, relays []string, signer nip34.Signer, contextSigner casnostr.Signer, handler *Handler, unwrap serveUnwrapper, wrap serveWrapper, publishWrapped serveWrappedPublisher, responsePublisher EventPublisher, reportError serveErrorReporter, verify serveVerifier, event *gonostr.Event) error {
	if event == nil {
		return nil
	}
	inner := event
	wrapped := event.Kind == gonostr.Kind(cascadia.NIP59_GIFT_WRAP)
	if verify == nil {
		verify = func(candidate *gonostr.Event) bool { return casnostr.VerifyEvent((*casnostr.Event)(candidate)) }
	}
	if !verify(event) {
		reportError("verify", fmt.Errorf("invalid event id or signature"), event)
		return nil
	}
	if wrapped {
		unwrapped, err := unwrap(ctx, contextSigner, event)
		if err != nil {
			reportError("unwrap", err, event)
			return nil
		}
		inner = unwrapped
	}
	if err := validateIntent(inner, handler.Recipient, verify); err != nil {
		reportError("validate_intent", err, inner)
		return nil
	}
	resp, err := handler.HandleIntent(ctx, inner, time.Now().UTC())
	if err != nil {
		reportError("handle_intent", err, inner)
		return nil
	}
	if wrapped {
		outer, err := wrap(ctx, contextSigner, inner.PubKey.Hex(), json.RawMessage(resp.Content))
		if err != nil {
			reportError("wrap_response", err, inner)
			return fmt.Errorf("wrap ContextVM response: %w", err)
		}
		if err := publishWrapped(ctx, relays, outer); err != nil {
			reportError("publish_wrapped_response", err, outer)
			return fmt.Errorf("publish wrapped ContextVM response: %w", err)
		}
		return nil
	}
	if err := responsePublisher.Publish(ctx, relays, signer, []*gonostr.Event{resp}); err != nil {
		reportError("publish_response", err, resp)
		return fmt.Errorf("publish ContextVM response: %w", err)
	}
	return nil
}

func validateIntent(ev *gonostr.Event, recipient string, verify serveVerifier) error {
	if ev == nil || ev.Kind != gonostr.Kind(nip34.KindContextVMIntent) {
		return fmt.Errorf("expected ContextVM intent")
	}
	if !verify(ev) {
		return fmt.Errorf("invalid intent id or signature")
	}
	recipients := nip34.TagAll(ev, "p")
	if len(recipients) != 1 || recipients[0] != strings.TrimSpace(recipient) {
		return fmt.Errorf("intent recipient mismatch")
	}
	schemas := nip34.TagAll(ev, "schema")
	if len(schemas) > 1 || (len(schemas) == 1 && schemas[0] != nip34.TaskIntentSchema) {
		return fmt.Errorf("invalid intent schema")
	}
	var req cascontextvm.Request
	if err := json.Unmarshal([]byte(ev.Content), &req); err != nil {
		return fmt.Errorf("decode intent: %w", err)
	}
	if !supportedMethod(req.Method) {
		return fmt.Errorf("unsupported method")
	}
	methods := nip34.TagAll(ev, "method")
	parts := strings.Split(req.Method, "/")
	domains, ops := nip34.TagAll(ev, "domain"), nip34.TagAll(ev, "op")
	if len(parts) != 2 {
		return fmt.Errorf("invalid intent method")
	}
	if len(methods)+len(domains)+len(ops) > 0 &&
		(len(methods) != 1 || methods[0] != req.Method || len(domains) != 1 || domains[0] != parts[0] || len(ops) != 1 || ops[0] != parts[1]) {
		return fmt.Errorf("intent method tags do not match content")
	}
	return nil
}

func eventAfter(a, b *gonostr.Event) bool {
	at, bt := nip34.EventTime(a), nip34.EventTime(b)
	if !at.Equal(bt) {
		return at.After(bt)
	}
	return a.ID.Hex() > b.ID.Hex()
}

func queueIdentifier(repoAddr, queue string) string {
	return "queue:" + strings.TrimSpace(repoAddr) + ":" + queueName(queue)
}

func hasMarkedTag(ev *gonostr.Event, name, value, marker string) bool {
	if ev == nil {
		return false
	}
	for _, tag := range ev.Tags {
		if len(tag) >= 4 && tag[0] == name && tag[1] == value && tag[3] == marker {
			return true
		}
	}
	return false
}

func defaultServeErrorReporter(stage string, err error, event *gonostr.Event) {
	if err == nil {
		return
	}
	if event != nil {
		fmt.Fprintf(os.Stderr, "nostrig serve %s failed for kind %d event %s: %v\n", stage, event.Kind, event.ID.Hex(), err)
		return
	}
	fmt.Fprintf(os.Stderr, "nostrig serve %s failed: %v\n", stage, err)
}

func contextVMSigner(s nip34.Signer) (casnostr.Signer, error) {
	keyer, ok := s.(casnostr.Signer)
	if !ok {
		return nil, fmt.Errorf("signer does not support ContextVM NIP-59 encryption")
	}
	return keyer, nil
}

func serveProductionMode() bool {
	env := strings.ToLower(strings.TrimSpace(os.Getenv("NOSTRIG_ENV")))
	return env == "production" || env == "prod"
}
