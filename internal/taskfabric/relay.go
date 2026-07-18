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
	SyncNIP34Status bool
}

func (l *RelayLedger) GetTask(ctx context.Context, id string) (*beadspb.Issue, error) {
	id = strings.TrimPrefix(strings.TrimSpace(id), "task:")
	if id == "" {
		return nil, fmt.Errorf("task id is required")
	}
	events, err := FetchTaskStateEvents(ctx, l.client(), SyncOptions{Relays: l.Relays, TaskIDs: []string{id}, Limit: 10})
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
	ev, err := nip34.BuildTaskStateEvent(issue, now)
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
	events, err := FetchTaskStateEvents(ctx, l.client(), SyncOptions{Relays: l.Relays, TaskIDs: []string{id}, Limit: 10})
	if err != nil {
		return nil, err
	}
	var latest *gonostr.Event
	for _, candidate := range events {
		d, _ := nip34.TagD(candidate)
		if d != "task:"+id {
			continue
		}
		if latest == nil || nip34.EventTime(candidate).After(nip34.EventTime(latest)) {
			latest = candidate
		}
	}
	if latest == nil {
		// NIP-09 deletion is idempotent. A relay may stop returning the target
		// immediately after accepting the first request, so at-least-once
		// command replay must treat an already-absent task as success.
		return nil, nil
	}
	tags := gonostr.Tags{{"e", latest.ID.Hex()}, {"k", fmt.Sprintf("%d", nip34.KindCanonicalState)}}
	tags = append(tags, gonostr.Tag{"a", fmt.Sprintf("%d:%s:task:%s", nip34.KindCanonicalState, latest.PubKey.Hex(), id)})
	ev := &gonostr.Event{Kind: gonostr.Kind(5), CreatedAt: gonostr.Now(), Tags: tags, Content: "delete task " + id}
	return l.publishOne(ctx, ev)
}

func (l *RelayLedger) GetQueue(ctx context.Context, queue string) ([]string, error) {
	queue = queueName(queue)
	relays := cleanStrings(l.Relays)
	if len(relays) == 0 {
		return nil, fmt.Errorf("at least one relay is required")
	}
	f := gonostr.Filter{Kinds: []gonostr.Kind{gonostr.Kind(nip34.KindNamedList)}, Tags: gonostr.TagMap{"d": []string{"queue:" + queue}}, Limit: 20}
	events, err := l.client().Fetch(ctx, relays, f)
	if err != nil {
		return nil, err
	}
	var latest *gonostr.Event
	for _, ev := range events {
		if latest == nil || nip34.EventTime(ev).After(nip34.EventTime(latest)) {
			latest = ev
		}
	}
	if latest == nil {
		return nil, nil
	}
	ids := []string{}
	for _, tag := range latest.Tags {
		if len(tag) >= 2 && tag[0] == "a" && strings.Contains(tag[1], "task:") {
			parts := strings.Split(tag[1], "task:")
			ids = append(ids, parts[len(parts)-1])
		}
	}
	return ids, nil
}

func (l *RelayLedger) PutQueue(ctx context.Context, queue string, ids []string) (*gonostr.Event, error) {
	ev := nip34.BuildQueueCollectionEvent(queue, ids, time.Now().UTC())
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

type ServeOptions struct {
	Relays          []string
	RepoAddrs       []string
	Signer          nip34.Signer
	PubKey          string
	SyncNIP34Status bool
	QualityProject  string
	HealthFile      string
}

func Serve(ctx context.Context, opts ServeOptions) error {
	relays := cleanStrings(opts.Relays)
	if len(relays) == 0 {
		return fmt.Errorf("at least one relay is required")
	}
	if opts.Signer == nil {
		return fmt.Errorf("signer is required")
	}
	pubkey := strings.TrimSpace(opts.PubKey)
	if pubkey == "" {
		if provider, ok := opts.Signer.(nip34.PublicKeyProvider); ok {
			pk, err := provider.PublicKey(ctx)
			if err != nil {
				return err
			}
			pubkey = pk
		}
	}
	if pubkey == "" {
		return fmt.Errorf("serve requires --pubkey when signer cannot provide one")
	}
	contextSigner, err := contextVMSigner(opts.Signer)
	if err != nil {
		return err
	}
	ledger := &RelayLedger{Relays: relays, Signer: opts.Signer, SyncNIP34Status: opts.SyncNIP34Status}
	quality := &RelayQualitySource{Relays: relays, Project: opts.QualityProject}
	handler := &Handler{Ledger: ledger, Quality: quality, RepoAddrs: cleanStrings(opts.RepoAddrs)}
	pool := gonostr.NewPool()
	defer pool.Close("nostrig serve complete")
	filter := gonostr.Filter{Kinds: []gonostr.Kind{gonostr.Kind(nip34.KindContextVMIntent), gonostr.Kind(cascadia.NIP59_GIFT_WRAP)}, Tags: gonostr.TagMap{"p": []string{pubkey}}}
	ch := pool.SubscribeMany(ctx, relays, filter, gonostr.SubscriptionOptions{})
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
			inner := &ev
			wrapped := ev.Kind == gonostr.Kind(cascadia.NIP59_GIFT_WRAP)
			if wrapped {
				unwrapped, err := cascontextvm.Unwrap(ctx, contextSigner, (*casnostr.Event)(&ev))
				if err != nil {
					continue
				}
				inner = (*gonostr.Event)(unwrapped)
			}
			if !allowedMethod(inner) {
				continue
			}
			resp, err := handler.HandleIntent(ctx, inner, time.Now().UTC())
			if err != nil {
				continue
			}
			if wrapped {
				outer, _, err := cascontextvm.Wrap(ctx, contextSigner, inner.PubKey.Hex(), json.RawMessage(resp.Content))
				if err == nil {
					_, _ = casnostr.Publish(ctx, relays, *outer)
				}
				continue
			}
			_ = nip34.NewPublisher().Publish(ctx, relays, opts.Signer, []*gonostr.Event{resp})
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

func contextVMSigner(s nip34.Signer) (casnostr.Signer, error) {
	keyer, ok := s.(casnostr.Signer)
	if !ok {
		return nil, fmt.Errorf("signer does not support ContextVM NIP-59 encryption")
	}
	return keyer, nil
}
