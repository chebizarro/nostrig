package taskfabric

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	beadspb "github.com/chebizarro/nostrig/gen/beads"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
	gonostr "github.com/nbd-wtf/go-nostr"
)

type RelayLedger struct {
	Relays []string
	Signer nip34.Signer
	Client *nip34.Client
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
	ev, err := nip34.BuildTaskStateEvent(issue, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	return l.publishOne(ctx, ev)
}

func (l *RelayLedger) GetQueue(ctx context.Context, queue string) ([]string, error) {
	queue = queueName(queue)
	relays := cleanStrings(l.Relays)
	if len(relays) == 0 {
		return nil, fmt.Errorf("at least one relay is required")
	}
	f := gonostr.Filter{Kinds: []int{nip34.KindNamedList}, Tags: gonostr.TagMap{"d": []string{"queue:" + queue}}, Limit: 20}
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
	if l.Signer == nil {
		return nil, fmt.Errorf("signer is required")
	}
	if err := nip34.NewPublisher().Publish(ctx, cleanStrings(l.Relays), l.Signer, []*gonostr.Event{ev}); err != nil {
		return nil, err
	}
	return ev, nil
}

func (l *RelayLedger) client() *nip34.Client {
	if l != nil && l.Client != nil {
		return l.Client
	}
	return nip34.NewClient()
}

type ServeOptions struct {
	Relays []string
	Signer nip34.Signer
	PubKey string
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
	ledger := &RelayLedger{Relays: relays, Signer: opts.Signer}
	handler := &Handler{Ledger: ledger}
	pool := gonostr.NewSimplePool(ctx)
	filters := gonostr.Filters{{Kinds: []int{nip34.KindContextVMIntent}, Tags: gonostr.TagMap{"p": []string{pubkey}, "domain": []string{"task", "queue"}}}}
	ch := pool.SubMany(ctx, relays, filters)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ie, ok := <-ch:
			if !ok {
				return fmt.Errorf("subscription closed")
			}
			if ie.Event == nil || !allowedMethod(ie.Event) {
				continue
			}
			resp, err := handler.HandleIntent(ctx, ie.Event, time.Now().UTC())
			if err != nil {
				continue
			}
			_ = nip34.NewPublisher().Publish(ctx, relays, opts.Signer, []*gonostr.Event{resp})
		}
	}
}

func allowedMethod(ev *gonostr.Event) bool {
	method, _ := nip34.TagFirst(ev, "method")
	if method == "" {
		var req rpcEnvelope
		_ = json.Unmarshal([]byte(ev.Content), &req)
		method = req.Method
	}
	switch method {
	case "task/claim", "task/assign", "task/update", "task/close", "queue/enqueue", "queue/dequeue", "queue/list":
		return true
	default:
		return false
	}
}
