package taskfabric

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	gonostr "fiatjaf.com/nostr"
	casnostr "git.sharegap.net/cascadia/cascadia-go/nostr"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
)

type ContextVMResponse struct {
	Event     *gonostr.Event   `json:"event,omitempty"`
	ID        string           `json:"id,omitempty"`
	Result    *json.RawMessage `json:"result,omitempty"`
	Error     string           `json:"error,omitempty"`
	ErrorCode int              `json:"error_code,omitempty"`
	ErrorData json.RawMessage  `json:"error_data,omitempty"`
}

type ContextVMResponseSource interface {
	FetchMany(ctx context.Context, relays []string, filters []gonostr.Filter) ([]*gonostr.Event, error)
	Subscribe(ctx context.Context, relays []string, filter gonostr.Filter) (<-chan gonostr.RelayEvent, error)
}

// ContextVMResponseWaiter owns subscribe-before-publish response observation.
// PrepareContextVMResponseWait must be called before publishing the command.
type ContextVMResponseWaiter struct {
	ctx            context.Context
	cancel         context.CancelFunc
	source         ContextVMResponseSource
	relays         []string
	filters        []gonostr.Filter
	command        *gonostr.Event
	expectedAuthor string
	preflight      []*gonostr.Event
	live           <-chan gonostr.RelayEvent
	closed         <-chan struct{}
	once           sync.Once
}

// PrepareContextVMResponseWait subscribes to every correlation filter before
// publication, performs a bounded EOSE preflight, and pins the expected server
// author. Wait then reconciles stored responses with the live subscriptions.
func PrepareContextVMResponseWait(ctx context.Context, relays []string, command *gonostr.Event, expectedAuthor string, timeout time.Duration) (*ContextVMResponseWaiter, error) {
	return prepareContextVMResponseWaitWithSource(ctx, relays, command, expectedAuthor, timeout, nip34.NewClient())
}

func prepareContextVMResponseWaitWithSource(ctx context.Context, relays []string, command *gonostr.Event, expectedAuthor string, timeout time.Duration, source ContextVMResponseSource) (*ContextVMResponseWaiter, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is nil")
	}
	if command == nil || command.ID == gonostr.ZeroID {
		return nil, fmt.Errorf("signed command event with id is required")
	}
	relays = cleanStrings(relays)
	if len(relays) == 0 {
		return nil, fmt.Errorf("at least one relay is required")
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	expectedAuthor = strings.ToLower(strings.TrimSpace(expectedAuthor))
	var expectedKey gonostr.PubKey
	if expectedAuthor != "" {
		var err error
		expectedKey, err = gonostr.PubKeyFromHex(expectedAuthor)
		if err != nil {
			return nil, fmt.Errorf("invalid expected ContextVM responder pubkey: %w", err)
		}
		expectedAuthor = expectedKey.Hex()
	}

	if source == nil {
		return nil, fmt.Errorf("ContextVM response source is nil")
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	filters := responseFilters(command, command.CreatedAt)
	if expectedAuthor != "" {
		for i := range filters {
			filters[i].Authors = []gonostr.PubKey{expectedKey}
		}
	}
	subscriptions := make([]<-chan gonostr.RelayEvent, 0, len(filters))
	for _, filter := range filters {
		ch, err := source.Subscribe(waitCtx, relays, filter)
		if err != nil {
			cancel()
			return nil, err
		}
		subscriptions = append(subscriptions, ch)
	}
	live, closed := mergeContextVMResponseSubscriptions(waitCtx, subscriptions...)
	preflight, err := source.FetchMany(waitCtx, relays, filters)
	if err != nil && !nip34.IsPartialFetch(err) {
		cancel()
		return nil, err
	}
	if waitCtx.Err() != nil {
		cancel()
		return nil, fmt.Errorf("prepare ContextVM response wait: %w", waitCtx.Err())
	}
	return &ContextVMResponseWaiter{
		ctx: waitCtx, cancel: cancel, source: source, relays: append([]string(nil), relays...), filters: filters,
		command: command, expectedAuthor: expectedAuthor, preflight: preflight, live: live, closed: closed,
	}, nil
}

func (w *ContextVMResponseWaiter) Close() {
	if w == nil {
		return
	}
	w.once.Do(func() {
		if w.cancel != nil {
			w.cancel()
		}
	})
}

func (w *ContextVMResponseWaiter) Wait() (*ContextVMResponse, error) {
	if w == nil || w.ctx == nil || w.source == nil || w.command == nil {
		return nil, fmt.Errorf("ContextVM response waiter is not initialized")
	}
	defer w.Close()
	for _, event := range w.preflight {
		if response, ok := matchAuthenticatedContextVMResponse(w.command, event, w.expectedAuthor); ok {
			return response, nil
		}
	}
	type fetchResult struct {
		events []*gonostr.Event
		err    error
	}
	reconciled := make(chan fetchResult, 1)
	go func() {
		events, err := w.source.FetchMany(w.ctx, w.relays, w.filters)
		reconciled <- fetchResult{events: events, err: err}
	}()
	live, closed := w.live, w.closed
	for {
		select {
		case <-w.ctx.Done():
			return nil, fmt.Errorf("wait for ContextVM response: %w", w.ctx.Err())
		case <-closed:
			closed = nil
			live = nil
			if reconciled == nil {
				return nil, fmt.Errorf("subscription closed before ContextVM response")
			}
		case result := <-reconciled:
			reconciled = nil
			for _, event := range result.events {
				if response, ok := matchAuthenticatedContextVMResponse(w.command, event, w.expectedAuthor); ok {
					return response, nil
				}
			}
			if result.err != nil && !nip34.IsPartialFetch(result.err) {
				return nil, result.err
			}
			if live == nil {
				return nil, fmt.Errorf("subscription closed before ContextVM response")
			}
		case relayEvent, ok := <-live:
			if !ok {
				live = nil
				if reconciled == nil {
					return nil, fmt.Errorf("subscription closed before ContextVM response")
				}
				continue
			}
			event := relayEvent.Event
			if response, ok := matchAuthenticatedContextVMResponse(w.command, &event, w.expectedAuthor); ok {
				return response, nil
			}
		}
	}
}

func WaitForContextVMResponseFrom(ctx context.Context, relays []string, command *gonostr.Event, expectedAuthor string, timeout time.Duration) (*ContextVMResponse, error) {
	waiter, err := PrepareContextVMResponseWait(ctx, relays, command, expectedAuthor, timeout)
	if err != nil {
		return nil, err
	}
	return waiter.Wait()
}

func WaitForContextVMResponse(ctx context.Context, relays []string, command *gonostr.Event, timeout time.Duration) (*ContextVMResponse, error) {
	return WaitForContextVMResponseFrom(ctx, relays, command, "", timeout)
}

func matchAuthenticatedContextVMResponse(command, candidate *gonostr.Event, expectedAuthor string) (*ContextVMResponse, bool) {
	if candidate == nil || !casnostr.VerifyEvent((*casnostr.Event)(candidate)) {
		return nil, false
	}
	if expectedAuthor != "" && candidate.PubKey.Hex() != expectedAuthor {
		return nil, false
	}
	return MatchContextVMResponse(command, candidate)
}

func mergeContextVMResponseSubscriptions(ctx context.Context, inputs ...<-chan gonostr.RelayEvent) (<-chan gonostr.RelayEvent, <-chan struct{}) {
	out := make(chan gonostr.RelayEvent, 64)
	closed := make(chan struct{}, 1)
	var wg sync.WaitGroup
	wg.Add(len(inputs))
	for _, input := range inputs {
		go func(ch <-chan gonostr.RelayEvent) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case event, ok := <-ch:
					if !ok {
						select {
						case closed <- struct{}{}:
						default:
						}
						return
					}
					select {
					case out <- event:
					case <-ctx.Done():
						return
					}
				}
			}
		}(input)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out, closed
}

func MatchContextVMResponse(command, candidate *gonostr.Event) (*ContextVMResponse, bool) {
	if command == nil || candidate == nil || candidate.Kind != nip34.KindContextVMIntent {
		return nil, false
	}
	cmdRPCID := jsonRPCID(command.Content)
	if !hasCorrelation(command, candidate, cmdRPCID) {
		return nil, false
	}
	var body struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  json.RawMessage `json:"result"`
		Error   json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal([]byte(candidate.Content), &body); err != nil {
		return nil, false
	}
	if len(body.Result) == 0 && (len(body.Error) == 0 || string(body.Error) == "null") {
		return nil, false
	}
	resp := &ContextVMResponse{Event: candidate, ID: rawIDString(body.ID)}
	if len(body.Result) > 0 {
		result := append(json.RawMessage(nil), body.Result...)
		resp.Result = &result
	}
	if len(body.Error) > 0 && string(body.Error) != "null" {
		var structured struct {
			Code    int             `json:"code"`
			Message string          `json:"message"`
			Data    json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(body.Error, &structured); err == nil && structured.Message != "" {
			resp.Error, resp.ErrorCode = structured.Message, structured.Code
			resp.ErrorData = append(json.RawMessage(nil), structured.Data...)
		} else if err := json.Unmarshal(body.Error, &resp.Error); err != nil {
			resp.Error = string(body.Error)
		}
	}
	return resp, true
}

func responseFilters(command *gonostr.Event, since gonostr.Timestamp) []gonostr.Filter {
	filters := []gonostr.Filter{{Kinds: []gonostr.Kind{gonostr.Kind(nip34.KindContextVMIntent)}, Tags: gonostr.TagMap{"e": []string{command.ID.Hex()}}, Since: since}}
	if id := jsonRPCID(command.Content); id != "" {
		filters = append(filters, gonostr.Filter{Kinds: []gonostr.Kind{gonostr.Kind(nip34.KindContextVMIntent)}, Tags: gonostr.TagMap{"correlation": []string{id}}, Since: since})
		filters = append(filters, gonostr.Filter{Kinds: []gonostr.Kind{gonostr.Kind(nip34.KindContextVMIntent)}, Tags: gonostr.TagMap{"request": []string{id}}, Since: since})
	}
	return filters
}

func hasCorrelation(command, candidate *gonostr.Event, cmdRPCID string) bool {
	if command.ID != gonostr.ZeroID {
		for _, e := range nip34.TagAll(candidate, "e") {
			if e == command.ID.Hex() {
				return true
			}
		}
	}
	if cmdRPCID != "" {
		for _, tag := range []string{"correlation", "request"} {
			for _, v := range nip34.TagAll(candidate, tag) {
				if v == cmdRPCID {
					return true
				}
			}
		}
		if jsonRPCID(candidate.Content) == cmdRPCID {
			return true
		}
	}
	return false
}

func jsonRPCID(content string) string {
	var body struct {
		ID json.RawMessage `json:"id"`
	}
	if err := json.Unmarshal([]byte(content), &body); err != nil {
		return ""
	}
	return rawIDString(body.ID)
}

func rawIDString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	var n float64
	if err := json.Unmarshal(raw, &n); err == nil {
		return fmt.Sprintf("%.0f", n)
	}
	return strings.TrimSpace(string(raw))
}
