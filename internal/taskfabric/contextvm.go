package taskfabric

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	nip34 "github.com/chebizarro/nostrig/internal/nostr"
	gonostr "github.com/nbd-wtf/go-nostr"
)

type ContextVMResponse struct {
	Event  *gonostr.Event   `json:"event,omitempty"`
	ID     string           `json:"id,omitempty"`
	Result *json.RawMessage `json:"result,omitempty"`
	Error  string           `json:"error,omitempty"`
}

func WaitForContextVMResponse(ctx context.Context, relays []string, command *gonostr.Event, timeout time.Duration) (*ContextVMResponse, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is nil")
	}
	if command == nil || strings.TrimSpace(command.ID) == "" {
		return nil, fmt.Errorf("signed command event with id is required")
	}
	relays = cleanStrings(relays)
	if len(relays) == 0 {
		return nil, fmt.Errorf("at least one relay is required")
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	since := command.CreatedAt
	filters := responseFilters(command, since)
	client := nip34.NewClient()
	if events, err := client.FetchMany(ctx, relays, filters); err == nil {
		for _, ev := range events {
			if resp, ok := MatchContextVMResponse(command, ev); ok {
				return resp, nil
			}
		}
	} else if ctx.Err() != nil {
		return nil, err
	}

	watchCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	pool := gonostr.NewSimplePool(watchCtx)
	ch := pool.SubMany(watchCtx, relays, gonostr.Filters(filters))
	for {
		select {
		case ie, ok := <-ch:
			if !ok {
				if err := watchCtx.Err(); err != nil {
					return nil, fmt.Errorf("wait for ContextVM response: %w", err)
				}
				return nil, fmt.Errorf("subscription closed before ContextVM response")
			}
			if ie.Event == nil {
				continue
			}
			if resp, ok := MatchContextVMResponse(command, ie.Event); ok {
				return resp, nil
			}
		case <-watchCtx.Done():
			return nil, fmt.Errorf("wait for ContextVM response: %w", watchCtx.Err())
		}
	}
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
		Error   any             `json:"error"`
	}
	if err := json.Unmarshal([]byte(candidate.Content), &body); err != nil {
		return nil, false
	}
	if len(body.Result) == 0 && body.Error == nil {
		return nil, false
	}
	resp := &ContextVMResponse{Event: candidate, ID: rawIDString(body.ID)}
	if len(body.Result) > 0 {
		result := append(json.RawMessage(nil), body.Result...)
		resp.Result = &result
	}
	if body.Error != nil {
		switch v := body.Error.(type) {
		case string:
			resp.Error = v
		default:
			encoded, _ := json.Marshal(v)
			resp.Error = string(encoded)
		}
	}
	return resp, true
}

func responseFilters(command *gonostr.Event, since gonostr.Timestamp) []gonostr.Filter {
	filters := []gonostr.Filter{{Kinds: []int{nip34.KindContextVMIntent}, Tags: gonostr.TagMap{"e": []string{command.ID}}, Since: &since}}
	if id := jsonRPCID(command.Content); id != "" {
		filters = append(filters, gonostr.Filter{Kinds: []int{nip34.KindContextVMIntent}, Tags: gonostr.TagMap{"correlation": []string{id}}, Since: &since})
		filters = append(filters, gonostr.Filter{Kinds: []int{nip34.KindContextVMIntent}, Tags: gonostr.TagMap{"request": []string{id}}, Since: &since})
	}
	return filters
}

func hasCorrelation(command, candidate *gonostr.Event, cmdRPCID string) bool {
	if command.ID != "" {
		for _, e := range nip34.TagAll(candidate, "e") {
			if e == command.ID {
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
