package taskfabric

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	gonostr "fiatjaf.com/nostr"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
)

func TestMatchContextVMResponseByEventTag(t *testing.T) {
	cmd, err := nip34.BuildClaimDispatch("task-1", "agent-a", "recipient", time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	cmd.ID = testID(4)
	respEvent := &gonostr.Event{Kind: nip34.KindContextVMIntent, Tags: gonostr.Tags{{"e", cmd.ID.Hex()}}, Content: `{"jsonrpc":"2.0","id":"` + jsonRPCID(cmd.Content) + `","result":{"ok":true}}`}
	resp, ok := MatchContextVMResponse(cmd, respEvent)
	if !ok {
		t.Fatal("response did not match")
	}
	if resp.Result == nil || !strings.Contains(string(*resp.Result), "ok") {
		t.Fatalf("unexpected response: %#v", resp)
	}
}

type responseWaitSource struct {
	subscriptions []chan gonostr.RelayEvent
	fetchCalls    int
}

func (s *responseWaitSource) FetchMany(context.Context, []string, []gonostr.Filter) ([]*gonostr.Event, error) {
	s.fetchCalls++
	return nil, nil
}

func (s *responseWaitSource) Subscribe(_ context.Context, _ []string, _ gonostr.Filter) (<-chan gonostr.RelayEvent, error) {
	ch := make(chan gonostr.RelayEvent, 4)
	s.subscriptions = append(s.subscriptions, ch)
	return ch, nil
}

func TestPreparedResponseWaiterObservesFastResponseAndRejectsWrongAuthor(t *testing.T) {
	command, err := nip34.BuildClaimDispatch("task-1", "agent-a", fmt.Sprintf("%064x", 9), time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Sign(gonostr.Generate()); err != nil {
		t.Fatal(err)
	}
	serverKey := gonostr.Generate()
	serverProbe := &gonostr.Event{Kind: nip34.KindContextVMIntent, CreatedAt: gonostr.Timestamp(2), Content: "probe"}
	if err := serverProbe.Sign(serverKey); err != nil {
		t.Fatal(err)
	}
	source := &responseWaitSource{}
	waiter, err := prepareContextVMResponseWaitWithSource(context.Background(), []string{"wss://relay.example"}, command, serverProbe.PubKey.Hex(), time.Second, source)
	if err != nil {
		t.Fatal(err)
	}
	defer waiter.Close()
	if len(source.subscriptions) == 0 || source.fetchCalls != 1 {
		t.Fatalf("waiter was not subscribed and preflighted before publish: subscriptions=%d fetches=%d", len(source.subscriptions), source.fetchCalls)
	}

	wrong := signedResponseEvent(t, command, gonostr.Generate(), "wrong")
	right := signedResponseEvent(t, command, serverKey, "right")
	source.subscriptions[0] <- gonostr.RelayEvent{Event: *wrong}
	source.subscriptions[0] <- gonostr.RelayEvent{Event: *right}
	response, err := waiter.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if response.Result == nil || !strings.Contains(string(*response.Result), "right") {
		t.Fatalf("wrong-author response was accepted or right response lost: %#v", response)
	}
}

func signedResponseEvent(t *testing.T, command *gonostr.Event, key gonostr.SecretKey, marker string) *gonostr.Event {
	t.Helper()
	event := &gonostr.Event{
		Kind: nip34.KindContextVMIntent, CreatedAt: gonostr.Timestamp(2),
		Tags:    gonostr.Tags{{"e", command.ID.Hex()}},
		Content: `{"jsonrpc":"2.0","id":"` + jsonRPCID(command.Content) + `","result":{"marker":"` + marker + `"}}`,
	}
	if err := event.Sign(key); err != nil {
		t.Fatal(err)
	}
	return event
}

func TestMatchContextVMResponseRejectsUncorrelated(t *testing.T) {
	cmd, err := nip34.BuildClaimDispatch("task-1", "agent-a", "recipient", time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	cmd.ID = testID(4)
	respEvent := &gonostr.Event{Kind: nip34.KindContextVMIntent, Tags: gonostr.Tags{{"e", "other"}}, Content: `{"jsonrpc":"2.0","id":"other","result":{"ok":true}}`}
	if _, ok := MatchContextVMResponse(cmd, respEvent); ok {
		t.Fatal("uncorrelated response matched")
	}
}

func TestMatchContextVMResponsePreservesStructuredError(t *testing.T) {
	cmd, err := nip34.BuildClaimDispatch("task-1", "agent-a", "recipient", time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	cmd.ID = testID(4)
	respEvent := &gonostr.Event{
		Kind: nip34.KindContextVMIntent, Tags: gonostr.Tags{{"e", cmd.ID.Hex()}},
		Content: `{"jsonrpc":"2.0","id":"` + jsonRPCID(cmd.Content) + `","error":{"code":-32009,"message":"task conflict","data":{"reason":"stale_revision"}}}`,
	}
	response, ok := MatchContextVMResponse(cmd, respEvent)
	if !ok || response.ErrorCode != ConflictErrorCode || response.Error != "task conflict" || !strings.Contains(string(response.ErrorData), "stale_revision") {
		t.Fatalf("structured error lost: %#v", response)
	}
}
