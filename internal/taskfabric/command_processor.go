package taskfabric

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	gonostr "fiatjaf.com/nostr"
	cascadia "git.sharegap.net/cascadia/cascadia-go"
	cascontextvm "git.sharegap.net/cascadia/cascadia-go/contextvm"
	casnostr "git.sharegap.net/cascadia/cascadia-go/nostr"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
)

const (
	requestIDConflictCode = -32010
	replayExpiredCode     = -32011
)

type commandProcessor struct {
	journal        *CommandJournal
	handler        *Handler
	signer         nip34.Signer
	contextSigner  casnostr.Signer
	relays         []string
	unwrap         serveUnwrapper
	wrap           serveWrapper
	publishWrapped serveWrappedPublisher
	publishPlain   func(context.Context, *gonostr.Event) error
	reportError    serveErrorReporter
	verify         serveVerifier
	now            func() time.Time
	phaseHook      func(CommandPhase)
	responseHook   func(*gonostr.Event)
	replayHook     func()
	processHook    func(*gonostr.Event, time.Duration)

	mu sync.Mutex
}

func (p *commandProcessor) Process(ctx context.Context, source *gonostr.Event) (processErr error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	started := time.Now()
	defer func() {
		if processErr == nil && source != nil && p.processHook != nil {
			p.processHook(source, time.Since(started))
		}
	}()
	if source == nil {
		return nil
	}
	if p.verify == nil {
		p.verify = func(event *gonostr.Event) bool { return casnostr.VerifyEvent((*casnostr.Event)(event)) }
	}
	if !p.verify(source) {
		p.report("verify", fmt.Errorf("invalid event id or signature"), source)
		return nil
	}
	command := source
	wrapped := source.Kind == gonostr.Kind(cascadia.NIP59_GIFT_WRAP)
	if wrapped {
		inner, err := p.unwrap(ctx, p.contextSigner, source)
		if err != nil {
			p.report("unwrap", err, source)
			return nil
		}
		command = inner
	}
	if err := validateIntent(command, p.handler.Recipient, p.verify); err != nil {
		p.report("validate_intent", err, command)
		return nil
	}
	return p.processCommand(ctx, source, command, wrapped)
}

func (p *commandProcessor) Resume(ctx context.Context, record *CommandRecord) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if record == nil {
		return nil
	}
	return p.runRecord(ctx, cloneCommandRecord(record))
}

func (p *commandProcessor) processCommand(ctx context.Context, source, command *gonostr.Event, wrapped bool) error {
	now := p.timeNow()
	if p.journal == nil {
		record := &CommandRecord{EventID: command.ID.Hex(), SourceEventID: source.ID.Hex(), Source: cloneNostrEvent(*source), Command: cloneNostrEvent(*command), Wrapped: wrapped, Phase: CommandReceived, ReceivedAt: now, UpdatedAt: now}
		return p.runRecord(ctx, record)
	}
	record, created, err := p.journal.Begin(source, command, wrapped, now)
	if err != nil {
		var conflict *RequestIDConflictError
		switch {
		case errors.As(err, &conflict):
			p.report("request_id_conflict", err, command)
			return p.publishProtocolError(ctx, command, wrapped, requestIDConflictCode, "request id reused with different content")
		case errors.Is(err, ErrReplayExpired):
			p.report("replay_expired", err, command)
			return p.publishProtocolError(ctx, command, wrapped, replayExpiredCode, "command is outside the replay retention window")
		default:
			return fmt.Errorf("begin durable command: %w", err)
		}
	}
	if created {
		p.hook(CommandReceived)
	} else if p.replayHook != nil {
		p.replayHook()
	}
	return p.runRecord(ctx, record)
}

func (p *commandProcessor) runRecord(ctx context.Context, record *CommandRecord) error {
	if record.Response != nil {
		if err := p.publishStored(ctx, record); err != nil {
			return err
		}
		if p.journal != nil && record.Phase != CommandComplete {
			if _, err := p.journal.Complete(record.EventID, p.timeNow()); err != nil {
				return fmt.Errorf("complete durable command: %w", err)
			}
			p.hook(CommandComplete)
		}
		return nil
	}

	response, err := p.handler.HandleIntent(ctx, &record.Command, p.timeNow())
	if err != nil {
		p.report("handle_intent", err, &record.Command)
		return nil
	}
	if p.responseHook != nil {
		p.responseHook(response)
	}
	outbound, err := p.prepareResponse(ctx, record.Wrapped, &record.Command, response)
	if err != nil {
		return err
	}
	record.Response = outbound
	record.ResponseJSON = response.Content
	record.Phase = CommandResponsePending
	if p.journal != nil {
		var saved *CommandRecord
		saved, err = p.journal.SaveResponse(record.EventID, outbound, response.Content, p.timeNow())
		if err != nil {
			return fmt.Errorf("cache durable response: %w", err)
		}
		record = saved
		p.hook(CommandResponsePending)
	}
	if err := p.publishStored(ctx, record); err != nil {
		return err
	}
	if p.journal != nil {
		if _, err := p.journal.Complete(record.EventID, p.timeNow()); err != nil {
			return fmt.Errorf("complete durable command: %w", err)
		}
		p.hook(CommandComplete)
	}
	return nil
}

func (p *commandProcessor) prepareResponse(ctx context.Context, wrapped bool, command, response *gonostr.Event) (*gonostr.Event, error) {
	if wrapped {
		outer, err := p.wrap(ctx, p.contextSigner, command.PubKey.Hex(), json.RawMessage(response.Content))
		if err != nil {
			p.report("wrap_response", err, command)
			return nil, fmt.Errorf("wrap ContextVM response: %w", err)
		}
		return outer, nil
	}
	if err := p.signer.SignEvent(ctx, response); err != nil {
		p.report("sign_response", err, response)
		return nil, fmt.Errorf("sign ContextVM response: %w", err)
	}
	return response, nil
}

func (p *commandProcessor) publishStored(ctx context.Context, record *CommandRecord) error {
	if record == nil || record.Response == nil {
		return fmt.Errorf("cached response is missing")
	}
	if record.Wrapped {
		if err := p.publishWrapped(ctx, p.relays, record.Response); err != nil {
			p.report("publish_wrapped_response", err, record.Response)
			return fmt.Errorf("publish wrapped ContextVM response: %w", err)
		}
		return nil
	}
	if err := p.publishPlain(ctx, record.Response); err != nil {
		p.report("publish_response", err, record.Response)
		return fmt.Errorf("publish ContextVM response: %w", err)
	}
	return nil
}

func (p *commandProcessor) publishProtocolError(ctx context.Context, command *gonostr.Event, wrapped bool, code int, message string) error {
	var request cascontextvm.Request
	if err := json.Unmarshal([]byte(command.Content), &request); err != nil {
		return nil
	}
	body := cascontextvm.NewErrorResponse(request.ID, code, message)
	response, err := newContextVMResponseEvent(command, request, body, p.timeNow())
	if err != nil {
		return err
	}
	outbound, err := p.prepareResponse(ctx, wrapped, command, response)
	if err != nil {
		return err
	}
	record := &CommandRecord{Wrapped: wrapped, Response: outbound}
	return p.publishStored(ctx, record)
}

func (p *commandProcessor) report(stage string, err error, event *gonostr.Event) {
	if p.reportError != nil {
		p.reportError(stage, err, event)
	}
}

func (p *commandProcessor) timeNow() time.Time {
	if p.now != nil {
		return p.now().UTC()
	}
	return time.Now().UTC()
}

func (p *commandProcessor) hook(phase CommandPhase) {
	if p.phaseHook != nil {
		p.phaseHook(phase)
	}
}

func repairPendingCommands(ctx context.Context, processor *commandProcessor, journal *CommandJournal) error {
	if journal == nil {
		return nil
	}
	pending, err := journal.Pending()
	if err != nil {
		return err
	}
	for _, record := range pending {
		if err := processor.Resume(ctx, record); err != nil {
			return err
		}
		if err := journal.AdvanceCursor(&record.Source); err != nil {
			return err
		}
	}
	return nil
}

func processCommandBackfill(ctx context.Context, processor *commandProcessor, journal *CommandJournal, events []*gonostr.Event) error {
	if journal == nil {
		return nil
	}
	cursor, err := journal.Cursor()
	if err != nil {
		return err
	}
	ordered := make([]*gonostr.Event, 0, len(events))
	for _, event := range events {
		if event != nil {
			ordered = append(ordered, event)
		}
	}
	sort.Slice(ordered, func(i, k int) bool {
		if ordered[i].CreatedAt != ordered[k].CreatedAt {
			return ordered[i].CreatedAt < ordered[k].CreatedAt
		}
		return ordered[i].ID.Hex() < ordered[k].ID.Hex()
	})
	for _, event := range ordered {
		if event == nil || !cursor.Before(event) {
			continue
		}
		if err := processor.Process(ctx, event); err != nil {
			return err
		}
		if err := journal.AdvanceCursor(event); err != nil {
			return err
		}
		cursor = CommandCursor{CreatedAt: event.CreatedAt, EventID: event.ID.Hex()}
	}
	return nil
}
