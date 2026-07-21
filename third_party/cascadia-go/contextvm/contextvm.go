package contextvm

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	gonostr "fiatjaf.com/nostr"
	cascadia "git.sharegap.net/cascadia/cascadia-go"
	casnostr "git.sharegap.net/cascadia/cascadia-go/nostr"
)

const (
	JSONRPCVersion = "2.0"

	ParseErrorCode     = -32700
	InvalidRequestCode = -32600
	MethodNotFoundCode = -32601
	InvalidParamsCode  = -32602
	InternalErrorCode  = -32603
)

const (
	tagReply     = "e"
	tagRecipient = "p"
)

type Meta struct {
	ProgressToken string `json:"progressToken,omitempty"`
}

type Params struct {
	Meta *Meta `json:"_meta,omitempty"`
}

type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type Response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *Error          `json:"error,omitempty"`
}

type Notification struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type Error struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func NewRequest(id json.RawMessage, method string, params any) (Request, error) {
	raw, err := marshalParams(params)
	if err != nil {
		return Request{}, err
	}
	return Request{JSONRPC: JSONRPCVersion, ID: id, Method: method, Params: raw}, nil
}

func NewResponse(id json.RawMessage, result any) Response {
	return Response{JSONRPC: JSONRPCVersion, ID: responseID(id), Result: result}
}

func NewErrorResponse(id json.RawMessage, code int, message string) Response {
	return Response{JSONRPC: JSONRPCVersion, ID: responseID(id), Error: &Error{Code: code, Message: message}}
}

func (r Request) IDOrNull() json.RawMessage {
	return responseID(r.ID)
}

func ProgressToken(params json.RawMessage) string {
	if len(params) == 0 {
		return ""
	}
	var decoded struct {
		Meta *Meta `json:"_meta"`
	}
	if err := json.Unmarshal(params, &decoded); err != nil || decoded.Meta == nil {
		return ""
	}
	return strings.TrimSpace(decoded.Meta.ProgressToken)
}

func marshalParams(params any) (json.RawMessage, error) {
	if params == nil {
		return nil, nil
	}
	if raw, ok := params.(json.RawMessage); ok {
		return raw, nil
	}
	b, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func responseID(id json.RawMessage) json.RawMessage {
	if len(id) == 0 {
		return json.RawMessage("null")
	}
	return id
}

// Wrap builds a CAS_INTENT inner event and NIP-59 gift-wraps it for recipientPubkey.
func Wrap(ctx context.Context, signer casnostr.Signer, recipientPubkey string, payload any) (*casnostr.Event, *casnostr.Event, error) {
	if signer == nil {
		return nil, nil, fmt.Errorf("contextvm signer is nil")
	}
	recipientPubkey = strings.TrimSpace(recipientPubkey)
	if recipientPubkey == "" {
		return nil, nil, fmt.Errorf("recipient pubkey is required")
	}
	recipient, err := gonostr.PubKeyFromHex(recipientPubkey)
	if err != nil {
		return nil, nil, fmt.Errorf("parse recipient pubkey: %w", err)
	}
	content, err := json.Marshal(payload)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal ContextVM payload: %w", err)
	}
	inner, err := casnostr.BuildEvent(ctx, signer, cascadia.CAS_INTENT, gonostr.Tags{{tagRecipient, recipientPubkey}}, string(content))
	if err != nil {
		return nil, nil, fmt.Errorf("sign ContextVM intent: %w", err)
	}
	innerJSON, err := json.Marshal(inner)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal ContextVM intent: %w", err)
	}
	ciphertext, err := signer.Encrypt(ctx, string(innerJSON), recipient)
	if err != nil {
		return nil, nil, fmt.Errorf("encrypt ContextVM gift wrap: %w", err)
	}
	outer, err := casnostr.BuildEvent(ctx, signer, cascadia.NIP59_GIFT_WRAP, gonostr.Tags{{tagRecipient, recipientPubkey}}, ciphertext)
	if err != nil {
		return nil, nil, fmt.Errorf("sign ContextVM gift wrap: %w", err)
	}
	return outer, inner, nil
}

func Unwrap(ctx context.Context, signer casnostr.Signer, outer *casnostr.Event) (*casnostr.Event, error) {
	if signer == nil {
		return nil, fmt.Errorf("contextvm signer is nil")
	}
	if outer == nil {
		return nil, fmt.Errorf("gift wrap event is nil")
	}
	if outer.Kind != cascadia.NIP59_GIFT_WRAP {
		return nil, fmt.Errorf("unexpected gift wrap kind %d", outer.Kind)
	}
	plaintext, err := signer.Decrypt(ctx, outer.Content, outer.PubKey)
	if err != nil {
		return nil, fmt.Errorf("decrypt ContextVM gift wrap: %w", err)
	}
	var inner casnostr.Event
	if err := json.Unmarshal([]byte(plaintext), &inner); err != nil {
		return nil, fmt.Errorf("decode ContextVM intent: %w", err)
	}
	if inner.Kind != cascadia.CAS_INTENT {
		return nil, fmt.Errorf("unexpected ContextVM intent kind %d", inner.Kind)
	}
	return &inner, nil
}

func PublishWrap(ctx context.Context, relayURLs []string, signer casnostr.Signer, recipientPubkey string, payload any) (*casnostr.Event, error) {
	outer, _, err := Wrap(ctx, signer, recipientPubkey, payload)
	if err != nil {
		return nil, err
	}
	accepted, err := casnostr.Publish(ctx, relayURLs, *outer)
	if err != nil {
		return nil, err
	}
	if accepted == 0 {
		return nil, fmt.Errorf("no relay accepted ContextVM gift wrap")
	}
	return outer, nil
}

type Handler func(context.Context, Request) (any, error)

type Registry struct {
	handlers map[string]Handler
}

func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

func Method(domain, op string) string {
	return strings.Trim(strings.TrimSpace(domain), "/") + "/" + strings.Trim(strings.TrimSpace(op), "/")
}

func (r *Registry) Register(domain, op string, handler Handler) {
	if r == nil || handler == nil {
		return
	}
	method := Method(domain, op)
	if method == "/" || strings.TrimSpace(method) == "" {
		return
	}
	r.handlers[method] = handler
}

func (r *Registry) Dispatch(ctx context.Context, request Request) Response {
	if request.JSONRPC != JSONRPCVersion || strings.TrimSpace(request.Method) == "" {
		return NewErrorResponse(request.ID, InvalidRequestCode, "invalid request")
	}
	if r == nil || r.handlers == nil {
		return NewErrorResponse(request.ID, MethodNotFoundCode, "method not found")
	}
	handler := r.handlers[request.Method]
	if handler == nil {
		return NewErrorResponse(request.ID, MethodNotFoundCode, "method not found")
	}
	result, err := handler(ctx, request)
	if err != nil {
		return NewErrorResponse(request.ID, InternalErrorCode, err.Error())
	}
	return NewResponse(request.ID, result)
}
