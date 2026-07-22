package taskfabric

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gonostr "fiatjaf.com/nostr"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
)

type staticPublisherSnapshot struct {
	snapshot nip34.PublisherSnapshot
	err      error
}

func (s *staticPublisherSnapshot) Snapshot() (nip34.PublisherSnapshot, error) {
	return s.snapshot, s.err
}

func TestObservabilitySeparatesLivenessAndAuthoritativeReadiness(t *testing.T) {
	relay := "wss://user:password@relay.example/private?token=secret"
	observer := newServiceObserver([]string{relay}, 1, 10)
	handler := observer.handler()

	assertStatus := func(path string, want int) string {
		t.Helper()
		request := httptest.NewRequest(http.MethodGet, path, nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != want {
			t.Fatalf("%s status=%d want=%d body=%s", path, response.Code, want, response.Body.String())
		}
		return response.Body.String()
	}

	assertStatus("/livez", http.StatusOK)
	assertStatus("/readyz", http.StatusServiceUnavailable)

	publisher := &staticPublisherSnapshot{}
	durablePath := filepath.Join(t.TempDir(), "commands.json")
	observer.setCheck("initial_backfill_complete", true, "")
	observer.refresh(context.Background(), testServeSigner{}, func(string) bool { return true }, []string{durablePath}, publisher)
	assertStatus("/readyz", http.StatusOK)

	publisher.snapshot.QueueDepth = 10
	observer.refresh(context.Background(), testServeSigner{}, func(string) bool { return true }, []string{durablePath}, publisher)
	assertStatus("/readyz", http.StatusServiceUnavailable)

	publisher.snapshot = nip34.PublisherSnapshot{Circuits: []nip34.RelayCircuitSnapshot{{
		URL: relay, Required: true, Open: true, OpenUntil: time.Now().Add(time.Minute),
	}}}
	observer.refresh(context.Background(), testServeSigner{}, func(string) bool { return true }, []string{durablePath}, publisher)
	assertStatus("/readyz", http.StatusServiceUnavailable)

	diagnostics := assertStatus("/diagnostics", http.StatusOK)
	for _, secret := range []string{"user", "password", "private", "token", "secret"} {
		if strings.Contains(diagnostics, secret) {
			t.Fatalf("diagnostics leaked %q: %s", secret, diagnostics)
		}
	}
}

func TestObservabilityExportsRequiredCountersHistogramsAndLastEvents(t *testing.T) {
	observer := newServiceObserver([]string{"wss://relay.example"}, 1, 10)
	event := &gonostr.Event{ID: testID(42), Kind: 25910}
	observer.recordProcessed(event, 12*time.Millisecond)
	observer.recordPublished([]*gonostr.Event{event}, 20*time.Millisecond)
	observer.observeResponse(&gonostr.Event{Content: `{"error":{"code":-32009,"message":"conflict"}}`})
	observer.recordReplay()
	audit := observedAuditSink{next: discardAudit{}, observer: observer}
	if err := audit.Record(context.Background(), AuthzAuditRecord{Decision: "deny"}); err != nil {
		t.Fatal(err)
	}
	observer.mu.Lock()
	observer.outbox = nip34.PublisherSnapshot{
		QueueDepth: 3, DeadLetters: 1, RetryTotal: 4, DeadLetterTotal: 2, PublishedTotal: 1,
	}
	observer.mu.Unlock()

	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	observer.handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("metrics status=%d body=%s", response.Code, response.Body.String())
	}
	body := response.Body.String()
	for _, metric := range []string{
		"nostrig_events_processed_total 1",
		"nostrig_events_published_total 1",
		"nostrig_conflicts_total 1",
		"nostrig_denials_total 1",
		"nostrig_retries_total 4",
		"nostrig_dead_letters_total 2",
		"nostrig_replays_total 1",
		"nostrig_outbox_queue_depth 3",
		"nostrig_outbox_dead_letter_depth 1",
		"nostrig_operation_duration_seconds_bucket",
		"nostrig_last_processed_event_info",
		"nostrig_last_published_event_info",
	} {
		if !strings.Contains(body, metric) {
			t.Errorf("metrics missing %q:\n%s", metric, body)
		}
	}
}
