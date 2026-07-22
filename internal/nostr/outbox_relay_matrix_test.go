package nostr

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	gonostr "fiatjaf.com/nostr"
)

func TestThreeRelayPublisherQuorumAndRecoveryMatrix(t *testing.T) {
	const (
		relay1 = "wss://relay-1"
		relay2 = "wss://relay-2"
		relay3 = "wss://relay-3"
	)
	cases := []struct {
		name        string
		failures    map[string]error
		wantError   bool
		wantAck     int
		recoverURLs []string
	}{
		{
			name:     "one relay down still reaches quorum",
			failures: map[string]error{relay3: errors.New("down")},
			wantAck:  2, recoverURLs: []string{relay3},
		},
		{
			name:      "two relays down remains durable below quorum",
			failures:  map[string]error{relay2: errors.New("down"), relay3: errors.New("down")},
			wantError: true, wantAck: 1, recoverURLs: []string{relay2, relay3},
		},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Unix(600+int64(i), 0).UTC()
			transport := &outboxTestTransport{failures: tc.failures}
			publisher := newTestReliablePublisher(
				t,
				filepath.Join(t.TempDir(), "outbox.json"),
				&now,
				transport,
				[]string{relay1, relay2, relay3},
				nil,
				2,
			)
			event := &gonostr.Event{ID: numberedID(20_000 + i), Kind: 30900, CreatedAt: gonostr.Timestamp(now.Unix())}
			reports, err := publisher.PublishSigned(context.Background(), event)
			if tc.wantError {
				var quorumErr *QuorumError
				if !errors.As(err, &quorumErr) {
					t.Fatalf("expected quorum error, got reports=%#v err=%v", reports, err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if len(reports) != 1 || reports[0].RequiredAcks != tc.wantAck ||
				reports[0].QuorumReached != (tc.wantAck >= 2) {
				t.Fatalf("unexpected report: %#v", reports)
			}
			entries, err := publisher.store.List()
			if err != nil || len(entries) != 1 {
				t.Fatalf("missing durable recovery entry: entries=%#v err=%v", entries, err)
			}

			for _, relay := range tc.recoverURLs {
				transport.setFailure(relay, nil)
			}
			now = now.Add(2 * time.Second)
			if _, err := publisher.DrainOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			entries, err = publisher.store.List()
			if err != nil || len(entries) != 0 {
				t.Fatalf("recovered relays did not drain queue: entries=%#v err=%v", entries, err)
			}
			if got := transport.callCount(relay1); got != 1 {
				t.Fatalf("already acknowledged relay retried: calls=%d", got)
			}
			for _, relay := range tc.recoverURLs {
				if got := transport.callCount(relay); got != 2 {
					t.Fatalf("recovered relay %s calls=%d, want 2", relay, got)
				}
			}
		})
	}
}
