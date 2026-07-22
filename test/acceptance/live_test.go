//go:build nostrig_acceptance

package acceptance

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gonostr "fiatjaf.com/nostr"
	nostrigNostr "github.com/chebizarro/nostrig/internal/nostr"
)

type liveAcceptanceConfig struct {
	relays       []string
	bunkerURL    string
	clientSecret string
	composeFile  string
}

func loadLiveAcceptanceConfig(t *testing.T) liveAcceptanceConfig {
	t.Helper()
	var relays []string
	for _, relay := range strings.Split(os.Getenv("NOSTRIG_ACCEPTANCE_RELAYS"), ",") {
		if relay = strings.TrimSpace(relay); relay != "" {
			relays = append(relays, relay)
		}
	}
	cfg := liveAcceptanceConfig{
		relays:       relays,
		bunkerURL:    strings.TrimSpace(os.Getenv("NOSTRIG_ACCEPTANCE_BUNKER_URL")),
		clientSecret: strings.TrimSpace(os.Getenv("NOSTRIG_ACCEPTANCE_CLIENT_SECRET")),
		composeFile:  strings.TrimSpace(os.Getenv("NOSTRIG_ACCEPTANCE_COMPOSE_FILE")),
	}
	if len(cfg.relays) < 3 || cfg.bunkerURL == "" || cfg.clientSecret == "" {
		t.Skip("live acceptance requires three relay URLs, a provisioned Signet bunker URL, and a stable NIP-46 client secret")
	}
	return cfg
}

func connectLiveSigner(t *testing.T, cfg liveAcceptanceConfig) *nostrigNostr.NIP46Signer {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	signer, err := nostrigNostr.ConnectNIP46Signer(ctx, cfg.bunkerURL, cfg.clientSecret)
	if err != nil {
		t.Fatalf("connect Signet: %v", err)
	}
	return signer
}

func TestLiveDisposableRelaysAndSignetAcceptance(t *testing.T) {
	cfg := loadLiveAcceptanceConfig(t)
	signer := connectLiveSigner(t, cfg)

	for _, count := range []int{2, 3} {
		count := count
		t.Run(fmt.Sprintf("%d-relay", count), func(t *testing.T) {
			publisher := liveReliablePublisher(t, cfg.relays[:count], count)
			event := &gonostr.Event{Kind: 30900, CreatedAt: gonostr.Timestamp(time.Now().Unix()), Content: fmt.Sprintf("nostrig-live-%d", count)}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			reports, err := publisher.PublishWithReport(ctx, signer, []*gonostr.Event{event})
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			if len(reports) != 1 || !reports[0].QuorumReached || reports[0].RequiredAcks != count {
				t.Fatalf("unexpected live relay report: %#v", reports)
			}
		})
	}

	t.Run("partial-relay-failure", func(t *testing.T) {
		relays := append([]string(nil), cfg.relays[:2]...)
		relays = append(relays, "ws://127.0.0.1:1")
		publisher := liveReliablePublisher(t, relays, 2)
		event := &gonostr.Event{Kind: 30900, CreatedAt: gonostr.Timestamp(time.Now().Unix()), Content: "nostrig-live-partial"}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		reports, err := publisher.PublishWithReport(ctx, signer, []*gonostr.Event{event})
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if len(reports) != 1 || !reports[0].QuorumReached || reports[0].RequiredAcks != 2 || !reports[0].Queued {
			t.Fatalf("partial failure did not retain recoverable delivery: %#v", reports)
		}
	})
}

func TestLiveSignetDisconnectReconnect(t *testing.T) {
	cfg := loadLiveAcceptanceConfig(t)
	if os.Getenv("NOSTRIG_ACCEPTANCE_CONTROL_SIGNET") != "1" || cfg.composeFile == "" {
		t.Skip("set NOSTRIG_ACCEPTANCE_CONTROL_SIGNET=1 and compose file to permit the destructive Signet restart check")
	}
	signer := connectLiveSigner(t, cfg)
	before := &gonostr.Event{Kind: 1, CreatedAt: gonostr.Timestamp(time.Now().Unix()), Content: "before restart"}
	if err := signer.SignEvent(context.Background(), before); err != nil {
		t.Fatal(err)
	}

	runCompose(t, cfg.composeFile, "stop", "signetd")
	restarted := false
	defer func() {
		if !restarted {
			_ = exec.Command("docker", "compose", "-f", cfg.composeFile, "start", "signetd").Run()
		}
	}()
	outageCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	err := signer.SignEvent(outageCtx, &gonostr.Event{Kind: 1, CreatedAt: gonostr.Timestamp(time.Now().Unix()), Content: "during outage"})
	cancel()
	if err == nil {
		t.Fatal("signing unexpectedly succeeded while Signet was stopped")
	}

	runCompose(t, cfg.composeFile, "start", "signetd")
	restarted = true
	deadline := time.Now().Add(45 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		reconnected, connectErr := nostrigNostr.ConnectNIP46Signer(ctx, cfg.bunkerURL, cfg.clientSecret)
		cancel()
		if connectErr == nil {
			event := &gonostr.Event{Kind: 1, CreatedAt: gonostr.Timestamp(time.Now().Unix()), Content: "after restart"}
			ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
			signErr := reconnected.SignEvent(ctx, event)
			cancel()
			if signErr == nil {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("Signet did not reconnect before deadline: %v", connectErr)
		}
		time.Sleep(time.Second)
	}
}

func liveReliablePublisher(t *testing.T, relays []string, quorum int) *nostrigNostr.ReliablePublisher {
	t.Helper()
	publisher, err := nostrigNostr.NewReliablePublisher(nostrigNostr.ReliablePublisherOptions{
		RequiredRelays:      relays,
		AckQuorum:           quorum,
		OutboxPath:          filepath.Join(t.TempDir(), "outbox.json"),
		PublishTimeout:      5 * time.Second,
		BaseBackoff:         time.Second,
		MaxBackoff:          time.Second,
		MaxAttempts:         1,
		CircuitFailureLimit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(publisher.Close)
	return publisher
}

func runCompose(t *testing.T, composeFile string, args ...string) {
	t.Helper()
	commandArgs := append([]string{"compose", "-f", composeFile}, args...)
	command := exec.Command("docker", commandArgs...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\\n%s", strings.Join(commandArgs, " "), err, output)
	}
}
