package taskfabric

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	gonostr "fiatjaf.com/nostr"
	cascontextvm "git.sharegap.net/cascadia/cascadia-go/contextvm"
	beadspb "github.com/chebizarro/nostrig/gen/beads"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
)

const maxFuzzEventBytes = 1 << 20

func FuzzContextVMIntentEventJSON(f *testing.F) {
	addContextVMFixtureSeeds(f, "intents")
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > maxFuzzEventBytes {
			t.Skip()
		}
		var event gonostr.Event
		if err := json.Unmarshal(data, &event); err != nil {
			return
		}
		ledger := &memoryLedger{
			tasks: map[string]*beadspb.Issue{
				"task-1": authzTestIssue("task-1", "30617:owner:repo", ""),
			},
			queues: map[string][]string{"30617:owner:repo|backlog": {"task-1"}},
		}
		handler := testHandler(ledger)
		response, err := handler.HandleIntent(context.Background(), &event, time.Unix(1, 0))
		if err != nil {
			return
		}
		if response == nil || response.Kind != gonostr.Kind(nip34.KindContextVMIntent) {
			t.Fatalf("successful parse returned invalid response: %#v", response)
		}
		var body cascontextvm.Response
		if err := json.Unmarshal([]byte(response.Content), &body); err != nil {
			t.Fatalf("handler emitted malformed ContextVM response: %v", err)
		}
		if body.JSONRPC != cascontextvm.JSONRPCVersion {
			t.Fatalf("handler emitted JSON-RPC version %q", body.JSONRPC)
		}
	})
}

func FuzzContextVMResponseEventJSON(f *testing.F) {
	addContextVMFixtureSeeds(f, "responses")
	command := &gonostr.Event{
		ID: testID(1), Kind: gonostr.Kind(nip34.KindContextVMIntent), CreatedAt: 1,
		Content: `{"jsonrpc":"2.0","id":"seed-request","method":"queue/list","params":{}}`,
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > maxFuzzEventBytes {
			t.Skip()
		}
		var candidate gonostr.Event
		if err := json.Unmarshal(data, &candidate); err != nil {
			return
		}
		response, matched := MatchContextVMResponse(command, &candidate)
		if !matched {
			return
		}
		if response == nil || response.Event != &candidate {
			t.Fatal("matched response did not preserve its source event")
		}
		if response.Result == nil && response.Error == "" {
			t.Fatal("matched response contained neither result nor error")
		}
	})
}

func addContextVMFixtureSeeds(f *testing.F, class string) {
	f.Helper()
	matches, err := filepath.Glob(filepath.Join("testdata", "contextvm", class, "*.json"))
	if err != nil {
		f.Fatal(err)
	}
	if len(matches) == 0 {
		f.Fatalf("no checked-in %s fixtures", class)
	}
	for _, path := range matches {
		data, err := os.ReadFile(path)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(data)
	}
}
