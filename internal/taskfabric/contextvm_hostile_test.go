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

func TestMalformedAndHostileContextVMEventCorpus(t *testing.T) {
	command := &gonostr.Event{
		ID: testID(1), Kind: gonostr.Kind(nip34.KindContextVMIntent), CreatedAt: 1,
		Content: `{"jsonrpc":"2.0","id":"seed-request","method":"queue/list","params":{}}`,
	}
	total := 0
	for _, class := range []string{"intents", "responses"} {
		matches, err := filepath.Glob(filepath.Join("testdata", "contextvm", class, "*.json"))
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range matches {
			total++
			t.Run(class+"/"+filepath.Base(path), func(t *testing.T) {
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				var event gonostr.Event
				if err := json.Unmarshal(data, &event); err != nil {
					return // malformed wire JSON is an expected hostile-corpus outcome
				}
				switch class {
				case "intents":
					ledger := &memoryLedger{
						tasks: map[string]*beadspb.Issue{
							"task-1": authzTestIssue("task-1", "30617:owner:repo", ""),
						},
						queues: map[string][]string{},
					}
					response, err := testHandler(ledger).HandleIntent(context.Background(), &event, time.Unix(1, 0))
					if err == nil {
						if response == nil || !json.Valid([]byte(response.Content)) {
							t.Fatalf("accepted hostile input produced malformed response: %#v", response)
						}
						var body cascontextvm.Response
						if err := json.Unmarshal([]byte(response.Content), &body); err != nil {
							t.Fatal(err)
						}
					}
				case "responses":
					if response, matched := MatchContextVMResponse(command, &event); matched {
						if response == nil || (response.Result == nil && response.Error == "") {
							t.Fatal("hostile response matched without result or error")
						}
					}
					if _, authenticated := matchAuthenticatedContextVMResponse(command, &event, ""); authenticated {
						t.Fatal("unsigned hostile corpus event authenticated")
					}
				}
			})
		}
	}
	if total < 6 {
		t.Fatalf("hostile corpus has only %d cases; want at least 6", total)
	}
}
