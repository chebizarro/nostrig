package acceptance

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

type scenarioContract struct {
	Version string   `json:"version"`
	Actors  []string `json:"actors"`
	Steps   []struct {
		ID        int      `json:"id"`
		Actor     string   `json:"actor"`
		Operation string   `json:"operation"`
		Expected  string   `json:"expected"`
		Evidence  []string `json:"evidence"`
	} `json:"steps"`
}

func TestThreeAgentAcceptanceContract(t *testing.T) {
	data, err := os.ReadFile("contracts/three-agent-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var contract scenarioContract
	if err := json.Unmarshal(data, &contract); err != nil {
		t.Fatal(err)
	}
	if contract.Version != "nostrig-three-agent/v1" {
		t.Fatalf("unsupported contract version %q", contract.Version)
	}
	declared := map[string]bool{}
	for _, actor := range contract.Actors {
		declared[strings.ToLower(strings.TrimSpace(actor))] = true
	}
	for _, actor := range []string{"stew", "netward", "gus"} {
		if !declared[actor] {
			t.Errorf("contract omits actor %s", actor)
		}
	}
	if len(contract.Steps) != 12 {
		t.Fatalf("steps=%d, want 12", len(contract.Steps))
	}
	required := map[string]bool{
		"create_and_assign":             false,
		"receive_and_ack":               false,
		"atomic_claim":                  false,
		"competing_claim":               false,
		"block_with_evidence":           false,
		"resolve_reassign_or_unblock":   false,
		"complete_and_request_review":   false,
		"record_review_and_quality":     false,
		"unauthorized_close":            false,
		"authorized_close":              false,
		"independent_state_convergence": false,
		"restart_service_and_relay":     false,
	}
	for i, step := range contract.Steps {
		if step.ID != i+1 {
			t.Errorf("step %d has id %d", i+1, step.ID)
		}
		if !declared[strings.ToLower(step.Actor)] {
			t.Errorf("step %d uses undeclared actor %q", step.ID, step.Actor)
		}
		if strings.TrimSpace(step.Expected) == "" || len(step.Evidence) == 0 {
			t.Errorf("step %d lacks expected outcome or evidence", step.ID)
		}
		if _, ok := required[step.Operation]; ok {
			required[step.Operation] = true
		} else {
			t.Errorf("step %d has unknown operation %q", step.ID, step.Operation)
		}
	}
	for operation, present := range required {
		if !present {
			t.Errorf("contract omits milestone %q", operation)
		}
	}
}
