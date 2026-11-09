package failure_test

import (
	"encoding/json"
	"os"
	"strconv"
	"testing"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/failure"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/scenario"
)

// TestAdversarialScenarios is the stable catalog selector used by each
// definition's reproduction command. The coordinated execution harness can
// consume the selected definition without maintaining a second scenario list.
func TestAdversarialScenarios(t *testing.T) {
	definitions := scenario.Catalog()
	if err := scenario.ValidateCatalog(definitions); err != nil {
		t.Fatal(err)
	}
	if selected := os.Getenv("KV_SCENARIO"); selected != "" {
		definition, found := scenario.Lookup(selected)
		if !found {
			t.Fatalf("unknown KV_SCENARIO %q", selected)
		}
		if seedText := os.Getenv("KV_SEED"); seedText != "" {
			seed, err := strconv.ParseInt(seedText, 10, 64)
			if err != nil {
				t.Fatalf("invalid KV_SEED: %v", err)
			}
			definition.Failure.Seed = seed
		}
		verifyDefinition(t, definition)
		return
	}
	for _, definition := range definitions {
		definition := definition
		t.Run(definition.ID, func(t *testing.T) { verifyDefinition(t, definition) })
	}
}

func verifyDefinition(t *testing.T, definition scenario.Definition) {
	t.Helper()
	if _, err := failure.NewInjector(definition.Failure); err != nil {
		t.Fatalf("initialize failure injector: %v", err)
	}
	encoded, err := json.Marshal(definition)
	if err != nil {
		t.Fatalf("encode scenario: %v", err)
	}
	t.Log(string(encoded))
}
