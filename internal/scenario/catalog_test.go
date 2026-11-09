package scenario

import (
	"strings"
	"testing"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/failure"
)

func TestCatalogIsCompleteAndValid(t *testing.T) {
	definitions := Catalog()
	if len(definitions) != 52 {
		t.Fatalf("catalog size = %d, want 52", len(definitions))
	}
	if err := ValidateCatalog(definitions); err != nil {
		t.Fatal(err)
	}
	counts := make(map[Category]int)
	for _, definition := range definitions {
		counts[definition.Category]++
	}
	want := map[Category]int{Election: 8, Replication: 8, QuorumLoss: 7, Healing: 7, Restart: 8, Snapshot: 7, Combined: 7}
	for category, count := range want {
		if counts[category] != count {
			t.Fatalf("%s scenarios = %d, want %d", category, counts[category], count)
		}
	}
}

func TestCatalogReturnsDeepCopies(t *testing.T) {
	first := Catalog()
	first[0].Assertions[0] = "changed"
	first[0].Failure.Network[0].Drop = false
	second := Catalog()
	if second[0].Assertions[0] != CommittedPrefix || !second[0].Failure.Network[0].Drop {
		t.Fatalf("catalog mutation escaped: %+v", second[0])
	}
}

func TestLookupReturnsExactScenario(t *testing.T) {
	definition, found := Lookup("snapshot-lossy-install")
	if !found || definition.Category != Snapshot || definition.Failure.Seed != 1502 {
		t.Fatalf("Lookup = %+v, %t", definition, found)
	}
	if _, found := Lookup("missing"); found {
		t.Fatal("missing scenario was found")
	}
}

func TestValidateCatalogRejectsSeedOnlyVariants(t *testing.T) {
	definitions := Catalog()
	duplicate := definitions[0]
	duplicate.ID = "seed-only-duplicate"
	duplicate.Failure.Seed = 99999
	duplicate.Command = ReproductionCommand(duplicate.ID, duplicate.Failure.Seed)
	definitions = append(definitions, duplicate)
	if err := ValidateCatalog(definitions); err == nil || !strings.Contains(err.Error(), "differ only") {
		t.Fatalf("ValidateCatalog error = %v", err)
	}
}

func TestValidateCatalogRejectsUnknownClusterNode(t *testing.T) {
	definitions := Catalog()
	definitions[0].Failure.Network[0] = failure.NetworkRule{
		From: "node-1", To: "node-9", Message: failure.AnyMessage, Drop: true,
	}
	if err := ValidateCatalog(definitions); err == nil || !strings.Contains(err.Error(), "outside") {
		t.Fatalf("ValidateCatalog error = %v", err)
	}
}
