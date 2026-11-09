// Package scenario defines the reproducible adversarial validation catalog.
package scenario

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/failure"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
)

// Category groups scenarios by the mechanism they stress.
type Category string

const (
	Election    Category = "election"
	Replication Category = "replication"
	QuorumLoss  Category = "quorum_loss"
	Healing     Category = "healing"
	Restart     Category = "restart"
	Snapshot    Category = "snapshot"
	Combined    Category = "combined"
)

// Assertion names an independently checked property.
type Assertion string

const (
	CommittedPrefix     Assertion = "committed_prefix"
	OrderedApplication  Assertion = "ordered_application"
	AcknowledgedWrites  Assertion = "acknowledged_writes_preserved"
	EventualConvergence Assertion = "eventual_convergence"
	AutomaticReelection Assertion = "automatic_reelection"
	MinorityNoProgress  Assertion = "minority_no_progress"
	SuffixRepair        Assertion = "uncommitted_suffix_repair"
	SnapshotState       Assertion = "snapshot_state_matches"
)

// Workload describes the client traffic active during fault injection.
type Workload struct {
	Pattern     string `json:"pattern"`
	Operations  int    `json:"operations"`
	Concurrency int    `json:"concurrency"`
	KeySpace    int    `json:"key_space"`
	ValueBytes  int    `json:"value_bytes"`
}

// Definition is one distinct and reproducible adversarial scenario.
type Definition struct {
	ID            string           `json:"id"`
	Category      Category         `json:"category"`
	Purpose       string           `json:"purpose"`
	Nodes         int              `json:"nodes"`
	FaultSchedule string           `json:"fault_schedule"`
	Workload      Workload         `json:"workload"`
	Expected      string           `json:"expected"`
	Assertions    []Assertion      `json:"assertions"`
	Failure       failure.Scenario `json:"failure"`
	Command       string           `json:"command"`
}

var idPattern = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*$`)

// Catalog returns deep copies of all declared scenarios in stable ID order.
func Catalog() []Definition {
	definitions := definitions()
	sort.Slice(definitions, func(i, j int) bool { return definitions[i].ID < definitions[j].ID })
	return cloneDefinitions(definitions)
}

// Lookup finds one catalog entry by ID.
func Lookup(id string) (Definition, bool) {
	definitions := Catalog()
	index, found := slices.BinarySearchFunc(definitions, id, func(definition Definition, target string) int {
		return strings.Compare(definition.ID, target)
	})
	if !found {
		return Definition{}, false
	}
	return definitions[index], true
}

// ValidateCatalog enforces completeness, distinctness, and runnable failure
// controls for every catalog entry.
func ValidateCatalog(definitions []Definition) error {
	if len(definitions) < 50 {
		return fmt.Errorf("catalog contains %d scenarios, want at least 50", len(definitions))
	}
	seenIDs := make(map[string]struct{}, len(definitions))
	seenSeeds := make(map[int64]string, len(definitions))
	seenSignatures := make(map[string]string, len(definitions))
	covered := make(map[Category]int)
	for index, definition := range definitions {
		if err := validateDefinition(definition); err != nil {
			return fmt.Errorf("scenario %d (%q): %w", index, definition.ID, err)
		}
		if _, exists := seenIDs[definition.ID]; exists {
			return fmt.Errorf("duplicate scenario ID %q", definition.ID)
		}
		seenIDs[definition.ID] = struct{}{}
		if prior, exists := seenSeeds[definition.Failure.Seed]; exists {
			return fmt.Errorf("scenarios %q and %q reuse seed %d", prior, definition.ID, definition.Failure.Seed)
		}
		seenSeeds[definition.Failure.Seed] = definition.ID
		signature, err := definitionSignature(definition)
		if err != nil {
			return fmt.Errorf("scenario %q signature: %w", definition.ID, err)
		}
		if prior, exists := seenSignatures[signature]; exists {
			return fmt.Errorf("scenarios %q and %q differ only in documentation or seed", prior, definition.ID)
		}
		seenSignatures[signature] = definition.ID
		covered[definition.Category]++
	}
	for _, category := range []Category{Election, Replication, QuorumLoss, Healing, Restart, Snapshot, Combined} {
		if covered[category] == 0 {
			return fmt.Errorf("catalog does not cover %s", category)
		}
	}
	return nil
}

func validateDefinition(definition Definition) error {
	if !idPattern.MatchString(definition.ID) {
		return errors.New("ID must be a lowercase hyphenated slug")
	}
	switch definition.Category {
	case Election, Replication, QuorumLoss, Healing, Restart, Snapshot, Combined:
	default:
		return fmt.Errorf("unknown category %q", definition.Category)
	}
	if definition.Nodes != 3 && definition.Nodes != 5 {
		return errors.New("node count must be three or five")
	}
	if definition.Purpose == "" || definition.FaultSchedule == "" || definition.Expected == "" {
		return errors.New("purpose, fault schedule, and expected behavior are required")
	}
	if definition.Workload.Pattern == "" || definition.Workload.Operations <= 0 || definition.Workload.Concurrency <= 0 || definition.Workload.KeySpace <= 0 || definition.Workload.ValueBytes <= 0 {
		return errors.New("workload fields must all be positive and the pattern must be named")
	}
	if len(definition.Assertions) == 0 {
		return errors.New("at least one assertion is required")
	}
	assertions := make(map[Assertion]struct{}, len(definition.Assertions))
	for _, assertion := range definition.Assertions {
		switch assertion {
		case CommittedPrefix, OrderedApplication, AcknowledgedWrites, EventualConvergence, AutomaticReelection, MinorityNoProgress, SuffixRepair, SnapshotState:
		default:
			return fmt.Errorf("unknown assertion %q", assertion)
		}
		if _, exists := assertions[assertion]; exists {
			return fmt.Errorf("duplicate assertion %q", assertion)
		}
		assertions[assertion] = struct{}{}
	}
	for _, required := range []Assertion{CommittedPrefix, OrderedApplication, AcknowledgedWrites, EventualConvergence} {
		if _, exists := assertions[required]; !exists {
			return fmt.Errorf("missing baseline assertion %q", required)
		}
	}
	if len(definition.Failure.Network) == 0 && len(definition.Failure.Actions) == 0 {
		return errors.New("scenario does not inject a fault")
	}
	if err := definition.Failure.Validate(); err != nil {
		return fmt.Errorf("failure controls: %w", err)
	}
	members := make(map[raft.NodeID]struct{}, definition.Nodes)
	for number := 1; number <= definition.Nodes; number++ {
		members[raft.NodeID("node-"+strconv.Itoa(number))] = struct{}{}
	}
	for _, rule := range definition.Failure.Network {
		if _, exists := members[rule.From]; !exists {
			return fmt.Errorf("network source %q is outside the %d-node cluster", rule.From, definition.Nodes)
		}
		if _, exists := members[rule.To]; !exists {
			return fmt.Errorf("network target %q is outside the %d-node cluster", rule.To, definition.Nodes)
		}
	}
	for _, action := range definition.Failure.Actions {
		if _, exists := members[action.Node]; !exists {
			return fmt.Errorf("process target %q is outside the %d-node cluster", action.Node, definition.Nodes)
		}
	}
	wantCommand := ReproductionCommand(definition.ID, definition.Failure.Seed)
	if definition.Command != wantCommand {
		return fmt.Errorf("reproduction command = %q, want %q", definition.Command, wantCommand)
	}
	return nil
}

func definitionSignature(definition Definition) (string, error) {
	input := struct {
		Nodes    int
		Workload Workload
		Network  []failure.NetworkRule
		Actions  []failure.ProcessAction
	}{definition.Nodes, definition.Workload, definition.Failure.Network, definition.Failure.Actions}
	encoded, err := json.Marshal(input)
	return string(encoded), err
}

// ReproductionCommand returns the stable selector used by the scenario suite.
func ReproductionCommand(id string, seed int64) string {
	return fmt.Sprintf("KV_SCENARIO=%s KV_SEED=%d go test ./test/failure -run TestAdversarialScenarios -count=1", id, seed)
}

func cloneDefinitions(definitions []Definition) []Definition {
	cloned := slices.Clone(definitions)
	for index := range cloned {
		cloned[index].Assertions = slices.Clone(cloned[index].Assertions)
		cloned[index].Failure.Network = slices.Clone(cloned[index].Failure.Network)
		cloned[index].Failure.Actions = slices.Clone(cloned[index].Failure.Actions)
	}
	return cloned
}
