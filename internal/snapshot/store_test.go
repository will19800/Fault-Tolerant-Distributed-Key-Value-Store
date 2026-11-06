package snapshot_test

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/snapshot"
)

func TestFileStoreRoundTripAndAtomicReplace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.snapshot")
	store, err := snapshot.NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if initial, err := store.Load(); err != nil || !reflect.DeepEqual(initial, raft.Snapshot{}) {
		t.Fatalf("initial Load = %+v, %v", initial, err)
	}
	first := raft.Snapshot{LastIncludedIndex: 4, LastIncludedTerm: 2, State: []byte("first")}
	second := raft.Snapshot{LastIncludedIndex: 9, LastIncludedTerm: 3, State: []byte("second")}
	if err := store.Save(first); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(second); err != nil {
		t.Fatal(err)
	}
	reopened, _ := snapshot.NewFileStore(path)
	if got, err := reopened.Load(); err != nil || !reflect.DeepEqual(got, second) {
		t.Fatalf("Load = %+v, %v; want %+v", got, err, second)
	}
}

func TestFileStoreRejectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.snapshot")
	store, _ := snapshot.NewFileStore(path)
	if err := store.Save(raft.Snapshot{LastIncludedIndex: 1, LastIncludedTerm: 1, State: []byte("state")}); err != nil {
		t.Fatal(err)
	}
	frame, _ := os.ReadFile(path)
	frame[len(frame)-1] ^= 0xff
	if err := os.WriteFile(path, frame, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(); !errors.Is(err, snapshot.ErrCorrupt) {
		t.Fatalf("Load error = %v, want ErrCorrupt", err)
	}
}

func TestFileStoreRejectsInvalidMetadata(t *testing.T) {
	store, _ := snapshot.NewFileStore(filepath.Join(t.TempDir(), "state.snapshot"))
	if err := store.Save(raft.Snapshot{State: []byte("state")}); err == nil {
		t.Fatal("Save accepted zero index and term")
	}
	if _, err := snapshot.NewFileStore(""); err == nil {
		t.Fatal("NewFileStore accepted an empty path")
	}
}
