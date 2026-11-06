package raft_test

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
)

func TestFileLogStoreRoundTripAndReplace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-log")
	store, err := raft.NewFileLogStore(path)
	if err != nil {
		t.Fatalf("NewFileLogStore: %v", err)
	}
	if entries, err := store.Load(); err != nil || len(entries) != 0 {
		t.Fatalf("initial Load = %+v, %v", entries, err)
	}
	first := []raft.Entry{
		{Index: 1, Term: 1, Command: []byte("one")},
		{Index: 2, Term: 2, Command: []byte{0x00, 0xff}},
	}
	if err := store.Save(first); err != nil {
		t.Fatalf("Save first: %v", err)
	}
	second := first[:1]
	if err := store.Save(second); err != nil {
		t.Fatalf("Save second: %v", err)
	}
	reopened, err := raft.NewFileLogStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := reopened.Load(); err != nil || !reflect.DeepEqual(got, second) {
		t.Fatalf("reopened Load = %+v, %v; want %+v", got, err, second)
	}
}

func TestFileLogStoreRejectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-log")
	store, err := raft.NewFileLogStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save([]raft.Entry{{Index: 1, Term: 1, Command: []byte("command")}}); err != nil {
		t.Fatal(err)
	}
	frame, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	frame[len(frame)-1] ^= 0xff
	if err := os.WriteFile(path, frame, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(); !errors.Is(err, raft.ErrLogCorrupt) {
		t.Fatalf("Load error = %v, want ErrLogCorrupt", err)
	}
}

func TestFileLogStoreRejectsInvalidLog(t *testing.T) {
	store, err := raft.NewFileLogStore(filepath.Join(t.TempDir(), "raft-log"))
	if err != nil {
		t.Fatal(err)
	}
	invalid := [][]raft.Entry{
		{{Index: 2, Term: 1}},
		{{Index: 1}},
	}
	for _, entries := range invalid {
		if err := store.Save(entries); err == nil {
			t.Fatalf("Save(%+v) returned nil error", entries)
		}
	}
	if _, err := raft.NewFileLogStore(""); err == nil {
		t.Fatal("NewFileLogStore accepted empty path")
	}
}

func TestFileLogStorePersistsCompactionBoundary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-log")
	store, err := raft.NewFileLogStore(path)
	if err != nil {
		t.Fatal(err)
	}
	entries := []raft.Entry{
		{Index: 1, Term: 1, Command: []byte("one")},
		{Index: 2, Term: 1, Command: []byte("two")},
		{Index: 3, Term: 2, Command: []byte("three")},
	}
	if err := store.Save(entries); err != nil {
		t.Fatal(err)
	}
	if err := store.Compact(2, 1, entries[2:]); err != nil {
		t.Fatal(err)
	}

	reopened, _ := raft.NewFileLogStore(path)
	retained, err := reopened.Load()
	if err != nil || !reflect.DeepEqual(retained, entries[2:]) {
		t.Fatalf("Load = %+v, %v; want %+v", retained, err, entries[2:])
	}
	if index, term := reopened.Base(); index != 2 || term != 1 {
		t.Fatalf("Base = (%d, %d), want (2, 1)", index, term)
	}
	retained = append(retained, raft.Entry{Index: 4, Term: 2, Command: []byte("four")})
	if err := reopened.Save(retained); err != nil {
		t.Fatalf("Save retained suffix: %v", err)
	}
}

func TestFileLogStoreInstallsSnapshotBeyondLocalLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-log")
	store, _ := raft.NewFileLogStore(path)
	if err := store.Save([]raft.Entry{{Index: 1, Term: 1, Command: []byte("old")}}); err != nil {
		t.Fatal(err)
	}
	suffix := []raft.Entry{{Index: 4, Term: 3, Command: []byte("retained")}}
	if err := store.InstallSnapshot(3, 2, suffix); err != nil {
		t.Fatal(err)
	}
	reopened, _ := raft.NewFileLogStore(path)
	if got, err := reopened.Load(); err != nil || !reflect.DeepEqual(got, suffix) {
		t.Fatalf("Load = %+v, %v; want %+v", got, err, suffix)
	}
	if index, term := reopened.Base(); index != 3 || term != 2 {
		t.Fatalf("Base = (%d, %d), want (3, 2)", index, term)
	}
}
