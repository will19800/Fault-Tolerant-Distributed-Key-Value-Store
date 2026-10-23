package database_test

import (
	"errors"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/database"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/storage"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/wal"
)

func TestStoreRecoversAcknowledgedMutations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	store, recovery, err := database.Open(path, wal.SyncEveryAppend)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if recovery != (database.Recovery{}) {
		t.Fatalf("initial recovery = %+v, want zero value", recovery)
	}

	if err := store.Put("theme", []byte("light")); err != nil {
		t.Fatal(err)
	}
	if err := store.Put("theme", []byte("dark")); err != nil {
		t.Fatal(err)
	}
	if err := store.Put("language", []byte("en")); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete("theme"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, recovery, err := database.Open(path, wal.SyncEveryAppend)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = recovered.Close() })
	if recovery.Records != 4 || recovery.TruncatedBytes != 0 {
		t.Fatalf("recovery = %+v, want 4 records and no truncation", recovery)
	}
	if _, err := recovered.Get("theme"); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("Get deleted theme error = %v, want %v", err, storage.ErrKeyNotFound)
	}
	value, err := recovered.Get("language")
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "en" {
		t.Fatalf("recovered language = %q, want en", value)
	}
}

func TestStorePreservesConcurrentWriteOrderAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	store, _, err := database.Open(path, wal.SyncOnClose)
	if err != nil {
		t.Fatal(err)
	}

	const writers = 32
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := store.Put("shared", []byte(strconv.Itoa(i))); err != nil {
				t.Errorf("Put(%d): %v", i, err)
			}
		}()
	}
	wg.Wait()
	want, err := store.Get("shared")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, recovery, err := database.Open(path, wal.SyncEveryAppend)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = recovered.Close() })
	got, err := recovered.Get("shared")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("recovered final value = %q, want %q", got, want)
	}
	if recovery.Records != writers {
		t.Fatalf("recovered records = %d, want %d", recovery.Records, writers)
	}
}

func TestStoreRejectsOperationsAfterClose(t *testing.T) {
	store, _, err := database.Open(filepath.Join(t.TempDir(), "store.wal"), wal.SyncEveryAppend)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Put("key", []byte("value")); !errors.Is(err, database.ErrClosed) {
		t.Fatalf("Put after close error = %v, want %v", err, database.ErrClosed)
	}
	if _, err := store.Get("key"); !errors.Is(err, database.ErrClosed) {
		t.Fatalf("Get after close error = %v, want %v", err, database.ErrClosed)
	}
	if err := store.Delete("key"); !errors.Is(err, database.ErrClosed) {
		t.Fatalf("Delete after close error = %v, want %v", err, database.ErrClosed)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
