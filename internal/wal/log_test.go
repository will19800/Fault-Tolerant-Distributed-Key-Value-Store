package wal

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
)

func TestLogAppendCloseAndReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	log, err := Open(path, SyncEveryAppend)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	put := Record{Operation: OperationPut, Key: "key", Value: []byte("value")}
	if err := log.Append(put); !errors.Is(err, ErrReplayRequired) {
		t.Fatalf("Append before replay error = %v, want %v", err, ErrReplayRequired)
	}
	if result, err := log.Replay(nil); err != nil || result != (ReplayResult{}) {
		t.Fatalf("initial Replay = %+v, %v", result, err)
	}
	if err := log.Append(put); err != nil {
		t.Fatalf("Append put: %v", err)
	}
	if err := log.Append(Record{Operation: OperationDelete, Key: "key"}); err != nil {
		t.Fatalf("Append delete: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(path, SyncEveryAppend)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	var records []Record
	result, err := reopened.Replay(func(record Record) error {
		records = append(records, record)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if result.Records != 2 || result.TruncatedBytes != 0 {
		t.Fatalf("Replay result = %+v, want 2 records and no truncation", result)
	}
	if len(records) != 2 || records[0].Operation != OperationPut || records[0].Key != put.Key || !slices.Equal(records[0].Value, put.Value) || records[1].Operation != OperationDelete {
		t.Fatalf("replayed records = %+v, want put then delete", records)
	}
}

func TestReplayRepairsIncompleteTail(t *testing.T) {
	complete, err := Encode(Record{Operation: OperationPut, Key: "complete", Value: []byte("value")})
	if err != nil {
		t.Fatal(err)
	}
	incomplete, err := Encode(Record{Operation: OperationPut, Key: "incomplete", Value: []byte("value")})
	if err != nil {
		t.Fatal(err)
	}
	incomplete = incomplete[:len(incomplete)-3]

	path := filepath.Join(t.TempDir(), "store.wal")
	if err := os.WriteFile(path, append(complete, incomplete...), 0o600); err != nil {
		t.Fatal(err)
	}
	log, err := Open(path, SyncEveryAppend)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })

	var records []Record
	result, err := log.Replay(func(record Record) error {
		records = append(records, record)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if result.Records != 1 || result.TruncatedBytes != int64(len(incomplete)) {
		t.Fatalf("Replay result = %+v, want 1 record and %d truncated bytes", result, len(incomplete))
	}
	if len(records) != 1 || records[0].Key != "complete" {
		t.Fatalf("replayed records = %+v, want complete record", records)
	}
	if info, err := os.Stat(path); err != nil || info.Size() != int64(len(complete)) {
		t.Fatalf("repaired file stat = %+v, %v; want size %d", info, err, len(complete))
	}

	if err := log.Append(Record{Operation: OperationDelete, Key: "complete"}); err != nil {
		t.Fatalf("Append after repair: %v", err)
	}
}

func TestReplayRejectsCorruption(t *testing.T) {
	frame, err := Encode(Record{Operation: OperationPut, Key: "key", Value: []byte("value")})
	if err != nil {
		t.Fatal(err)
	}
	frame[len(frame)-1] ^= 0xff

	path := filepath.Join(t.TempDir(), "store.wal")
	if err := os.WriteFile(path, frame, 0o600); err != nil {
		t.Fatal(err)
	}
	log, err := Open(path, SyncEveryAppend)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })

	if _, err := log.Replay(nil); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("Replay corruption error = %v, want %v", err, ErrChecksumMismatch)
	}
	if err := log.Append(Record{Operation: OperationDelete, Key: "key"}); !errors.Is(err, ErrReplayRequired) {
		t.Fatalf("Append after failed replay error = %v, want %v", err, ErrReplayRequired)
	}
}

func TestLogSerializesConcurrentAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	log, err := Open(path, SyncOnClose)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Replay(nil); err != nil {
		t.Fatal(err)
	}

	const writers = 32
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := log.Append(Record{Operation: OperationPut, Key: string(rune('a' + i)), Value: []byte{byte(i)}}); err != nil {
				t.Errorf("Append writer %d: %v", i, err)
			}
		}()
	}
	wg.Wait()
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path, SyncEveryAppend)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	result, err := reopened.Replay(nil)
	if err != nil {
		t.Fatalf("Replay concurrent appends: %v", err)
	}
	if result.Records != writers {
		t.Fatalf("Replay record count = %d, want %d", result.Records, writers)
	}
}

func TestLogCloseBehavior(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	log, err := Open(path, SyncOnClose)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Replay(nil); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := log.Append(Record{Operation: OperationPut, Key: "key"}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Append after Close error = %v, want %v", err, ErrClosed)
	}
	if _, err := log.Replay(nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("Replay after Close error = %v, want %v", err, ErrClosed)
	}
}

func TestOpenValidatesPolicyAndCreatesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	if _, err := Open(path, 0); !errors.Is(err, ErrInvalidSyncPolicy) {
		t.Fatalf("Open invalid policy error = %v, want %v", err, ErrInvalidSyncPolicy)
	}
	log, err := Open(path, SyncEveryAppend)
	if err != nil {
		t.Fatalf("Open new file: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("Stat created WAL: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
}
