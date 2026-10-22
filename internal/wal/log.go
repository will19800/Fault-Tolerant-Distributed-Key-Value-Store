package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

var (
	ErrClosed            = errors.New("WAL is closed")
	ErrReplayRequired    = errors.New("WAL replay required before append")
	ErrAlreadyReplayed   = errors.New("WAL has already been replayed")
	ErrInvalidSyncPolicy = errors.New("invalid WAL sync policy")
)

// SyncPolicy controls when appended records are forced to durable storage.
type SyncPolicy uint8

const (
	// SyncEveryAppend calls fsync before each successful Append returns.
	SyncEveryAppend SyncPolicy = iota + 1
	// SyncOnClose delays fsync until Close and is not durable per append.
	SyncOnClose
)

// ReplayResult summarizes recovery work performed while replaying a log.
type ReplayResult struct {
	Records        int
	TruncatedBytes int64
}

// Log is an append-only file of framed records.
type Log struct {
	mu       sync.Mutex
	file     *os.File
	policy   SyncPolicy
	replayed bool
	closed   bool
}

// Open opens or creates a log at path. The caller must call Replay before
// Append so recovery can validate existing frames and repair a partial tail.
func Open(path string, policy SyncPolicy) (*Log, error) {
	if policy != SyncEveryAppend && policy != SyncOnClose {
		return nil, fmt.Errorf("%w: %d", ErrInvalidSyncPolicy, policy)
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open WAL %q: %w", path, err)
	}
	return &Log{file: file, policy: policy}, nil
}

// Replay validates and applies each complete record in order. If the file ends
// with an incomplete frame, Replay removes only that tail and reports its size.
func (l *Log) Replay(apply func(Record) error) (ReplayResult, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return ReplayResult{}, ErrClosed
	}
	if l.replayed {
		return ReplayResult{}, ErrAlreadyReplayed
	}
	if _, err := l.file.Seek(0, io.SeekStart); err != nil {
		return ReplayResult{}, fmt.Errorf("seek WAL start: %w", err)
	}

	result := ReplayResult{}
	for {
		recordOffset, err := l.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return result, fmt.Errorf("read WAL offset: %w", err)
		}

		record, err := Decode(l.file)
		switch {
		case errors.Is(err, io.EOF):
			l.replayed = true
			return result, nil
		case errors.Is(err, ErrTruncatedRecord):
			truncatedBytes, repairErr := l.truncateTail(recordOffset)
			if repairErr != nil {
				return result, repairErr
			}
			result.TruncatedBytes = truncatedBytes
			l.replayed = true
			return result, nil
		case err != nil:
			return result, fmt.Errorf("decode WAL record %d at offset %d: %w", result.Records, recordOffset, err)
		}

		if apply != nil {
			if err := apply(record); err != nil {
				return result, fmt.Errorf("apply WAL record %d: %w", result.Records, err)
			}
		}
		result.Records++
	}
}

// Append encodes and appends one record. With SyncEveryAppend, success means
// the record has also been forced to durable storage.
func (l *Log) Append(record Record) error {
	frame, err := Encode(record)
	if err != nil {
		return err
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return ErrClosed
	}
	if !l.replayed {
		return ErrReplayRequired
	}
	if err := writeFull(l.file, frame); err != nil {
		return fmt.Errorf("append WAL record: %w", err)
	}
	if l.policy == SyncEveryAppend {
		if err := l.file.Sync(); err != nil {
			return fmt.Errorf("sync WAL record: %w", err)
		}
	}
	return nil
}

// Close syncs and closes the log. Repeated calls succeed.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil
	}
	l.closed = true
	return errors.Join(l.file.Sync(), l.file.Close())
}

func (l *Log) truncateTail(offset int64) (int64, error) {
	info, err := l.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat WAL during tail repair: %w", err)
	}
	if err := l.file.Truncate(offset); err != nil {
		return 0, fmt.Errorf("truncate WAL tail at offset %d: %w", offset, err)
	}
	if err := l.file.Sync(); err != nil {
		return 0, fmt.Errorf("sync repaired WAL tail: %w", err)
	}
	if _, err := l.file.Seek(0, io.SeekEnd); err != nil {
		return 0, fmt.Errorf("seek repaired WAL end: %w", err)
	}
	return info.Size() - offset, nil
}

func writeFull(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}
