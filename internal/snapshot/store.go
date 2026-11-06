// Package snapshot atomically persists compacted state-machine data.
package snapshot

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
)

const (
	version    uint8  = 1
	magic      uint32 = 0x4b56534e // KVSN
	headerSize        = 32
)

var (
	ErrCorrupt    = errors.New("corrupt snapshot")
	ErrVersion    = errors.New("unsupported snapshot version")
	checksumTable = crc32.MakeTable(crc32.Castagnoli)
)

// FileStore publishes each snapshot as one checksummed atomic file.
type FileStore struct {
	mu   sync.Mutex
	path string
}

func NewFileStore(path string) (*FileStore, error) {
	if path == "" {
		return nil, errors.New("snapshot path must not be empty")
	}
	return &FileStore{path: path}, nil
}

// Save flushes a temporary file, renames it over the previous snapshot, and
// syncs the parent directory before reporting success.
func (s *FileStore) Save(snapshot raft.Snapshot) error {
	frame, err := encode(snapshot)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return replaceAtomically(s.path, frame)
}

// Load returns a zero snapshot when no snapshot has been published.
func (s *FileStore) Load() (raft.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	frame, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return raft.Snapshot{}, nil
	}
	if err != nil {
		return raft.Snapshot{}, fmt.Errorf("read snapshot %q: %w", s.path, err)
	}
	snapshot, err := decode(frame)
	if err != nil {
		return raft.Snapshot{}, fmt.Errorf("decode snapshot %q: %w", s.path, err)
	}
	return snapshot, nil
}

func encode(snapshot raft.Snapshot) ([]byte, error) {
	if snapshot.LastIncludedIndex == 0 || snapshot.LastIncludedTerm == 0 {
		return nil, errors.New("snapshot index and term must be positive")
	}
	if uint64(len(snapshot.State)) > uint64(^uint32(0)) {
		return nil, errors.New("snapshot state exceeds 4 GiB")
	}
	frame := make([]byte, headerSize+len(snapshot.State))
	binary.BigEndian.PutUint32(frame[0:4], magic)
	frame[4] = version
	binary.BigEndian.PutUint64(frame[8:16], uint64(snapshot.LastIncludedIndex))
	binary.BigEndian.PutUint64(frame[16:24], uint64(snapshot.LastIncludedTerm))
	binary.BigEndian.PutUint32(frame[24:28], uint32(len(snapshot.State)))
	copy(frame[headerSize:], snapshot.State)
	binary.BigEndian.PutUint32(frame[28:32], checksum(frame))
	return frame, nil
}

func decode(frame []byte) (raft.Snapshot, error) {
	if len(frame) < headerSize {
		return raft.Snapshot{}, fmt.Errorf("%w: truncated header", ErrCorrupt)
	}
	if binary.BigEndian.Uint32(frame[0:4]) != magic {
		return raft.Snapshot{}, fmt.Errorf("%w: invalid magic", ErrCorrupt)
	}
	if frame[4] != version {
		return raft.Snapshot{}, fmt.Errorf("%w: got %d", ErrVersion, frame[4])
	}
	if frame[5] != 0 || frame[6] != 0 || frame[7] != 0 {
		return raft.Snapshot{}, fmt.Errorf("%w: nonzero reserved bytes", ErrCorrupt)
	}
	stateLength := binary.BigEndian.Uint32(frame[24:28])
	if uint64(headerSize)+uint64(stateLength) != uint64(len(frame)) {
		return raft.Snapshot{}, fmt.Errorf("%w: state length does not match file size", ErrCorrupt)
	}
	if binary.BigEndian.Uint32(frame[28:32]) != checksum(frame) {
		return raft.Snapshot{}, fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
	}
	snapshot := raft.Snapshot{
		LastIncludedIndex: raft.LogIndex(binary.BigEndian.Uint64(frame[8:16])),
		LastIncludedTerm:  raft.Term(binary.BigEndian.Uint64(frame[16:24])),
		State:             slices.Clone(frame[headerSize:]),
	}
	if snapshot.LastIncludedIndex == 0 || snapshot.LastIncludedTerm == 0 {
		return raft.Snapshot{}, fmt.Errorf("%w: index and term must be positive", ErrCorrupt)
	}
	return snapshot, nil
}

func checksum(frame []byte) uint32 {
	digest := crc32.New(checksumTable)
	_, _ = digest.Write(frame[4:28])
	_, _ = digest.Write(frame[headerSize:])
	return digest.Sum32()
}

func replaceAtomically(path string, data []byte) error {
	directoryPath := filepath.Dir(path)
	temporary, err := os.CreateTemp(directoryPath, ".snapshot-*")
	if err != nil {
		return fmt.Errorf("create temporary snapshot: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	for len(data) > 0 {
		written, writeErr := temporary.Write(data)
		if writeErr != nil {
			_ = temporary.Close()
			return fmt.Errorf("write temporary snapshot: %w", writeErr)
		}
		if written == 0 {
			_ = temporary.Close()
			return fmt.Errorf("write temporary snapshot: %w", io.ErrShortWrite)
		}
		data = data[written:]
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync temporary snapshot: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary snapshot: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("publish snapshot: %w", err)
	}
	directory, err := os.Open(directoryPath)
	if err != nil {
		return fmt.Errorf("open snapshot directory: %w", err)
	}
	if err := errors.Join(directory.Sync(), directory.Close()); err != nil {
		return fmt.Errorf("sync snapshot directory: %w", err)
	}
	return nil
}
