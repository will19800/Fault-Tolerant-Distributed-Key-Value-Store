package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	hardStateVersion    uint8  = 1
	hardStateMagic      uint32 = 0x4b564853 // KVHS
	hardStateHeaderSize        = 20
)

var (
	// ErrHardStateCorrupt means persisted Raft metadata failed validation.
	ErrHardStateCorrupt = errors.New("corrupt Raft hard state")
	// ErrHardStateVersion means persisted metadata uses an unknown version.
	ErrHardStateVersion = errors.New("unsupported Raft hard-state version")
)

var hardStateChecksumTable = crc32.MakeTable(crc32.Castagnoli)

// HardState is the Raft metadata that must survive every restart.
type HardState struct {
	CurrentTerm Term
	VotedFor    NodeID
}

// Validate checks hard-state invariants.
func (s HardState) Validate() error {
	if s.VotedFor == "" {
		return nil
	}
	if s.CurrentTerm == 0 {
		return errors.New("vote requires a positive current term")
	}
	if err := s.VotedFor.Validate(); err != nil {
		return fmt.Errorf("voted-for node ID: %w", err)
	}
	return nil
}

// HardStateStore loads and atomically replaces durable Raft metadata.
type HardStateStore interface {
	Load() (HardState, error)
	Save(HardState) error
}

// FileHardStateStore stores checksummed hard state in one atomic file.
type FileHardStateStore struct {
	mu   sync.Mutex
	path string
}

// NewFileHardStateStore constructs a file store. The parent directory must
// exist before Save is called.
func NewFileHardStateStore(path string) (*FileHardStateStore, error) {
	if path == "" {
		return nil, errors.New("hard-state path must not be empty")
	}
	return &FileHardStateStore{path: path}, nil
}

// Load returns zero hard state when the file does not exist.
func (s *FileHardStateStore) Load() (HardState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	frame, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return HardState{}, nil
	}
	if err != nil {
		return HardState{}, fmt.Errorf("read hard state %q: %w", s.path, err)
	}
	state, err := decodeHardState(frame)
	if err != nil {
		return HardState{}, fmt.Errorf("decode hard state %q: %w", s.path, err)
	}
	return state, nil
}

// Save durably replaces the current hard-state file.
func (s *FileHardStateStore) Save(state HardState) error {
	frame, err := encodeHardState(state)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	directory := filepath.Dir(s.path)
	temporary, err := os.CreateTemp(directory, ".hard-state-*")
	if err != nil {
		return fmt.Errorf("create hard-state temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)

	if err := writeHardStateFile(temporary, frame); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close hard-state temporary file: %w", err)
	}
	if err := os.Rename(temporaryPath, s.path); err != nil {
		return fmt.Errorf("publish hard state: %w", err)
	}
	if err := syncDirectory(directory); err != nil {
		return fmt.Errorf("sync hard-state directory: %w", err)
	}
	return nil
}

func encodeHardState(state HardState) ([]byte, error) {
	if err := state.Validate(); err != nil {
		return nil, err
	}
	voteLength := len(state.VotedFor)
	frame := make([]byte, hardStateHeaderSize+voteLength)
	binary.BigEndian.PutUint32(frame[0:4], hardStateMagic)
	frame[4] = hardStateVersion
	frame[5] = byte(voteLength)
	binary.BigEndian.PutUint64(frame[8:16], uint64(state.CurrentTerm))
	copy(frame[hardStateHeaderSize:], state.VotedFor)
	binary.BigEndian.PutUint32(frame[16:20], hardStateChecksum(frame))
	return frame, nil
}

func decodeHardState(frame []byte) (HardState, error) {
	if len(frame) < hardStateHeaderSize {
		return HardState{}, fmt.Errorf("%w: truncated header", ErrHardStateCorrupt)
	}
	if magic := binary.BigEndian.Uint32(frame[0:4]); magic != hardStateMagic {
		return HardState{}, fmt.Errorf("%w: invalid magic %#x", ErrHardStateCorrupt, magic)
	}
	if version := frame[4]; version != hardStateVersion {
		return HardState{}, fmt.Errorf("%w: got %d, want %d", ErrHardStateVersion, version, hardStateVersion)
	}
	if frame[6] != 0 || frame[7] != 0 {
		return HardState{}, fmt.Errorf("%w: nonzero reserved bytes", ErrHardStateCorrupt)
	}
	voteLength := int(frame[5])
	if voteLength > MaxNodeIDBytes || len(frame) != hardStateHeaderSize+voteLength {
		return HardState{}, fmt.Errorf("%w: invalid voted-for length %d", ErrHardStateCorrupt, voteLength)
	}
	wantChecksum := binary.BigEndian.Uint32(frame[16:20])
	if checksum := hardStateChecksum(frame); checksum != wantChecksum {
		return HardState{}, fmt.Errorf("%w: checksum mismatch", ErrHardStateCorrupt)
	}
	state := HardState{
		CurrentTerm: Term(binary.BigEndian.Uint64(frame[8:16])),
		VotedFor:    NodeID(frame[hardStateHeaderSize:]),
	}
	if err := state.Validate(); err != nil {
		return HardState{}, fmt.Errorf("%w: %v", ErrHardStateCorrupt, err)
	}
	return state, nil
}

func hardStateChecksum(frame []byte) uint32 {
	checksum := crc32.New(hardStateChecksumTable)
	_, _ = checksum.Write(frame[4:16])
	_, _ = checksum.Write(frame[hardStateHeaderSize:])
	return checksum.Sum32()
}

func writeHardStateFile(file *os.File, frame []byte) error {
	for len(frame) > 0 {
		written, err := file.Write(frame)
		if err != nil {
			return fmt.Errorf("write hard-state temporary file: %w", err)
		}
		if written == 0 {
			return fmt.Errorf("write hard-state temporary file: %w", io.ErrShortWrite)
		}
		frame = frame[written:]
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync hard-state temporary file: %w", err)
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
