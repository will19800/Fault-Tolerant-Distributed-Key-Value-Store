package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"slices"
	"sync"
)

const (
	logStoreVersion     uint8  = 2
	logStoreMagic       uint32 = 0x4b56524c // KVRL
	logStoreHeaderSize         = 36
	legacyLogHeaderSize        = 20
	logEntryHeaderSize         = 20
	MaxCommandBytes            = 64 << 20 // 64 MiB
)

var (
	// ErrLogCorrupt means persisted Raft log bytes failed validation.
	ErrLogCorrupt = errors.New("corrupt Raft log")
	// ErrLogVersion means persisted log bytes use an unknown version.
	ErrLogVersion = errors.New("unsupported Raft log version")
)

var logChecksumTable = crc32.MakeTable(crc32.Castagnoli)

// LogStore loads and atomically replaces the durable Raft log.
type LogStore interface {
	Load() ([]Entry, error)
	Save([]Entry) error
}

// FileLogStore stores a contiguous, checksummed Raft log in one atomic file.
type FileLogStore struct {
	mu        sync.Mutex
	path      string
	baseIndex LogIndex
	baseTerm  Term
}

// NewFileLogStore constructs a file-backed log store. The parent directory
// must exist before Save is called.
func NewFileLogStore(path string) (*FileLogStore, error) {
	if path == "" {
		return nil, errors.New("Raft log path must not be empty")
	}
	return &FileLogStore{path: path}, nil
}

// Load returns an empty log when the file does not exist.
func (s *FileLogStore) Load() ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	frame, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read Raft log %q: %w", s.path, err)
	}
	baseIndex, baseTerm, entries, err := decodeLog(frame)
	if err != nil {
		return nil, fmt.Errorf("decode Raft log %q: %w", s.path, err)
	}
	s.baseIndex, s.baseTerm = baseIndex, baseTerm
	return entries, nil
}

// Save validates and durably replaces the complete log.
func (s *FileLogStore) Save(entries []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	frame, err := encodeLog(s.baseIndex, s.baseTerm, entries)
	if err != nil {
		return err
	}
	return replaceFileAtomically(s.path, ".raft-log-*", frame)
}

// Base returns the last compacted log position.
func (s *FileLogStore) Base() (LogIndex, Term) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.baseIndex, s.baseTerm
}

// Compact atomically replaces the log with a snapshot boundary and the
// entries that follow it.
func (s *FileLogStore) Compact(index LogIndex, term Term, suffix []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index < s.baseIndex || (index == s.baseIndex && term != s.baseTerm) {
		return errors.New("log compaction cannot move the snapshot boundary backward")
	}
	frame, err := os.ReadFile(s.path)
	if err != nil {
		return fmt.Errorf("read Raft log before compaction: %w", err)
	}
	storedBaseIndex, storedBaseTerm, entries, err := decodeLog(frame)
	if err != nil {
		return fmt.Errorf("decode Raft log before compaction: %w", err)
	}
	if storedBaseIndex != s.baseIndex || storedBaseTerm != s.baseTerm {
		return errors.New("Raft log compaction base changed unexpectedly")
	}
	coveredTerm, ok := logTermAt(s.baseIndex, s.baseTerm, entries, index)
	if !ok || coveredTerm != term {
		return fmt.Errorf("snapshot boundary (%d, %d) is not present in the Raft log", index, term)
	}
	wantSuffix := logEntriesFrom(s.baseIndex, entries, index+1)
	if !equalEntries(suffix, wantSuffix) {
		return errors.New("compaction suffix does not match the durable Raft log")
	}
	frame, err = encodeLog(index, term, suffix)
	if err != nil {
		return err
	}
	if err := replaceFileAtomically(s.path, ".raft-log-*", frame); err != nil {
		return err
	}
	s.baseIndex, s.baseTerm = index, term
	return nil
}

// InstallSnapshot replaces any local prefix with a received snapshot
// boundary. The caller may retain a suffix only when it matches that snapshot.
func (s *FileLogStore) InstallSnapshot(index LogIndex, term Term, suffix []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index == 0 || term == 0 {
		return errors.New("installed snapshot index and term must be positive")
	}
	frame, err := encodeLog(index, term, suffix)
	if err != nil {
		return err
	}
	if err := replaceFileAtomically(s.path, ".raft-log-*", frame); err != nil {
		return err
	}
	s.baseIndex, s.baseTerm = index, term
	return nil
}

func equalEntries(first, second []Entry) bool {
	return slices.EqualFunc(first, second, func(a, b Entry) bool {
		return a.Index == b.Index && a.Term == b.Term && bytes.Equal(a.Command, b.Command)
	})
}

func encodeLog(baseIndex LogIndex, baseTerm Term, entries []Entry) ([]byte, error) {
	if err := validateLogFrom(baseIndex, baseTerm, entries); err != nil {
		return nil, err
	}
	payloadLength := 0
	for _, entry := range entries {
		if len(entry.Command) > MaxCommandBytes {
			return nil, fmt.Errorf("entry %d command exceeds %d bytes", entry.Index, MaxCommandBytes)
		}
		if payloadLength > int(^uint32(0))-logEntryHeaderSize-len(entry.Command) {
			return nil, errors.New("Raft log payload exceeds 4 GiB")
		}
		payloadLength += logEntryHeaderSize + len(entry.Command)
	}
	frame := make([]byte, logStoreHeaderSize+payloadLength)
	binary.BigEndian.PutUint32(frame[0:4], logStoreMagic)
	frame[4] = logStoreVersion
	binary.BigEndian.PutUint64(frame[8:16], uint64(baseIndex))
	binary.BigEndian.PutUint64(frame[16:24], uint64(baseTerm))
	binary.BigEndian.PutUint32(frame[24:28], uint32(len(entries)))
	binary.BigEndian.PutUint32(frame[28:32], uint32(payloadLength))
	offset := logStoreHeaderSize
	for _, entry := range entries {
		binary.BigEndian.PutUint64(frame[offset:offset+8], uint64(entry.Index))
		binary.BigEndian.PutUint64(frame[offset+8:offset+16], uint64(entry.Term))
		binary.BigEndian.PutUint32(frame[offset+16:offset+20], uint32(len(entry.Command)))
		copy(frame[offset+logEntryHeaderSize:], entry.Command)
		offset += logEntryHeaderSize + len(entry.Command)
	}
	binary.BigEndian.PutUint32(frame[32:36], logChecksum(frame))
	return frame, nil
}

func decodeLog(frame []byte) (LogIndex, Term, []Entry, error) {
	if len(frame) < legacyLogHeaderSize {
		return 0, 0, nil, fmt.Errorf("%w: truncated header", ErrLogCorrupt)
	}
	if magic := binary.BigEndian.Uint32(frame[0:4]); magic != logStoreMagic {
		return 0, 0, nil, fmt.Errorf("%w: invalid magic %#x", ErrLogCorrupt, magic)
	}
	if frame[4] == 1 {
		entries, err := decodeLegacyLog(frame)
		return 0, 0, entries, err
	}
	if version := frame[4]; version != logStoreVersion {
		return 0, 0, nil, fmt.Errorf("%w: got %d, want %d", ErrLogVersion, version, logStoreVersion)
	}
	if len(frame) < logStoreHeaderSize {
		return 0, 0, nil, fmt.Errorf("%w: truncated header", ErrLogCorrupt)
	}
	if frame[5] != 0 || frame[6] != 0 || frame[7] != 0 {
		return 0, 0, nil, fmt.Errorf("%w: nonzero reserved bytes", ErrLogCorrupt)
	}
	baseIndex := LogIndex(binary.BigEndian.Uint64(frame[8:16]))
	baseTerm := Term(binary.BigEndian.Uint64(frame[16:24]))
	entryCount := binary.BigEndian.Uint32(frame[24:28])
	payloadLength := binary.BigEndian.Uint32(frame[28:32])
	if uint64(logStoreHeaderSize)+uint64(payloadLength) != uint64(len(frame)) {
		return 0, 0, nil, fmt.Errorf("%w: payload length %d does not match file size", ErrLogCorrupt, payloadLength)
	}
	if uint64(entryCount)*logEntryHeaderSize > uint64(payloadLength) {
		return 0, 0, nil, fmt.Errorf("%w: entry count %d exceeds payload capacity", ErrLogCorrupt, entryCount)
	}
	if checksum := logChecksum(frame); checksum != binary.BigEndian.Uint32(frame[32:36]) {
		return 0, 0, nil, fmt.Errorf("%w: checksum mismatch", ErrLogCorrupt)
	}
	entries := make([]Entry, 0, entryCount)
	offset := logStoreHeaderSize
	for position := uint32(0); position < entryCount; position++ {
		if len(frame)-offset < logEntryHeaderSize {
			return 0, 0, nil, fmt.Errorf("%w: truncated entry %d header", ErrLogCorrupt, position)
		}
		commandLength := binary.BigEndian.Uint32(frame[offset+16 : offset+20])
		if commandLength > MaxCommandBytes || uint64(offset)+logEntryHeaderSize+uint64(commandLength) > uint64(len(frame)) {
			return 0, 0, nil, fmt.Errorf("%w: invalid entry %d command length %d", ErrLogCorrupt, position, commandLength)
		}
		commandStart := offset + logEntryHeaderSize
		commandEnd := commandStart + int(commandLength)
		entries = append(entries, Entry{
			Index:   LogIndex(binary.BigEndian.Uint64(frame[offset : offset+8])),
			Term:    Term(binary.BigEndian.Uint64(frame[offset+8 : offset+16])),
			Command: slices.Clone(frame[commandStart:commandEnd]),
		})
		offset = commandEnd
	}
	if offset != len(frame) {
		return 0, 0, nil, fmt.Errorf("%w: trailing payload bytes", ErrLogCorrupt)
	}
	if err := validateLogFrom(baseIndex, baseTerm, entries); err != nil {
		return 0, 0, nil, fmt.Errorf("%w: %v", ErrLogCorrupt, err)
	}
	return baseIndex, baseTerm, entries, nil
}

func decodeLegacyLog(frame []byte) ([]Entry, error) {
	if frame[5] != 0 || frame[6] != 0 || frame[7] != 0 {
		return nil, fmt.Errorf("%w: nonzero reserved bytes", ErrLogCorrupt)
	}
	entryCount := binary.BigEndian.Uint32(frame[8:12])
	payloadLength := binary.BigEndian.Uint32(frame[12:16])
	if uint64(legacyLogHeaderSize)+uint64(payloadLength) != uint64(len(frame)) {
		return nil, fmt.Errorf("%w: payload length %d does not match file size", ErrLogCorrupt, payloadLength)
	}
	if uint64(entryCount)*logEntryHeaderSize > uint64(payloadLength) {
		return nil, fmt.Errorf("%w: entry count %d exceeds payload capacity", ErrLogCorrupt, entryCount)
	}
	checksum := crc32.New(logChecksumTable)
	_, _ = checksum.Write(frame[4:16])
	_, _ = checksum.Write(frame[legacyLogHeaderSize:])
	if checksum.Sum32() != binary.BigEndian.Uint32(frame[16:20]) {
		return nil, fmt.Errorf("%w: checksum mismatch", ErrLogCorrupt)
	}
	entries := make([]Entry, 0, entryCount)
	offset := legacyLogHeaderSize
	for position := uint32(0); position < entryCount; position++ {
		if len(frame)-offset < logEntryHeaderSize {
			return nil, fmt.Errorf("%w: truncated entry %d header", ErrLogCorrupt, position)
		}
		commandLength := binary.BigEndian.Uint32(frame[offset+16 : offset+20])
		if commandLength > MaxCommandBytes || uint64(offset)+logEntryHeaderSize+uint64(commandLength) > uint64(len(frame)) {
			return nil, fmt.Errorf("%w: invalid entry %d command length %d", ErrLogCorrupt, position, commandLength)
		}
		commandStart := offset + logEntryHeaderSize
		commandEnd := commandStart + int(commandLength)
		entries = append(entries, Entry{
			Index:   LogIndex(binary.BigEndian.Uint64(frame[offset : offset+8])),
			Term:    Term(binary.BigEndian.Uint64(frame[offset+8 : offset+16])),
			Command: slices.Clone(frame[commandStart:commandEnd]),
		})
		offset = commandEnd
	}
	if offset != len(frame) {
		return nil, fmt.Errorf("%w: trailing payload bytes", ErrLogCorrupt)
	}
	if err := validateLog(entries); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrLogCorrupt, err)
	}
	return entries, nil
}

func validateLog(entries []Entry) error {
	return validateLogFrom(0, 0, entries)
}

func validateLogFrom(baseIndex LogIndex, baseTerm Term, entries []Entry) error {
	if (baseIndex == 0) != (baseTerm == 0) {
		return errors.New("log base index and term must both be zero or both be positive")
	}
	for position, entry := range entries {
		if err := entry.Validate(); err != nil {
			return fmt.Errorf("entry %d: %w", position, err)
		}
		wantIndex := baseIndex + LogIndex(position) + 1
		if entry.Index != wantIndex {
			return fmt.Errorf("entry %d index is %d, want %d", position, entry.Index, wantIndex)
		}
		previousTerm := baseTerm
		if position > 0 {
			previousTerm = entries[position-1].Term
		}
		if entry.Term < previousTerm {
			return fmt.Errorf("entry %d term %d is below previous term %d", position, entry.Term, previousTerm)
		}
		if len(entry.Command) > MaxCommandBytes {
			return fmt.Errorf("entry %d command exceeds %d bytes", entry.Index, MaxCommandBytes)
		}
	}
	return nil
}

func logChecksum(frame []byte) uint32 {
	checksum := crc32.New(logChecksumTable)
	_, _ = checksum.Write(frame[4:32])
	_, _ = checksum.Write(frame[logStoreHeaderSize:])
	return checksum.Sum32()
}
