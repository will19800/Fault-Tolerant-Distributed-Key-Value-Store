package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"slices"
	"sync"
)

const (
	logStoreVersion    uint8  = 1
	logStoreMagic      uint32 = 0x4b56524c // KVRL
	logStoreHeaderSize        = 20
	logEntryHeaderSize        = 20
	MaxCommandBytes           = 64 << 20 // 64 MiB
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
	mu   sync.Mutex
	path string
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
	entries, err := decodeLog(frame)
	if err != nil {
		return nil, fmt.Errorf("decode Raft log %q: %w", s.path, err)
	}
	return entries, nil
}

// Save validates and durably replaces the complete log.
func (s *FileLogStore) Save(entries []Entry) error {
	frame, err := encodeLog(entries)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return replaceFileAtomically(s.path, ".raft-log-*", frame)
}

func encodeLog(entries []Entry) ([]byte, error) {
	if err := validateLog(entries); err != nil {
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
	binary.BigEndian.PutUint32(frame[8:12], uint32(len(entries)))
	binary.BigEndian.PutUint32(frame[12:16], uint32(payloadLength))
	offset := logStoreHeaderSize
	for _, entry := range entries {
		binary.BigEndian.PutUint64(frame[offset:offset+8], uint64(entry.Index))
		binary.BigEndian.PutUint64(frame[offset+8:offset+16], uint64(entry.Term))
		binary.BigEndian.PutUint32(frame[offset+16:offset+20], uint32(len(entry.Command)))
		copy(frame[offset+logEntryHeaderSize:], entry.Command)
		offset += logEntryHeaderSize + len(entry.Command)
	}
	binary.BigEndian.PutUint32(frame[16:20], logChecksum(frame))
	return frame, nil
}

func decodeLog(frame []byte) ([]Entry, error) {
	if len(frame) < logStoreHeaderSize {
		return nil, fmt.Errorf("%w: truncated header", ErrLogCorrupt)
	}
	if magic := binary.BigEndian.Uint32(frame[0:4]); magic != logStoreMagic {
		return nil, fmt.Errorf("%w: invalid magic %#x", ErrLogCorrupt, magic)
	}
	if version := frame[4]; version != logStoreVersion {
		return nil, fmt.Errorf("%w: got %d, want %d", ErrLogVersion, version, logStoreVersion)
	}
	if frame[5] != 0 || frame[6] != 0 || frame[7] != 0 {
		return nil, fmt.Errorf("%w: nonzero reserved bytes", ErrLogCorrupt)
	}
	entryCount := binary.BigEndian.Uint32(frame[8:12])
	payloadLength := binary.BigEndian.Uint32(frame[12:16])
	if uint64(logStoreHeaderSize)+uint64(payloadLength) != uint64(len(frame)) {
		return nil, fmt.Errorf("%w: payload length %d does not match file size", ErrLogCorrupt, payloadLength)
	}
	if uint64(entryCount)*logEntryHeaderSize > uint64(payloadLength) {
		return nil, fmt.Errorf("%w: entry count %d exceeds payload capacity", ErrLogCorrupt, entryCount)
	}
	if checksum := logChecksum(frame); checksum != binary.BigEndian.Uint32(frame[16:20]) {
		return nil, fmt.Errorf("%w: checksum mismatch", ErrLogCorrupt)
	}
	entries := make([]Entry, 0, entryCount)
	offset := logStoreHeaderSize
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
	for position, entry := range entries {
		if err := entry.Validate(); err != nil {
			return fmt.Errorf("entry %d: %w", position, err)
		}
		wantIndex := LogIndex(position + 1)
		if entry.Index != wantIndex {
			return fmt.Errorf("entry %d index is %d, want %d", position, entry.Index, wantIndex)
		}
		if position > 0 && entry.Term < entries[position-1].Term {
			return fmt.Errorf("entry %d term %d is below previous term %d", position, entry.Term, entries[position-1].Term)
		}
		if len(entry.Command) > MaxCommandBytes {
			return fmt.Errorf("entry %d command exceeds %d bytes", entry.Index, MaxCommandBytes)
		}
	}
	return nil
}

func logChecksum(frame []byte) uint32 {
	checksum := crc32.New(logChecksumTable)
	_, _ = checksum.Write(frame[4:16])
	_, _ = checksum.Write(frame[logStoreHeaderSize:])
	return checksum.Sum32()
}
