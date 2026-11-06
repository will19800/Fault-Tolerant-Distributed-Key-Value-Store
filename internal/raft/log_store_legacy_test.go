package raft

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestFileLogStoreLoadsVersionOneLog(t *testing.T) {
	entries := []Entry{{Index: 1, Term: 1, Command: []byte("existing")}}
	payloadLength := logEntryHeaderSize + len(entries[0].Command)
	frame := make([]byte, legacyLogHeaderSize+payloadLength)
	binary.BigEndian.PutUint32(frame[0:4], logStoreMagic)
	frame[4] = 1
	binary.BigEndian.PutUint32(frame[8:12], 1)
	binary.BigEndian.PutUint32(frame[12:16], uint32(payloadLength))
	offset := legacyLogHeaderSize
	binary.BigEndian.PutUint64(frame[offset:offset+8], 1)
	binary.BigEndian.PutUint64(frame[offset+8:offset+16], 1)
	binary.BigEndian.PutUint32(frame[offset+16:offset+20], uint32(len(entries[0].Command)))
	copy(frame[offset+logEntryHeaderSize:], entries[0].Command)
	digest := crc32.New(logChecksumTable)
	_, _ = digest.Write(frame[4:16])
	_, _ = digest.Write(frame[legacyLogHeaderSize:])
	binary.BigEndian.PutUint32(frame[16:20], digest.Sum32())

	path := filepath.Join(t.TempDir(), "raft-log")
	if err := os.WriteFile(path, frame, 0o600); err != nil {
		t.Fatal(err)
	}
	store, _ := NewFileLogStore(path)
	if got, err := store.Load(); err != nil || !reflect.DeepEqual(got, entries) {
		t.Fatalf("Load = %+v, %v; want %+v", got, err, entries)
	}
}
