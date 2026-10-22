package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	CurrentVersion uint8 = 1

	MaxKeyBytes   = 1 << 20  // 1 MiB
	MaxValueBytes = 64 << 20 // 64 MiB

	frameMagic uint32 = 0x4b56574c // KVWL
	headerSize        = 20

	magicOffset       = 0
	versionOffset     = 4
	operationOffset   = 5
	flagsOffset       = 6
	keyLengthOffset   = 8
	valueLengthOffset = 12
	checksumOffset    = 16
)

var (
	ErrTruncatedRecord    = errors.New("truncated WAL record")
	ErrInvalidMagic       = errors.New("invalid WAL magic")
	ErrUnsupportedVersion = errors.New("unsupported WAL version")
	ErrInvalidOperation   = errors.New("invalid WAL operation")
	ErrInvalidFlags       = errors.New("invalid WAL flags")
	ErrInvalidKey         = errors.New("invalid WAL key")
	ErrValueTooLarge      = errors.New("WAL value too large")
	ErrUnexpectedValue    = errors.New("delete record contains a value")
	ErrChecksumMismatch   = errors.New("WAL checksum mismatch")
)

var checksumTable = crc32.MakeTable(crc32.Castagnoli)

// Operation identifies the state-machine mutation stored in a record.
type Operation uint8

const (
	OperationPut Operation = iota + 1
	OperationDelete
)

// Record is one durable key-value mutation.
type Record struct {
	Operation Operation
	Key       string
	Value     []byte
}

// Encode serializes one record into the current WAL frame format.
func Encode(record Record) ([]byte, error) {
	if err := validateRecord(record); err != nil {
		return nil, err
	}

	keyLength := len(record.Key)
	valueLength := len(record.Value)
	frame := make([]byte, headerSize+keyLength+valueLength)
	binary.BigEndian.PutUint32(frame[magicOffset:versionOffset], frameMagic)
	frame[versionOffset] = CurrentVersion
	frame[operationOffset] = byte(record.Operation)
	binary.BigEndian.PutUint32(frame[keyLengthOffset:valueLengthOffset], uint32(keyLength))
	binary.BigEndian.PutUint32(frame[valueLengthOffset:checksumOffset], uint32(valueLength))
	copy(frame[headerSize:headerSize+keyLength], record.Key)
	copy(frame[headerSize+keyLength:], record.Value)
	binary.BigEndian.PutUint32(frame[checksumOffset:headerSize], frameChecksum(frame))
	return frame, nil
}

// Decode reads and validates one record. It returns io.EOF when reader has no
// next frame and ErrTruncatedRecord when only part of the next frame is present.
func Decode(reader io.Reader) (Record, error) {
	header := make([]byte, headerSize)
	if _, err := io.ReadFull(reader, header); err != nil {
		if errors.Is(err, io.EOF) {
			return Record{}, io.EOF
		}
		return Record{}, fmt.Errorf("%w: header: %v", ErrTruncatedRecord, err)
	}

	if got := binary.BigEndian.Uint32(header[magicOffset:versionOffset]); got != frameMagic {
		return Record{}, fmt.Errorf("%w: got %#x", ErrInvalidMagic, got)
	}
	if got := header[versionOffset]; got != CurrentVersion {
		return Record{}, fmt.Errorf("%w: got %d, want %d", ErrUnsupportedVersion, got, CurrentVersion)
	}
	operation := Operation(header[operationOffset])
	if operation != OperationPut && operation != OperationDelete {
		return Record{}, fmt.Errorf("%w: %d", ErrInvalidOperation, operation)
	}
	if flags := binary.BigEndian.Uint16(header[flagsOffset:keyLengthOffset]); flags != 0 {
		return Record{}, fmt.Errorf("%w: %#x", ErrInvalidFlags, flags)
	}

	keyLength := binary.BigEndian.Uint32(header[keyLengthOffset:valueLengthOffset])
	valueLength := binary.BigEndian.Uint32(header[valueLengthOffset:checksumOffset])
	if keyLength == 0 || keyLength > MaxKeyBytes {
		return Record{}, fmt.Errorf("%w: length %d exceeds range 1..%d", ErrInvalidKey, keyLength, MaxKeyBytes)
	}
	if valueLength > MaxValueBytes {
		return Record{}, fmt.Errorf("%w: length %d exceeds %d", ErrValueTooLarge, valueLength, MaxValueBytes)
	}
	if operation == OperationDelete && valueLength != 0 {
		return Record{}, fmt.Errorf("%w: length %d", ErrUnexpectedValue, valueLength)
	}

	payload := make([]byte, int(keyLength)+int(valueLength))
	if _, err := io.ReadFull(reader, payload); err != nil {
		return Record{}, fmt.Errorf("%w: payload: %v", ErrTruncatedRecord, err)
	}

	frame := append(header, payload...)
	wantChecksum := binary.BigEndian.Uint32(header[checksumOffset:headerSize])
	if gotChecksum := frameChecksum(frame); gotChecksum != wantChecksum {
		return Record{}, fmt.Errorf("%w: got %#x, want %#x", ErrChecksumMismatch, gotChecksum, wantChecksum)
	}

	return Record{
		Operation: operation,
		Key:       string(payload[:keyLength]),
		Value:     payload[keyLength:],
	}, nil
}

func validateRecord(record Record) error {
	if record.Operation != OperationPut && record.Operation != OperationDelete {
		return fmt.Errorf("%w: %d", ErrInvalidOperation, record.Operation)
	}
	if len(record.Key) == 0 || len(record.Key) > MaxKeyBytes {
		return fmt.Errorf("%w: length %d exceeds range 1..%d", ErrInvalidKey, len(record.Key), MaxKeyBytes)
	}
	if len(record.Value) > MaxValueBytes {
		return fmt.Errorf("%w: length %d exceeds %d", ErrValueTooLarge, len(record.Value), MaxValueBytes)
	}
	if record.Operation == OperationDelete && len(record.Value) != 0 {
		return fmt.Errorf("%w: length %d", ErrUnexpectedValue, len(record.Value))
	}
	return nil
}

func frameChecksum(frame []byte) uint32 {
	checksum := crc32.New(checksumTable)
	_, _ = checksum.Write(frame[versionOffset:checksumOffset])
	_, _ = checksum.Write(frame[headerSize:])
	return checksum.Sum32()
}
