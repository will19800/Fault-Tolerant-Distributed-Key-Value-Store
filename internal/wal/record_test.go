package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
)

func TestRecordRoundTrip(t *testing.T) {
	tests := []Record{
		{Operation: OperationPut, Key: "user:123:preferences", Value: []byte(`{"theme":"dark"}`)},
		{Operation: OperationPut, Key: "empty-value", Value: []byte{}},
		{Operation: OperationPut, Key: "binary", Value: []byte{0x00, 0x7f, 0xff}},
		{Operation: OperationDelete, Key: "obsolete"},
	}

	for _, want := range tests {
		encoded, err := Encode(want)
		if err != nil {
			t.Fatalf("Encode(%+v): %v", want, err)
		}
		got, err := Decode(bytes.NewReader(encoded))
		if err != nil {
			t.Fatalf("Decode(%+v): %v", want, err)
		}
		if got.Operation != want.Operation || got.Key != want.Key || !slices.Equal(got.Value, want.Value) {
			t.Fatalf("round trip = %+v, want %+v", got, want)
		}
	}
}

func TestDecodeConsecutiveFrames(t *testing.T) {
	first, err := Encode(Record{Operation: OperationPut, Key: "key", Value: []byte("value")})
	if err != nil {
		t.Fatal(err)
	}
	second, err := Encode(Record{Operation: OperationDelete, Key: "key"})
	if err != nil {
		t.Fatal(err)
	}
	reader := bytes.NewReader(append(first, second...))

	if record, err := Decode(reader); err != nil || record.Operation != OperationPut {
		t.Fatalf("first Decode = %+v, %v", record, err)
	}
	if record, err := Decode(reader); err != nil || record.Operation != OperationDelete {
		t.Fatalf("second Decode = %+v, %v", record, err)
	}
	if _, err := Decode(reader); !errors.Is(err, io.EOF) {
		t.Fatalf("final Decode error = %v, want io.EOF", err)
	}
}

func TestEncodeRejectsInvalidRecords(t *testing.T) {
	tests := []struct {
		name   string
		record Record
		want   error
	}{
		{name: "unknown operation", record: Record{Operation: 99, Key: "key"}, want: ErrInvalidOperation},
		{name: "empty key", record: Record{Operation: OperationPut}, want: ErrInvalidKey},
		{name: "oversized key", record: Record{Operation: OperationPut, Key: strings.Repeat("k", MaxKeyBytes+1)}, want: ErrInvalidKey},
		{name: "delete value", record: Record{Operation: OperationDelete, Key: "key", Value: []byte("value")}, want: ErrUnexpectedValue},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := Encode(test.record); !errors.Is(err, test.want) {
				t.Fatalf("Encode error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestDecodeDetectsTruncatedFrame(t *testing.T) {
	frame, err := Encode(Record{Operation: OperationPut, Key: "key", Value: []byte("value")})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := Decode(bytes.NewReader(nil)); !errors.Is(err, io.EOF) {
		t.Fatalf("empty Decode error = %v, want io.EOF", err)
	}
	for length := 1; length < len(frame); length++ {
		if _, err := Decode(bytes.NewReader(frame[:length])); !errors.Is(err, ErrTruncatedRecord) {
			t.Fatalf("Decode prefix length %d error = %v, want %v", length, err, ErrTruncatedRecord)
		}
	}
}

func TestDecodeRejectsCorruption(t *testing.T) {
	valid, err := Encode(Record{Operation: OperationPut, Key: "key", Value: []byte("value")})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		mutate func([]byte)
		want   error
	}{
		{name: "magic", mutate: func(frame []byte) { frame[magicOffset] ^= 0xff }, want: ErrInvalidMagic},
		{name: "version", mutate: func(frame []byte) { frame[versionOffset]++ }, want: ErrUnsupportedVersion},
		{name: "operation", mutate: func(frame []byte) { frame[operationOffset] = 99 }, want: ErrInvalidOperation},
		{name: "flags", mutate: func(frame []byte) { frame[flagsOffset] = 1 }, want: ErrInvalidFlags},
		{name: "key length", mutate: func(frame []byte) {
			binary.BigEndian.PutUint32(frame[keyLengthOffset:valueLengthOffset], MaxKeyBytes+1)
		}, want: ErrInvalidKey},
		{name: "value length", mutate: func(frame []byte) {
			binary.BigEndian.PutUint32(frame[valueLengthOffset:checksumOffset], MaxValueBytes+1)
		}, want: ErrValueTooLarge},
		{name: "payload checksum", mutate: func(frame []byte) { frame[len(frame)-1] ^= 0xff }, want: ErrChecksumMismatch},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			frame := slices.Clone(valid)
			test.mutate(frame)
			if _, err := Decode(bytes.NewReader(frame)); !errors.Is(err, test.want) {
				t.Fatalf("Decode error = %v, want %v", err, test.want)
			}
		})
	}
}
