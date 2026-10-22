// Package wal encodes and persists versioned write-ahead log records.
//
// Each record is a self-contained frame containing a magic number, format
// version, operation, key and value lengths, a CRC32C checksum, and the key and
// value bytes. Decode returns io.EOF only when no next frame exists; a partial
// frame returns ErrTruncatedRecord so recovery code can distinguish a clean end
// of file from an interrupted append. A newly opened Log must be replayed
// before it accepts appends, ensuring an interrupted tail is repaired first.
package wal
