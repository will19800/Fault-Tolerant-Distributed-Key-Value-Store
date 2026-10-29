package raft

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func replaceFileAtomically(path, temporaryPattern string, data []byte) error {
	directoryPath := filepath.Dir(path)
	temporary, err := os.CreateTemp(directoryPath, temporaryPattern)
	if err != nil {
		return fmt.Errorf("create temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)

	if err := writeFileAndSync(temporary, data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary file: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("publish file: %w", err)
	}
	directory, err := os.Open(directoryPath)
	if err != nil {
		return fmt.Errorf("open parent directory: %w", err)
	}
	if err := errors.Join(directory.Sync(), directory.Close()); err != nil {
		return fmt.Errorf("sync parent directory: %w", err)
	}
	return nil
}

func writeFileAndSync(file *os.File, data []byte) error {
	for len(data) > 0 {
		written, err := file.Write(data)
		if err != nil {
			return fmt.Errorf("write temporary file: %w", err)
		}
		if written == 0 {
			return fmt.Errorf("write temporary file: %w", io.ErrShortWrite)
		}
		data = data[written:]
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync temporary file: %w", err)
	}
	return nil
}
