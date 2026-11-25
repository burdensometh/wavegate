package statefile

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"wavegate/v1"
)

func Read(path string, value any) (bool, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer file.Close()
	if err := v1.Decode(file, value); err != nil {
		return false, fmt.Errorf("%s: %w", path, err)
	}
	return true, nil
}

// Write replaces a complete document. A post-rename error leaves the caller's
// view uncertain. Stores stop accepting mutations until reopened in that case.
func Write(path string, value any) (committed bool, err error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return false, err
	}
	temp, err := os.CreateTemp(dir, ".wavegate-*")
	if err != nil {
		return false, err
	}
	defer os.Remove(temp.Name())
	if err := json.NewEncoder(temp).Encode(value); err != nil {
		_ = temp.Close()
		return false, err
	}
	syncErr := temp.Sync()
	closeErr := temp.Close()
	if err := errors.Join(syncErr, closeErr); err != nil {
		return false, err
	}
	if err := os.Rename(temp.Name(), path); err != nil {
		return false, err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return true, err
	}
	return true, errors.Join(directory.Sync(), directory.Close())
}
