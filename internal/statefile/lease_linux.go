package statefile

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

func Lock(dir string) (*os.File, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(filepath.Join(dir, ".wavegate.lock"), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("lock state directory: %w", err)
	}
	return file, nil
}
