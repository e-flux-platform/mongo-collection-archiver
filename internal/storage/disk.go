package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type Disk struct {
	basePath string
}

func newDisk(basePath string) *Disk {
	return &Disk{
		basePath: basePath,
	}
}

func (d *Disk) Create(_ context.Context, relativePath string) (io.WriteCloser, error) {
	absPath, err := filepath.Abs(filepath.Join(d.basePath, relativePath))
	if err != nil {
		return nil, err
	}
	absDir := filepath.Dir(absPath)
	if err = os.MkdirAll(absDir, 0700); err != nil {
		return nil, err
	}
	return os.Create(absPath)
}

func (d *Disk) Exists(_ context.Context, relativePath string) (bool, error) {
	absPath, err := filepath.Abs(filepath.Join(d.basePath, relativePath))
	if err != nil {
		return false, err
	}
	if _, err = os.Stat(absPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("failed to stat file: %w", err)
	}
	return true, nil
}

func (d *Disk) Close() error {
	return nil
}
