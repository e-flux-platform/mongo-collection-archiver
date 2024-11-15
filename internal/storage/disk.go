package storage

import (
	"context"
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

func (d *Disk) Create(_ context.Context, path string) (io.WriteCloser, error) {
	absPath, err := filepath.Abs(filepath.Join(d.basePath, path))
	if err != nil {
		return nil, err
	}
	absDir := filepath.Dir(absPath)
	if err = os.MkdirAll(absDir, 0700); err != nil {
		return nil, err
	}
	return os.Create(absPath)
}

func (d *Disk) Close() error {
	return nil
}
