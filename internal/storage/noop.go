package storage

import (
	"context"
	"io"
)

type Noop struct{}

func newNoop() *Noop {
	return &Noop{}
}

func (n *Noop) Create(_ context.Context, _ string) (io.WriteCloser, error) {
	return &nopCloser{Writer: io.Discard}, nil
}

func (n *Noop) Close() error {
	return nil
}

type nopCloser struct {
	io.Writer
}

func (*nopCloser) Close() error {
	return nil
}
