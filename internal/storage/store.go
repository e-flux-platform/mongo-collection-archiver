package storage

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
)

type Store interface {
	Create(ctx context.Context, path string) (io.WriteCloser, error)
	io.Closer
}

func FromURL(ctx context.Context, rawURL string) (Store, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}

	switch u.Scheme {
	case "file":
		return newDisk(u.Path), nil
	case "gcs":
		return newGCS(ctx, u.Host, strings.TrimPrefix(u.Path, "/"))
	case "noop":
		return newNoop(), nil
	default:
		return nil, fmt.Errorf("unsupported storage scheme: %s", u.Scheme)
	}
}
