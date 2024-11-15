package storage

import (
	"context"
	"io"
	"path"

	"cloud.google.com/go/storage"
)

type GCS struct {
	bucket   *storage.BucketHandle
	basePath string
	closer   io.Closer
}

func newGCS(ctx context.Context, bucket, basePath string) (*GCS, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	return &GCS{
		bucket:   client.Bucket(bucket),
		basePath: basePath,
		closer:   client,
	}, nil
}

func (gcs *GCS) Create(ctx context.Context, relativePath string) (io.WriteCloser, error) {
	fullPath := path.Join(gcs.basePath, relativePath)
	wc := gcs.bucket.Object(fullPath).NewWriter(ctx)
	wc.ChunkSize = 0
	return wc, nil
}

func (gcs *GCS) Close() error {
	return gcs.closer.Close()
}
