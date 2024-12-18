package archive

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path"
	"time"

	"github.com/e-flux-platform/mongo-collection-archiver/internal/source"
)

// Archiver deals with archiving documents from a particular source
type Archiver struct {
	source                documentSource
	store                 store
	skipDelete            bool
	ignoreFileExistsError bool
	delay                 time.Duration
}

type documentSource interface {
	FindAllFromDate(ctx context.Context, date time.Time) source.StreamingResult
	DeleteAllFromDate(ctx context.Context, date time.Time) (int, error)
	EarliestCreatedAt(ctx context.Context) (time.Time, error)
}

type store interface {
	Create(ctx context.Context, path string) (io.WriteCloser, error)
	Exists(ctx context.Context, path string) (bool, error)
}

// NewArchiver initializes and returns an Archiver
func NewArchiver(source documentSource, storage store, skipDelete, ignoreFileExistsError bool, delay time.Duration) *Archiver {
	return &Archiver{
		source:                source,
		store:                 storage,
		skipDelete:            skipDelete,
		ignoreFileExistsError: ignoreFileExistsError,
		delay:                 delay,
	}
}

// Run executes the archiving process
func (a *Archiver) Run(ctx context.Context, target time.Time) error {
	// Resolve the earliest document in the collection
	earliest, err := a.source.EarliestCreatedAt(ctx)
	if err != nil {
		return fmt.Errorf("failed to get earliest created at: %w", err)
	}

	slog.Info(
		"archiver running",
		slog.String("target", target.String()),
		slog.String("earliest", earliest.String()),
	)

	// Iterate one day at a time, until we hit the target
	var total int
	for date := earliest.Truncate(time.Hour * 24); date.Before(target); date = date.AddDate(0, 0, 1) {
		slog.Info("archiving", slog.String("date", date.String()))

		if err = a.archiveDocumentsAndDelete(ctx, date); err != nil {
			return fmt.Errorf("archival failed: %w", err)
		}

		total++

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(a.delay):
		}
	}

	slog.Info("target reached", slog.Int("datesArchived", total))

	return nil
}

func (a *Archiver) archiveDocumentsAndDelete(ctx context.Context, date time.Time) error {
	if err := a.archiveDocuments(ctx, date); err != nil {
		return fmt.Errorf("failed to archive documents: %w", err)
	}
	if !a.skipDelete {
		deleted, err := a.source.DeleteAllFromDate(ctx, date)
		if err != nil {
			return fmt.Errorf("failed to delete documents: %w", err)
		}
		slog.Info("documents deleted", slog.Int("total", deleted))
	}
	return nil
}

func (a *Archiver) archiveDocuments(ctx context.Context, date time.Time) (err error) {
	fileName := path.Join(
		date.Format("2006"),
		date.Format("01"),
		date.Format("02")+".json.gz",
	)

	// Check if target file already exists - the default behaviour of the storage implementations is to overwrite
	exists, err := a.store.Exists(ctx, fileName)
	if err != nil {
		return fmt.Errorf("failed to check if file exists: %w", err)
	}
	if exists {
		slog.Error("target file already exists", slog.String("file", fileName))
		if a.ignoreFileExistsError {
			// Archiver can be configured to skip past cases of the target file already existing. This should only be
			// done if it is safe to assume the file already contains the full set of documents that is expected to be
			// deleted.
			slog.Warn("skipping documents write")
			return nil
		}
		return errors.New("target file exists")
	}

	slog.Info("writing to file", slog.String("fileName", fileName))

	// Create target file in the underlying store
	w, err := a.store.Create(ctx, fileName)
	if err != nil {
		return err
	}
	defer func() {
		// Close the file writer
		if cErr := w.Close(); cErr != nil {
			err = errors.Join(err, fmt.Errorf("failed to close file: %w", cErr))
		}
	}()

	// Contents will be gzipped
	gw, err := gzip.NewWriterLevel(w, gzip.DefaultCompression)
	if err != nil {
		return err
	}
	defer func() {
		// Close the gzip writer - note that does not close the underlying file writer
		if cErr := gw.Close(); cErr != nil {
			err = errors.Join(err, fmt.Errorf("failed to close gzip writer: %w", cErr))
		}
	}()

	// Iterate each document to be archived
	var i int
	res := a.source.FindAllFromDate(ctx, date)
	for doc := range res.Iter(ctx) {
		i++
		buf := bytes.NewBuffer(doc)
		if err = buf.WriteByte('\n'); err != nil {
			return err
		}
		if _, err = io.Copy(gw, buf); err != nil {
			return err
		}
	}
	if err = res.Err(); err != nil {
		return err
	}

	slog.Info("documents written", slog.Int("total", i))

	return nil
}
