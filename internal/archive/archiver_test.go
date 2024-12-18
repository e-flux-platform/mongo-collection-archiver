package archive_test

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"iter"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e-flux-platform/mongo-collection-archiver/internal/archive"
	"github.com/e-flux-platform/mongo-collection-archiver/internal/source"
)

func TestArchiver(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("without delete skipping", func(t *testing.T) {
		t.Parallel()

		doc1 := `{"id":1}`
		doc2 := `{"id":2}`
		doc3 := `{"id":3}`
		doc4 := `{"id":4}`
		doc5 := `{"id":5}`

		day1 := time.Date(2024, time.November, 1, 0, 0, 0, 0, time.UTC)
		day2 := day1.AddDate(0, 0, 1)
		day3 := day2.AddDate(0, 0, 1)

		src := newMockDocumentSource()
		src.add(day1, doc1)
		src.add(day1, doc2)
		src.add(day2, doc3)
		src.add(day2, doc4)
		src.add(day3, doc5)

		dest := newMockStorage()

		archiver := archive.NewArchiver(src, dest, false, false, time.Duration(0))
		err := archiver.Run(ctx, day3)
		require.NoError(t, err)
		assert.Len(t, dest.files, 2) // 3rd file should not be written, since that is the target

		day1Docs, err := dest.read("2024/11/01.json.gz")
		require.NoError(t, err)
		assert.Equal(t, []string{doc1, doc2}, day1Docs)

		day2Docs, err := dest.read("2024/11/02.json.gz")
		require.NoError(t, err)
		assert.Equal(t, []string{doc3, doc4}, day2Docs)

		earliest, err := src.EarliestCreatedAt(ctx)
		require.NoError(t, err)
		assert.Equal(t, day3, earliest)
	})

	t.Run("with delete skipping", func(t *testing.T) {
		t.Parallel()

		doc := `{"id":1}`
		day := time.Date(2024, time.November, 1, 0, 0, 0, 0, time.UTC)

		src := newMockDocumentSource()
		src.add(day, doc)

		dest := newMockStorage()

		archiver := archive.NewArchiver(src, dest, true, false, time.Duration(0))
		err := archiver.Run(ctx, day.AddDate(0, 0, 1))
		require.NoError(t, err)
		assert.Len(t, dest.files, 1)

		day1Docs, err := dest.read("2024/11/01.json.gz")
		require.NoError(t, err)
		assert.Equal(t, []string{doc}, day1Docs)

		earliest, err := src.EarliestCreatedAt(ctx)
		require.NoError(t, err)
		assert.Equal(t, day, earliest) // documents have not been deleted, so the date hasn't changed
	})

	t.Run("with file close error", func(t *testing.T) {
		t.Parallel()

		doc := `{"id":1}`
		day := time.Date(2024, time.November, 1, 0, 0, 0, 0, time.UTC)

		src := newMockDocumentSource()
		src.add(day, doc)

		dest := newMockStorage()
		dest.forceCloseError = errors.New("force-close-error")

		archiver := archive.NewArchiver(src, dest, false, false, time.Duration(0))
		err := archiver.Run(ctx, day.AddDate(0, 0, 1))
		assert.Error(t, err)
		assert.ErrorIs(t, err, dest.forceCloseError)

		earliest, err := src.EarliestCreatedAt(ctx)
		require.NoError(t, err)
		assert.Equal(t, day, earliest) // document should not have been deleted
	})

	t.Run("with file already exists and no ignore", func(t *testing.T) {
		t.Parallel()

		doc := `{"id":1}`
		day := time.Date(2024, time.November, 1, 0, 0, 0, 0, time.UTC)

		src := newMockDocumentSource()
		src.add(day, doc)

		dest := newMockStorage()
		dest.files["2024/11/01.json.gz"] = bytes.NewBuffer(nil)

		archiver := archive.NewArchiver(src, dest, false, false, time.Duration(0))
		err := archiver.Run(ctx, day.AddDate(0, 0, 1))
		assert.Error(t, err)
		assert.ErrorContains(t, err, "file exists")
	})

	t.Run("with file already exists and ignore", func(t *testing.T) {
		t.Parallel()

		doc := `{"id":1}`
		day := time.Date(2024, time.November, 1, 0, 0, 0, 0, time.UTC)

		src := newMockDocumentSource()
		src.add(day, doc)

		dest := newMockStorage()
		dest.files["2024/11/01.json.gz"] = bytes.NewBuffer(nil)

		archiver := archive.NewArchiver(src, dest, false, true, time.Duration(0))
		err := archiver.Run(ctx, day.AddDate(0, 0, 1))
		assert.NoError(t, err)

		assert.Len(t, src.docs, 0)
	})
}

type mockDocumentSource struct {
	docs map[time.Time][][]byte
}

func newMockDocumentSource() *mockDocumentSource {
	return &mockDocumentSource{
		docs: make(map[time.Time][][]byte),
	}
}

func (m *mockDocumentSource) add(date time.Time, doc string) {
	docs, found := m.docs[date]
	if !found {
		docs = [][]byte{}
	}
	docs = append(docs, []byte(doc))
	m.docs[date] = docs
}

func (m *mockDocumentSource) FindAllFromDate(_ context.Context, date time.Time) source.StreamingResult {
	return &mockStreamingResult{
		docs: m.docs[date],
	}
}

func (m *mockDocumentSource) DeleteAllFromDate(_ context.Context, date time.Time) (int, error) {
	total := len(m.docs[date])
	delete(m.docs, date)
	return total, nil
}

func (m *mockDocumentSource) EarliestCreatedAt(_ context.Context) (time.Time, error) {
	if len(m.docs) == 0 {
		return time.Time{}, errors.New("no documents found")
	}
	var earliest time.Time
	for t := range m.docs {
		if t.Before(earliest) || earliest.IsZero() {
			earliest = t
		}
	}
	return earliest, nil
}

type mockStreamingResult struct {
	docs [][]byte
}

func (m *mockStreamingResult) Iter(_ context.Context) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		for _, doc := range m.docs {
			if !yield(doc) {
				return
			}
		}
	}
}

func (m *mockStreamingResult) Err() error {
	return nil
}

type mockStorage struct {
	files           map[string]*bytes.Buffer
	forceCloseError error
}

func newMockStorage() *mockStorage {
	return &mockStorage{
		files: make(map[string]*bytes.Buffer),
	}
}

func (m *mockStorage) Create(_ context.Context, path string) (io.WriteCloser, error) {
	buf := bytes.NewBuffer(nil)
	m.files[path] = buf
	return &errCloser{
		Writer: buf,
		err:    m.forceCloseError,
	}, nil
}

func (m *mockStorage) Exists(_ context.Context, path string) (bool, error) {
	_, exists := m.files[path]
	return exists, nil
}

func (m *mockStorage) read(path string) ([]string, error) {
	buf := m.files[path]

	var reader io.Reader
	if strings.HasSuffix(path, ".gz") {
		gz, err := gzip.NewReader(buf)
		if err != nil {
			return nil, err
		}
		defer gz.Close()
		reader = gz
	} else {
		reader = buf
	}

	lines := make([]string, 0)
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return lines, nil
}

type errCloser struct {
	io.Writer
	err error
}

func (e *errCloser) Close() error {
	return e.err
}
