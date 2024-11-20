package archive_test

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/e-flux-platform/mongo-collection-archiver/internal/archive"
	"github.com/e-flux-platform/mongo-collection-archiver/internal/source"
	"github.com/e-flux-platform/mongo-collection-archiver/internal/storage"
	"github.com/e-flux-platform/mongo-collection-archiver/internal/testutil"
)

func TestArchiver_Integration(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client := testutil.StartMongoDB(ctx, t)

	date := time.Date(2024, time.November, 1, 0, 0, 0, 0, time.UTC)

	doc1 := bson.M{
		"_id":       objectIDFromHex(t, "5d6fd699ee45770009e17140"),
		"createdAt": primitive.NewDateTimeFromTime(date.Add(time.Second * -1)),
	}
	doc2 := bson.M{
		"_id":       objectIDFromHex(t, "5d6fd8ec10ca90000998cf31"),
		"createdAt": primitive.NewDateTimeFromTime(date),
	}
	doc3 := bson.M{
		"_id":       objectIDFromHex(t, "5d6fdf658a583b0009929c06"),
		"createdAt": primitive.NewDateTimeFromTime(date.Add(time.Hour * 3)),
	}
	doc4 := bson.M{
		"_id":       objectIDFromHex(t, "5d6fdf85451f58001939950a"),
		"createdAt": primitive.NewDateTimeFromTime(date.Add(time.Hour * 24)),
	}

	// Populate collection with the above 4 documents
	collection := client.Database(uuid.NewString()).Collection("test")
	_, err := collection.InsertMany(ctx, []any{doc1, doc2, doc3, doc4})
	require.NoError(t, err)

	// Prepare temp directory for disk store to use
	baseDir, err := os.MkdirTemp(os.TempDir(), "mongo-collection-archiver")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = os.RemoveAll(baseDir)
	})

	// Initialise source and target
	src := source.NewMongoDB(collection)
	target, err := storage.FromURL(ctx, fmt.Sprintf("file://%s", baseDir))
	require.NoError(t, err)
	defer target.Close()

	// Run archiver
	archiver := archive.NewArchiver(src, target, false, false, time.Duration(0))
	err = archiver.Run(ctx, date.Add(time.Hour*24))
	require.NoError(t, err)

	// Verify only doc4 remains
	ids := readMongoIDs(ctx, t, collection)
	require.Len(t, ids, 1)
	assert.Equal(t, "5d6fdf85451f58001939950a", ids[0].Hex()) // doc4

	// Verify doc1 was archived
	archived1 := readFile(t, filepath.Join(baseDir, "2024/10/31.json.gz"))
	assert.Equal(t, []bson.M{doc1}, archived1)

	// Verify doc2 and doc3 were archived
	archived2 := readFile(t, filepath.Join(baseDir, "2024/11/01.json.gz"))
	assert.Equal(t, []bson.M{doc2, doc3}, archived2)

	// Verify we can restore doc1, doc2, and doc3
	_, err = collection.InsertMany(ctx, []any{archived1[0], archived2[0], archived2[1]})
	require.NoError(t, err)
	ids = readMongoIDs(ctx, t, collection)
	require.Len(t, ids, 4)

	// Verify earliest is resolved to the createdAt time of doc1
	earliest, err := src.EarliestCreatedAt(ctx)
	require.NoError(t, err)
	assert.Equal(t, date.Add(time.Second*-1), earliest)
}

func readMongoIDs(ctx context.Context, t *testing.T, collection *mongo.Collection) (ids []primitive.ObjectID) {
	t.Helper()

	cursor, err := collection.Find(ctx, bson.M{})
	require.NoError(t, err)

	var docs []struct {
		ID primitive.ObjectID `bson:"_id,omitempty"`
	}
	err = cursor.All(ctx, &docs)
	require.NoError(t, err)

	for _, doc := range docs {
		ids = append(ids, doc.ID)
	}
	return ids
}

func readFile(t *testing.T, path string) (docs []bson.M) {
	t.Helper()

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	r, err := gzip.NewReader(f)
	require.NoError(t, err)
	defer r.Close()

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Bytes()
		var doc bson.M
		err = bson.UnmarshalExtJSON(line, true, &doc)
		require.NoError(t, err)
		docs = append(docs, doc)
	}

	return docs
}

func objectIDFromHex(t *testing.T, hex string) primitive.ObjectID {
	t.Helper()
	id, err := primitive.ObjectIDFromHex(hex)
	require.NoError(t, err)
	return id
}
