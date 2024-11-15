package source_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/e-flux-platform/mongo-collection-archiver/internal/source"
	"github.com/e-flux-platform/mongo-collection-archiver/internal/testutil"
)

func TestMongoDB(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client := testutil.StartMongoDB(ctx, t)

	t.Run("FindAllFromDate", func(t *testing.T) {
		t.Parallel()

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

		collection := client.Database(uuid.NewString()).Collection("test")
		_, err := collection.InsertMany(ctx, []any{doc1, doc2, doc3, doc4})
		require.NoError(t, err)

		var docs []string
		res := source.NewMongoDB(collection).FindAllFromDate(ctx, date)
		for doc := range res.Iter(ctx) {
			docs = append(docs, string(doc))
		}
		require.NoError(t, res.Err())
		require.Len(t, docs, 2)

		jsonDoc2 := `{"_id":{"$oid":"5d6fd8ec10ca90000998cf31"},"createdAt":{"$date":{"$numberLong":"1730419200000"}}}`
		jsonDoc3 := `{"_id":{"$oid":"5d6fdf658a583b0009929c06"},"createdAt":{"$date":{"$numberLong":"1730430000000"}}}`
		assert.Equal(t, jsonDoc2, docs[0])
		assert.Equal(t, jsonDoc3, docs[1])
	})

	t.Run("EarliestCreatedAt", func(t *testing.T) {
		t.Parallel()

		expected := time.Date(2024, time.November, 1, 0, 0, 0, 0, time.UTC)

		doc1 := bson.M{
			"_id":       objectIDFromHex(t, "5d6fd699ee45770009e17140"),
			"createdAt": primitive.NewDateTimeFromTime(expected.Add(time.Second)),
		}
		doc2 := bson.M{
			"_id":       objectIDFromHex(t, "5d6fd8ec10ca90000998cf31"),
			"createdAt": primitive.NewDateTimeFromTime(expected),
		}

		collection := client.Database(uuid.NewString()).Collection("test")
		_, err := collection.InsertMany(ctx, []any{doc1, doc2})
		require.NoError(t, err)

		earliest, err := source.NewMongoDB(collection).EarliestCreatedAt(ctx)
		require.NoError(t, err)
		assert.Equal(t, expected, earliest)
	})

	t.Run("DeleteAllFromDate", func(t *testing.T) {
		t.Parallel()

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

		collection := client.Database(uuid.NewString()).Collection("test")
		_, err := collection.InsertMany(ctx, []any{doc1, doc2, doc3, doc4})
		require.NoError(t, err)

		err = source.NewMongoDB(collection).DeleteAllFromDate(ctx, date)
		require.NoError(t, err)

		cursor, err := collection.Find(ctx, bson.M{})
		require.NoError(t, err)

		var docs []struct {
			ID primitive.ObjectID `bson:"_id,omitempty"`
		}
		err = cursor.All(ctx, &docs)
		require.NoError(t, err)
		require.Len(t, docs, 2)

		assert.Equal(t, "5d6fd699ee45770009e17140", docs[0].ID.Hex()) // doc1
		assert.Equal(t, "5d6fdf85451f58001939950a", docs[1].ID.Hex()) // doc4
	})
}

func objectIDFromHex(t *testing.T, hex string) primitive.ObjectID {
	t.Helper()
	id, err := primitive.ObjectIDFromHex(hex)
	require.NoError(t, err)
	return id
}
