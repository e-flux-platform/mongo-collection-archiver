package source

import (
	"context"
	"errors"
	"iter"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoDB is a mongodb source of documents
type MongoDB struct {
	collection *mongo.Collection
}

// NewMongoDB initializes and returns a MongoDB instance
func NewMongoDB(collection *mongo.Collection) *MongoDB {
	return &MongoDB{
		collection: collection,
	}
}

// FindAllFromDate resolves all documents with a createdAt on the supplied date
func (a *MongoDB) FindAllFromDate(ctx context.Context, date time.Time) StreamingResult {
	t := date.Truncate(time.Hour * 24)

	cursor, err := a.collection.Find(
		ctx,
		bson.M{
			"createdAt": bson.M{
				"$gte": t,
				"$lt":  t.AddDate(0, 0, 1),
			},
		},
	)
	return &mongoStreamingResult{
		cursor: cursor,
		err:    err,
	}
}

// EarliestCreatedAt returns the earliest createdAt time in the underlying collection
func (a *MongoDB) EarliestCreatedAt(ctx context.Context) (time.Time, error) {
	res := a.collection.FindOne(
		ctx,
		bson.M{
			"createdAt": bson.M{
				"$exists": true,
			},
		},
		options.FindOne().
			SetSort(bson.M{"createdAt": 1}).
			SetProjection(bson.M{"createdAt": 1}),
	)
	var projection struct {
		CreatedAt time.Time `bson:"createdAt"`
	}
	if err := res.Decode(&projection); err != nil {
		return time.Time{}, err
	}
	return projection.CreatedAt, nil
}

// DeleteAllFromDate removes all documents with a createdAt on the supplied date
func (a *MongoDB) DeleteAllFromDate(ctx context.Context, date time.Time) (int, error) {
	t := date.Truncate(time.Hour * 24)

	res, err := a.collection.DeleteMany(
		ctx,
		bson.M{
			"createdAt": bson.M{
				"$gte": t,
				"$lt":  t.AddDate(0, 0, 1),
			},
		},
	)
	if err != nil {
		return 0, err
	}

	return int(res.DeletedCount), nil
}

type StreamingResult interface {
	Iter(ctx context.Context) iter.Seq[[]byte]
	Err() error
}

type mongoStreamingResult struct {
	err    error
	cursor *mongo.Cursor
}

func (sr *mongoStreamingResult) Iter(ctx context.Context) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		if sr.err != nil {
			return
		}

		defer func() {
			if err := sr.cursor.Err(); err != nil {
				sr.err = errors.Join(sr.err, err)
			}
			if err := sr.cursor.Close(ctx); err != nil {
				sr.err = errors.Join(sr.err, err)
			}
		}()

		for sr.cursor.Next(ctx) {
			var raw bson.Raw
			if err := sr.cursor.Decode(&raw); err != nil {
				sr.err = err
				return
			}

			doc, err := bson.MarshalExtJSON(raw, true, false)
			if err != nil {
				sr.err = err
				return
			}

			if !yield(doc) {
				return
			}
		}
	}
}

func (sr *mongoStreamingResult) Err() error {
	return sr.err
}
