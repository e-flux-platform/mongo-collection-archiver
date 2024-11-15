package testutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func StartMongoDB(ctx context.Context, t *testing.T) *mongo.Client {
	t.Helper()

	container, err := mongodb.Run(ctx, "mongo:6")
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = container.Terminate(context.Background())
	})

	url, err := container.ConnectionString(ctx)
	require.NoError(t, err)

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(url))
	require.NoError(t, err)

	return client
}
