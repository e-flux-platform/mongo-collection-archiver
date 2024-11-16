package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/joho/godotenv/autoload"
	"github.com/urfave/cli/v2"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/e-flux-platform/mongo-collection-archiver/internal/archive"
	"github.com/e-flux-platform/mongo-collection-archiver/internal/source"
	"github.com/e-flux-platform/mongo-collection-archiver/internal/storage"
)

type config struct {
	storageURL      string
	mongoURL        string
	mongoDatabase   string
	mongoCollection string
	delete          bool
	retention       time.Duration
	delay           time.Duration
}

func main() {
	var cfg config

	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "storage-url",
				EnvVars:     []string{"STORAGE_URL"},
				Required:    true,
				Destination: &cfg.storageURL,
			},
			&cli.StringFlag{
				Name:        "mongo-url",
				EnvVars:     []string{"MONGO_URL"},
				Required:    true,
				Destination: &cfg.mongoURL,
			},
			&cli.StringFlag{
				Name:        "mongo-database",
				EnvVars:     []string{"MONGO_DATABASE"},
				Required:    true,
				Destination: &cfg.mongoDatabase,
			},
			&cli.StringFlag{
				Name:        "mongo-collection",
				EnvVars:     []string{"MONGO_COLLECTION"},
				Required:    true,
				Destination: &cfg.mongoCollection,
			},
			&cli.BoolFlag{
				Name:        "delete",
				EnvVars:     []string{"DELETE"},
				Destination: &cfg.delete,
			},
			&cli.DurationFlag{
				Name:        "retention",
				EnvVars:     []string{"RETENTION"},
				Required:    true,
				Destination: &cfg.retention,
			},
			&cli.DurationFlag{
				Name:        "delay",
				EnvVars:     []string{"DELAY"},
				Destination: &cfg.delay,
				Value:       time.Second * 30,
			},
		},
		Action: func(cCtx *cli.Context) error {
			ctx, cancel := signal.NotifyContext(cCtx.Context, syscall.SIGTERM, syscall.SIGINT)
			defer cancel()
			return run(ctx, cfg)
		},
	}

	if err := app.RunContext(context.Background(), os.Args); err != nil {
		slog.Error("exiting", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config) error {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.mongoURL))
	if err != nil {
		return fmt.Errorf("unable to connect to mongo: %w", err)
	}

	collection := client.Database(cfg.mongoDatabase).Collection(cfg.mongoCollection)
	docSource := source.NewMongoDB(collection)

	store, err := storage.FromURL(ctx, cfg.storageURL)
	if err != nil {
		return fmt.Errorf("unable to connect to storage: %w", err)
	}
	defer store.Close()

	targetDate := time.Now().UTC().Add(cfg.retention * -1)

	slog.Info(
		"running",
		slog.String("mongoURL", cfg.mongoURL),
		slog.String("database", cfg.mongoDatabase),
		slog.String("collection", cfg.mongoCollection),
		slog.String("storageURL", cfg.storageURL),
		slog.Duration("retention", cfg.retention),
		slog.Duration("delay", cfg.delay),
		slog.String("targetDate", targetDate.String()),
	)

	archiver := archive.NewArchiver(docSource, store, !cfg.delete, cfg.delay)
	return archiver.Run(ctx, targetDate)
}
