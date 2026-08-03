package repository

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"hls-engine/internal/videolease"
)

func TestVideoDeletionSagaIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect test database: %v", err)
	}
	defer pool.Close()
	repo := New(pool)

	t.Run("tombstone fence and fresh final transaction", func(t *testing.T) {
		videoID := uuid.NewString()
		insertTestVideo(t, pool, videoID, "ready")
		t.Cleanup(func() {
			_, _ = pool.Exec(context.Background(), `DELETE FROM videos WHERE id = $1`, videoID)
		})

		deletion, err := repo.BeginVideoDeletion(ctx, videoID)
		if err != nil {
			t.Fatalf("BeginVideoDeletion: %v", err)
		}
		defer deletion.Release(context.Background())

		var status string
		if err := pool.QueryRow(
			ctx,
			`SELECT status FROM videos WHERE id = $1`,
			videoID,
		).Scan(&status); err != nil {
			t.Fatalf("read tombstone: %v", err)
		}
		if status != "deleting" {
			t.Fatalf("status = %q, want deleting", status)
		}

		if _, err := repo.BeginVideoDeletion(
			ctx,
			videoID,
		); !errors.Is(err, ErrVideoDeletionInProgress) {
			t.Fatalf("concurrent BeginVideoDeletion = %v", err)
		}
		if err := deletion.Finalize(ctx); err != nil {
			t.Fatalf("Finalize: %v", err)
		}
		if _, err := repo.GetVideo(ctx, videoID); !errors.Is(err, ErrNotFound) {
			t.Fatalf("GetVideo after finalize = %v", err)
		}
	})

	t.Run("active work blocks tombstone", func(t *testing.T) {
		videoID := uuid.NewString()
		jobID := uuid.NewString()
		insertTestVideo(t, pool, videoID, "processing")
		if _, err := pool.Exec(ctx, `
			INSERT INTO jobs (id, video_id, status)
			VALUES ($1, $2, 'transcoding')
		`, jobID, videoID); err != nil {
			t.Fatalf("insert active job: %v", err)
		}
		t.Cleanup(func() {
			_, _ = pool.Exec(context.Background(), `DELETE FROM videos WHERE id = $1`, videoID)
		})

		if _, err := repo.BeginVideoDeletion(
			ctx,
			videoID,
		); !errors.Is(err, ErrVideoActive) {
			t.Fatalf("BeginVideoDeletion = %v, want ErrVideoActive", err)
		}
		var status string
		if err := pool.QueryRow(
			ctx,
			`SELECT status FROM videos WHERE id = $1`,
			videoID,
		).Scan(&status); err != nil {
			t.Fatalf("read active video status: %v", err)
		}
		if status != "processing" {
			t.Fatalf("status = %q, want processing", status)
		}
	})

	t.Run("parallel upload leases exclude deletion", func(t *testing.T) {
		videoID := uuid.NewString()
		insertTestVideo(t, pool, videoID, "uploading")
		t.Cleanup(func() {
			_, _ = pool.Exec(context.Background(), `DELETE FROM videos WHERE id = $1`, videoID)
		})

		first, err := videolease.TryAcquireShared(ctx, pool, videoID)
		if err != nil {
			t.Fatalf("first shared lease: %v", err)
		}
		defer first.Release(context.Background())
		second, err := videolease.TryAcquireShared(ctx, pool, videoID)
		if err != nil {
			t.Fatalf("second shared lease: %v", err)
		}
		defer second.Release(context.Background())

		if _, err := repo.BeginVideoDeletion(
			ctx,
			videoID,
		); !errors.Is(err, ErrVideoDeletionInProgress) {
			t.Fatalf("deletion during shared uploads = %v", err)
		}
		if err := first.Release(ctx); err != nil {
			t.Fatalf("release first upload lease: %v", err)
		}
		if err := second.Release(ctx); err != nil {
			t.Fatalf("release second upload lease: %v", err)
		}

		deletion, err := repo.BeginVideoDeletion(ctx, videoID)
		if err != nil {
			t.Fatalf("deletion after uploads: %v", err)
		}
		defer deletion.Release(context.Background())
		if err := deletion.Finalize(ctx); err != nil {
			t.Fatalf("finalize after uploads: %v", err)
		}
	})
}

func insertTestVideo(
	t *testing.T,
	pool *pgxpool.Pool,
	videoID, status string,
) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO videos (id, title, status)
		VALUES ($1, 'deletion integration test', $2)
	`, videoID, status); err != nil {
		t.Fatalf("insert test video: %v", err)
	}
}
