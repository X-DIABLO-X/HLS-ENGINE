// Package videolease serializes upload commits and destructive video cleanup.
//
// The lock is session-scoped in PostgreSQL so it remains held while a caller
// performs object-store work without keeping a database transaction open.
package videolease

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrBusy          = errors.New("video mutation is already in progress")
	ErrVideoNotFound = errors.New("video not found")
)

const advisoryLockNamespace = "hls-engine:video-mutation:"

// Lease owns a PostgreSQL session advisory lock. It must be released.
type Lease struct {
	conn      *pgxpool.Conn
	videoID   string
	shared    bool
	releaseMu sync.Mutex
	released  bool
}

// TryAcquire obtains a cross-service, per-video mutation fence without
// waiting. A caller can therefore return 409/Retry-After instead of tying up
// an HTTP connection behind a long upload or deletion.
func TryAcquire(
	ctx context.Context,
	pool *pgxpool.Pool,
	videoID string,
) (*Lease, error) {
	return tryAcquire(ctx, pool, videoID, false)
}

// TryAcquireShared obtains a shared upload lease. Multiple parts for one
// video can stream in parallel, while the exclusive deletion lease cannot be
// acquired until every part/create/completion operation has finished.
func TryAcquireShared(
	ctx context.Context,
	pool *pgxpool.Pool,
	videoID string,
) (*Lease, error) {
	return tryAcquire(ctx, pool, videoID, true)
}

func tryAcquire(
	ctx context.Context,
	pool *pgxpool.Pool,
	videoID string,
	shared bool,
) (*Lease, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire video mutation connection: %w", err)
	}

	lockQuery := `SELECT pg_try_advisory_lock(hashtextextended($1, 0))`
	if shared {
		lockQuery = `SELECT pg_try_advisory_lock_shared(hashtextextended($1, 0))`
	}
	var locked bool
	err = conn.QueryRow(
		ctx,
		lockQuery,
		advisoryLockName(videoID),
	).Scan(&locked)
	if err != nil {
		conn.Release()
		return nil, fmt.Errorf("acquire video mutation lock: %w", err)
	}
	if !locked {
		conn.Release()
		return nil, ErrBusy
	}
	return &Lease{conn: conn, videoID: videoID, shared: shared}, nil
}

// Begin starts a new transaction on the connection that owns the advisory
// lock. Callers can safely use separate short transactions around external
// work without releasing the cross-service fence.
func (l *Lease) Begin(ctx context.Context) (pgx.Tx, error) {
	l.releaseMu.Lock()
	defer l.releaseMu.Unlock()
	if l.released || l.conn == nil {
		return nil, errors.New("video mutation lease is released")
	}
	return l.conn.Begin(ctx)
}

// VideoStatus reads the current durable status while the mutation fence is
// held.
func (l *Lease) VideoStatus(ctx context.Context) (string, error) {
	l.releaseMu.Lock()
	defer l.releaseMu.Unlock()
	if l.released || l.conn == nil {
		return "", errors.New("video mutation lease is released")
	}

	var status string
	if err := l.conn.QueryRow(
		ctx,
		`SELECT status FROM videos WHERE id = $1`,
		l.videoID,
	).Scan(&status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrVideoNotFound
		}
		return "", fmt.Errorf("read video status: %w", err)
	}
	return status, nil
}

// Release unlocks the advisory lock on the same database session. If the
// unlock fails, the connection is closed rather than returned to the pool
// while it may still own a session lock.
func (l *Lease) Release(ctx context.Context) error {
	l.releaseMu.Lock()
	defer l.releaseMu.Unlock()
	if l.released {
		return nil
	}
	l.released = true
	if l.conn == nil {
		return nil
	}

	unlockQuery := `SELECT pg_advisory_unlock(hashtextextended($1, 0))`
	if l.shared {
		unlockQuery = `SELECT pg_advisory_unlock_shared(hashtextextended($1, 0))`
	}
	var unlocked bool
	err := l.conn.QueryRow(
		ctx,
		unlockQuery,
		advisoryLockName(l.videoID),
	).Scan(&unlocked)
	if err != nil || !unlocked {
		raw := l.conn.Hijack()
		l.conn = nil
		closeErr := raw.Close(context.Background())
		if err != nil {
			return fmt.Errorf("release video mutation lock: %w", err)
		}
		if closeErr != nil {
			return fmt.Errorf(
				"video mutation lock was not held; close connection: %w",
				closeErr,
			)
		}
		return errors.New("video mutation lock was not held")
	}

	l.conn.Release()
	l.conn = nil
	return nil
}

func advisoryLockName(videoID string) string {
	return advisoryLockNamespace + videoID
}
