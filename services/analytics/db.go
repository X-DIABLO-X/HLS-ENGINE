package main

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// Store handles persistence for raw events, rollups, and ephemeral counters.
type Store struct {
	db    *pgxpool.Pool
	redis *redis.Client
}

// NewStore opens PostgreSQL and Redis connections and verifies them.
func NewStore(ctx context.Context, cfg Config) (*Store, error) {
	pool, err := pgxpool.New(ctx, cfg.DBURL)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, err
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisURL,
		Password: cfg.RedisPassword,
		DB:       0,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	return &Store{db: pool, redis: rdb}, nil
}

// Close releases database and cache connections.
func (s *Store) Close() {
	if s.db != nil {
		s.db.Close()
	}
	if s.redis != nil {
		s.redis.Close()
	}
}

// SaveEvent writes a single analytics event to PostgreSQL. event_id is unique
// so duplicate submissions are silently ignored.
func (s *Store) SaveEvent(ctx context.Context, e Event) error {
	_, err := s.db.Exec(ctx, `
		INSERT INTO analytics_events (
			event_id, event_time, video_id, user_session_id, event_type,
			quality, buffer_duration_ms, error_code, error_message,
			position_ms, duration_ms, payload, trace_id, span_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		ON CONFLICT (event_id) DO NOTHING
	`, e.EventID, e.EventTime, e.VideoID, e.UserSessionID, e.EventType,
		e.Quality, e.BufferDurationMs, e.ErrorCode, e.ErrorMessage,
		e.PositionMs, e.DurationMs, e.Payload, e.TraceID, e.SpanID)
	return err
}

// RecordActiveSession marks a session as currently viewing a video.
func (s *Store) RecordActiveSession(ctx context.Context, videoID, sessionID string) error {
	key := "active_sessions:" + videoID
	pipe := s.redis.Pipeline()
	pipe.SAdd(ctx, key, sessionID)
	pipe.Expire(ctx, key, 5*time.Minute)
	_, err := pipe.Exec(ctx)
	return err
}

// RemoveActiveSession drops a session from the active viewers set.
func (s *Store) RemoveActiveSession(ctx context.Context, videoID, sessionID string) error {
	return s.redis.SRem(ctx, "active_sessions:"+videoID, sessionID).Err()
}

// CountConcurrentViewers sums the cardinality of all active session sets.
func (s *Store) CountConcurrentViewers(ctx context.Context) (int64, error) {
	keys, err := s.redis.Keys(ctx, "active_sessions:*").Result()
	if err != nil || len(keys) == 0 {
		return 0, err
	}
	counts, err := s.redis.SUnion(ctx, keys...).Result()
	if err != nil {
		return 0, err
	}
	return int64(len(counts)), nil
}

// VideoMetrics holds per-video aggregations.
type VideoMetrics struct {
	VideoID       string         `json:"video_id"`
	Plays         int64          `json:"plays"`
	UniqueViewers int64          `json:"unique_viewers"`
	WatchTimeMs   int64          `json:"watch_time_ms"`
	BufferEvents  int64          `json:"buffer_events"`
	TotalBufferMs int64          `json:"total_buffer_ms"`
	Errors        int64          `json:"errors"`
	BufferRatio   float64        `json:"buffer_ratio"`
	TopQualities  []QualityCount `json:"top_qualities"`
}

// QualityCount pairs a rendition with its selection count.
type QualityCount struct {
	Quality string `json:"quality"`
	Count   int64  `json:"count"`
}

// GetVideoMetrics aggregates raw events for a single video.
func (s *Store) GetVideoMetrics(ctx context.Context, videoID string, since time.Time) (*VideoMetrics, error) {
	row := s.db.QueryRow(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE event_type = 'play'),
			COUNT(*) FILTER (WHERE event_type = 'buffering'),
			COALESCE(SUM(buffer_duration_ms) FILTER (WHERE event_type = 'buffering'), 0),
			COUNT(*) FILTER (WHERE event_type = 'error'),
			COUNT(DISTINCT user_session_id),
			COALESCE(SUM(duration_ms) FILTER (WHERE event_type IN ('heartbeat', 'ended')), 0)
		FROM analytics_events
		WHERE video_id = $1 AND event_time >= $2
	`, videoID, since)

	m := &VideoMetrics{VideoID: videoID}
	if err := row.Scan(&m.Plays, &m.BufferEvents, &m.TotalBufferMs, &m.Errors, &m.UniqueViewers, &m.WatchTimeMs); err != nil {
		return nil, err
	}

	if m.Plays > 0 {
		m.BufferRatio = float64(m.BufferEvents) / float64(m.Plays)
	}

	rows, err := s.db.Query(ctx, `
		SELECT quality, COUNT(*) AS n
		FROM analytics_events
		WHERE video_id = $1 AND event_time >= $2 AND quality <> ''
		GROUP BY quality
		ORDER BY n DESC
		LIMIT 5
	`, videoID, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var q QualityCount
		if err := rows.Scan(&q.Quality, &q.Count); err != nil {
			return nil, err
		}
		m.TopQualities = append(m.TopQualities, q)
	}
	return m, rows.Err()
}

// DashboardMetrics holds platform-wide aggregations.
type DashboardMetrics struct {
	TotalPlays        int64 `json:"total_plays"`
	ActiveViewers     int64 `json:"active_viewers"`
	TotalErrors       int64 `json:"total_errors"`
	TotalBufferEvents int64 `json:"total_buffer_events"`
	VideosWithEvents  int64 `json:"videos_with_events"`
}

// GetDashboardMetrics returns global analytics for the requested window.
func (s *Store) GetDashboardMetrics(ctx context.Context, since time.Time) (*DashboardMetrics, error) {
	row := s.db.QueryRow(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE event_type = 'play'),
			COUNT(DISTINCT user_session_id),
			COUNT(*) FILTER (WHERE event_type = 'error'),
			COUNT(*) FILTER (WHERE event_type = 'buffering'),
			COUNT(DISTINCT video_id)
		FROM analytics_events
		WHERE event_time >= $1
	`, since)

	d := &DashboardMetrics{}
	if err := row.Scan(&d.TotalPlays, &d.ActiveViewers, &d.TotalErrors, &d.TotalBufferEvents, &d.VideosWithEvents); err != nil {
		return nil, err
	}

	// Prefer Redis for live concurrent viewers when available.
	if cv, err := s.CountConcurrentViewers(ctx); err == nil && cv > 0 {
		d.ActiveViewers = cv
	}
	return d, nil
}

// RollupHourly materialises hourly aggregates for events in the supplied window.
func (s *Store) RollupHourly(ctx context.Context, start, end time.Time) error {
	_, err := s.db.Exec(ctx, `
		INSERT INTO analytics_hourly (
			video_id, bucket, event_type, quality,
			count, total_buffer_duration_ms, unique_sessions
		)
		SELECT
			video_id,
			date_trunc('hour', event_time) AS bucket,
			event_type,
			COALESCE(quality, '') AS quality,
			COUNT(*) AS n,
			COALESCE(SUM(buffer_duration_ms), 0) AS buffer_ms,
			COUNT(DISTINCT user_session_id) AS sessions
		FROM analytics_events
		WHERE event_time >= $1 AND event_time < $2
		GROUP BY video_id, date_trunc('hour', event_time), event_type, COALESCE(quality, '')
		ON CONFLICT (video_id, bucket, event_type, quality) DO UPDATE SET
			count = EXCLUDED.count,
			total_buffer_duration_ms = EXCLUDED.total_buffer_duration_ms,
			unique_sessions = EXCLUDED.unique_sessions,
			updated_at = NOW()
	`, start, end)
	return err
}

// RollupDaily materialises daily aggregates for events in the supplied window.
func (s *Store) RollupDaily(ctx context.Context, start, end time.Time) error {
	_, err := s.db.Exec(ctx, `
		INSERT INTO analytics_daily (
			video_id, bucket, event_type, quality,
			count, total_buffer_duration_ms, unique_sessions
		)
		SELECT
			video_id,
			date_trunc('day', event_time) AS bucket,
			event_type,
			COALESCE(quality, '') AS quality,
			COUNT(*) AS n,
			COALESCE(SUM(buffer_duration_ms), 0) AS buffer_ms,
			COUNT(DISTINCT user_session_id) AS sessions
		FROM analytics_events
		WHERE event_time >= $1 AND event_time < $2
		GROUP BY video_id, date_trunc('day', event_time), event_type, COALESCE(quality, '')
		ON CONFLICT (video_id, bucket, event_type, quality) DO UPDATE SET
			count = EXCLUDED.count,
			total_buffer_duration_ms = EXCLUDED.total_buffer_duration_ms,
			unique_sessions = EXCLUDED.unique_sessions,
			updated_at = NOW()
	`, start, end)
	return err
}

// AcquireLease is a lightweight Redis lock used to keep a single aggregator
// instance running in a replicated deployment.
func (s *Store) AcquireLease(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	ok, err := s.redis.SetNX(ctx, key, "1", ttl).Result()
	return ok, err
}

// HealthCheck returns nil if both stores are reachable.
func (s *Store) HealthCheck(ctx context.Context) error {
	if err := s.db.Ping(ctx); err != nil {
		return err
	}
	return s.redis.Ping(ctx).Err()
}

// RowCount is a small helper so the readiness probe can verify the events table exists.
func (s *Store) RowCount(ctx context.Context) (int64, error) {
	var n int64
	err := s.db.QueryRow(ctx, "SELECT COUNT(*) FROM analytics_events").Scan(&n)
	return n, err
}

// pgxCopyFromEvents bulk inserts events using the fast COPY protocol. This is
// used by the batch ingest handler when throughput matters.
func (s *Store) pgxCopyFromEvents(ctx context.Context, events []Event) (int64, error) {
	rows := make([][]interface{}, len(events))
	for i, e := range events {
		rows[i] = []interface{}{
			e.EventID, e.EventTime, e.VideoID, e.UserSessionID, e.EventType,
			e.Quality, e.BufferDurationMs, e.ErrorCode, e.ErrorMessage,
			e.PositionMs, e.DurationMs, e.Payload, e.TraceID, e.SpanID,
		}
	}

	copyCount, err := s.db.CopyFrom(ctx, pgx.Identifier{"analytics_events"}, []string{
		"event_id", "event_time", "video_id", "user_session_id", "event_type",
		"quality", "buffer_duration_ms", "error_code", "error_message",
		"position_ms", "duration_ms", "payload", "trace_id", "span_id",
	}, pgx.CopyFromRows(rows))
	return copyCount, err
}
