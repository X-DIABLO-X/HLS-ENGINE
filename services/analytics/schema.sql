-- Analytics service schema for HLS-ENGINE.
-- Apply this file to the PostgreSQL database used by services/analytics.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Raw playback events ingested from the frontend player.
CREATE TABLE IF NOT EXISTS analytics_events (
    id                   BIGSERIAL PRIMARY KEY,
    event_id             UUID NOT NULL UNIQUE,
    received_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    event_time           TIMESTAMPTZ NOT NULL,
    video_id             TEXT NOT NULL,
    user_session_id      TEXT NOT NULL,
    event_type           TEXT NOT NULL,
    quality              TEXT,
    buffer_duration_ms   BIGINT DEFAULT 0,
    error_code           TEXT,
    error_message        TEXT,
    position_ms          BIGINT,
    duration_ms          BIGINT,
    payload              JSONB,
    trace_id             TEXT,
    span_id              TEXT
);

CREATE INDEX IF NOT EXISTS idx_analytics_events_video_id ON analytics_events(video_id);
CREATE INDEX IF NOT EXISTS idx_analytics_events_event_type ON analytics_events(event_type);
CREATE INDEX IF NOT EXISTS idx_analytics_events_received_at ON analytics_events(received_at);
CREATE INDEX IF NOT EXISTS idx_analytics_events_event_time ON analytics_events(event_time);
CREATE INDEX IF NOT EXISTS idx_analytics_events_video_time ON analytics_events(video_id, event_time);

-- Hourly materialised rollups.
CREATE TABLE IF NOT EXISTS analytics_hourly (
    video_id                 TEXT NOT NULL,
    bucket                   TIMESTAMPTZ NOT NULL,
    event_type               TEXT NOT NULL,
    quality                  TEXT NOT NULL DEFAULT '',
    count                    BIGINT NOT NULL DEFAULT 0,
    total_buffer_duration_ms BIGINT NOT NULL DEFAULT 0,
    unique_sessions          BIGINT NOT NULL DEFAULT 0,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (video_id, bucket, event_type, quality)
);

-- Daily materialised rollups.
CREATE TABLE IF NOT EXISTS analytics_daily (
    video_id                 TEXT NOT NULL,
    bucket                   TIMESTAMPTZ NOT NULL,
    event_type               TEXT NOT NULL,
    quality                  TEXT NOT NULL DEFAULT '',
    count                    BIGINT NOT NULL DEFAULT 0,
    total_buffer_duration_ms BIGINT NOT NULL DEFAULT 0,
    unique_sessions          BIGINT NOT NULL DEFAULT 0,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (video_id, bucket, event_type, quality)
);

CREATE INDEX IF NOT EXISTS idx_analytics_hourly_bucket ON analytics_hourly(bucket);
CREATE INDEX IF NOT EXISTS idx_analytics_daily_bucket ON analytics_daily(bucket);
