-- HLS-ENGINE consolidated database schema
-- This file runs automatically when PostgreSQL initializes for the first time.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "citext";

-- ----------------------------------------------------------------------------
-- Auth service tables
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    username VARCHAR(255),
    password_hash TEXT NOT NULL,
    role VARCHAR(50) NOT NULL DEFAULT 'user',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS refresh_tokens (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS api_keys (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    key_hash TEXT NOT NULL,
    name VARCHAR(255),
    scopes TEXT[],
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ----------------------------------------------------------------------------
-- Metadata service tables
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS videos (
    id UUID PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    description TEXT,
    status VARCHAR(50) NOT NULL DEFAULT 'uploading',
    duration DOUBLE PRECISION,
    tags TEXT[],
    metadata JSONB,
    -- Transcoder service columns
    source_url TEXT,
    width INTEGER,
    height INTEGER,
    video_codec VARCHAR(100),
    frame_rate DOUBLE PRECISION,
    bitrate INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_videos_status ON videos(status);
CREATE INDEX IF NOT EXISTS idx_videos_created_at ON videos(created_at DESC);

CREATE TABLE IF NOT EXISTS renditions (
    id UUID PRIMARY KEY,
    video_id UUID NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    name VARCHAR(255),
    is_original BOOLEAN NOT NULL DEFAULT FALSE,
    codec VARCHAR(100),
    bandwidth INTEGER NOT NULL,
    width INTEGER,
    height INTEGER,
    master_url TEXT,
    video_bitrate INTEGER,
    audio_bitrate INTEGER,
    profile VARCHAR(100),
    segment_path TEXT,
    playlist_path TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_renditions_video_id ON renditions(video_id);

CREATE TABLE IF NOT EXISTS audio_tracks (
    id UUID PRIMARY KEY,
    video_id UUID NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    language VARCHAR(10) NOT NULL,
    name VARCHAR(255),
    codec VARCHAR(100),
    "default" BOOLEAN NOT NULL DEFAULT FALSE,
    bitrate INTEGER,
    channels INTEGER,
    bandwidth INTEGER,
    delay_ms DOUBLE PRECISION NOT NULL DEFAULT 0,
    file_path TEXT,
    playlist_path TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audio_tracks_video_id ON audio_tracks(video_id);

CREATE TABLE IF NOT EXISTS subtitles (
    id UUID PRIMARY KEY,
    video_id UUID NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    language VARCHAR(10) NOT NULL,
    name VARCHAR(255),
    format VARCHAR(50),
    file_path TEXT,
    playlist_path TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_subtitles_video_id ON subtitles(video_id);

-- ----------------------------------------------------------------------------
-- Transcoder service tables
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY,
    video_id UUID NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    input_path TEXT,
    output_prefix TEXT,
    error_message TEXT,
    progress FLOAT DEFAULT 0.0,
    dispatch_count INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_video_id ON jobs(video_id);

-- ----------------------------------------------------------------------------
-- Analytics service tables
-- ----------------------------------------------------------------------------
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


-- ----------------------------------------------------------------------------
-- Multi-GPU / per-title optimization tables (additive, idempotent)
-- ----------------------------------------------------------------------------

-- Additive columns on videos (nullable; safe to re-run)
ALTER TABLE videos ADD COLUMN IF NOT EXISTS complexity_score FLOAT;
ALTER TABLE videos ADD COLUMN IF NOT EXISTS encoding_strategy VARCHAR(50);
ALTER TABLE videos ADD COLUMN IF NOT EXISTS source_hash VARCHAR(64);

CREATE TABLE IF NOT EXISTS gpu_workers (
    id UUID PRIMARY KEY,
    worker_id VARCHAR NOT NULL UNIQUE,
    hostname VARCHAR NOT NULL,
    gpu_index INTEGER NOT NULL,
    gpu_uuid VARCHAR,
    capacity INTEGER NOT NULL,
    status VARCHAR NOT NULL DEFAULT 'active',
    last_heartbeat TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gpu_workers_worker_id ON gpu_workers(worker_id);

CREATE TABLE IF NOT EXISTS gpu_assignments (
    id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    gpu_worker_id UUID NOT NULL REFERENCES gpu_workers(id) ON DELETE CASCADE,
    rendition_group VARCHAR NOT NULL,
    status VARCHAR NOT NULL DEFAULT 'assigned',
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_gpu_assignments_job_id ON gpu_assignments(job_id);

CREATE TABLE IF NOT EXISTS encoding_metrics (
    id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    rendition VARCHAR NOT NULL,
    codec VARCHAR NOT NULL,
    gpu_index INTEGER,
    gpu_worker_id VARCHAR,
    fps FLOAT NOT NULL,
    encode_duration_sec FLOAT NOT NULL,
    input_bytes BIGINT NOT NULL,
    output_bytes BIGINT NOT NULL,
    complexity_score FLOAT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_encoding_metrics_job_id ON encoding_metrics(job_id);

CREATE TABLE IF NOT EXISTS per_title_analysis (
    id UUID PRIMARY KEY,
    video_id UUID NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    complexity_score FLOAT NOT NULL,
    recommended_ladder JSONB NOT NULL,
    analyzed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_per_title_analysis_video_id ON per_title_analysis(video_id);
