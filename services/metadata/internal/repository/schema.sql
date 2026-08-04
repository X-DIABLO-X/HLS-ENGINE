-- Metadata service schema
CREATE TABLE IF NOT EXISTS videos (
    id UUID PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    description TEXT,
    status VARCHAR(50) NOT NULL DEFAULT 'draft',
    duration DOUBLE PRECISION,
    tags TEXT[],
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_videos_status ON videos(status);
CREATE INDEX IF NOT EXISTS idx_videos_created_at ON videos(created_at DESC);

CREATE TABLE IF NOT EXISTS renditions (
    id UUID PRIMARY KEY,
    video_id UUID NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    is_original BOOLEAN NOT NULL DEFAULT FALSE,
    codec VARCHAR(100),
    bandwidth INTEGER NOT NULL,
    width INTEGER,
    height INTEGER,
    master_url TEXT,
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
    delay_ms DOUBLE PRECISION NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audio_tracks_video_id ON audio_tracks(video_id);

CREATE TABLE IF NOT EXISTS subtitles (
    id UUID PRIMARY KEY,
    video_id UUID NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    language VARCHAR(10) NOT NULL,
    name VARCHAR(255),
    format VARCHAR(50),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_subtitles_video_id ON subtitles(video_id);
