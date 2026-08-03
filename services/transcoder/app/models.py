"""ORM models for the transcoder service."""

import enum
import re
import unicodedata
import uuid
from datetime import datetime
from typing import Any, Dict, Iterable, List

from sqlalchemy import BigInteger, Boolean, Column, DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.db import Base


_LANGUAGE_TAG_RE = re.compile(r"^[A-Za-z0-9]{1,8}(?:-[A-Za-z0-9]{1,8})*$")
_PATH_TOKEN_RE = re.compile(r"[^a-z0-9]+")
_TRACK_ID_RE = re.compile(r"^[a-z0-9]+(?:_[a-z0-9]+)*$")

_LANGUAGE_DISPLAY_NAMES = {
    "ara": "Arabic",
    "ben": "Bengali",
    "deu": "German",
    "eng": "English",
    "fra": "French",
    "hin": "Hindi",
    "ind": "Indonesian",
    "ita": "Italian",
    "jpn": "Japanese",
    "kor": "Korean",
    "por": "Portuguese",
    "rus": "Russian",
    "spa": "Spanish",
    "tam": "Tamil",
    "tel": "Telugu",
    "tha": "Thai",
    "tur": "Turkish",
    "und": "Unknown language",
}


def normalize_track_language(value: Any) -> str:
    """Return a safe, compact BCP-47-ish language tag for storage and HLS.

    The database schema currently allows ten characters. Invalid container
    metadata must never be copied into either SQL or a quoted HLS attribute.
    """
    candidate = str(value or "und").strip().replace("_", "-")
    if not _LANGUAGE_TAG_RE.fullmatch(candidate):
        return "und"

    parts: List[str] = []
    for part in candidate.split("-"):
        proposed = "-".join(parts + [part])
        if len(proposed) > 10:
            break
        parts.append(part)
    return "-".join(parts).lower() or "und"


def _path_token(value: Any, fallback: str = "und") -> str:
    normalized = unicodedata.normalize("NFKD", str(value or ""))
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii").lower()
    token = _PATH_TOKEN_RE.sub("_", ascii_value).strip("_")[:64]
    return token or fallback


def safe_track_identity(value: Any, language: Any = "und", stream_index: Any = 0) -> str:
    """Validate a dispatched identity or derive a collision-resistant fallback.

    Normal pipeline dispatch always supplies ``track_id``. The stream-index
    suffix protects direct task calls and messages produced by older services.
    """
    supplied = str(value or "").strip().lower()
    if supplied and len(supplied) <= 64 and _TRACK_ID_RE.fullmatch(supplied):
        return supplied

    base = _path_token(normalize_track_language(language))
    try:
        index = max(0, int(stream_index))
    except (TypeError, ValueError):
        index = 0
    return base if index == 0 else f"{base}_stream_{index + 1}"


def hls_attribute(value: Any, fallback: str = "") -> str:
    """Sanitize a value embedded in an HLS quoted-string attribute."""
    text = " ".join(str(value or fallback).split())
    text = text.replace("\\", "/").replace('"', "'")
    return text[:255] or fallback


def suffixed_hls_name(value: Any, suffix: int) -> str:
    suffix_text = f" {suffix}"
    base = hls_attribute(value, "TRACK")
    return f"{base[:255 - len(suffix_text)]}{suffix_text}"


def track_display_name(info: Dict[str, Any], language: str, ordinal: int = 1) -> str:
    """Return a viewer-facing language name, never an uploader title.

    Container titles commonly contain release-group names such as
    ``MoviesMod.army``. They are not useful playback labels and should not be
    exposed in the player or the master playlist.
    """
    normalized = normalize_track_language(language)
    primary = normalized.split("-", 1)[0]
    base = _LANGUAGE_DISPLAY_NAMES.get(primary, primary.upper())
    return base if ordinal <= 1 else f"{base} {ordinal}"


def assign_track_identities(
    tracks: Iterable[Dict[str, Any]],
    stream_index_key: str,
) -> List[Dict[str, Any]]:
    """Copy track metadata and assign stable, path-safe IDs and unique names."""
    prepared: List[Dict[str, Any]] = []
    language_counts: Dict[str, int] = {}
    used_ids = set()
    used_names = set()

    for original in tracks or []:
        info = dict(original or {})
        language = normalize_track_language(info.get("language"))
        ordinal = language_counts.get(language, 0) + 1
        language_counts[language] = ordinal

        base = _path_token(language)
        candidate = base if ordinal == 1 else f"{base}_{ordinal}"
        suffix = ordinal
        while candidate in used_ids:
            suffix += 1
            candidate = f"{base}_{suffix}"
        used_ids.add(candidate)

        requested_name = track_display_name(info, language, ordinal)
        name = requested_name
        name_suffix = 1
        while name.casefold() in used_names:
            name_suffix += 1
            name = suffixed_hls_name(requested_name, name_suffix)
        used_names.add(name.casefold())

        info["language"] = language
        info["track_id"] = candidate
        info["name"] = name
        try:
            info[stream_index_key] = max(0, int(info.get(stream_index_key, 0)))
        except (TypeError, ValueError):
            info[stream_index_key] = 0
        prepared.append(info)

    return prepared


def deterministic_track_row_id(video_id: Any, kind: str, track_id: str) -> str:
    """Stable row ID makes redelivery converge even under a commit race."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"hls-engine:{video_id}:{kind}:{track_id}"))


def track_identity_from_path(track: Any, kind: str) -> str:
    """Recover the dispatched identity from a persisted output/raw path."""
    prefix = f"{kind}_"
    for path in (getattr(track, "playlist_path", None), getattr(track, "file_path", None)):
        if not path:
            continue
        parent = str(path).replace("\\", "/").rstrip("/").rsplit("/", 2)
        if len(parent) < 2:
            continue
        directory = parent[-2].lower()
        if directory.startswith(prefix):
            candidate = directory[len(prefix):]
        elif (
            len(parent) == 3
            and parent[0].replace("\\", "/").rstrip("/").rsplit("/", 1)[-1].lower()
            == kind
        ):
            candidate = directory
        else:
            continue
        if candidate and _TRACK_ID_RE.fullmatch(candidate):
            return candidate
    return safe_track_identity(None, getattr(track, "language", "und"), 0)


class JobStatus(str, enum.Enum):
    pending = "pending"
    queued = "queued"
    probing = "probing"
    extracting = "extracting"
    transcoding = "transcoding"
    packaging = "packaging"
    publishing = "publishing"
    completed = "completed"
    failed = "failed"


class Video(Base):
    __tablename__ = "videos"

    id = Column(UUID(as_uuid=False), primary_key=True)
    source_url = Column(String, nullable=False)
    title = Column(String)
    status = Column(String, default="pending")

    duration = Column(Float)
    width = Column(Integer)
    height = Column(Integer)
    video_codec = Column(String)
    frame_rate = Column(Float)
    bitrate = Column(Integer)
    complexity_score = Column(Float)
    encoding_strategy = Column(String(50))
    source_hash = Column(String(64))

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    renditions = relationship(
        "Rendition", back_populates="video", cascade="all, delete-orphan", lazy="dynamic"
    )
    audio_tracks = relationship(
        "AudioTrack", back_populates="video", cascade="all, delete-orphan", lazy="dynamic"
    )
    subtitles = relationship(
        "Subtitle", back_populates="video", cascade="all, delete-orphan", lazy="dynamic"
    )
    jobs = relationship(
        "Job", back_populates="video", cascade="all, delete-orphan", lazy="dynamic"
    )


class Rendition(Base):
    __tablename__ = "renditions"

    id = Column(UUID(as_uuid=False), primary_key=True)
    video_id = Column(UUID(as_uuid=False), ForeignKey("videos.id", ondelete="CASCADE"), nullable=False)

    name = Column(String)
    height = Column(Integer, nullable=False)
    width = Column(Integer)
    video_bitrate = Column(Integer)
    audio_bitrate = Column(Integer)
    codec = Column(String, default="h264")
    profile = Column(String, default="high")

    segment_path = Column(String)
    playlist_path = Column(String)
    bandwidth = Column(Integer)

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    video = relationship("Video", back_populates="renditions")


class AudioTrack(Base):
    __tablename__ = "audio_tracks"

    id = Column(UUID(as_uuid=False), primary_key=True)
    video_id = Column(UUID(as_uuid=False), ForeignKey("videos.id", ondelete="CASCADE"), nullable=False)

    language = Column(String, nullable=False, default="und")
    name = Column(String)
    codec = Column(String, default="aac")
    default = Column("default", Boolean, nullable=False, default=False)
    bitrate = Column(Integer)
    channels = Column(Integer)
    bandwidth = Column(Integer)
    delay_ms = Column(Float, default=0.0)

    file_path = Column(String)
    playlist_path = Column(String)

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    video = relationship("Video", back_populates="audio_tracks")


class Subtitle(Base):
    __tablename__ = "subtitles"

    id = Column(UUID(as_uuid=False), primary_key=True)
    video_id = Column(UUID(as_uuid=False), ForeignKey("videos.id", ondelete="CASCADE"), nullable=False)

    language = Column(String, nullable=False, default="und")
    name = Column(String)
    format = Column(String, default="webvtt")

    file_path = Column(String)
    playlist_path = Column(String)

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    video = relationship("Video", back_populates="subtitles")


class Job(Base):
    __tablename__ = "jobs"

    id = Column(UUID(as_uuid=False), primary_key=True)
    video_id = Column(UUID(as_uuid=False), ForeignKey("videos.id", ondelete="CASCADE"), nullable=False)

    status = Column(String, default=JobStatus.pending.value)
    input_path = Column(String)
    output_prefix = Column(String)
    error_message = Column(Text)
    progress = Column(Float, default=0.0)
    dispatch_count = Column(Integer, default=0)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    video = relationship("Video", back_populates="jobs")


class GpuWorker(Base):
    __tablename__ = "gpu_workers"

    id = Column(UUID(as_uuid=False), primary_key=True)
    worker_id = Column(String, unique=True, nullable=False)
    hostname = Column(String, nullable=False)
    gpu_index = Column(Integer, nullable=False)
    gpu_uuid = Column(String, nullable=True)
    capacity = Column(Integer, nullable=False)
    status = Column(String, nullable=False, default="active")
    last_heartbeat = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class GpuAssignment(Base):
    __tablename__ = "gpu_assignments"

    id = Column(UUID(as_uuid=False), primary_key=True)
    job_id = Column(UUID(as_uuid=False), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False)
    gpu_worker_id = Column(UUID(as_uuid=False), ForeignKey("gpu_workers.id", ondelete="CASCADE"), nullable=False)
    rendition_group = Column(String, nullable=False)
    status = Column(String, nullable=False, default="assigned")
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)


class EncodingMetric(Base):
    __tablename__ = "encoding_metrics"

    id = Column(UUID(as_uuid=False), primary_key=True)
    job_id = Column(UUID(as_uuid=False), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False)
    rendition = Column(String, nullable=False)
    codec = Column(String, nullable=False)
    gpu_index = Column(Integer, nullable=True)
    gpu_worker_id = Column(String, nullable=True)
    fps = Column(Float, nullable=False)
    encode_duration_sec = Column(Float, nullable=False)
    input_bytes = Column(BigInteger, nullable=False)
    output_bytes = Column(BigInteger, nullable=False)
    complexity_score = Column(Float, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class PerTitleAnalysis(Base):
    __tablename__ = "per_title_analysis"

    id = Column(UUID(as_uuid=False), primary_key=True)
    video_id = Column(UUID(as_uuid=False), ForeignKey("videos.id", ondelete="CASCADE"), nullable=False)
    complexity_score = Column(Float, nullable=False)
    recommended_ladder = Column(JSONB, nullable=False)
    analyzed_at = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
