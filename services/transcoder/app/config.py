"""Pydantic settings loaded from environment variables."""

from functools import lru_cache
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    APP_NAME: str = "hls-transcoder"
    DEBUG: bool = False

    # Database
    DATABASE_URL: str = "postgresql+psycopg2://postgres:postgres@localhost:5432/hls"

    # Celery / RabbitMQ / Redis
    CELERY_BROKER_URL: str = "amqp://guest:guest@localhost:5672//"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/0"
    REDIS_URL: str = "redis://localhost:6379/0"
    CELERY_TASK_SOFT_TIME_LIMIT_SEC: int = Field(default=6900, ge=60)
    CELERY_TASK_TIME_LIMIT_SEC: int = Field(default=7200, ge=120)
    CELERY_ACK_TIMEOUT_MARGIN_SEC: int = Field(default=900, ge=60)
    RABBITMQ_CONSUMER_TIMEOUT_MS: int = Field(default=10_800_000, ge=60_000)

    # MinIO
    MINIO_ENDPOINT: str = "localhost:9000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    MINIO_BUCKET: str = "hls-output"
    MINIO_RAW_BUCKET: str = "uploads-raw"
    MINIO_HLS_BUCKET: str = "hls-output"
    MINIO_THUMBNAILS_BUCKET: str = "thumbnails"
    MINIO_SECURE: bool = False
    MINIO_REGION: str = ""

    # RabbitMQ events
    RABBITMQ_URL: str = "amqp://guest:guest@localhost:5672//"
    RABBITMQ_EXCHANGE: str = "hls.events"

    # Pipeline
    WORK_DIR: str = "/tmp/hls-work"
    SEGMENT_DURATION: int = 6
    DEFAULT_RENDITIONS: str = "360,480,720,1080"
    GPU_ENABLED: bool = True

    # Multi-GPU scheduling & optimization (contract section C)
    GPU_INDEX: int = 0
    NVIDIA_VISIBLE_DEVICES: str = "all"
    GPU_REGISTRY_ENABLED: bool = True
    NVENC_MAX_SESSIONS: int = 3
    GPU_WORKER_ID: str = ""  # auto from hostname if empty
    HLS_SEGMENT_FORMAT: str = "fmp4"  # fmp4|ts
    VIDEO_CODEC: str = "h264"  # h264|hevc|av1
    # Single-pass is the audited safe default. Chunking stays available as an
    # explicit opt-in until retry fencing prevents stale chunks from overwriting
    # a newer attempt's output.
    CHUNKED_ENCODING: bool = False
    # When explicitly enabled, long-media tasks are split into at most five
    # minutes of source media. The pipeline may raise the 60-second base toward
    # that ceiling, but no chunk can regress into a feature-length ack.
    CHUNK_DURATION_SEC: int = Field(default=60, ge=30, le=300)
    CHUNK_MIN_DURATION_SEC: int = Field(default=600, ge=60)
    # CPU fallback is always split into one-rendition tasks with no more than
    # this much source media per delivery.  It is deliberately independent
    # from CHUNKED_ENCODING: a missing/lost GPU must never turn a feature film
    # into one late-acknowledged libx264 task.
    CPU_FALLBACK_CHUNK_DURATION_SEC: int = Field(default=120, ge=30, le=300)
    CPU_VIDEO_PRESET: Literal[
        "ultrafast",
        "superfast",
        "veryfast",
        "faster",
        "fast",
        "medium",
        "slow",
        "slower",
        "veryslow",
    ] = "veryfast"
    # The CPU worker runs several processes concurrently.  Never let each
    # fallback FFmpeg auto-claim every host core with ``-threads 0``.
    CPU_FALLBACK_THREADS_PER_TASK: int = Field(default=2, ge=1, le=16)
    # FFmpeg gets its own wall-clock deadline inside the Celery deadline.  The
    # remaining margin covers validation, durable DB writes, task replacement,
    # and orderly process-tree teardown.
    GPU_FFMPEG_TIMEOUT_SEC: int = Field(default=5400, ge=60)
    CPU_FALLBACK_FFMPEG_TIMEOUT_SEC: int = Field(default=5400, ge=60)
    FFMPEG_TASK_RUNTIME_MARGIN_SEC: int = Field(default=900, ge=60)
    PER_TITLE_ENCODING: bool = True
    LOUDNORM: bool = True
    TRICKPLAY: bool = True
    TRANSCODER_METRICS_ENABLED: bool = True

    # FFmpeg stall detection: kill a hung ffmpeg process if it produces no
    # progress output for this many seconds. Prevents a single NVENC/CUDA
    # deadlock from blocking a Celery worker forever (stuck at 99%).
    FFMPEG_STALL_TIMEOUT_SEC: float = 300.0

    # Stuck-job watchdog: scan for "processing" videos with no progress update
    # for this many seconds and auto-re-dispatch their pipeline. Catches chord
    # deadlocks where ffmpeg was killed but the callback never fired.
    STUCK_JOB_TIMEOUT_SEC: float = 900.0      # 15 min
    STUCK_JOB_MAX_RETRIES: int = 2
    WATCHDOG_INTERVAL_SEC: float = 300.0      # scan every 5 min

    # Failed/superseded workspace reclamation. Cleanup never races an active
    # mutator: it takes the job-exclusive filesystem lock, re-reads generation
    # state, observes this grace period, and retries from a durable Celery
    # delivery. The periodic scan recovers intents lost across any restart.
    WORKSPACE_REAPER_ENABLED: bool = True
    WORKSPACE_CLEANUP_GRACE_SEC: float = Field(default=60.0, ge=0.0)
    WORKSPACE_CLEANUP_RETRY_SEC: float = Field(default=30.0, ge=1.0)
    WORKSPACE_CLEANUP_MAX_RETRIES: int = Field(default=360, ge=0)
    WORKSPACE_REAPER_INTERVAL_SEC: float = Field(default=60.0, ge=10.0)
    WORKSPACE_REAPER_SCAN_LIMIT: int = Field(default=1000, ge=1, le=10_000)

    @model_validator(mode="after")
    def validate_task_time_limits(self):
        if self.CELERY_TASK_SOFT_TIME_LIMIT_SEC >= self.CELERY_TASK_TIME_LIMIT_SEC:
            raise ValueError(
                "CELERY_TASK_SOFT_TIME_LIMIT_SEC must be less than "
                "CELERY_TASK_TIME_LIMIT_SEC"
            )
        minimum_ack_timeout_ms = (
            self.CELERY_TASK_TIME_LIMIT_SEC + self.CELERY_ACK_TIMEOUT_MARGIN_SEC
        ) * 1000
        if self.RABBITMQ_CONSUMER_TIMEOUT_MS <= minimum_ack_timeout_ms:
            raise ValueError(
                "RABBITMQ_CONSUMER_TIMEOUT_MS must exceed the Celery hard task "
                "limit plus CELERY_ACK_TIMEOUT_MARGIN_SEC"
            )
        for name, timeout in (
            ("GPU_FFMPEG_TIMEOUT_SEC", self.GPU_FFMPEG_TIMEOUT_SEC),
            (
                "CPU_FALLBACK_FFMPEG_TIMEOUT_SEC",
                self.CPU_FALLBACK_FFMPEG_TIMEOUT_SEC,
            ),
        ):
            if (
                timeout + self.FFMPEG_TASK_RUNTIME_MARGIN_SEC
                >= self.CELERY_TASK_SOFT_TIME_LIMIT_SEC
            ):
                raise ValueError(
                    f"{name} plus FFMPEG_TASK_RUNTIME_MARGIN_SEC must be less "
                    "than CELERY_TASK_SOFT_TIME_LIMIT_SEC"
                )
        cleanup_retry_window = (
            self.WORKSPACE_CLEANUP_RETRY_SEC
            * self.WORKSPACE_CLEANUP_MAX_RETRIES
        )
        minimum_cleanup_window = (
            self.CELERY_TASK_TIME_LIMIT_SEC
            + self.WORKSPACE_CLEANUP_GRACE_SEC
        )
        if (
            self.WORKSPACE_REAPER_ENABLED
            and cleanup_retry_window < minimum_cleanup_window
        ):
            raise ValueError(
                "workspace cleanup retries must cover the Celery hard task "
                "limit plus WORKSPACE_CLEANUP_GRACE_SEC"
            )
        return self

    class Config:
        env_file = ".env"
        extra = "ignore"


@lru_cache
def get_settings() -> Settings:
    return Settings()
