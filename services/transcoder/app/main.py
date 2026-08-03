"""FastAPI application for the HLS transcoder service."""

import csv
import logging
import os
import subprocess
import uuid
from contextlib import asynccontextmanager
from typing import List, Optional
from urllib.parse import quote, unquote, urlsplit

from fastapi import FastAPI, HTTPException, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from pydantic import BaseModel
from sqlalchemy import text

from app import gpu_registry
from app.config import get_settings
from app.db import SessionLocal, engine

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Import models so they register with Base.metadata before creating tables.
    from app import models  # noqa: F401
    from app.db import init_db

    init_db()
    yield


app = FastAPI(
    title="HLS Transcoder",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Prometheus metrics (contract section E). The Grafana dashboards and
# Prometheus alerts owned by the Monitoring agent depend on these exact names.
# `transcode_jobs_total{status}` is incremented here for "submitted"; the
# pipeline tasks increment started/completed/failed. The GPU/NVENC and
# queue-depth gauges are refreshed on each /metrics scrape.
# ---------------------------------------------------------------------------
transcode_jobs_total = Counter(
    "transcode_jobs_total", "Total transcoding jobs by status", ["status"]
)
transcode_fps = Gauge(
    "transcode_fps", "Encoding FPS by rendition/codec/gpu", ["rendition", "codec", "gpu"]
)
transcode_duration_seconds = Histogram(
    "transcode_duration_seconds", "Transcode duration in seconds"
)
nvenc_sessions_active = Gauge(
    "nvenc_sessions_active", "Active NVENC sessions per GPU", ["gpu"]
)
nvenc_sessions_max = Gauge(
    "nvenc_sessions_max", "Max NVENC sessions per GPU", ["gpu"]
)
gpu_utilization_percent = Gauge(
    "gpu_utilization_percent", "GPU utilization percent", ["gpu"]
)
gpu_memory_used_bytes = Gauge(
    "gpu_memory_used_bytes", "GPU memory used bytes", ["gpu"]
)
gpu_memory_total_bytes = Gauge(
    "gpu_memory_total_bytes", "GPU memory total bytes", ["gpu"]
)
transcode_queue_depth = Gauge(
    "transcode_queue_depth", "Celery queue depth by queue", ["queue"]
)
gpu_workers_active = Gauge(
    "gpu_workers_active", "Active GPU workers"
)
workspace_cleanup_events_total = Gauge(
    "workspace_cleanup_events_total",
    "Durable workspace cleanup outcomes recorded by workers",
    ["outcome", "reason"],
)
workspace_cleanup_backlog = Gauge(
    "workspace_cleanup_backlog",
    "Direct UUID job workspaces awaiting a terminal cleanup decision",
)


def _poll_gpu_metrics() -> None:
    """Refresh GPU/queue gauges from nvidia-smi + gpu_registry (best-effort)."""
    settings = get_settings()
    if not settings.TRANSCODER_METRICS_ENABLED:
        return

    # nvidia-smi GPU utilization + memory (skip silently if not installed).
    try:
        out = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=index,utilization.gpu,memory.used,memory.total",
                "--format=csv,noheader,nounits",
            ],
            capture_output=True,
            text=True,
            timeout=3,
        )
        if out.returncode == 0:
            for row in csv.reader(out.stdout.splitlines()):
                row = [c.strip() for c in row]
                if len(row) >= 4 and row[0]:
                    idx = row[0]
                    gpu_utilization_percent.labels(gpu=idx).set(
                        float(row[1]) if row[1] else 0.0
                    )
                    # nvidia-smi reports memory in MiB; expose bytes.
                    gpu_memory_used_bytes.labels(gpu=idx).set(
                        float(row[2]) * 1024 * 1024 if row[2] else 0.0
                    )
                    gpu_memory_total_bytes.labels(gpu=idx).set(
                        float(row[3]) * 1024 * 1024 if row[3] else 0.0
                    )
    except Exception:
        pass

    # gpu_registry: NVENC sessions + active worker count.
    try:
        active_workers = 0
        for g in gpu_registry.get_gpu_status():
            idx = str(g.get("index"))
            nvenc_sessions_active.labels(gpu=idx).set(float(g.get("in_use", 0)))
            nvenc_sessions_max.labels(gpu=idx).set(float(g.get("capacity", 0)))
            # Worker liveness is represented by its fresh registry entry, not
            # by whether it happens to hold an encoding lease at scrape time.
            if g.get("worker_id"):
                active_workers += 1
        gpu_workers_active.set(active_workers)
    except Exception:
        pass

    # Queue depth from RabbitMQ's read-only management API. Celery's legacy
    # inspect/pidbox channel creates transient non-exclusive reply queues,
    # which RabbitMQ 4.3 deliberately rejects. Queue telemetry must never
    # re-enable that deprecated broker feature.
    try:
        import httpx

        broker = urlsplit(settings.CELERY_BROKER_URL)
        if broker.scheme not in {"amqp", "amqps"} or not broker.hostname:
            raise ValueError("unsupported broker URL for queue metrics")
        vhost = unquote(broker.path.lstrip("/")) or "/"
        management_port = int(
            os.environ.get("RABBITMQ_MANAGEMENT_INTERNAL_PORT", "15672")
        )
        management_url = (
            f"http://{broker.hostname}:{management_port}/api/queues/"
            f"{quote(vhost, safe='')}"
        )
        response = httpx.get(
            management_url,
            auth=(
                unquote(broker.username or "guest"),
                unquote(broker.password or "guest"),
            ),
            timeout=2,
        )
        response.raise_for_status()
        for queue in response.json():
            name = str(queue.get("name") or "")
            if name:
                transcode_queue_depth.labels(queue=name).set(
                    float(queue.get("messages") or 0)
                )
    except Exception:
        pass


def _poll_workspace_cleanup_metrics() -> None:
    """Expose worker-side durable counters and the shared-volume backlog."""
    try:
        import redis
        from app.tasks.workspace_cleanup import (
            WORKSPACE_METRICS_KEY,
            _canonical_job_id,
        )

        settings = get_settings()
        client = redis.from_url(
            settings.REDIS_URL,
            socket_connect_timeout=1,
            socket_timeout=1,
        )
        for raw_key, raw_value in (client.hgetall(WORKSPACE_METRICS_KEY) or {}).items():
            key = (
                raw_key.decode("utf-8", "replace")
                if isinstance(raw_key, bytes)
                else str(raw_key)
            )
            outcome, separator, reason = key.partition(":")
            if not separator:
                continue
            workspace_cleanup_events_total.labels(
                outcome=outcome,
                reason=reason,
            ).set(float(raw_value))

        backlog = 0
        if os.path.isdir(settings.WORK_DIR):
            with os.scandir(settings.WORK_DIR) as entries:
                for entry in entries:
                    try:
                        _canonical_job_id(entry.name)
                    except ValueError:
                        continue
                    if entry.is_dir(follow_symlinks=False):
                        backlog += 1
        workspace_cleanup_backlog.set(backlog)
    except Exception:
        # Metrics are diagnostic; readiness and media work must not depend on
        # the reaper counter cache or the read-only API volume.
        pass


class SubmitJobRequest(BaseModel):
    source_url: str
    video_id: Optional[str] = None
    version: Optional[str] = "v1"
    renditions: Optional[List[int]] = None
    audio_languages: Optional[List[str]] = None
    subtitle_languages: Optional[List[str]] = None


class SubmitJobResponse(BaseModel):
    job_id: str
    video_id: str
    status: str


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/ready")
async def ready():
    settings = get_settings()
    errors = []

    # PostgreSQL
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        errors.append(f"database: {exc}")

    # Redis
    try:
        import redis

        r = redis.from_url(settings.REDIS_URL)
        r.ping()
    except Exception as exc:
        errors.append(f"redis: {exc}")

    # RabbitMQ
    try:
        from kombu import Connection

        with Connection(settings.RABBITMQ_URL) as conn:
            conn.connect()
    except Exception as exc:
        errors.append(f"rabbitmq: {exc}")

    # MinIO
    try:
        from app.minio_client import get_client

        client = get_client()
        client.bucket_exists(settings.MINIO_BUCKET)
    except Exception as exc:
        errors.append(f"minio: {exc}")

    # The API mounts the worker-owned volume read-only so cleanup verification
    # cannot claim a false absence when that mount is missing.
    if (
        not os.path.isdir(settings.WORK_DIR)
        or os.path.islink(settings.WORK_DIR)
    ):
        errors.append("workspace: WORK_DIR is not a mounted real directory")

    if errors:
        raise HTTPException(status_code=503, detail={"status": "not ready", "errors": errors})
    return {"status": "ready"}


@app.post("/jobs", response_model=SubmitJobResponse)
async def submit_job(req: SubmitJobRequest):
    from app import models
    from app.tasks.pipeline import run_pipeline

    video_id = req.video_id or str(uuid.uuid4())
    job_id = str(uuid.uuid4())

    db = SessionLocal()
    try:
        video = models.Video(
            id=video_id, source_url=req.source_url, status="pending",
            title=req.source_url.rsplit("/", 1)[-1] or "untitled",
        )
        db.add(video)
        job = models.Job(
            id=job_id,
            video_id=video_id,
            status=models.JobStatus.pending.value,
            input_path=req.source_url,
        )
        db.add(job)
        db.commit()
    finally:
        db.close()

    run_pipeline.delay(
        job_id=job_id,
        source_url=req.source_url,
        video_id=video_id,
        version=req.version,
        renditions=req.renditions,
        audio_languages=req.audio_languages,
        subtitle_languages=req.subtitle_languages,
    )
    transcode_jobs_total.labels(status="submitted").inc()
    return {"job_id": job_id, "video_id": video_id, "status": "submitted"}


@app.get("/jobs/{job_id}")
async def get_job(job_id: str):
    db = SessionLocal()
    try:
        from app import models

        job = db.query(models.Job).filter(models.Job.id == job_id).first()
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        related_jobs = (
            db.query(models.Job)
            .filter(models.Job.video_id == job.video_id)
            .order_by(models.Job.created_at.asc(), models.Job.id.asc())
            .all()
        )
        from app.tasks.workspace_cleanup import workspace_status

        return {
            "id": job.id,
            "video_id": job.video_id,
            "status": job.status,
            "progress": job.progress,
            "output_prefix": job.output_prefix,
            "error_message": job.error_message,
            "created_at": job.created_at,
            "updated_at": job.updated_at,
            "workspace": workspace_status(job_id),
            # Retry/watchdog generations use distinct directories. Returning
            # every durable sibling lets the local E2E cleanup prove no old or
            # replacement workspace remains before metadata cascades Job rows.
            "workspaces": [
                {
                    "job_id": str(related.id),
                    "job_status": str(related.status),
                    **workspace_status(str(related.id)),
                }
                for related in related_jobs
            ],
        }
    finally:
        db.close()


@app.get("/videos/{video_id}")
async def get_video(video_id: str):
    db = SessionLocal()
    try:
        from app import models
        from app.job_fencing import lock_video

        try:
            video = lock_video(db, video_id)
        except ValueError:
            raise HTTPException(status_code=404, detail="Video not found")
        return {
            "id": video.id,
            "source_url": video.source_url,
            "status": video.status,
            "duration": video.duration,
            "width": video.width,
            "height": video.height,
            "video_codec": video.video_codec,
            "frame_rate": video.frame_rate,
            "renditions": [
                {
                    "height": r.height,
                    "width": r.width,
                    "bandwidth": r.bandwidth,
                    "playlist_path": r.playlist_path,
                }
                for r in video.renditions
            ],
            "audio_tracks": [
                {"language": a.language, "playlist_path": a.playlist_path}
                for a in video.audio_tracks
            ],
            "subtitles": [
                {"language": s.language, "playlist_path": s.playlist_path}
                for s in video.subtitles
            ],
        }
    finally:
        db.close()


@app.get("/metrics")
async def metrics():
    """Prometheus scrape endpoint (contract section E)."""
    _poll_gpu_metrics()
    _poll_workspace_cleanup_metrics()
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ---------------------------------------------------------------------------
# Retry endpoint — re-dispatch a stuck or failed job.
# ---------------------------------------------------------------------------

class RetryJobResponse(BaseModel):
    job_id: str
    video_id: str
    status: str
    message: str


def _seed_progress_generation(video_id: str, job_id: str) -> None:
    """Best-effort generation claim that cannot clobber a newer retry."""
    from app import progress as progress_tracker
    from app.job_fencing import StaleJobError, lock_current_job

    db = SessionLocal()
    try:
        lock_current_job(db, job_id)
        progress_tracker.init_progress(
            video_id,
            0,
            [],
            job_id=job_id,
        )
        db.commit()
    except (StaleJobError, ValueError):
        rollback = getattr(db, "rollback", None)
        if callable(rollback):
            rollback()
        logger.info(
            "not seeding progress for stale retry job=%s video=%s",
            job_id,
            video_id,
        )
    except Exception:
        # Redis is reconstructable. The pipeline will retry initialization;
        # never turn a durable retry job into another duplicate generation.
        rollback = getattr(db, "rollback", None)
        if callable(rollback):
            rollback()
        logger.warning(
            "could not seed retry progress job=%s video=%s",
            job_id,
            video_id,
            exc_info=True,
        )
    finally:
        db.close()


@app.post("/videos/{video_id}/retry", response_model=RetryJobResponse)
async def retry_video(video_id: str):
    """Re-dispatch the transcoding pipeline for a stuck or failed video.

    Creates a fresh job and dispatches run_pipeline. Cleans up any stale
    progress state and old rendition/audio/subtitle rows so the retry starts
    clean. The idempotency guard in run_pipeline (dispatch_count) is reset by
    creating a brand-new job ID.

    This covers three scenarios:
      1. ffmpeg hung / killed by stall-timeout → task failed → chord broken.
      2. Worker crash mid-task → chord lost → video stuck in "processing".
      3. User wants to retry a "failed" video.
    """
    from app import models
    from app.job_fencing import lock_video, supersede_active_jobs
    from app.tasks.pipeline import run_pipeline
    from app.tasks.workspace_cleanup import request_workspace_cleanup
    from app import progress as progress_tracker

    old_job_ids: list[str] = []
    db = SessionLocal()
    try:
        try:
            video = lock_video(db, video_id)
        except ValueError:
            raise HTTPException(status_code=404, detail="Video not found")

        # Already ready? No point retrying.
        if video.status == "ready":
            raise HTTPException(status_code=409, detail="Video is already ready; nothing to retry")
        if video.status == "deleting":
            raise HTTPException(
                status_code=409,
                detail="Video is being deleted; retry is fenced",
            )

        source_url = str(video.source_url) if video.source_url else ""
        if not source_url:
            # Older ingestion events did not persist videos.source_url, but
            # their jobs still retain the immutable input path. Recover from
            # that durable record and backfill the video for future retries.
            source_job = (
                db.query(models.Job)
                .filter(
                    models.Job.video_id == video_id,
                    models.Job.input_path.isnot(None),
                )
                .order_by(models.Job.created_at.desc())
                .first()
            )
            if not source_job or not source_job.input_path:
                raise HTTPException(
                    status_code=400,
                    detail="Video has no recoverable source URL; cannot retry",
                )
            source_url = str(source_job.input_path)
            video.source_url = source_url
        # SessionLocal expires ORM attributes on commit. Keep the source as a
        # primitive so dispatch never dereferences a detached Video instance.

        # Clean up old outputs and DB rows from the previous attempt so the
        # retry doesn't conflict with stale rendition/audio/subtitle records.
        db.query(models.Rendition).filter(models.Rendition.video_id == video_id).delete()
        db.query(models.AudioTrack).filter(models.AudioTrack.video_id == video_id).delete()
        db.query(models.Subtitle).filter(models.Subtitle.video_id == video_id).delete()

        # Reset all old jobs for this video to 'failed' so they don't confuse
        # the watchdog; create a fresh job for the retry.
        existing_jobs_query = db.query(models.Job).filter(
            models.Job.video_id == video_id
        )
        list_existing_jobs = getattr(existing_jobs_query, "all", None)
        existing_jobs = (
            list_existing_jobs() if callable(list_existing_jobs) else []
        )
        old_job_ids = [str(existing.id) for existing in existing_jobs]
        job_id = str(uuid.uuid4())
        supersede_active_jobs(
            db,
            video_id,
            replacement_job_id=job_id,
            reason="manual retry",
        )
        job = models.Job(
            id=job_id,
            video_id=video_id,
            status=models.JobStatus.pending.value,
            input_path=source_url,
        )
        db.add(job)
        video.status = "processing"
        db.commit()
    finally:
        db.close()

    for old_job_id in old_job_ids:
        request_workspace_cleanup(
            old_job_id,
            trigger="manual-retry-superseded",
        )

    _seed_progress_generation(video_id, job_id)

    settings = progress_tracker.get_default_settings()
    run_pipeline.delay(
        job_id=job_id,
        source_url=source_url,
        video_id=video_id,
        version="v1",
        settings=settings,
    )
    transcode_jobs_total.labels(status="retried").inc()
    return {
        "job_id": job_id,
        "video_id": video_id,
        "status": "submitted",
        "message": "Pipeline re-dispatched for retry",
    }


# ---------------------------------------------------------------------------
# Stuck-job watchdog — periodically detects and auto-retries hung jobs.
# ---------------------------------------------------------------------------

def _watchdog_progress_updated_at(redis_client, key: str, job_id: str):
    """Return a timestamp only when Redis belongs to this job generation."""
    progress_job_id = redis_client.hget(key, "job_id")
    if isinstance(progress_job_id, bytes):
        progress_job_id = progress_job_id.decode("utf-8", "replace")
    if str(progress_job_id or "") != str(job_id):
        return None
    return redis_client.hget(key, "updated_at")


def _watchdog_tick() -> None:
    """Retry only the current generation when its own progress is stale."""
    import time as _time
    from app import models, progress as progress_tracker
    from app.job_fencing import lock_video, supersede_active_jobs
    from app.tasks.workspace_cleanup import request_workspace_cleanup
    import redis as _redis

    settings = get_settings()
    threshold_sec = getattr(settings, "STUCK_JOB_TIMEOUT_SEC", 900.0)
    max_retries = getattr(settings, "STUCK_JOB_MAX_RETRIES", 2)
    now = _time.time()

    db = SessionLocal()
    try:
        candidates = (
            db.query(models.Video)
            .filter(models.Video.status == "processing")
            .all()
        )
        if not candidates:
            return

        r = _redis.from_url(settings.REDIS_URL)
        for candidate in candidates:
            try:
                video = lock_video(db, str(candidate.id))
            except ValueError:
                continue
            if video.status != "processing":
                db.rollback()
                continue

            current_job = (
                db.query(models.Job)
                .filter(
                    models.Job.video_id == video.id,
                    models.Job.status != models.JobStatus.failed.value,
                )
                .order_by(
                    models.Job.created_at.desc(),
                    models.Job.id.desc(),
                )
                .first()
            )
            if (
                not current_job
                or current_job.status == models.JobStatus.completed.value
            ):
                db.rollback()
                continue

            retry_count = (
                db.query(models.Job)
                .filter(
                    models.Job.video_id == video.id,
                    models.Job.status == models.JobStatus.failed.value,
                    models.Job.error_message.like(
                        "Superseded by watchdog retry%"
                    ),
                )
                .count()
            )
            if retry_count >= max_retries:
                current_job.status = models.JobStatus.failed.value
                current_job.error_message = "Watchdog exceeded max retries"
                video.status = "failed"
                current_job_id = str(current_job.id)
                current_video_id = str(video.id)
                db.commit()
                request_workspace_cleanup(
                    current_job_id,
                    trigger="watchdog-max-retries",
                )
                progress_tracker.fail_task(
                    current_video_id,
                    "watchdog",
                    "Exceeded max retries",
                    job_id=current_job_id,
                )
                logger.warning(
                    "[watchdog] video=%s exceeded %d retries, marking failed",
                    current_video_id,
                    max_retries,
                )
                continue

            key = f"video:{video.id}:progress"
            raw_updated_at = _watchdog_progress_updated_at(
                r,
                key,
                str(current_job.id),
            )
            if raw_updated_at is None:
                age = now - (
                    current_job.created_at.timestamp()
                    if current_job.created_at
                    else now
                )
                if age < threshold_sec:
                    db.rollback()
                    continue
            else:
                try:
                    updated_at = float(raw_updated_at)
                except (TypeError, ValueError):
                    updated_at = None
                if (
                    updated_at is not None
                    and updated_at <= now + 60
                    and (now - updated_at) < threshold_sec
                ):
                    db.rollback()
                    continue
                if updated_at is None or updated_at > now + 60:
                    age = now - (
                        current_job.created_at.timestamp()
                        if current_job.created_at
                        else now
                    )
                    if age < threshold_sec:
                        db.rollback()
                        continue

            if not video.source_url:
                logger.error(
                    "[watchdog] video=%s has no source_url, cannot retry",
                    video.id,
                )
                db.rollback()
                continue

            logger.warning(
                "[watchdog] detected stuck video=%s (retry %d/%d), "
                "creating a fenced generation",
                video.id,
                retry_count + 1,
                max_retries,
            )

            db.query(models.Rendition).filter(
                models.Rendition.video_id == video.id
            ).delete()
            db.query(models.AudioTrack).filter(
                models.AudioTrack.video_id == video.id
            ).delete()
            db.query(models.Subtitle).filter(
                models.Subtitle.video_id == video.id
            ).delete()

            job_id = str(uuid.uuid4())
            source_url = str(video.source_url)
            current_video_id = str(video.id)
            old_job_ids = [
                str(existing.id)
                for existing in db.query(models.Job)
                .filter(models.Job.video_id == current_video_id)
                .all()
            ]
            supersede_active_jobs(
                db,
                current_video_id,
                replacement_job_id=job_id,
                reason="watchdog retry",
            )
            db.add(
                models.Job(
                    id=job_id,
                    video_id=current_video_id,
                    status=models.JobStatus.pending.value,
                    input_path=source_url,
                )
            )
            video.status = "processing"
            db.commit()

            for old_job_id in old_job_ids:
                request_workspace_cleanup(
                    old_job_id,
                    trigger="watchdog-retry-superseded",
                )

            _seed_progress_generation(current_video_id, job_id)

            from app.tasks.pipeline import run_pipeline
            pipeline_settings = progress_tracker.get_default_settings()
            run_pipeline.delay(
                job_id=job_id,
                source_url=source_url,
                video_id=current_video_id,
                version="v1",
                settings=pipeline_settings,
            )
            transcode_jobs_total.labels(status="auto_retried").inc()
    except Exception as exc:
        db.rollback()
        logger.exception("[watchdog] tick failed: %s", exc)
    finally:
        db.close()


def _run_watchdog_loop() -> None:
    """Background loop that calls _watchdog_tick every WATCHDOG_INTERVAL_SEC."""
    import time as _time

    settings = get_settings()
    interval = getattr(settings, "WATCHDOG_INTERVAL_SEC", 300.0)
    logger.info("[watchdog] stuck-job watchdog started (interval=%.0fs)", interval)
    while True:
        try:
            _watchdog_tick()
        except Exception as exc:
            logger.exception("[watchdog] error: %s", exc)
        _time.sleep(interval)


import threading as _threading

# Start the watchdog as a daemon thread when the module loads in a worker
# process (not in the FastAPI app process — that runs in transcoder-api).
_watchdog_started = False


def start_watchdog() -> None:
    """Start the stuck-job watchdog daemon thread (idempotent)."""
    global _watchdog_started
    if _watchdog_started:
        return
    _watchdog_started = True
    t = _threading.Thread(target=_run_watchdog_loop, daemon=True)
    t.start()
