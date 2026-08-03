"""Probe source media with ffprobe."""

import logging
from app.celery_app import celery_app
from app.config import get_settings
from app.db import SessionLocal
from app import ffmpeg_utils, models
from app.job_fencing import (
    StaleJobError,
    advance_job_status,
    ensure_local_source,
    lock_current_job,
    stale_result,
)
from app.job_lock import job_lock
from app.rabbitmq import publish_event
from app import progress as progress_tracker

logger = logging.getLogger(__name__)


def _run_probe(job_id: str, source_url: str) -> dict:
    """Download source, run ffprobe and persist metadata.

    Pure function (no Celery) so the pipeline task can invoke it synchronously
    in-process instead of round-tripping through the broker, which Celery
    forbids (calling .delay().get() inside a task raises E_WOULDBLOCK).
    """
    logger.info("[probe] job=%s source=%s", job_id, source_url)
    db = SessionLocal()
    try:
        job, _video = lock_current_job(db, job_id)
        advance_job_status(job, models.JobStatus.probing.value)
        db.commit()

        video_id = str(job.video_id)
        progress_tracker.start_task(
            video_id,
            "probe",
            "Probing source media",
            job_id=job_id,
        )

        local_source = ensure_local_source(job_id, source_url)

        probe_data = ffmpeg_utils.ffprobe(local_source)
        parsed = ffmpeg_utils.parse_probe(probe_data)

        job, video = lock_current_job(db, job_id)
        video.duration = parsed["duration"]
        video.width = parsed["width"]
        video.height = parsed["height"]
        video.video_codec = parsed["video_codec"]
        video.frame_rate = parsed["frame_rate"]
        video.bitrate = parsed["video_bitrate"]
        db.commit()

        progress_tracker.complete_task(video_id, "probe", job_id=job_id)
        publish_event(
            "probe.completed",
            {"job_id": job_id, "video_id": video_id, "duration": parsed["duration"], "streams": parsed},
        )
        return parsed
    finally:
        db.close()


def run_probe(job_id: str, source_url: str) -> dict:
    """Serialize probe/source preparation against duplicate deliveries."""
    work_root = get_settings().WORK_DIR
    with job_lock(
        work_root,
        job_id,
        purpose="probe:job",
        shared=True,
    ):
        with job_lock(
            work_root,
            f"{job_id}:probe",
            purpose="probe",
        ):
            return _run_probe(job_id, source_url)


@celery_app.task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    max_retries=3,
)
def probe(self, job_id: str, source_url: str) -> dict:
    """Celery wrapper around run_probe (kept for the probe queue / retries)."""
    try:
        return run_probe(job_id, source_url)
    except StaleJobError as exc:
        logger.info("[probe] skipping stale job=%s: %s", job_id, exc)
        return stale_result(job_id, "probe")
    except Exception:
        db = SessionLocal()
        try:
            job = db.query(models.Job).filter(models.Job.id == job_id).first()
            if job:
                progress_tracker.fail_task(
                    job.video_id,
                    "probe",
                    str(self),
                    job_id=job_id,
                )
        finally:
            db.close()
        raise
