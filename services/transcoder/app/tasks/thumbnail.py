"""Generate poster image and thumbnail sprites."""

import logging
import os
import shutil

from app.celery_app import celery_app
from app.config import get_settings
from app.db import SessionLocal
from app import ffmpeg_utils, models
from app.job_fencing import (
    StaleJobError,
    ensure_local_source,
    lock_current_job,
    stale_result,
)
from app.job_lock import job_lock
from app import progress as progress_tracker

logger = logging.getLogger(__name__)


def _run_thumbnail(self, job_id: str, source_url: str, settings: dict = None) -> dict:
    logger.info("[thumbnail] job=%s", job_id)
    task_name = "thumbnail"
    db = SessionLocal()
    job = None
    try:
        job, video = lock_current_job(db, job_id)
        db.commit()
        video_id = str(job.video_id)

        progress_tracker.start_task(
            video_id,
            task_name,
            "Generating thumbnails",
            job_id=job_id,
        )

        work_dir = os.path.join(get_settings().WORK_DIR, job_id)
        local_source = ensure_local_source(job_id, source_url)

        duration = video.duration if video and video.duration else 0.0

        output_dir = os.path.join(work_dir, "output", "thumbnails")
        # A killed FFmpeg can leave an old sprite/poster pair. Every retry
        # rebuilds this task-owned directory while its track lock is held.
        if os.path.isdir(output_dir):
            shutil.rmtree(output_dir)
        os.makedirs(output_dir, exist_ok=True)

        poster_cmd, thumbs_cmd = ffmpeg_utils.thumbnail_commands(
            local_source, output_dir, duration
        )
        ffmpeg_utils.run_cmd(poster_cmd)
        ffmpeg_utils.run_cmd(thumbs_cmd)

        result = {
            "thumbnails_dir": output_dir.replace("\\", "/"),
            "poster": os.path.join(output_dir, "poster.jpg").replace("\\", "/"),
        }

        # Optional trickplay sprite + WebVTT index for scrubber previews.
        if settings.get("trickplay", True):
            try:
                sprite_cmd, index_cmd = ffmpeg_utils.trickplay_commands(
                    local_source, output_dir, duration
                )
                ffmpeg_utils.run_cmd(sprite_cmd)
                ffmpeg_utils.run_cmd(index_cmd)
                result["sprite"] = os.path.join(output_dir, "sprite.jpg").replace("\\", "/")
                result["sprite_vtt"] = os.path.join(output_dir, "sprite.vtt").replace("\\", "/")
            except Exception as exc:
                logger.warning("[thumbnail] trickplay generation failed for job=%s: %s", job_id, exc)

        lock_current_job(db, job_id)
        db.commit()
        progress_tracker.complete_task(video_id, task_name, job_id=job_id)
        return result
    except StaleJobError:
        raise
    except Exception:
        if job:
            progress_tracker.fail_task(
                job.video_id,
                task_name,
                str(self),
                job_id=job_id,
            )
        raise
    finally:
        db.close()


@celery_app.task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    max_retries=3,
)
def thumbnail(self, job_id: str, source_url: str, settings: dict = None) -> dict:
    """Serialize thumbnail output and coordinate with package cleanup."""
    work_root = get_settings().WORK_DIR
    try:
        with job_lock(
            work_root,
            job_id,
            purpose="thumbnail:job",
            shared=True,
        ):
            with job_lock(
                work_root,
                f"{job_id}:thumbnail",
                purpose="thumbnail",
            ):
                return _run_thumbnail(
                    self,
                    job_id,
                    source_url,
                    settings,
                )
    except StaleJobError as exc:
        logger.info("[thumbnail] skipping stale job=%s: %s", job_id, exc)
        return stale_result(job_id, "thumbnail")
