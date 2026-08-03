"""Authoritative generation fencing for video processing jobs.

Jobs are deliberately kept in the existing schema.  The locked ``videos`` row
is the serialization point for retry creation and for every durable mutation.
At most one non-failed job is authoritative: the newest such row.  Retry
creation additionally marks every older non-terminal row failed, so normal
operation has exactly one active generation.
"""

import logging
import os
import uuid
from typing import Optional, Tuple

from app import models
from app.config import get_settings
from app.job_lock import job_lock
from app.minio_client import download_source


logger = logging.getLogger(__name__)

SUPERSEDED_PREFIX = "Superseded by "
NONTERMINAL_JOB_STATUSES = (
    models.JobStatus.pending.value,
    models.JobStatus.queued.value,
    models.JobStatus.probing.value,
    models.JobStatus.extracting.value,
    models.JobStatus.transcoding.value,
    models.JobStatus.packaging.value,
    models.JobStatus.publishing.value,
)

_STATUS_ORDER = {
    models.JobStatus.pending.value: 0,
    models.JobStatus.probing.value: 1,
    models.JobStatus.queued.value: 2,
    models.JobStatus.extracting.value: 3,
    models.JobStatus.transcoding.value: 4,
    models.JobStatus.packaging.value: 5,
    models.JobStatus.publishing.value: 6,
    models.JobStatus.completed.value: 7,
}


class StaleJobError(RuntimeError):
    """A delivery belongs to a completed, failed, or superseded generation."""


class InconsistentJobStateError(RuntimeError):
    """Durable Job/Video terminal state is internally contradictory."""


def _for_update(query):
    """Keep small in-memory test doubles usable while locking in production."""
    method = getattr(query, "with_for_update", None)
    return method() if callable(method) else query


def lock_video(db, video_id: str):
    """Lock and return one video row."""
    video = _for_update(
        db.query(models.Video).filter(models.Video.id == video_id)
    ).first()
    if not video:
        raise ValueError(f"Video {video_id} not found")
    return video


def _latest_nonfailed_job(db, video_id: str):
    query = db.query(models.Job).filter(
        models.Job.video_id == video_id,
        models.Job.status != models.JobStatus.failed.value,
    )
    order_by = getattr(query, "order_by", None)
    if callable(order_by):
        query = order_by(
            models.Job.created_at.desc(),
            models.Job.id.desc(),
        )
    return query.first()


def lock_current_job(
    db,
    job_id: str,
    *,
    allow_completed: bool = False,
) -> Tuple[models.Job, models.Video]:
    """Lock a job's video generation and reject stale/terminal deliveries.

    The first unlocked lookup only discovers the parent video.  The video row
    is then locked before the Job is re-read, matching retry creation's lock
    order and avoiding a Job/Video deadlock.
    """
    discovered = db.query(models.Job).filter(models.Job.id == job_id).first()
    if not discovered:
        raise ValueError(f"Job {job_id} not found")
    if str(getattr(discovered, "id", "")) != str(job_id):
        raise StaleJobError(
            f"job lookup returned {getattr(discovered, 'id', None)}, "
            f"expected {job_id}"
        )

    video = lock_video(db, str(discovered.video_id))
    job = _for_update(
        db.query(models.Job).filter(models.Job.id == job_id)
    ).first()
    if not job:
        raise StaleJobError(f"job {job_id} disappeared")
    if str(getattr(job, "id", "")) != str(job_id):
        raise StaleJobError(
            f"locked job lookup returned {getattr(job, 'id', None)}, "
            f"expected {job_id}"
        )

    status = str(job.status)
    if status == models.JobStatus.failed.value:
        raise StaleJobError(f"job {job_id} is failed or superseded")

    current = _latest_nonfailed_job(db, str(video.id))
    if not current or str(current.id) != str(job.id):
        current_id = str(current.id) if current else "none"
        raise StaleJobError(
            f"job {job_id} is not the current generation ({current_id})"
        )

    if status == models.JobStatus.completed.value:
        if getattr(video, "status", None) in {"deleting", "deleted"}:
            raise StaleJobError(
                f"video {video.id} is {video.status}; completed job is stale"
            )
        if allow_completed and getattr(video, "status", None) == "ready":
            return job, video
        raise InconsistentJobStateError(
            f"job {job_id} is completed but video {video.id} is "
            f"{getattr(video, 'status', None)!r}"
        )

    if getattr(video, "status", None) in {
        "ready",
        "failed",
        "deleting",
        "deleted",
    }:
        raise StaleJobError(
            f"video {video.id} is {video.status}; job {job_id} cannot mutate it"
        )
    return job, video


def advance_job_status(job, status: str) -> bool:
    """Advance a non-terminal job without parallel tasks regressing it."""
    current = str(job.status or models.JobStatus.pending.value)
    if current in (
        models.JobStatus.completed.value,
        models.JobStatus.failed.value,
    ):
        return False
    if _STATUS_ORDER.get(status, -1) >= _STATUS_ORDER.get(current, -1):
        if current != status:
            job.status = status
            return True
    return False


def supersede_active_jobs(
    db,
    video_id: str,
    *,
    replacement_job_id: Optional[str],
    reason: str,
) -> int:
    """Mark every older non-terminal generation failed.

    Callers must hold the parent Video row lock.  The bulk update obtains the
    relevant Job row locks before the replacement is committed.
    """
    query = db.query(models.Job).filter(
        models.Job.video_id == video_id,
        models.Job.status.in_(NONTERMINAL_JOB_STATUSES),
    )
    if replacement_job_id:
        query = query.filter(models.Job.id != replacement_job_id)
    values = {
        models.Job.status: models.JobStatus.failed.value,
        models.Job.error_message: f"{SUPERSEDED_PREFIX}{reason}",
    }
    try:
        return query.update(values, synchronize_session=False)
    except TypeError:
        # Lightweight unit-test sessions generally expose only update(values).
        return query.update(values)


def ensure_local_source(job_id: str, source_url: str) -> str:
    """Download a job source exactly once and atomically promote it.

    Header tasks share a job lock but have different track locks, so source
    preparation needs its own exclusive lock.  A failed download can only
    leave a uniquely named partial file, never ``source.mp4``.
    """
    work_root = get_settings().WORK_DIR
    work_dir = os.path.join(work_root, job_id)
    local_source = os.path.join(work_dir, "source.mp4")
    with job_lock(
        work_root,
        f"{job_id}:source",
        purpose="source-download",
    ):
        if os.path.isfile(local_source) and os.path.getsize(local_source) > 0:
            return local_source

        os.makedirs(work_dir, exist_ok=True)
        partial = os.path.join(
            work_dir,
            f".source-{uuid.uuid4().hex}.part",
        )
        try:
            download_source(source_url, partial)
            if not os.path.isfile(partial) or os.path.getsize(partial) <= 0:
                raise RuntimeError(
                    f"downloaded source for job {job_id} is empty"
                )
            os.replace(partial, local_source)
        finally:
            try:
                if os.path.exists(partial):
                    os.remove(partial)
            except OSError:
                logger.warning(
                    "could not remove partial source download %s",
                    partial,
                    exc_info=True,
                )
    return local_source


def stale_result(job_id: str, task_type: str) -> dict:
    """Stable success payload for harmless delayed deliveries."""
    return {
        "job_id": job_id,
        "type": task_type,
        "skipped": True,
        "superseded": True,
    }
