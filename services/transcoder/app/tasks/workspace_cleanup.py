"""Durable, lock-aware reclamation of transcoder job workspaces.

Every task which mutates ``WORK_DIR/<job_id>`` holds the shared job lock.  The
reaper takes that lock exclusively and non-blockingly, then re-reads durable
generation state before removing anything.  This lets cleanup wait/retry
without occupying a worker while FFmpeg is still alive and makes a redelivered
cleanup task harmless after a crash.
"""

from __future__ import annotations

import logging
import os
import shutil
import stat
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import redis

from app import models
from app.celery_app import celery_app
from app.config import get_settings
from app.db import SessionLocal
from app.job_lock import JobLockBusy, job_lock, reap_idle_job_locks
from app.minio_client import published_prefix_exists


logger = logging.getLogger(__name__)

WORKSPACE_METRICS_KEY = "transcoder:workspace_cleanup:metrics"
_LOCK_DIR_NAME = ".job-locks"
_ORPHAN_OBSERVED_MARKER = ".workspace-cleanup-observed"
_scheduler_guard = threading.Lock()
_scheduler_started = False


class UnsafeWorkspacePath(ValueError):
    """A workspace target is not one direct, real directory under WORK_DIR."""


class WorkspaceCleanupDeferred(RuntimeError):
    """Cleanup is safe later, after a lock, grace period, or state transition."""


@dataclass(frozen=True)
class DurableWorkspaceState:
    """Small immutable snapshot used to make one cleanup decision."""

    job_exists: bool
    job_status: Optional[str] = None
    video_status: Optional[str] = None
    output_prefix: Optional[str] = None
    current_job_id: Optional[str] = None
    updated_at: Optional[datetime] = None


def _canonical_job_id(job_id: str) -> str:
    value = str(job_id or "")
    try:
        parsed = uuid.UUID(value)
    except (ValueError, TypeError, AttributeError) as exc:
        raise UnsafeWorkspacePath("workspace name is not a UUID job id") from exc
    if str(parsed) != value.lower():
        raise UnsafeWorkspacePath("workspace name is not a canonical UUID")
    return value


def _workspace_candidate(work_root: str, job_id: str) -> str:
    """Return an exact direct-child target without resolving through a symlink."""
    canonical_id = _canonical_job_id(job_id)
    root = os.path.abspath(work_root)
    if os.path.islink(root):
        raise UnsafeWorkspacePath("configured work root is a symlink")
    candidate = os.path.abspath(os.path.join(root, canonical_id))
    if os.path.dirname(candidate) != root:
        raise UnsafeWorkspacePath("workspace is not a direct child of WORK_DIR")
    return candidate


def workspace_status(job_id: str, *, work_root: Optional[str] = None) -> dict:
    """Describe the exact job directory without walking it or mutating it."""
    root = work_root or get_settings().WORK_DIR
    try:
        candidate = _workspace_candidate(root, job_id)
    except UnsafeWorkspacePath as exc:
        return {
            "job_id": str(job_id),
            "exists": False,
            "state": "unsafe",
            "error": str(exc),
        }

    if not os.path.isdir(os.path.abspath(root)):
        return {
            "job_id": str(job_id),
            "path": candidate,
            "exists": None,
            "state": "unavailable",
            "error": "configured WORK_DIR is not mounted or is not a directory",
        }

    if not os.path.lexists(candidate):
        return {
            "job_id": str(job_id),
            "path": candidate,
            "exists": False,
            "state": "absent",
        }

    try:
        mode = os.lstat(candidate).st_mode
    except FileNotFoundError:
        return {
            "job_id": str(job_id),
            "path": candidate,
            "exists": False,
            "state": "absent",
        }
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        return {
            "job_id": str(job_id),
            "path": candidate,
            "exists": True,
            "state": "unsafe",
            "error": "workspace target is not a real directory",
        }
    if os.path.dirname(os.path.realpath(candidate)) != os.path.realpath(root):
        return {
            "job_id": str(job_id),
            "path": candidate,
            "exists": True,
            "state": "unsafe",
            "error": "workspace resolves outside WORK_DIR",
        }
    return {
        "job_id": str(job_id),
        "path": candidate,
        "exists": True,
        "state": "present",
    }


def _for_update(query):
    method = getattr(query, "with_for_update", None)
    return method() if callable(method) else query


def _load_durable_state(db, job_id: str) -> DurableWorkspaceState:
    # Match retry/package fencing order: discover parent, lock Video, then
    # lock Job. Taking Job first can deadlock a concurrent retry which already
    # owns the Video row and is bulk-updating superseded Jobs.
    discovered = db.query(models.Job).filter(models.Job.id == job_id).first()
    if discovered is None:
        return DurableWorkspaceState(job_exists=False)

    video = _for_update(
        db.query(models.Video).filter(models.Video.id == discovered.video_id)
    ).first()
    job = _for_update(
        db.query(models.Job).filter(models.Job.id == job_id)
    ).first()
    if job is None:
        return DurableWorkspaceState(job_exists=False)
    current = (
        db.query(models.Job)
        .filter(
            models.Job.video_id == job.video_id,
            models.Job.status != models.JobStatus.failed.value,
        )
        .order_by(models.Job.created_at.desc(), models.Job.id.desc())
        .first()
    )
    updated_at = getattr(job, "updated_at", None) or getattr(
        job, "created_at", None
    )
    return DurableWorkspaceState(
        job_exists=True,
        job_status=str(job.status or ""),
        video_status=(
            str(video.status or "") if video is not None else None
        ),
        output_prefix=(
            str(job.output_prefix) if getattr(job, "output_prefix", None) else None
        ),
        current_job_id=(str(current.id) if current is not None else None),
        updated_at=updated_at,
    )


def _age_seconds(value: Optional[datetime], *, now: float, fallback: float) -> float:
    if value is None:
        return fallback
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    try:
        return max(0.0, now - value.timestamp())
    except (OverflowError, OSError, ValueError):
        return fallback


def _orphan_observation_age(candidate: str, *, now: float) -> float:
    """Require one durable observation before deleting a DB-less workspace.

    A removed Job row has no trustworthy terminal timestamp. The directory's
    own mtime may be hours old even if a hard-killed FFmpeg process released
    its inherited workspace only milliseconds ago. This marker turns the
    configured grace period into a restart-safe two-pass decision.
    """
    marker = os.path.join(candidate, _ORPHAN_OBSERVED_MARKER)
    try:
        marker_stat = os.lstat(marker)
    except FileNotFoundError:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(marker, flags, 0o600)
        try:
            os.write(descriptor, f"{now:.6f}\n".encode("ascii"))
        finally:
            os.close(descriptor)
        return 0.0
    if stat.S_ISLNK(marker_stat.st_mode) or not stat.S_ISREG(
        marker_stat.st_mode
    ):
        raise UnsafeWorkspacePath("orphan observation marker is unsafe")
    return max(0.0, now - marker_stat.st_mtime)


def _eligible_reason(state: DurableWorkspaceState, job_id: str) -> Optional[str]:
    if not state.job_exists:
        return "orphan"
    if state.job_status == models.JobStatus.failed.value:
        return "failed"
    if (
        state.current_job_id is not None
        and state.current_job_id != str(job_id)
    ):
        return "superseded"
    if state.video_status in {"deleting", "deleted"}:
        return "deleting"
    if (
        state.video_status == "failed"
        and state.job_status != models.JobStatus.completed.value
    ):
        # Once the exclusive filesystem lock is held, no mutator remains.
        # Future redeliveries are rejected by generation fencing.
        return "failed_video"
    if state.job_status == models.JobStatus.completed.value:
        if (
            state.video_status == "ready"
            and state.output_prefix
            and published_prefix_exists(state.output_prefix)
        ):
            return "completed_durable"
        return None
    return None


def _record_metric(outcome: str, reason: str) -> None:
    """Persist bounded cleanup counters so the API process can expose them."""
    try:
        settings = get_settings()
        client = redis.from_url(
            settings.REDIS_URL,
            socket_connect_timeout=1,
            socket_timeout=1,
        )
        client.hincrby(
            WORKSPACE_METRICS_KEY,
            f"{outcome}:{reason}",
            1,
        )
    except Exception:
        logger.debug(
            "workspace cleanup metric write failed outcome=%s reason=%s",
            outcome,
            reason,
            exc_info=True,
        )


def _remove_exact_workspace(candidate: str, work_root: str) -> None:
    """Remove one already-validated direct child and verify it is gone."""
    status_payload = workspace_status(
        os.path.basename(candidate),
        work_root=work_root,
    )
    if status_payload["state"] == "absent":
        return
    if status_payload["state"] != "present":
        raise UnsafeWorkspacePath(
            status_payload.get("error") or "workspace path is unsafe"
        )
    shutil.rmtree(candidate)
    if os.path.lexists(candidate):
        raise OSError(f"workspace still exists after cleanup: {candidate}")


def _cleanup_workspace_once(
    job_id: str,
    *,
    trigger: str,
    now: Optional[float] = None,
) -> dict:
    """Attempt one non-blocking, generation-aware cleanup."""
    settings = get_settings()
    work_root = settings.WORK_DIR
    status_payload = workspace_status(job_id, work_root=work_root)
    if status_payload["state"] == "absent":
        _record_metric("skipped", "absent")
        return {**status_payload, "outcome": "absent", "trigger": trigger}
    if status_payload["state"] == "unavailable":
        _record_metric("failed", "work_root_unavailable")
        raise OSError(status_payload["error"])
    if status_payload["state"] != "present":
        _record_metric("refused", "unsafe")
        logger.error(
            "metric=workspace_cleanup outcome=refused reason=unsafe "
            "job=%s trigger=%s detail=%s",
            job_id,
            trigger,
            status_payload.get("error"),
        )
        return {**status_payload, "outcome": "unsafe", "trigger": trigger}

    candidate = status_payload["path"]
    now_value = time.time() if now is None else float(now)
    try:
        with job_lock(
            work_root,
            job_id,
            purpose=f"workspace-cleanup:{trigger}",
            blocking=False,
        ):
            # Re-check after acquiring exclusion; another cleanup could have
            # won between the initial status call and this lock.
            locked_status = workspace_status(job_id, work_root=work_root)
            if locked_status["state"] == "absent":
                _record_metric("skipped", "absent")
                return {
                    **locked_status,
                    "outcome": "absent",
                    "trigger": trigger,
                }
            if locked_status["state"] != "present":
                _record_metric("refused", "unsafe")
                return {
                    **locked_status,
                    "outcome": "unsafe",
                    "trigger": trigger,
                }

            try:
                fallback_age = max(
                    0.0,
                    now_value - os.lstat(candidate).st_mtime,
                )
            except FileNotFoundError:
                fallback_age = float("inf")

            db = SessionLocal()
            try:
                state = _load_durable_state(db, job_id)
                reason = _eligible_reason(state, job_id)
                if reason is None:
                    active_reason = (
                        "ready_inconsistent"
                        if state.video_status == "ready"
                        else "active"
                    )
                    _record_metric("skipped", active_reason)
                    logger.info(
                        "metric=workspace_cleanup outcome=skipped reason=%s "
                        "job=%s status=%s video_status=%s current_job=%s",
                        active_reason,
                        job_id,
                        state.job_status,
                        state.video_status,
                        state.current_job_id,
                    )
                    return {
                        **locked_status,
                        "outcome": "active",
                        "reason": active_reason,
                        "trigger": trigger,
                    }

                if reason == "orphan":
                    fallback_age = _orphan_observation_age(
                        candidate,
                        now=now_value,
                    )
                age = _age_seconds(
                    state.updated_at,
                    now=now_value,
                    fallback=fallback_age,
                )
                grace = float(settings.WORKSPACE_CLEANUP_GRACE_SEC)
                if age < grace:
                    _record_metric("deferred", "grace")
                    logger.info(
                        "metric=workspace_cleanup outcome=deferred "
                        "reason=grace job=%s eligibility=%s trigger=%s "
                        "retry_after_sec=%.3f",
                        job_id,
                        reason,
                        trigger,
                        grace - age,
                    )
                    return {
                        **locked_status,
                        "outcome": "deferred",
                        "reason": reason,
                        "retry_after_seconds": round(grace - age, 3),
                        "trigger": trigger,
                    }

                _remove_exact_workspace(candidate, work_root)
                _record_metric("deleted", reason)
                logger.info(
                    "metric=workspace_cleanup outcome=deleted reason=%s "
                    "job=%s trigger=%s path=%s age_sec=%.3f",
                    reason,
                    job_id,
                    trigger,
                    candidate,
                    age,
                )
                return {
                    **workspace_status(job_id, work_root=work_root),
                    "outcome": "deleted",
                    "reason": reason,
                    "trigger": trigger,
                }
            finally:
                db.close()
    except JobLockBusy:
        _record_metric("deferred", "busy")
        logger.info(
            "metric=workspace_cleanup outcome=deferred reason=busy "
            "job=%s trigger=%s",
            job_id,
            trigger,
        )
        return {
            **status_payload,
            "outcome": "busy",
            "reason": "mutator_lock_held",
            "trigger": trigger,
        }


def request_workspace_cleanup(job_id: str, *, trigger: str) -> bool:
    """Best-effort durable enqueue; the periodic scan is the fallback."""
    try:
        cleanup_job_workspace.apply_async(
            kwargs={"job_id": str(job_id), "trigger": trigger},
        )
        _record_metric("queued", trigger)
        logger.info(
            "metric=workspace_cleanup outcome=queued job=%s trigger=%s",
            job_id,
            trigger,
        )
        return True
    except Exception:
        _record_metric("failed", "enqueue")
        logger.exception(
            "workspace cleanup enqueue failed job=%s trigger=%s; "
            "periodic reaper will retry",
            job_id,
            trigger,
        )
        return False


@celery_app.task(bind=True, max_retries=None)
def cleanup_job_workspace(
    self,
    job_id: str,
    trigger: str = "requested",
) -> dict:
    """Retry a cleanup intent until exclusion/grace permit it."""
    settings = get_settings()
    try:
        result = _cleanup_workspace_once(job_id, trigger=trigger)
    except Exception as exc:
        _record_metric("failed", "exception")
        logger.exception(
            "workspace cleanup attempt failed job=%s trigger=%s",
            job_id,
            trigger,
        )
        raise self.retry(
            exc=exc,
            countdown=float(settings.WORKSPACE_CLEANUP_RETRY_SEC),
            max_retries=int(settings.WORKSPACE_CLEANUP_MAX_RETRIES),
        )

    if result["outcome"] in {"busy", "deferred", "active"}:
        raise self.retry(
            exc=WorkspaceCleanupDeferred(
                f"workspace cleanup deferred: {result['outcome']}"
            ),
            countdown=float(settings.WORKSPACE_CLEANUP_RETRY_SEC),
            max_retries=int(settings.WORKSPACE_CLEANUP_MAX_RETRIES),
        )
    return result


@celery_app.task(bind=True)
def reap_workspaces(self, cursor: str = "") -> dict:
    """Scan bounded UUID job directories; crashes/redeliveries are idempotent."""
    del self
    settings = get_settings()
    started = time.monotonic()
    summary = {
        "scanned": 0,
        "deleted": 0,
        "absent": 0,
        "busy": 0,
        "deferred": 0,
        "active": 0,
        "unsafe": 0,
        "errors": 0,
    }
    if not settings.WORKSPACE_REAPER_ENABLED:
        return {**summary, "disabled": True}

    os.makedirs(settings.WORK_DIR, exist_ok=True)
    summary["job_locks"] = reap_idle_job_locks(settings.WORK_DIR)
    try:
        with os.scandir(settings.WORK_DIR) as iterator:
            entries = sorted(iterator, key=lambda item: item.name)
    except OSError:
        _record_metric("failed", "scan")
        logger.exception("workspace reaper could not scan %s", settings.WORK_DIR)
        raise

    candidates = []
    for entry in entries:
        if entry.name == _LOCK_DIR_NAME or entry.name <= cursor:
            continue
        try:
            _canonical_job_id(entry.name)
        except UnsafeWorkspacePath:
            continue
        candidates.append(entry)

    limit = int(settings.WORKSPACE_REAPER_SCAN_LIMIT)
    selected = candidates[:limit]
    for entry in selected:
        summary["scanned"] += 1
        try:
            result = _cleanup_workspace_once(
                entry.name,
                trigger="periodic-reaper",
            )
            outcome = result["outcome"]
            if outcome in summary:
                summary[outcome] += 1
        except Exception:
            summary["errors"] += 1
            _record_metric("failed", "reaper_item")
            logger.exception(
                "workspace reaper item failed job=%s",
                entry.name,
            )

    if len(candidates) > limit and selected:
        next_cursor = selected[-1].name
        try:
            reap_workspaces.apply_async(kwargs={"cursor": next_cursor})
            summary["continuation_queued"] = True
            summary["next_cursor"] = next_cursor
        except Exception:
            summary["continuation_queued"] = False
            summary["errors"] += 1
            _record_metric("failed", "continuation")
            logger.exception(
                "workspace reaper could not queue continuation cursor=%s",
                next_cursor,
            )

    summary["seconds"] = round(time.monotonic() - started, 3)
    logger.info(
        "metric=workspace_reaper scanned=%d deleted=%d active=%d busy=%d "
        "deferred=%d unsafe=%d errors=%d seconds=%.3f",
        summary["scanned"],
        summary["deleted"],
        summary["active"],
        summary["busy"],
        summary["deferred"],
        summary["unsafe"],
        summary["errors"],
        summary["seconds"],
    )
    return summary


def _run_reaper_scheduler() -> None:
    settings = get_settings()
    interval = float(settings.WORKSPACE_REAPER_INTERVAL_SEC)
    logger.info(
        "workspace reaper scheduler started interval_sec=%.1f",
        interval,
    )
    while True:
        try:
            reap_workspaces.apply_async()
        except Exception:
            _record_metric("failed", "schedule")
            logger.exception(
                "workspace reaper scheduling failed; retrying next interval"
            )
        time.sleep(interval)


def start_workspace_reaper_scheduler() -> None:
    """Start the event-worker scheduler once; actual cleanup runs on Celery."""
    global _scheduler_started
    settings = get_settings()
    if not settings.WORKSPACE_REAPER_ENABLED:
        logger.info("workspace reaper scheduler is disabled")
        return
    with _scheduler_guard:
        if _scheduler_started:
            return
        _scheduler_started = True
        thread = threading.Thread(
            target=_run_reaper_scheduler,
            name="workspace-reaper-scheduler",
            daemon=True,
        )
        thread.start()
