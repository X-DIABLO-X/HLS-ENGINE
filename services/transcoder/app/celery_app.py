"""Celery application configuration."""

import logging
import os
import socket

from celery import Celery
from celery.signals import worker_shutdown

from app import gpu_registry
from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

celery_app = Celery(
    "transcoder",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=[
        "app.tasks.pipeline",
        "app.tasks.probe",
        "app.tasks.extract_audio",
        "app.tasks.extract_subtitles",
        "app.tasks.transcode_video",
        "app.tasks.package",
        "app.tasks.thumbnail",
        "app.tasks.workspace_cleanup",
    ],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_soft_time_limit=settings.CELERY_TASK_SOFT_TIME_LIMIT_SEC,
    task_time_limit=settings.CELERY_TASK_TIME_LIMIT_SEC,
    # Ack on completion (not receipt) so a worker crash/reconnect mid-task
    # redelivers the task instead of losing it (prevents chord deadlocks on
    # long transcodes). Prefetch=1 so one worker never hoards queued tasks.
    task_acks_late=True,
    task_acks_on_failure_or_timeout=True,
    task_reject_on_worker_lost=True,
    # With late acknowledgements, work is re-delivered after a broker
    # connection loss. Stop the now-unacknowledgeable local copy immediately
    # instead of letting it keep the worker alive and duplicate expensive work.
    worker_cancel_long_running_tasks_on_connection_loss=True,
    worker_prefetch_multiplier=1,
    # RabbitMQ 4.3 no longer permits the transient, non-exclusive reply
    # queues used by Celery's legacy pidbox remote-control channel. Pipeline
    # execution, chords, late acknowledgements, and retries do not depend on
    # pidbox, so keep it disabled instead of opting back into a broker feature
    # that is scheduled for removal.
    worker_enable_remote_control=False,
    broker_heartbeat=30,
    broker_connection_retry=True,
    broker_connection_retry_on_startup=True,
    result_expires=3600 * 24,
    task_routes={
        # Explicit per-task routes (win over the module wildcards below).
        "app.tasks.transcode_video.transcode_group": {"queue": "video"},
        "app.tasks.transcode_video.transcode_chunk": {"queue": "video"},
        "app.tasks.transcode_video.merge_chunk_results": {"queue": "package"},
        "app.tasks.transcode_video.concat_segments": {"queue": "package"},
        "app.tasks.pipeline.*": {"queue": "pipeline"},
        "app.tasks.probe.*": {"queue": "probe"},
        "app.tasks.extract_audio.*": {"queue": "audio"},
        "app.tasks.extract_subtitles.*": {"queue": "subtitle"},
        "app.tasks.transcode_video.*": {"queue": "video"},
        "app.tasks.package.*": {"queue": "package"},
        "app.tasks.workspace_cleanup.*": {"queue": "package"},
        "app.tasks.thumbnail.*": {"queue": "thumbnail"},
    },
    task_default_queue="celery",
)


@worker_shutdown.connect
def unregister_gpu_worker_on_shutdown(**_kwargs) -> None:
    """Remove this GPU worker's exact registration after Celery drains."""
    if os.environ.get("GPU_WORKER_ROLE", "").lower() != "true":
        return
    stop_file = os.environ.get("GPU_REGISTRATION_STOP_FILE")
    if stop_file:
        try:
            with open(stop_file, "w", encoding="utf-8") as marker:
                marker.write("stopping\n")
        except OSError:
            logger.warning(
                "GPU worker shutdown could not signal registration helper",
                exc_info=True,
            )
    worker_id = settings.GPU_WORKER_ID or socket.gethostname()
    registration_id = os.environ.get("GPU_REGISTRATION_ID") or None
    if not gpu_registry.unregister_worker(worker_id, registration_id):
        logger.warning(
            "GPU worker %s shutdown could not remove its bounded "
            "registration because Redis was unavailable",
            worker_id,
        )
