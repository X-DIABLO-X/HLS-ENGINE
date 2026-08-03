"""RabbitMQ event consumer that triggers transcoding jobs."""

import json
import logging
import time

from kombu import Connection, Queue
from kombu.mixins import ConsumerMixin

from app import models, progress as progress_tracker
from app.celery_app import celery_app
from app.config import get_settings
from app.db import SessionLocal
from app.ingestion import ingestion_job_id, minio_source_url
from app.tasks.pipeline import run_pipeline

logger = logging.getLogger(__name__)


def _parse_event(body):
    if isinstance(body, (dict, list)):
        return body
    if isinstance(body, (str, bytes, bytearray)):
        return json.loads(body)
    return body


def _prepare_ingestion_job(video_id: str, source_url: str, job_id: str):
    """Persist the source and return the idempotent ingestion Job state.

    Locking the video serializes duplicate deliveries for the same upload.
    The deterministic Job primary key provides a second idempotency barrier if
    multiple consumer replicas receive the same message.
    """
    db = SessionLocal()
    try:
        video = (
            db.query(models.Video)
            .filter(models.Video.id == video_id)
            .with_for_update()
            .first()
        )
        if not video:
            return None

        # DELETE commits this durable tombstone before removing media. A late
        # or redelivered upload event must not recreate a job or resurrect the
        # video after cleanup has started.
        if video.status == "deleting":
            return {
                "created": False,
                "status": "deleting",
                "dispatch_count": 1,
            }

        job = db.query(models.Job).filter(models.Job.id == job_id).first()
        created = job is None
        if created:
            job = models.Job(
                id=job_id,
                video_id=video_id,
                status=models.JobStatus.pending.value,
                input_path=source_url,
            )
            db.add(job)
        elif str(job.video_id) != str(video_id):
            raise ValueError(
                f"Ingestion job {job_id} belongs to video {job.video_id}, not {video_id}"
            )

        # Metadata-created videos do not initially have source_url. Persisting
        # the immutable MinIO object URL makes watchdog and manual retry able
        # to reconstruct the same input after a worker restart.
        video.source_url = source_url
        if job.status not in (
            models.JobStatus.completed.value,
            models.JobStatus.failed.value,
        ) and video.status != "ready":
            video.status = "processing"

        state = {
            "created": created,
            "status": job.status or models.JobStatus.pending.value,
            "dispatch_count": int(job.dispatch_count or 0),
        }
        db.commit()
        return state
    finally:
        db.close()


def _mark_ingestion_job_queued(job_id: str) -> None:
    """Record successful broker submission without regressing a running job."""
    db = SessionLocal()
    try:
        (
            db.query(models.Job)
            .filter(
                models.Job.id == job_id,
                models.Job.status == models.JobStatus.pending.value,
                models.Job.dispatch_count == 0,
            )
            .update({models.Job.status: models.JobStatus.queued.value})
        )
        db.commit()
    finally:
        db.close()


class UploadEventConsumer(ConsumerMixin):
    def __init__(self, connection):
        self.connection = connection
        self.settings = get_settings()

    def get_consumers(self, Consumer, channel):
        return [
            Consumer(
                queues=[Queue("upload.completed", durable=True)],
                callbacks=[self.on_upload_completed],
                prefetch_count=1,
            )
        ]

    def on_upload_completed(self, body, message):
        try:
            event = _parse_event(body)
            if not isinstance(event, dict):
                raise ValueError("upload.completed body must be a JSON object")

            video_id = event.get("video_id")
            object_name = event.get("object_name")
            bucket = event.get("bucket") or self.settings.MINIO_RAW_BUCKET
            if not video_id or not object_name:
                logger.warning("upload.completed missing video_id or object_name: %s", event)
                message.ack()
                return

            source_url = minio_source_url(bucket, object_name)
            job_id = ingestion_job_id(video_id, bucket, object_name)
            state = _prepare_ingestion_job(video_id, source_url, job_id)
            if state is None:
                logger.warning(
                    "upload.completed for unknown video=%s; skipping pipeline "
                    "(video record missing — create the video before uploading)",
                    video_id,
                )
                message.ack()
                return

            # A duplicate delivery for a queued/running/terminal ingestion job
            # is already durably represented and must not create more work.
            should_dispatch = state["created"] or (
                state["status"] == models.JobStatus.pending.value
                and state["dispatch_count"] == 0
            )
            if not should_dispatch:
                logger.info(
                    "upload.completed duplicate for video=%s job=%s status=%s; acknowledging",
                    video_id,
                    job_id,
                    state["status"],
                )
                message.ack()
                return

            settings = progress_tracker.get_default_settings()
            logger.info(
                "triggering pipeline for video=%s job=%s source=%s",
                video_id,
                job_id,
                source_url,
            )
            run_pipeline.delay(
                job_id=job_id,
                source_url=source_url,
                video_id=video_id,
                version="v1",
                settings=settings,
            )
            try:
                _mark_ingestion_job_queued(job_id)
            except Exception as exc:
                # The Celery publish already succeeded. Requeuing solely for
                # this advisory update would risk duplicate pipeline messages.
                logger.warning("failed to mark ingestion job=%s queued: %s", job_id, exc)
            message.ack()
        except Exception as exc:
            logger.exception("failed to handle upload.completed: %s", exc)
            message.requeue()


def main():
    logging.basicConfig(level=logging.INFO)
    settings = get_settings()
    _ = celery_app

    # Start the stuck-job watchdog in this long-running process so it survives
    # Celery worker restarts.
    try:
        from app.main import start_watchdog

        start_watchdog()
    except Exception as exc:
        logger.warning("could not start watchdog: %s", exc)

    # The scheduler only publishes durable maintenance tasks. Cleanup itself
    # runs on a worker that owns the shared hls-work volume and is fenced by
    # the same per-job locks as every media mutator.
    try:
        from app.tasks.workspace_cleanup import (
            start_workspace_reaper_scheduler,
        )

        start_workspace_reaper_scheduler()
    except Exception as exc:
        logger.warning("could not start workspace reaper scheduler: %s", exc)

    while True:
        try:
            with Connection(settings.RABBITMQ_URL, heartbeat=4) as conn:
                logger.info("upload event consumer starting")
                consumer = UploadEventConsumer(conn)
                consumer.run()
        except Exception as exc:
            logger.exception("event consumer crashed: %s", exc)
            time.sleep(5)


if __name__ == "__main__":
    main()
