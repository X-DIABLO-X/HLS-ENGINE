import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from app import event_consumer, models


class _Store:
    def __init__(self):
        self.video = SimpleNamespace(
            id="video-1",
            source_url=None,
            status="uploading",
        )
        self.jobs = {}
        self.commits = 0


class _Query:
    def __init__(self, store, model):
        self.store = store
        self.model = model

    def filter(self, *args):
        return self

    def with_for_update(self):
        return self

    def first(self):
        if self.model is models.Video:
            return self.store.video
        if self.model is models.Job:
            return next(iter(self.store.jobs.values()), None)
        raise AssertionError(f"unexpected model: {self.model}")


class _Session:
    def __init__(self, store):
        self.store = store

    def query(self, model):
        return _Query(self.store, model)

    def add(self, value):
        if isinstance(value, models.Job):
            self.store.jobs[str(value.id)] = value

    def commit(self):
        self.store.commits += 1

    def close(self):
        pass


class UploadIngestionTests(unittest.TestCase):
    def test_duplicate_delivery_reuses_one_job_and_persists_source(self):
        store = _Store()
        source_url = "minio://uploads-raw/raw/video-1/source.mp4"
        job_id = "cd73420a-cafe-57a2-b680-522786e1889d"

        with patch.object(event_consumer, "SessionLocal", side_effect=lambda: _Session(store)):
            first = event_consumer._prepare_ingestion_job(
                store.video.id, source_url, job_id
            )
            second = event_consumer._prepare_ingestion_job(
                store.video.id, source_url, job_id
            )

        self.assertTrue(first["created"])
        self.assertFalse(second["created"])
        self.assertEqual(len(store.jobs), 1)
        self.assertEqual(store.video.source_url, source_url)
        self.assertEqual(store.video.status, "processing")

    def test_late_upload_event_cannot_resurrect_deleting_video(self):
        store = _Store()
        store.video.status = "deleting"
        source_url = "minio://uploads-raw/raw/video-1/source.mp4"

        with patch.object(
            event_consumer,
            "SessionLocal",
            side_effect=lambda: _Session(store),
        ):
            state = event_consumer._prepare_ingestion_job(
                store.video.id,
                source_url,
                "cd73420a-cafe-57a2-b680-522786e1889d",
            )

        self.assertEqual(state["status"], "deleting")
        self.assertFalse(state["created"])
        self.assertEqual(store.jobs, {})
        self.assertIsNone(store.video.source_url)
        self.assertEqual(store.video.status, "deleting")

    def test_queued_duplicate_is_acknowledged_without_second_dispatch(self):
        consumer = object.__new__(event_consumer.UploadEventConsumer)
        consumer.settings = SimpleNamespace(MINIO_RAW_BUCKET="uploads-raw")
        first_message = Mock()
        duplicate_message = Mock()
        event = {
            "video_id": "video-1",
            "bucket": "uploads-raw",
            "object_name": "raw/video-1/source.mp4",
        }

        states = [
            {"created": True, "status": "pending", "dispatch_count": 0},
            {"created": False, "status": "queued", "dispatch_count": 0},
        ]
        with (
            patch.object(event_consumer, "_prepare_ingestion_job", side_effect=states),
            patch.object(event_consumer, "_mark_ingestion_job_queued"),
            patch.object(
                event_consumer.progress_tracker,
                "get_default_settings",
                return_value={},
            ),
            patch.object(event_consumer.run_pipeline, "delay") as delay,
        ):
            consumer.on_upload_completed(event, first_message)
            consumer.on_upload_completed(event, duplicate_message)

        delay.assert_called_once()
        first_message.ack.assert_called_once()
        duplicate_message.ack.assert_called_once()
        duplicate_message.requeue.assert_not_called()

    def test_broker_failure_requeues_pending_ingestion(self):
        consumer = object.__new__(event_consumer.UploadEventConsumer)
        consumer.settings = SimpleNamespace(MINIO_RAW_BUCKET="uploads-raw")
        message = Mock()
        event = {
            "video_id": "video-1",
            "object_name": "raw/video-1/source.mp4",
        }

        state = {"created": True, "status": "pending", "dispatch_count": 0}
        with (
            patch.object(event_consumer, "_prepare_ingestion_job", return_value=state),
            patch.object(
                event_consumer.progress_tracker,
                "get_default_settings",
                return_value={},
            ),
            patch.object(
                event_consumer.run_pipeline,
                "delay",
                side_effect=RuntimeError("broker unavailable"),
            ),
            patch.object(event_consumer.logger, "exception"),
        ):
            consumer.on_upload_completed(event, message)

        message.ack.assert_not_called()
        message.requeue.assert_called_once()


if __name__ == "__main__":
    unittest.main()
