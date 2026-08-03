import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import redis
from fastapi import HTTPException

from app import main, models, progress
from app.tasks import pipeline


class _ExpiringVideo:
    def __init__(self, source_url):
        self.id = "video-1"
        self.status = "failed"
        self._source_url = source_url
        self.expired = False

    @property
    def source_url(self):
        if self.expired:
            raise AssertionError("detached Video.source_url was accessed")
        return self._source_url

    @source_url.setter
    def source_url(self, value):
        self._source_url = value


class _RetryQuery:
    def __init__(self, model, video, source_job=None):
        self.model = model
        self.video = video
        self.source_job = source_job

    def filter(self, *args):
        return self

    def order_by(self, *args):
        return self

    def first(self):
        if self.model is models.Video:
            return self.video
        if self.model is models.Job:
            return self.source_job
        return None

    def delete(self):
        return 0

    def update(self, values):
        return 0


class _RetrySession:
    def __init__(self, video, source_job=None):
        self.video = video
        self.source_job = source_job
        self.added = []

    def query(self, model):
        return _RetryQuery(model, self.video, self.source_job)

    def add(self, value):
        self.added.append(value)

    def commit(self):
        # Mirrors expire_on_commit=True closely enough to catch any later ORM
        # attribute access in the endpoint.
        self.video.expired = True

    def close(self):
        pass


class RetryEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_retry_cannot_resurrect_deleting_video(self):
        video = _ExpiringVideo(
            "minio://uploads-raw/raw/video-1/source.mp4"
        )
        video.status = "deleting"
        session = _RetrySession(video)

        with patch.object(main, "SessionLocal", return_value=session):
            with self.assertRaises(HTTPException) as raised:
                await main.retry_video(video.id)

        self.assertEqual(raised.exception.status_code, 409)
        self.assertEqual(session.added, [])
        self.assertEqual(video.status, "deleting")

    async def test_dispatch_uses_source_captured_before_commit(self):
        source_url = "minio://uploads-raw/raw/video-1/source.mp4"
        video = _ExpiringVideo(source_url)
        session = _RetrySession(video)
        redis_client = Mock()

        with (
            patch.object(main, "SessionLocal", return_value=session),
            patch.object(progress, "get_default_settings", return_value={}),
            patch.object(pipeline.run_pipeline, "delay") as delay,
            patch.object(redis, "from_url", return_value=redis_client),
            patch.object(
                main,
                "get_settings",
                return_value=SimpleNamespace(REDIS_URL="redis://unused"),
            ),
            patch.object(main, "transcode_jobs_total") as jobs_total,
        ):
            jobs_total.labels.return_value = jobs_total
            result = await main.retry_video(video.id)

        self.assertEqual(result["video_id"], video.id)
        self.assertEqual(len(session.added), 1)
        self.assertEqual(session.added[0].input_path, source_url)
        self.assertEqual(delay.call_args.kwargs["source_url"], source_url)

    async def test_retry_recovers_source_from_latest_job(self):
        source_url = "minio://uploads-raw/raw/video-1/legacy-source.mkv"
        video = _ExpiringVideo(None)
        source_job = SimpleNamespace(input_path=source_url)
        session = _RetrySession(video, source_job)
        redis_client = Mock()

        with (
            patch.object(main, "SessionLocal", return_value=session),
            patch.object(progress, "get_default_settings", return_value={}),
            patch.object(pipeline.run_pipeline, "delay") as delay,
            patch.object(redis, "from_url", return_value=redis_client),
            patch.object(
                main,
                "get_settings",
                return_value=SimpleNamespace(REDIS_URL="redis://unused"),
            ),
            patch.object(main, "transcode_jobs_total") as jobs_total,
        ):
            jobs_total.labels.return_value = jobs_total
            result = await main.retry_video(video.id)

        self.assertEqual(result["video_id"], video.id)
        self.assertEqual(video._source_url, source_url)
        self.assertEqual(session.added[0].input_path, source_url)
        self.assertEqual(delay.call_args.kwargs["source_url"], source_url)


if __name__ == "__main__":
    unittest.main()
