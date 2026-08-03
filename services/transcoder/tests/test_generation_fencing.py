import asyncio
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import redis

from app import main, models, progress
from app.job_fencing import StaleJobError, lock_current_job
from app.tasks import package as package_module
from app.tasks import pipeline, thumbnail as thumbnail_module


class _FirstQuery:
    def __init__(self, value):
        self.value = value

    def filter(self, *_args):
        return self

    def order_by(self, *_args):
        return self

    def with_for_update(self):
        return self

    def first(self):
        return self.value


class _SequenceSession:
    def __init__(self, video, jobs):
        self.video = video
        self.jobs = iter(jobs)

    def query(self, model):
        if model is models.Video:
            return _FirstQuery(self.video)
        if model is models.Job:
            return _FirstQuery(next(self.jobs))
        raise AssertionError(f"unexpected model {model}")


class GenerationAuthorityTests(unittest.TestCase):
    def test_old_job_is_rejected_when_new_generation_exists(self):
        video = SimpleNamespace(id="video-1", status="processing")
        old = SimpleNamespace(
            id="job-old",
            video_id=video.id,
            status=models.JobStatus.transcoding.value,
        )
        new = SimpleNamespace(
            id="job-new",
            video_id=video.id,
            status=models.JobStatus.pending.value,
        )
        db = _SequenceSession(video, [old, old, new])

        with self.assertRaisesRegex(StaleJobError, "not the current"):
            lock_current_job(db, old.id)

    def test_delayed_errback_cannot_fail_ready_video(self):
        job = SimpleNamespace(
            id="job-1",
            video_id="video-1",
            status=models.JobStatus.completed.value,
            error_message=None,
        )
        video = SimpleNamespace(id="video-1", status="ready")
        db = Mock()

        with (
            patch.object(pipeline, "SessionLocal", return_value=db),
            patch.object(
                pipeline,
                "lock_current_job",
                return_value=(job, video),
            ),
            patch.object(
                pipeline.progress_tracker,
                "set_percent",
            ) as set_percent,
            patch.object(pipeline, "publish_event") as publish_event,
        ):
            pipeline.on_pipeline_failure.run(video.id, job.id)

        self.assertEqual(job.status, models.JobStatus.completed.value)
        self.assertEqual(video.status, "ready")
        db.commit.assert_not_called()
        set_percent.assert_not_called()
        publish_event.assert_not_called()

    def test_late_header_after_package_is_a_harmless_success(self):
        with tempfile.TemporaryDirectory() as work_root:
            with (
                patch.object(
                    thumbnail_module,
                    "get_settings",
                    return_value=SimpleNamespace(WORK_DIR=work_root),
                ),
                patch.object(
                    thumbnail_module,
                    "lock_current_job",
                    side_effect=StaleJobError("already completed"),
                ),
                patch.object(
                    thumbnail_module,
                    "SessionLocal",
                    return_value=Mock(),
                ),
                patch.object(
                    thumbnail_module,
                    "ensure_local_source",
                ) as ensure_source,
                patch.object(
                    thumbnail_module.ffmpeg_utils,
                    "run_cmd",
                ) as run_cmd,
            ):
                result = thumbnail_module.thumbnail.run(
                    "job-old",
                    "minio://uploads/source.mkv",
                    {},
                )

        self.assertTrue(result["skipped"])
        self.assertTrue(result["superseded"])
        ensure_source.assert_not_called()
        run_cmd.assert_not_called()


class _PipelineSession:
    def __init__(self, job, *, fail_commit=False):
        self.job = job
        self.fail_commit = fail_commit
        self.rollbacks = 0

    def commit(self):
        if self.fail_commit:
            self.job.dispatch_count = 0
            self.job.status = models.JobStatus.probing.value
            raise OSError("post-send commit failed")

    def rollback(self):
        self.rollbacks += 1
        self.job.dispatch_count = 0
        self.job.status = models.JobStatus.probing.value

    def close(self):
        pass


class PipelineDispatchTests(unittest.TestCase):
    def test_post_send_commit_failure_redelivery_is_harmless(self):
        job = SimpleNamespace(
            id="job-1",
            video_id="video-1",
            status=models.JobStatus.pending.value,
            dispatch_count=0,
            input_path=None,
        )
        video = SimpleNamespace(
            id="video-1",
            status="pending",
        )
        failed_dispatch_db = _PipelineSession(job, fail_commit=True)
        successful_dispatch_db = _PipelineSession(job)
        sessions = [
            _PipelineSession(job),
            _PipelineSession(job),
            failed_dispatch_db,
            _PipelineSession(job),
            _PipelineSession(job),
            successful_dispatch_db,
        ]
        dispatched = Mock()
        chord_factory = Mock(return_value=dispatched)
        probe_result = {
            "duration": 60.0,
            "height": 1080,
            "width": 1920,
            "audio_tracks": [],
            "subtitle_tracks": [],
        }
        rendition = {
            "height": 720,
            "width": 1280,
            "bitrate": 2_000_000,
            "codec": "h264",
        }

        with (
            patch.object(
                pipeline,
                "SessionLocal",
                side_effect=sessions,
            ),
            patch.object(
                pipeline,
                "lock_current_job",
                side_effect=lambda *_args, **_kwargs: (job, video),
            ),
            patch.object(
                pipeline,
                "_run_probe_sync",
                return_value=probe_result,
            ),
            patch.object(
                pipeline,
                "ensure_local_source",
                return_value="/work/source.mp4",
            ),
            patch.object(
                pipeline.ffmpeg_utils,
                "get_ladder_for_qualities",
                return_value=[rendition],
            ),
            patch.object(
                pipeline.gpu_registry,
                "get_gpu_status",
                return_value=[],
            ),
            patch.object(pipeline, "publish_event"),
            patch.object(pipeline, "chord", chord_factory),
            patch.object(pipeline.progress_tracker, "init_progress"),
            patch.object(pipeline.progress_tracker, "complete_task"),
        ):
            with self.assertRaisesRegex(OSError, "post-send"):
                pipeline._run_pipeline(
                    Mock(),
                    job.id,
                    "minio://uploads/source.mkv",
                    video.id,
                    renditions=[rendition],
                    settings={
                        "per_title_encoding": False,
                        "chunked_encoding": False,
                    },
                )
            result = pipeline._run_pipeline(
                Mock(),
                job.id,
                "minio://uploads/source.mkv",
                video.id,
                renditions=[rendition],
                settings={
                    "per_title_encoding": False,
                    "chunked_encoding": False,
                },
            )

        self.assertEqual(result["status"], "dispatched")
        self.assertEqual(dispatched.apply_async.call_count, 2)
        self.assertEqual(job.dispatch_count, 1)
        self.assertEqual(failed_dispatch_db.rollbacks, 1)


class _RetryStore:
    def __init__(self):
        self.lock = threading.Lock()
        self.video = SimpleNamespace(
            id="video-1",
            status="failed",
            source_url="minio://uploads/source.mkv",
        )
        self.jobs = []


class _RetryQuery:
    def __init__(self, session, model):
        self.session = session
        self.model = model

    def filter(self, *_args):
        return self

    def order_by(self, *_args):
        return self

    def with_for_update(self):
        if self.model is models.Video and not self.session.locked:
            self.session.store.lock.acquire()
            self.session.locked = True
        return self

    def first(self):
        if self.model is models.Video:
            return self.session.store.video
        if self.model is models.Job:
            return self.session.store.jobs[-1] if self.session.store.jobs else None
        return None

    def delete(self):
        return 0

    def update(self, values, **_kwargs):
        count = 0
        for job in self.session.store.jobs:
            if job.status in {
                models.JobStatus.pending.value,
                models.JobStatus.queued.value,
                models.JobStatus.probing.value,
                models.JobStatus.extracting.value,
                models.JobStatus.transcoding.value,
                models.JobStatus.packaging.value,
                models.JobStatus.publishing.value,
            }:
                job.status = values[models.Job.status]
                job.error_message = values[models.Job.error_message]
                count += 1
        return count


class _RetrySession:
    def __init__(self, store):
        self.store = store
        self.locked = False

    def query(self, model):
        return _RetryQuery(self, model)

    def add(self, value):
        self.store.jobs.append(value)

    def commit(self):
        if self.locked:
            self.store.lock.release()
            self.locked = False

    def close(self):
        if self.locked:
            self.store.lock.release()
            self.locked = False


class _RetryRedis:
    def __init__(self):
        self.lock = threading.Lock()
        self.data = {}

    def eval(self, _script, _num_keys, _key, *args):
        with self.lock:
            self.data = {
                str(args[index]): str(args[index + 1])
                for index in range(0, len(args), 2)
            }
        return 1


class ConcurrentRetryTests(unittest.TestCase):
    def test_concurrent_retries_leave_only_newest_generation_active(self):
        store = _RetryStore()
        barrier = threading.Barrier(2)
        results = []
        errors = []
        redis_client = _RetryRedis()

        def invoke():
            try:
                barrier.wait(5)
                results.append(asyncio.run(main.retry_video(store.video.id)))
            except BaseException as exc:
                errors.append(exc)

        with (
            patch.object(
                main,
                "SessionLocal",
                side_effect=lambda: _RetrySession(store),
            ),
            patch.object(
                progress,
                "get_default_settings",
                return_value={},
            ),
            patch.object(
                pipeline.run_pipeline,
                "delay",
            ) as delay,
            patch.object(redis, "from_url", return_value=redis_client),
            patch.object(
                main,
                "get_settings",
                return_value=SimpleNamespace(REDIS_URL="redis://unused"),
            ),
            patch.object(main, "transcode_jobs_total") as jobs_total,
        ):
            jobs_total.labels.return_value = jobs_total
            threads = [
                threading.Thread(target=invoke),
                threading.Thread(target=invoke),
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(5)

        self.assertEqual(errors, [])
        self.assertEqual(len(results), 2)
        self.assertEqual(delay.call_count, 2)
        active = [
            job
            for job in store.jobs
            if job.status != models.JobStatus.failed.value
        ]
        self.assertEqual(len(active), 1)
        superseded = [
            job
            for job in store.jobs
            if job.status == models.JobStatus.failed.value
        ]
        self.assertEqual(len(superseded), 1)
        self.assertIn("manual retry", superseded[0].error_message)
        self.assertEqual(redis_client.data["job_id"], str(active[0].id))


class PackageVideoLockTests(unittest.TestCase):
    def test_different_job_ids_for_one_video_cannot_publish_together(self):
        entered_first = threading.Event()
        release_first = threading.Event()
        entered_second = threading.Event()
        errors = []

        def session_factory():
            job_id = threading.current_thread().name
            job = SimpleNamespace(id=job_id, video_id="video-shared")
            return SimpleNamespace(
                query=lambda _model: _FirstQuery(job),
                close=lambda: None,
            )

        def fake_package(_self, _results, job_id, *_args, **_kwargs):
            if job_id == "job-a":
                entered_first.set()
                if not release_first.wait(5):
                    raise AssertionError("first package was not released")
            else:
                entered_second.set()
            return {"job_id": job_id}

        def invoke(job_id):
            try:
                package_module.package.run(
                    [],
                    job_id,
                    "minio://uploads/source.mkv",
                    "v1",
                )
            except BaseException as exc:
                errors.append(exc)

        with tempfile.TemporaryDirectory() as work_root:
            with (
                patch.object(
                    package_module,
                    "get_settings",
                    return_value=SimpleNamespace(WORK_DIR=work_root),
                ),
                patch.object(
                    package_module,
                    "SessionLocal",
                    side_effect=session_factory,
                ),
                patch.object(
                    package_module,
                    "_run_package",
                    side_effect=fake_package,
                ),
            ):
                first = threading.Thread(
                    target=invoke,
                    args=("job-a",),
                    name="job-a",
                )
                second = threading.Thread(
                    target=invoke,
                    args=("job-b",),
                    name="job-b",
                )
                first.start()
                self.assertTrue(entered_first.wait(5))
                second.start()
                self.assertFalse(entered_second.wait(0.25))
                release_first.set()
                first.join(5)
                second.join(5)

        self.assertEqual(errors, [])
        self.assertTrue(entered_second.is_set())


if __name__ == "__main__":
    unittest.main()
