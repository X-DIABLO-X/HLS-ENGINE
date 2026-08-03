import os
import shutil
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

from app import models
import app.tasks.package as package_module


class _Query:
    def __init__(self, rows):
        self.rows = list(rows)

    def filter(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def first(self):
        return self.rows[0] if self.rows else None

    def all(self):
        return list(self.rows)


class _FakeDb:
    def __init__(self, rows, commit_hook=None):
        self.rows = rows
        self.commit_hook = commit_hook
        self.commits = 0
        self.closed = False

    def query(self, model):
        return _Query(self.rows.get(model, []))

    def commit(self):
        self.commits += 1
        if self.commit_hook:
            self.commit_hook(self)

    def close(self):
        self.closed = True


def _job(status, output_prefix=None):
    return SimpleNamespace(
        id="job-id",
        video_id="ab-video-id",
        status=status,
        output_prefix=output_prefix,
        progress=0.0,
    )


def _video(status):
    return SimpleNamespace(
        id="ab-video-id",
        status=status,
        duration=60.0,
    )


class PackageCompletionTests(unittest.TestCase):
    def test_completed_work_cleanup_is_confined_to_work_root(self):
        with tempfile.TemporaryDirectory() as parent:
            work_root = os.path.join(parent, "work")
            child = os.path.join(work_root, "job-id")
            outside = os.path.join(parent, "outside")
            os.makedirs(child)
            os.makedirs(outside)

            with patch.object(
                package_module,
                "get_settings",
                return_value=SimpleNamespace(WORK_DIR=work_root),
            ):
                package_module._remove_completed_work_dir(child)
                package_module._remove_completed_work_dir(outside)

            self.assertFalse(os.path.exists(child))
            self.assertTrue(os.path.isdir(outside))

    def test_completed_cleanup_refuses_symlink_to_sibling_workspace(self):
        with tempfile.TemporaryDirectory() as parent:
            work_root = os.path.join(parent, "work")
            target = os.path.join(work_root, "other-job")
            link = os.path.join(work_root, "completed-job")
            os.makedirs(target)
            marker = os.path.join(target, "source.mkv")
            with open(marker, "wb") as handle:
                handle.write(b"keep")
            try:
                os.symlink(target, link, target_is_directory=True)
            except (OSError, NotImplementedError):
                self.skipTest("directory symlinks are unavailable")

            with patch.object(
                package_module,
                "get_settings",
                return_value=SimpleNamespace(WORK_DIR=work_root),
            ):
                package_module._remove_completed_work_dir(link)

            self.assertTrue(os.path.isfile(marker))
            self.assertTrue(os.path.islink(link))

    def test_delivery_cache_is_reseeded_as_one_transaction(self):
        client = MagicMock()
        pipeline = client.pipeline.return_value
        with (
            patch.object(
                package_module,
                "get_settings",
                return_value=SimpleNamespace(
                    REDIS_URL="redis://cache/0"
                ),
            ),
            patch.object(
                package_module.redis,
                "from_url",
                return_value=client,
            ),
        ):
            package_module._cache_published_version(
                "video-id", "v2", "ab/video-id/v2/"
            )

        client.pipeline.assert_called_once_with(transaction=True)
        self.assertEqual(
            pipeline.set.call_args_list,
            [
                call(
                    "video:video-id:version",
                    "v2",
                    ex=60 * 60 * 24 * 30,
                ),
                call(
                    "video:video-id:prefix",
                    "ab/video-id/v2/",
                    ex=60 * 60 * 24 * 30,
                ),
            ],
        )
        pipeline.execute.assert_called_once_with()

    def test_completed_ready_redelivery_reseeds_without_local_files(self):
        job = _job(
            models.JobStatus.completed.value,
            "ab/ab-video-id/v1/",
        )
        video = _video("ready")
        db = _FakeDb(
            {
                models.Job: [job],
                models.Video: [video],
            }
        )

        with tempfile.TemporaryDirectory() as work_root:
            with (
                patch.object(package_module, "SessionLocal", return_value=db),
                patch.object(
                    package_module,
                    "get_settings",
                    return_value=SimpleNamespace(WORK_DIR=work_root),
                ),
                patch.object(
                    package_module,
                    "published_prefix_exists",
                    return_value=True,
                ) as marker,
                patch.object(
                    package_module,
                    "_cache_published_version",
                ) as cache,
                patch.object(
                    package_module.progress_tracker,
                    "complete_task",
                    side_effect=OSError("redis unavailable"),
                ),
                patch.object(
                    package_module.progress_tracker,
                    "set_percent",
                    side_effect=OSError("redis unavailable"),
                ),
                patch.object(
                    package_module,
                    "atomic_publish",
                ) as atomic_publish,
            ):
                result = package_module.package.run(
                    [],
                    "job-id",
                    "minio://uploads/source.mkv",
                    "v1",
                )

        self.assertTrue(result["reused"])
        marker.assert_called_once_with("ab/ab-video-id/v1/")
        cache.assert_called_once_with(
            "ab-video-id", "v1", "ab/ab-video-id/v1/"
        )
        atomic_publish.assert_not_called()
        self.assertEqual(db.commits, 0)
        self.assertTrue(db.closed)

    def test_completed_redeliveries_reuse_after_job_directory_cleanup(self):
        job = _job(
            models.JobStatus.completed.value,
            "ab/ab-video-id/v1/",
        )
        video = _video("ready")
        first_db = _FakeDb(
            {
                models.Job: [job],
                models.Video: [video],
            }
        )
        second_db = _FakeDb(
            {
                models.Job: [job],
                models.Video: [video],
            }
        )

        with tempfile.TemporaryDirectory() as work_root:
            # This is the state after a successful initial package cleanup.
            self.assertFalse(
                os.path.exists(os.path.join(work_root, "job-id"))
            )
            with (
                patch.object(
                    package_module,
                    "SessionLocal",
                    side_effect=[first_db, second_db],
                ),
                patch.object(
                    package_module,
                    "get_settings",
                    return_value=SimpleNamespace(WORK_DIR=work_root),
                ),
                patch.object(
                    package_module,
                    "published_prefix_exists",
                    return_value=True,
                ) as marker,
                patch.object(
                    package_module,
                    "_cache_published_version",
                ),
                patch.object(
                    package_module.progress_tracker,
                    "complete_task",
                ),
                patch.object(
                    package_module.progress_tracker,
                    "set_percent",
                ),
                patch.object(
                    package_module,
                    "atomic_publish",
                ) as atomic_publish,
            ):
                first = package_module.package.run(
                    [],
                    "job-id",
                    "minio://uploads/source.mkv",
                    "v1",
                )
                second = package_module.package.run(
                    [],
                    "job-id",
                    "minio://uploads/source.mkv",
                    "v1",
                )

            lock_dir = os.path.join(work_root, ".job-locks")
            self.assertTrue(os.path.isdir(lock_dir))
            # Per-job targets are reclaimed without unlinking a queued
            # waiter's inode; only the namespace coordinator persists.
            self.assertEqual(os.listdir(lock_dir), [".namespace.lock"])
            self.assertFalse(
                os.path.exists(os.path.join(work_root, "job-id"))
            )

        self.assertTrue(first["reused"])
        self.assertTrue(second["reused"])
        self.assertEqual(marker.call_count, 2)
        atomic_publish.assert_not_called()
        self.assertTrue(first_db.closed)
        self.assertTrue(second_db.closed)

    def test_completed_job_without_durable_marker_fails_closed(self):
        job = _job(
            models.JobStatus.completed.value,
            "ab/ab-video-id/v1/",
        )
        video = _video("ready")
        db = _FakeDb(
            {
                models.Job: [job],
                models.Video: [video],
            }
        )

        with tempfile.TemporaryDirectory() as work_root:
            work_dir = os.path.join(work_root, "job-id")
            os.makedirs(work_dir)
            diagnostic = os.path.join(work_dir, "ffmpeg.log")
            with open(diagnostic, "w", encoding="utf-8") as handle:
                handle.write("diagnostic")

            with (
                patch.object(package_module, "SessionLocal", return_value=db),
                patch.object(
                    package_module,
                    "get_settings",
                    return_value=SimpleNamespace(WORK_DIR=work_root),
                ),
                patch.object(
                    package_module,
                    "published_prefix_exists",
                    return_value=False,
                ),
                patch.object(
                    package_module,
                    "_cache_published_version",
                ) as cache,
                patch.object(
                    package_module,
                    "atomic_publish",
                ) as atomic_publish,
            ):
                with self.assertRaisesRegex(
                    RuntimeError, "missing its master commit marker"
                ):
                    package_module.package.run(
                        [],
                        "job-id",
                        "minio://uploads/source.mkv",
                        "v1",
                    )

            self.assertTrue(os.path.isfile(diagnostic))
            cache.assert_not_called()
            atomic_publish.assert_not_called()

    def test_terminal_state_without_output_prefix_fails_closed(self):
        job = _job(models.JobStatus.completed.value, None)
        video = _video("ready")
        db = _FakeDb(
            {
                models.Job: [job],
                models.Video: [video],
            }
        )

        with tempfile.TemporaryDirectory() as work_root:
            with (
                patch.object(package_module, "SessionLocal", return_value=db),
                patch.object(
                    package_module,
                    "get_settings",
                    return_value=SimpleNamespace(WORK_DIR=work_root),
                ),
                patch.object(
                    package_module,
                    "published_prefix_exists",
                ) as marker,
            ):
                with self.assertRaisesRegex(
                    RuntimeError, "inconsistent terminal state"
                ):
                    package_module.package.run(
                        [],
                        "job-id",
                        "minio://uploads/source.mkv",
                        "v1",
                    )

            marker.assert_not_called()

    def test_initial_publish_cleans_only_after_marker_and_ready_commit(self):
        events = []
        job = _job(models.JobStatus.transcoding.value)
        video = _video("processing")

        def record_commit(_db):
            events.append(("commit", job.status, video.status))

        db = _FakeDb(
            {
                models.Job: [job],
                models.Video: [video],
                models.AudioTrack: [],
                models.Subtitle: [],
            },
            commit_hook=record_commit,
        )

        with tempfile.TemporaryDirectory() as work_root:
            work_dir = os.path.join(work_root, "job-id")
            rendition_dir = os.path.join(work_dir, "output", "video_720p")
            os.makedirs(rendition_dir)
            rendition_playlist = os.path.join(rendition_dir, "video.m3u8")
            with open(
                rendition_playlist, "w", encoding="utf-8"
            ) as handle:
                handle.write(
                    "#EXTM3U\n"
                    "#EXTINF:6.000,\nsegment.m4s\n"
                    "#EXT-X-ENDLIST\n"
                )
            rendition = SimpleNamespace(
                bandwidth=1_000_000,
                width=1280,
                height=720,
                codec="h264",
                playlist_path=rendition_playlist,
            )
            db.rows[models.Rendition] = [rendition]

            def publish(_output_dir, prefix):
                events.append(("publish", job.status, video.status))
                return prefix

            def marker(prefix):
                events.append(("marker", job.status, video.status))
                return True

            def cleanup(path):
                events.append(("cleanup", job.status, video.status))
                shutil.rmtree(path)

            with (
                patch.object(package_module, "SessionLocal", return_value=db),
                patch.object(
                    package_module,
                    "get_settings",
                    return_value=SimpleNamespace(WORK_DIR=work_root),
                ),
                patch.object(
                    package_module,
                    "atomic_publish",
                    side_effect=publish,
                ),
                patch.object(
                    package_module,
                    "published_prefix_exists",
                    side_effect=marker,
                ),
                patch.object(
                    package_module,
                    "_remove_completed_work_dir",
                    side_effect=cleanup,
                ),
                patch.object(
                    package_module,
                    "_cache_published_version",
                ),
                patch.object(package_module, "publish_event"),
                patch.object(
                    package_module.progress_tracker, "start_task"
                ),
                patch.object(
                    package_module.progress_tracker, "complete_task"
                ),
                patch.object(
                    package_module.progress_tracker, "set_percent"
                ),
            ):
                result = package_module.package.run(
                    [],
                    "job-id",
                    "minio://uploads/source.mkv",
                    "v1",
                )

            self.assertFalse(os.path.exists(work_dir))

        self.assertEqual(result["output_prefix"], "ab/ab-video-id/v1/")
        marker_index = next(
            index for index, event in enumerate(events)
            if event[0] == "marker"
        )
        ready_commit_index = next(
            index for index, event in enumerate(events)
            if event == (
                "commit",
                models.JobStatus.completed.value,
                "ready",
            )
        )
        cleanup_index = next(
            index for index, event in enumerate(events)
            if event[0] == "cleanup"
        )
        self.assertLess(marker_index, ready_commit_index)
        self.assertLess(ready_commit_index, cleanup_index)

    def test_missing_post_publish_marker_keeps_diagnostics(self):
        job = _job(models.JobStatus.transcoding.value)
        video = _video("processing")
        db = _FakeDb(
            {
                models.Job: [job],
                models.Video: [video],
                models.AudioTrack: [],
                models.Subtitle: [],
            }
        )

        with tempfile.TemporaryDirectory() as work_root:
            work_dir = os.path.join(work_root, "job-id")
            rendition_dir = os.path.join(work_dir, "output", "video_720p")
            os.makedirs(rendition_dir)
            rendition_playlist = os.path.join(rendition_dir, "video.m3u8")
            with open(
                rendition_playlist, "w", encoding="utf-8"
            ) as handle:
                handle.write("#EXTM3U\n")
            db.rows[models.Rendition] = [
                SimpleNamespace(
                    bandwidth=1_000_000,
                    width=1280,
                    height=720,
                    codec="h264",
                    playlist_path=rendition_playlist,
                )
            ]

            with (
                patch.object(package_module, "SessionLocal", return_value=db),
                patch.object(
                    package_module,
                    "get_settings",
                    return_value=SimpleNamespace(WORK_DIR=work_root),
                ),
                patch.object(
                    package_module,
                    "atomic_publish",
                    return_value="ab/ab-video-id/v1/",
                ),
                patch.object(
                    package_module,
                    "published_prefix_exists",
                    return_value=False,
                ),
                patch.object(
                    package_module.progress_tracker, "start_task"
                ),
                patch.object(
                    package_module.progress_tracker, "set_percent"
                ),
            ):
                with self.assertRaisesRegex(
                    RuntimeError, "without a durable master"
                ):
                    package_module.package.run(
                        [],
                        "job-id",
                        "minio://uploads/source.mkv",
                        "v1",
                    )

            self.assertTrue(os.path.isdir(work_dir))
            self.assertNotEqual(
                job.status, models.JobStatus.completed.value
            )
            self.assertNotEqual(video.status, "ready")

    def test_ready_commit_failure_keeps_work_directory(self):
        job = _job(models.JobStatus.transcoding.value)
        video = _video("processing")

        def fail_ready_commit(_db):
            if (
                job.status == models.JobStatus.completed.value
                and video.status == "ready"
            ):
                raise OSError("database commit failed")

        db = _FakeDb(
            {
                models.Job: [job],
                models.Video: [video],
                models.AudioTrack: [],
                models.Subtitle: [],
            },
            commit_hook=fail_ready_commit,
        )

        with tempfile.TemporaryDirectory() as work_root:
            work_dir = os.path.join(work_root, "job-id")
            rendition_dir = os.path.join(work_dir, "output", "video_720p")
            os.makedirs(rendition_dir)
            rendition_playlist = os.path.join(rendition_dir, "video.m3u8")
            with open(
                rendition_playlist, "w", encoding="utf-8"
            ) as handle:
                handle.write("#EXTM3U\n")
            db.rows[models.Rendition] = [
                SimpleNamespace(
                    bandwidth=1_000_000,
                    width=1280,
                    height=720,
                    codec="h264",
                    playlist_path=rendition_playlist,
                )
            ]

            with (
                patch.object(package_module, "SessionLocal", return_value=db),
                patch.object(
                    package_module,
                    "get_settings",
                    return_value=SimpleNamespace(WORK_DIR=work_root),
                ),
                patch.object(
                    package_module,
                    "atomic_publish",
                    return_value="ab/ab-video-id/v1/",
                ),
                patch.object(
                    package_module,
                    "published_prefix_exists",
                    return_value=True,
                ),
                patch.object(
                    package_module,
                    "withhold_published_prefix",
                ) as withhold,
                patch.object(
                    package_module.progress_tracker, "start_task"
                ),
                patch.object(
                    package_module.progress_tracker, "set_percent"
                ),
            ):
                with self.assertRaisesRegex(
                    OSError, "database commit failed"
                ):
                    package_module.package.run(
                        [],
                        "job-id",
                        "minio://uploads/source.mkv",
                        "v1",
                    )

            self.assertTrue(os.path.isdir(work_dir))
            withhold.assert_called_once_with("ab/ab-video-id/v1/")

    def test_cleanup_failure_does_not_redeliver_completed_result(self):
        job = _job(models.JobStatus.transcoding.value)
        video = _video("processing")
        db = _FakeDb(
            {
                models.Job: [job],
                models.Video: [video],
                models.AudioTrack: [],
                models.Subtitle: [],
            }
        )

        with tempfile.TemporaryDirectory() as work_root:
            work_dir = os.path.join(work_root, "job-id")
            rendition_dir = os.path.join(work_dir, "output", "video_720p")
            os.makedirs(rendition_dir)
            rendition_playlist = os.path.join(rendition_dir, "video.m3u8")
            with open(
                rendition_playlist, "w", encoding="utf-8"
            ) as handle:
                handle.write("#EXTM3U\n")
            db.rows[models.Rendition] = [
                SimpleNamespace(
                    bandwidth=1_000_000,
                    width=1280,
                    height=720,
                    codec="h264",
                    playlist_path=rendition_playlist,
                )
            ]

            with (
                patch.object(package_module, "SessionLocal", return_value=db),
                patch.object(
                    package_module,
                    "get_settings",
                    return_value=SimpleNamespace(WORK_DIR=work_root),
                ),
                patch.object(
                    package_module,
                    "atomic_publish",
                    return_value="ab/ab-video-id/v1/",
                ),
                patch.object(
                    package_module,
                    "published_prefix_exists",
                    return_value=True,
                ),
                patch.object(
                    package_module.shutil,
                    "rmtree",
                    side_effect=OSError("locked"),
                ),
                patch.object(
                    package_module,
                    "_cache_published_version",
                ),
                patch.object(package_module, "publish_event"),
                patch.object(
                    package_module.progress_tracker, "start_task"
                ),
                patch.object(
                    package_module.progress_tracker, "complete_task"
                ),
                patch.object(
                    package_module.progress_tracker, "set_percent"
                ),
            ):
                result = package_module.package.run(
                    [],
                    "job-id",
                    "minio://uploads/source.mkv",
                    "v1",
                )

            self.assertTrue(os.path.isdir(work_dir))

        self.assertEqual(result["output_prefix"], "ab/ab-video-id/v1/")
        self.assertEqual(job.status, models.JobStatus.completed.value)
        self.assertEqual(video.status, "ready")


if __name__ == "__main__":
    unittest.main()
