import asyncio
import os
import shutil
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from app import main, models
from app.job_lock import job_lock
from app.tasks import workspace_cleanup


FAILED_JOB_ID = "11111111-1111-4111-8111-111111111111"
CURRENT_JOB_ID = "22222222-2222-4222-8222-222222222222"
READY_JOB_ID = "33333333-3333-4333-8333-333333333333"


class FakeDb:
    def close(self):
        pass


def terminal_state(
    *,
    job_status=models.JobStatus.failed.value,
    video_status="failed",
    current_job_id=None,
    output_prefix=None,
):
    return workspace_cleanup.DurableWorkspaceState(
        job_exists=True,
        job_status=job_status,
        video_status=video_status,
        output_prefix=output_prefix,
        current_job_id=current_job_id,
        updated_at=datetime.now(timezone.utc) - timedelta(hours=1),
    )


class WorkspaceCleanupTests(unittest.TestCase):
    def settings(self, work_root, grace=0):
        return SimpleNamespace(
            WORK_DIR=str(work_root),
            REDIS_URL="redis://unused/0",
            WORKSPACE_CLEANUP_GRACE_SEC=grace,
            WORKSPACE_CLEANUP_RETRY_SEC=1,
            WORKSPACE_CLEANUP_MAX_RETRIES=10,
            WORKSPACE_REAPER_ENABLED=True,
            WORKSPACE_REAPER_SCAN_LIMIT=100,
            WORKSPACE_REAPER_INTERVAL_SEC=60,
        )

    def make_workspace(self, root, job_id=FAILED_JOB_ID):
        path = Path(root) / job_id
        path.mkdir(parents=True)
        (path / "source.mp4").write_bytes(b"worker-copy")
        return path

    def cleanup_once(self, root, state, job_id=FAILED_JOB_ID):
        with (
            patch.object(
                workspace_cleanup,
                "get_settings",
                return_value=self.settings(root),
            ),
            patch.object(
                workspace_cleanup,
                "SessionLocal",
                return_value=FakeDb(),
            ),
            patch.object(
                workspace_cleanup,
                "_load_durable_state",
                return_value=state,
            ),
            patch.object(workspace_cleanup, "_record_metric"),
        ):
            return workspace_cleanup._cleanup_workspace_once(
                job_id,
                trigger="unit-test",
            )

    def test_parallel_header_failure_waits_for_all_mutators(self):
        with tempfile.TemporaryDirectory() as root:
            path = self.make_workspace(root)
            state = terminal_state()
            results = []
            with (
                patch.object(
                    workspace_cleanup,
                    "get_settings",
                    return_value=self.settings(root),
                ),
                patch.object(
                    workspace_cleanup,
                    "SessionLocal",
                    return_value=FakeDb(),
                ),
                patch.object(
                    workspace_cleanup,
                    "_load_durable_state",
                    return_value=state,
                ),
                patch.object(workspace_cleanup, "_record_metric"),
                job_lock(
                    root,
                    FAILED_JOB_ID,
                    purpose="parallel-header-task",
                    shared=True,
                ),
            ):
                thread = threading.Thread(
                    target=lambda: results.append(
                        workspace_cleanup._cleanup_workspace_once(
                            FAILED_JOB_ID,
                            trigger="pipeline-failure",
                        )
                    )
                )
                thread.start()
                thread.join(3)
                self.assertFalse(thread.is_alive())
                self.assertEqual(results[0]["outcome"], "busy")
                self.assertTrue(path.is_dir())

            result = self.cleanup_once(root, state)
            self.assertEqual(result["outcome"], "deleted")
            self.assertFalse(path.exists())

    def test_hard_timeout_failed_generation_is_reclaimed(self):
        with tempfile.TemporaryDirectory() as root:
            path = self.make_workspace(root)
            result = self.cleanup_once(
                root,
                terminal_state(
                    job_status=models.JobStatus.failed.value,
                    video_status="failed",
                    current_job_id=None,
                ),
            )

            self.assertEqual(result["reason"], "failed")
            self.assertFalse(path.exists())

    def test_state_snapshot_locks_video_before_job_to_match_retry_order(self):
        events = []
        video = SimpleNamespace(id="video-id", status="failed")
        job = SimpleNamespace(
            id=FAILED_JOB_ID,
            video_id=video.id,
            status=models.JobStatus.failed.value,
            output_prefix=None,
            updated_at=datetime.now(timezone.utc),
            created_at=datetime.now(timezone.utc),
        )

        class Query:
            def __init__(self, value, lock_name=None):
                self.value = value
                self.lock_name = lock_name

            def filter(self, *_args):
                return self

            def order_by(self, *_args):
                return self

            def with_for_update(self):
                events.append(self.lock_name)
                return self

            def first(self):
                return self.value

        class OrderedDb:
            def __init__(self):
                self.job_queries = 0

            def query(self, model):
                if model is models.Video:
                    return Query(video, "video")
                self.job_queries += 1
                if self.job_queries == 1:
                    return Query(job)
                if self.job_queries == 2:
                    return Query(job, "job")
                return Query(None)

        state = workspace_cleanup._load_durable_state(
            OrderedDb(),
            FAILED_JOB_ID,
        )

        self.assertTrue(state.job_exists)
        self.assertEqual(events, ["video", "job"])

    def test_superseded_retry_removes_old_but_never_current_workspace(self):
        with tempfile.TemporaryDirectory() as root:
            old_path = self.make_workspace(root, FAILED_JOB_ID)
            current_path = self.make_workspace(root, CURRENT_JOB_ID)
            old_state = terminal_state(
                job_status=models.JobStatus.transcoding.value,
                video_status="processing",
                current_job_id=CURRENT_JOB_ID,
            )
            current_state = terminal_state(
                job_status=models.JobStatus.transcoding.value,
                video_status="processing",
                current_job_id=CURRENT_JOB_ID,
            )

            old_result = self.cleanup_once(root, old_state, FAILED_JOB_ID)
            current_result = self.cleanup_once(
                root,
                current_state,
                CURRENT_JOB_ID,
            )

            self.assertEqual(old_result["reason"], "superseded")
            self.assertFalse(old_path.exists())
            self.assertEqual(current_result["outcome"], "active")
            self.assertTrue(current_path.is_dir())

    def test_cleanup_crash_redelivery_is_idempotent(self):
        with tempfile.TemporaryDirectory() as root:
            path = self.make_workspace(root)
            real_rmtree = shutil.rmtree

            def delete_then_crash(candidate):
                real_rmtree(candidate)
                raise RuntimeError("worker crashed after unlink")

            with (
                patch.object(
                    workspace_cleanup,
                    "get_settings",
                    return_value=self.settings(root),
                ),
                patch.object(
                    workspace_cleanup,
                    "SessionLocal",
                    return_value=FakeDb(),
                ),
                patch.object(
                    workspace_cleanup,
                    "_load_durable_state",
                    return_value=terminal_state(),
                ),
                patch.object(workspace_cleanup, "_record_metric"),
                patch.object(
                    workspace_cleanup.shutil,
                    "rmtree",
                    side_effect=delete_then_crash,
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "crashed"):
                    workspace_cleanup._cleanup_workspace_once(
                        FAILED_JOB_ID,
                        trigger="first-delivery",
                    )

            self.assertFalse(path.exists())
            redelivery = self.cleanup_once(root, terminal_state())
            self.assertEqual(redelivery["outcome"], "absent")

    def test_never_deletes_current_or_ready_inconsistent_active_work(self):
        with tempfile.TemporaryDirectory() as root:
            active_path = self.make_workspace(root, CURRENT_JOB_ID)
            ready_path = self.make_workspace(root, READY_JOB_ID)

            active = self.cleanup_once(
                root,
                terminal_state(
                    job_status=models.JobStatus.transcoding.value,
                    video_status="processing",
                    current_job_id=CURRENT_JOB_ID,
                ),
                CURRENT_JOB_ID,
            )
            ready = self.cleanup_once(
                root,
                terminal_state(
                    job_status=models.JobStatus.publishing.value,
                    video_status="ready",
                    current_job_id=READY_JOB_ID,
                ),
                READY_JOB_ID,
            )

            self.assertEqual(active["reason"], "active")
            self.assertEqual(ready["reason"], "ready_inconsistent")
            self.assertTrue(active_path.is_dir())
            self.assertTrue(ready_path.is_dir())

    def test_completed_workspace_requires_durable_master(self):
        with tempfile.TemporaryDirectory() as root:
            path = self.make_workspace(root, READY_JOB_ID)
            state = terminal_state(
                job_status=models.JobStatus.completed.value,
                video_status="ready",
                current_job_id=READY_JOB_ID,
                output_prefix="aa/video/v1/",
            )
            with patch.object(
                workspace_cleanup,
                "published_prefix_exists",
                return_value=False,
            ):
                skipped = self.cleanup_once(root, state, READY_JOB_ID)
            self.assertEqual(skipped["outcome"], "active")
            self.assertTrue(path.is_dir())

            with patch.object(
                workspace_cleanup,
                "published_prefix_exists",
                return_value=True,
            ):
                removed = self.cleanup_once(root, state, READY_JOB_ID)
            self.assertEqual(removed["reason"], "completed_durable")
            self.assertFalse(path.exists())

    def test_direct_child_symlink_is_refused_and_target_is_untouched(self):
        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as outside:
            target = Path(outside) / "source"
            target.mkdir()
            marker = target / "keep.mkv"
            marker.write_bytes(b"original")
            link = Path(root) / FAILED_JOB_ID
            try:
                os.symlink(target, link, target_is_directory=True)
            except (OSError, NotImplementedError):
                self.skipTest("directory symlinks are unavailable")

            result = self.cleanup_once(root, terminal_state())

            self.assertEqual(result["outcome"], "unsafe")
            self.assertTrue(marker.is_file())
            self.assertTrue(link.exists())

    def test_missing_work_volume_never_reports_false_absence(self):
        with tempfile.TemporaryDirectory() as parent:
            missing_root = Path(parent) / "not-mounted"
            status = workspace_cleanup.workspace_status(
                FAILED_JOB_ID,
                work_root=str(missing_root),
            )

        self.assertEqual(status["state"], "unavailable")
        self.assertIsNone(status["exists"])

    def test_periodic_reaper_recovers_orphan_after_database_deletion(self):
        with tempfile.TemporaryDirectory() as root:
            path = self.make_workspace(root)
            orphan_state = workspace_cleanup.DurableWorkspaceState(
                job_exists=False
            )
            with (
                patch.object(
                    workspace_cleanup,
                    "get_settings",
                    return_value=self.settings(root),
                ),
                patch.object(
                    workspace_cleanup,
                    "SessionLocal",
                    return_value=FakeDb(),
                ),
                patch.object(
                    workspace_cleanup,
                    "_load_durable_state",
                    return_value=orphan_state,
                ),
                patch.object(workspace_cleanup, "_record_metric"),
            ):
                summary = workspace_cleanup.reap_workspaces.run()

            self.assertEqual(summary["deleted"], 1)
            self.assertFalse(path.exists())

    def test_periodic_reaper_also_runs_bounded_idle_lock_sweep(self):
        with tempfile.TemporaryDirectory() as root:
            lock_summary = {
                "scanned": 4,
                "removed": 4,
                "busy": 0,
                "unsafe": 0,
                "errors": 0,
            }
            with (
                patch.object(
                    workspace_cleanup,
                    "get_settings",
                    return_value=self.settings(root),
                ),
                patch.object(
                    workspace_cleanup,
                    "reap_idle_job_locks",
                    return_value=lock_summary,
                ) as sweep,
            ):
                summary = workspace_cleanup.reap_workspaces.run()

            sweep.assert_called_once_with(str(root))
            self.assertEqual(summary["job_locks"], lock_summary)

    def test_bounded_scan_queues_cursor_continuation_without_starvation(self):
        with tempfile.TemporaryDirectory() as root:
            first = self.make_workspace(root, FAILED_JOB_ID)
            second = self.make_workspace(root, CURRENT_JOB_ID)
            settings = self.settings(root)
            settings.WORKSPACE_REAPER_SCAN_LIMIT = 1
            with (
                patch.object(
                    workspace_cleanup,
                    "get_settings",
                    return_value=settings,
                ),
                patch.object(
                    workspace_cleanup,
                    "SessionLocal",
                    return_value=FakeDb(),
                ),
                patch.object(
                    workspace_cleanup,
                    "_load_durable_state",
                    return_value=terminal_state(),
                ),
                patch.object(workspace_cleanup, "_record_metric"),
                patch.object(
                    workspace_cleanup.reap_workspaces,
                    "apply_async",
                ) as continuation,
            ):
                summary = workspace_cleanup.reap_workspaces.run()

            self.assertTrue(summary["continuation_queued"])
            continuation.assert_called_once_with(
                kwargs={"cursor": FAILED_JOB_ID}
            )
            self.assertFalse(first.exists())
            self.assertTrue(second.exists())

    def test_orphan_grace_uses_restart_safe_first_observation_marker(self):
        with tempfile.TemporaryDirectory() as root:
            path = self.make_workspace(root)
            old = time.time() - 3600
            os.utime(path, (old, old))
            state = workspace_cleanup.DurableWorkspaceState(job_exists=False)
            settings = self.settings(root, grace=60)
            with (
                patch.object(
                    workspace_cleanup,
                    "get_settings",
                    return_value=settings,
                ),
                patch.object(
                    workspace_cleanup,
                    "SessionLocal",
                    return_value=FakeDb(),
                ),
                patch.object(
                    workspace_cleanup,
                    "_load_durable_state",
                    return_value=state,
                ),
                patch.object(workspace_cleanup, "_record_metric"),
            ):
                first = workspace_cleanup._cleanup_workspace_once(
                    FAILED_JOB_ID,
                    trigger="first-orphan-observation",
                )
                marker = (
                    path / workspace_cleanup._ORPHAN_OBSERVED_MARKER
                )
                self.assertEqual(first["outcome"], "deferred")
                self.assertTrue(marker.is_file())
                self.assertTrue(path.is_dir())

                old_marker = time.time() - 61
                os.utime(marker, (old_marker, old_marker))
                second = workspace_cleanup._cleanup_workspace_once(
                    FAILED_JOB_ID,
                    trigger="post-restart-reaper",
                )

            self.assertEqual(second["reason"], "orphan")
            self.assertFalse(path.exists())

    def test_job_status_lists_every_generation_workspace(self):
        first = SimpleNamespace(
            id=FAILED_JOB_ID,
            video_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            status=models.JobStatus.failed.value,
            progress=0,
            output_prefix=None,
            error_message="failed",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        second = SimpleNamespace(
            **{
                **first.__dict__,
                "id": CURRENT_JOB_ID,
                "status": models.JobStatus.transcoding.value,
            }
        )

        class Query:
            def filter(self, *_args):
                return self

            def order_by(self, *_args):
                return self

            def first(self):
                return first

            def all(self):
                return [first, second]

        class ApiDb:
            def query(self, _model):
                return Query()

            def close(self):
                pass

        def status(job_id):
            return {
                "job_id": job_id,
                "path": f"/tmp/hls-work/{job_id}",
                "exists": job_id == CURRENT_JOB_ID,
                "state": (
                    "present" if job_id == CURRENT_JOB_ID else "absent"
                ),
            }

        with (
            patch.object(main, "SessionLocal", return_value=ApiDb()),
            patch.object(workspace_cleanup, "workspace_status", side_effect=status),
        ):
            result = asyncio.run(main.get_job(FAILED_JOB_ID))

        self.assertEqual(result["workspace"]["state"], "absent")
        self.assertEqual(
            [item["job_id"] for item in result["workspaces"]],
            [FAILED_JOB_ID, CURRENT_JOB_ID],
        )
        self.assertTrue(result["workspaces"][1]["exists"])


if __name__ == "__main__":
    unittest.main()
