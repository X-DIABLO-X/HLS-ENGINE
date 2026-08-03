import multiprocessing
import tempfile
import threading
import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch

import app.job_lock as job_lock_module
from app.job_lock import JobLockBusy, job_lock
import app.tasks.package as package_module
from app.tasks import transcode_video as transcode_tasks


def _acquire_in_spawned_process(
    work_root,
    key,
    attempting,
    acquired,
):
    attempting.set()
    with job_lock(work_root, key, purpose="spawned-test"):
        acquired.set()


class JobLockTests(unittest.TestCase):
    def test_released_targets_leave_only_the_persistent_namespace_guard(self):
        with tempfile.TemporaryDirectory() as work_root:
            with job_lock(
                work_root,
                "completed-job",
                purpose="release-cleanup-test",
            ) as target:
                self.assertTrue(job_lock_module.os.path.isfile(target))

            lock_dir = job_lock_module.os.path.join(
                work_root,
                job_lock_module._LOCK_DIR,
            )
            self.assertEqual(
                job_lock_module.os.listdir(lock_dir),
                [job_lock_module._NAMESPACE_GUARD],
            )

    def test_waiter_and_new_opener_never_split_across_target_cleanup(self):
        waiter_at_target = threading.Event()
        waiter_acquired = threading.Event()
        release_waiter = threading.Event()
        newcomer_acquired = threading.Event()
        errors = []
        real_acquire = job_lock_module._acquire_handle

        def observed_acquire(handle, *, shared, blocking):
            if (
                threading.current_thread().name == "lock-waiter"
                and not handle.name.endswith(
                    job_lock_module._NAMESPACE_GUARD
                )
            ):
                waiter_at_target.set()
            return real_acquire(
                handle,
                shared=shared,
                blocking=blocking,
            )

        def waiter(work_root):
            try:
                with job_lock(
                    work_root,
                    "same-key",
                    purpose="queued-waiter",
                ):
                    waiter_acquired.set()
                    if not release_waiter.wait(5):
                        raise AssertionError("waiter was not released")
            except BaseException as exc:
                errors.append(exc)

        def newcomer(work_root):
            try:
                with job_lock(
                    work_root,
                    "same-key",
                    purpose="new-opener",
                ):
                    newcomer_acquired.set()
            except BaseException as exc:
                errors.append(exc)

        with tempfile.TemporaryDirectory() as work_root:
            with patch.object(
                job_lock_module,
                "_acquire_handle",
                side_effect=observed_acquire,
            ):
                with job_lock(
                    work_root,
                    "same-key",
                    purpose="initial-owner",
                ):
                    queued = threading.Thread(
                        target=waiter,
                        args=(work_root,),
                        name="lock-waiter",
                    )
                    queued.start()
                    self.assertTrue(waiter_at_target.wait(5))
                    self.assertFalse(waiter_acquired.is_set())

                self.assertTrue(waiter_acquired.wait(5))
                newcomer = threading.Thread(
                    target=newcomer,
                    args=(work_root,),
                    name="lock-newcomer",
                )
                newcomer.start()
                self.assertFalse(newcomer_acquired.wait(0.3))
                release_waiter.set()
                queued.join(5)
                newcomer.join(5)

            lock_dir = job_lock_module.os.path.join(
                work_root,
                job_lock_module._LOCK_DIR,
            )
            self.assertEqual(
                job_lock_module.os.listdir(lock_dir),
                [job_lock_module._NAMESPACE_GUARD],
            )

        self.assertEqual(errors, [])
        self.assertFalse(queued.is_alive())
        self.assertFalse(newcomer.is_alive())
        self.assertTrue(newcomer_acquired.is_set())

    def test_bounded_sweep_skips_active_target_and_reclaims_stale_files(self):
        with tempfile.TemporaryDirectory() as work_root:
            with job_lock(
                work_root,
                "a-active",
                purpose="active-during-sweep",
            ):
                first_stale = job_lock_module._lock_path(
                    work_root,
                    "b-stale",
                )
                second_stale = job_lock_module._lock_path(
                    work_root,
                    "c-stale",
                )
                for path in (first_stale, second_stale):
                    with open(path, "wb") as handle:
                        handle.write(b"\0")

                first = job_lock_module.reap_idle_job_locks(
                    work_root,
                    limit=2,
                )

                self.assertEqual(first["scanned"], 2)
                self.assertEqual(first["busy"], 1)
                self.assertEqual(first["removed"], 1)
                self.assertFalse(job_lock_module.os.path.exists(first_stale))
                self.assertTrue(job_lock_module.os.path.exists(second_stale))

            second = job_lock_module.reap_idle_job_locks(work_root, limit=2)
            self.assertEqual(second["removed"], 1)
            lock_dir = job_lock_module.os.path.join(
                work_root,
                job_lock_module._LOCK_DIR,
            )
            self.assertEqual(
                job_lock_module.os.listdir(lock_dir),
                [job_lock_module._NAMESPACE_GUARD],
            )

    @unittest.skipIf(
        job_lock_module.fcntl is None,
        "shared advisory locks are a POSIX production feature",
    )
    def test_shared_legacy_rungs_overlap_while_package_exclusive_waits(self):
        release_shared = threading.Event()
        first_acquired = threading.Event()
        second_acquired = threading.Event()
        exclusive_attempting = threading.Event()
        exclusive_acquired = threading.Event()
        errors = []

        def shared_delivery(work_root, acquired):
            try:
                with job_lock(
                    work_root,
                    "job-shared",
                    purpose="legacy-rung",
                    shared=True,
                ):
                    acquired.set()
                    if not release_shared.wait(5):
                        raise AssertionError("shared test lock was not released")
            except BaseException as exc:
                errors.append(exc)

        def exclusive_package(work_root):
            try:
                exclusive_attempting.set()
                with job_lock(
                    work_root,
                    "job-shared",
                    purpose="package-test",
                ):
                    exclusive_acquired.set()
            except BaseException as exc:
                errors.append(exc)

        with tempfile.TemporaryDirectory() as work_root:
            first = threading.Thread(
                target=shared_delivery,
                args=(work_root, first_acquired),
            )
            second = threading.Thread(
                target=shared_delivery,
                args=(work_root, second_acquired),
            )
            package = threading.Thread(
                target=exclusive_package,
                args=(work_root,),
            )
            first.start()
            second.start()
            self.assertTrue(first_acquired.wait(5))
            self.assertTrue(second_acquired.wait(5))

            package.start()
            self.assertTrue(exclusive_attempting.wait(5))
            self.assertFalse(exclusive_acquired.wait(0.3))
            release_shared.set()

            first.join(5)
            second.join(5)
            package.join(5)

        self.assertEqual(errors, [])
        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertFalse(package.is_alive())
        self.assertTrue(exclusive_acquired.is_set())

    def test_lock_excludes_a_separate_process(self):
        context = multiprocessing.get_context("spawn")
        with tempfile.TemporaryDirectory() as work_root:
            attempting = context.Event()
            acquired = context.Event()
            process = context.Process(
                target=_acquire_in_spawned_process,
                args=(
                    work_root,
                    "job-cross-process",
                    attempting,
                    acquired,
                ),
            )

            with job_lock(
                work_root,
                "job-cross-process",
                purpose="parent-test",
            ):
                process.start()
                self.assertTrue(attempting.wait(5))
                self.assertFalse(acquired.wait(0.3))

            self.assertTrue(acquired.wait(5))
            process.join(5)
            if process.is_alive():
                process.terminate()
                process.join(5)

        self.assertEqual(process.exitcode, 0)

    def test_exception_releases_lock_for_next_delivery(self):
        with tempfile.TemporaryDirectory() as work_root:
            with self.assertRaisesRegex(RuntimeError, "injected"):
                with job_lock(
                    work_root,
                    "job-exception",
                    purpose="failing-test",
                ):
                    raise RuntimeError("injected")

            with job_lock(
                work_root,
                "job-exception",
                purpose="retry-test",
            ):
                acquired_after_failure = True

        self.assertTrue(acquired_after_failure)

    def test_nonblocking_cleanup_lock_reports_busy_without_waiting(self):
        with tempfile.TemporaryDirectory() as work_root:
            with job_lock(
                work_root,
                "job-busy",
                purpose="active-header",
                shared=True,
            ):
                started = threading.Event()
                finished = threading.Event()
                errors = []

                def attempt_cleanup():
                    started.set()
                    try:
                        with job_lock(
                            work_root,
                            "job-busy",
                            purpose="workspace-cleanup",
                            blocking=False,
                        ):
                            errors.append(
                                AssertionError("cleanup unexpectedly acquired")
                            )
                    except JobLockBusy:
                        pass
                    finally:
                        finished.set()

                thread = threading.Thread(target=attempt_cleanup)
                thread.start()
                self.assertTrue(started.wait(2))
                self.assertTrue(finished.wait(2))
                thread.join(2)

        self.assertEqual(errors, [])

    def test_transcode_and_package_mutation_paths_cannot_overlap(self):
        transcode_entered = threading.Event()
        release_transcode = threading.Event()
        package_waiting = threading.Event()
        package_entered = threading.Event()
        errors = []
        order = []

        def fake_transcode(*_args, **_kwargs):
            order.append("transcode-start")
            transcode_entered.set()
            if not release_transcode.wait(5):
                raise AssertionError("test did not release transcode")
            order.append("transcode-end")
            return {"job_id": "job-overlap"}

        def fake_package(*_args, **_kwargs):
            order.append("package-start")
            package_entered.set()
            return {"job_id": "job-overlap", "reused": True}

        fake_job = SimpleNamespace(
            id="job-overlap",
            video_id="video-overlap",
        )
        fake_db = SimpleNamespace(
            query=lambda _model: SimpleNamespace(
                filter=lambda *_args: SimpleNamespace(
                    first=lambda: fake_job
                )
            ),
            close=lambda: None,
        )

        real_package_lock = package_module.job_lock

        @contextmanager
        def observed_package_lock(*args, **kwargs):
            package_waiting.set()
            with real_package_lock(*args, **kwargs) as path:
                yield path

        def invoke(callable_, *args):
            try:
                callable_(*args)
            except BaseException as exc:  # Surface thread failures in the test.
                errors.append(exc)

        with tempfile.TemporaryDirectory() as work_root:
            settings = SimpleNamespace(WORK_DIR=work_root)
            with (
                patch.object(
                    transcode_tasks,
                    "get_settings",
                    return_value=settings,
                ),
                patch.object(
                    package_module,
                    "get_settings",
                    return_value=settings,
                ),
                patch.object(
                    transcode_tasks,
                    "_run_transcode_group",
                    side_effect=fake_transcode,
                ),
                patch.object(
                    package_module,
                    "_run_package",
                    side_effect=fake_package,
                ),
                patch.object(
                    package_module,
                    "SessionLocal",
                    return_value=fake_db,
                ),
                patch.object(
                    package_module,
                    "job_lock",
                    observed_package_lock,
                ),
            ):
                transcode_thread = threading.Thread(
                    target=invoke,
                    args=(
                        transcode_tasks.transcode_group.run,
                        "job-overlap",
                        "minio://unused/source.mkv",
                        [{"height": 720, "width": 1280, "bitrate": 1}],
                        None,
                        {},
                    ),
                )
                package_thread = threading.Thread(
                    target=invoke,
                    args=(
                        package_module.package.run,
                        [],
                        "job-overlap",
                        "minio://unused/source.mkv",
                        "v1",
                    ),
                )

                transcode_thread.start()
                self.assertTrue(transcode_entered.wait(5))
                package_thread.start()
                self.assertTrue(package_waiting.wait(5))
                self.assertFalse(package_entered.wait(0.3))
                self.assertTrue(package_thread.is_alive())

                release_transcode.set()
                transcode_thread.join(5)
                package_thread.join(5)

        self.assertFalse(transcode_thread.is_alive())
        self.assertFalse(package_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(
            order,
            ["transcode-start", "transcode-end", "package-start"],
        )

    def test_concat_waits_for_duplicate_chunk_directory_mutation(self):
        chunk_entered = threading.Event()
        release_chunk = threading.Event()
        concat_entered = threading.Event()
        errors = []

        def fake_chunk(*_args, **_kwargs):
            chunk_entered.set()
            if not release_chunk.wait(5):
                raise AssertionError("test did not release chunk")
            return {"chunk_index": 0}

        def fake_concat(*_args, **_kwargs):
            concat_entered.set()
            return {"job_id": "job-chunk"}

        def invoke(callable_, *args):
            try:
                callable_(*args)
            except BaseException as exc:
                errors.append(exc)

        with tempfile.TemporaryDirectory() as work_root:
            settings = SimpleNamespace(WORK_DIR=work_root)
            with (
                patch.object(
                    transcode_tasks,
                    "get_settings",
                    return_value=settings,
                ),
                patch.object(
                    transcode_tasks,
                    "_run_transcode_chunk",
                    side_effect=fake_chunk,
                ),
                patch.object(
                    transcode_tasks,
                    "_run_concat_segments",
                    side_effect=fake_concat,
                ),
            ):
                chunk_thread = threading.Thread(
                    target=invoke,
                    args=(
                        transcode_tasks.transcode_chunk.run,
                        "job-chunk",
                        "minio://unused/source.mkv",
                        [{"height": 720, "width": 1280, "bitrate": 1}],
                        0.0,
                        60.0,
                        0,
                        None,
                        {},
                    ),
                )
                concat_thread = threading.Thread(
                    target=invoke,
                    args=(
                        transcode_tasks.concat_segments.run,
                        [],
                        "job-chunk",
                        {},
                    ),
                )
                chunk_thread.start()
                self.assertTrue(chunk_entered.wait(5))
                concat_thread.start()
                self.assertFalse(concat_entered.wait(0.25))
                release_chunk.set()
                chunk_thread.join(5)
                concat_thread.join(5)

        self.assertEqual(errors, [])
        self.assertTrue(concat_entered.is_set())

    def test_duplicate_group_deliveries_cannot_enter_mutation_together(self):
        first_entered = threading.Event()
        release_first = threading.Event()
        second_entered = threading.Event()
        calls_guard = threading.Lock()
        call_count = 0
        errors = []

        def fake_transcode(*_args, **_kwargs):
            nonlocal call_count
            with calls_guard:
                call_count += 1
                ordinal = call_count
            if ordinal == 1:
                first_entered.set()
                if not release_first.wait(5):
                    raise AssertionError("first delivery was not released")
            else:
                second_entered.set()
            return {"job_id": "job-duplicate"}

        def invoke():
            try:
                transcode_tasks.transcode_group.run(
                    "job-duplicate",
                    "minio://unused/source.mkv",
                    [{"height": 720, "width": 1280, "bitrate": 1}],
                    None,
                    {},
                )
            except BaseException as exc:
                errors.append(exc)

        with tempfile.TemporaryDirectory() as work_root:
            with (
                patch.object(
                    transcode_tasks,
                    "get_settings",
                    return_value=SimpleNamespace(WORK_DIR=work_root),
                ),
                patch.object(
                    transcode_tasks,
                    "_run_transcode_group",
                    side_effect=fake_transcode,
                ),
            ):
                first = threading.Thread(target=invoke)
                second = threading.Thread(target=invoke)
                first.start()
                self.assertTrue(first_entered.wait(5))
                second.start()
                self.assertFalse(second_entered.wait(0.3))
                self.assertTrue(second.is_alive())
                release_first.set()
                first.join(5)
                second.join(5)

        self.assertEqual(errors, [])
        self.assertEqual(call_count, 2)
        self.assertTrue(second_entered.is_set())
        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())


if __name__ == "__main__":
    unittest.main()
