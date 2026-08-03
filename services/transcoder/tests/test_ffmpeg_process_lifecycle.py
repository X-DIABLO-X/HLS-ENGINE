import ctypes
import os
import signal
import subprocess
import sys
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from app import ffmpeg_utils


def _pid_is_running(pid: int) -> bool:
    if os.name == "nt":
        process_query_limited_information = 0x1000
        still_active = 259
        kernel32 = ctypes.windll.kernel32
        handle = kernel32.OpenProcess(
            process_query_limited_information,
            False,
            pid,
        )
        if not handle:
            return False
        try:
            exit_code = ctypes.c_ulong()
            if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                return False
            return exit_code.value == still_active
        finally:
            kernel32.CloseHandle(handle)

    proc_stat = Path(f"/proc/{pid}/stat")
    if proc_stat.exists():
        try:
            # Treat an already-dead, not-yet-reaped orphan as stopped.
            if proc_stat.read_text(encoding="utf-8").split()[2] == "Z":
                return False
        except (OSError, IndexError):
            pass
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _wait_until_stopped(pid: int, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _pid_is_running(pid):
            return True
        time.sleep(0.05)
    return not _pid_is_running(pid)


_SPAWN_CHILD_AND_WAIT = r"""
import pathlib
import subprocess
import sys
import time

child = subprocess.Popen(
    [sys.executable, "-c", "import time; time.sleep(120)"],
)
pathlib.Path(sys.argv[1]).write_text(str(child.pid), encoding="utf-8")
print("helper-stderr", file=sys.stderr, flush=True)
print("out_time_ms=1000000", flush=True)
time.sleep(120)
"""


_SPAWN_CHILD_AND_FAIL = r"""
import pathlib
import subprocess
import sys

child = subprocess.Popen(
    [sys.executable, "-c", "import time; time.sleep(120)"],
)
pathlib.Path(sys.argv[1]).write_text(str(child.pid), encoding="utf-8")
print("nonzero-stderr", file=sys.stderr, flush=True)
raise SystemExit(7)
"""

_RECORD_PROCESS_TREE_AND_WAIT = r"""
import os
import pathlib
import subprocess
import sys
import time

child = subprocess.Popen(
    [sys.executable, "-c", "import time; time.sleep(120)"],
)
pathlib.Path(sys.argv[1]).write_text(
    f"{os.getpid()} {child.pid}",
    encoding="utf-8",
)
print("out_time_ms=1000000", flush=True)
time.sleep(120)
"""


class _TaskCancelled(BaseException):
    pass


class SoftTimeLimitExceeded(Exception):
    pass


class FFmpegProcessLifecycleTests(unittest.TestCase):
    def _helper_cmd(self, script: str, pid_file: Path):
        return [sys.executable, "-u", "-c", script, str(pid_file)]

    def _assert_child_stopped(self, pid_file: Path):
        self.assertTrue(pid_file.exists(), "helper did not record its child pid")
        child_pid = int(pid_file.read_text(encoding="utf-8"))
        if not _wait_until_stopped(child_pid):
            # Keep a failed test from leaking its helper for the rest of the
            # suite. This is test cleanup, not the behavior under assertion.
            if os.name == "nt":
                subprocess.run(
                    ["taskkill", "/PID", str(child_pid), "/T", "/F"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                )
            else:
                try:
                    os.kill(child_pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            self.fail(f"child process {child_pid} survived runner cleanup")

    def test_progress_callback_failure_does_not_abort_encoder(self):
        callback_calls = []

        def broken_callback(percent):
            callback_calls.append(percent)
            raise RuntimeError("progress backend unavailable")

        script = (
            'import sys;'
            'print("callback-stderr", file=sys.stderr, flush=True);'
            'print("out_time_ms=1000000", flush=True);'
            'print("progress=end", flush=True)'
        )
        with self.assertLogs("app.ffmpeg_utils", level="ERROR") as logs:
            result = ffmpeg_utils.run_cmd_with_progress(
                [sys.executable, "-u", "-c", script],
                total_duration=2,
                on_progress=broken_callback,
                stall_timeout=10,
            )

        self.assertEqual(result.returncode, 0)
        self.assertIn("callback-stderr", result.stderr)
        self.assertEqual(callback_calls, [50, 100])
        self.assertTrue(
            any("encoding continues" in message for message in logs.output)
        )

    def test_base_exception_stops_process_tree(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmpdir:
            pid_file = Path(tmpdir) / "child.pid"

            def cancel_task(_percent):
                raise _TaskCancelled()

            with (
                patch.object(
                    ffmpeg_utils,
                    "_PROCESS_TERMINATE_TIMEOUT_SEC",
                    0.25,
                ),
                patch.object(
                    ffmpeg_utils,
                    "_PROCESS_KILL_TIMEOUT_SEC",
                    2.0,
                ),
                self.assertRaises(_TaskCancelled),
            ):
                ffmpeg_utils.run_cmd_with_progress(
                    self._helper_cmd(_SPAWN_CHILD_AND_WAIT, pid_file),
                    total_duration=2,
                    on_progress=cancel_task,
                    stall_timeout=10,
                )

            self._assert_child_stopped(pid_file)

    @unittest.skipUnless(
        sys.platform.startswith("linux"),
        "Linux parent-death signal behavior",
    )
    def test_hard_task_death_kills_guarded_media_process_tree(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmpdir:
            pid_file = Path(tmpdir) / "tree.pids"
            runner_script = (
                "import sys\n"
                "from app import ffmpeg_utils\n"
                f"target = {_RECORD_PROCESS_TREE_AND_WAIT!r}\n"
                "ffmpeg_utils.run_cmd_with_progress(\n"
                "    [sys.executable, '-u', '-c', target, sys.argv[1]],\n"
                "    total_duration=2,\n"
                "    stall_timeout=30,\n"
                ")\n"
            )
            task_process = subprocess.Popen(
                [sys.executable, "-u", "-c", runner_script, str(pid_file)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            pids = []
            try:
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline and not pid_file.exists():
                    if task_process.poll() is not None:
                        self.fail(
                            "runner exited before starting guarded process tree"
                        )
                    time.sleep(0.05)
                self.assertTrue(
                    pid_file.exists(),
                    "guarded command did not record process tree",
                )
                pids = [
                    int(value)
                    for value in pid_file.read_text(
                        encoding="utf-8"
                    ).split()
                ]

                os.kill(task_process.pid, signal.SIGKILL)
                task_process.wait(timeout=5)

                for pid in pids:
                    self.assertTrue(
                        _wait_until_stopped(pid),
                        f"media process {pid} survived hard task death",
                    )
            finally:
                if task_process.poll() is None:
                    task_process.kill()
                    task_process.wait(timeout=5)
                for pid in pids:
                    if _pid_is_running(pid):
                        try:
                            os.kill(pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass

    @unittest.skipUnless(
        sys.platform.startswith("linux"),
        "Linux parent-death signal behavior",
    )
    def test_hard_task_death_kills_plain_command_process_tree(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmpdir:
            pid_file = Path(tmpdir) / "plain-tree.pids"
            runner_script = (
                "import sys\n"
                "from app import ffmpeg_utils\n"
                f"target = {_RECORD_PROCESS_TREE_AND_WAIT!r}\n"
                "ffmpeg_utils.run_cmd(\n"
                "    [sys.executable, '-u', '-c', target, sys.argv[1]],\n"
                ")\n"
            )
            task_process = subprocess.Popen(
                [sys.executable, "-u", "-c", runner_script, str(pid_file)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            pids = []
            try:
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline and not pid_file.exists():
                    if task_process.poll() is not None:
                        self.fail(
                            "runner exited before starting guarded process tree"
                        )
                    time.sleep(0.05)
                self.assertTrue(
                    pid_file.exists(),
                    "guarded command did not record process tree",
                )
                pids = [
                    int(value)
                    for value in pid_file.read_text(
                        encoding="utf-8"
                    ).split()
                ]

                os.kill(task_process.pid, signal.SIGKILL)
                task_process.wait(timeout=5)

                for pid in pids:
                    self.assertTrue(
                        _wait_until_stopped(pid),
                        f"plain media process {pid} survived hard task death",
                    )
            finally:
                if task_process.poll() is None:
                    task_process.kill()
                    task_process.wait(timeout=5)
                for pid in pids:
                    if _pid_is_running(pid):
                        try:
                            os.kill(pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass

    def test_plain_command_timeout_stops_process_tree(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmpdir:
            pid_file = Path(tmpdir) / "plain-child.pid"
            with (
                patch.object(
                    ffmpeg_utils,
                    "_PROCESS_TERMINATE_TIMEOUT_SEC",
                    0.25,
                ),
                patch.object(
                    ffmpeg_utils,
                    "_PROCESS_KILL_TIMEOUT_SEC",
                    2.0,
                ),
                self.assertRaises(subprocess.TimeoutExpired),
            ):
                ffmpeg_utils.run_cmd(
                    self._helper_cmd(_SPAWN_CHILD_AND_WAIT, pid_file),
                    timeout=1.0,
                )

            self._assert_child_stopped(pid_file)

    def test_soft_time_limit_stops_process_tree(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmpdir:
            pid_file = Path(tmpdir) / "child.pid"

            def expire_task(_percent):
                raise SoftTimeLimitExceeded()

            with (
                patch.object(
                    ffmpeg_utils,
                    "_PROCESS_TERMINATE_TIMEOUT_SEC",
                    0.25,
                ),
                patch.object(
                    ffmpeg_utils,
                    "_PROCESS_KILL_TIMEOUT_SEC",
                    2.0,
                ),
                self.assertRaises(SoftTimeLimitExceeded),
            ):
                ffmpeg_utils.run_cmd_with_progress(
                    self._helper_cmd(_SPAWN_CHILD_AND_WAIT, pid_file),
                    total_duration=2,
                    on_progress=expire_task,
                    stall_timeout=10,
                )

            self._assert_child_stopped(pid_file)

    def test_lost_gpu_lease_stops_process_tree(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmpdir:
            pid_file = Path(tmpdir) / "child.pid"
            lease_lost = threading.Event()

            def lose_lease(_percent):
                lease_lost.set()

            with (
                patch.object(
                    ffmpeg_utils,
                    "_PROCESS_TERMINATE_TIMEOUT_SEC",
                    0.25,
                ),
                patch.object(
                    ffmpeg_utils,
                    "_PROCESS_KILL_TIMEOUT_SEC",
                    2.0,
                ),
                self.assertRaisesRegex(
                    ffmpeg_utils.FFmpegError,
                    "GPU reservation was lost",
                ),
            ):
                ffmpeg_utils.run_cmd_with_progress(
                    self._helper_cmd(_SPAWN_CHILD_AND_WAIT, pid_file),
                    total_duration=2,
                    on_progress=lose_lease,
                    stall_timeout=10,
                    cancel_event=lease_lost,
                )

            self._assert_child_stopped(pid_file)

    def test_stall_stops_process_tree_and_reports_stall(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmpdir:
            pid_file = Path(tmpdir) / "child.pid"
            script = _SPAWN_CHILD_AND_WAIT.replace(
                'print("out_time_ms=1000000", flush=True)',
                "",
            )
            with (
                patch.object(
                    ffmpeg_utils,
                    "_PROCESS_TERMINATE_TIMEOUT_SEC",
                    0.25,
                ),
                patch.object(
                    ffmpeg_utils,
                    "_PROCESS_KILL_TIMEOUT_SEC",
                    2.0,
                ),
                self.assertRaisesRegex(
                    ffmpeg_utils.FFmpegError,
                    "FFmpeg stalled",
                ) as raised,
            ):
                ffmpeg_utils.run_cmd_with_progress(
                    self._helper_cmd(script, pid_file),
                    total_duration=2,
                    stall_timeout=0.1,
                )

            self._assert_child_stopped(pid_file)
            self.assertIn("helper-stderr", str(raised.exception))

    def test_nonzero_exit_stops_lingering_process_group(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmpdir:
            pid_file = Path(tmpdir) / "child.pid"
            with (
                patch.object(
                    ffmpeg_utils,
                    "_PROCESS_TERMINATE_TIMEOUT_SEC",
                    0.25,
                ),
                patch.object(
                    ffmpeg_utils,
                    "_PROCESS_KILL_TIMEOUT_SEC",
                    2.0,
                ),
                self.assertRaisesRegex(
                    ffmpeg_utils.FFmpegError,
                    r"Command failed \(7\)",
                ) as raised,
            ):
                ffmpeg_utils.run_cmd_with_progress(
                    self._helper_cmd(_SPAWN_CHILD_AND_FAIL, pid_file),
                    total_duration=2,
                    stall_timeout=10,
                )

            self._assert_child_stopped(pid_file)
            self.assertIn("nonzero-stderr", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
