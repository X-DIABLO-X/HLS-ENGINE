"""FFmpeg / ffprobe helpers, GPU detection and command builders.

Optimized pipeline:
- Zero-copy CUDA decode -> GPU scale -> NVENC encode (h264/hevc/av1).
- Single-pass multi-rendition from one decode (split filter) per GPU.
- Chunked (GOP-aligned) parallel encoding for long sources.
- CMAF fMP4 or MPEG-TS segments.
- Per-title complexity analysis + complexity-aware bitrate ladder.
- Combined audio extract -> loudnorm -> AAC HLS in one pass.
- Trick-play sprite + WebVTT index for scrubber previews.
"""

import json
import logging
import math
import os
import re
import signal
import subprocess
import sys
from typing import Any, Callable, Dict, List, Optional, Tuple

from app.config import get_settings

logger = logging.getLogger(__name__)

SEGMENT_DURATION = 6


class FFmpegError(Exception):
    pass


def is_nvdec_initialization_failure(error: Any) -> bool:
    """Return whether FFmpeg failed while initializing CUDA video decode.

    The classifier is intentionally narrower than a generic CUDA or
    out-of-memory check. NVENC and GPU filters can report the same CUDA error,
    but software decode plus ``hwupload_cuda`` only bypasses failures in the
    NVDEC/CUVID input path.
    """
    text = str(error or "")
    if isinstance(error, BaseException):
        for attr in ("stderr", "output"):
            value = getattr(error, attr, None)
            if value:
                text += f"\n{value}"
    normalized = " ".join(text.lower().split())

    if (
        "cannot load libnvcuvid" in normalized
        or "failed loading nvcuvid" in normalized
    ):
        return True

    # Keep the decoder-creation evidence and failure marker on the same log
    # line. A broad multi-line CUDA match could incorrectly retry an NVENC or
    # GPU-filter failure that software decoding cannot bypass.
    for line in text.splitlines():
        normalized_line = " ".join(line.lower().split())
        marker = "cuvidcreatedecoder"
        marker_index = normalized_line.find(marker)
        if marker_index < 0:
            continue
        failure_index = normalized_line.find(
            " failed",
            marker_index + len(marker),
        )
        if failure_index < 0:
            continue
        call_context = normalized_line[
            marker_index + len(marker):failure_index
        ]
        if ";" not in call_context and "succeeded" not in call_context:
            return True
    return False


# ----------------------------------------------------------------------------
# Command runners
# ----------------------------------------------------------------------------


_PROCESS_TERMINATE_TIMEOUT_SEC = 5.0
_PROCESS_KILL_TIMEOUT_SEC = 5.0
_PROCESS_WAIT_POLL_SEC = 0.05
_PIPE_DRAIN_TIMEOUT_SEC = 5.0


def run_cmd(
    cmd: List[str],
    check: bool = True,
    **kwargs,
) -> subprocess.CompletedProcess:
    """Run one bounded, parent-guarded external media command.

    Auxiliary FFmpeg/ffprobe work needs the same process-tree ownership as the
    progress-aware encoder.  In particular, a Celery hard kill must not leave
    thumbnail, subtitle, audio fallback, concat, or probe descendants writing
    after the job lock has been released.
    """
    logger.info("[ffmpeg] %s", " ".join(cmd))
    timeout = kwargs.pop("timeout", None)
    if timeout is None:
        try:
            settings = get_settings()
            timeout = min(
                float(settings.GPU_FFMPEG_TIMEOUT_SEC),
                float(settings.CPU_FALLBACK_FFMPEG_TIMEOUT_SEC),
            )
        except Exception:
            timeout = 5400.0

    input_data = kwargs.pop("input", None)
    windows_job: Optional[int] = None
    timeout_error: Optional[subprocess.TimeoutExpired] = None
    stdout = ""
    stderr = ""
    proc = subprocess.Popen(
        _parent_guarded_command(cmd),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        **_progress_popen_kwargs(),
        **kwargs,
    )
    try:
        windows_job = _create_windows_kill_job(proc)
        try:
            stdout, stderr = proc.communicate(
                input=input_data,
                timeout=float(timeout) if timeout and timeout > 0 else None,
            )
        except subprocess.TimeoutExpired as exc:
            timeout_error = exc
    finally:
        # Covers success, non-zero exit, timeout, Celery soft limits,
        # KeyboardInterrupt/SystemExit, and every other Python-visible exit.
        # On Linux, process_guard's PDEATHSIG covers a SIGKILL of this Python
        # process when this finally block cannot run.
        _terminate_process_tree(proc, windows_job=windows_job)
        if (
            (proc.stdout is not None and not proc.stdout.closed)
            or (proc.stderr is not None and not proc.stderr.closed)
        ):
            try:
                drained_stdout, drained_stderr = proc.communicate(
                    timeout=_PIPE_DRAIN_TIMEOUT_SEC,
                )
                stdout = stdout or drained_stdout or ""
                stderr = stderr or drained_stderr or ""
            except Exception:
                for pipe in (proc.stdout, proc.stderr):
                    try:
                        if pipe is not None:
                            pipe.close()
                    except Exception:
                        pass

    if timeout_error is not None:
        raise subprocess.TimeoutExpired(
            cmd,
            timeout_error.timeout,
            output=timeout_error.output,
            stderr=timeout_error.stderr,
        )
    result = subprocess.CompletedProcess(
        args=cmd,
        returncode=proc.returncode,
        stdout=stdout,
        stderr=stderr,
    )
    if check and result.returncode != 0:
        raise FFmpegError(
            f"Command failed ({result.returncode}): {' '.join(cmd)}\n{result.stderr}"
        )
    return result


def _enqueue_output(pipe, q: "_queue.Queue[str]"):
    """Read lines from a pipe and put them into a queue (runs in a thread)."""
    try:
        for line in iter(pipe.readline, ""):
            q.put(line)
        pipe.close()
    except Exception:
        pass


def _progress_popen_kwargs() -> Dict[str, Any]:
    """Return platform-specific flags that isolate FFmpeg's process tree."""
    if os.name == "posix":
        # A dedicated session makes the process id the process-group id, so a
        # cancellation can terminate FFmpeg and any helpers it has spawned.
        return {"start_new_session": True}
    if os.name == "nt":
        # taskkill /T can then target the new process group/tree without
        # affecting the worker's console process group.
        return {
            "creationflags": getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        }
    return {}


def _parent_guarded_command(cmd: List[str]) -> List[str]:
    """Keep a Linux media process tree bound to its Celery pool child."""
    if not sys.platform.startswith("linux"):
        return cmd
    return [
        sys.executable,
        "-m",
        "app.process_guard",
        str(os.getpid()),
        *cmd,
    ]


def _create_windows_kill_job(proc: subprocess.Popen) -> Optional[int]:
    """Place the child in a kill-on-close Job Object when Windows permits it.

    The job handle protects the narrow case where FFmpeg's leader exits before
    cleanup can run: taskkill can no longer discover that leader, but the job
    still owns and can terminate its surviving descendants. This is best
    effort because some hosted Windows environments prohibit nested jobs.
    """
    if os.name != "nt":
        return None
    try:
        import ctypes
        from ctypes import wintypes

        class _IO_COUNTERS(ctypes.Structure):
            _fields_ = [
                ("ReadOperationCount", ctypes.c_ulonglong),
                ("WriteOperationCount", ctypes.c_ulonglong),
                ("OtherOperationCount", ctypes.c_ulonglong),
                ("ReadTransferCount", ctypes.c_ulonglong),
                ("WriteTransferCount", ctypes.c_ulonglong),
                ("OtherTransferCount", ctypes.c_ulonglong),
            ]

        class _BASIC_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", ctypes.c_longlong),
                ("PerJobUserTimeLimit", ctypes.c_longlong),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD),
            ]

        class _EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", _BASIC_LIMIT_INFORMATION),
                ("IoInfo", _IO_COUNTERS),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
        kernel32.CreateJobObjectW.restype = wintypes.HANDLE
        kernel32.SetInformationJobObject.argtypes = [
            wintypes.HANDLE,
            ctypes.c_int,
            ctypes.c_void_p,
            wintypes.DWORD,
        ]
        kernel32.SetInformationJobObject.restype = wintypes.BOOL
        kernel32.AssignProcessToJobObject.argtypes = [
            wintypes.HANDLE,
            wintypes.HANDLE,
        ]
        kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel32.CloseHandle.restype = wintypes.BOOL

        job = kernel32.CreateJobObjectW(None, None)
        if not job:
            return None
        info = _EXTENDED_LIMIT_INFORMATION()
        # If Python is terminated before its cleanup handler completes, the
        # operating system closes this handle and kills the whole child job.
        info.BasicLimitInformation.LimitFlags = 0x00002000
        if not kernel32.SetInformationJobObject(
            job,
            9,  # JobObjectExtendedLimitInformation
            ctypes.byref(info),
            ctypes.sizeof(info),
        ):
            kernel32.CloseHandle(job)
            return None
        process_handle = wintypes.HANDLE(int(proc._handle))
        if not kernel32.AssignProcessToJobObject(job, process_handle):
            error = ctypes.get_last_error()
            kernel32.CloseHandle(job)
            logger.debug(
                "[ffmpeg] Windows Job Object unavailable for process %s "
                "(error %s); using taskkill fallback",
                proc.pid,
                error,
            )
            return None
        return int(job)
    except Exception as exc:
        logger.debug("[ffmpeg] could not create Windows Job Object: %s", exc)
        return None


def _windows_job_has_processes(job_handle: Optional[int]) -> bool:
    if os.name != "nt" or job_handle is None:
        return False
    try:
        import ctypes
        from ctypes import wintypes

        class _BASIC_ACCOUNTING_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("TotalUserTime", ctypes.c_longlong),
                ("TotalKernelTime", ctypes.c_longlong),
                ("ThisPeriodTotalUserTime", ctypes.c_longlong),
                ("ThisPeriodTotalKernelTime", ctypes.c_longlong),
                ("TotalPageFaultCount", wintypes.DWORD),
                ("TotalProcesses", wintypes.DWORD),
                ("ActiveProcesses", wintypes.DWORD),
                ("TotalTerminatedProcesses", wintypes.DWORD),
            ]

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.QueryInformationJobObject.argtypes = [
            wintypes.HANDLE,
            ctypes.c_int,
            ctypes.c_void_p,
            wintypes.DWORD,
            ctypes.c_void_p,
        ]
        kernel32.QueryInformationJobObject.restype = wintypes.BOOL
        info = _BASIC_ACCOUNTING_INFORMATION()
        ok = kernel32.QueryInformationJobObject(
            wintypes.HANDLE(job_handle),
            1,  # JobObjectBasicAccountingInformation
            ctypes.byref(info),
            ctypes.sizeof(info),
            None,
        )
        return bool(ok and info.ActiveProcesses)
    except Exception:
        return False


def _terminate_windows_job(job_handle: Optional[int]) -> bool:
    if os.name != "nt" or job_handle is None:
        return False
    try:
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
        kernel32.TerminateJobObject.restype = wintypes.BOOL
        return bool(
            kernel32.TerminateJobObject(wintypes.HANDLE(job_handle), 1)
        )
    except Exception:
        return False


def _close_windows_job(job_handle: Optional[int]) -> None:
    if os.name != "nt" or job_handle is None:
        return
    try:
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel32.CloseHandle.restype = wintypes.BOOL
        kernel32.CloseHandle(wintypes.HANDLE(job_handle))
    except Exception:
        pass


def _linux_process_group_has_live_members(process_group_id: int) -> Optional[bool]:
    """Return whether a Linux process group has any non-zombie members."""
    if not sys.platform.startswith("linux") or not os.path.isdir("/proc"):
        return None
    try:
        saw_process_stat = False
        with os.scandir("/proc") as entries:
            for entry in entries:
                if not entry.name.isdigit():
                    continue
                try:
                    with open(
                        os.path.join(entry.path, "stat"),
                        "r",
                        encoding="utf-8",
                    ) as proc_stat:
                        value = proc_stat.read()
                    # comm may contain spaces or ')' characters, so parse
                    # fields only after its final closing parenthesis. The
                    # remaining values begin with state, ppid and group id.
                    fields = value[value.rfind(")") + 2 :].split()
                    state = fields[0]
                    member_group_id = int(fields[2])
                    saw_process_stat = True
                    if (
                        member_group_id == process_group_id
                        and state not in {"X", "Z"}
                    ):
                        return True
                except (
                    FileNotFoundError,
                    PermissionError,
                    ValueError,
                    IndexError,
                ):
                    continue
        if saw_process_stat:
            return False
    except OSError:
        pass
    return None


def _process_group_exists(
    proc: subprocess.Popen,
    windows_job: Optional[int] = None,
) -> bool:
    """Best-effort check for the isolated process group."""
    if os.name != "posix":
        if _windows_job_has_processes(windows_job):
            return True
        try:
            return proc.poll() is None
        except Exception:
            return False
    linux_group_alive = _linux_process_group_has_live_members(proc.pid)
    if linux_group_alive is not None:
        return linux_group_alive
    try:
        os.killpg(proc.pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        try:
            return proc.poll() is None
        except Exception:
            return False


def _signal_process_tree(
    proc: subprocess.Popen,
    force: bool,
    windows_job: Optional[int] = None,
) -> None:
    """Signal FFmpeg's complete process tree without raising cleanup errors."""
    try:
        if os.name == "posix":
            os.killpg(proc.pid, signal.SIGKILL if force else signal.SIGTERM)
            return
        if os.name == "nt":
            if force and _terminate_windows_job(windows_job):
                return
            taskkill_cmd = ["taskkill", "/PID", str(proc.pid), "/T"]
            if force:
                taskkill_cmd.append("/F")
            try:
                subprocess.run(
                    taskkill_cmd,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                    timeout=5,
                )
            except (OSError, subprocess.SubprocessError):
                # taskkill can be unavailable in stripped-down local test
                # environments; Popen still gives us a direct-child fallback.
                if proc.poll() is None:
                    (proc.kill if force else proc.terminate)()
            return
        if proc.poll() is None:
            (proc.kill if force else proc.terminate)()
    except ProcessLookupError:
        pass
    except Exception as exc:
        logger.warning(
            "[ffmpeg] failed to %s process tree %s: %s",
            "kill" if force else "terminate",
            getattr(proc, "pid", "?"),
            exc,
        )


def _wait_for_process_tree(
    proc: subprocess.Popen,
    timeout: float,
    windows_job: Optional[int] = None,
) -> bool:
    """Wait a bounded time for the process group to exit and reap its leader."""
    import time as _time

    deadline = _time.monotonic() + max(0.0, timeout)
    while True:
        try:
            returncode = proc.poll()
        except Exception:
            returncode = None
        group_exists = _process_group_exists(proc, windows_job)
        if returncode is not None and not group_exists:
            try:
                proc.wait(timeout=0)
            except Exception:
                pass
            return True
        remaining = deadline - _time.monotonic()
        if remaining <= 0:
            return False
        _time.sleep(min(_PROCESS_WAIT_POLL_SEC, remaining))


def _terminate_process_tree(
    proc: subprocess.Popen,
    windows_job: Optional[int] = None,
) -> None:
    """Terminate, then kill if needed, and always reap the FFmpeg process."""
    try:
        _signal_process_tree(proc, force=False, windows_job=windows_job)
        if not _wait_for_process_tree(
            proc,
            _PROCESS_TERMINATE_TIMEOUT_SEC,
            windows_job=windows_job,
        ):
            logger.warning(
                "[ffmpeg] process tree %s ignored terminate; escalating to kill",
                getattr(proc, "pid", "?"),
            )
            _signal_process_tree(proc, force=True, windows_job=windows_job)
            if not _wait_for_process_tree(
                proc,
                _PROCESS_KILL_TIMEOUT_SEC,
                windows_job=windows_job,
            ):
                logger.error(
                    "[ffmpeg] process tree %s did not exit after kill",
                    getattr(proc, "pid", "?"),
                )
        # poll()/wait() above normally reaps the process. Keep this final
        # bounded wait for platforms/fakes whose poll does not reap.
        try:
            proc.wait(timeout=0)
        except Exception:
            pass
    finally:
        # KILL_ON_JOB_CLOSE is the final Windows safety net for any descendant
        # that outlived or detached from its process-group leader.
        _close_windows_job(windows_job)


_CANCELLATION_EXCEPTION_NAMES = {
    "SoftTimeLimitExceeded",
    "TimeLimitExceeded",
    "TaskRevokedError",
    "Terminated",
    "WorkerLostError",
    "WorkerShutdown",
    "WorkerTerminate",
}


def _report_progress(
    on_progress: Optional[Callable[[int], None]], percent: int
) -> None:
    """Report progress without letting an observability failure kill FFmpeg."""
    if on_progress is None:
        return
    try:
        on_progress(percent)
    except BaseException as exc:
        # Celery control-flow signals and Python cancellation must unwind so
        # run_cmd_with_progress's finally block can terminate FFmpeg. Ordinary
        # backend/callback failures are observability failures, not encode
        # failures, and therefore must not abandon a healthy encoder.
        if (
            not isinstance(exc, Exception)
            or type(exc).__name__ in _CANCELLATION_EXCEPTION_NAMES
        ):
            raise
        logger.exception(
            "[ffmpeg] progress callback failed at %d%%; encoding continues",
            percent,
        )


def _parse_progress_line(
    line: str,
    total_duration: float,
    on_progress: Optional[Callable[[int], None]],
    last_percent: int,
) -> int:
    """Parse one line from FFmpeg's machine-readable progress stream."""
    line = line.strip()
    if line.startswith("out_time_ms="):
        try:
            out_ms = int(line.split("=", 1)[1])
            if total_duration > 0:
                pct = min(
                    99,
                    int((out_ms / 1_000_000 / total_duration) * 100),
                )
                if pct > last_percent:
                    _report_progress(on_progress, pct)
                    return pct
        except (ValueError, ZeroDivisionError):
            pass
    elif line.startswith("progress=") and "end" in line and last_percent < 100:
        _report_progress(on_progress, 100)
        return 100
    return last_percent


def _drain_queue(q: "_queue.Queue[str]") -> List[str]:
    """Remove and return every currently queued line."""
    import queue as _queue

    lines: List[str] = []
    while True:
        try:
            lines.append(q.get_nowait())
        except _queue.Empty:
            return lines


def run_cmd_with_progress(
    cmd: List[str],
    total_duration: float,
    on_progress: Optional[Callable[[int], None]] = None,
    stall_timeout: Optional[float] = None,
    cancel_event: Optional[Any] = None,
    wall_timeout: Optional[float] = None,
) -> subprocess.CompletedProcess:
    """Run FFmpeg with -progress pipe:1 and report percentage via callback.

    Fixes pipe buffer deadlock by using threads to read both stdout and stderr
    concurrently. Without this, ffmpeg's stderr can fill the pipe buffer (64KB)
    causing a deadlock when the reader is blocked on stdout.

    Also adds -loglevel error to minimise stderr output, reducing the chance of
    buffer saturation in the first place.

    ``stall_timeout`` (seconds): if ffmpeg produces no progress output for this
    duration, it is considered hung (a known NVENC / CUDA deadlock scenario)
    and is killed so the Celery task can fail and retry instead of blocking the
    worker forever. Defaults to ``Settings.FFMPEG_STALL_TIMEOUT_SEC`` (300s).

    ``cancel_event`` is checked at least once per second. It lets a task-owned
    GPU lease heartbeat stop FFmpeg before an expired reservation can be
    reassigned to another encoder.

    ``wall_timeout`` is a hard wall-clock budget for FFmpeg itself.  It is
    intentionally shorter than the Celery soft limit for bounded fallback
    work, leaving time to terminate/reap the process tree and either replace
    or retry the task without a hard worker kill.
    """
    if stall_timeout is None:
        try:
            stall_timeout = get_settings().FFMPEG_STALL_TIMEOUT_SEC
        except Exception:
            stall_timeout = 300.0
    cmd = cmd + ["-loglevel", "error", "-progress", "pipe:1", "-nostats"]
    logger.info("[ffmpeg] %s", " ".join(cmd))

    import queue as _queue
    import threading
    import time as _time

    stdout_queue: _queue.Queue[str] = _queue.Queue()
    stderr_queue: _queue.Queue[str] = _queue.Queue()
    reader_threads: List[threading.Thread] = []
    remaining_stdout: List[str] = []
    windows_job: Optional[int] = None
    last_percent = -1
    started_at = _time.monotonic()
    last_output_time = started_at
    killed_for_stall = False
    killed_for_wall_timeout = False
    cancelled = False

    proc = subprocess.Popen(
        _parent_guarded_command(cmd),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        **_progress_popen_kwargs(),
    )
    try:
        windows_job = _create_windows_kill_job(proc)
        t_out = threading.Thread(
            target=_enqueue_output,
            args=(proc.stdout, stdout_queue),
            name=f"ffmpeg-stdout-{proc.pid}",
            daemon=True,
        )
        t_err = threading.Thread(
            target=_enqueue_output,
            args=(proc.stderr, stderr_queue),
            name=f"ffmpeg-stderr-{proc.pid}",
            daemon=True,
        )
        for reader in (t_out, t_err):
            reader.start()
            reader_threads.append(reader)

        while True:
            if (
                wall_timeout is not None
                and wall_timeout > 0
                and (_time.monotonic() - started_at) > wall_timeout
            ):
                logger.error(
                    "[ffmpeg] wall timeout after %.0fs; terminating process "
                    "tree: %s",
                    wall_timeout,
                    " ".join(cmd[:6]),
                )
                killed_for_wall_timeout = True
                break
            if cancel_event is not None and cancel_event.is_set():
                logger.error(
                    "[ffmpeg] cancellation requested; terminating process tree: %s",
                    " ".join(cmd[:6]),
                )
                cancelled = True
                break
            try:
                line = stdout_queue.get(timeout=1.0)
                last_output_time = _time.monotonic()
            except _queue.Empty:
                if proc.poll() is not None:
                    break
                # Stall detection: no progress output for stall_timeout seconds.
                if (
                    stall_timeout > 0
                    and (_time.monotonic() - last_output_time) > stall_timeout
                ):
                    logger.error(
                        "[ffmpeg] no progress for %.0fs, terminating hung process tree: %s",
                        stall_timeout,
                        " ".join(cmd[:6]),
                    )
                    killed_for_stall = True
                    break
                continue
            last_percent = _parse_progress_line(
                line,
                total_duration,
                on_progress,
                last_percent,
            )
    finally:
        # This runs for every Python-visible exit path, including Celery
        # cancellation/timeouts, KeyboardInterrupt/SystemExit, reader errors,
        # progress callback control-flow exceptions, stalls and non-zero exits.
        _terminate_process_tree(proc, windows_job=windows_job)
        for reader in reader_threads:
            reader.join(timeout=_PIPE_DRAIN_TIMEOUT_SEC)
            if reader.is_alive():
                logger.warning(
                    "[ffmpeg] %s reader did not finish draining within %.0fs",
                    reader.name,
                    _PIPE_DRAIN_TIMEOUT_SEC,
                )
        remaining_stdout = _drain_queue(stdout_queue)

    for line in remaining_stdout:
        last_percent = _parse_progress_line(
            line,
            total_duration,
            on_progress,
            last_percent,
        )

    stderr = "".join(_drain_queue(stderr_queue))
    if cancelled:
        raise FFmpegError(
            "FFmpeg cancelled because its GPU reservation was lost; "
            f"process tree terminated: {' '.join(cmd[:6])}\n{stderr}"
        )
    if killed_for_stall:
        raise FFmpegError(
            f"FFmpeg stalled (no progress for {stall_timeout:.0f}s), process tree terminated: {' '.join(cmd[:6])}\n{stderr}"
        )
    if killed_for_wall_timeout:
        raise FFmpegError(
            f"FFmpeg exceeded its {wall_timeout:.0f}s wall-clock budget; "
            f"process tree terminated: {' '.join(cmd[:6])}\n{stderr}"
        )
    if proc.returncode != 0:
        raise FFmpegError(
            f"Command failed ({proc.returncode}): {' '.join(cmd)}\n{stderr}"
        )
    return subprocess.CompletedProcess(
        args=cmd, returncode=proc.returncode, stdout="", stderr=stderr
    )


def ffprobe(path: str) -> Dict[str, Any]:
    """Return ffprobe metadata enriched with initial packet timestamps.

    A container's stream ``start_time`` is not always the timestamp of the
    first decodable packet.  Matroska files in particular can declare a zero
    stream start while a language track begins much later.  Independent HLS
    audio packaging must preserve that real offset, so collect it during the
    normal (once-per-upload) probe rather than guessing from stream metadata.
    """
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-print_format",
        "json",
        "-show_streams",
        "-show_format",
        path,
    ]
    probe = json.loads(run_cmd(cmd).stdout)
    streams = probe.get("streams", [])
    if not isinstance(streams, list):
        return probe

    video_seen = 0
    audio_seen = 0
    for stream in streams:
        if not isinstance(stream, dict):
            continue
        codec_type = stream.get("codec_type")
        if codec_type == "video":
            selector = f"v:{video_seen}"
            video_seen += 1
        elif codec_type == "audio":
            selector = f"a:{audio_seen}"
            audio_seen += 1
        else:
            continue
        first_packet_time = _ffprobe_first_packet_time(path, selector)
        if first_packet_time is not None:
            stream["first_packet_time"] = first_packet_time
    return probe


def _ffprobe_first_packet_time(path: str, selector: str) -> Optional[float]:
    """Return the first presentation timestamp for one selected stream.

    The very short interval intentionally seeks to the start of the file.
    ffprobe returns the first packet of a stream even when that stream has an
    authored leading gap, without scanning the entire upload.
    """
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        selector,
        "-read_intervals",
        "0%+0.001",
        "-show_packets",
        "-show_entries",
        "packet=pts_time",
        "-of",
        "json",
        path,
    ]
    try:
        payload = json.loads(run_cmd(cmd).stdout)
        packets = payload.get("packets", [])
        if not isinstance(packets, list) or not packets:
            return None
        value = packets[0].get("pts_time")
        timestamp = float(value)
        return timestamp if math.isfinite(timestamp) else None
    except (FFmpegError, OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        logger.warning(
            "[ffprobe] unable to read initial packet timestamp for %s: %s",
            selector,
            exc,
        )
        return None


def ffprobe_video_packets(path: str) -> List[Dict[str, Any]]:
    """Return the minimal packet timeline needed to validate a copied HLS."""
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "packet=pts_time,dts_time,duration_time,flags",
        "-of",
        "json",
        path,
    ]
    payload = json.loads(run_cmd(cmd).stdout)
    packets = payload.get("packets", [])
    return packets if isinstance(packets, list) else []


# ----------------------------------------------------------------------------
# GPU / encoder detection
# ----------------------------------------------------------------------------


def detect_gpu() -> bool:
    """Probe whether h264_nvenc is usable.

    The test frame must exceed NVENC's minimum supported dimensions; a 64x64
    frame is rejected with "Frame Dimension less than the minimum supported
    value" on recent drivers, which silently forces every transcode onto the
    CPU. 256x256 is safely above the limit on all known NVIDIA drivers.
    """
    if not get_settings().GPU_ENABLED:
        logger.info("[ffmpeg] GPU explicitly disabled")
        return False
    cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-f",
        "lavfi",
        "-i",
        "color=c=black:s=256x256:d=1",
        "-c:v",
        "h264_nvenc",
        "-f",
        "null",
        "-",
    ]
    try:
        # Startup detection must never prevent the worker from reaching Celery.
        # A wedged driver/runtime is a failed probe, not an indefinitely hung
        # container that cannot consume the CPU fallback queue.
        result = run_cmd(cmd, check=False, timeout=15)
        is_gpu_working = result.returncode == 0
        if is_gpu_working:
            logger.info("[ffmpeg] NVENC GPU available and working")
        else:
            logger.warning("[ffmpeg] NVENC not available, falling back to CPU")
        return is_gpu_working
    except Exception as e:
        logger.warning("[ffmpeg] GPU detection failed: %s", e)
        return False


_GPU_AVAILABLE: Optional[bool] = None


def is_gpu_available() -> bool:
    global _GPU_AVAILABLE
    if _GPU_AVAILABLE is None:
        _GPU_AVAILABLE = detect_gpu()
        logger.info("GPU (h264_nvenc) available: %s", _GPU_AVAILABLE)
    return _GPU_AVAILABLE


def get_video_encoder(force_gpu: Optional[bool] = None) -> str:
    if force_gpu is None:
        return "h264_nvenc" if is_gpu_available() else "libx264"
    return "h264_nvenc" if force_gpu else "libx264"


_FILTERS_CACHE: Optional[set] = None
_ENCODERS_CACHE: Optional[set] = None


def _ffmpeg_filters() -> set:
    """Return the set of ffmpeg filter names (cached)."""
    global _FILTERS_CACHE
    if _FILTERS_CACHE is None:
        try:
            result = run_cmd(["ffmpeg", "-hide_banner", "-filters"], check=False)
            names = set()
            for line in result.stdout.splitlines():
                # Lines: "<flags> <name> <IO->IO> <description>" e.g. ".. C. scale_npp N->V ..."
                parts = line.split()
                if len(parts) >= 3 and "->" in parts[2] and re.match(r"^[a-z0-9_]+$", parts[1]):
                    names.add(parts[1])
            _FILTERS_CACHE = names
        except Exception:
            _FILTERS_CACHE = set()
    return _FILTERS_CACHE


def _ffmpeg_encoders() -> set:
    """Return the set of ffmpeg encoder names (cached)."""
    global _ENCODERS_CACHE
    if _ENCODERS_CACHE is None:
        try:
            result = run_cmd(["ffmpeg", "-hide_banner", "-encoders"], check=False)
            names = set()
            for line in result.stdout.splitlines():
                # Lines: "<flags> <name> <description>" e.g. " V..... h264_nvenc NVIDIA ..."
                parts = line.split()
                if (
                    len(parts) >= 2
                    and parts[0]
                    and parts[0][0] in "VASD"
                    and re.match(r"^[A-Za-z0-9_-]+$", parts[1])
                ):
                    names.add(parts[1])
            _ENCODERS_CACHE = names
        except Exception:
            _ENCODERS_CACHE = set()
    return _ENCODERS_CACHE


def _gpu_scaler() -> Optional[str]:
    """Return the best available GPU scaling filter, or None."""
    filters = _ffmpeg_filters()
    if "scale_npp" in filters:
        return "scale_npp"
    if "scale_cuda" in filters:
        return "scale_cuda"
    return None


_GPU_CODEC_ENCODERS = {
    "h264": "h264_nvenc",
    "hevc": "hevc_nvenc",
    "av1": "av1_nvenc",
}
_CPU_CODEC_ENCODERS = {
    "h264": "libx264",
    "hevc": "libx265",
    "av1": "libaom-av1",
}


def _gpu_encoder(codec: str) -> str:
    """Map a codec to an NVENC encoder, falling back to h264_nvenc."""
    encoders = _ffmpeg_encoders()
    enc = _GPU_CODEC_ENCODERS.get(
        (codec or "h264").lower(),
        "h264_nvenc",
    )
    if enc in encoders:
        return enc
    # detect_gpu() already proved h264_nvenc works on this host; if the
    # encoders parser somehow missed it (race / partial output), trust the
    # probe so we don't silently fall back to a slow CPU encode.
    if (codec or "h264").lower() == "h264" and is_gpu_available():
        return "h264_nvenc"
    if "h264_nvenc" in encoders:
        return "h264_nvenc"
    return "libx264"  # caller should treat non-nvenc as CPU


def _required_gpu_encoder(codec: str) -> str:
    """Resolve an exact NVENC encoder without launching a test encode."""
    normalized = (codec or "h264").lower()
    enc = _GPU_CODEC_ENCODERS.get(normalized)
    if enc is None:
        raise FFmpegError(f"unsupported GPU codec {codec}")
    if enc not in _ffmpeg_encoders():
        raise FFmpegError(
            f"required NVENC encoder is unavailable for codec {codec}"
        )
    return enc


def _cpu_encoder(codec: str) -> str:
    """Map a codec to a CPU encoder, falling back to libx264."""
    encoders = _ffmpeg_encoders()
    enc = _CPU_CODEC_ENCODERS.get(
        (codec or "h264").lower(),
        "libx264",
    )
    if enc in encoders:
        return enc
    return "libx264"


def select_hwaccel_args(gpu_index: Optional[int], use_gpu: bool) -> List[str]:
    """Hardware-accelerated decode args. Zero-copy only when a GPU scaler
    exists; otherwise decode-accel-only (frames download to CPU for a CPU
    scale, then NVENC uploads). Returns [] for CPU."""
    if not use_gpu:
        return []
    args = ["-hwaccel", "cuda"]
    if gpu_index is not None:
        args += ["-hwaccel_device", str(gpu_index)]
    if _gpu_scaler() is not None:
        args += ["-hwaccel_output_format", "cuda"]
    return args


def _cuda_filter_device_args(gpu_index: Optional[int]) -> List[str]:
    """Initialize CUDA for filters while leaving input decoding on the CPU."""
    device = "cuda=gpu"
    if gpu_index is not None:
        device += f":{int(gpu_index)}"
    return ["-init_hw_device", device, "-filter_hw_device", "gpu"]


def nvenc_max_sessions() -> int:
    """Best-effort NVENC concurrent session limit for the installed GPU."""
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=5,
        )
        if result.returncode == 0:
            name = (result.stdout or "").lower()
            # Data-center / professional parts have no consumer 3-session cap.
            if any(k in name for k in ("tesla", "quadro", "t4", "a100", "a10",
                                       "l4", "l40", "v100", "a16", "rtx a")):
                return 32
            return 3
    except Exception:
        pass
    return 3


# ----------------------------------------------------------------------------
# Geometry / ladders
# ----------------------------------------------------------------------------


def _even(n: int) -> int:
    """Ensure a dimension is even (H.264 requirement)."""
    return n if n % 2 == 0 else n + 1


def width_for_height(src_w: int, src_h: int, target_h: int) -> int:
    if src_h == 0:
        return 0
    return _even(int(round(src_w * target_h / src_h)))


QUALITY_LADDER = {
    # Width is a bounding-box ceiling, not a forced display aspect ratio.
    # This prevents cinematic sources from turning a nominal 720p rung into
    # 1720x720 (and a 480p rung into 1146x480), which wastes encoder work and
    # bandwidth. ``force_original_aspect_ratio=decrease`` keeps the picture
    # undistorted inside these standard HLS boxes.
    2160: {"width": 3840, "bitrate": 14_000_000, "label": "4K"},
    1080: {"width": 1920, "bitrate": 6_000_000, "label": "Full HD"},
    720:  {"width": 1280, "bitrate": 3_000_000, "label": "HD"},
    480:  {"width": 854,  "bitrate": 1_500_000, "label": "SD"},
    360:  {"width": 640,  "bitrate": 800_000,   "label": "Low"},
    240:  {"width": 426,  "bitrate": 400_000,   "label": "Very Low"},
    144:  {"width": 256,  "bitrate": 200_000,   "label": "Minimal"},
}


def get_ladder(source_height: int, source_width: int) -> List[Dict[str, int]]:
    """Return a default bitrate ladder up to the source resolution."""
    ladder = [
        {
            "height": height,
            "width": entry["width"],
            "bitrate": entry["bitrate"],
        }
        for height, entry in QUALITY_LADDER.items()
        if height >= 360
    ]
    selected = [r for r in ladder if r["height"] <= source_height]
    if not selected:
        selected = [ladder[-1]]
    for rung in selected:
        rung["width"] = min(
            int(rung["width"]),
            width_for_height(source_width, source_height, rung["height"]),
        )
    return selected


def get_ladder_for_qualities(
    source_height: int,
    source_width: int,
    qualities: List[int],
) -> List[Dict[str, int]]:
    """Return a bitrate ladder for the requested qualities, capped by source res."""
    selected = []
    for q in sorted(qualities, reverse=True):
        if q > source_height:
            continue
        entry = QUALITY_LADDER.get(
            q,
            {
                "width": width_for_height(source_width, source_height, q),
                "bitrate": 800_000,
            },
        )
        rung = {
            "height": q,
            "bitrate": entry["bitrate"],
            "width": min(
                int(entry["width"]),
                width_for_height(source_width, source_height, q),
            ),
        }
        selected.append(rung)
    if not selected:
        q = min(qualities) if qualities else 360
        entry = QUALITY_LADDER.get(
            q,
            {
                "width": width_for_height(source_width, source_height, q),
                "bitrate": 800_000,
            },
        )
        rung = {
            "height": q,
            "bitrate": entry["bitrate"],
            "width": min(
                int(entry["width"]),
                width_for_height(source_width, source_height, q),
            ),
        }
        selected.append(rung)
    return selected


# ----------------------------------------------------------------------------
# Per-title complexity
# ----------------------------------------------------------------------------


_YDIF_RE = re.compile(r"YDIF=(\d+)")
_YMAX_RE = re.compile(r"YMAX=(\d+)")
_YMIN_RE = re.compile(r"YMIN=(\d+)")


def _sample_complexity(input_path: str, start: float, dur: float) -> Optional[Tuple[float, float]]:
    """Run signalstats on a short sample; return (mean_ydif, mean_range)."""
    cmd = [
        "ffmpeg", "-hide_banner", "-y",
        "-ss", f"{start:.3f}",
        "-t", f"{dur:.3f}",
        "-i", input_path,
        "-an", "-vf", "signalstats,metadata=print",
        "-f", "null", "-",
    ]
    try:
        result = run_cmd(cmd, check=False, timeout=60)
    except Exception as exc:
        logger.warning("[complexity] sample failed at %.1fs: %s", start, exc)
        return None
    blob = (result.stdout or "") + (result.stderr or "")
    ydifs = [int(x) for x in _YDIF_RE.findall(blob)]
    ymaxs = [int(x) for x in _YMAX_RE.findall(blob)]
    ymins = [int(x) for x in _YMIN_RE.findall(blob)]
    if not ydifs:
        return None
    mean_ydif = sum(ydifs) / len(ydifs)
    ranges = [a - b for a, b in zip(ymaxs, ymins) if a >= b]
    mean_range = (sum(ranges) / len(ranges)) if ranges else 0.0
    return mean_ydif, mean_range


def analyze_complexity(input_path: str, duration: float, sample_seconds: float = 10.0) -> float:
    """Return a 0..1 content complexity score (spatial + temporal).

    Fast: samples a few short windows with ffmpeg signalstats and combines the
    average temporal motion (YDIF) and spatial dynamic range (YMAX-YMIN).
    Completes in seconds, not minutes. Falls back to 0.5 on any failure.
    """
    duration = float(duration or 0)
    if duration <= 0 or not os.path.exists(input_path):
        return 0.5
    sample_seconds = max(2.0, min(float(sample_seconds), 15.0))
    # Up to three sample windows: beginning, middle, near-end.
    mid = duration / 2.0
    end = max(0.0, duration - sample_seconds)
    starts = []
    for s in (0.0, mid - sample_seconds / 2.0, end):
        s = max(0.0, min(s, max(0.0, duration - sample_seconds)))
        if s not in starts:
            starts.append(s)
    ydifs, ranges = [], []
    for s in starts:
        res = _sample_complexity(input_path, s, sample_seconds)
        if res:
            ydifs.append(res[0])
            ranges.append(res[1])
    if not ydifs:
        return 0.5
    mean_ydif = sum(ydifs) / len(ydifs)
    mean_range = sum(ranges) / len(ranges)
    # Normalize: YDIF ~0..30 (motion), range ~0..255 (spatial).
    temporal = min(1.0, mean_ydif / 18.0)
    spatial = min(1.0, mean_range / 180.0)
    score = 0.5 * temporal + 0.5 * spatial
    return max(0.0, min(1.0, score))


_CODEC_EFFICIENCY = {"h264": 1.0, "hevc": 0.6, "av1": 0.5}


def get_per_title_ladder(
    source_height: int,
    source_width: int,
    qualities: List[int],
    complexity: float,
    codec: str = "h264",
) -> List[Dict[str, Any]]:
    """Complexity- and codec-aware bitrate ladder.

    Bitrate is scaled by a complexity factor (0.7x for simple content up to
    1.3x for complex) and by codec efficiency (HEVC ~0.6x, AV1 ~0.5x vs
    H.264). Each rung carries height/width/bitrate/codec/level.
    """
    base = get_ladder_for_qualities(source_height, source_width, qualities)
    complexity = max(0.0, min(1.0, float(complexity)))
    cf = 0.7 + 0.6 * complexity          # 0.7 .. 1.3
    eff = _CODEC_EFFICIENCY.get((codec or "h264").lower(), 1.0)
    factor = cf * eff
    out = []
    for r in base:
        h = r["height"]
        out.append({
            "height": h,
            "width": r["width"],
            "bitrate": max(200_000, int(r["bitrate"] * factor)),
            "codec": codec,
            "level": _level_label(codec, h),
        })
    return out


def _level_label(codec: str, height: int) -> str:
    c = (codec or "h264").lower()
    if c == "hevc":
        return "5.1" if height >= 2160 else ("4.1" if height > 720 else "4.0")
    if c == "av1":
        return "5.1" if height >= 2160 else ("4.0" if height > 720 else "3.1")
    if height >= 2160:
        return "5.1"
    if height > 720:
        return "4.1"
    if height > 480:
        return "4.0"
    if height > 360:
        return "4.0"
    if height > 240:
        return "3.1"
    return "3.0"


_DIRECT_H264_PROFILE_PREFIX = "direct-h264:"
_H264_RFC6381_PROFILE_BYTES = {
    "baseline": "4200",
    "constrained baseline": "42e0",
    "main": "4d00",
    "high": "6400",
}


def direct_h264_profile_metadata(profile: Any, level: Any) -> str:
    """Serialize direct-play H.264 profile/level into the existing DB field.

    Encoded renditions continue to persist their historical ``"high"`` value.
    The namespaced value is only used for direct stream-copy renditions, where
    packaging must advertise the source bitstream instead of inferring a level
    from output height.
    """
    normalized_profile = str(profile or "").strip().lower()
    if normalized_profile not in _H264_RFC6381_PROFILE_BYTES:
        raise ValueError("unsupported direct-play H.264 profile")
    try:
        normalized_level = int(level)
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid direct-play H.264 level") from exc
    if not 10 <= normalized_level <= 52:
        raise ValueError("unsupported direct-play H.264 level")
    return (
        f"{_DIRECT_H264_PROFILE_PREFIX}"
        f"{normalized_profile}:{normalized_level}"
    )


def _direct_h264_codecs_string(profile_metadata: Any) -> Optional[str]:
    value = str(profile_metadata or "").strip().lower()
    if not value.startswith(_DIRECT_H264_PROFILE_PREFIX):
        return None
    payload = value[len(_DIRECT_H264_PROFILE_PREFIX):]
    try:
        profile, raw_level = payload.rsplit(":", 1)
        level = int(raw_level)
        profile_bytes = _H264_RFC6381_PROFILE_BYTES[profile]
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("invalid persisted direct-play H.264 metadata") from exc
    if not 10 <= level <= 52:
        raise ValueError("unsupported persisted direct-play H.264 level")
    return f"avc1.{profile_bytes}{level:02x}"


def codecs_string(codec: str, height: int, profile: str = "high") -> str:
    """Return an HLS CODECS attribute string for a rendition."""
    h = int(height or 1080)
    c = (codec or "h264").lower()
    if c == "hevc":
        return "hvc1.1.6.L150.B0" if h >= 2160 else "hvc1.1.6.L120.B0"
    if c == "av1":
        return "av01.0.12M.08" if h >= 2160 else "av01.0.08M.08"
    direct_codecs = _direct_h264_codecs_string(profile)
    if direct_codecs is not None:
        return direct_codecs
    # H.264 High profile (profile_idc=0x64, constraint=0x00).
    if h >= 2160:
        lvl = "33"   # 5.1
    elif h > 720:
        lvl = "29"   # 4.1
    elif h > 480:
        lvl = "28"   # 4.0
    elif h > 360:
        lvl = "28"
    elif h > 240:
        lvl = "1f"   # 3.1
    else:
        lvl = "1e"   # 3.0
    return f"avc1.6400{lvl}"


# ----------------------------------------------------------------------------
# Probe parsing
# ----------------------------------------------------------------------------


def _frame_rate(stream: Dict[str, Any]) -> float:
    value = stream.get("r_frame_rate", "0/1")
    try:
        return float(eval(value))
    except Exception:
        return 0.0


def _safe_start_time(stream: Dict[str, Any]) -> float:
    """Return a stream's start_time in seconds (0 if absent / unparseable).

    ffprobe reports per-stream `start_time` for containers that carry an
    edit-list or an initial PTS offset (MP4 elst, Matroska CueTime, TS PCR
    wrap, etc.). A non-zero audio start_time relative to video is the most
    common cause of lipsync drift after re-muxing.
    """
    val = stream.get("start_time")
    if val is None:
        return 0.0
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def _effective_start_time(stream: Dict[str, Any]) -> float:
    """Return a stream's first decodable PTS, with metadata as fallback."""
    value = stream.get("first_packet_time")
    if value is not None:
        try:
            timestamp = float(value)
            if math.isfinite(timestamp):
                return timestamp
        except (TypeError, ValueError):
            pass
    return _safe_start_time(stream)


def audio_delay_ms(stream: Dict[str, Any], video_start: float = 0.0) -> float:
    """Return the source audio/video presentation offset in milliseconds.

    Video and audio are packaged independently, so their streams must retain
    the relative timestamps that a source player such as VLC uses.  Re-basing
    both streams to zero independently drops this offset and produces a
    language-specific lip-sync error even when the original file is correct.

    Codec priming metadata such as ``encoder_delay`` is deliberately ignored:
    FFmpeg's decoder/encoder handles it and it is not a program-level A/V
    offset.
    """
    return (_effective_start_time(stream) - float(video_start or 0.0)) * 1000.0


def _video_rotation(stream: Dict[str, Any]) -> float:
    """Return display rotation metadata in degrees, defaulting to zero."""
    values = [(stream.get("tags", {}) or {}).get("rotate")]
    values.extend(
        side_data.get("rotation")
        for side_data in (stream.get("side_data_list", []) or [])
        if isinstance(side_data, dict)
    )
    for value in values:
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            return float("nan")
    return 0.0


def _stream_duration(stream: Dict[str, Any]) -> float:
    """Return a positive stream duration, including Matroska DURATION tags."""
    try:
        duration = float(stream.get("duration", 0) or 0)
    except (TypeError, ValueError):
        duration = 0.0
    if math.isfinite(duration) and duration > 0:
        return duration

    raw_tag = (stream.get("tags", {}) or {}).get("DURATION")
    if raw_tag is None:
        return 0.0
    try:
        hours, minutes, seconds = str(raw_tag).strip().split(":")
        duration = (
            float(hours) * 3600.0
            + float(minutes) * 60.0
            + float(seconds)
        )
    except (TypeError, ValueError):
        return 0.0
    return duration if math.isfinite(duration) and duration > 0 else 0.0


def parse_probe(probe: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize ffprobe output into the metadata we need."""
    video = next(
        (s for s in probe.get("streams", []) if s.get("codec_type") == "video"), {}
    )

    audio = []
    subtitles = []
    for s in probe.get("streams", []):
        if s.get("codec_type") == "audio":
            s["audio_index"] = len(audio)
            audio.append(s)
        elif s.get("codec_type") == "subtitle":
            s["subtitle_index"] = len(subtitles)
            subtitles.append(s)

    fmt = probe.get("format", {})
    duration = float(fmt.get("duration", 0) or 0)
    video_duration = _stream_duration(video) or duration

    def lang(stream: Dict[str, Any]) -> str:
        return (
            stream.get("tags", {}).get("language")
            or stream.get("language")
            or "und"
        )

    for s in audio + subtitles:
        s["language"] = lang(s)

    video_start = _effective_start_time(video)
    for a in audio:
        # Preserve the declared timestamp for diagnostics, but calculate HLS
        # alignment from the first packet whenever ffprobe could determine it.
        a["start_time"] = _safe_start_time(a)
        a["effective_start_time"] = _effective_start_time(a)
        a["delay_ms"] = audio_delay_ms(a, video_start)

    return {
        "duration": duration,
        "video_duration": video_duration,
        "width": int(video.get("width", 0) or 0),
        "height": int(video.get("height", 0) or 0),
        "video_codec": video.get("codec_name"),
        "video_profile": video.get("profile"),
        "video_level": video.get("level"),
        "video_pix_fmt": video.get("pix_fmt"),
        "video_field_order": video.get("field_order"),
        "video_sample_aspect_ratio": video.get("sample_aspect_ratio"),
        "video_rotation": _video_rotation(video),
        "video_bitrate": int(video.get("bit_rate", 0) or fmt.get("bit_rate", 0) or 0),
        "frame_rate": _frame_rate(video),
        "video_start_time": video_start,
        "audio_tracks": audio,
        "subtitle_tracks": subtitles,
    }


_H264_DIRECT_PLAY_PROFILES = {
    "baseline",
    "constrained baseline",
    "main",
    "high",
}


def h264_direct_play_eligibility(
    probe: Dict[str, Any],
    requested_codec: str,
) -> Tuple[bool, str]:
    """Fail-closed H.264/HLS stream-copy compatibility decision."""
    if str(requested_codec or "").strip().lower() != "h264":
        return False, "requested output codec is not h264"
    if str(probe.get("video_codec") or "").strip().lower() != "h264":
        return False, "source video codec is not h264"
    if str(probe.get("video_pix_fmt") or "").strip().lower() != "yuv420p":
        return False, "source pixel format is not 8-bit yuv420p"
    if (
        str(probe.get("video_field_order") or "").strip().lower()
        != "progressive"
    ):
        return False, "source is interlaced or field order is unknown"
    if str(probe.get("video_sample_aspect_ratio") or "").strip() != "1:1":
        return False, "source sample aspect ratio is missing or non-square"

    profile = str(probe.get("video_profile") or "").strip().lower()
    if profile not in _H264_DIRECT_PLAY_PROFILES:
        return False, "source H.264 profile is missing or unsupported"

    try:
        width = int(probe.get("width"))
        height = int(probe.get("height"))
        frame_rate = float(probe.get("frame_rate"))
        duration = float(probe.get("duration"))
        bitrate = int(probe.get("video_bitrate"))
        level = int(probe.get("video_level"))
        rotation = float(probe.get("video_rotation"))
    except (TypeError, ValueError):
        return False, "required source compatibility metadata is missing"

    if (
        width < 16
        or height < 16
        or width > 4096
        or height > 2160
        or width * height > 4096 * 2160
        or width % 2
        or height % 2
    ):
        return False, "source dimensions are odd or outside safe bounds"
    if not math.isfinite(frame_rate) or not 1.0 <= frame_rate <= 60.0:
        return False, "source frame rate is outside safe bounds"
    if not math.isfinite(rotation) or abs(rotation % 360.0) > 0.001:
        return False, "source carries unsupported display rotation"
    if not math.isfinite(duration) or duration <= 0:
        return False, "source duration is missing or invalid"
    if bitrate <= 0:
        return False, "source video bitrate is missing or invalid"
    if not 10 <= level <= 52:
        return False, "source H.264 level is missing or unsupported"
    return True, "eligible"


def _gop_size(fps: float, seg_dur: int) -> int:
    if fps and fps > 0:
        return max(1, int(round(fps * seg_dur)))
    return 60


# ----------------------------------------------------------------------------
# Filter / encoder arg builders
# ----------------------------------------------------------------------------


def _scale_filter_for(
    use_gpu: bool,
    w: int,
    h: int,
    align_pts: bool = False,
    prefer_scale_cuda: bool = False,
) -> str:
    """Return the scale filter chain for the active pipeline.

    When ``align_pts`` is True, prepend ``setpts=PTS-STARTPTS`` so the video
    timeline starts at PTS 0. This mirrors what the audio path does with
    ``asetpts=PTS-STARTPTS`` and keeps video/audio in sync when the source
    container has a non-zero video start_time (MP4 edit list, Matroska offset).
    """
    prefix = "setpts=PTS-STARTPTS," if align_pts else ""
    scaler = _gpu_scaler() if use_gpu else None
    if prefer_scale_cuda and use_gpu and "scale_cuda" in _ffmpeg_filters():
        scaler = "scale_cuda"
    if scaler == "scale_npp":
        return (
            f"{prefix}scale_npp={w}:{h}:force_original_aspect_ratio=decrease:"
            "force_divisible_by=2:format=yuv420p"
        )
    if scaler == "scale_cuda":
        return (
            f"{prefix}scale_cuda={w}:{h}:force_original_aspect_ratio=decrease:"
            "force_divisible_by=2"
        )
    return (
        f"{prefix}scale={w}:{h}:flags=lanczos:force_original_aspect_ratio=decrease,"
        f"pad=ceil(iw/2)*2:ceil(ih/2)*2"
    )


def _nvenc_preset(preset: str) -> str:
    """Map a preset token to a valid NVENC p-token (p1..p7)."""
    p = (preset or "p6").lower()
    if p in {"p1", "p2", "p3", "p4", "p5", "p6", "p7"}:
        return p
    if p in {"slow", "slower", "slowest", "medium"}:
        return "p6"
    if p in {"fast", "faster", "veryfast", "superfast", "ultrafast"}:
        return "p4"
    return "p6"


_NVENC_PROFILES: Dict[str, Dict[str, Any]] = {
    "quality": {
        "preset": "p6",
        "multipass": "fullres",
        "lookahead": 32,
        "spatial_aq": True,
    },
    "balanced": {
        "preset": "p4",
        "multipass": "qres",
        "lookahead": 12,
        "spatial_aq": False,
    },
    "turbo": {
        "preset": "p3",
        "multipass": "disabled",
        "lookahead": 0,
        "spatial_aq": False,
    },
}
_NVENC_PROFILE_FROM_ENV = object()


def _configured_nvenc_profile(
    profile: Any = _NVENC_PROFILE_FROM_ENV,
) -> Optional[str]:
    """Return a validated explicit NVENC profile, if any.

    Pipeline tasks pass their immutable settings snapshot. The environment
    lookup remains only for direct/legacy command-builder callers that omit the
    argument entirely.
    """
    if profile is _NVENC_PROFILE_FROM_ENV:
        try:
            profile = getattr(get_settings(), "NVENC_PROFILE", None)
        except Exception:
            return None
    normalized = str(profile or "").strip().lower()
    return normalized if normalized in _NVENC_PROFILES else None


def _nvenc_encode_args(
    enc: str, bitrate: int, gop: int, seg: int, preset: str,
    lookahead: bool, bf: int, aq: bool,
    nvenc_profile: Any = _NVENC_PROFILE_FROM_ENV,
) -> List[str]:
    maxrate = int(bitrate * 1.5)
    bufsize = bitrate * 2
    configured_profile = _configured_nvenc_profile(nvenc_profile)
    tuning = (
        _NVENC_PROFILES[configured_profile]
        if configured_profile is not None
        else None
    )
    effective_preset = (
        str(tuning["preset"]) if tuning is not None else _nvenc_preset(preset)
    )
    codec_profile = "high" if enc == "h264_nvenc" else "main"
    args = [
        "-preset", effective_preset,
        "-tune", "hq", "-profile:v", codec_profile,
        "-rc", "vbr", "-b:v", str(bitrate),
        "-maxrate", str(maxrate), "-bufsize", str(bufsize),
        "-g", str(gop), "-keyint_min", str(gop),
        "-sc_threshold", "0", "-flags", "+cgop",
        "-force_key_frames", f"expr:gte(t,n_forced*{seg})",
    ]
    if tuning is not None:
        cq = {
            "h264_nvenc": "23",
            "hevc_nvenc": "25",
            "av1_nvenc": "28",
        }.get(enc)
        if cq is None:
            return args
        args += [
            "-cq", cq,
            "-multipass", str(tuning["multipass"]),
        ]
        if tuning["spatial_aq"]:
            args += ["-spatial-aq", "1"]
        # Balanced and turbo deliberately retain temporal AQ: it has a
        # smaller throughput cost than spatial AQ and protects motion detail.
        args += ["-temporal-aq", "1"]
        if enc in {"h264_nvenc", "hevc_nvenc"} and bf:
            args += ["-bf", str(bf)]
            if configured_profile == "quality":
                # Keep the current quality profile's legacy two-pass switch.
                args += ["-2pass", "1"]
        profile_lookahead = int(tuning["lookahead"])
        args += ["-rc-lookahead", str(profile_lookahead)]
        if profile_lookahead:
            args += ["-no-scenecut", "1"]
        return args

    # No configured profile: preserve the legacy command construction exactly.
    if enc == "h264_nvenc":
        args += ["-cq", "23", "-multipass", "fullres"]
        if aq:
            args += ["-spatial-aq", "1", "-temporal-aq", "1"]
        if bf:
            args += ["-bf", str(bf), "-2pass", "1"]
        if lookahead:
            args += ["-rc-lookahead", "32", "-no-scenecut", "1"]
    elif enc == "hevc_nvenc":
        args += ["-cq", "25"]
        if aq:
            args += ["-spatial-aq", "1", "-temporal-aq", "1"]
        if bf:
            args += ["-bf", str(bf), "-2pass", "1"]
        if lookahead:
            args += ["-rc-lookahead", "32", "-no-scenecut", "1"]
    elif enc == "av1_nvenc":
        args += ["-cq", "28"]
        # AV1 NVENC has no B-frame/lookahead options matching h264.
    return args


def _cpu_encode_args(
    enc: str, bitrate: int, gop: int, seg: int, preset: str,
) -> List[str]:
    if enc == "libx264":
        profile = "high"
    elif enc == "libaom-av1":
        profile = "0"
    else:
        profile = "main"
    args = [
        "-profile:v", profile,
        "-b:v", str(bitrate),
        "-maxrate", str(int(bitrate * 1.5)), "-bufsize", str(bitrate * 2),
        "-g", str(gop), "-keyint_min", str(gop),
        "-sc_threshold", "0", "-flags", "+cgop",
        "-force_key_frames", f"expr:gte(t,n_forced*{seg})",
    ]
    if enc in {"libx264", "libx265"}:
        valid_presets = {
            "ultrafast",
            "superfast",
            "veryfast",
            "faster",
            "fast",
            "medium",
            "slow",
            "slower",
            "veryslow",
        }
        cpu_preset = str(preset or "veryfast").lower()
        if cpu_preset not in valid_presets:
            logger.warning(
                "[ffmpeg] invalid CPU preset %r for %s; using veryfast",
                preset,
                enc,
            )
            cpu_preset = "veryfast"
        args[0:0] = ["-preset", cpu_preset]
    if enc == "libx265":
        args += ["-x265-params", "log-level=error"]
    if enc == "libaom-av1":
        args += ["-cpu-used", "4", "-row-mt", "1", "-b:v", str(bitrate)]
    return args


def _hls_mux_args(
    seg: int, segment_format: str, playlist: str, seg_pattern: str,
) -> List[str]:
    args = ["-an", "-sn"]
    if segment_format == "fmp4":
        args += [
            "-hls_segment_type", "fmp4",
            "-hls_fmp4_init_filename", "init.mp4",
        ]
    args += [
        "-hls_time", str(seg),
        "-hls_playlist_type", "vod",
        "-hls_flags", "independent_segments",
        "-hls_segment_filename", seg_pattern,
        "-f", "hls", playlist,
    ]
    return args


def remux_h264_hls_command(
    input_path: str,
    output_dir: str,
    segment_duration: int = SEGMENT_DURATION,
    segment_format: str = "fmp4",
) -> List[str]:
    """Stream-copy one H.264 source rendition into a complete VOD HLS."""
    os.makedirs(output_dir, exist_ok=True)
    seg = max(1, int(segment_duration))
    fmt = (segment_format or "fmp4").strip().lower()
    if fmt not in {"fmp4", "ts"}:
        raise ValueError(f"unsupported HLS segment format: {segment_format}")
    playlist = os.path.join(output_dir, "video.m3u8")
    segment_pattern = _seg_pattern(output_dir, fmt)
    cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-fflags",
        "+genpts",
        "-i",
        input_path,
        "-map",
        "0:v:0",
        "-an",
        "-sn",
        "-dn",
        "-c:v",
        "copy",
        "-avoid_negative_ts",
        "make_zero",
        "-muxdelay",
        "0",
    ]
    if fmt == "fmp4":
        cmd += [
            "-hls_segment_type",
            "fmp4",
            "-hls_fmp4_init_filename",
            "init.mp4",
        ]
    cmd += [
        "-hls_time",
        str(seg),
        "-hls_playlist_type",
        "vod",
        "-hls_flags",
        "independent_segments",
        "-hls_segment_filename",
        segment_pattern,
        "-f",
        "hls",
        playlist,
    ]
    return cmd


def _seg_pattern(output_dir: str, segment_format: str, prefix: str = "%05d") -> str:
    ext = "m4s" if segment_format == "fmp4" else "ts"
    return os.path.join(output_dir, f"{prefix}.{ext}")


# ----------------------------------------------------------------------------
# Single-rendition transcode command
# ----------------------------------------------------------------------------


def transcode_video_command(
    input_path: str,
    output_dir: str,
    width: int,
    height: int,
    bitrate: int,
    fps: float,
    force_gpu: Optional[bool] = None,
    segment_duration: int = SEGMENT_DURATION,
    preset: str = "medium",
    gpu_index: Optional[int] = None,
    codec: str = "h264",
    segment_format: str = "fmp4",
    lookahead: bool = True,
    bf: int = 2,
    aq: bool = True,
    nvenc_profile: Any = _NVENC_PROFILE_FROM_ENV,
) -> List[str]:
    os.makedirs(output_dir, exist_ok=True)
    use_gpu = (force_gpu if force_gpu is not None else is_gpu_available())
    if use_gpu and not is_gpu_available():
        use_gpu = False
    playlist = os.path.join(output_dir, "video.m3u8")
    seg_pat = _seg_pattern(output_dir, segment_format)
    gop = _gop_size(fps, segment_duration)

    cmd = ["ffmpeg", "-y", "-hide_banner"]
    if use_gpu:
        cmd += select_hwaccel_args(gpu_index, True)
    cmd += ["-i", input_path]

    if use_gpu:
        enc = _gpu_encoder(codec)
        if not enc.endswith("_nvenc"):
            use_gpu = False  # no NVENC at all -> CPU path
    if use_gpu:
        cmd += ["-vf", _scale_filter_for(True, width, height, align_pts=True)]
        cmd += ["-c:v", _gpu_encoder(codec)]
        cmd += _nvenc_encode_args(
            _gpu_encoder(codec),
            bitrate,
            gop,
            segment_duration,
            "p6",
            lookahead,
            bf,
            aq,
            nvenc_profile,
        )
    else:
        vf = _scale_filter_for(False, width, height, align_pts=True)
        enc = _cpu_encoder(codec)
        cmd += ["-vf", vf, "-c:v", enc, "-threads", "0"]
        cmd += _cpu_encode_args(enc, bitrate, gop, segment_duration, preset)

    cmd += _hls_mux_args(segment_duration, segment_format, playlist, seg_pat)
    return cmd


# ----------------------------------------------------------------------------
# Single-pass multi-rendition transcode (one decode -> N encodes per GPU)
# ----------------------------------------------------------------------------


def _filter_complex_split(
    renditions: List[Dict[str, Any]],
    use_gpu: bool,
    align_pts: bool = False,
    software_decode_gpu: bool = False,
) -> Tuple[str, List[str]]:
    """Build a split+scale filter_complex and the list of [vN] labels.

    When ``align_pts`` is True, ``setpts=PTS-STARTPTS`` is applied to the
    split input so every rendition's timeline starts at PTS 0, matching the
    audio path's ``asetpts=PTS-STARTPTS`` alignment.
    """
    if software_decode_gpu and not use_gpu:
        raise ValueError("software_decode_gpu requires GPU encoding")
    if software_decode_gpu:
        filters = _ffmpeg_filters()
        missing = {"hwupload_cuda", "scale_cuda"} - filters
        if missing:
            raise FFmpegError(
                "software-decode GPU mode requires FFmpeg filters: "
                + ", ".join(sorted(missing))
            )

    n = len(renditions)
    input_filters = []
    if align_pts:
        input_filters.append("setpts=PTS-STARTPTS")
    if software_decode_gpu:
        input_filters += ["format=nv12", "hwupload_cuda"]
    input_prefix = ",".join(input_filters)
    if input_prefix:
        input_prefix += ","
    split_labels = "".join(f"[in{i}]" for i in range(n))
    fc = f"[0:v]{input_prefix}split={n}{split_labels}"
    labels = []
    for i, r in enumerate(renditions):
        w = int(r["width"])
        h = int(r["height"])
        fc += (
            f";[in{i}]"
            f"{_scale_filter_for(use_gpu, w, h, prefer_scale_cuda=software_decode_gpu)}"
            f"[v{i}]"
        )
        labels.append(f"[v{i}]")
    return fc, labels


def transcode_multi_command(
    input_path: str,
    output_base_dir: str,
    renditions: List[Dict[str, Any]],
    fps: float,
    gpu_index: Optional[int] = None,
    segment_duration: int = SEGMENT_DURATION,
    codec: str = "h264",
    segment_format: str = "fmp4",
    preset: str = "p6",
    require_gpu: bool = False,
    software_decode_gpu: bool = False,
    nvenc_profile: Any = _NVENC_PROFILE_FROM_ENV,
) -> List[str]:
    """One decode -> split -> N scaled NVENC-encoded HLS outputs.

    Shares a single hardware decode across all renditions on one GPU
    (zero-copy when a GPU scaler is available). Each rendition writes
    `<output_base_dir>/video_<height>p/video.m3u8` + segments.
    """
    os.makedirs(output_base_dir, exist_ok=True)
    # A required GPU command is built only after the task owns a live GPU
    # lease. Trust that lease instead of launching a separate one-frame NVENC
    # process immediately before the real encode.
    use_gpu = True if require_gpu else is_gpu_available()
    if require_gpu and not use_gpu:
        raise FFmpegError(
            "GPU lease was acquired but NVENC/CUDA is no longer available"
        )
    if software_decode_gpu and not use_gpu:
        raise FFmpegError(
            "software-decode GPU mode requires an available GPU"
        )
    primary_encoder = (
        _required_gpu_encoder(codec)
        if require_gpu
        else _gpu_encoder(codec)
    ) if use_gpu else ""
    if use_gpu and not primary_encoder.endswith("_nvenc"):
        if require_gpu:
            raise FFmpegError(
                f"no NVENC encoder is available for codec {codec}"
            )
        use_gpu = False
    gop = _gop_size(fps, segment_duration)

    cmd = ["ffmpeg", "-y", "-hide_banner"]
    if use_gpu and software_decode_gpu:
        cmd += _cuda_filter_device_args(gpu_index)
    elif use_gpu:
        cmd += select_hwaccel_args(gpu_index, True)
    cmd += ["-i", input_path]

    fc, labels = _filter_complex_split(
        renditions,
        use_gpu,
        align_pts=True,
        software_decode_gpu=software_decode_gpu,
    )
    cmd += ["-filter_complex", fc]

    for i, r in enumerate(renditions):
        h = int(r["height"])
        w = int(r["width"])
        bitrate = int(r["bitrate"])
        rc = r.get("codec", codec)
        rdir = os.path.join(output_base_dir, f"video_{h}p")
        os.makedirs(rdir, exist_ok=True)
        playlist = os.path.join(rdir, "video.m3u8")
        seg_pat = _seg_pattern(rdir, segment_format)
        cmd += ["-map", labels[i]]
        if use_gpu:
            enc = (
                _required_gpu_encoder(rc)
                if require_gpu
                else _gpu_encoder(rc)
            )
            cmd += ["-c:v", enc]
            cmd += _nvenc_encode_args(
                enc,
                bitrate,
                gop,
                segment_duration,
                preset,
                True,
                2,
                True,
                nvenc_profile,
            )
        else:
            enc = _cpu_encoder(rc)
            cmd += ["-c:v", enc, "-threads", "0"]
            cmd += _cpu_encode_args(enc, bitrate, gop, segment_duration, "medium")
        cmd += _hls_mux_args(segment_duration, segment_format, playlist, seg_pat)
    return cmd


# ----------------------------------------------------------------------------
# Chunked (time-sliced) parallel encoding
# ----------------------------------------------------------------------------


def transcode_chunk_command(
    input_path: str,
    output_dir: str,
    renditions: List[Dict[str, Any]],
    start_sec: float,
    duration_sec: float,
    fps: float,
    gpu_index: Optional[int] = None,
    segment_duration: int = SEGMENT_DURATION,
    codec: str = "h264",
    segment_format: str = "fmp4",
    force_software: bool = False,
    cpu_preset: str = "veryfast",
    cpu_threads: int = 2,
    require_gpu: bool = False,
    require_cpu_codec: bool = False,
    software_decode_gpu: bool = False,
    nvenc_profile: Any = _NVENC_PROFILE_FROM_ENV,
) -> List[str]:
    """Encode one [start_sec, start_sec+duration_sec) slice for all renditions.

    Fast input seek (`-ss` before `-i`) plus forced keyframes at segment
    boundaries keep chunks independently decodable. Outputs per rendition:
    `<output_dir>/video_<height>p/chunk.m3u8` + `chunk_%05d.<ext>` (+ init.mp4
    for fMP4).
    """
    os.makedirs(output_dir, exist_ok=True)
    if force_software and require_gpu:
        raise ValueError("force_software and require_gpu are mutually exclusive")
    if force_software and software_decode_gpu:
        raise ValueError(
            "force_software and software_decode_gpu are mutually exclusive"
        )
    if require_cpu_codec and not force_software:
        raise ValueError(
            "require_cpu_codec is only valid with force_software"
        )
    gpu_available = (
        False
        if force_software
        else (True if require_gpu else is_gpu_available())
    )
    use_gpu = gpu_available and not force_software
    if require_gpu and not use_gpu:
        raise FFmpegError(
            "GPU lease was acquired but NVENC/CUDA is no longer available"
        )
    if software_decode_gpu and not use_gpu:
        raise FFmpegError(
            "software-decode GPU mode requires an available GPU"
        )
    primary_encoder = (
        _required_gpu_encoder(codec)
        if require_gpu
        else _gpu_encoder(codec)
    ) if use_gpu else ""
    if use_gpu and not primary_encoder.endswith("_nvenc"):
        if require_gpu:
            raise FFmpegError(
                f"no NVENC encoder is available for codec {codec}"
            )
        use_gpu = False
    gop = _gop_size(fps, segment_duration)
    try:
        bounded_cpu_threads = min(16, max(1, int(cpu_threads)))
    except (TypeError, ValueError):
        bounded_cpu_threads = 2

    cmd = ["ffmpeg", "-y", "-hide_banner"]
    if use_gpu and software_decode_gpu:
        cmd += _cuda_filter_device_args(gpu_index)
    # Fast seek before input for speed; accurate enough with forced keyframes.
    cmd += ["-ss", f"{float(start_sec):.3f}"]
    if use_gpu and not software_decode_gpu:
        cmd += select_hwaccel_args(gpu_index, True)
    # Keep the range limit on the input. An output-scoped ``-t`` only applies
    # to the next output file, so a multi-rendition chunk could otherwise let
    # later outputs continue through the rest of the source.
    cmd += ["-t", f"{float(duration_sec):.3f}", "-i", input_path]

    fc, labels = _filter_complex_split(
        renditions,
        use_gpu,
        align_pts=True,
        software_decode_gpu=software_decode_gpu,
    )
    if not use_gpu:
        cmd += ["-filter_complex_threads", str(bounded_cpu_threads)]
    cmd += ["-filter_complex", fc]

    for i, r in enumerate(renditions):
        h = int(r["height"])
        w = int(r["width"])
        bitrate = int(r["bitrate"])
        rc = r.get("codec", codec)
        rdir = os.path.join(output_dir, f"video_{h}p")
        os.makedirs(rdir, exist_ok=True)
        playlist = os.path.join(rdir, "chunk.m3u8")
        seg_pat = os.path.join(rdir, f"chunk_%05d.{'m4s' if segment_format == 'fmp4' else 'ts'}")
        cmd += ["-map", labels[i]]
        if use_gpu:
            enc = (
                _required_gpu_encoder(rc)
                if require_gpu
                else _gpu_encoder(rc)
            )
            cmd += ["-c:v", enc]
            cmd += _nvenc_encode_args(
                enc,
                bitrate,
                gop,
                segment_duration,
                "p6",
                True,
                2,
                True,
                nvenc_profile,
            )
        else:
            enc = _cpu_encoder(rc)
            if (
                require_cpu_codec
                and enc != _CPU_CODEC_ENCODERS.get(str(rc).lower())
            ):
                raise FFmpegError(
                    f"required CPU encoder is unavailable for codec {rc}"
                )
            cmd += ["-c:v", enc, "-threads", str(bounded_cpu_threads)]
            cmd += _cpu_encode_args(
                enc,
                bitrate,
                gop,
                segment_duration,
                cpu_preset,
            )
        cmd += _hls_mux_args(segment_duration, segment_format, playlist, seg_pat)
    return cmd


def _detect_seg_format_from_list(list_path: str) -> str:
    """Inspect a concat list and return 'fmp4' if it references .m4s else 'ts'."""
    try:
        with open(list_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line.startswith("file "):
                    p = line[5:].strip().strip("'\"")
                    if p.lower().endswith(".m4s"):
                        return "fmp4"
                    if p.lower().endswith(".ts"):
                        return "ts"
    except Exception:
        pass
    return "ts"


def _concat_ts_bitstream_filter(codec: str) -> Optional[str]:
    """Return the codec-appropriate MP4/extradata-to-TS filter, if any."""
    normalized = str(codec or "h264").lower()
    if normalized in {"h264", "avc", "libx264", "h264_nvenc"}:
        return "h264_mp4toannexb"
    if normalized in {"hevc", "h265", "libx265", "hevc_nvenc"}:
        return "hevc_mp4toannexb"
    if normalized in {"av1", "libaom-av1", "av1_nvenc"}:
        # FFmpeg can write AV1 payloads into MPEG-TS, but its concat demuxer
        # exposes those segments as bin_data rather than a video stream.  Fail
        # before producing an unpackageable presentation.
        raise ValueError(
            "AV1 is not supported by the MPEG-TS chunk concat path"
        )
    raise ValueError(f"unsupported chunk concat codec: {codec}")


def concat_segments_command(
    list_path: str,
    out_playlist: str,
    codec: str = "h264",
    segment_duration: int = SEGMENT_DURATION,
) -> List[str]:
    """Stitch a concat-demuxer segment list into one HLS variant playlist.

    Re-muxes with stream copy (`-c copy`) and re-segments at SEGMENT_DURATION.
    MPEG-TS inputs concatenate cleanly; fMP4 fragment lists work on recent
    ffmpeg builds. For maximum robustness the chunked path should use TS.
    """
    out_dir = os.path.dirname(out_playlist) or "."
    os.makedirs(out_dir, exist_ok=True)
    fmt = _detect_seg_format_from_list(list_path)
    seg = max(1, int(segment_duration))
    if fmt == "fmp4":
        seg_pat = _seg_pattern(out_dir, "fmp4")
        cmd = [
            "ffmpeg", "-y", "-hide_banner",
            "-f", "concat", "-safe", "0", "-i", list_path,
            "-c", "copy",
            "-hls_time", str(seg), "-hls_playlist_type", "vod",
            "-hls_flags", "independent_segments",
            "-hls_segment_type", "fmp4", "-hls_fmp4_init_filename", "init.mp4",
            "-hls_segment_filename", seg_pat, "-f", "hls", out_playlist,
        ]
    else:
        seg_pat = _seg_pattern(out_dir, "ts")
        cmd = [
            "ffmpeg", "-y", "-hide_banner",
            "-f", "concat", "-safe", "0", "-i", list_path,
            "-c", "copy",
        ]
        bitstream_filter = _concat_ts_bitstream_filter(codec)
        if bitstream_filter:
            cmd += ["-bsf:v", bitstream_filter]
        cmd += [
            "-hls_time", str(seg), "-hls_playlist_type", "vod",
            "-hls_flags", "independent_segments",
            "-hls_segment_filename", seg_pat, "-f", "hls", out_playlist,
        ]
    return cmd


# ----------------------------------------------------------------------------
# Audio
# ----------------------------------------------------------------------------


def _audio_filter_chain(
    base_filters: List[str], audio_delay_ms: Optional[float] = None,
) -> str:
    """Build an -af chain that aligns PTS to 0 and preserves source sync.

    ``audio_delay_ms`` is already expressed in milliseconds. Positive values
    mean audio starts after video, so ``adelay`` inserts silence. Negative
    values mean audio leads video, which cannot be represented by ``adelay``;
    trim that leading audio before rebasing its timestamps instead.
    """
    delay = float(audio_delay_ms or 0.0)
    filters = list(base_filters)
    if delay <= -1.0:
        filters.append(f"atrim=start={-delay / 1000.0:.6f}")
    filters.append("asetpts=PTS-STARTPTS")
    if delay >= 1.0:
        # FFmpeg's adelay unit is milliseconds by default.
        filters.append(f"adelay={int(round(delay))}:all=1")
        filters.append("aresample=async=1")
    elif delay <= -1.0:
        filters.append("aresample=async=1")
    return ",".join(filters)


def _can_passthrough_aac(
    *,
    source_codec: Optional[str],
    source_profile: Any,
    source_channels: Any,
    source_sample_rate: Any,
    output_channels: Any,
    loudnorm: bool,
    audio_delay_ms: Optional[float],
    output_sample_rate: int = 48000,
) -> bool:
    """Return whether AAC can be copied without bypassing required work.

    Unknown stream metadata is deliberately ineligible. Delay magnitudes below
    one millisecond are eligible because the existing filter path does not
    apply a delay/trim adjustment for them either.
    """
    codec = (source_codec or "").strip().lower()
    profile = str(source_profile or "").strip().lower()
    if (
        codec != "aac"
        or profile not in {"lc", "aac lc", "low complexity", "mpeg-4 aac lc"}
        or loudnorm
    ):
        return False
    try:
        source_channel_count = int(source_channels)
        output_channel_count = int(output_channels)
        source_rate = int(source_sample_rate)
        output_rate = int(output_sample_rate)
        delay = float(audio_delay_ms or 0.0)
    except (TypeError, ValueError):
        return False
    return (
        source_channel_count > 0
        and source_channel_count == output_channel_count
        and source_rate > 0
        and source_rate == output_rate
        and math.isfinite(delay)
        and abs(delay) < 1.0
    )


def extract_audio_command(
    input_path: str,
    output_path: str,
    stream_index: int,
    bitrate: str,
    channels: int,
    language: str = "und",
    audio_delay_ms: Optional[float] = None,
    loudnorm: bool = False,
) -> List[str]:
    """Extract one audio track to an m4a (two-step fallback path).

    The audio timeline is rebased to PTS 0 to match the video path. If the
    source declared an intentional audio delay (``audio_delay_ms``), it is
    re-applied so lipsync is preserved.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    base = (
        ["loudnorm=I=-16:LRA=11:TP=-1.5:linear=true"]
        if loudnorm
        else []
    )
    af = _audio_filter_chain(base, audio_delay_ms)
    cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-i",
        input_path,
        "-map",
        f"0:a:{stream_index}",
        "-sn",
        "-vn",
        "-af",
        af,
        "-c:a",
        "aac",
        "-b:a",
        bitrate,
        "-ac",
        str(channels),
        "-ar",
        "48000",
        output_path,
    ]
    return cmd


def combined_audio_command(
    input_path: str,
    output_dir: str,
    stream_index: int,
    language: str = "und",
    bitrate_kbps: int = 128,
    channels: int = 2,
    segment_duration: int = SEGMENT_DURATION,
    loudnorm: bool = True,
    source_codec: str = None,
    source_profile: Any = None,
    audio_delay_ms: Optional[float] = None,
    aac_passthrough_enabled: bool = False,
    source_channels: Any = None,
    source_sample_rate: Any = None,
) -> List[str]:
    """One-pass audio: safely copy AAC or re-encode it to HLS segments.

    The output is forced to start at PTS 0 (matching the video path) and
    mpegts muxdelay is disabled so the packaged HLS audio timeline aligns
    with the video timeline. If the source carried an intentional audio
    delay (``audio_delay_ms``), it is re-applied with ``adelay`` so the
    relative audio/video offset is preserved. AAC passthrough is used only
    when no filter, channel conversion, or sample-rate conversion is needed.
    """
    os.makedirs(output_dir, exist_ok=True)
    playlist = os.path.join(output_dir, "audio.m3u8")
    seg_pat = os.path.join(output_dir, "%05d.aac")
    cmd = [
        "ffmpeg", "-y", "-hide_banner",
        "-analyzeduration", "100000000", "-probesize", "100000000",
        "-i", input_path,
        "-map", f"0:a:{stream_index}",
        "-sn", "-vn",
    ]
    passthrough = bool(aac_passthrough_enabled) and _can_passthrough_aac(
        source_codec=source_codec,
        source_profile=source_profile,
        source_channels=source_channels,
        source_sample_rate=source_sample_rate,
        output_channels=channels,
        loudnorm=loudnorm,
        audio_delay_ms=audio_delay_ms,
    )
    if passthrough:
        cmd += ["-c:a", "copy"]
    else:
        if loudnorm:
            # linear=true runs a single pass (instead of the slow two-pass
            # dynamic mode) and is widely used in production for ~50%
            # lower audio latency.
            base: List[str] = [
                "loudnorm=I=-16:LRA=11:TP=-1.5:linear=true"
            ]
        else:
            base = []
        cmd += ["-af", _audio_filter_chain(base, audio_delay_ms)]
        cmd += [
            "-c:a", "aac", "-b:a", f"{int(bitrate_kbps)}k",
            "-ac", str(int(channels)), "-ar", "48000",
        ]
    cmd += [
        # Disable mpegts muxdelay so HLS ADTS segment timestamps start at 0.
        "-muxdelay", "0",
        "-hls_time", str(segment_duration),
        "-hls_playlist_type", "vod",
        "-hls_flags", "independent_segments",
        "-hls_segment_filename", seg_pat,
        "-f", "hls", playlist,
    ]
    return cmd


def extract_subtitle_command(
    input_path: str, output_path: str, stream_index: int
) -> List[str]:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    return [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-i",
        input_path,
        "-map",
        f"0:s:{stream_index}",
        "-c:s",
        "webvtt",
        output_path,
    ]


def package_audio_command(input_path: str, output_dir: str, segment_duration: int = SEGMENT_DURATION) -> List[str]:
    os.makedirs(output_dir, exist_ok=True)
    playlist = os.path.join(output_dir, "audio.m3u8")
    segment_pattern = os.path.join(output_dir, "%05d.aac")
    return [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-i",
        input_path,
        # The input m4a was already aligned to start at 0 by
        # extract_audio_command; copy it and disable mpegts muxdelay so the HLS
        # segment timestamps also start at 0 instead of the default 1.4 s shift.
        "-c:a",
        "copy",
        "-muxdelay",
        "0",
        "-hls_time",
        str(segment_duration),
        "-hls_playlist_type",
        "vod",
        "-hls_flags",
        "independent_segments",
        "-hls_segment_filename",
        segment_pattern,
        "-f",
        "hls",
        playlist,
    ]


_WEBVTT_TIMING_RE = re.compile(
    r"^(?P<leading>\s*)(?P<start>(?:\d{2,}:)?\d{2}:\d{2}(?:\.\d+)?)"
    r"\s+-->\s+(?P<end>(?:\d{2,}:)?\d{2}:\d{2}(?:\.\d+)?)(?P<settings>.*)$"
)


def _subtitle_mpegts_start_pts(segment_format: str) -> int:
    """Return the first WebVTT timestamp-map PTS for the HLS format.

    FFmpeg's MPEG-TS HLS muxer starts its transport timeline at 1.4 seconds,
    whereas fMP4 output preserves our normalized zero-based presentation
    timeline.  A fixed value shifts every subtitle cue in one of those two
    formats, so packaging must use the rendition format selected for the job.
    """
    fmt = (segment_format or "fmp4").strip().lower()
    if fmt == "fmp4":
        return 0
    if fmt == "ts":
        return 126_000
    raise ValueError(f"unsupported HLS segment format: {segment_format}")


def _webvtt_seconds(value: str) -> float:
    """Parse a WebVTT timestamp into seconds."""
    parts = value.strip().split(":")
    if len(parts) == 2:
        hours = 0.0
        minutes, seconds = parts
    elif len(parts) == 3:
        hours, minutes, seconds = parts
    else:
        raise ValueError(f"invalid WebVTT timestamp: {value}")
    return float(hours) * 3600 + float(minutes) * 60 + float(seconds)


def _webvtt_timestamp(seconds: float) -> str:
    milliseconds = max(0, int(round(seconds * 1000)))
    hours, milliseconds = divmod(milliseconds, 3_600_000)
    minutes, milliseconds = divmod(milliseconds, 60_000)
    whole_seconds, milliseconds = divmod(milliseconds, 1_000)
    return f"{hours:02d}:{minutes:02d}:{whole_seconds:02d}.{milliseconds:03d}"


def _webvtt_cues(content: str) -> Tuple[List[str], List[Tuple[List[str], float, float, int]]]:
    """Return reusable header blocks and parsed cue blocks from a VTT file."""
    normalized = content.lstrip("\ufeff").replace("\r\n", "\n").replace("\r", "\n")
    blocks = [block for block in re.split(r"\n{2,}", normalized.strip()) if block.strip()]
    if not blocks or not blocks[0].lstrip().startswith("WEBVTT"):
        raise ValueError("subtitle input is not valid WebVTT")

    header_blocks: List[str] = []
    first_header = blocks[0].split("\n", 1)
    if len(first_header) == 2 and first_header[1].strip():
        header_blocks.append(first_header[1].strip())

    cues: List[Tuple[List[str], float, float, int]] = []
    for block in blocks[1:]:
        lines = block.split("\n")
        timing_index = next((i for i, line in enumerate(lines) if "-->" in line), -1)
        match = _WEBVTT_TIMING_RE.match(lines[timing_index]) if timing_index >= 0 else None
        if match is None:
            header_blocks.append(block.strip())
            continue
        start = _webvtt_seconds(match.group("start"))
        end = _webvtt_seconds(match.group("end"))
        if end > start:
            cues.append((lines, start, end, timing_index))
    return header_blocks, cues


def _subtitle_segment_content(
    header_blocks: List[str],
    cues: List[Tuple[List[str], float, float, int]],
    segment_start: float,
    segment_end: float,
    mpegts_start_pts: int,
) -> str:
    """Build one standards-compliant, segment-local WebVTT document."""
    mpegts = mpegts_start_pts + int(round(segment_start * 90_000))
    header_lines = [
        "WEBVTT",
        f"X-TIMESTAMP-MAP=LOCAL:00:00:00.000,MPEGTS:{mpegts}",
    ]
    if header_blocks:
        header_lines.extend(header_blocks)
    cue_blocks: List[str] = []
    for lines, start, end, timing_index in cues:
        if start >= segment_end or end <= segment_start:
            continue
        cue_lines = list(lines)
        match = _WEBVTT_TIMING_RE.match(cue_lines[timing_index])
        if match is None:  # Defensive: parsing above already verified this.
            continue
        local_start = _webvtt_timestamp(max(start, segment_start) - segment_start)
        local_end = _webvtt_timestamp(min(end, segment_end) - segment_start)
        cue_lines[timing_index] = (
            f"{match.group('leading')}{local_start} --> {local_end}{match.group('settings')}"
        )
        cue_blocks.append("\n".join(cue_lines))
    # hls.js parses the timestamp map only before the first blank header line.
    # Keep WEBVTT and X-TIMESTAMP-MAP adjacent, then separate cue blocks.
    body = "\n".join(header_lines)
    if cue_blocks:
        body += "\n\n" + "\n\n".join(cue_blocks)
    return body + "\n"


def package_subtitle(
    input_path: str,
    output_dir: str,
    duration: float = 0,
    segment_duration: int = SEGMENT_DURATION,
    segment_format: str = "fmp4",
) -> str:
    """Package WebVTT into timeline-aligned HLS subtitle segments.

    A standalone multi-hour VTT referenced by one media-playlist entry omits
    mandatory HLS timeline information and is rejected by strict clients.
    Segmenting it alongside video produces small, seekable subtitle resources
    with an ``X-TIMESTAMP-MAP`` for each transport-stream timeline position.
    """
    os.makedirs(output_dir, exist_ok=True)
    with open(input_path, "r", encoding="utf-8-sig") as handle:
        headers, cues = _webvtt_cues(handle.read())

    try:
        requested_duration = float(duration or 0)
    except (TypeError, ValueError):
        requested_duration = 0.0
    cue_duration = max((end for _lines, _start, end, _index in cues), default=0.0)
    total_duration = max(requested_duration, cue_duration)
    segment_length = max(1, int(segment_duration or SEGMENT_DURATION))
    segment_count = max(1, int(math.ceil(total_duration / segment_length)))
    mpegts_start_pts = _subtitle_mpegts_start_pts(segment_format)

    playlist_path = os.path.join(output_dir, "subtitles.m3u8")
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:6",
        f"#EXT-X-TARGETDURATION:{segment_length}",
        "#EXT-X-MEDIA-SEQUENCE:0",
        "#EXT-X-PLAYLIST-TYPE:VOD",
    ]
    for index in range(segment_count):
        segment_start = index * segment_length
        segment_end = min(total_duration, segment_start + segment_length)
        if segment_end <= segment_start:
            segment_end = segment_start + segment_length
        name = f"subtitles_{index:05d}.vtt"
        with open(os.path.join(output_dir, name), "w", encoding="utf-8") as handle:
            handle.write(
                _subtitle_segment_content(
                    headers,
                    cues,
                    segment_start,
                    segment_end,
                    mpegts_start_pts,
                )
            )
        lines.append(f"#EXTINF:{segment_end - segment_start:.3f},")
        lines.append(name)
    lines.append("#EXT-X-ENDLIST")
    with open(playlist_path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")
    return playlist_path


def thumbnail_commands(
    input_path: str, output_dir: str, duration: float
) -> Tuple[List[str], List[str]]:
    os.makedirs(output_dir, exist_ok=True)
    duration = float(duration or 0)
    thumb_interval = 10
    if duration > 0:
        # Cap gallery thumbs ~120 (interval grows for long sources); guarantee >=1.
        thumb_interval = max(1, min(max(10, int(math.ceil(duration / 120))),
                                     max(1, int(math.ceil(duration)))))
    # A fixed one-second poster routinely catches distributor slates, fades,
    # and black openings. Sample into the programme instead: 12% is far enough
    # past an intro on feature-length sources while the 12-second floor keeps
    # short clips recognisable. Never seek beyond the final two seconds.
    poster_seek = 1.0
    if duration > 0:
        poster_seek = min(max(12.0, duration * 0.12), max(0.0, duration - 2.0))
    poster_cmd = [
        "ffmpeg", "-y", "-hide_banner",
        "-ss", f"{poster_seek:.3f}", "-i", input_path,
        "-frames:v", "1",
        "-q:v", "3", "-pix_fmt", "yuvj420p",
        os.path.join(output_dir, "poster.jpg"),
    ]
    # One thumbnail every ~10s (denser for short sources) for the gallery.
    # Decode keyframes only (fast on long sources) and sample one thumb per interval.
    thumbs_cmd = [
        "ffmpeg", "-y", "-hide_banner",
        "-skip_frame", "nokey",
        "-i", input_path,
        "-an", "-sn",
        "-vf", f"fps=1/{thumb_interval},scale=320:-2",
        "-q:v", "4", "-pix_fmt", "yuvj420p",
        os.path.join(output_dir, "thumb_%04d.jpg"),
    ]
    return poster_cmd, thumbs_cmd


def _write_trickplay_vtt(
    vtt_path: str, sprite_name: str, num: int, interval: int,
    cols: int, tile_w: int, tile_h: int,
) -> None:
    def fmt(t: float) -> str:
        h = int(t // 3600)
        m = int((t % 3600) // 60)
        s = t % 60
        return f"{h:02d}:{m:02d}:{s:06.3f}"

    lines = ["WEBVTT", ""]
    for i in range(num):
        t0 = i * interval
        t1 = t0 + interval
        col = i % cols
        row = i // cols
        x = col * tile_w
        y = row * tile_h
        lines.append(f"{fmt(float(t0))} --> {fmt(float(t1))}")
        lines.append(f"{sprite_name}#xywh={x},{y},{tile_w},{tile_h}")
        lines.append("")
    with open(vtt_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))


def trickplay_commands(
    input_path: str,
    output_dir: str,
    duration: float,
    tile_w: int = 160,
    tile_h: int = 90,
    interval_sec: int = 10,
    cols: int = 10,
) -> Tuple[List[str], List[str]]:
    """Build a tiled thumbnail sprite + a WebVTT scrubber index.

    Returns (sprite_cmd, index_cmd). The WebVTT file is written immediately as
    a side effect (it is pure text); index_cmd is a harmless ffmpeg no-op so
    callers that run both commands stay simple.
    """
    os.makedirs(output_dir, exist_ok=True)
    duration = float(duration or 0)
    interval = max(1, int(interval_sec))
    # Cap sprite thumbs ~120 (interval grows for long sources); guarantee >=1.
    if duration > 0:
        interval = max(1, min(max(interval, int(math.ceil(duration / 120))),
                               max(1, int(math.ceil(duration)))))
    num = max(1, int(math.ceil(duration / interval))) if duration > 0 else 1
    rows = max(1, math.ceil(num / cols))
    sprite_path = os.path.join(output_dir, "sprite.jpg")
    sprite_name = os.path.basename(sprite_path)
    sprite_cmd = [
        "ffmpeg", "-y", "-hide_banner",
        "-skip_frame", "nokey",
        "-i", input_path,
        "-an", "-sn",
        "-vf", f"fps=1/{interval},scale={tile_w}:{tile_h},tile={cols}x{rows}",
        "-frames:v", "1", "-q:v", "3", "-pix_fmt", "yuvj420p", sprite_path,
    ]
    vtt_path = os.path.join(output_dir, "sprite.vtt")
    try:
        _write_trickplay_vtt(vtt_path, sprite_name, num, interval, cols, tile_w, tile_h)
    except Exception as exc:
        logger.warning("[trickplay] failed to write VTT index: %s", exc)
    # No-op command (VTT already written). Cheap ffmpeg run to keep the
    # caller's run_cmd(sprite) + run_cmd(index) pattern uniform.
    index_cmd = [
        "ffmpeg", "-y", "-hide_banner",
        "-f", "lavfi", "-i", "color=c=black:s=1x1:d=0.001",
        "-frames:v", "1", "-f", "null", "-",
    ]
    return sprite_cmd, index_cmd


def compute_directory_bitrate(directory: str, duration: float) -> int:
    """Estimate variant bandwidth from segment sizes and total duration."""
    if not directory or not os.path.isdir(directory) or duration <= 0:
        return 1
    total_bytes = 0
    for name in os.listdir(directory):
        path = os.path.join(directory, name)
        if os.path.isfile(path):
            total_bytes += os.path.getsize(path)
    return max(1, int(total_bytes * 8 / duration))
