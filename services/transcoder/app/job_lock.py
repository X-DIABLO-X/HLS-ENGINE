"""Cross-process advisory locks for shared transcoder job workspaces."""

import errno
import hashlib
import logging
import os
import re
import threading
import time
from contextlib import contextmanager

try:  # Linux containers and other POSIX workers.
    import fcntl
except ImportError:  # pragma: no cover - exercised on Windows hosts.
    fcntl = None

try:  # Keep imports and local tests functional on Windows.
    import msvcrt
except ImportError:  # pragma: no cover - exercised on POSIX workers.
    msvcrt = None


logger = logging.getLogger(__name__)

_LOCK_DIR = ".job-locks"
_NAMESPACE_GUARD = ".namespace.lock"
_DEFAULT_REAP_LIMIT = 256
_SAFE_KEY_RE = re.compile(r"[^A-Za-z0-9_.-]+")
_WINDOWS_RETRY_ERRNOS = {
    errno.EACCES,
    errno.EAGAIN,
    errno.EDEADLK,
}
_fallback_guard = threading.Lock()
_fallback_locks = {}


class JobLockBusy(RuntimeError):
    """The requested non-blocking job lock is currently owned elsewhere."""


def _lock_dir(work_root: str) -> str:
    lock_dir = os.path.join(os.path.abspath(work_root), _LOCK_DIR)
    os.makedirs(lock_dir, exist_ok=True)
    return lock_dir


def _lock_path(work_root: str, key: str) -> str:
    """Return a stable, traversal-safe path for one logical lock key."""
    key = str(key)
    readable = _SAFE_KEY_RE.sub("-", key).strip(".-")[:72] or "job"
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]
    return os.path.join(
        _lock_dir(work_root),
        f"{readable}-{digest}.lock",
    )


def _namespace_guard_path(work_root: str) -> str:
    """Return the one persistent lock coordinating target open/unlink."""
    return os.path.join(_lock_dir(work_root), _NAMESPACE_GUARD)


def _fallback_lock(path: str):
    """Process-local fallback for unsupported development platforms."""
    with _fallback_guard:
        return _fallback_locks.setdefault(path, threading.RLock())


def _acquire_posix(handle, shared: bool, blocking: bool) -> None:
    operation = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
    if not blocking:
        operation |= fcntl.LOCK_NB
    while True:
        try:
            fcntl.flock(handle.fileno(), operation)
            return
        except InterruptedError:
            continue
        except BlockingIOError as exc:
            raise JobLockBusy("job lock is already held") from exc


def _release_posix(handle) -> None:
    while True:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            return
        except InterruptedError:
            continue


def _acquire_windows(handle, blocking: bool) -> None:
    """Acquire byte zero with retry; msvcrt has no shared-lock equivalent."""
    while True:
        handle.seek(0)
        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            return
        except OSError as exc:
            if exc.errno not in _WINDOWS_RETRY_ERRNOS:
                raise
            if not blocking:
                raise JobLockBusy("job lock is already held") from exc
            time.sleep(0.05)


def _release_windows(handle) -> None:
    handle.seek(0)
    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)


def _seed_lock_file(handle) -> None:
    """Ensure Windows byte-range locking has a byte zero to lock."""
    if os.fstat(handle.fileno()).st_size == 0:
        handle.write(b"\0")


def _acquire_handle(
    handle,
    *,
    shared: bool,
    blocking: bool,
) -> None:
    if fcntl is not None:
        _acquire_posix(handle, shared, blocking)
    else:
        _acquire_windows(handle, blocking)


def _release_handle(handle) -> None:
    if fcntl is not None:
        _release_posix(handle)
    else:
        _release_windows(handle)


@contextmanager
def _namespace_guard(
    work_root: str,
    *,
    exclusive: bool,
    blocking: bool,
):
    """Coordinate every target-file open with safe idle-file unlinking.

    An opener retains a shared guard until its target lock is acquired. A
    sweeper takes the guard exclusively, so it cannot unlink while an opener
    is queued on an old inode. Platforms without shared locks serialize this
    short transition exclusively.
    """
    path = _namespace_guard_path(work_root)
    handle = None
    local_lock = None
    acquired = False
    try:
        if fcntl is not None or msvcrt is not None:
            handle = open(path, "a+b", buffering=0)
            _seed_lock_file(handle)
            _acquire_handle(
                handle,
                shared=not exclusive,
                blocking=blocking,
            )
        else:  # pragma: no cover - only an exotic development fallback.
            local_lock = _fallback_lock(path)
            if not local_lock.acquire(blocking=blocking):
                raise JobLockBusy("job-lock namespace is already held")
        acquired = True
        yield
    finally:
        if acquired:
            try:
                if handle is not None:
                    _release_handle(handle)
                else:
                    local_lock.release()
            finally:
                if handle is not None:
                    handle.close()
        elif handle is not None:
            handle.close()


def _remove_idle_lock_file_under_guard(path: str) -> str:
    """Remove one idle regular lock file while namespace exclusion is held."""
    handle = None
    local_lock = None
    acquired = False
    try:
        try:
            file_stat = os.lstat(path)
        except FileNotFoundError:
            return "absent"
        if not os.path.isfile(path) or os.path.islink(path):
            return "unsafe"

        if fcntl is not None or msvcrt is not None:
            handle = open(path, "r+b", buffering=0)
            _acquire_handle(handle, shared=False, blocking=False)
        else:  # pragma: no cover - only an exotic development fallback.
            local_lock = _fallback_lock(path)
            if not local_lock.acquire(blocking=False):
                return "busy"
        acquired = True

        # Refuse a path replacement even though WORK_DIR is a trusted volume.
        if handle is not None:
            opened_stat = os.fstat(handle.fileno())
            if (
                opened_stat.st_dev != file_stat.st_dev
                or opened_stat.st_ino != file_stat.st_ino
            ):
                return "unsafe"
    except (BlockingIOError, JobLockBusy):
        return "busy"
    except FileNotFoundError:
        return "absent"
    finally:
        if acquired:
            try:
                if handle is not None:
                    _release_handle(handle)
                else:
                    local_lock.release()
            finally:
                if handle is not None:
                    handle.close()
        elif handle is not None:
            handle.close()

    # Releasing the target lock before unlink is safe here: the exclusive
    # namespace guard prevents every compliant opener from entering.
    try:
        os.unlink(path)
    except FileNotFoundError:
        return "absent"
    return "removed"


def _remove_released_lock_file(work_root: str, path: str) -> None:
    """Best-effort fast-path cleanup which never delays completed work."""
    try:
        with _namespace_guard(
            work_root,
            exclusive=True,
            blocking=False,
        ):
            _remove_idle_lock_file_under_guard(path)
    except JobLockBusy:
        # A waiter or another cleanup owns the namespace transition. The
        # periodic bounded sweep will reclaim this path later.
        return
    except Exception:
        logger.debug(
            "Idle job-lock cleanup failed path=%s",
            path,
            exc_info=True,
        )


def reap_idle_job_locks(
    work_root: str,
    *,
    limit: int = _DEFAULT_REAP_LIMIT,
) -> dict:
    """Remove at most ``limit`` idle lock anchors left by crashes.

    Normal releases remove their own target. This bounded sweep handles a
    process killed before its ``finally`` block and pre-upgrade persistent
    anchors. It never waits for the namespace or a target lock.
    """
    summary = {
        "scanned": 0,
        "removed": 0,
        "busy": 0,
        "unsafe": 0,
        "errors": 0,
    }
    scan_limit = max(0, int(limit))
    if scan_limit == 0:
        return summary

    lock_dir = _lock_dir(work_root)
    try:
        with _namespace_guard(
            work_root,
            exclusive=True,
            blocking=False,
        ):
            with os.scandir(lock_dir) as iterator:
                entries = sorted(iterator, key=lambda item: item.name)
            candidates = [
                entry.path
                for entry in entries
                if entry.name != _NAMESPACE_GUARD
                and entry.name.endswith(".lock")
            ][:scan_limit]
            for path in candidates:
                summary["scanned"] += 1
                try:
                    outcome = _remove_idle_lock_file_under_guard(path)
                except Exception:
                    summary["errors"] += 1
                    logger.debug(
                        "Idle job-lock sweep failed path=%s",
                        path,
                        exc_info=True,
                    )
                    continue
                if outcome in summary:
                    summary[outcome] += 1
    except JobLockBusy:
        summary["namespace_busy"] = True
    return summary


@contextmanager
def job_lock(
    work_root: str,
    key: str,
    *,
    purpose: str,
    shared: bool = False,
    blocking: bool = True,
):
    """Hold a filesystem lock shared by every container using ``WORK_DIR``.

    Target files are removed only while holding a namespace guard that every
    opener retains until target acquisition. This prevents a waiter and a new
    opener from locking different inodes while avoiding unbounded per-job
    lock-file growth. The namespace guard itself intentionally persists.
    """
    path = _lock_path(work_root, key)
    mode = "shared" if shared and fcntl is not None else "exclusive"
    started = time.monotonic()
    logger.info(
        "Waiting for %s job lock purpose=%s key=%s path=%s",
        mode,
        purpose,
        key,
        path,
    )

    handle = None
    local_lock = None
    acquired = False
    try:
        if fcntl is not None or msvcrt is not None:
            with _namespace_guard(
                work_root,
                exclusive=False,
                blocking=blocking,
            ):
                handle = open(path, "a+b", buffering=0)
                _seed_lock_file(handle)
                _acquire_handle(
                    handle,
                    shared=shared,
                    blocking=blocking,
                )
        else:  # pragma: no cover - only an exotic development fallback.
            logger.warning(
                "No OS advisory-lock API is available; using process-local lock"
            )
            with _namespace_guard(
                work_root,
                exclusive=False,
                blocking=blocking,
            ):
                local_lock = _fallback_lock(path)
                if not local_lock.acquire(blocking=blocking):
                    raise JobLockBusy("job lock is already held")
        acquired = True
        logger.info(
            "Acquired %s job lock purpose=%s key=%s wait_sec=%.3f",
            mode,
            purpose,
            key,
            time.monotonic() - started,
        )
        yield path
    finally:
        try:
            if acquired:
                try:
                    if handle is not None:
                        if fcntl is not None:
                            _release_posix(handle)
                        else:
                            _release_windows(handle)
                    elif local_lock is not None:
                        local_lock.release()
                except Exception:
                    # Closing the descriptor below is itself an OS-level
                    # unlock.  Do not turn completed durable work into a retry
                    # solely because an explicit unlock syscall failed.
                    logger.exception(
                        "Explicit job-lock release failed; closing descriptor "
                        "purpose=%s key=%s",
                        purpose,
                        key,
                    )
        finally:
            if handle is not None:
                handle.close()
            if acquired:
                _remove_released_lock_file(work_root, path)
                logger.info(
                    "Released %s job lock purpose=%s key=%s",
                    mode,
                    purpose,
                    key,
                )
