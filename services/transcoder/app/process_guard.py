"""Linux subprocess guard that kills a media process tree with its task.

Celery can hard-kill a prefork child. Without a parent-death guard, an FFmpeg
process in its own session becomes an orphan and may keep using NVENC after the
task's Redis lease expires. This tiny supervisor is launched as the session
leader, asks Linux for a parent-death signal, and kills its whole process group
if the owning task process disappears.
"""

from __future__ import annotations

import ctypes
import os
import signal
import subprocess
import sys


_PR_SET_PDEATHSIG = 1


def _kill_process_group(_signum=None, _frame=None) -> None:
    try:
        os.killpg(os.getpgrp(), signal.SIGKILL)
    except ProcessLookupError:
        os._exit(128 + signal.SIGKILL)


def _arm_parent_death_signal(expected_parent_pid: int) -> None:
    signal.signal(signal.SIGTERM, _kill_process_group)
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(
        _PR_SET_PDEATHSIG,
        signal.SIGTERM,
        0,
        0,
        0,
    ) != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))

    # The parent may have died between creating this process and prctl().
    if os.getppid() != expected_parent_pid:
        _kill_process_group()


def main(argv: list[str]) -> int:
    if len(argv) < 3:
        print(
            "usage: python -m app.process_guard PARENT_PID COMMAND [ARG ...]",
            file=sys.stderr,
        )
        return 64
    try:
        expected_parent_pid = int(argv[1])
    except ValueError:
        print("process_guard: invalid parent pid", file=sys.stderr)
        return 64

    _arm_parent_death_signal(expected_parent_pid)
    command = argv[2:]
    try:
        child = subprocess.Popen(command)
    except OSError as exc:
        print(f"process_guard: could not start command: {exc}", file=sys.stderr)
        return 127
    return child.wait()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
