#!/usr/bin/env python3
"""Stream low-overhead Docker, GPU, and host-disk samples to JSONL."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import signal
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TextIO


CONTAINERS = (
    "hls-engine-transcoder-gpu-worker-1",
    "hls-engine-transcoder-cpu-worker",
    "hls-engine-minio",
    "hls-engine-postgres",
    "hls-engine-rabbitmq",
    "hls-engine-upload",
)
ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--disk-interval", type=float, default=5.0)
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Recorder:
    def __init__(self, stream: TextIO, run_id: str) -> None:
        self.stream = stream
        self.run_id = run_id
        self.started = time.monotonic()
        self.lock = threading.Lock()

    def write(self, source: str, payload: Any) -> None:
        record = {
            "run_id": self.run_id,
            "utc_ts": utc_now(),
            "elapsed_seconds": round(time.monotonic() - self.started, 3),
            "source": source,
            "payload": payload,
        }
        with self.lock:
            self.stream.write(json.dumps(record, separators=(",", ":")) + "\n")
            self.stream.flush()


def stream_process(
    stop: threading.Event,
    recorder: Recorder,
    source: str,
    command: list[str],
    *,
    parse_json: bool = False,
) -> None:
    process: subprocess.Popen[str] | None = None
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        assert process.stdout is not None
        for raw_line in process.stdout:
            if stop.is_set():
                break
            line = ANSI_ESCAPE.sub("", raw_line).strip()
            if not line:
                continue
            payload: Any = line
            if parse_json:
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    payload = {"unparsed": line}
            recorder.write(source, payload)
    except Exception as error:  # observer failure must not affect the pipeline
        recorder.write(source, {"observer_error": f"{type(error).__name__}: {error}"})
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()


def sample_disks(
    stop: threading.Event,
    recorder: Recorder,
    interval: float,
) -> None:
    while not stop.is_set():
        payload: dict[str, Any] = {}
        for drive in ("C:\\", "D:\\"):
            try:
                usage = shutil.disk_usage(drive)
                payload[drive[0]] = {
                    "total_bytes": usage.total,
                    "used_bytes": usage.used,
                    "free_bytes": usage.free,
                }
            except OSError as error:
                payload[drive[0]] = {"error": str(error)}
        recorder.write("host_disk", payload)
        stop.wait(interval)


def main() -> int:
    args = parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    stop = threading.Event()

    def request_stop(_signum: int, _frame: object) -> None:
        stop.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    with args.output.open("a", encoding="utf-8", buffering=1) as stream:
        recorder = Recorder(stream, args.run_id)
        recorder.write("observer", {"event": "started"})

        docker_command = [
            "docker",
            "stats",
            "--format",
            "{{json .}}",
            *CONTAINERS,
        ]
        gpu_command = [
            "nvidia-smi",
            "--query-gpu=timestamp,index,utilization.gpu,utilization.encoder,"
            "utilization.decoder,memory.used,memory.total,power.draw,temperature.gpu",
            "--format=csv,noheader,nounits",
            "--loop-ms=2000",
        ]

        threads = [
            threading.Thread(
                target=stream_process,
                args=(stop, recorder, "docker_stats", docker_command),
                kwargs={"parse_json": True},
                daemon=True,
            ),
            threading.Thread(
                target=stream_process,
                args=(stop, recorder, "nvidia_smi", gpu_command),
                daemon=True,
            ),
            threading.Thread(
                target=sample_disks,
                args=(stop, recorder, args.disk_interval),
                daemon=True,
            ),
        ]
        for thread in threads:
            thread.start()

        try:
            while not stop.wait(1):
                pass
        finally:
            stop.set()
            for thread in threads:
                thread.join(timeout=7)
            recorder.write("observer", {"event": "stopped"})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
