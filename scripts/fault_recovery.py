#!/usr/bin/env python3
"""Verify upload completion survives a RabbitMQ outage and retry."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import httpx


INGESTION_JOB_NAMESPACE = uuid.UUID("35f1bb68-928d-4f1f-9f63-8ac8c176c4df")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8088")
    parser.add_argument("--media", type=Path, default=Path("test-run.e2e.mp4"))
    parser.add_argument("--timeout-seconds", type=int, default=900)
    parser.add_argument(
        "--report", type=Path, default=Path("test-run.fault-recovery.json")
    )
    return parser.parse_args()


def emit(message: str, *, error: bool = False) -> None:
    try:
        print(message, file=sys.stderr if error else sys.stdout, flush=True)
    except (BrokenPipeError, OSError):
        pass


def require(response: httpx.Response, expected: int | tuple[int, ...]) -> httpx.Response:
    statuses = (expected,) if isinstance(expected, int) else expected
    if response.status_code not in statuses:
        raise RuntimeError(
            f"{response.request.method} {response.request.url} returned "
            f"{response.status_code}, expected {statuses}: {response.text[:1000]}"
        )
    return response


def compose(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "compose", *arguments],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=120,
    )


def rabbit_health() -> str:
    result = subprocess.run(
        [
            "docker",
            "inspect",
            "--format={{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}",
            "hls-engine-rabbitmq",
        ],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        timeout=10,
    )
    return result.stdout.strip() if result.returncode == 0 else "missing"


def wait_for_rabbit(deadline: float) -> None:
    while time.monotonic() < deadline:
        if rabbit_health() == "healthy":
            return
        time.sleep(1)
    raise TimeoutError("RabbitMQ did not become healthy")


def ingestion_job_id(video_id: str, bucket: str, object_name: str) -> str:
    identity = "\n".join((video_id, bucket, object_name))
    return str(uuid.uuid5(INGESTION_JOB_NAMESPACE, identity))


def main() -> int:
    args = parse_args()
    media = args.media.resolve()
    if not media.is_file() or media.stat().st_size <= 0:
        raise RuntimeError(f"media file is missing or empty: {media}")

    base_url = args.base_url.rstrip("/") + "/"
    api_url = urljoin(base_url, "api/v1/")
    started = time.monotonic()
    run_id = uuid.uuid4().hex[:12]
    report: dict[str, Any] = {
        "run_id": run_id,
        "base_url": base_url.rstrip("/"),
        "media": {"name": media.name, "bytes": media.stat().st_size},
        "timeline": [],
        "passed": False,
    }
    rabbit_stopped = False

    def record(stage: str, **details: Any) -> None:
        elapsed = round(time.monotonic() - started, 3)
        report["timeline"].append({"stage": stage, "elapsed_seconds": elapsed, **details})
        summary = " ".join(f"{key}={value}" for key, value in details.items())
        emit(f"[{elapsed:7.2f}s] {stage}" + (f" {summary}" if summary else ""))

    try:
        wait_for_rabbit(time.monotonic() + 30)
        with httpx.Client(timeout=httpx.Timeout(60, write=180)) as client:
            email = f"hls-fault-{run_id}@example.test"
            password = f"Fault-{run_id}-Safe!"
            require(
                client.post(
                    urljoin(api_url, "auth/register"),
                    json={
                        "email": email,
                        "username": f"fault-{run_id}",
                        "password": password,
                    },
                ),
                201,
            )
            login = require(
                client.post(
                    urljoin(api_url, "auth/login"),
                    json={"email": email, "password": password},
                ),
                200,
            ).json()
            headers = {"Authorization": f"Bearer {login['access_token']}"}

            video = require(
                client.post(
                    urljoin(api_url, "videos"),
                    headers=headers,
                    json={"title": f"Broker recovery {run_id}", "tags": ["fault-test"]},
                ),
                201,
            ).json()
            video_id = video["id"]

            session = require(
                client.post(
                    urljoin(api_url, f"videos/{video_id}/upload/session"),
                    headers=headers,
                    json={
                        "filename": media.name,
                        "size": media.stat().st_size,
                        "content_type": "video/mp4",
                    },
                ),
                201,
            ).json()
            session_id = session["session_id"]
            part_size = int(session["part_size"])
            total_parts = (media.stat().st_size + part_size - 1) // part_size
            with media.open("rb") as source:
                for part_number in range(1, total_parts + 1):
                    chunk = source.read(part_size)
                    require(
                        client.put(
                            urljoin(
                                api_url,
                                f"videos/{video_id}/upload/session/{session_id}/parts/"
                                f"{part_number}",
                            ),
                            headers={
                                **headers,
                                "Content-Type": "application/octet-stream",
                            },
                            content=chunk,
                        ),
                        200,
                    )
            record("upload_staged", video_id=video_id, session_id=session_id)

            compose("stop", "-t", "5", "rabbitmq")
            rabbit_stopped = True
            record("rabbitmq_stopped")

            complete_url = urljoin(
                api_url,
                f"videos/{video_id}/upload/session/{session_id}/complete",
            )
            failed_completion = client.post(complete_url, headers=headers)
            if failed_completion.status_code != 503:
                raise RuntimeError(
                    "completion during broker outage returned "
                    f"{failed_completion.status_code}, expected 503: "
                    f"{failed_completion.text[:1000]}"
                )
            retry_after = failed_completion.headers.get("retry-after")
            session_state = require(
                client.get(
                    urljoin(
                        api_url,
                        f"videos/{video_id}/upload/session/{session_id}",
                    ),
                    headers=headers,
                ),
                200,
            ).json()
            if session_state.get("status") != "completed_pending_event":
                raise RuntimeError(
                    "completion recovery state was not retained: "
                    f"{session_state.get('status')}"
                )
            record(
                "completion_failed_safely",
                status=failed_completion.status_code,
                retry_after=retry_after,
                session_status=session_state.get("status"),
            )

            compose("start", "rabbitmq")
            rabbit_stopped = False
            wait_for_rabbit(time.monotonic() + 90)
            record("rabbitmq_recovered")

            # Container health can turn green just before Docker DNS and every
            # long-lived publisher observe the restarted broker. Honor the
            # API's retry contract until the bounded recovery window closes.
            completion_attempts = 0
            completion_deadline = time.monotonic() + 30
            while True:
                completion_attempts += 1
                completion_response = client.post(complete_url, headers=headers)
                if completion_response.status_code == 200:
                    first_completion = completion_response.json()
                    break
                if (
                    completion_response.status_code != 503
                    or time.monotonic() >= completion_deadline
                ):
                    require(completion_response, 200)
                retry_after_seconds = int(
                    completion_response.headers.get("retry-after", "1")
                )
                time.sleep(max(1, retry_after_seconds))

            second_completion = require(
                client.post(complete_url, headers=headers),
                200,
            ).json()
            if first_completion != second_completion:
                raise RuntimeError("replayed completion returned a different response")
            record(
                "completion_replayed_idempotently",
                event_id=first_completion["id"],
                recovery_attempts=completion_attempts,
            )

            object_name = first_completion["object_name"]
            bucket = first_completion["bucket"]
            job_id = ingestion_job_id(video_id, bucket, object_name)
            report.update(
                {
                    "video_id": video_id,
                    "job_id": job_id,
                    "event_id": first_completion["id"],
                    "object_name": object_name,
                }
            )

            deadline = time.monotonic() + args.timeout_seconds
            last_status: str | None = None
            while time.monotonic() < deadline:
                response = client.get(
                    urljoin(api_url, f"jobs/{job_id}"),
                    headers=headers,
                )
                if response.status_code == 404:
                    time.sleep(1)
                    continue
                job = require(response, 200).json()
                status = str(job.get("status"))
                if status != last_status:
                    record("job_status", status=status)
                    last_status = status
                if status == "completed":
                    break
                if status == "failed":
                    raise RuntimeError(f"recovered job failed: {job.get('error_message')}")
                time.sleep(2)
            else:
                raise TimeoutError("recovered upload did not complete transcoding")

            video_state = require(
                client.get(urljoin(api_url, f"videos/{video_id}"), headers=headers),
                200,
            ).json()
            if video_state.get("status") != "ready":
                raise RuntimeError(f"video status is {video_state.get('status')}, expected ready")

            report["passed"] = True
            record("broker_outage_recovery_passed")
    except Exception as error:
        report["error"] = f"{type(error).__name__}: {error}"
        record("failed", error=report["error"])
    finally:
        if rabbit_stopped or rabbit_health() != "healthy":
            try:
                compose("start", "rabbitmq")
                wait_for_rabbit(time.monotonic() + 90)
                record("rabbitmq_restored_in_cleanup")
            except Exception as cleanup_error:
                report["cleanup_error"] = (
                    f"{type(cleanup_error).__name__}: {cleanup_error}"
                )
        report["duration_seconds"] = round(time.monotonic() - started, 3)
        args.report.write_text(
            json.dumps(report, indent=2, default=str) + "\n",
            encoding="utf-8",
        )
        emit(f"Report: {args.report.resolve()}")

    if report["passed"] and "cleanup_error" not in report:
        emit("RESULT: PASS")
        return 0
    emit("RESULT: FAIL", error=True)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
