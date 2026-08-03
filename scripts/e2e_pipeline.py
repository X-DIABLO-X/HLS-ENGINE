#!/usr/bin/env python3
"""Exercise the HLS engine from registration through signed segment playback."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import httpx


INGESTION_JOB_NAMESPACE = uuid.UUID("35f1bb68-928d-4f1f-9f63-8ac8c176c4df")


def emit(message: str, *, error: bool = False) -> None:
    """Keep the test running when a detached Windows console closes its pipe."""
    try:
        print(message, file=sys.stderr if error else sys.stdout, flush=True)
    except (BrokenPipeError, OSError):
        pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8088")
    parser.add_argument("--media", type=Path, default=Path("test-run.e2e.mp4"))
    parser.add_argument("--timeout-seconds", type=int, default=900)
    parser.add_argument("--poll-seconds", type=float, default=3)
    parser.add_argument(
        "--delete-after-test",
        action="store_true",
        help="Delete the generated video and stored media after validation.",
    )
    parser.add_argument(
        "--cleanup-timeout-seconds",
        type=float,
        default=9000.0,
        help=(
            "Bounded time for lock-aware workspace reclamation and retryable "
            "media deletion. The default exceeds the two-hour task hard limit."
        ),
    )
    parser.add_argument(
        "--report", type=Path, default=Path("test-run.e2e-report.json")
    )
    return parser.parse_args()


def require(response: httpx.Response, expected: int | tuple[int, ...]) -> httpx.Response:
    statuses = (expected,) if isinstance(expected, int) else expected
    if response.status_code not in statuses:
        body = response.text[:1_000]
        raise RuntimeError(
            f"{response.request.method} {response.request.url} returned "
            f"{response.status_code}, expected {statuses}: {body}"
        )
    return response


def first_playlist_uri(manifest: str) -> str:
    uris = playlist_uris(manifest)
    if uris:
        return uris[0]
    raise RuntimeError("manifest contains no media URI")


def playlist_uris(manifest: str) -> list[str]:
    return [
        raw_line.strip()
        for raw_line in manifest.splitlines()
        if raw_line.strip() and not raw_line.lstrip().startswith("#")
    ]


def media_entries(master: str) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    attribute_pattern = re.compile(r'([A-Z0-9-]+)=("[^"]*"|[^,]*)')
    for raw_line in master.splitlines():
        line = raw_line.strip()
        if not line.startswith("#EXT-X-MEDIA:"):
            continue
        attributes: dict[str, str] = {}
        for key, value in attribute_pattern.findall(line.split(":", 1)[1]):
            attributes[key] = value[1:-1] if value.startswith('"') else value
        if attributes.get("TYPE") in {"AUDIO", "SUBTITLES"}:
            entries.append(attributes)
    return entries


def extinf_duration(manifest: str) -> float:
    values = re.findall(r"^#EXTINF:([0-9.]+)", manifest, flags=re.MULTILINE)
    return sum(float(value) for value in values)


def probe_source(media: Path) -> dict[str, Any]:
    command = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration:stream=codec_type:stream_tags=language",
        "-of",
        "json",
        str(media),
    ]
    completed = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=120,
    )
    payload = json.loads(completed.stdout)
    streams = payload.get("streams", [])

    def languages(kind: str) -> list[str]:
        return [
            str((stream.get("tags") or {}).get("language") or "und")
            for stream in streams
            if stream.get("codec_type") == kind
        ]

    return {
        "duration_seconds": float((payload.get("format") or {}).get("duration") or 0),
        "audio_languages": languages("audio"),
        "subtitle_languages": languages("subtitle"),
    }


def map_uri(manifest: str) -> str | None:
    match = re.search(r'#EXT-X-MAP:[^\n]*URI="([^"]+)"', manifest)
    return match.group(1) if match else None


def absolute(base_url: str, value: str) -> str:
    return value if value.startswith(("http://", "https://")) else urljoin(base_url, value)


def _retry_after(response: httpx.Response, fallback: float) -> float:
    raw = response.headers.get("retry-after", "").strip()
    try:
        requested = float(raw)
    except ValueError:
        requested = fallback
    return min(10.0, max(0.0, requested))


def ingestion_job_id(video_id: str, bucket: str, object_name: str) -> str:
    """Mirror the engine's deterministic upload-event generation id."""
    identity = "\n".join((str(video_id), str(bucket), str(object_name)))
    return str(uuid.uuid5(INGESTION_JOB_NAMESPACE, identity))


def wait_for_workspace_cleanup(
    client: httpx.Client,
    job_url: str,
    headers: dict[str, str],
    *,
    timeout_seconds: float,
    poll_seconds: float = 2.0,
) -> dict[str, Any]:
    """Wait through active/hard-timeout work and prove its exact directory left."""
    started = time.monotonic()
    deadline = started + max(1.0, timeout_seconds)
    attempts = 0
    last_job_status: str | None = None
    last_workspaces: list[dict[str, Any]] | None = None
    while time.monotonic() < deadline:
        attempts += 1
        request_timeout = max(0.1, min(60.0, deadline - time.monotonic()))
        try:
            response = client.get(
                job_url,
                headers=headers,
                timeout=request_timeout,
            )
            if response.status_code == 200:
                payload = response.json()
                last_job_status = str(payload.get("status") or "unknown")
                workspace = payload.get("workspace")
                if not isinstance(workspace, dict):
                    raise RuntimeError(
                        "job API did not return exact workspace status; "
                        "transcoder API may not have the read-only hls-work volume"
                    )
                workspaces = payload.get("workspaces")
                if not isinstance(workspaces, list) or not workspaces:
                    raise RuntimeError(
                        "job API did not return all generation workspaces"
                    )
                if not all(isinstance(item, dict) for item in workspaces):
                    raise RuntimeError("job API returned malformed workspaces")
                last_workspaces = workspaces
                unsafe = [
                    item
                    for item in workspaces
                    if item.get("state") == "unsafe"
                ]
                if unsafe:
                    raise RuntimeError(
                        f"transcoder refused unsafe workspace: {unsafe}"
                    )
                if all(
                    item.get("state") == "absent"
                    and item.get("exists") is False
                    and item.get("job_status") in {"completed", "failed"}
                    for item in workspaces
                ):
                    return {
                        "passed": True,
                        "attempts": attempts,
                        "seconds": round(time.monotonic() - started, 3),
                        "job_status": last_job_status,
                        "workspace": workspace,
                        "workspaces": workspaces,
                    }
            elif response.status_code == 404:
                # Metadata deletion has not started yet, so a missing durable
                # job can only be a short event-consumer delay. Keep waiting;
                # never infer filesystem cleanup from a missing database row.
                last_job_status = "not-created"
            elif response.status_code < 500:
                require(response, 200)
        except (httpx.TimeoutException, httpx.TransportError):
            pass

        delay = min(
            max(0.1, poll_seconds),
            max(0.0, deadline - time.monotonic()),
        )
        if delay:
            time.sleep(delay)

    raise TimeoutError(
        f"exact transcoder workspace was not verified absent within "
        f"{timeout_seconds:.1f}s; job_status={last_job_status}, "
        f"workspaces={last_workspaces}, attempts={attempts}"
    )


def delete_video_with_retry(
    client: httpx.Client,
    video_url: str,
    headers: dict[str, str],
    *,
    timeout_seconds: float,
    initial_backoff_seconds: float = 0.5,
) -> dict[str, Any]:
    """Resume the durable deletion saga until GET confirms a missing row."""
    cleanup_started = time.monotonic()
    deadline = cleanup_started + max(1.0, timeout_seconds)
    backoff = max(0.0, initial_backoff_seconds)
    attempts: list[dict[str, Any]] = []
    last_delete_status: int | None = None
    last_lookup_status: int | None = None

    while time.monotonic() < deadline:
        attempt: dict[str, Any] = {
            "attempt": len(attempts) + 1,
            "elapsed_seconds": round(time.monotonic() - cleanup_started, 3),
        }
        try:
            request_timeout = max(
                0.1,
                min(60.0, deadline - time.monotonic()),
            )
            deletion = client.delete(
                video_url,
                headers=headers,
                timeout=request_timeout,
            )
            last_delete_status = deletion.status_code
            attempt["delete_status"] = deletion.status_code
            if deletion.status_code in (204, 404):
                lookup = client.get(
                    video_url,
                    headers=headers,
                    timeout=max(
                        0.1,
                        min(60.0, deadline - time.monotonic()),
                    ),
                )
                last_lookup_status = lookup.status_code
                attempt["lookup_status"] = lookup.status_code
                if lookup.status_code == 404:
                    attempts.append(attempt)
                    return {
                        "passed": True,
                        "attempts": len(attempts),
                        "delete_status": last_delete_status,
                        "lookup_status": last_lookup_status,
                        "seconds": round(
                            time.monotonic() - cleanup_started,
                            3,
                        ),
                        "history": attempts,
                    }
                if lookup.status_code >= 500:
                    delay = backoff
                elif lookup.status_code == 200:
                    # A timed-out/ambiguous earlier request may have committed
                    # only the tombstone. Reissue DELETE to resume the saga.
                    delay = backoff
                else:
                    require(lookup, 404)
            elif deletion.status_code in (409, 503):
                delay = _retry_after(deletion, backoff)
            else:
                require(deletion, (204, 404))
        except (httpx.TimeoutException, httpx.TransportError) as error:
            attempt["error"] = f"{type(error).__name__}: {error}"
            delay = backoff

        delay = min(delay, max(0.0, deadline - time.monotonic()))
        attempt["retry_in_seconds"] = round(delay, 3)
        attempts.append(attempt)
        if delay > 0:
            time.sleep(delay)
        backoff = min(10.0, max(0.5, backoff * 2))

    raise TimeoutError(
        f"video deletion was not verified within {timeout_seconds:.1f}s; "
        f"last delete={last_delete_status}, lookup={last_lookup_status}, "
        f"attempts={len(attempts)}"
    )


def main() -> int:
    args = parse_args()
    media = args.media.resolve()
    if not media.is_file():
        raise RuntimeError(f"media file not found: {media}")
    if media.stat().st_size <= 0:
        raise RuntimeError(f"media file is empty: {media}")

    base_url = args.base_url.rstrip("/") + "/"
    api_url = urljoin(base_url, "api/v1/")
    content_type = {
        ".mkv": "video/x-matroska",
        ".mov": "video/quicktime",
        ".webm": "video/webm",
    }.get(media.suffix.lower(), "video/mp4")
    run_id = uuid.uuid4().hex[:12]
    email = f"hls-e2e-{run_id}@example.test"
    password = f"E2e-{run_id}-Safe!"
    started = time.monotonic()
    source_identity = {
        "path": str(media),
        "bytes": media.stat().st_size,
        "mtime_ns": media.stat().st_mtime_ns,
    }
    report: dict[str, Any] = {
        "run_id": run_id,
        "base_url": base_url.rstrip("/"),
        "media": {"name": media.name, "bytes": media.stat().st_size},
        "checks": {},
        "timeline": [],
    }
    access_token: str | None = None
    video_id: str | None = None
    job_id: str | None = None

    def record(stage: str, **details: Any) -> None:
        elapsed = round(time.monotonic() - started, 3)
        report["timeline"].append({"stage": stage, "elapsed_seconds": elapsed, **details})
        summary = " ".join(f"{key}={value}" for key, value in details.items())
        emit(f"[{elapsed:7.2f}s] {stage}" + (f" {summary}" if summary else ""))

    try:
        source_probe = probe_source(media)
        report["source_probe"] = source_probe
        record(
            "source_probed",
            duration_seconds=round(source_probe["duration_seconds"], 3),
            audio_tracks=len(source_probe["audio_languages"]),
            subtitle_tracks=len(source_probe["subtitle_languages"]),
        )

        with httpx.Client(timeout=httpx.Timeout(60.0, write=180.0)) as client:
            require(client.get(base_url), 200)
            record("edge_ready")

            require(
                client.post(
                    urljoin(api_url, "auth/register"),
                    json={"email": email, "username": f"e2e-{run_id}", "password": password},
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
            access_token = login["access_token"]
            refresh_token = login["refresh_token"]
            headers = {"Authorization": f"Bearer {access_token}"}
            record("authenticated")

            # A refresh token must never authorize a protected API request.
            refresh_attempt = client.get(
                urljoin(api_url, "videos"),
                headers={"Authorization": f"Bearer {refresh_token}"},
            )
            refresh_rejected = refresh_attempt.status_code == 401
            report["checks"]["refresh_token_rejected_as_access"] = {
                "passed": refresh_rejected,
                "status": refresh_attempt.status_code,
            }
            record(
                "refresh_token_boundary",
                status=refresh_attempt.status_code,
                passed=refresh_rejected,
            )

            video = require(
                client.post(
                    urljoin(api_url, "videos"),
                    headers=headers,
                    json={
                        "title": f"HLS end-to-end {run_id}",
                        "description": "Automated upload-to-playback resilience test",
                        "tags": ["e2e", "automated"],
                    },
                ),
                201,
            ).json()
            video_id = video["id"]
            report["video_id"] = video_id
            record("video_created", video_id=video_id)

            session = require(
                client.post(
                    urljoin(api_url, f"videos/{video_id}/upload/session"),
                    headers=headers,
                    json={
                        "filename": media.name,
                        "size": media.stat().st_size,
                        "content_type": content_type,
                    },
                ),
                201,
            ).json()
            session_id = session["session_id"]
            part_size = int(session["part_size"])
            total_parts = (media.stat().st_size + part_size - 1) // part_size
            record(
                "upload_session_created",
                session_id=session_id,
                part_size=part_size,
                total_parts=total_parts,
            )

            upload_started = time.monotonic()
            uploaded_bytes = 0
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
                    uploaded_bytes += len(chunk)
                    record(
                        "upload_part_complete",
                        part=part_number,
                        bytes=len(chunk),
                    )
            upload_seconds = time.monotonic() - upload_started
            record(
                "upload_transfer_complete",
                bytes=uploaded_bytes,
                seconds=round(upload_seconds, 3),
                throughput_mbps=round(
                    uploaded_bytes * 8 / max(upload_seconds, 0.001) / 1_000_000,
                    3,
                ),
            )

            completion = require(
                client.post(
                    urljoin(
                        api_url,
                        f"videos/{video_id}/upload/session/{session_id}/complete",
                    ),
                    headers=headers,
                ),
                200,
            ).json()
            report["upload"] = {
                "object_name": completion.get("object_name"),
                "bytes": completion.get("size"),
            }
            object_name = str(completion.get("object_name") or "")
            bucket = str(completion.get("bucket") or "uploads-raw")
            if not object_name:
                raise RuntimeError("upload completion omitted object_name")
            job_id = ingestion_job_id(video_id, bucket, object_name)
            report["job_id"] = job_id
            record("upload_complete", object_name=completion.get("object_name"))

            deadline = time.monotonic() + args.timeout_seconds
            last_status: str | None = None
            last_progress: dict[str, Any] | None = None
            while time.monotonic() < deadline:
                current = require(
                    client.get(urljoin(api_url, f"videos/{video_id}"), headers=headers),
                    200,
                ).json()
                status = str(current.get("status", "unknown"))
                try:
                    progress_response = client.get(
                        urljoin(api_url, f"videos/{video_id}/progress"),
                        headers=headers,
                    )
                    if progress_response.status_code == 200:
                        last_progress = progress_response.json()
                except httpx.HTTPError:
                    pass

                progress_key = json.dumps(last_progress, sort_keys=True, default=str)
                status_key = f"{status}:{progress_key}"
                if status_key != last_status:
                    record("processing", status=status, progress=last_progress)
                    last_status = status_key
                if status == "ready":
                    report["video"] = current
                    break
                if status == "failed":
                    raise RuntimeError(f"transcoding failed: {last_progress}")
                time.sleep(args.poll_seconds)
            else:
                raise TimeoutError(
                    f"video {video_id} did not become ready within "
                    f"{args.timeout_seconds} seconds; last progress={last_progress}"
                )

            manifest_data = require(
                client.get(
                    urljoin(api_url, f"videos/{video_id}/manifest"),
                    headers=headers,
                ),
                200,
            ).json()
            master_url = absolute(base_url, manifest_data["url"])
            master_response = require(client.get(master_url), 200)
            master = master_response.text
            if not master.startswith("#EXTM3U"):
                raise RuntimeError("master response is not an HLS manifest")
            record("master_manifest_playable", bytes=len(master_response.content))

            variant_values = playlist_uris(master)
            if not variant_values:
                raise RuntimeError("master manifest contains no video variants")
            manifest_media = media_entries(master)
            audio_media = [
                entry for entry in manifest_media if entry.get("TYPE") == "AUDIO"
            ]
            subtitle_media = [
                entry
                for entry in manifest_media
                if entry.get("TYPE") == "SUBTITLES"
            ]
            expected_audio = len(source_probe["audio_languages"])
            expected_subtitles = len(source_probe["subtitle_languages"])
            if len(audio_media) != expected_audio:
                raise RuntimeError(
                    f"master exposes {len(audio_media)} audio tracks; "
                    f"source contains {expected_audio}"
                )
            if len(subtitle_media) != expected_subtitles:
                raise RuntimeError(
                    f"master exposes {len(subtitle_media)} subtitle tracks; "
                    f"source contains {expected_subtitles}"
                )
            for kind, entries in (
                ("audio", audio_media),
                ("subtitle", subtitle_media),
            ):
                names = [entry.get("NAME", "") for entry in entries]
                uris = [entry.get("URI", "") for entry in entries]
                if any(not value for value in names + uris):
                    raise RuntimeError(f"{kind} track is missing NAME or URI")
                if len(names) != len(set(names)):
                    raise RuntimeError(f"{kind} track NAME values are not unique")
                if len(uris) != len(set(uris)):
                    raise RuntimeError(f"{kind} track URI values are not unique")
            record(
                "master_tracks_verified",
                variants=len(variant_values),
                audio_tracks=len(audio_media),
                subtitle_tracks=len(subtitle_media),
            )

            variant_url = absolute(master_url, variant_values[0])
            variant_response = require(client.get(variant_url), 200)
            variant = variant_response.text
            if not variant.startswith("#EXTM3U"):
                raise RuntimeError("variant response is not an HLS manifest")
            record("variant_manifest_playable", bytes=len(variant_response.content))

            init_value = map_uri(variant)
            init_bytes = 0
            if init_value:
                init_response = require(
                    client.get(absolute(variant_url, init_value)),
                    200,
                )
                init_bytes = len(init_response.content)
                if init_bytes == 0:
                    raise RuntimeError("HLS init segment is empty")
                record("init_segment_playable", bytes=init_bytes)

            segment_url = absolute(variant_url, first_playlist_uri(variant))
            segment_response = require(client.get(segment_url), 200)
            segment_bytes = len(segment_response.content)
            if segment_bytes == 0:
                raise RuntimeError("HLS media segment is empty")
            record("media_segment_playable", bytes=segment_bytes)

            range_response = client.get(
                segment_url,
                headers={"Range": "bytes=0-1023"},
            )
            range_semantics = range_response.status_code == 206
            report["checks"]["http_range_semantics"] = {
                "passed": range_semantics,
                "status": range_response.status_code,
                "bytes": len(range_response.content),
                "content_range": range_response.headers.get("content-range"),
            }
            record(
                "range_request",
                status=range_response.status_code,
                bytes=len(range_response.content),
                passed=range_semantics,
            )

            unsigned_response = client.get(
                urljoin(base_url, f"hls/{video_id}/master.m3u8")
            )
            unsigned_denied = unsigned_response.status_code in (401, 403)
            report["checks"]["unsigned_hls_denied"] = {
                "passed": unsigned_denied,
                "status": unsigned_response.status_code,
            }
            record(
                "unsigned_hls_boundary",
                status=unsigned_response.status_code,
                passed=unsigned_denied,
            )

            expected_duration = float(source_probe["duration_seconds"])
            # Matroska's format duration can legitimately exceed the last
            # decodable A/V packet (this source differs by about 13 seconds),
            # but a wider percentage would hide a missing group of segments.
            duration_tolerance = max(20.0, expected_duration * 0.002)
            published_playlists: list[dict[str, Any]] = []
            playlist_specs = [
                *[
                    ("video", index, absolute(master_url, value))
                    for index, value in enumerate(variant_values, start=1)
                ],
                *[
                    (
                        entry["TYPE"].lower(),
                        index,
                        absolute(master_url, entry["URI"]),
                    )
                    for index, entry in enumerate(manifest_media, start=1)
                ],
            ]
            for kind, index, playlist_url in playlist_specs:
                response = require(client.get(playlist_url), 200)
                body = response.text
                if not body.startswith("#EXTM3U"):
                    raise RuntimeError(f"{kind} playlist {index} is not HLS")
                if "#EXT-X-ENDLIST" not in body:
                    raise RuntimeError(f"{kind} playlist {index} is incomplete")
                object_uris = playlist_uris(body)
                if not object_uris:
                    raise RuntimeError(f"{kind} playlist {index} has no media objects")
                duration = extinf_duration(body)
                if abs(duration - expected_duration) > duration_tolerance:
                    raise RuntimeError(
                        f"{kind} playlist {index} duration {duration:.3f}s differs "
                        f"from source {expected_duration:.3f}s"
                    )
                edge_indexes = sorted({0, len(object_uris) - 1})
                for object_index in edge_indexes:
                    object_url = absolute(playlist_url, object_uris[object_index])
                    edge_response = client.get(
                        object_url,
                        headers={"Range": "bytes=0-1023"},
                    )
                    if edge_response.status_code != 206 or not edge_response.content:
                        raise RuntimeError(
                            f"{kind} playlist {index} object {object_index} "
                            f"failed range playback: {edge_response.status_code}"
                        )
                published_playlists.append(
                    {
                        "type": kind,
                        "index": index,
                        "segments": len(object_uris),
                        "duration_seconds": round(duration, 3),
                    }
                )
            report["published_playlists"] = published_playlists
            record(
                "all_playlists_verified",
                playlists=len(published_playlists),
                edge_objects=sum(
                    min(2, item["segments"]) for item in published_playlists
                ),
            )

            report["playback"] = {
                "master_url": manifest_data["url"],
                "master_bytes": len(master_response.content),
                "variant_bytes": len(variant_response.content),
                "init_bytes": init_bytes,
                "segment_bytes": segment_bytes,
            }
            required_checks = (
                refresh_rejected,
                range_semantics,
                unsigned_denied,
            )
            report["passed"] = all(required_checks)
    except Exception as error:
        report["passed"] = False
        report["error"] = f"{type(error).__name__}: {error}"
        record("failed", error=report["error"])
    finally:
        if args.delete_after_test and video_id and access_token:
            cleanup_started = time.monotonic()
            try:
                cleanup_headers = {
                    "Authorization": f"Bearer {access_token}",
                }
                with httpx.Client(timeout=60.0) as cleanup_client:
                    if job_id:
                        try:
                            report["workspace_cleanup"] = (
                                wait_for_workspace_cleanup(
                                    cleanup_client,
                                    urljoin(api_url, f"jobs/{job_id}"),
                                    cleanup_headers,
                                    timeout_seconds=args.cleanup_timeout_seconds,
                                    poll_seconds=max(0.5, args.poll_seconds),
                                )
                            )
                            record(
                                "workspace_deleted",
                                job_id=job_id,
                                attempts=report["workspace_cleanup"]["attempts"],
                                cleanup_seconds=report["workspace_cleanup"][
                                    "seconds"
                                ],
                                exact_path=report["workspace_cleanup"][
                                    "workspace"
                                ].get("path"),
                                exact_paths=[
                                    item.get("path")
                                    for item in report["workspace_cleanup"][
                                        "workspaces"
                                    ]
                                ],
                            )
                        except Exception as workspace_error:
                            # Still run the resumable media deletion so a
                            # failed assertion cannot strand the uploaded
                            # object. The report remains failed because exact
                            # local reclamation was not proven.
                            report["passed"] = False
                            report["workspace_cleanup"] = {
                                "passed": False,
                                "error": (
                                    f"{type(workspace_error).__name__}: "
                                    f"{workspace_error}"
                                ),
                            }
                            record(
                                "workspace_cleanup_failed",
                                error=report["workspace_cleanup"]["error"],
                            )
                    report["cleanup"] = delete_video_with_retry(
                        cleanup_client,
                        urljoin(api_url, f"videos/{video_id}"),
                        cleanup_headers,
                        timeout_seconds=args.cleanup_timeout_seconds,
                    )
                record(
                    "video_deleted",
                    delete_status=report["cleanup"]["delete_status"],
                    lookup_status=report["cleanup"]["lookup_status"],
                    attempts=report["cleanup"]["attempts"],
                    cleanup_seconds=report["cleanup"]["seconds"],
                )
            except Exception as cleanup_error:
                report["passed"] = False
                report["cleanup"] = {
                    "passed": False,
                    "seconds": round(time.monotonic() - cleanup_started, 3),
                    "error": (
                        f"{type(cleanup_error).__name__}: {cleanup_error}"
                    ),
                }
                record(
                    "cleanup_failed",
                    error=report["cleanup"]["error"],
                )
        source_preserved = (
            media.is_file()
            and media.stat().st_size == source_identity["bytes"]
            and media.stat().st_mtime_ns == source_identity["mtime_ns"]
        )
        report["source_preserved"] = {
            "passed": source_preserved,
            **source_identity,
        }
        if not source_preserved:
            report["passed"] = False
            record("source_preservation_failed", path=str(media))
        report["duration_seconds"] = round(time.monotonic() - started, 3)
        args.report.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
        emit(f"Report: {args.report.resolve()}")

    if report["passed"]:
        emit("RESULT: PASS")
        return 0
    emit("RESULT: FAIL", error=True)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
