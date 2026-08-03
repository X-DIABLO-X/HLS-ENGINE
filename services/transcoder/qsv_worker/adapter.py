"""Isolated Intel QSV HLS encoding adapter.

The adapter has no database, Redis, RabbitMQ, object-storage, or Celery
dependencies.  It accepts one immutable local source and writes one new output
directory.  Existing output directories are never overwritten.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import signal
import socket
import subprocess
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from fractions import Fraction
from pathlib import Path, PurePosixPath
from typing import Callable, Sequence, TextIO


MANIFEST_SCHEMA_VERSION = 1
DEFAULT_SEGMENT_SECONDS = 6.0
_RENDITION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
_QSV_PRESETS = {
    "veryfast",
    "faster",
    "fast",
    "medium",
    "slow",
    "slower",
    "veryslow",
}
_H264_PROFILES = {"baseline", "main", "high"}


class QsvWorkerError(RuntimeError):
    """Base class for errors that are safe to expose as structured output."""

    error_code = "qsv_worker_error"
    exit_code = 4

    def as_dict(self) -> dict:
        return {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "status": "failed",
            "error": {
                "code": self.error_code,
                "message": str(self),
            },
        }


class JobConfigurationError(QsvWorkerError):
    error_code = "invalid_job"
    exit_code = 2


class QsvUnavailableError(QsvWorkerError):
    error_code = "qsv_unavailable"
    exit_code = 3


class EncodingFailedError(QsvWorkerError):
    error_code = "encode_failed"
    exit_code = 4


class CommandTimeoutError(QsvWorkerError):
    error_code = "command_timeout"
    exit_code = 4


@dataclass(frozen=True)
class ProcessResult:
    args: tuple[str, ...]
    returncode: int
    stdout: str = ""
    stderr: str = ""


Runner = Callable[[Sequence[str], float | None], ProcessResult]


@dataclass(frozen=True)
class SourceRange:
    start_seconds: float
    duration_seconds: float

    def validate(self, segment_seconds: float) -> None:
        if not math.isfinite(self.start_seconds) or self.start_seconds < 0:
            raise JobConfigurationError("start_seconds must be finite and >= 0")
        if not math.isfinite(self.duration_seconds) or self.duration_seconds <= 0:
            raise JobConfigurationError(
                "duration_seconds must be finite and greater than 0"
            )

        # Every independently encoded chunk must share the global six-second
        # HLS boundary grid.  The duration may be shorter for the final tail.
        boundary_index = self.start_seconds / segment_seconds
        if not math.isclose(
            boundary_index,
            round(boundary_index),
            rel_tol=0,
            abs_tol=1e-7,
        ):
            raise JobConfigurationError(
                "start_seconds must align to the configured segment duration"
            )


@dataclass(frozen=True)
class RenditionSettings:
    rendition_id: str
    width: int
    height: int
    bitrate_kbps: int
    maxrate_kbps: int
    bufsize_kbps: int
    preset: str = "veryfast"
    low_power: bool = True
    profile: str = "high"
    b_frames: int = 2
    segment_seconds: float = DEFAULT_SEGMENT_SECONDS
    fps: Fraction | None = None

    def validate(self) -> None:
        if not _RENDITION_ID_RE.fullmatch(self.rendition_id):
            raise JobConfigurationError(
                "rendition_id must use only letters, numbers, '.', '_' or '-'"
            )
        if self.width <= 0 or self.height <= 0:
            raise JobConfigurationError("width and height must be greater than 0")
        if self.width % 2 or self.height % 2:
            raise JobConfigurationError(
                "QSV NV12 output requires even width and height"
            )
        if self.bitrate_kbps <= 0:
            raise JobConfigurationError("bitrate_kbps must be greater than 0")
        if self.maxrate_kbps < self.bitrate_kbps:
            raise JobConfigurationError(
                "maxrate_kbps must be greater than or equal to bitrate_kbps"
            )
        if self.bufsize_kbps <= 0:
            raise JobConfigurationError("bufsize_kbps must be greater than 0")
        if self.preset not in _QSV_PRESETS:
            raise JobConfigurationError(
                f"unsupported QSV preset {self.preset!r}"
            )
        if self.profile not in _H264_PROFILES:
            raise JobConfigurationError(
                f"unsupported H.264 profile {self.profile!r}"
            )
        if self.b_frames < 0 or self.b_frames > 3:
            raise JobConfigurationError("b_frames must be between 0 and 3")
        if (
            not math.isfinite(self.segment_seconds)
            or self.segment_seconds <= 0
        ):
            raise JobConfigurationError(
                "segment_seconds must be finite and greater than 0"
            )
        if self.fps is not None and self.fps <= 0:
            raise JobConfigurationError("fps must be greater than 0")


@dataclass(frozen=True)
class EncodeJob:
    source: Path
    output_dir: Path
    source_range: SourceRange
    rendition: RenditionSettings

    def normalized(self) -> "EncodeJob":
        source = self.source.expanduser().resolve(strict=False)
        output_dir = self.output_dir.expanduser().resolve(strict=False)
        return EncodeJob(
            source=source,
            output_dir=output_dir,
            source_range=self.source_range,
            rendition=self.rendition,
        )

    def validate(self) -> None:
        self.rendition.validate()
        self.source_range.validate(self.rendition.segment_seconds)
        if not self.source.exists() or not self.source.is_file():
            raise JobConfigurationError(
                f"source is not a readable file: {self.source}"
            )
        if self.output_dir.exists():
            raise JobConfigurationError(
                f"output directory already exists; refusing to overwrite: "
                f"{self.output_dir}"
            )
        if self.output_dir.name in {"", ".", ".."}:
            raise JobConfigurationError("output directory must have a file name")


class SubprocessRunner:
    """Run a media process and terminate its process tree on interruption."""

    def __call__(
        self,
        args: Sequence[str],
        timeout: float | None = None,
    ) -> ProcessResult:
        command = [str(value) for value in args]
        popen_options: dict = {
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE,
            "text": True,
            "encoding": "utf-8",
            "errors": "replace",
        }
        if os.name == "nt":
            popen_options["creationflags"] = (
                subprocess.CREATE_NEW_PROCESS_GROUP
            )
        else:
            popen_options["start_new_session"] = True

        process = subprocess.Popen(command, **popen_options)
        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            _terminate_process_tree(process)
            stdout, stderr = process.communicate()
            raise CommandTimeoutError(
                f"command exceeded {timeout:.1f}s timeout: "
                f"{_command_label(command)}; stderr={_tail(stderr)}"
            ) from exc
        except BaseException:
            _terminate_process_tree(process)
            raise

        return ProcessResult(
            args=tuple(command),
            returncode=process.returncode,
            stdout=stdout,
            stderr=stderr,
        )


def _terminate_process_tree(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return

    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        return

    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def _command_label(command: Sequence[str]) -> str:
    return Path(command[0]).name if command else "process"


def _tail(value: str, limit: int = 2000) -> str:
    clean = (value or "").strip()
    return clean[-limit:]


def resolve_executable(value: str) -> str:
    """Resolve an executable without invoking a shell."""

    candidate = Path(value).expanduser()
    if candidate.parent != Path(".") or candidate.is_absolute():
        resolved = candidate.resolve(strict=False)
        if not resolved.is_file():
            raise QsvUnavailableError(f"executable not found: {resolved}")
        return str(resolved)

    resolved = shutil.which(value)
    if not resolved:
        raise QsvUnavailableError(f"executable not found on PATH: {value}")
    return resolved


def parse_fraction(value: str) -> Fraction:
    try:
        parsed = Fraction(value)
    except (ValueError, ZeroDivisionError) as exc:
        raise JobConfigurationError(f"invalid frame rate: {value!r}") from exc
    if parsed <= 0:
        raise JobConfigurationError("frame rate must be greater than 0")
    return parsed


def fit_dimensions_within_box(
    source_width: int,
    source_height: int,
    box_width: int,
    box_height: int,
) -> tuple[int, int]:
    """Fit square-pixel source dimensions inside an even-sized output box."""

    if source_width <= 0 or source_height <= 0:
        raise JobConfigurationError(
            "source video dimensions must be greater than 0"
        )
    if box_width <= 0 or box_height <= 0 or box_width % 2 or box_height % 2:
        raise JobConfigurationError(
            "rendition bounding-box dimensions must be positive and even"
        )

    scale = min(box_width / source_width, box_height / source_height)
    scaled_width = source_width * scale
    scaled_height = source_height * scale

    # QSV's NV12 path requires even dimensions.  Nearest-even rounding matches
    # the production 1920x804 -> 854x358 rendition while keeping both values
    # within the configured 854x480 bounding box.
    output_width = max(2, int(round(scaled_width / 2.0)) * 2)
    output_height = max(2, int(round(scaled_height / 2.0)) * 2)
    output_width = min(output_width, box_width)
    output_height = min(output_height, box_height)
    return output_width, output_height


class QsvAdapter:
    """Probe Intel QSV and encode one isolated HLS rendition."""

    def __init__(
        self,
        ffmpeg: str,
        ffprobe: str,
        runner: Runner | None = None,
    ) -> None:
        self.ffmpeg = ffmpeg
        self.ffprobe = ffprobe
        self.runner = runner or SubprocessRunner()

    def build_probe_command(self) -> list[str]:
        # Encoding one generated frame catches a compiled-but-unusable QSV
        # backend, including the common Docker/WSL case where /dev/dri is absent.
        # The generated frame enters QSV from system memory because D3D11 QSV
        # surface upload is driver-specific; the real job verifies hardware
        # decode and VPP against its source before anything is published.
        return [
            self.ffmpeg,
            "-hide_banner",
            "-nostdin",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=854x480:rate=24",
            "-frames:v",
            "1",
            "-vf",
            "format=nv12",
            "-an",
            "-c:v",
            "h264_qsv",
            "-preset",
            "veryfast",
            "-low_power",
            "1",
            "-f",
            "null",
            "-",
        ]

    def probe_qsv(self, timeout: float = 20.0) -> dict:
        result = self.runner(self.build_probe_command(), timeout)
        if result.returncode != 0:
            raise QsvUnavailableError(
                "Intel QSV device probe failed. Ensure Intel graphics is "
                f"enabled and its media driver is installed. stderr="
                f"{_tail(result.stderr)}"
            )
        return {
            "available": True,
            "backend": "intel_qsv",
            "encoder": "h264_qsv",
            "probe_elapsed_limit_seconds": timeout,
        }

    def probe_source(self, source: Path) -> dict:
        command = [
            self.ffprobe,
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            (
                "stream=codec_name,width,height,pix_fmt,avg_frame_rate,"
                "r_frame_rate:format=duration"
            ),
            "-of",
            "json",
            str(source),
        ]
        result = self.runner(command, 30.0)
        if result.returncode != 0:
            raise JobConfigurationError(
                f"ffprobe could not inspect source: {_tail(result.stderr)}"
            )
        payload = _load_json_result(result, "source ffprobe")
        streams = payload.get("streams") or []
        if not streams:
            raise JobConfigurationError("source has no video stream")
        stream = streams[0]
        frame_rate_value = (
            stream.get("avg_frame_rate")
            or stream.get("r_frame_rate")
            or ""
        )
        frame_rate = parse_fraction(frame_rate_value)
        return {
            "codec_name": stream.get("codec_name"),
            "width": int(stream.get("width") or 0),
            "height": int(stream.get("height") or 0),
            "pix_fmt": stream.get("pix_fmt"),
            "avg_frame_rate": str(frame_rate),
            "duration_seconds": _optional_float(
                (payload.get("format") or {}).get("duration")
            ),
        }

    def build_encode_command(
        self,
        job: EncodeJob,
        staging_dir: Path,
        fps: Fraction,
        output_dimensions: tuple[int, int],
    ) -> list[str]:
        rendition = job.rendition
        output_width, output_height = output_dimensions
        playlist = staging_dir / "index.m3u8"
        segment_pattern = staging_dir / "segment_%05d.ts"
        gop_frames = max(1, round(float(fps) * rendition.segment_seconds))
        segment_seconds = _number(rendition.segment_seconds)

        return [
            self.ffmpeg,
            "-hide_banner",
            "-nostdin",
            "-loglevel",
            "warning",
            "-y",
            "-init_hw_device",
            "qsv=hw",
            "-filter_hw_device",
            "hw",
            "-hwaccel",
            "qsv",
            "-hwaccel_output_format",
            "qsv",
            "-ss",
            _number(job.source_range.start_seconds),
            "-i",
            str(job.source),
            "-t",
            _number(job.source_range.duration_seconds),
            "-map",
            "0:v:0",
            "-vf",
            f"vpp_qsv=w={output_width}:h={output_height}",
            "-an",
            "-c:v",
            "h264_qsv",
            "-preset",
            rendition.preset,
            "-low_power",
            "1" if rendition.low_power else "0",
            "-profile:v",
            rendition.profile,
            "-b:v",
            f"{rendition.bitrate_kbps}k",
            "-maxrate",
            f"{rendition.maxrate_kbps}k",
            "-bufsize",
            f"{rendition.bufsize_kbps}k",
            "-g",
            str(gop_frames),
            "-keyint_min",
            str(gop_frames),
            "-bf",
            str(rendition.b_frames),
            "-adaptive_i",
            "0",
            "-adaptive_b",
            "0",
            "-forced_idr",
            "1",
            "-force_key_frames",
            f"expr:gte(t,n_forced*{segment_seconds})",
            "-fps_mode",
            "passthrough",
            "-hls_time",
            segment_seconds,
            "-hls_segment_type",
            "mpegts",
            "-hls_playlist_type",
            "vod",
            "-hls_flags",
            "independent_segments+temp_file",
            "-start_number",
            "0",
            "-hls_segment_filename",
            str(segment_pattern),
            "-progress",
            "pipe:1",
            "-nostats",
            str(playlist),
        ]

    def run(
        self,
        job: EncodeJob,
        timeout: float | None = None,
    ) -> dict:
        job = job.normalized()
        job.validate()
        source_identity_before = _source_identity(job.source)
        qsv_probe = self.probe_qsv()
        source_probe = self.probe_source(job.source)
        fps = job.rendition.fps or parse_fraction(
            source_probe["avg_frame_rate"]
        )
        output_dimensions = fit_dimensions_within_box(
            source_width=source_probe["width"],
            source_height=source_probe["height"],
            box_width=job.rendition.width,
            box_height=job.rendition.height,
        )

        output_parent = job.output_dir.parent
        output_parent.mkdir(parents=True, exist_ok=True)
        lock_path = output_parent / f".{job.output_dir.name}.qsv.lock"
        staging_dir = output_parent / (
            f".{job.output_dir.name}.{uuid.uuid4().hex}.partial"
        )

        lock_handle: TextIO | None = None
        lock_token: str | None = None
        stale_lock_recovered = False
        started_monotonic = time.monotonic()
        started_at = _utc_now()
        try:
            (
                lock_handle,
                lock_token,
                stale_lock_recovered,
            ) = _acquire_output_lock(
                lock_path=lock_path,
                output_dir=job.output_dir,
                created_at=started_at,
            )

            # Recheck after taking the cooperative lock.
            if job.output_dir.exists():
                raise JobConfigurationError(
                    "output directory appeared while acquiring lock; "
                    "refusing to overwrite"
                )
            staging_dir.mkdir(mode=0o700)
            command = self.build_encode_command(
                job,
                staging_dir,
                fps,
                output_dimensions,
            )
            effective_timeout = timeout
            if effective_timeout is None:
                effective_timeout = max(
                    300.0,
                    job.source_range.duration_seconds * 5.0,
                )

            result = self.runner(command, effective_timeout)
            (staging_dir / "ffmpeg.progress.log").write_text(
                result.stdout,
                encoding="utf-8",
            )
            (staging_dir / "ffmpeg.stderr.log").write_text(
                result.stderr,
                encoding="utf-8",
            )
            if result.returncode != 0:
                raise EncodingFailedError(
                    f"QSV FFmpeg exited with {result.returncode}: "
                    f"{_tail(result.stderr)}"
                )

            playlist_validation = self._validate_hls_output(
                staging_dir=staging_dir,
                expected_duration=job.source_range.duration_seconds,
                segment_seconds=job.rendition.segment_seconds,
                fps=fps,
            )
            output_probe = self._probe_output(staging_dir / "index.m3u8")
            if output_probe["codec_name"] != "h264":
                raise EncodingFailedError(
                    "output validation expected H.264 video, got "
                    f"{output_probe['codec_name']!r}"
                )
            if (
                output_probe["width"] != output_dimensions[0]
                or output_probe["height"] != output_dimensions[1]
            ):
                raise EncodingFailedError(
                    "output dimensions do not match requested rendition"
                )

            source_identity_after = _verify_source_identity_unchanged(
                job.source,
                source_identity_before,
            )
            elapsed = time.monotonic() - started_monotonic
            command_for_manifest = _portable_command(
                command,
                staging_dir,
            )
            artifacts = _artifact_inventory(staging_dir)
            manifest = {
                "schema_version": MANIFEST_SCHEMA_VERSION,
                "status": "complete",
                "backend": "intel_qsv",
                "started_at": started_at,
                "completed_at": _utc_now(),
                "elapsed_seconds": round(elapsed, 6),
                "source": {
                    "path": str(job.source),
                    "size_bytes": source_identity_after["size_bytes"],
                    "mtime_ns": source_identity_after["mtime_ns"],
                    "identity": source_identity_after,
                    "identity_verified_unchanged": True,
                    "probe": source_probe,
                },
                "range": {
                    "start_seconds": job.source_range.start_seconds,
                    "duration_seconds": job.source_range.duration_seconds,
                },
                "rendition": {
                    "id": job.rendition.rendition_id,
                    "width": output_dimensions[0],
                    "height": output_dimensions[1],
                    "bounding_box_width": job.rendition.width,
                    "bounding_box_height": job.rendition.height,
                    "bitrate_kbps": job.rendition.bitrate_kbps,
                    "maxrate_kbps": job.rendition.maxrate_kbps,
                    "bufsize_kbps": job.rendition.bufsize_kbps,
                    "preset": job.rendition.preset,
                    "low_power": job.rendition.low_power,
                    "profile": job.rendition.profile,
                    "b_frames": job.rendition.b_frames,
                    "fps": str(fps),
                    "segment_seconds": job.rendition.segment_seconds,
                    "gop_frames": max(
                        1,
                        round(
                            float(fps)
                            * job.rendition.segment_seconds
                        ),
                    ),
                },
                "hls": playlist_validation,
                "output_probe": output_probe,
                "qsv_probe": qsv_probe,
                "output_lock": {
                    "stale_lock_recovered": stale_lock_recovered,
                },
                "ffmpeg": {
                    "executable": self.ffmpeg,
                    "ffprobe_executable": self.ffprobe,
                    "command": command_for_manifest,
                    "final_progress": _parse_final_progress(result.stdout),
                },
                "artifacts": artifacts,
            }
            _write_json_atomic(staging_dir / "manifest.json", manifest)

            if job.output_dir.exists():
                raise JobConfigurationError(
                    "output directory appeared before publish; "
                    "refusing to overwrite"
                )
            # os.rename does not replace an existing directory on the intended
            # Windows-native worker.  The cooperative lock protects other
            # instances of this adapter on all supported platforms.
            os.rename(staging_dir, job.output_dir)
            return manifest
        finally:
            if lock_handle is not None:
                _release_output_lock(
                    lock_handle,
                    lock_path,
                    lock_token,
                )
            _remove_owned_staging(staging_dir, output_parent)

    def _probe_output(self, playlist: Path) -> dict:
        command = [
            self.ffprobe,
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=codec_name,width,height,avg_frame_rate:format=duration",
            "-of",
            "json",
            str(playlist),
        ]
        result = self.runner(command, 30.0)
        if result.returncode != 0:
            raise EncodingFailedError(
                f"ffprobe could not validate output: {_tail(result.stderr)}"
            )
        payload = _load_json_result(result, "output ffprobe")
        streams = payload.get("streams") or []
        if not streams:
            raise EncodingFailedError("encoded playlist has no video stream")
        stream = streams[0]
        return {
            "codec_name": stream.get("codec_name"),
            "width": int(stream.get("width") or 0),
            "height": int(stream.get("height") or 0),
            "avg_frame_rate": stream.get("avg_frame_rate"),
            "duration_seconds": _optional_float(
                (payload.get("format") or {}).get("duration")
            ),
        }

    def _probe_segment_packets(self, segment_path: Path) -> list[dict]:
        command = [
            self.ffprobe,
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "packet=pts_time,dts_time,duration_time,flags",
            "-show_packets",
            "-of",
            "json",
            str(segment_path),
        ]
        result = self.runner(command, 30.0)
        if result.returncode != 0:
            raise EncodingFailedError(
                "ffprobe could not inspect HLS segment packets: "
                f"{segment_path.name}: {_tail(result.stderr)}"
            )
        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise EncodingFailedError(
                "segment packet ffprobe returned invalid JSON: "
                f"{segment_path.name}"
            ) from exc
        packets = payload.get("packets") if isinstance(payload, dict) else None
        if not isinstance(packets, list) or not packets:
            raise EncodingFailedError(
                f"HLS segment has no video packets: {segment_path.name}"
            )
        return packets

    def _validate_segment_packets(
        self,
        staging_dir: Path,
        segment_uris: Sequence[str],
        declared_durations: Sequence[float],
        expected_duration: float,
        fps: Fraction,
    ) -> dict:
        frame_seconds = 1.0 / float(fps)
        tolerance = max(0.25, 3.0 * frame_seconds)
        segment_results: list[dict] = []
        previous_end_dts: float | None = None
        timeline_start_dts: float | None = None

        for segment_index, (uri, declared_duration) in enumerate(
            zip(segment_uris, declared_durations, strict=True)
        ):
            packets = self._probe_segment_packets(
                staging_dir / PurePosixPath(uri)
            )
            first_packet = packets[0]
            if "K" not in str(first_packet.get("flags") or ""):
                raise EncodingFailedError(
                    "HLS segment does not begin with an actual keyframe: "
                    f"index={segment_index} uri={uri}"
                )

            packet_times: list[tuple[float, float, float]] = []
            prior_dts: float | None = None
            for packet_index, packet in enumerate(packets):
                pts = _required_packet_time(
                    packet,
                    "pts_time",
                    uri,
                    packet_index,
                )
                dts = _required_packet_time(
                    packet,
                    "dts_time",
                    uri,
                    packet_index,
                )
                duration = _optional_float(packet.get("duration_time"))
                if duration is None or duration <= 0:
                    duration = frame_seconds
                if prior_dts is not None and dts + frame_seconds / 10 < prior_dts:
                    raise EncodingFailedError(
                        "video decode timestamps move backwards within HLS "
                        f"segment: uri={uri} packet={packet_index}"
                    )
                prior_dts = dts
                packet_times.append((pts, dts, duration))

            first_pts, first_dts, _ = packet_times[0]
            _, last_dts, last_duration = packet_times[-1]
            end_dts = last_dts + last_duration
            actual_duration = end_dts - first_dts
            if actual_duration <= 0:
                raise EncodingFailedError(
                    f"HLS segment has a non-positive packet timeline: {uri}"
                )
            if not math.isclose(
                actual_duration,
                declared_duration,
                rel_tol=0,
                abs_tol=tolerance,
            ):
                raise EncodingFailedError(
                    "actual segment packet duration disagrees with EXTINF: "
                    f"uri={uri} declared={declared_duration} "
                    f"actual={actual_duration}"
                )

            continuity_delta = None
            if previous_end_dts is not None:
                continuity_delta = first_dts - previous_end_dts
                if abs(continuity_delta) > tolerance:
                    raise EncodingFailedError(
                        "HLS video packet timeline is discontinuous between "
                        f"segments: uri={uri} delta={continuity_delta}"
                    )
            else:
                timeline_start_dts = first_dts
            previous_end_dts = end_dts
            segment_results.append(
                {
                    "uri": uri,
                    "packet_count": len(packets),
                    "first_packet_keyframe": True,
                    "first_pts_seconds": first_pts,
                    "first_dts_seconds": first_dts,
                    "end_dts_seconds": end_dts,
                    "actual_duration_seconds": actual_duration,
                    "continuity_delta_seconds": continuity_delta,
                }
            )

        assert timeline_start_dts is not None
        assert previous_end_dts is not None
        timeline_duration = previous_end_dts - timeline_start_dts
        if not math.isclose(
            timeline_duration,
            expected_duration,
            rel_tol=0,
            abs_tol=tolerance,
        ):
            raise EncodingFailedError(
                "actual HLS packet timeline does not match requested range: "
                f"expected={expected_duration} actual={timeline_duration}"
            )
        return {
            "validated": True,
            "timeline_duration_seconds": timeline_duration,
            "continuity_tolerance_seconds": tolerance,
            "segments": segment_results,
        }

    def _validate_hls_output(
        self,
        staging_dir: Path,
        expected_duration: float,
        segment_seconds: float,
        fps: Fraction,
    ) -> dict:
        playlist = staging_dir / "index.m3u8"
        if not playlist.is_file() or playlist.stat().st_size == 0:
            raise EncodingFailedError("FFmpeg did not produce index.m3u8")
        lines = [
            line.strip()
            for line in playlist.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        if not lines or lines[0] != "#EXTM3U":
            raise EncodingFailedError("output playlist is not valid HLS")
        if "#EXT-X-ENDLIST" not in lines:
            raise EncodingFailedError("output playlist is incomplete")
        if "#EXT-X-INDEPENDENT-SEGMENTS" not in lines:
            raise EncodingFailedError(
                "output playlist does not declare independent segments"
            )
        if "#EXT-X-DISCONTINUITY" in lines:
            raise EncodingFailedError(
                "output playlist unexpectedly declares a discontinuity"
            )

        durations: list[float] = []
        segment_uris: list[str] = []
        pending_duration: float | None = None
        for line in lines[1:]:
            if line.startswith("#EXTINF:"):
                try:
                    pending_duration = float(
                        line.split(":", 1)[1].split(",", 1)[0]
                    )
                except ValueError as exc:
                    raise EncodingFailedError(
                        f"invalid EXTINF line: {line}"
                    ) from exc
                continue
            if line.startswith("#"):
                continue
            if pending_duration is None:
                raise EncodingFailedError(
                    f"segment URI has no EXTINF duration: {line}"
                )
            _validate_relative_artifact_uri(line)
            segment_path = staging_dir / PurePosixPath(line)
            if not segment_path.is_file() or segment_path.stat().st_size == 0:
                raise EncodingFailedError(
                    f"playlist references missing or empty segment: {line}"
                )
            segment_uris.append(line)
            durations.append(pending_duration)
            pending_duration = None

        if not segment_uris:
            raise EncodingFailedError("output playlist contains no segments")
        if pending_duration is not None:
            raise EncodingFailedError("playlist ends with an unused EXTINF")

        tolerance = max(0.25, 2.0 / float(fps))
        for index, duration in enumerate(durations):
            if duration <= 0:
                raise EncodingFailedError("segment duration must be positive")
            is_final = index == len(durations) - 1
            if is_final:
                if duration > segment_seconds + tolerance:
                    raise EncodingFailedError(
                        "final segment exceeds the six-second GOP boundary"
                    )
            elif not math.isclose(
                duration,
                segment_seconds,
                rel_tol=0,
                abs_tol=tolerance,
            ):
                raise EncodingFailedError(
                    "non-final segment is not aligned to the six-second "
                    f"boundary: index={index} duration={duration}"
                )

        playlist_duration = sum(durations)
        if not math.isclose(
            playlist_duration,
            expected_duration,
            rel_tol=0,
            abs_tol=tolerance,
        ):
            raise EncodingFailedError(
                "playlist duration does not match requested range: "
                f"expected={expected_duration} actual={playlist_duration}"
            )
        packet_validation = self._validate_segment_packets(
            staging_dir=staging_dir,
            segment_uris=segment_uris,
            declared_durations=durations,
            expected_duration=expected_duration,
            fps=fps,
        )
        return {
            "playlist": "index.m3u8",
            "segment_type": "mpegts",
            "segment_count": len(segment_uris),
            "segment_uris": segment_uris,
            "segment_durations": durations,
            "duration_seconds": playlist_duration,
            "independent_segments": True,
            "packet_validation": packet_validation,
        }


def _load_json_result(result: ProcessResult, label: str) -> dict:
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise JobConfigurationError(
            f"{label} returned invalid JSON: {_tail(result.stdout)}"
        ) from exc
    if not isinstance(payload, dict):
        raise JobConfigurationError(f"{label} JSON must be an object")
    return payload


def _optional_float(value) -> float | None:
    if value in (None, "", "N/A"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _required_packet_time(
    packet: dict,
    field: str,
    uri: str,
    packet_index: int,
) -> float:
    value = _optional_float(packet.get(field))
    if value is None or not math.isfinite(value):
        raise EncodingFailedError(
            "HLS video packet has no finite timestamp: "
            f"uri={uri} packet={packet_index} field={field}"
        )
    return value


def _source_identity(source: Path) -> dict:
    try:
        stat = source.stat()
    except OSError as exc:
        raise EncodingFailedError(
            f"source became unavailable while processing: {source}"
        ) from exc
    return {
        "device": stat.st_dev,
        "file_id": stat.st_ino,
        "size_bytes": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "ctime_ns": stat.st_ctime_ns,
    }


def _verify_source_identity_unchanged(
    source: Path,
    identity_before: dict,
) -> dict:
    identity_after = _source_identity(source)
    if identity_after != identity_before:
        changed_fields = sorted(
            key
            for key in identity_before
            if identity_before.get(key) != identity_after.get(key)
        )
        raise EncodingFailedError(
            "source identity changed during QSV processing; refusing to "
            f"publish output: fields={','.join(changed_fields)}"
        )
    return identity_after


def _process_start_marker(pid: int) -> str | None:
    if pid <= 0:
        return None
    if os.name == "nt":
        try:
            import ctypes
            from ctypes import wintypes

            process_query_limited_information = 0x1000
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.OpenProcess.argtypes = (
                wintypes.DWORD,
                wintypes.BOOL,
                wintypes.DWORD,
            )
            kernel32.OpenProcess.restype = wintypes.HANDLE
            kernel32.GetProcessTimes.argtypes = (
                wintypes.HANDLE,
                ctypes.POINTER(wintypes.FILETIME),
                ctypes.POINTER(wintypes.FILETIME),
                ctypes.POINTER(wintypes.FILETIME),
                ctypes.POINTER(wintypes.FILETIME),
            )
            kernel32.GetProcessTimes.restype = wintypes.BOOL
            kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
            kernel32.CloseHandle.restype = wintypes.BOOL
            handle = kernel32.OpenProcess(
                process_query_limited_information,
                False,
                pid,
            )
            if not handle:
                return None
            try:
                creation = wintypes.FILETIME()
                exit_time = wintypes.FILETIME()
                kernel = wintypes.FILETIME()
                user = wintypes.FILETIME()
                if not kernel32.GetProcessTimes(
                    handle,
                    ctypes.byref(creation),
                    ctypes.byref(exit_time),
                    ctypes.byref(kernel),
                    ctypes.byref(user),
                ):
                    return None
                value = (creation.dwHighDateTime << 32) | creation.dwLowDateTime
                return f"windows-filetime:{value}"
            finally:
                kernel32.CloseHandle(handle)
        except (AttributeError, OSError):
            return None

    proc_stat = Path(f"/proc/{pid}/stat")
    try:
        # Field 22 is the process start time.  Split after the command name,
        # which can itself contain spaces and parentheses.
        remainder = proc_stat.read_text(encoding="utf-8").rsplit(")", 1)[1]
        fields = remainder.split()
        return f"proc-start-ticks:{fields[19]}"
    except (IndexError, OSError):
        return None


def _pid_is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    if pid == os.getpid():
        return True
    try:
        os.kill(pid, 0)
    except PermissionError:
        return True
    except OSError as exc:
        # Access denied means the process exists but cannot be queried.
        return getattr(exc, "winerror", None) == 5
    return True


def _lock_owner_may_be_live(payload: dict | None, output_dir: Path) -> bool:
    if not isinstance(payload, dict):
        return True
    try:
        pid = int(payload["pid"])
    except (KeyError, TypeError, ValueError):
        return True
    recorded_output = payload.get("output_dir")
    if recorded_output and Path(recorded_output).resolve(strict=False) != output_dir:
        return True
    recorded_host = payload.get("host")
    if recorded_host and str(recorded_host).casefold() != socket.gethostname().casefold():
        # A shared filesystem lock from another host cannot be proven stale.
        return True
    if not _pid_is_running(pid):
        return False

    recorded_marker = payload.get("process_start_marker")
    actual_marker = _process_start_marker(pid)
    if recorded_marker and actual_marker:
        # A mismatch proves that the PID was recycled after the lock owner died.
        return str(recorded_marker) == actual_marker
    return True


def _try_lock_file(handle: TextIO) -> bool:
    handle.flush()
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            return False
        return True

    import fcntl

    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        return False
    return True


def _unlock_file(handle: TextIO) -> None:
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            pass
        return

    import fcntl

    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    except OSError:
        pass


def _read_lock_payload(handle: TextIO) -> dict | None:
    try:
        handle.seek(0)
        payload = json.load(handle)
    except (json.JSONDecodeError, OSError, UnicodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _write_lock_payload(handle: TextIO, payload: dict) -> None:
    handle.seek(0)
    handle.truncate()
    json.dump(payload, handle, sort_keys=True)
    handle.write("\n")
    handle.flush()
    os.fsync(handle.fileno())


def _acquire_output_lock(
    lock_path: Path,
    output_dir: Path,
    created_at: str,
) -> tuple[TextIO, str, bool]:
    token = uuid.uuid4().hex
    payload = {
        "schema_version": 2,
        "token": token,
        "pid": os.getpid(),
        "process_start_marker": _process_start_marker(os.getpid()),
        "host": socket.gethostname(),
        "created_at": created_at,
        "output_dir": str(output_dir),
    }

    for _ in range(3):
        created = False
        try:
            handle = lock_path.open("x+", encoding="utf-8", newline="\n")
            created = True
            _write_lock_payload(handle, payload)
        except FileExistsError:
            try:
                handle = lock_path.open("r+", encoding="utf-8", newline="\n")
            except FileNotFoundError:
                continue

        locked = _try_lock_file(handle)
        if not locked:
            handle.close()
            raise JobConfigurationError(
                f"another QSV attempt holds output lock: {lock_path}"
            )

        if created:
            return handle, token, False

        previous_payload = _read_lock_payload(handle)
        if _lock_owner_may_be_live(previous_payload, output_dir):
            _unlock_file(handle)
            handle.close()
            raise JobConfigurationError(
                f"another QSV attempt holds output lock: {lock_path}"
            )

        # The old process is provably gone (or its PID was recycled).  Reuse
        # the already locked inode, avoiding an unlink/create window in which
        # a concurrent worker could have its new live lock stolen.
        _write_lock_payload(handle, payload)
        return handle, token, True

    raise JobConfigurationError(
        f"could not acquire QSV output lock after a concurrent change: {lock_path}"
    )


def _release_output_lock(
    handle: TextIO,
    lock_path: Path,
    token: str | None,
) -> None:
    _unlock_file(handle)
    handle.close()
    if token is None:
        return
    try:
        payload = json.loads(lock_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError, UnicodeError):
        return
    if isinstance(payload, dict) and payload.get("token") == token:
        lock_path.unlink(missing_ok=True)


def _number(value: float) -> str:
    return format(value, ".12g")


def _validate_relative_artifact_uri(uri: str) -> None:
    if "\\" in uri or "?" in uri or "#" in uri:
        raise EncodingFailedError(f"unsafe local segment URI: {uri}")
    path = PurePosixPath(uri)
    if path.is_absolute() or ".." in path.parts or not path.name:
        raise EncodingFailedError(f"unsafe local segment URI: {uri}")


def _artifact_inventory(staging_dir: Path) -> list[dict]:
    artifacts = []
    for path in sorted(staging_dir.iterdir(), key=lambda item: item.name):
        if not path.is_file() or path.name == "manifest.json":
            continue
        artifacts.append(
            {
                "path": path.name,
                "size_bytes": path.stat().st_size,
                "sha256": _sha256(path),
            }
        )
    return artifacts


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _portable_command(
    command: Sequence[str],
    staging_dir: Path,
) -> list[str]:
    staging_value = str(staging_dir)
    return [
        str(value).replace(staging_value, "${OUTPUT_DIR}")
        for value in command
    ]


def _parse_final_progress(stdout: str) -> dict:
    progress: dict[str, str] = {}
    current: dict[str, str] = {}
    for raw_line in (stdout or "").splitlines():
        if "=" not in raw_line:
            continue
        key, value = raw_line.split("=", 1)
        current[key.strip()] = value.strip()
        if key.strip() == "progress":
            progress = dict(current)
            current = {}
    return progress or current


def _write_json_atomic(path: Path, payload: dict) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    with temporary.open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _remove_owned_staging(staging_dir: Path, expected_parent: Path) -> None:
    if not staging_dir.exists():
        return
    if staging_dir.parent != expected_parent:
        raise RuntimeError("refusing to clean staging outside expected parent")
    if not staging_dir.name.endswith(".partial"):
        raise RuntimeError("refusing to clean a non-partial directory")
    shutil.rmtree(staging_dir)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
