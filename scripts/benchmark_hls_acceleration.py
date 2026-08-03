#!/usr/bin/env python3
"""Reproducible, self-cleaning H.264/HLS acceleration benchmark.

The harness is dry-run-only unless ``--execute`` is supplied.  It never writes
inside or deletes the source path.  Every media artifact is placed below a
unique run directory and removed in ``finally`` blocks after measurement.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import random
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from fractions import Fraction
from pathlib import Path, PurePosixPath
from typing import Any, Protocol, Sequence


REPORT_SCHEMA_VERSION = 2
SEGMENT_SECONDS = 6.0
DEFAULT_CLIP_SECONDS = 120.0
DEFAULT_CLIP_COUNT = 2
DEFAULT_TEMPORAL_CHUNKS = 4
DEFAULT_NVENC_MAX_SESSIONS = 2
DEFAULT_CANDIDATE_ORDER_SEED = 20260803
DEFAULT_SEGMENT_FORMAT = "fmp4"
_VMAF_RE = re.compile(r"VMAF score:\s*([0-9.]+)", re.IGNORECASE)
_SSIM_RE = re.compile(r"All:([0-9.]+)", re.IGNORECASE)
_PSNR_RE = re.compile(r"average:([0-9.]+|inf)", re.IGNORECASE)
_MAP_URI_RE = re.compile(r'URI="([^"]+)"')


class HarnessError(RuntimeError):
    pass


@dataclass(frozen=True)
class SourceInfo:
    path: Path
    size_bytes: int
    mtime_ns: int
    duration_seconds: float
    codec_name: str
    width: int
    height: int
    fps: Fraction


@dataclass(frozen=True)
class Clip:
    index: int
    start_seconds: float
    duration_seconds: float


@dataclass(frozen=True)
class Rendition:
    rendition_id: str
    width: int
    height: int
    bitrate_kbps: int


@dataclass(frozen=True)
class NvencProfile:
    name: str
    preset: str
    multipass: str
    lookahead: int
    spatial_aq: bool
    temporal_aq: bool
    b_frames: int


NVENC_PROFILES = {
    "quality": NvencProfile(
        name="quality",
        preset="p6",
        multipass="fullres",
        lookahead=32,
        spatial_aq=True,
        temporal_aq=True,
        b_frames=2,
    ),
    "balanced": NvencProfile(
        name="balanced",
        preset="p4",
        multipass="qres",
        lookahead=12,
        spatial_aq=False,
        temporal_aq=True,
        b_frames=2,
    ),
    "turbo": NvencProfile(
        name="turbo",
        preset="p3",
        multipass="disabled",
        lookahead=0,
        spatial_aq=False,
        temporal_aq=True,
        b_frames=2,
    ),
}


@dataclass(frozen=True)
class CommandSpec:
    label: str
    args: tuple[str, ...]


@dataclass(frozen=True)
class OutputTarget:
    playlist: Path
    rendition_id: str
    width: int
    height: int
    source_start_seconds: float
    expected_duration_seconds: float
    segment_format: str = DEFAULT_SEGMENT_FORMAT
    strict_six_second_grid: bool = True
    direct_copy: bool = False
    qsv_worker_output: bool = False
    box_width: int | None = None
    box_height: int | None = None
    source_width: int | None = None
    source_height: int | None = None


@dataclass(frozen=True)
class BatchPlan:
    label: str
    commands: tuple[CommandSpec, ...]
    outputs: tuple[OutputTarget, ...]
    parallel: bool
    validate_outputs: bool = True


@dataclass(frozen=True)
class StitchPlan:
    label: str
    chunks: tuple[OutputTarget, ...]
    output: OutputTarget


@dataclass(frozen=True)
class ClipPlan:
    clip: Clip
    batches: tuple[BatchPlan, ...]
    stitches: tuple[StitchPlan, ...] = ()


@dataclass(frozen=True)
class CandidatePlan:
    candidate_id: str
    category: str
    settings: dict[str, Any]
    clips: tuple[ClipPlan, ...]


@dataclass(frozen=True)
class ProcessResult:
    label: str
    args: tuple[str, ...]
    returncode: int
    wall_seconds: float
    stdout: str
    stderr: str
    timed_out: bool = False


@dataclass(frozen=True)
class BatchResult:
    wall_seconds: float
    processes: tuple[ProcessResult, ...]


class Executor(Protocol):
    def run_batch(
        self,
        commands: Sequence[CommandSpec],
        *,
        parallel: bool,
        timeout_seconds: float,
    ) -> BatchResult:
        ...


class SubprocessExecutor:
    """Launch a command batch without a shell and bound every process tree."""

    def run_batch(
        self,
        commands: Sequence[CommandSpec],
        *,
        parallel: bool,
        timeout_seconds: float,
    ) -> BatchResult:
        if not commands:
            return BatchResult(0.0, ())
        if not parallel and len(commands) > 1:
            results: list[ProcessResult] = []
            started = time.monotonic()
            for command in commands:
                result = self.run_batch(
                    [command],
                    parallel=True,
                    timeout_seconds=timeout_seconds,
                )
                results.extend(result.processes)
            return BatchResult(time.monotonic() - started, tuple(results))

        batch_started = time.monotonic()
        launched: list[dict[str, Any]] = []
        try:
            for spec in commands:
                options: dict[str, Any] = {
                    "stdout": subprocess.PIPE,
                    "stderr": subprocess.PIPE,
                    "text": True,
                    "encoding": "utf-8",
                    "errors": "replace",
                }
                if os.name == "nt":
                    options["creationflags"] = (
                        subprocess.CREATE_NEW_PROCESS_GROUP
                    )
                else:
                    options["start_new_session"] = True
                process = subprocess.Popen(list(spec.args), **options)
                launched.append(
                    {
                        "spec": spec,
                        "process": process,
                        "started": time.monotonic(),
                        "completed": None,
                        "stdout": "",
                        "stderr": "",
                    }
                )

            threads = []

            def communicate(item: dict[str, Any]) -> None:
                stdout, stderr = item["process"].communicate()
                item["stdout"] = stdout
                item["stderr"] = stderr
                item["completed"] = time.monotonic()

            for item in launched:
                thread = threading.Thread(
                    target=communicate,
                    args=(item,),
                    daemon=True,
                )
                thread.start()
                threads.append(thread)

            deadline = batch_started + timeout_seconds
            timed_out = False
            while any(thread.is_alive() for thread in threads):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    timed_out = True
                    for item in launched:
                        _terminate_process_tree(item["process"])
                    break
                for thread in threads:
                    thread.join(timeout=min(0.05, remaining))
            for thread in threads:
                thread.join(timeout=5)

            completed_at = time.monotonic()
            results = []
            for item in launched:
                process = item["process"]
                ended = item["completed"] or completed_at
                results.append(
                    ProcessResult(
                        label=item["spec"].label,
                        args=item["spec"].args,
                        returncode=(
                            process.returncode
                            if process.returncode is not None
                            else -9
                        ),
                        wall_seconds=ended - item["started"],
                        stdout=item["stdout"],
                        stderr=item["stderr"],
                        timed_out=timed_out and item["completed"] is None,
                    )
                )
            return BatchResult(completed_at - batch_started, tuple(results))
        except BaseException:
            for item in launched:
                _terminate_process_tree(item["process"])
            raise


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


def resolve_executable(value: str) -> str:
    candidate = Path(value).expanduser()
    if candidate.is_absolute() or candidate.parent != Path("."):
        resolved = candidate.resolve(strict=False)
        if not resolved.is_file():
            raise HarnessError(f"executable not found: {resolved}")
        return str(resolved)
    resolved = shutil.which(value)
    if not resolved:
        raise HarnessError(f"executable not found on PATH: {value}")
    return resolved


def _run_one(
    executor: Executor,
    label: str,
    args: Sequence[str],
    timeout_seconds: float,
) -> ProcessResult:
    batch = executor.run_batch(
        [CommandSpec(label, tuple(str(value) for value in args))],
        parallel=False,
        timeout_seconds=timeout_seconds,
    )
    if len(batch.processes) != 1:
        raise HarnessError(f"{label} returned an invalid process result")
    return batch.processes[0]


def inspect_ffmpeg(
    ffmpeg: str,
    executor: Executor,
) -> dict[str, Any]:
    version = _run_one(
        executor,
        "ffmpeg-version",
        [ffmpeg, "-version"],
        30.0,
    )
    if version.returncode != 0:
        raise HarnessError(f"ffmpeg -version failed: {_tail(version.stderr)}")
    first_line = (version.stdout or version.stderr).splitlines()
    filters = _run_one(
        executor,
        "ffmpeg-filters",
        [ffmpeg, "-hide_banner", "-filters"],
        30.0,
    )
    filter_text = f"{filters.stdout}\n{filters.stderr}"
    libvmaf_available = (
        filters.returncode == 0
        and re.search(r"\blibvmaf\b", filter_text) is not None
    )
    return {
        "executable": ffmpeg,
        "version": first_line[0] if first_line else "unknown",
        "libvmaf_available": libvmaf_available,
        "quality_metric_path": (
            "libvmaf" if libvmaf_available else "ssim_psnr"
        ),
    }


def probe_source(
    source: Path,
    ffprobe: str,
    executor: Executor,
) -> SourceInfo:
    result = _run_one(
        executor,
        "probe-source",
        [
            ffprobe,
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            (
                "stream=codec_name,width,height,avg_frame_rate,"
                "r_frame_rate:format=duration"
            ),
            "-of",
            "json",
            str(source),
        ],
        60.0,
    )
    if result.returncode != 0:
        raise HarnessError(f"source probe failed: {_tail(result.stderr)}")
    try:
        payload = json.loads(result.stdout)
        stream = payload["streams"][0]
        duration = float(payload["format"]["duration"])
        fps_value = (
            stream.get("avg_frame_rate")
            or stream.get("r_frame_rate")
        )
        fps = Fraction(fps_value)
    except (KeyError, IndexError, TypeError, ValueError, ZeroDivisionError) as exc:
        raise HarnessError("source probe returned incomplete metadata") from exc
    if duration <= 0 or fps <= 0:
        raise HarnessError("source duration and frame rate must be positive")
    stat = source.stat()
    return SourceInfo(
        path=source,
        size_bytes=stat.st_size,
        mtime_ns=stat.st_mtime_ns,
        duration_seconds=duration,
        codec_name=str(stream.get("codec_name") or ""),
        width=int(stream.get("width") or 0),
        height=int(stream.get("height") or 0),
        fps=fps,
    )


def select_aligned_clips(
    source_duration: float,
    *,
    clip_seconds: float,
    clip_count: int,
    segment_seconds: float = SEGMENT_SECONDS,
) -> tuple[Clip, ...]:
    if clip_count < 1:
        raise HarnessError("clip_count must be at least 1")
    if clip_seconds < segment_seconds:
        raise HarnessError("clip_seconds must be at least one segment")
    units = clip_seconds / segment_seconds
    if not math.isclose(units, round(units), abs_tol=1e-8):
        raise HarnessError("clip_seconds must be a multiple of six seconds")
    if source_duration < clip_seconds + segment_seconds:
        raise HarnessError("source is too short for the requested clip length")

    latest_start = source_duration - clip_seconds
    starts: list[float] = []
    for index in range(clip_count):
        fraction = (index + 1) / (clip_count + 1)
        start = math.floor(
            (latest_start * fraction) / segment_seconds
        ) * segment_seconds
        start = min(start, math.floor(latest_start / segment_seconds) * segment_seconds)
        if start not in starts:
            starts.append(start)
    if len(starts) != clip_count:
        raise HarnessError(
            "source is too short to choose distinct representative clips"
        )
    return tuple(
        Clip(index=index, start_seconds=start, duration_seconds=clip_seconds)
        for index, start in enumerate(starts)
    )


def renditions_for_source(source: SourceInfo) -> tuple[Rendition, Rendition]:
    if source.height < 720:
        raise HarnessError("benchmark source must be at least 720 pixels high")
    # These are the production ladder bounding boxes.  The scaler preserves
    # display aspect ratio inside each box instead of stretching to its edges.
    return (
        Rendition("720p", 1280, 720, 3000),
        Rendition("480p", 854, 480, 1500),
    )


def fit_even_dimensions_within_box(
    source_width: int,
    source_height: int,
    box_width: int,
    box_height: int,
) -> tuple[int, int]:
    """Return the nearest-even, aspect-preserved size inside a rendition box."""

    if source_width <= 0 or source_height <= 0:
        raise HarnessError("source dimensions must be positive")
    if box_width <= 0 or box_height <= 0 or box_width % 2 or box_height % 2:
        raise HarnessError("rendition boxes must use positive even dimensions")
    scale = min(box_width / source_width, box_height / source_height)
    width = max(2, int(round((source_width * scale) / 2.0)) * 2)
    height = max(2, int(round((source_height * scale) / 2.0)) * 2)
    return min(width, box_width), min(height, box_height)


def _nvenc_output_target(
    *,
    playlist: Path,
    rendition: Rendition,
    source: SourceInfo,
    clip: Clip,
    segment_format: str,
) -> OutputTarget:
    width, height = fit_even_dimensions_within_box(
        source.width,
        source.height,
        rendition.width,
        rendition.height,
    )
    return OutputTarget(
        playlist=playlist,
        rendition_id=rendition.rendition_id,
        width=width,
        height=height,
        source_start_seconds=clip.start_seconds,
        expected_duration_seconds=clip.duration_seconds,
        segment_format=segment_format,
        box_width=rendition.width,
        box_height=rendition.height,
        source_width=source.width,
        source_height=source.height,
    )


def _ffmpeg_nvenc_input(
    ffmpeg: str,
    source: Path,
    clip: Clip,
) -> list[str]:
    return [
        ffmpeg,
        "-hide_banner",
        "-nostdin",
        "-loglevel",
        "warning",
        "-y",
        "-progress",
        "pipe:1",
        "-nostats",
        "-init_hw_device",
        "cuda=gpu:0",
        "-filter_hw_device",
        "gpu",
        "-hwaccel",
        "cuda",
        "-hwaccel_device",
        "0",
        "-hwaccel_output_format",
        "cuda",
        "-ss",
        _number(clip.start_seconds),
        "-t",
        _number(clip.duration_seconds),
        "-i",
        str(source),
    ]


def _nvenc_args(
    profile: NvencProfile,
    rendition: Rendition,
    fps: Fraction,
) -> list[str]:
    gop = max(1, round(float(fps) * SEGMENT_SECONDS))
    bitrate = rendition.bitrate_kbps * 1000
    arguments = [
        "-c:v",
        "h264_nvenc",
        "-preset",
        profile.preset,
        "-tune",
        "hq",
        "-profile:v",
        "high",
        "-rc",
        "vbr",
        "-b:v",
        str(bitrate),
        "-maxrate",
        str(int(bitrate * 1.5)),
        "-bufsize",
        str(bitrate * 2),
        "-g",
        str(gop),
        "-keyint_min",
        str(gop),
        "-sc_threshold",
        "0",
        "-flags",
        "+cgop",
        "-force_key_frames",
        "expr:gte(t,n_forced*6)",
        "-cq",
        "23",
        "-multipass",
        profile.multipass,
    ]
    if profile.spatial_aq:
        arguments += ["-spatial-aq", "1"]
    if profile.temporal_aq:
        arguments += ["-temporal-aq", "1"]
    if profile.b_frames:
        arguments += ["-bf", str(profile.b_frames)]
        if profile.name == "quality":
            arguments += ["-2pass", "1"]
    arguments += ["-rc-lookahead", str(profile.lookahead)]
    if profile.lookahead:
        arguments += ["-no-scenecut", "1"]
    return arguments


def _hls_args(
    playlist: Path,
    segment_format: str = DEFAULT_SEGMENT_FORMAT,
) -> list[str]:
    if segment_format not in {"fmp4", "mpegts"}:
        raise HarnessError(f"unsupported HLS segment format: {segment_format}")
    extension = "m4s" if segment_format == "fmp4" else "ts"
    segment_pattern = playlist.parent / f"%05d.{extension}"
    arguments = [
        "-an",
        "-sn",
    ]
    if segment_format == "fmp4":
        arguments += [
            "-hls_segment_type",
            "fmp4",
            "-hls_fmp4_init_filename",
            "init.mp4",
        ]
    arguments += [
        "-hls_time",
        "6",
        "-hls_playlist_type",
        "vod",
        "-hls_flags",
        "independent_segments",
        "-hls_segment_filename",
        segment_pattern.as_posix(),
        "-f",
        "hls",
        playlist.as_posix(),
    ]
    return arguments


def _grouped_nvenc_batch(
    ffmpeg: str,
    source: SourceInfo,
    clip: Clip,
    renditions: tuple[Rendition, Rendition],
    profile: NvencProfile,
    output_root: Path,
    label: str,
    segment_format: str = DEFAULT_SEGMENT_FORMAT,
) -> BatchPlan:
    command = _ffmpeg_nvenc_input(ffmpeg, source.path, clip)
    split_labels = "".join(f"[in{index}]" for index in range(len(renditions)))
    graph = f"[0:v]setpts=PTS-STARTPTS,split=2{split_labels}"
    output_targets = []
    for index, rendition in enumerate(renditions):
        graph += (
            f";[in{index}]scale_cuda={rendition.width}:"
            f"{rendition.height}:force_original_aspect_ratio=decrease"
            f"[v{index}]"
        )
    command += ["-filter_complex", graph]
    for index, rendition in enumerate(renditions):
        playlist = output_root / rendition.rendition_id / "video.m3u8"
        command += ["-map", f"[v{index}]"]
        command += _nvenc_args(profile, rendition, source.fps)
        command += _hls_args(playlist, segment_format)
        output_targets.append(
            _nvenc_output_target(
                playlist=playlist,
                rendition=rendition,
                source=source,
                clip=clip,
                segment_format=segment_format,
            )
        )
    return BatchPlan(
        label=label,
        commands=(CommandSpec(label, tuple(command)),),
        outputs=tuple(output_targets),
        parallel=False,
    )


def _single_nvenc_command(
    ffmpeg: str,
    source: SourceInfo,
    clip: Clip,
    rendition: Rendition,
    profile: NvencProfile,
    output_root: Path,
    label: str,
    segment_format: str = DEFAULT_SEGMENT_FORMAT,
) -> tuple[CommandSpec, OutputTarget]:
    playlist = output_root / rendition.rendition_id / "video.m3u8"
    command = _ffmpeg_nvenc_input(ffmpeg, source.path, clip)
    command += [
        "-vf",
        (
            "setpts=PTS-STARTPTS,"
            f"scale_cuda={rendition.width}:{rendition.height}:"
            "force_original_aspect_ratio=decrease"
        ),
    ]
    command += _nvenc_args(profile, rendition, source.fps)
    command += _hls_args(playlist, segment_format)
    return (
        CommandSpec(label, tuple(command)),
        _nvenc_output_target(
            playlist=playlist,
            rendition=rendition,
            source=source,
            clip=clip,
            segment_format=segment_format,
        ),
    )


def _qsv_batch(
    python_executable: str,
    ffmpeg: str,
    ffprobe: str,
    source: SourceInfo,
    clip: Clip,
    rendition: Rendition,
    output_root: Path,
) -> BatchPlan:
    qsv_output = output_root / rendition.rendition_id
    output_width, output_height = fit_even_dimensions_within_box(
        source.width,
        source.height,
        rendition.width,
        rendition.height,
    )
    command = [
        python_executable,
        "-m",
        "services.transcoder.qsv_worker",
        "--ffmpeg",
        ffmpeg,
        "--ffprobe",
        ffprobe,
        "encode",
        "--source",
        str(source.path),
        "--output-dir",
        str(qsv_output),
        "--start-seconds",
        _number(clip.start_seconds),
        "--duration-seconds",
        _number(clip.duration_seconds),
        "--rendition-id",
        rendition.rendition_id,
        "--width",
        str(rendition.width),
        "--height",
        str(rendition.height),
        "--bitrate-kbps",
        str(rendition.bitrate_kbps),
        "--maxrate-kbps",
        str(math.ceil(rendition.bitrate_kbps * 1.07)),
        "--bufsize-kbps",
        str(rendition.bitrate_kbps * 2),
        "--preset",
        "veryfast",
        "--low-power",
    ]
    return BatchPlan(
        label=f"qsv-clip-{clip.index}",
        commands=(
            CommandSpec(f"qsv-clip-{clip.index}", tuple(command)),
        ),
        outputs=(
            OutputTarget(
                playlist=qsv_output / "index.m3u8",
                rendition_id=rendition.rendition_id,
                width=output_width,
                height=output_height,
                source_start_seconds=clip.start_seconds,
                expected_duration_seconds=clip.duration_seconds,
                segment_format="mpegts",
                qsv_worker_output=True,
                box_width=rendition.width,
                box_height=rendition.height,
                source_width=source.width,
                source_height=source.height,
            ),
        ),
        parallel=False,
    )


def _direct_copy_batch(
    ffmpeg: str,
    source: SourceInfo,
    clip: Clip,
    output_root: Path,
    segment_format: str = DEFAULT_SEGMENT_FORMAT,
) -> BatchPlan:
    playlist = output_root / "source" / "video.m3u8"
    command = [
        ffmpeg,
        "-hide_banner",
        "-nostdin",
        "-loglevel",
        "warning",
        "-y",
        "-progress",
        "pipe:1",
        "-nostats",
        "-fflags",
        "+genpts",
        "-ss",
        _number(clip.start_seconds),
        "-t",
        _number(clip.duration_seconds),
        "-i",
        str(source.path),
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
    hls_arguments = _hls_args(playlist, segment_format)
    command += hls_arguments[2:]
    return BatchPlan(
        label=f"stream-copy-clip-{clip.index}",
        commands=(
            CommandSpec(f"stream-copy-clip-{clip.index}", tuple(command)),
        ),
        outputs=(
            OutputTarget(
                playlist=playlist,
                rendition_id="source",
                width=source.width,
                height=source.height,
                source_start_seconds=clip.start_seconds,
                expected_duration_seconds=clip.duration_seconds,
                segment_format=segment_format,
                strict_six_second_grid=False,
                direct_copy=True,
            ),
        ),
        parallel=False,
    )


def build_candidate_matrix(
    *,
    ffmpeg: str,
    ffprobe: str,
    python_executable: str,
    source: SourceInfo,
    clips: Sequence[Clip],
    run_root: Path,
    temporal_chunks: int,
    nvenc_max_sessions: int = DEFAULT_NVENC_MAX_SESSIONS,
    segment_format: str = DEFAULT_SEGMENT_FORMAT,
) -> tuple[CandidatePlan, ...]:
    if temporal_chunks < 2:
        raise HarnessError(
            "temporal_chunks must be at least 2 for a parallel benchmark"
        )
    if nvenc_max_sessions < 2:
        raise HarnessError(
            "nvenc_max_sessions must be at least 2 so temporal chunks can "
            "actually run in parallel"
        )
    renditions = renditions_for_source(source)
    quality = NVENC_PROFILES["quality"]
    candidates: list[CandidatePlan] = []

    for profile_name in ("quality", "balanced", "turbo"):
        profile = NVENC_PROFILES[profile_name]
        candidate_id = f"grouped_nvenc_{profile_name}"
        clip_plans = []
        for clip in clips:
            output = run_root / candidate_id / f"clip_{clip.index:02d}"
            batch = _grouped_nvenc_batch(
                ffmpeg,
                source,
                clip,
                renditions,
                profile,
                output,
                f"{candidate_id}-clip-{clip.index}",
                segment_format,
            )
            clip_plans.append(ClipPlan(clip, (batch,)))
        candidates.append(
            CandidatePlan(
                candidate_id,
                "grouped_dual_nvenc",
                {
                    "profile": asdict(profile),
                    "processes_per_clip": 1,
                    "renditions_per_process": 2,
                    "segment_format": segment_format,
                },
                tuple(clip_plans),
            )
        )

    for rendition in renditions:
        candidate_id = f"single_nvenc_{rendition.rendition_id}"
        clip_plans = []
        for clip in clips:
            output = run_root / candidate_id / f"clip_{clip.index:02d}"
            command, target = _single_nvenc_command(
                ffmpeg,
                source,
                clip,
                rendition,
                quality,
                output,
                f"{candidate_id}-clip-{clip.index}",
                segment_format,
            )
            clip_plans.append(
                ClipPlan(
                    clip,
                    (
                        BatchPlan(
                            f"{candidate_id}-clip-{clip.index}",
                            (command,),
                            (target,),
                            False,
                        ),
                    ),
                )
            )
        candidates.append(
            CandidatePlan(
                candidate_id,
                "single_rendition_nvenc",
                {
                    "profile": asdict(quality),
                    "rendition": asdict(rendition),
                    "processes_per_clip": 1,
                    "segment_format": segment_format,
                },
                tuple(clip_plans),
            )
        )

    separate_id = "separate_nvenc_renditions_parallel"
    separate_clips = []
    for clip in clips:
        output = run_root / separate_id / f"clip_{clip.index:02d}"
        commands = []
        targets = []
        for rendition in renditions:
            command, target = _single_nvenc_command(
                ffmpeg,
                source,
                clip,
                rendition,
                quality,
                output,
                f"{separate_id}-{rendition.rendition_id}-clip-{clip.index}",
                segment_format,
            )
            commands.append(command)
            targets.append(target)
        separate_clips.append(
            ClipPlan(
                clip,
                (
                    BatchPlan(
                        f"{separate_id}-clip-{clip.index}",
                        tuple(commands),
                        tuple(targets),
                        True,
                    ),
                ),
            )
        )
    candidates.append(
        CandidatePlan(
            separate_id,
            "parallel_rendition_processes",
            {
                "profile": asdict(quality),
                "processes_per_clip": 2,
                "renditions_per_process": 1,
                "segment_format": segment_format,
            },
            tuple(separate_clips),
        )
    )

    temporal_id = "temporal_nvenc_chunks_parallel"
    temporal_clip_plans = []
    for clip in clips:
        chunk_duration = clip.duration_seconds / temporal_chunks
        chunk_units = chunk_duration / SEGMENT_SECONDS
        if not math.isclose(chunk_units, round(chunk_units), abs_tol=1e-8):
            raise HarnessError(
                "clip_seconds / temporal_chunks must be a multiple of six"
            )
        batches: list[BatchPlan] = []
        chunks_by_rendition: dict[str, list[OutputTarget]] = {
            rendition.rendition_id: [] for rendition in renditions
        }
        for chunk_index in range(temporal_chunks):
            chunk = Clip(
                index=chunk_index,
                start_seconds=(
                    clip.start_seconds + chunk_index * chunk_duration
                ),
                duration_seconds=chunk_duration,
            )
            for rendition in renditions:
                output = (
                    run_root
                    / temporal_id
                    / f"clip_{clip.index:02d}"
                    / "chunks"
                    / rendition.rendition_id
                    / f"chunk_{chunk_index:02d}"
                )
                command, target = _single_nvenc_command(
                    ffmpeg,
                    source,
                    chunk,
                    rendition,
                    quality,
                    output,
                    (
                        f"{temporal_id}-{rendition.rendition_id}-"
                        f"clip-{clip.index}-chunk-{chunk_index}"
                    ),
                    segment_format,
                )
                chunks_by_rendition[rendition.rendition_id].append(target)
                # Commands are scheduled below in per-rendition waves so each
                # parallel batch contains different temporal chunks.
                batches.append(
                    BatchPlan(
                        label=command.label,
                        commands=(command,),
                        outputs=(target,),
                        parallel=False,
                        validate_outputs=False,
                    )
                )

        scheduled_batches: list[BatchPlan] = []
        for rendition in renditions:
            rendition_batches = [
                batch
                for batch in batches
                if (
                    f"-{rendition.rendition_id}-"
                    in batch.commands[0].label
                )
            ]
            for wave_index in range(
                0,
                len(rendition_batches),
                nvenc_max_sessions,
            ):
                wave = rendition_batches[
                    wave_index:wave_index + nvenc_max_sessions
                ]
                scheduled_batches.append(
                    BatchPlan(
                        label=(
                            f"{temporal_id}-{rendition.rendition_id}-"
                            f"clip-{clip.index}-wave-"
                            f"{wave_index // nvenc_max_sessions}"
                        ),
                        commands=tuple(
                            batch.commands[0] for batch in wave
                        ),
                        outputs=tuple(
                            batch.outputs[0] for batch in wave
                        ),
                        parallel=len(wave) > 1,
                        validate_outputs=False,
                    )
                )

        stitches = []
        for rendition in renditions:
            final_playlist = (
                run_root
                / temporal_id
                / f"clip_{clip.index:02d}"
                / "final"
                / rendition.rendition_id
                / "video.m3u8"
            )
            final_target = _nvenc_output_target(
                playlist=final_playlist,
                rendition=rendition,
                source=source,
                clip=clip,
                segment_format=segment_format,
            )
            stitches.append(
                StitchPlan(
                    label=(
                        f"{temporal_id}-{rendition.rendition_id}-"
                        f"clip-{clip.index}-stitch"
                    ),
                    chunks=tuple(
                        chunks_by_rendition[rendition.rendition_id]
                    ),
                    output=final_target,
                )
            )
        temporal_clip_plans.append(
            ClipPlan(
                clip,
                tuple(scheduled_batches),
                tuple(stitches),
            )
        )
    candidates.append(
        CandidatePlan(
            temporal_id,
            "parallel_temporal_chunks",
            {
                "profile": asdict(quality),
                "chunk_processes_per_clip": temporal_chunks * len(renditions),
                "renditions_per_process": 1,
                "nvenc_sessions_per_process": 1,
                "nvenc_max_sessions": nvenc_max_sessions,
                "max_parallel_processes": min(
                    temporal_chunks,
                    nvenc_max_sessions,
                ),
                "chunk_duration_seconds": (
                    clips[0].duration_seconds / temporal_chunks
                ),
                "stitching": (
                    "move-only HLS assembly with per-chunk discontinuities"
                ),
                "segment_format": segment_format,
            },
            tuple(temporal_clip_plans),
        )
    )

    qsv_id = "intel_qsv_480p"
    qsv_clips = []
    qsv_rendition = renditions[1]
    for clip in clips:
        output = run_root / qsv_id / f"clip_{clip.index:02d}"
        qsv_clips.append(
            ClipPlan(
                clip,
                (
                    _qsv_batch(
                        python_executable,
                        ffmpeg,
                        ffprobe,
                        source,
                        clip,
                        qsv_rendition,
                        output,
                    ),
                ),
            )
        )
    candidates.append(
        CandidatePlan(
            qsv_id,
            "intel_qsv",
            {
                "encoder": "h264_qsv",
                "preset": "veryfast",
                "low_power": True,
                "rendition": asdict(qsv_rendition),
                "adapter": "services.transcoder.qsv_worker",
                "segment_format": "mpegts",
            },
            tuple(qsv_clips),
        )
    )

    hybrid_id = "hybrid_nvenc_720p_qsv_480p_parallel"
    hybrid_clips = []
    hybrid_nvenc_rendition = renditions[0]
    hybrid_qsv_rendition = renditions[1]
    hybrid_profile = NVENC_PROFILES["balanced"]
    for clip in clips:
        output = run_root / hybrid_id / f"clip_{clip.index:02d}"
        nvenc_command, nvenc_target = _single_nvenc_command(
            ffmpeg,
            source,
            clip,
            hybrid_nvenc_rendition,
            hybrid_profile,
            output,
            f"{hybrid_id}-nvenc-clip-{clip.index}",
            segment_format,
        )
        qsv_batch = _qsv_batch(
            python_executable,
            ffmpeg,
            ffprobe,
            source,
            clip,
            hybrid_qsv_rendition,
            output,
        )
        hybrid_clips.append(
            ClipPlan(
                clip,
                (
                    BatchPlan(
                        label=f"{hybrid_id}-clip-{clip.index}",
                        commands=(
                            nvenc_command,
                            qsv_batch.commands[0],
                        ),
                        outputs=(
                            nvenc_target,
                            qsv_batch.outputs[0],
                        ),
                        parallel=True,
                    ),
                ),
            )
        )
    candidates.append(
        CandidatePlan(
            hybrid_id,
            "parallel_distinct_hardware_encoders",
            {
                "processes_per_clip": 2,
                "max_parallel_processes": 2,
                "nvenc": {
                    "encoder": "h264_nvenc",
                    "profile": asdict(hybrid_profile),
                    "rendition": asdict(hybrid_nvenc_rendition),
                    "sessions": 1,
                    "device": "nvidia_gpu_0",
                    "segment_format": segment_format,
                },
                "qsv": {
                    "encoder": "h264_qsv",
                    "preset": "veryfast",
                    "low_power": True,
                    "rendition": asdict(hybrid_qsv_rendition),
                    "adapter": "services.transcoder.qsv_worker",
                    "device": "intel_integrated_gpu",
                    "segment_format": "mpegts",
                },
                "scheduling": (
                    "one NVENC and one QSV process launched simultaneously"
                ),
            },
            tuple(hybrid_clips),
        )
    )

    direct_id = "h264_stream_copy_direct_play"
    direct_clips = []
    for clip in clips:
        output = run_root / direct_id / f"clip_{clip.index:02d}"
        direct_clips.append(
            ClipPlan(
                clip,
                (
                    _direct_copy_batch(
                        ffmpeg,
                        source,
                        clip,
                        output,
                        segment_format,
                    ),
                ),
            )
        )
    candidates.append(
        CandidatePlan(
            direct_id,
            "stream_copy",
            {
                "encoder": "copy",
                "eligible": source.codec_name == "h264",
                "quality_metric_path": "lossless-by-copy",
                "segment_format": segment_format,
            },
            tuple(direct_clips),
        )
    )
    return tuple(candidates)


def _prepare_batch_outputs(batch: BatchPlan) -> None:
    for target in batch.outputs:
        if target.qsv_worker_output:
            target.playlist.parent.parent.mkdir(parents=True, exist_ok=True)
        else:
            target.playlist.parent.mkdir(parents=True, exist_ok=True)


def _resolve_playlist_uri(
    playlist: Path,
    value: str,
    *,
    description: str,
) -> Path:
    if "\\" in value or "?" in value or "#" in value:
        raise HarnessError(
            f"unsafe {description} URI in {playlist}: {value}"
        )
    uri = PurePosixPath(value)
    if uri.is_absolute() or ".." in uri.parts:
        raise HarnessError(
            f"unsafe {description} URI in {playlist}: {value}"
        )
    path = playlist.parent / uri
    if not path.is_file() or path.stat().st_size == 0:
        raise HarnessError(f"missing HLS {description}: {path}")
    return path


def _playlist_entries(playlist: Path) -> list[dict[str, Any]]:
    """Return safe media entries and the active initialization map."""

    if not playlist.is_file() or playlist.stat().st_size == 0:
        raise HarnessError(f"missing or empty playlist: {playlist}")
    lines = [
        line.strip()
        for line in playlist.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if not lines or lines[0] != "#EXTM3U":
        raise HarnessError(f"invalid HLS playlist: {playlist}")
    if "#EXT-X-ENDLIST" not in lines:
        raise HarnessError(f"incomplete HLS playlist: {playlist}")

    entries: list[dict[str, Any]] = []
    current_map: Path | None = None
    pending_duration: float | None = None
    for line in lines[1:]:
        if line.startswith("#EXT-X-MAP:"):
            match = _MAP_URI_RE.search(line)
            if match is None:
                raise HarnessError(f"invalid EXT-X-MAP in {playlist}")
            current_map = _resolve_playlist_uri(
                playlist,
                match.group(1),
                description="initialization segment",
            )
            continue
        if line.startswith("#EXTINF:"):
            pending_duration = float(
                line.split(":", 1)[1].split(",", 1)[0]
            )
            continue
        if line.startswith("#"):
            continue
        if pending_duration is None:
            raise HarnessError(f"segment without EXTINF in {playlist}")
        entries.append(
            {
                "duration_seconds": pending_duration,
                "segment": _resolve_playlist_uri(
                    playlist,
                    line,
                    description="media segment",
                ),
                "initialization": current_map,
            }
        )
        pending_duration = None
    if not entries:
        raise HarnessError(f"playlist contains no media segments: {playlist}")
    return entries


def stitch_hls_chunks(plan: StitchPlan) -> dict[str, Any]:
    """Move chunk media into one discontinuity-safe, playable HLS playlist."""

    if len(plan.chunks) < 2:
        raise HarnessError("a temporal stitch requires at least two chunks")
    expected_start = plan.output.source_start_seconds
    for chunk in plan.chunks:
        if chunk.rendition_id != plan.output.rendition_id:
            raise HarnessError("stitch chunks mix rendition identifiers")
        if chunk.segment_format != plan.output.segment_format:
            raise HarnessError("stitch chunks mix HLS segment formats")
        if not math.isclose(
            chunk.source_start_seconds,
            expected_start,
            rel_tol=0,
            abs_tol=1e-6,
        ):
            raise HarnessError("stitch chunks are not contiguous and ordered")
        expected_start += chunk.expected_duration_seconds
    if not math.isclose(
        expected_start - plan.output.source_start_seconds,
        plan.output.expected_duration_seconds,
        rel_tol=0,
        abs_tol=1e-6,
    ):
        raise HarnessError("stitched chunk durations do not cover the clip")

    output_dir = plan.output.playlist.parent
    output_dir.mkdir(parents=True, exist_ok=False)
    playlist_lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:7",
        "#EXT-X-MEDIA-SEQUENCE:0",
        "#EXT-X-INDEPENDENT-SEGMENTS",
    ]
    maximum_duration = 0.0
    moved_files = 0
    moved_bytes = 0
    seen_source_media: set[Path] = set()
    for chunk_index, chunk in enumerate(plan.chunks):
        parsed = _parse_playlist(chunk.playlist)
        entries = _playlist_entries(chunk.playlist)
        if not parsed["independent_segments_declared"]:
            raise HarnessError(
                f"chunk lacks independent-segment declaration: "
                f"{chunk.playlist}"
            )
        if chunk_index:
            playlist_lines.append("#EXT-X-DISCONTINUITY")

        map_destinations: dict[Path, str] = {}
        active_map_name: str | None = None
        for entry_index, entry in enumerate(entries):
            initialization = entry["initialization"]
            if plan.output.segment_format == "fmp4":
                if initialization is None:
                    raise HarnessError(
                        f"fMP4 chunk lacks EXT-X-MAP: {chunk.playlist}"
                    )
                if initialization not in map_destinations:
                    map_name = (
                        f"chunk_{chunk_index:02d}_"
                        f"init_{len(map_destinations):02d}.mp4"
                    )
                    destination = output_dir / map_name
                    moved_bytes += initialization.stat().st_size
                    initialization.replace(destination)
                    moved_files += 1
                    map_destinations[initialization] = map_name
                map_name = map_destinations[initialization]
                if map_name != active_map_name:
                    playlist_lines.append(
                        f'#EXT-X-MAP:URI="{map_name}"'
                    )
                    active_map_name = map_name
            elif initialization is not None:
                raise HarnessError(
                    f"MPEG-TS chunk unexpectedly has EXT-X-MAP: "
                    f"{chunk.playlist}"
                )

            segment = entry["segment"]
            if segment in seen_source_media:
                raise HarnessError("a chunk segment was referenced more than once")
            seen_source_media.add(segment)
            segment_name = (
                f"chunk_{chunk_index:02d}_segment_{entry_index:05d}"
                f"{segment.suffix.lower()}"
            )
            destination = output_dir / segment_name
            duration = float(entry["duration_seconds"])
            maximum_duration = max(maximum_duration, duration)
            moved_bytes += segment.stat().st_size
            segment.replace(destination)
            moved_files += 1
            playlist_lines.extend(
                [f"#EXTINF:{_number(duration)},", segment_name]
            )

    playlist_lines.insert(
        3,
        f"#EXT-X-TARGETDURATION:{max(1, math.ceil(maximum_duration))}",
    )
    playlist_lines.append("#EXT-X-ENDLIST")
    temporary = plan.output.playlist.with_name(
        f".{plan.output.playlist.name}.{uuid.uuid4().hex}.tmp"
    )
    temporary.write_text(
        "\n".join(playlist_lines) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    os.replace(temporary, plan.output.playlist)
    return {
        "label": plan.label,
        "chunks": len(plan.chunks),
        "moved_files": moved_files,
        "moved_bytes": moved_bytes,
        "playlist": str(plan.output.playlist),
    }


def _parse_playlist(playlist: Path) -> dict[str, Any]:
    if not playlist.is_file() or playlist.stat().st_size == 0:
        raise HarnessError(f"missing or empty playlist: {playlist}")
    lines = [
        line.strip()
        for line in playlist.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if not lines or lines[0] != "#EXTM3U":
        raise HarnessError(f"invalid HLS playlist: {playlist}")
    if "#EXT-X-ENDLIST" not in lines:
        raise HarnessError(f"incomplete HLS playlist: {playlist}")

    durations: list[float] = []
    segment_bytes = 0
    segment_count = 0
    initialization_bytes = 0
    initialization_uri: str | None = None
    initialization_uris: list[str] = []
    seen_initializations: set[Path] = set()
    pending: float | None = None
    for line in lines[1:]:
        if line.startswith("#EXTINF:"):
            pending = float(line.split(":", 1)[1].split(",", 1)[0])
            continue
        if line.startswith("#EXT-X-MAP:"):
            match = _MAP_URI_RE.search(line)
            if match is None:
                raise HarnessError(f"invalid EXT-X-MAP in {playlist}")
            current_uri = match.group(1)
            if initialization_uri is None:
                initialization_uri = current_uri
            if current_uri not in initialization_uris:
                initialization_uris.append(current_uri)
            initialization = _resolve_playlist_uri(
                playlist,
                current_uri,
                description="initialization segment",
            )
            if initialization not in seen_initializations:
                initialization_bytes += initialization.stat().st_size
                seen_initializations.add(initialization)
            continue
        if line.startswith("#"):
            continue
        if pending is None:
            raise HarnessError(f"segment without EXTINF in {playlist}")
        segment = _resolve_playlist_uri(
            playlist,
            line,
            description="media segment",
        )
        durations.append(pending)
        segment_bytes += segment.stat().st_size
        segment_count += 1
        pending = None
    if not durations:
        raise HarnessError(f"playlist contains no media segments: {playlist}")
    return {
        "playlist_bytes": playlist.stat().st_size,
        "segment_bytes": segment_bytes,
        "initialization_bytes": initialization_bytes,
        "initialization_uri": initialization_uri,
        "initialization_uris": initialization_uris,
        "segment_count": segment_count,
        "segment_durations_seconds": [
            round(value, 6) for value in durations
        ],
        "playlist_duration_seconds": round(sum(durations), 6),
        "independent_segments_declared": (
            "#EXT-X-INDEPENDENT-SEGMENTS" in lines
        ),
    }


def _probe_playlist(
    playlist: Path,
    ffprobe: str,
    executor: Executor,
) -> dict[str, Any]:
    result = _run_one(
        executor,
        f"probe-{playlist.parent.name}",
        [
            ffprobe,
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            (
                "stream=codec_name,width,height,avg_frame_rate:"
                "format=duration"
            ),
            "-of",
            "json",
            str(playlist),
        ],
        60.0,
    )
    if result.returncode != 0:
        raise HarnessError(f"output probe failed: {_tail(result.stderr)}")
    try:
        payload = json.loads(result.stdout)
        stream = payload["streams"][0]
    except (json.JSONDecodeError, KeyError, IndexError, TypeError) as exc:
        raise HarnessError("output probe returned invalid JSON") from exc
    return {
        "codec_name": str(stream.get("codec_name") or ""),
        "width": int(stream.get("width") or 0),
        "height": int(stream.get("height") or 0),
        "avg_frame_rate": stream.get("avg_frame_rate"),
        "duration_seconds": _optional_float(
            (payload.get("format") or {}).get("duration")
        ),
    }


def validate_output(
    target: OutputTarget,
    *,
    ffprobe: str,
    executor: Executor,
    fps: Fraction,
) -> dict[str, Any]:
    playlist = _parse_playlist(target.playlist)
    probe = _probe_playlist(target.playlist, ffprobe, executor)
    errors = []
    geometry: dict[str, Any] | None = None
    if probe["codec_name"] != "h264":
        errors.append(f"expected h264, got {probe['codec_name']!r}")
    if probe["width"] != target.width or probe["height"] != target.height:
        errors.append(
            "dimensions differ: "
            f"expected={target.width}x{target.height} "
            f"actual={probe['width']}x{probe['height']}"
        )
    if (
        target.box_width is not None
        and target.box_height is not None
        and target.source_width is not None
        and target.source_height is not None
    ):
        actual_width = probe["width"]
        actual_height = probe["height"]
        source_ratio = target.source_width / target.source_height
        actual_ratio = (
            actual_width / actual_height if actual_height > 0 else 0.0
        )
        relative_aspect_error = (
            abs(actual_ratio - source_ratio) / source_ratio
            if source_ratio > 0
            else math.inf
        )
        geometry = {
            "box_width": target.box_width,
            "box_height": target.box_height,
            "dimensions_are_even": (
                actual_width > 0
                and actual_height > 0
                and actual_width % 2 == 0
                and actual_height % 2 == 0
            ),
            "inside_box": (
                0 < actual_width <= target.box_width
                and 0 < actual_height <= target.box_height
            ),
            "source_display_aspect_ratio": round(source_ratio, 8),
            "actual_display_aspect_ratio": round(actual_ratio, 8),
            "relative_aspect_error": round(relative_aspect_error, 8),
        }
        if not geometry["dimensions_are_even"]:
            errors.append("scaled dimensions are not positive and even")
        if not geometry["inside_box"]:
            errors.append("scaled dimensions exceed the rendition box")
        if relative_aspect_error > 0.005:
            errors.append(
                "scaled display aspect ratio differs from the source by "
                "more than 0.5%"
            )
    tolerance = max(0.25, 2.0 / float(fps))
    durations = playlist["segment_durations_seconds"]
    if target.strict_six_second_grid:
        for duration in durations[:-1]:
            if not math.isclose(
                duration,
                SEGMENT_SECONDS,
                rel_tol=0,
                abs_tol=tolerance,
            ):
                errors.append(f"non-final segment is {duration}s, not 6s")
        if durations[-1] > SEGMENT_SECONDS + tolerance:
            errors.append("final segment exceeds six-second boundary")
        if not math.isclose(
            playlist["playlist_duration_seconds"],
            target.expected_duration_seconds,
            rel_tol=0,
            abs_tol=tolerance,
        ):
            errors.append(
                "playlist duration differs from requested clip: "
                f"{playlist['playlist_duration_seconds']} vs "
                f"{target.expected_duration_seconds}"
            )
    else:
        # Stream-copy can only cut at keyframes already present in the source.
        if (
            abs(
                playlist["playlist_duration_seconds"]
                - target.expected_duration_seconds
            )
            > SEGMENT_SECONDS * 2
        ):
            errors.append("stream-copy duration differs by more than two GOPs")
    if not playlist["independent_segments_declared"]:
        errors.append("playlist lacks EXT-X-INDEPENDENT-SEGMENTS")
    if (
        target.segment_format == "fmp4"
        and playlist["initialization_uri"] is None
    ):
        errors.append("fMP4 playlist lacks EXT-X-MAP initialization segment")
    if (
        target.segment_format == "mpegts"
        and playlist["initialization_uri"] is not None
    ):
        errors.append("MPEG-TS playlist unexpectedly declares EXT-X-MAP")
    return {
        "status": "passed" if not errors else "failed",
        "errors": errors,
        "playlist": str(target.playlist),
        "rendition_id": target.rendition_id,
        "source_start_seconds": target.source_start_seconds,
        "requested_duration_seconds": target.expected_duration_seconds,
        "playlist_validation": playlist,
        "actual": probe,
        "geometry_validation": geometry,
        "output_bytes": (
            playlist["playlist_bytes"]
            + playlist["segment_bytes"]
            + playlist["initialization_bytes"]
        ),
    }


def build_quality_command(
    *,
    ffmpeg: str,
    source: Path,
    source_start_seconds: float,
    duration_seconds: float,
    playlist: Path,
    width: int,
    height: int,
    metric_path: str,
) -> list[str]:
    command = [
        ffmpeg,
        "-hide_banner",
        "-nostdin",
        "-loglevel",
        "info",
        "-ss",
        _number(source_start_seconds),
        "-i",
        str(source),
        "-i",
        str(playlist),
        "-t",
        _number(duration_seconds),
    ]
    reference = (
        f"[0:v]setpts=PTS-STARTPTS,scale={width}:{height}:"
        "flags=bicubic,format=yuv420p[ref]"
    )
    distorted = (
        "[1:v]setpts=PTS-STARTPTS,format=yuv420p[dist]"
    )
    if metric_path == "libvmaf":
        graph = (
            f"{reference};{distorted};"
            "[dist][ref]libvmaf=n_threads=4[metric]"
        )
    else:
        graph = (
            f"{reference};{distorted};"
            "[ref]split=2[ref_ssim][ref_psnr];"
            "[dist]split=2[dist_ssim][dist_psnr];"
            "[dist_ssim][ref_ssim]ssim[ssim_out];"
            "[dist_psnr][ref_psnr]psnr[metric];"
            "[ssim_out]nullsink"
        )
    command += [
        "-filter_complex",
        graph,
        "-map",
        "[metric]",
        "-an",
        "-f",
        "null",
        "-",
    ]
    return command


def measure_quality(
    target: OutputTarget,
    *,
    source: SourceInfo,
    ffmpeg: str,
    executor: Executor,
    metric_path: str,
) -> dict[str, Any]:
    if target.direct_copy:
        return {
            "status": "not-required",
            "metric_path": "lossless-by-copy",
        }
    command = build_quality_command(
        ffmpeg=ffmpeg,
        source=source.path,
        source_start_seconds=target.source_start_seconds,
        duration_seconds=target.expected_duration_seconds,
        playlist=target.playlist,
        width=target.width,
        height=target.height,
        metric_path=metric_path,
    )
    result = _run_one(
        executor,
        f"quality-{target.rendition_id}",
        command,
        max(300.0, target.expected_duration_seconds * 5),
    )
    combined = f"{result.stdout}\n{result.stderr}"
    payload: dict[str, Any] = {
        "status": "passed" if result.returncode == 0 else "failed",
        "metric_path": metric_path,
        "wall_seconds": round(result.wall_seconds, 6),
        "command": command,
        "stderr_tail": _tail(result.stderr),
    }
    if metric_path == "libvmaf":
        match = _VMAF_RE.search(combined)
        if not match:
            payload["status"] = "failed"
            payload["error"] = "VMAF score was not present in FFmpeg output"
        else:
            payload["vmaf"] = float(match.group(1))
    else:
        ssim = _SSIM_RE.search(combined)
        psnr = _PSNR_RE.search(combined)
        if not ssim or not psnr:
            payload["status"] = "failed"
            payload["error"] = "SSIM/PSNR summary was not present"
        else:
            payload["ssim"] = float(ssim.group(1))
            payload["psnr_db"] = (
                "inf"
                if psnr.group(1).lower() == "inf"
                else float(psnr.group(1))
            )
    return payload


def _quality_summary(records: Sequence[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        grouped.setdefault(record["rendition_id"], []).append(record)
    summary: dict[str, Any] = {}
    for rendition_id, values in grouped.items():
        metric_path = values[0]["quality"]["metric_path"]
        entry: dict[str, Any] = {
            "metric_path": metric_path,
            "measurements": len(values),
        }
        passed = [
            value
            for value in values
            if value["quality"]["status"] in {"passed", "not-required"}
        ]
        if metric_path == "lossless-by-copy":
            entry["result"] = "lossless-by-copy"
        elif not passed:
            entry["status"] = "failed"
        elif metric_path == "libvmaf":
            entry["vmaf_mean"] = round(
                sum(value["quality"]["vmaf"] for value in passed)
                / len(passed),
                6,
            )
        else:
            entry["ssim_mean"] = round(
                sum(value["quality"]["ssim"] for value in passed)
                / len(passed),
                8,
            )
            finite_psnr = []
            for value in passed:
                psnr_value = value["quality"]["psnr_db"]
                if isinstance(psnr_value, (int, float)) and math.isfinite(
                    psnr_value
                ):
                    finite_psnr.append(float(psnr_value))
            entry["psnr_db_mean"] = (
                round(sum(finite_psnr) / len(finite_psnr), 6)
                if finite_psnr
                else "inf"
            )
        summary[rendition_id] = entry
    return summary


def _process_record(result: ProcessResult) -> dict[str, Any]:
    return {
        "label": result.label,
        "command": list(result.args),
        "returncode": result.returncode,
        "wall_seconds": round(result.wall_seconds, 6),
        "timed_out": result.timed_out,
        "stdout_tail": _tail(result.stdout),
        "stderr_tail": _tail(result.stderr),
    }


def run_candidate(
    candidate: CandidatePlan,
    *,
    source: SourceInfo,
    ffmpeg: str,
    ffprobe: str,
    executor: Executor,
    metric_path: str,
    run_root: Path,
    timeout_multiplier: float,
) -> dict[str, Any]:
    candidate_root = run_root / candidate.candidate_id
    result: dict[str, Any] = {
        "candidate_id": candidate.candidate_id,
        "category": candidate.category,
        "settings": candidate.settings,
        "status": "running",
        "clips": [],
    }
    encode_wall = 0.0
    output_bytes = 0
    source_seconds = 0.0
    output_seconds = 0.0
    quality_records: list[dict[str, Any]] = []
    total_started = time.monotonic()
    source_identity_before = _source_identity(source.path)
    try:
        if (
            candidate.category == "stream_copy"
            and source.codec_name != "h264"
        ):
            result["status"] = "skipped"
            result["reason"] = (
                f"source codec is {source.codec_name!r}, not H.264"
            )
            return result
        candidate_root.mkdir(parents=True, exist_ok=False)
        for clip_plan in candidate.clips:
            clip_result: dict[str, Any] = {
                "clip": asdict(clip_plan.clip),
                "batches": [],
                "outputs": [],
            }
            clip_ok = True
            for batch in clip_plan.batches:
                _prepare_batch_outputs(batch)
                timeout = max(
                    300.0,
                    clip_plan.clip.duration_seconds * timeout_multiplier,
                )
                batch_result = executor.run_batch(
                    batch.commands,
                    parallel=batch.parallel,
                    timeout_seconds=timeout,
                )
                encode_wall += batch_result.wall_seconds
                process_records = [
                    _process_record(process)
                    for process in batch_result.processes
                ]
                batch_ok = all(
                    process.returncode == 0 and not process.timed_out
                    for process in batch_result.processes
                )
                clip_result["batches"].append(
                    {
                        "label": batch.label,
                        "parallel": batch.parallel,
                        "wall_seconds": round(
                            batch_result.wall_seconds,
                            6,
                        ),
                        "status": "passed" if batch_ok else "failed",
                        "processes": process_records,
                    }
                )
                if not batch_ok:
                    clip_ok = False
                    continue
                if not batch.validate_outputs:
                    continue
                for target in batch.outputs:
                    validation = validate_output(
                        target,
                        ffprobe=ffprobe,
                        executor=executor,
                        fps=source.fps,
                    )
                    quality = measure_quality(
                        target,
                        source=source,
                        ffmpeg=ffmpeg,
                        executor=executor,
                        metric_path=metric_path,
                    )
                    record = {
                        **validation,
                        "quality": quality,
                    }
                    clip_result["outputs"].append(record)
                    output_bytes += validation["output_bytes"]
                    output_seconds += validation[
                        "playlist_validation"
                    ]["playlist_duration_seconds"]
                    if (
                        validation["status"] != "passed"
                        or quality["status"] == "failed"
                    ):
                        clip_ok = False
                    quality_records.append(
                        {
                            "rendition_id": target.rendition_id,
                            "quality": quality,
                        }
                    )
            if clip_ok:
                for stitch in clip_plan.stitches:
                    stitch_started = time.monotonic()
                    stitch_details: dict[str, Any] = {}
                    stitch_error: str | None = None
                    try:
                        stitch_details = stitch_hls_chunks(stitch)
                    except Exception as exc:
                        stitch_error = f"{type(exc).__name__}: {exc}"
                    stitch_wall = time.monotonic() - stitch_started
                    encode_wall += stitch_wall
                    stitch_ok = stitch_error is None
                    stitch_record: dict[str, Any] = {
                        "label": stitch.label,
                        "kind": "move-only-hls-stitch",
                        "parallel": False,
                        "wall_seconds": round(stitch_wall, 6),
                        "status": "passed" if stitch_ok else "failed",
                        "processes": [],
                        "details": stitch_details,
                    }
                    if stitch_error is not None:
                        stitch_record["error"] = stitch_error
                    clip_result["batches"].append(stitch_record)
                    if not stitch_ok:
                        clip_ok = False
                        continue
                    target = stitch.output
                    validation = validate_output(
                        target,
                        ffprobe=ffprobe,
                        executor=executor,
                        fps=source.fps,
                    )
                    quality = measure_quality(
                        target,
                        source=source,
                        ffmpeg=ffmpeg,
                        executor=executor,
                        metric_path=metric_path,
                    )
                    record = {
                        **validation,
                        "quality": quality,
                    }
                    clip_result["outputs"].append(record)
                    output_bytes += validation["output_bytes"]
                    output_seconds += validation[
                        "playlist_validation"
                    ]["playlist_duration_seconds"]
                    if (
                        validation["status"] != "passed"
                        or quality["status"] == "failed"
                    ):
                        clip_ok = False
                    quality_records.append(
                        {
                            "rendition_id": target.rendition_id,
                            "quality": quality,
                        }
                    )
            if clip_ok:
                source_seconds += clip_plan.clip.duration_seconds
            clip_result["status"] = "passed" if clip_ok else "failed"
            result["clips"].append(clip_result)

        result["status"] = (
            "passed"
            if result["clips"]
            and all(clip["status"] == "passed" for clip in result["clips"])
            else "failed"
        )
    except KeyboardInterrupt:
        result["status"] = "interrupted"
        raise
    except Exception as exc:
        result["status"] = "failed"
        result["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        result["encode_wall_seconds"] = round(encode_wall, 6)
        result["total_wall_seconds"] = round(
            time.monotonic() - total_started,
            6,
        )
        result["source_seconds_processed"] = round(source_seconds, 6)
        result["output_seconds_processed"] = round(output_seconds, 6)
        result["source_realtime_factor"] = (
            round(source_seconds / encode_wall, 6)
            if encode_wall > 0
            else None
        )
        result["output_realtime_factor"] = (
            round(output_seconds / encode_wall, 6)
            if encode_wall > 0
            else None
        )
        result["output_bytes"] = output_bytes
        result["quality_by_rendition"] = _quality_summary(
            quality_records
        )
        cleanup = cleanup_owned_tree(
            candidate_root,
            owner_root=run_root,
            source=source.path,
        )
        source_identity_after = _source_identity(source.path)
        cleanup_reported_source_preserved = bool(
            cleanup.get("source_preserved", False)
        )
        cleanup["source_preserved"] = (
            cleanup_reported_source_preserved
            and source_identity_before == source_identity_after
        )
        result["cleanup"] = cleanup
        if not cleanup.get("deleted") or not cleanup["source_preserved"]:
            previous_status = result["status"]
            result["status"] = "failed"
            result["status_before_cleanup"] = previous_status
            result["cleanup_error"] = (
                "candidate cleanup did not delete all generated media"
                if not cleanup.get("deleted")
                else "source preservation check failed during candidate cleanup"
            )
    return result


def cleanup_owned_tree(
    target: Path,
    *,
    owner_root: Path,
    source: Path,
) -> dict[str, Any]:
    outcome = {
        "target": str(target),
        "attempted": False,
        "deleted": False,
        "bytes_removed": 0,
        "source_preserved": source.exists(),
        "error": None,
    }
    if not target.exists():
        outcome["deleted"] = True
        return outcome
    outcome["attempted"] = True
    try:
        resolved_target = target.resolve(strict=True)
        resolved_owner = owner_root.resolve(strict=True)
        resolved_source = source.resolve(strict=True)
        if resolved_target == resolved_owner:
            raise HarnessError("refusing to delete the owner root as a candidate")
        if not resolved_target.is_relative_to(resolved_owner):
            raise HarnessError("cleanup target escaped the owned run root")
        if (
            resolved_source == resolved_target
            or resolved_source.is_relative_to(resolved_target)
        ):
            raise HarnessError("cleanup target contains the source")
        total = 0
        for path in resolved_target.rglob("*"):
            if path.is_file():
                total += path.stat().st_size
        shutil.rmtree(resolved_target)
        outcome["bytes_removed"] = total
        outcome["deleted"] = not resolved_target.exists()
    except Exception as exc:
        outcome["error"] = f"{type(exc).__name__}: {exc}"
    outcome["source_preserved"] = source.exists()
    return outcome


def _planned_candidate(candidate: CandidatePlan) -> dict[str, Any]:
    return {
        "candidate_id": candidate.candidate_id,
        "category": candidate.category,
        "settings": candidate.settings,
        "status": "planned",
        "clips": [
            {
                "clip": asdict(clip_plan.clip),
                "batches": [
                    {
                        "label": batch.label,
                        "parallel": batch.parallel,
                        "validate_outputs": batch.validate_outputs,
                        "commands": [
                            {
                                "label": command.label,
                                "args": list(command.args),
                            }
                            for command in batch.commands
                        ],
                        "outputs": [
                            {
                                **asdict(target),
                                "playlist": str(target.playlist),
                            }
                            for target in batch.outputs
                        ],
                    }
                    for batch in clip_plan.batches
                ],
                "stitches": [
                    {
                        "label": stitch.label,
                        "chunks": [
                            {
                                **asdict(target),
                                "playlist": str(target.playlist),
                            }
                            for target in stitch.chunks
                        ],
                        "output": {
                            **asdict(stitch.output),
                            "playlist": str(stitch.output.playlist),
                        },
                    }
                    for stitch in clip_plan.stitches
                ],
            }
            for clip_plan in candidate.clips
        ],
        "cleanup": {
            "attempted": False,
            "deleted": True,
            "reason": "dry-run created no media",
        },
    }


def _source_identity(source: Path) -> dict[str, Any]:
    if not source.exists():
        return {"exists": False}
    stat = source.stat()
    return {
        "exists": True,
        "size_bytes": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }


def _write_report_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    with temporary.open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(
            payload,
            handle,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _candidate_filter_values(args: argparse.Namespace) -> tuple[str, ...]:
    raw_values = getattr(args, "candidate", None) or ()
    if isinstance(raw_values, str):
        raw_values = (raw_values,)
    requested: list[str] = []
    for raw_value in raw_values:
        for value in str(raw_value).split(","):
            candidate_id = value.strip()
            if candidate_id and candidate_id not in requested:
                requested.append(candidate_id)
    return tuple(requested)


def _filter_candidates(
    candidates: Sequence[CandidatePlan],
    requested_ids: Sequence[str],
) -> list[CandidatePlan]:
    if not requested_ids:
        return list(candidates)
    by_id = {candidate.candidate_id: candidate for candidate in candidates}
    unknown = [
        candidate_id
        for candidate_id in requested_ids
        if candidate_id not in by_id
    ]
    if unknown:
        available = ", ".join(sorted(by_id))
        raise HarnessError(
            "unknown candidate ID(s): "
            f"{', '.join(unknown)}; available candidates: {available}"
        )
    requested_set = set(requested_ids)
    return [
        candidate
        for candidate in candidates
        if candidate.candidate_id in requested_set
    ]


def run_harness(
    args: argparse.Namespace,
    *,
    executor: Executor | None = None,
) -> dict[str, Any]:
    executor = executor or SubprocessExecutor()
    source = args.source.expanduser().resolve(strict=False)
    if not source.is_file():
        raise HarnessError(f"source is not a file: {source}")
    report_path = args.report.expanduser().resolve(strict=False)
    work_root = args.work_root.expanduser().resolve(strict=False)
    if source.is_relative_to(work_root):
        raise HarnessError("work root must not contain the source")
    work_root.mkdir(parents=True, exist_ok=True)

    ffmpeg = resolve_executable(args.ffmpeg)
    ffprobe = resolve_executable(args.ffprobe)
    python_executable = resolve_executable(args.python_executable)
    ffmpeg_info = inspect_ffmpeg(ffmpeg, executor)
    source_info = probe_source(source, ffprobe, executor)
    clips = select_aligned_clips(
        source_info.duration_seconds,
        clip_seconds=args.clip_seconds,
        clip_count=args.clip_count,
    )
    run_id = uuid.uuid4().hex
    run_root = work_root / f"run-{run_id}"
    if report_path.is_relative_to(run_root):
        raise HarnessError("report must be outside the disposable run root")
    nvenc_max_sessions = getattr(
        args,
        "nvenc_max_sessions",
        DEFAULT_NVENC_MAX_SESSIONS,
    )
    candidate_order_seed = getattr(
        args,
        "candidate_order_seed",
        DEFAULT_CANDIDATE_ORDER_SEED,
    )
    requested_candidate_ids = _candidate_filter_values(args)
    candidate_list = _filter_candidates(
        build_candidate_matrix(
            ffmpeg=ffmpeg,
            ffprobe=ffprobe,
            python_executable=python_executable,
            source=source_info,
            clips=clips,
            run_root=run_root,
            temporal_chunks=args.temporal_chunks,
            nvenc_max_sessions=nvenc_max_sessions,
            segment_format=args.segment_format,
        ),
        requested_candidate_ids,
    )
    random.Random(candidate_order_seed).shuffle(candidate_list)
    candidates = tuple(candidate_list)
    source_before = _source_identity(source)
    started_at = _utc_now()
    report: dict[str, Any] = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "run_id": run_id,
        "mode": "execute" if args.execute else "dry-run",
        "status": "running",
        "started_at": started_at,
        "host": {
            "platform": platform.platform(),
            "python": sys.version.split()[0],
        },
        "source": {
            "path": str(source_info.path),
            "size_bytes": source_info.size_bytes,
            "mtime_ns": source_info.mtime_ns,
            "duration_seconds": source_info.duration_seconds,
            "codec_name": source_info.codec_name,
            "width": source_info.width,
            "height": source_info.height,
            "fps": str(source_info.fps),
        },
        "ffmpeg": ffmpeg_info,
        "ffprobe_executable": ffprobe,
        "benchmark": {
            "segment_seconds": SEGMENT_SECONDS,
            "segment_format": args.segment_format,
            "clip_seconds": args.clip_seconds,
            "clip_count": args.clip_count,
            "temporal_chunks": args.temporal_chunks,
            "nvenc_max_sessions": nvenc_max_sessions,
            "candidate_filter": list(requested_candidate_ids),
            "candidate_order": {
                "strategy": "deterministic-randomized",
                "seed": candidate_order_seed,
                "candidate_ids": [
                    candidate.candidate_id for candidate in candidates
                ],
            },
            "clips": [asdict(clip) for clip in clips],
        },
        "candidates": [],
    }
    run_cleanup: dict[str, Any] = {
        "attempted": False,
        "deleted": True,
        "source_preserved": True,
    }
    interrupted = False
    try:
        if args.execute:
            run_root.mkdir(parents=True, exist_ok=False)
            for candidate in candidates:
                candidate_result = run_candidate(
                    candidate,
                    source=source_info,
                    ffmpeg=ffmpeg,
                    ffprobe=ffprobe,
                    executor=executor,
                    metric_path=ffmpeg_info["quality_metric_path"],
                    run_root=run_root,
                    timeout_multiplier=args.timeout_multiplier,
                )
                report["candidates"].append(candidate_result)
                _write_report_atomic(report_path, report)
        else:
            report["candidates"] = [
                _planned_candidate(candidate) for candidate in candidates
            ]
        report["status"] = (
            "planned"
            if not args.execute
            else (
                "passed"
                if all(
                    candidate["status"] in {"passed", "skipped"}
                    for candidate in report["candidates"]
                )
                else "completed-with-failures"
            )
        )
    except KeyboardInterrupt:
        interrupted = True
        report["status"] = "interrupted"
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        if run_root.exists():
            run_cleanup = cleanup_owned_tree(
                run_root,
                owner_root=work_root,
                source=source,
            )
        source_after = _source_identity(source)
        cleanup_reported_source_preserved = bool(
            run_cleanup.get("source_preserved", False)
        )
        source_preserved = (
            cleanup_reported_source_preserved
            and source_before == source_after
        )
        run_cleanup["source_preserved"] = source_preserved
        report["cleanup"] = run_cleanup
        report["source_identity_before"] = source_before
        report["source_identity_after"] = source_after
        report["completed_at"] = _utc_now()
        if not source_preserved:
            report["status"] = "failed"
            report["source_integrity_error"] = (
                "source preservation check failed during benchmark"
            )
        if not run_cleanup.get("deleted"):
            report["status"] = "failed"
            report["cleanup_error"] = (
                "run cleanup did not delete all generated media"
            )
        _write_report_atomic(report_path, report)
    if interrupted:
        raise KeyboardInterrupt
    return report


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark HLS acceleration candidates. Dry-run is the default; "
            "pass --execute to run media workloads."
        )
    )
    parser.add_argument(
        "--source",
        type=Path,
        required=True,
        help=r"Exact source file to benchmark, for example D:\media\source.mkv",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=Path(".benchmarks/hls-acceleration-report.json"),
    )
    parser.add_argument(
        "--work-root",
        type=Path,
        default=Path(".benchmarks/hls-acceleration-work"),
    )
    parser.add_argument("--ffmpeg", default="ffmpeg")
    parser.add_argument("--ffprobe", default="ffprobe")
    parser.add_argument("--python-executable", default=sys.executable)
    parser.add_argument("--clip-seconds", type=float, default=DEFAULT_CLIP_SECONDS)
    parser.add_argument("--clip-count", type=int, default=DEFAULT_CLIP_COUNT)
    parser.add_argument(
        "--temporal-chunks",
        type=int,
        default=DEFAULT_TEMPORAL_CHUNKS,
    )
    parser.add_argument(
        "--nvenc-max-sessions",
        type=int,
        default=DEFAULT_NVENC_MAX_SESSIONS,
        help=(
            "Maximum simultaneous NVENC sessions used by any candidate "
            "(default: 2)."
        ),
    )
    parser.add_argument(
        "--candidate-order-seed",
        type=int,
        default=DEFAULT_CANDIDATE_ORDER_SEED,
        help=(
            "Seed for deterministic randomized candidate execution order."
        ),
    )
    parser.add_argument(
        "--candidate",
        action="append",
        default=[],
        metavar="ID[,ID...]",
        help=(
            "Run only the selected candidate ID. Repeat the option or pass "
            "a comma-separated list to select multiple candidates."
        ),
    )
    parser.add_argument(
        "--segment-format",
        choices=("fmp4", "mpegts"),
        default=DEFAULT_SEGMENT_FORMAT,
        help=(
            "HLS media segment format for NVENC and stream-copy candidates "
            "(default: production fMP4 layout)."
        ),
    )
    parser.add_argument("--timeout-multiplier", type=float, default=5.0)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Opt in to running GPU/CPU media workloads.",
    )
    args = parser.parse_args(argv)
    if args.temporal_chunks < 2:
        parser.error("--temporal-chunks must be at least 2")
    if args.nvenc_max_sessions < 2:
        parser.error("--nvenc-max-sessions must be at least 2")
    if args.timeout_multiplier <= 0 or not math.isfinite(
        args.timeout_multiplier
    ):
        parser.error("--timeout-multiplier must be positive and finite")
    return args


def _optional_float(value: Any) -> float | None:
    if value in (None, "", "N/A"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _number(value: float) -> str:
    return format(value, ".12g")


def _tail(value: str, limit: int = 1500) -> str:
    return (value or "").strip()[-limit:]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        report = run_harness(args)
    except KeyboardInterrupt:
        return 130
    except HarnessError as exc:
        print(f"benchmark setup failed: {exc}", file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "status": report["status"],
                "mode": report["mode"],
                "report": str(args.report.resolve(strict=False)),
            },
            indent=2,
        )
    )
    return 0 if report["status"] in {"planned", "passed"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
