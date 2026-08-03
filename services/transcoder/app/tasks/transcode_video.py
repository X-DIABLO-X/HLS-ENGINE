"""Transcode video renditions (GPU-grouped single-pass, chunked, per-rendition fallback)."""

import logging
import math
import os
import re
import shutil
import socket
import time
import uuid
from contextlib import ExitStack
from urllib.parse import unquote, urlsplit

from celery import chord, group

from app.celery_app import celery_app
from app.config import get_settings
from app.db import SessionLocal
from app import ffmpeg_utils, gpu_registry, models
from app.job_fencing import (
    StaleJobError,
    advance_job_status,
    ensure_local_source,
    lock_current_job,
    stale_result,
)
from app.job_lock import job_lock
from app.rabbitmq import publish_event
from app import progress as progress_tracker
from app.transcode_safety import (
    CPU_FALLBACK_DEFAULT_SLICE_SEC,
    CPU_FALLBACK_MAX_SLICE_SEC,
    bounded_cpu_plan,
    validate_bounded_chunk_codecs,
)

logger = logging.getLogger(__name__)

CPU_VIDEO_QUEUE = "video_cpu"
PACKAGE_QUEUE = "package"
DIRECT_PLAY_PENDING = "direct_play_pending"
DIRECT_PLAY_FALLBACK = "direct_play_fallback"
DIRECT_PLAY_COMPLETE = "direct_play"


class BoundedCPUFallbackRequired(RuntimeError):
    """Signal that the current GPU delivery must be replaced, not continued."""

    def __init__(
        self,
        reason: str,
        duration: float,
        video_id: str | None = None,
    ):
        super().__init__(reason)
        self.duration = float(duration)
        self.video_id = str(video_id) if video_id is not None else None


class DirectPlayFallbackRequired(RuntimeError):
    """Replace one failed direct remux with the normal encoded ladder."""

    def __init__(
        self,
        reason: str,
        video_id: str,
        source_height: int,
    ):
        super().__init__(reason)
        self.video_id = str(video_id)
        self.source_height = int(source_height)


def _worker_id() -> str:
    """Stable worker identifier for the GPU registry (env override or hostname)."""
    try:
        cfg = get_settings()
        wid = getattr(cfg, "GPU_WORKER_ID", "") or ""
        if wid:
            return wid
    except Exception:
        pass
    return socket.gethostname() or "worker"


def _seg_ext(segment_format: str) -> str:
    return "m4s" if (segment_format or "fmp4").lower() == "fmp4" else "ts"


def _file_size(path: str) -> int:
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


def _dir_size(directory: str) -> int:
    """Return regular-file bytes below a directory without following links."""
    total = 0
    try:
        for root, dirnames, filenames in os.walk(
            directory,
            topdown=True,
            followlinks=False,
        ):
            dirnames[:] = [
                name
                for name in dirnames
                if not os.path.islink(os.path.join(root, name))
            ]
            for name in filenames:
                path = os.path.join(root, name)
                if not os.path.islink(path):
                    total += _file_size(path)
    except OSError:
        pass
    return total


def _effective_fps(
    source_fps: float,
    processed_duration_sec: float,
    wall_duration_sec: float,
) -> float:
    """Convert processed source time and elapsed wall time to throughput FPS."""
    try:
        source_fps = float(source_fps)
        processed_duration_sec = float(processed_duration_sec)
        wall_duration_sec = float(wall_duration_sec)
    except (TypeError, ValueError):
        return 0.0
    if (
        not math.isfinite(source_fps)
        or not math.isfinite(processed_duration_sec)
        or not math.isfinite(wall_duration_sec)
        or source_fps <= 0
        or processed_duration_sec <= 0
        or wall_duration_sec <= 0
    ):
        return 0.0
    return (source_fps * processed_duration_sec) / wall_duration_sec


def _record_metric(
    db,
    job_id: str,
    rendition: str,
    codec: str,
    gpu_index,
    worker_id: str,
    fps: float,
    encode_duration_sec: float,
    input_bytes: int,
    output_bytes: int,
    complexity_score,
    commit: bool = True,
) -> None:
    """Persist an EncodingMetric row, best-effort (never let it break the task)."""
    try:
        metric = models.EncodingMetric(
            id=str(uuid.uuid4()),
            job_id=job_id,
            rendition=rendition,
            codec=codec,
            gpu_index=gpu_index,
            gpu_worker_id=worker_id,
            fps=fps or 0.0,
            encode_duration_sec=encode_duration_sec or 0.0,
            input_bytes=int(input_bytes or 0),
            output_bytes=int(output_bytes or 0),
            complexity_score=complexity_score,
        )
        db.add(metric)
        if commit:
            db.commit()
    except Exception as exc:
        logger.warning("failed to record EncodingMetric: %s", exc)


def _run_transcode_video(
    self,
    job_id: str,
    source_url: str,
    rendition,
    settings: dict = None,
) -> dict:
    settings = settings or {}
    seg_dur = settings.get("segment_duration_sec", ffmpeg_utils.SEGMENT_DURATION)
    seg_fmt = settings.get("segment_format", "fmp4")
    preset = settings.get("video_preset", "medium")
    force_cpu = settings.get("force_cpu", False)
    codec = settings.get("codec", "h264")
    nvenc_profile = settings.get("nvenc_profile")
    video_id = None
    task_name = None
    attempt_root = None
    progress_started = False

    db = SessionLocal()
    try:
        job, video = lock_current_job(db, job_id, allow_completed=True)
        video_id = str(job.video_id)

        normalized = _normalize_renditions([rendition], video, codec)
        spec = normalized[0]
        height = spec["height"]
        width = spec["width"]
        bitrate = spec["bitrate"]
        duration = (
            float(settings.get("_source_video_duration") or 0.0)
            or video.duration
            or 1.0
        )

        task_name = f"transcode_{height}p"
        is_direct_original = bool(
            spec.get("is_original") and spec.get("direct_play")
        )
        display_name = "Original" if spec.get("is_original") else f"{height}p"

        work_dir = os.path.join(get_settings().WORK_DIR, job_id)
        canonical_output = os.path.join(work_dir, "output")
        current_status = str(job.status)
        if current_status == models.JobStatus.failed.value:
            raise RuntimeError(
                f"refusing stale transcode delivery for failed job {job_id}"
            )
        completed_details = _completed_rendition_results(
            db,
            job,
            video,
            canonical_output,
            normalized,
            seg_fmt,
        )
        if completed_details is not None:
            return {
                **completed_details[height],
                "reused": True,
                "durable": True,
            }

        canonical_reusable = _canonical_renditions_reusable(
            db,
            video_id,
            canonical_output,
            normalized,
            seg_fmt,
            duration,
        )
        if current_status in _ADVANCED_JOB_STATES and not canonical_reusable:
            raise RuntimeError(
                f"refusing stale transcode delivery for job {job_id} "
                f"in state {current_status}"
            )
        if canonical_reusable:
            if current_status not in _ADVANCED_JOB_STATES:
                progress_tracker.start_task(
                    video_id,
                    task_name,
                    f"Preparing {display_name} video",
                    job_id=job_id,
                )
                progress_started = True
            _rows, details = _prepare_rendition_rows(
                db,
                video_id,
                canonical_output,
                normalized,
                seg_fmt,
                duration,
            )
            db.commit()
            result = details[height]
            if current_status not in _ADVANCED_JOB_STATES:
                progress_tracker.complete_task(
                    video_id,
                    task_name,
                    job_id=job_id,
                )
                publish_event(
                    "video.rendition.completed",
                    {
                        "job_id": job_id,
                        "video_id": video_id,
                        "height": height,
                        "bandwidth": result["bandwidth"],
                    },
                )
            return {**result, "reused": True}

        os.makedirs(canonical_output, exist_ok=True)
        progress_tracker.start_task(
            video_id,
            task_name,
            f"Preparing {display_name} video",
            job_id=job_id,
        )
        progress_started = True
        advance_job_status(job, models.JobStatus.transcoding.value)
        db.commit()

        local_source = ensure_local_source(job_id, source_url)

        fps = video.frame_rate or 30.0
        use_gpu = ffmpeg_utils.is_gpu_available() and not force_cpu

        def on_progress(pct):
            progress_tracker.update_task(
                video_id,
                task_name,
                pct,
                f"Preparing {display_name} video",
                job_id=job_id,
            )

        attempt_root, attempt_output = _new_attempt_output(
            work_dir,
            _delivery_id(self),
        )
        attempt_rendition = os.path.join(
            attempt_output,
            f"video_{height}p",
        )
        def encode_original(force_gpu: bool) -> None:
            cmd = ffmpeg_utils.transcode_video_command(
                local_source, attempt_rendition,
                width, height, bitrate, fps,
                force_gpu=force_gpu,
                segment_duration=seg_dur,
                preset=preset,
                codec=spec["codec"],
                segment_format=seg_fmt,
                nvenc_profile=nvenc_profile,
            )
            ffmpeg_utils.run_cmd_with_progress(cmd, duration, on_progress)

        if is_direct_original:
            try:
                cmd = ffmpeg_utils.remux_h264_hls_command(
                    local_source,
                    attempt_rendition,
                    segment_duration=seg_dur,
                    segment_format=seg_fmt,
                )
                ffmpeg_utils.run_cmd_with_progress(cmd, duration, on_progress)
                _validate_direct_play_output(
                    attempt_rendition,
                    spec,
                    seg_fmt,
                    duration,
                    fps,
                    seg_dur,
                )
            except Exception as exc:
                # Original remains mandatory. A remux issue (for example a
                # sparse keyframe layout) falls back to a source-size H.264
                # encode instead of removing Original from the ladder.
                logger.warning(
                    "Original remux failed for job=%s; encoding source-size "
                    "fallback: %s",
                    job_id,
                    exc,
                )
                _remove_rendition_directories(attempt_output, [height])
                spec["direct_play"] = False
                spec.pop("profile", None)
                spec.pop("level", None)
                try:
                    encode_original(use_gpu)
                except ffmpeg_utils.FFmpegError as encode_exc:
                    if not use_gpu:
                        raise
                    logger.warning(
                        "Original GPU fallback failed for job=%s; using CPU: %s",
                        job_id,
                        encode_exc,
                    )
                    _remove_rendition_directories(attempt_output, [height])
                    encode_original(False)
        else:
            try:
                encode_original(use_gpu)
            except ffmpeg_utils.FFmpegError as exc:
                if not use_gpu:
                    raise
                logger.warning(
                    "GPU transcode failed for job=%s, falling back to CPU: %s",
                    job_id, exc,
                )
                _remove_rendition_directories(attempt_output, [height])
                encode_original(False)

        _validate_rendition_set(
            attempt_output,
            normalized,
            seg_fmt,
            duration,
        )
        job, video = lock_current_job(db, job_id, allow_completed=True)
        current_status = str(job.status)
        if current_status in _ADVANCED_JOB_STATES:
            if not _canonical_renditions_reusable(
                db,
                video_id,
                canonical_output,
                normalized,
                seg_fmt,
                duration,
            ):
                raise RuntimeError(
                    f"job {job_id} advanced to {current_status}; "
                    "attempt output will not replace canonical rendition"
                )
        else:
            _promote_rendition_directories(
                attempt_output,
                canonical_output,
                normalized,
                seg_fmt,
                duration,
            )

        _rows, details = _prepare_rendition_rows(
            db,
            video_id,
            canonical_output,
            normalized,
            seg_fmt,
            duration,
        )
        db.commit()

        result = details[height]
        if current_status not in _ADVANCED_JOB_STATES:
            progress_tracker.complete_task(
                video_id,
                task_name,
                job_id=job_id,
            )
            publish_event(
                "video.rendition.completed",
                {
                    "job_id": job_id,
                    "video_id": video_id,
                    "height": height,
                    "bandwidth": result["bandwidth"],
                },
            )
        return {
            **result,
            "reused": current_status in _ADVANCED_JOB_STATES,
        }
    except StaleJobError:
        _rollback_quietly(db)
        raise
    except Exception as exc:
        _rollback_quietly(db)
        if progress_started and video_id and task_name:
            progress_tracker.fail_task(
                video_id,
                task_name,
                str(exc),
                job_id=job_id,
            )
        raise
    finally:
        if attempt_root:
            _cleanup_attempt_root(attempt_root)
        db.close()


@celery_app.task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    max_retries=3,
)
def transcode_video(
    self,
    job_id: str,
    source_url: str,
    rendition,
    settings: dict = None,
) -> dict:
    """Serialize duplicate legacy rungs while allowing different rungs in parallel."""
    height = (
        int(rendition)
        if isinstance(rendition, int)
        else int(rendition["height"])
    )
    work_root = get_settings().WORK_DIR
    # A shared job lock lets different legacy rendition heights encode at the
    # same time on Linux, while the package task's exclusive job lock waits for
    # all of them.  The rendition lock serializes duplicate delivery of one rung.
    with job_lock(
        work_root,
        job_id,
        purpose=f"transcode_video:{height}p:job",
        shared=True,
    ):
        with job_lock(
            work_root,
            f"{job_id}:rendition:{height}",
            purpose=f"transcode_video:{height}p",
        ):
            try:
                return _run_transcode_video(
                    self,
                    job_id,
                    source_url,
                    rendition,
                    settings,
                )
            except StaleJobError as exc:
                logger.info(
                    "[transcode_video] skipping stale job=%s height=%s: %s",
                    job_id,
                    height,
                    exc,
                )
                return stale_result(job_id, "video")


# ---- Helpers for the grouped / chunked transcoding tasks ----

_NUM_RE = re.compile(r"(\d+)")


def _natural_key(name: str):
    return [int(t) if t.isdigit() else t for t in _NUM_RE.split(name)]


def _normalize_renditions(renditions, video, codec: str) -> list:
    """Coerce a list of rendition specs (int height or dict) into full dicts."""
    norm = []
    for r in (renditions or []):
        if isinstance(r, int):
            ladder = ffmpeg_utils.get_ladder(video.height or r, video.width or 1920)
            rung = next((x for x in ladder if x["height"] == r), ladder[-1])
            norm.append({
                "height": rung["height"], "width": rung["width"],
                "bitrate": rung["bitrate"], "codec": codec,
            })
        else:
            h = r["height"]
            w = r.get("width") or ffmpeg_utils.width_for_height(
                video.width or 1920, video.height or h, h
            )
            normalized = {
                "height": h, "width": w,
                "bitrate": r["bitrate"], "codec": r.get("codec", codec),
            }
            for key in ("name", "is_original", "direct_play", "profile", "level"):
                if key in r:
                    normalized[key] = r[key]
            norm.append(normalized)
    return norm


def _abspath(p: str) -> str:
    return os.path.abspath(p).replace("\\", "/")


_HLS_MAP_URI_RE = re.compile(r'URI=(?:"([^"]+)"|([^,]+))')
_HLS_EXTINF_RE = re.compile(r"^#EXTINF:([0-9]+(?:\.[0-9]+)?)")
_MEDIA_SEGMENT_EXTENSIONS = {".m4s", ".ts"}
_ADVANCED_JOB_STATES = {
    models.JobStatus.packaging.value,
    models.JobStatus.publishing.value,
    models.JobStatus.completed.value,
}


class RenditionValidationError(ValueError):
    """Raised when a rendition directory is not a complete VOD presentation."""


def _same_path(left: str, right: str) -> bool:
    if not left or not right:
        return False
    return os.path.normcase(os.path.abspath(left)) == os.path.normcase(
        os.path.abspath(right)
    )


def _local_hls_reference(rendition_dir: str, uri: str) -> str:
    """Resolve one local HLS URI without allowing URL or directory escape."""
    parsed = urlsplit(uri.strip())
    if parsed.scheme or parsed.netloc:
        raise RenditionValidationError(
            f"rendition references a non-local asset: {uri}"
        )

    decoded = unquote(parsed.path)
    if not decoded or os.path.isabs(decoded):
        raise RenditionValidationError(f"invalid rendition asset URI: {uri}")

    root = os.path.abspath(rendition_dir)
    target = os.path.abspath(
        os.path.join(root, decoded.replace("/", os.sep))
    )
    try:
        if os.path.commonpath((root, target)) != root:
            raise RenditionValidationError(
                f"rendition asset escapes its directory: {uri}"
            )
    except ValueError as exc:
        raise RenditionValidationError(
            f"invalid rendition asset path: {uri}"
        ) from exc
    return target


def _validate_rendition_directory(
    rendition_dir: str,
    expected_segment_ext: str = None,
    expected_duration: float = None,
) -> dict:
    """Validate a complete media playlist and every asset it references.

    In addition to requiring ENDLIST and non-empty referenced segments, reject
    unreferenced media segments.  That latter check prevents stale, higher
    numbered files from an older attempt being swept up by the packager.
    """
    playlist_path = os.path.join(rendition_dir, "video.m3u8")
    try:
        with open(playlist_path, "r", encoding="utf-8-sig") as playlist:
            lines = [line.strip() for line in playlist if line.strip()]
    except OSError as exc:
        raise RenditionValidationError(
            f"missing or unreadable rendition playlist: {playlist_path}"
        ) from exc

    if not lines or lines[0] != "#EXTM3U":
        raise RenditionValidationError(
            f"invalid rendition playlist header: {playlist_path}"
        )
    if "#EXT-X-ENDLIST" not in lines:
        raise RenditionValidationError(
            f"incomplete rendition playlist (ENDLIST missing): {playlist_path}"
        )

    segment_uris = [line for line in lines if not line.startswith("#")]
    extinf_matches = [
        _HLS_EXTINF_RE.match(line)
        for line in lines
        if line.startswith("#EXTINF:")
    ]
    if (
        not segment_uris
        or len(extinf_matches) != len(segment_uris)
        or any(match is None for match in extinf_matches)
    ):
        raise RenditionValidationError(
            f"rendition playlist has no complete segment list: {playlist_path}"
        )
    playlist_duration = sum(
        float(match.group(1)) for match in extinf_matches
    )
    if expected_duration and float(expected_duration) > 0:
        expected_duration = float(expected_duration)
        allowed_shortfall = max(
            1.0,
            min(5.0, expected_duration * 0.001),
        )
        if playlist_duration < expected_duration - allowed_shortfall:
            raise RenditionValidationError(
                "rendition playlist duration is incomplete: "
                f"{playlist_duration:.3f}s of {expected_duration:.3f}s"
            )
        # A complete VOD can differ by a few frames, but accepting a whole
        # extra HLS segment can conceal a duplicated/replayed chunk.
        allowed_excess = max(
            1.0,
            min(5.0, expected_duration * 0.001),
        )
        if playlist_duration > expected_duration + allowed_excess:
            raise RenditionValidationError(
                "rendition playlist duration exceeds the source: "
                f"{playlist_duration:.3f}s for {expected_duration:.3f}s"
            )

    expected_ext = (
        expected_segment_ext.lower()
        if expected_segment_ext
        else None
    )
    segment_paths = []
    for uri in segment_uris:
        segment_path = _local_hls_reference(rendition_dir, uri)
        if expected_ext and os.path.splitext(segment_path)[1].lower() != expected_ext:
            raise RenditionValidationError(
                f"unexpected segment type in rendition playlist: {uri}"
            )
        if not os.path.isfile(segment_path) or _file_size(segment_path) <= 0:
            raise RenditionValidationError(
                f"missing or empty rendition segment: {uri}"
            )
        segment_paths.append(segment_path)

    map_paths = []
    for line in lines:
        if not line.startswith("#EXT-X-MAP:"):
            continue
        match = _HLS_MAP_URI_RE.search(line)
        if not match:
            raise RenditionValidationError(
                f"invalid EXT-X-MAP in rendition playlist: {playlist_path}"
            )
        map_uri = match.group(1) or match.group(2)
        map_path = _local_hls_reference(rendition_dir, map_uri)
        if not os.path.isfile(map_path) or _file_size(map_path) <= 0:
            raise RenditionValidationError(
                f"missing or empty rendition init segment: {map_uri}"
            )
        map_paths.append(map_path)
    if expected_ext == ".m4s" and not map_paths:
        raise RenditionValidationError(
            f"fMP4 rendition has no EXT-X-MAP: {playlist_path}"
        )

    referenced_segments = {
        os.path.normcase(os.path.abspath(path)) for path in segment_paths
    }
    for root, _dirs, files in os.walk(rendition_dir):
        for filename in files:
            candidate = os.path.join(root, filename)
            if os.path.splitext(filename)[1].lower() not in _MEDIA_SEGMENT_EXTENSIONS:
                continue
            if os.path.normcase(os.path.abspath(candidate)) not in referenced_segments:
                raise RenditionValidationError(
                    f"unreferenced stale media segment: {candidate}"
                )

    return {
        "playlist_path": _abspath(playlist_path),
        "segment_paths": [_abspath(path) for path in segment_paths],
        "map_paths": [_abspath(path) for path in map_paths],
        "duration_sec": playlist_duration,
    }


def _validate_rendition_set(
    output_base: str,
    renditions: list,
    segment_format: str,
    expected_duration: float = None,
) -> dict:
    """Validate every expected rendition before any state is committed."""
    ext = f".{_seg_ext(segment_format)}"
    validated = {}
    seen = set()
    for rendition in renditions:
        height = int(rendition["height"])
        if height in seen:
            raise RenditionValidationError(
                f"duplicate rendition height requested: {height}"
            )
        seen.add(height)
        rendition_dir = os.path.join(output_base, f"video_{height}p")
        validated[height] = _validate_rendition_directory(
            rendition_dir,
            expected_segment_ext=ext,
            expected_duration=expected_duration,
        )
    if not validated:
        raise RenditionValidationError("no rendition outputs to validate")
    return validated


def _validate_direct_play_output(
    rendition_dir: str,
    rendition: dict,
    segment_format: str,
    expected_duration: float,
    frame_rate: float,
    target_segment_duration: int = ffmpeg_utils.SEGMENT_DURATION,
) -> dict:
    """Validate copied media, timeline, keyframes, and HLS completeness."""
    ext = f".{_seg_ext(segment_format)}"
    validated = _validate_rendition_directory(
        rendition_dir,
        expected_segment_ext=ext,
        expected_duration=expected_duration,
    )
    playlist_path = validated["playlist_path"]
    try:
        with open(playlist_path, "r", encoding="utf-8-sig") as playlist:
            manifest_lines = [
                line.strip() for line in playlist if line.strip()
            ]
    except OSError as exc:
        raise RenditionValidationError(
            f"direct-play playlist is unreadable: {playlist_path}"
        ) from exc
    if "#EXT-X-INDEPENDENT-SEGMENTS" not in manifest_lines:
        raise RenditionValidationError(
            "direct-play playlist does not declare independent segments"
        )
    if "#EXT-X-PLAYLIST-TYPE:VOD" not in manifest_lines:
        raise RenditionValidationError(
            "direct-play playlist is not a VOD presentation"
        )
    extinf_durations = [
        float(match.group(1))
        for line in manifest_lines
        for match in [_HLS_EXTINF_RE.match(line)]
        if match is not None
    ]
    target_lines = [
        line for line in manifest_lines
        if line.startswith("#EXT-X-TARGETDURATION:")
    ]
    try:
        declared_target = int(target_lines[0].split(":", 1)[1])
    except (IndexError, TypeError, ValueError) as exc:
        raise RenditionValidationError(
            "direct-play playlist has no valid target duration"
        ) from exc
    if not extinf_durations or declared_target < math.ceil(
        max(extinf_durations)
    ):
        raise RenditionValidationError(
            "direct-play playlist target duration is invalid"
        )
    expected_segment = max(1.0, float(target_segment_duration))
    if max(extinf_durations) > max(
        expected_segment * 2.0,
        expected_segment + 2.0,
    ):
        raise RenditionValidationError(
            "direct-play source keyframes produce oversized HLS segments"
        )

    try:
        probe = ffmpeg_utils.ffprobe(playlist_path)
    except Exception as exc:
        raise RenditionValidationError(
            f"could not probe direct-play playlist: {exc}"
        ) from exc
    video_stream = next(
        (
            stream
            for stream in probe.get("streams", [])
            if stream.get("codec_type") == "video"
        ),
        None,
    )
    if not video_stream:
        raise RenditionValidationError(
            "direct-play playlist has no video stream"
        )
    try:
        actual_width = int(video_stream.get("width"))
        actual_height = int(video_stream.get("height"))
    except (TypeError, ValueError) as exc:
        raise RenditionValidationError(
            "direct-play playlist has invalid video dimensions"
        ) from exc
    if str(video_stream.get("codec_name") or "").lower() != "h264":
        raise RenditionValidationError(
            "direct-play playlist is not H.264"
        )
    actual_profile = str(video_stream.get("profile") or "").strip().lower()
    if actual_profile not in ffmpeg_utils._H264_DIRECT_PLAY_PROFILES:
        raise RenditionValidationError(
            "direct-play playlist has an unsupported H.264 profile"
        )
    expected_profile = str(rendition.get("profile") or "").strip().lower()
    if expected_profile and actual_profile != expected_profile:
        raise RenditionValidationError(
            "direct-play H.264 profile changed during remux"
        )
    try:
        actual_level = int(video_stream.get("level"))
    except (TypeError, ValueError) as exc:
        raise RenditionValidationError(
            "direct-play playlist has no valid H.264 level"
        ) from exc
    if not 10 <= actual_level <= 52:
        raise RenditionValidationError(
            "direct-play playlist has an unsupported H.264 level"
        )
    expected_level = rendition.get("level")
    if expected_level is not None:
        try:
            expected_level = int(expected_level)
        except (TypeError, ValueError) as exc:
            raise RenditionValidationError(
                "direct-play rendition has no valid expected H.264 level"
            ) from exc
        if actual_level != expected_level:
            raise RenditionValidationError(
                "direct-play H.264 level changed during remux"
            )
    if str(video_stream.get("pix_fmt") or "").lower() != "yuv420p":
        raise RenditionValidationError(
            "direct-play playlist is not 8-bit yuv420p"
        )
    if str(video_stream.get("field_order") or "").lower() != "progressive":
        raise RenditionValidationError(
            "direct-play playlist is not progressive"
        )
    if str(video_stream.get("sample_aspect_ratio") or "") != "1:1":
        raise RenditionValidationError(
            "direct-play playlist does not have square pixels"
        )
    if (
        actual_width != int(rendition["width"])
        or actual_height != int(rendition["height"])
    ):
        raise RenditionValidationError(
            "direct-play playlist dimensions changed during remux"
        )

    try:
        probed_duration = float(probe.get("format", {}).get("duration"))
    except (TypeError, ValueError) as exc:
        raise RenditionValidationError(
            "direct-play playlist duration is missing"
        ) from exc
    duration = float(expected_duration)
    duration_tolerance = max(2.0, min(30.0, duration * 0.01))
    if (
        not math.isfinite(probed_duration)
        or abs(probed_duration - duration) > duration_tolerance
    ):
        raise RenditionValidationError(
            "direct-play probed duration does not match the source: "
            f"{probed_duration:.3f}s vs {duration:.3f}s"
        )

    try:
        packets = ffmpeg_utils.ffprobe_video_packets(playlist_path)
    except Exception as exc:
        raise RenditionValidationError(
            f"could not inspect direct-play packet timestamps: {exc}"
        ) from exc
    if not packets:
        raise RenditionValidationError(
            "direct-play playlist contains no video packets"
        )
    if "K" not in str(packets[0].get("flags") or ""):
        raise RenditionValidationError(
            "direct-play playlist does not begin with a keyframe"
        )

    timestamps = []
    previous_dts = None
    keyframe_count = 0
    for packet in packets:
        try:
            pts = float(packet.get("pts_time"))
            dts = float(packet.get("dts_time"))
        except (TypeError, ValueError) as exc:
            raise RenditionValidationError(
                "direct-play packet is missing PTS/DTS"
            ) from exc
        if (
            not math.isfinite(pts)
            or not math.isfinite(dts)
            or pts < -0.001
            or dts < -0.001
        ):
            raise RenditionValidationError(
                "direct-play packet timeline contains invalid timestamps"
            )
        if previous_dts is not None and dts + 0.000001 < previous_dts:
            raise RenditionValidationError(
                "direct-play packet DTS is not monotonic"
            )
        previous_dts = dts
        timestamps.extend((pts, dts))
        if "K" in str(packet.get("flags") or ""):
            keyframe_count += 1

    if keyframe_count < len(validated["segment_paths"]):
        raise RenditionValidationError(
            "direct-play playlist has fewer keyframes than HLS segments"
        )
    start_tolerance = max(0.1, 2.0 / max(1.0, float(frame_rate)))
    if min(timestamps) > start_tolerance:
        raise RenditionValidationError(
            "direct-play packet timeline was not rebased near zero"
        )
    return validated


def _new_attempt_output(work_dir: str, delivery_id: str = None) -> tuple:
    """Create an output tree that no other Celery delivery can write into."""
    safe_delivery = re.sub(
        r"[^A-Za-z0-9_.-]+",
        "-",
        str(delivery_id or "delivery"),
    )[:80]
    attempt_root = os.path.join(
        work_dir,
        ".transcode-attempts",
        f"{safe_delivery}-{uuid.uuid4().hex}",
    )
    output_base = os.path.join(attempt_root, "output")
    os.makedirs(output_base, exist_ok=False)
    return attempt_root, output_base


def _remove_rendition_directories(output_base: str, heights) -> None:
    """Remove only the named attempt renditions before a fallback encode."""
    for height in heights:
        rendition_dir = os.path.join(
            output_base,
            f"video_{int(height)}p",
        )
        shutil.rmtree(
            rendition_dir,
            ignore_errors=True,
        )
        if os.path.lexists(rendition_dir):
            raise ffmpeg_utils.FFmpegError(
                "could not remove partial rendition before GPU recovery: "
                f"{rendition_dir}"
            )


def _cleanup_attempt_root(attempt_root: str) -> None:
    """Remove an attempt unless it still holds canonical rollback data."""
    backup_root = os.path.join(attempt_root, "replaced-canonical")
    try:
        if os.path.isdir(backup_root):
            with os.scandir(backup_root) as entries:
                has_backups = next(entries, None) is not None
        else:
            has_backups = False
    except OSError:
        has_backups = True
    if has_backups:
        logger.error(
            "preserving transcode attempt with unrecovered canonical backups: %s",
            attempt_root,
        )
        return
    shutil.rmtree(attempt_root, ignore_errors=True)


def _promote_rendition_directories(
    attempt_output: str,
    canonical_output: str,
    renditions: list,
    segment_format: str,
    expected_duration: float = None,
) -> dict:
    """Promote only a fully validated rendition set via directory renames.

    Each destination directory changes from one complete tree to another in a
    rename operation.  Backups live outside ``canonical_output`` so a packager
    can never discover them.
    """
    _validate_rendition_set(
        attempt_output,
        renditions,
        segment_format,
        expected_duration,
    )
    os.makedirs(canonical_output, exist_ok=True)
    backup_root = os.path.join(
        os.path.dirname(attempt_output),
        "replaced-canonical",
    )
    os.makedirs(backup_root, exist_ok=True)

    promoted = []
    backups = {}
    try:
        for rendition in renditions:
            height = int(rendition["height"])
            name = f"video_{height}p"
            source = os.path.join(attempt_output, name)
            destination = os.path.join(canonical_output, name)
            backup = os.path.join(backup_root, name)
            if os.path.lexists(destination):
                os.replace(destination, backup)
                backups[height] = backup
            try:
                os.replace(source, destination)
            except Exception:
                if height in backups and not os.path.lexists(destination):
                    os.replace(backups[height], destination)
                    backups.pop(height, None)
                raise
            promoted.append(height)

        validated = _validate_rendition_set(
            canonical_output,
            renditions,
            segment_format,
            expected_duration,
        )
    except Exception:
        for height in reversed(promoted):
            name = f"video_{height}p"
            destination = os.path.join(canonical_output, name)
            source = os.path.join(attempt_output, name)
            if os.path.lexists(destination):
                os.replace(destination, source)
            backup = backups.get(height)
            if backup and os.path.lexists(backup):
                os.replace(backup, destination)
        raise

    for backup in backups.values():
        shutil.rmtree(backup, ignore_errors=True)
    shutil.rmtree(backup_root, ignore_errors=True)
    return validated


def _load_rendition_rows(db, video_id: str, heights) -> list:
    heights = [int(height) for height in heights]
    return (
        db.query(models.Rendition)
        .filter(
            models.Rendition.video_id == video_id,
            models.Rendition.height.in_(heights),
        )
        .all()
    )


def _row_matches_rendition(
    row,
    rendition: dict,
    canonical_output: str,
    segment_format: str,
) -> bool:
    height = int(rendition["height"])
    rendition_dir = os.path.join(canonical_output, f"video_{height}p")
    try:
        scalar_match = (
            int(row.height) == height
            and int(row.width) == int(rendition["width"])
            and int(row.video_bitrate) == int(rendition["bitrate"])
            and str(row.codec or "").lower()
            == str(rendition["codec"] or "").lower()
            and str(row.profile or "").lower()
            == _persisted_rendition_profile(rendition).lower()
            and bool(getattr(row, "is_original", False))
            == bool(rendition.get("is_original", False))
        )
    except (TypeError, ValueError):
        return False
    return (
        scalar_match
        and _same_path(
            row.playlist_path,
            os.path.join(rendition_dir, "video.m3u8"),
        )
        and _same_path(
            row.segment_path,
            os.path.join(
                rendition_dir,
                f"%05d.{_seg_ext(segment_format)}",
            ),
        )
    )


def _completed_rendition_results(
    db,
    job,
    video,
    canonical_output: str,
    renditions: list,
    segment_format: str,
):
    """Return durable DB metadata for a delivery arriving after local cleanup."""
    if str(job.status) != models.JobStatus.completed.value:
        return None
    if getattr(video, "status", None) != "ready" or not getattr(
        job,
        "output_prefix",
        None,
    ):
        raise RuntimeError(
            "completed transcode delivery has inconsistent durable state"
        )

    rows = _load_rendition_rows(
        db,
        job.video_id,
        [rendition["height"] for rendition in renditions],
    )
    by_height = {}
    for row in rows:
        by_height.setdefault(int(row.height), []).append(row)

    details = {}
    for rendition in renditions:
        height = int(rendition["height"])
        candidates = [
            row
            for row in by_height.get(height, [])
            if _row_matches_rendition(
                row,
                rendition,
                canonical_output,
                segment_format,
            )
        ]
        if not candidates:
            raise RuntimeError(
                f"completed job has no matching persisted {height}p rendition"
            )
        deterministic_id = _deterministic_rendition_id(
            job.video_id,
            height,
        )
        row = min(
            candidates,
            key=lambda candidate: (
                str(getattr(candidate, "id", "")) != deterministic_id,
                str(getattr(candidate, "id", "")),
            ),
        )
        details[height] = {
            "name": getattr(row, "name", None) or f"{height}p",
            "is_original": bool(getattr(row, "is_original", False)),
            "height": height,
            "width": rendition["width"],
            "bandwidth": row.bandwidth or 1,
            "playlist": row.playlist_path,
        }
    return details


def _canonical_renditions_reusable(
    db,
    video_id: str,
    canonical_output: str,
    renditions: list,
    segment_format: str,
    expected_duration: float = None,
) -> bool:
    """Return true only when files and persisted encoding specs both match."""
    try:
        _validate_rendition_set(
            canonical_output,
            renditions,
            segment_format,
            expected_duration,
        )
    except RenditionValidationError:
        return False

    rows = _load_rendition_rows(
        db,
        video_id,
        [rendition["height"] for rendition in renditions],
    )
    by_height = {}
    for row in rows:
        by_height.setdefault(int(row.height), []).append(row)
    return all(
        any(
            _row_matches_rendition(
                row,
                rendition,
                canonical_output,
                segment_format,
            )
            for row in by_height.get(int(rendition["height"]), [])
        )
        for rendition in renditions
    )


def _deterministic_rendition_id(video_id: str, height: int) -> str:
    return str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"hls-engine:rendition:{video_id}:{int(height)}",
        )
    )


def _persisted_rendition_profile(rendition: dict) -> str:
    """Keep encoded metadata stable and preserve direct source AVC metadata."""
    if (
        str(rendition.get("codec") or "").strip().lower() == "h264"
        and bool(rendition.get("direct_play"))
        and rendition.get("profile") is not None
        and rendition.get("level") is not None
    ):
        return ffmpeg_utils.direct_h264_profile_metadata(
            rendition["profile"],
            rendition["level"],
        )
    return "high"


def _prepare_rendition_rows(
    db,
    video_id: str,
    canonical_output: str,
    renditions: list,
    segment_format: str,
    duration: float,
) -> tuple:
    """Validate all files, then stage a deterministic de-duplicated upsert.

    This helper deliberately does not commit.  The caller can add metrics and
    then commit every rendition row in one database transaction.
    """
    validated = _validate_rendition_set(
        canonical_output,
        renditions,
        segment_format,
        duration,
    )
    heights = [int(rendition["height"]) for rendition in renditions]
    existing = _load_rendition_rows(db, video_id, heights)
    by_height = {}
    for row in existing:
        by_height.setdefault(int(row.height), []).append(row)

    rows = []
    details = {}
    for rendition in renditions:
        height = int(rendition["height"])
        candidates = sorted(
            by_height.get(height, []),
            key=lambda row: str(getattr(row, "id", "")),
        )
        deterministic_id = _deterministic_rendition_id(video_id, height)
        row = next(
            (
                candidate
                for candidate in candidates
                if str(getattr(candidate, "id", "")) == deterministic_id
            ),
            None,
        )
        if row is None:
            row = models.Rendition(id=deterministic_id, video_id=video_id)
            db.add(row)
        for duplicate in candidates:
            if duplicate is not row:
                db.delete(duplicate)

        rendition_dir = os.path.join(canonical_output, f"video_{height}p")
        bandwidth = ffmpeg_utils.compute_directory_bitrate(
            rendition_dir,
            duration,
        )
        row.video_id = video_id
        row.name = str(rendition.get("name") or f"{height}p")
        row.is_original = bool(rendition.get("is_original", False))
        row.height = height
        row.width = rendition["width"]
        row.video_bitrate = rendition["bitrate"]
        row.audio_bitrate = 0
        row.codec = rendition["codec"]
        row.profile = _persisted_rendition_profile(rendition)
        row.segment_path = _abspath(
            os.path.join(
                rendition_dir,
                f"%05d.{_seg_ext(segment_format)}",
            )
        )
        row.playlist_path = validated[height]["playlist_path"]
        row.bandwidth = bandwidth
        rows.append(row)
        details[height] = {
            "name": row.name,
            "is_original": row.is_original,
            "height": height,
            "width": rendition["width"],
            "bandwidth": bandwidth,
            "playlist": row.playlist_path,
        }
    return rows, details


def _delivery_id(task) -> str:
    request = getattr(task, "request", None)
    return str(getattr(request, "id", None) or "delivery")


def _chunk_group_key(renditions: list) -> str:
    """Stable path/lock identity for one capacity-bounded chunk group."""
    heights = sorted(
        {
            int(rendition)
            if isinstance(rendition, int)
            else int(rendition["height"])
            for rendition in (renditions or [])
        }
    )
    return "-".join(str(height) for height in heights) or "empty"


def _rendition_height(rendition) -> int:
    return int(
        rendition
        if isinstance(rendition, int)
        else rendition.get("height", 0)
    )


def _rendition_progress_tasks(renditions: list) -> list[str]:
    return [
        f"transcode_{height}p"
        for height in sorted(
            {_rendition_height(rendition) for rendition in renditions}
        )
    ]


def _configure_chunk_progress(
    video_id: str,
    job_id: str,
    settings: dict,
    renditions: list,
) -> None:
    for task_name in _rendition_progress_tasks(renditions):
        total = progress_tracker.task_chunk_count(settings, task_name)
        if total:
            progress_tracker.configure_chunked_task(
                video_id,
                task_name,
                total,
                stage=f"Transcoding {task_name[10:]} video in chunks",
                job_id=job_id,
            )


_CHUNK_PROGRESS_TASK_RE = re.compile(r"^transcode_(\d+)p$")


def _planned_chunk_totals(settings: dict) -> dict[int, int]:
    """Read the fail-closed rendition/chunk contract carried by the canvas."""
    raw_totals = (settings or {}).get(
        progress_tracker.CHUNK_TOTALS_SETTING
    )
    if not isinstance(raw_totals, dict) or not raw_totals:
        raise ValueError("concat_segments is missing planned chunk totals")

    totals = {}
    for task_name, raw_total in raw_totals.items():
        match = _CHUNK_PROGRESS_TASK_RE.fullmatch(str(task_name))
        if match is None:
            raise ValueError(
                f"concat_segments has an invalid chunk task: {task_name!r}"
            )
        try:
            total = int(raw_total)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"concat_segments has an invalid chunk total: {raw_total!r}"
            ) from exc
        if total <= 0:
            raise ValueError("concat_segments chunk totals must be positive")
        height = int(match.group(1))
        if height in totals:
            raise ValueError(
                f"concat_segments repeats planned rendition {height}p"
            )
        totals[height] = total
    return totals


def _validated_chunk_results(
    results,
    job_id: str,
    settings: dict,
) -> tuple[dict, dict]:
    """Validate chord cardinality before concatenating any chunk output."""
    planned = _planned_chunk_totals(settings)
    if not isinstance(results, (list, tuple)) or not results:
        raise ValueError("concat_segments received no chunk results")

    by_height = {height: [] for height in planned}
    meta = {}
    seen_indices = {height: set() for height in planned}
    for position, chunk_result in enumerate(results):
        if not isinstance(chunk_result, dict):
            raise ValueError(
                "concat_segments received a malformed result at "
                f"position {position}"
            )
        if str(chunk_result.get("job_id") or "") != str(job_id):
            raise ValueError(
                "concat_segments received a result for a different job"
            )
        chunk_index = chunk_result.get("chunk_index")
        if isinstance(chunk_index, bool) or not isinstance(chunk_index, int):
            raise ValueError("concat_segments received an invalid chunk index")
        if chunk_index < 0:
            raise ValueError("concat_segments received a negative chunk index")

        renditions = chunk_result.get("renditions")
        if not isinstance(renditions, list) or not renditions:
            raise ValueError(
                f"concat_segments chunk {chunk_index} has no renditions"
            )
        result_heights = set()
        for rendition in renditions:
            if not isinstance(rendition, dict):
                raise ValueError(
                    f"concat_segments chunk {chunk_index} has malformed "
                    "rendition metadata"
                )
            try:
                height = int(rendition["height"])
                width = int(rendition["width"])
                bitrate = int(rendition["bitrate"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError(
                    f"concat_segments chunk {chunk_index} has invalid "
                    "rendition metadata"
                ) from exc
            codec = str(rendition.get("codec") or "").strip().lower()
            chunk_dir = rendition.get("dir")
            if (
                height not in planned
                or width <= 0
                or bitrate <= 0
                or not codec
                or not isinstance(chunk_dir, str)
                or not chunk_dir
            ):
                raise ValueError(
                    f"concat_segments chunk {chunk_index} has unexpected "
                    f"{height}p rendition metadata"
                )
            if height in result_heights or chunk_index in seen_indices[height]:
                raise ValueError(
                    "concat_segments received a duplicate "
                    f"{height}p chunk index {chunk_index}"
                )
            result_heights.add(height)
            seen_indices[height].add(chunk_index)

            normalized = dict(rendition)
            normalized.update(
                {
                    "height": height,
                    "width": width,
                    "bitrate": bitrate,
                    "codec": codec,
                }
            )
            by_height[height].append((chunk_index, normalized))
            signature = (width, bitrate, codec)
            if height in meta and meta[height] != signature:
                raise ValueError(
                    "concat_segments received inconsistent metadata for "
                    f"{height}p"
                )
            meta[height] = signature

    for height, total in planned.items():
        expected = set(range(total))
        actual = seen_indices[height]
        if actual != expected:
            missing = sorted(expected - actual)
            unexpected = sorted(actual - expected)
            raise ValueError(
                f"concat_segments {height}p chunk cardinality mismatch: "
                f"missing={missing} unexpected={unexpected}"
            )
    normalized_meta = {
        height: {
            "width": signature[0],
            "bitrate": signature[1],
            "codec": signature[2],
        }
        for height, signature in meta.items()
    }
    return by_height, normalized_meta


def _runtime_limit(name: str, default: float) -> float:
    try:
        return float(getattr(get_settings(), name))
    except (AttributeError, TypeError, ValueError):
        return float(default)


def _remaining_wall_timeout(
    configured_timeout: float,
    deadline: float | None,
) -> float:
    """Keep primary and recovery FFmpeg attempts inside one wall budget."""
    if deadline is None:
        return float(configured_timeout)
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ffmpeg_utils.FFmpegError(
            "GPU FFmpeg wall-clock budget was exhausted before recovery"
        )
    return remaining


def _cpu_fallback_settings(settings: dict) -> dict:
    """Return a JSON-safe copy with the explicit software encoder preset."""
    fallback = dict(settings or {})
    try:
        configured_preset = get_settings().CPU_VIDEO_PRESET
    except (AttributeError, TypeError, ValueError):
        configured_preset = "veryfast"
    fallback["cpu_video_preset"] = str(
        fallback.get("cpu_video_preset") or configured_preset
    ).lower()
    try:
        configured_threads = get_settings().CPU_FALLBACK_THREADS_PER_TASK
    except (AttributeError, TypeError, ValueError):
        configured_threads = 2
    try:
        requested_threads = int(
            fallback.get("cpu_fallback_threads_per_task")
            or configured_threads
        )
    except (TypeError, ValueError):
        requested_threads = int(configured_threads)
    fallback["cpu_fallback_threads_per_task"] = min(
        16,
        max(1, requested_threads),
    )
    return fallback


def _bounded_cpu_group_canvas(
    job_id: str,
    source_url: str,
    renditions: list,
    duration: float,
    settings: dict,
    video_id: str | None = None,
):
    """Replace one feature-length GPU task with bounded CPU chunk tasks."""
    fallback_settings = _cpu_fallback_settings(settings)
    validate_bounded_chunk_codecs(
        renditions,
        fallback_settings.get("codec", "h264"),
    )
    requested_slice = fallback_settings.get(
        "cpu_fallback_chunk_duration_sec",
        _runtime_limit(
            "CPU_FALLBACK_CHUNK_DURATION_SEC",
            CPU_FALLBACK_DEFAULT_SLICE_SEC,
        ),
    )
    groups, chunks = bounded_cpu_plan(
        duration,
        renditions,
        requested_slice,
        rendition_height=_rendition_height,
    )
    fallback_settings = progress_tracker.with_chunked_tasks(
        fallback_settings,
        _rendition_progress_tasks(renditions),
        len(chunks),
    )
    if video_id is not None:
        _configure_chunk_progress(
            video_id,
            job_id,
            fallback_settings,
            renditions,
        )
    signatures = []
    for group_renditions in groups:
        for chunk_index, (start_sec, duration_sec) in enumerate(chunks):
            signatures.append(
                transcode_chunk.s(
                    job_id,
                    source_url,
                    group_renditions,
                    start_sec,
                    duration_sec,
                    chunk_index,
                    None,
                    fallback_settings,
                    True,
                ).set(queue=CPU_VIDEO_QUEUE)
            )
    return chord(
        group(*signatures),
        concat_segments.s(job_id, fallback_settings).set(queue=PACKAGE_QUEUE),
    )


def _bounded_cpu_chunk_canvas(
    job_id: str,
    source_url: str,
    renditions: list,
    start_sec: float,
    duration_sec: float,
    chunk_index: int,
    settings: dict,
):
    """Replace one failed GPU slice with one-rendition CPU deliveries."""
    duration = float(duration_sec)
    if duration <= 0 or duration > CPU_FALLBACK_MAX_SLICE_SEC:
        raise ValueError(
            "GPU chunk fallback duration must be in "
            f"(0, {CPU_FALLBACK_MAX_SLICE_SEC}] seconds"
        )
    fallback_settings = _cpu_fallback_settings(settings)
    validate_bounded_chunk_codecs(
        renditions,
        fallback_settings.get("codec", "h264"),
    )
    groups, chunks = bounded_cpu_plan(
        duration,
        renditions,
        duration,
        rendition_height=_rendition_height,
    )
    if len(chunks) != 1:
        raise ValueError("GPU chunk fallback unexpectedly required subdivision")
    signatures = [
        transcode_chunk.s(
            job_id,
            source_url,
            group_renditions,
            start_sec,
            duration,
            chunk_index,
            None,
            fallback_settings,
            True,
        ).set(queue=CPU_VIDEO_QUEUE)
        for group_renditions in groups
    ]
    if len(signatures) == 1:
        return signatures[0]
    expected_heights = sorted(_rendition_height(r) for r in renditions)
    return chord(
        group(*signatures),
        merge_chunk_results.s(
            job_id,
            int(chunk_index),
            expected_heights,
        ).set(queue=PACKAGE_QUEUE),
    )


def _direct_play_fallback_canvas(
    job_id: str,
    source_url: str,
    settings: dict,
    video_id: str | None = None,
):
    """Recreate the pipeline's normal encoded video canvas after copy failure."""
    plan = (settings or {}).get("_video_direct_play")
    if not isinstance(plan, dict):
        raise ValueError("direct-play fallback plan is missing")
    routes = plan.get("fallback_routes") or []
    fallback_renditions = plan.get("fallback_renditions") or []
    if not routes or not fallback_renditions:
        raise ValueError("direct-play fallback ladder is empty")

    fallback_settings = dict(settings or {})
    fallback_settings.pop("_video_direct_play", None)
    fallback_settings["video_passthrough_enabled"] = False
    use_chunked = bool(plan.get("fallback_use_chunked"))
    fallback_settings["chunked_encoding"] = use_chunked
    queue = str(plan.get("fallback_queue") or CPU_VIDEO_QUEUE)
    force_cpu = bool(plan.get("fallback_force_cpu"))

    if use_chunked:
        chunks = plan.get("fallback_chunks") or []
        if not chunks:
            raise ValueError("direct-play chunk fallback plan is empty")
        fallback_settings = progress_tracker.with_chunked_tasks(
            fallback_settings,
            _rendition_progress_tasks(fallback_renditions),
            len(chunks),
        )
        if video_id is not None:
            _configure_chunk_progress(
                video_id,
                job_id,
                fallback_settings,
                fallback_renditions,
            )
        signatures = []
        for route in routes:
            gpu_index = route.get("gpu_index")
            route_renditions = route.get("renditions") or []
            for chunk_index, chunk in enumerate(chunks):
                if not isinstance(chunk, (list, tuple)) or len(chunk) != 2:
                    raise ValueError("invalid direct-play fallback chunk")
                start_sec, duration_sec = chunk
                signatures.append(
                    transcode_chunk.s(
                        job_id,
                        source_url,
                        route_renditions,
                        start_sec,
                        duration_sec,
                        chunk_index,
                        gpu_index,
                        fallback_settings,
                        force_cpu,
                    ).set(queue=queue)
                )
        if not signatures:
            raise ValueError("direct-play fallback produced no chunk tasks")
        return chord(
            group(*signatures),
            concat_segments.s(
                job_id,
                fallback_settings,
            ).set(queue=PACKAGE_QUEUE),
        )

    signatures = [
        transcode_group.s(
            job_id,
            source_url,
            route.get("renditions") or [],
            route.get("gpu_index"),
            fallback_settings,
        ).set(queue=queue)
        for route in routes
    ]
    if len(signatures) == 1:
        return signatures[0]
    return group(*signatures)


def _rollback_quietly(db) -> None:
    rollback = getattr(db, "rollback", None)
    if callable(rollback):
        try:
            rollback()
        except Exception:
            pass


def _transition_direct_play_to_fallback(
    job_id: str,
    video_id: str,
) -> bool:
    """Durably claim a direct-play fallback for the current job generation.

    Returning ``False`` means an earlier delivery already committed the same
    intent. The caller must still reconstruct the fallback canvas so a crash
    between this commit and ``self.replace`` remains recoverable.
    """
    db = SessionLocal()
    try:
        job, video = lock_current_job(db, job_id)
        if str(job.video_id) != str(video_id):
            raise StaleJobError(
                f"direct-play fallback job {job_id} belongs to video "
                f"{job.video_id}, not {video_id}"
            )
        strategy = str(getattr(video, "encoding_strategy", "") or "")
        if strategy == DIRECT_PLAY_FALLBACK:
            return False
        if strategy not in ("", DIRECT_PLAY_PENDING):
            raise RuntimeError(
                f"cannot transition direct-play job {job_id} from "
                f"encoding strategy {strategy!r}"
            )
        video.encoding_strategy = DIRECT_PLAY_FALLBACK
        db.commit()
        return True
    except Exception:
        _rollback_quietly(db)
        raise
    finally:
        db.close()


def _run_transcode_group(
    self,
    job_id: str,
    source_url: str,
    renditions: list,
    gpu_index: int = None,
    settings: dict = None,
) -> dict:
    """Single-pass multi-rendition GPU transcode.

    This function never runs a feature-length CPU fallback in the same Celery
    delivery.  A missing/lost GPU raises ``BoundedCPUFallbackRequired`` so the
    task wrapper can replace itself with a bounded CPU canvas.
    """
    settings = settings or {}
    codec = settings.get("codec", "h264")
    direct_plan = settings.get("_video_direct_play")
    direct_mode = isinstance(direct_plan, dict)
    seg = settings.get("segment_duration_sec", ffmpeg_utils.SEGMENT_DURATION)
    seg_fmt = settings.get("segment_format", "fmp4")
    preset = settings.get("video_preset", "p6")
    nvenc_profile = settings.get("nvenc_profile")
    worker_id = _worker_id()
    video_id = None
    attempt_root = None
    progress_started = False

    db = SessionLocal()
    try:
        job, video = lock_current_job(db, job_id, allow_completed=True)
        video_id = str(job.video_id)

        norm_rend = _normalize_renditions(renditions, video, codec)
        if not norm_rend:
            raise ValueError("transcode_group received no renditions")
        heights = [r["height"] for r in norm_rend]
        duration = (
            float(settings.get("_source_video_duration") or 0.0)
            or video.duration
            or 1.0
        )
        if direct_mode:
            strategy = str(getattr(video, "encoding_strategy", "") or "")
            source_height = heights[0]
            if strategy == DIRECT_PLAY_FALLBACK:
                raise DirectPlayFallbackRequired(
                    "direct-play fallback was already committed",
                    video_id,
                    source_height,
                )
            if strategy not in (
                "",
                DIRECT_PLAY_PENDING,
                DIRECT_PLAY_COMPLETE,
            ):
                raise DirectPlayFallbackRequired(
                    f"direct-play durable state is {strategy!r}",
                    video_id,
                    source_height,
                )
            eligible, reason = ffmpeg_utils.h264_direct_play_eligibility(
                direct_plan.get("probe") or {},
                codec,
            )
            source_probe = direct_plan.get("probe") or {}
            if norm_rend:
                # Normal rendition coercion intentionally ignores caller
                # encoder-profile hints. Direct play is the exception: its
                # source profile and level must survive into the master CODECS
                # attribute because no encoder normalizes the bitstream.
                norm_rend[0]["profile"] = source_probe.get("video_profile")
                norm_rend[0]["level"] = source_probe.get("video_level")
            source_spec_matches = (
                len(norm_rend) == 1
                and int(norm_rend[0]["width"])
                == int(source_probe.get("width") or 0)
                and int(norm_rend[0]["height"])
                == int(source_probe.get("height") or 0)
                and str(norm_rend[0]["codec"]).lower() == "h264"
            )
            if not eligible or not source_spec_matches:
                raise DirectPlayFallbackRequired(
                    reason
                    if not eligible
                    else "direct-play source rendition metadata changed",
                    video_id,
                    source_height,
                )

        work_dir = os.path.join(get_settings().WORK_DIR, job_id)
        canonical_output = os.path.join(work_dir, "output")
        current_status = str(job.status)
        if current_status == models.JobStatus.failed.value:
            raise RuntimeError(
                f"refusing stale transcode delivery for failed job {job_id}"
            )
        completed_details = _completed_rendition_results(
            db,
            job,
            video,
            canonical_output,
            norm_rend,
            seg_fmt,
        )
        if completed_details is not None:
            return {
                "job_id": job_id,
                "renditions": [
                    completed_details[height] for height in heights
                ],
                "gpu_index": None,
                "reused": True,
                "durable": True,
            }

        canonical_reusable = _canonical_renditions_reusable(
            db,
            video_id,
            canonical_output,
            norm_rend,
            seg_fmt,
            duration,
        )
        if current_status in _ADVANCED_JOB_STATES and not canonical_reusable:
            raise RuntimeError(
                f"refusing stale transcode delivery for job {job_id} "
                f"in state {current_status}"
            )

        # A prior delivery may have committed a complete canonical tree and
        # rows but lost its acknowledgement.  Reuse it instead of encoding.
        if canonical_reusable:
            if current_status not in _ADVANCED_JOB_STATES:
                for h in heights:
                    progress_tracker.start_task(
                        video_id,
                        f"transcode_{h}p",
                        f"Transcoding {h}p video",
                        job_id=job_id,
                    )
                progress_started = True
            _rows, details = _prepare_rendition_rows(
                db,
                video_id,
                canonical_output,
                norm_rend,
                seg_fmt,
                duration,
            )
            db.commit()
            if current_status not in _ADVANCED_JOB_STATES:
                for h in heights:
                    progress_tracker.complete_task(
                        video_id,
                        f"transcode_{h}p",
                        job_id=job_id,
                    )
                    publish_event(
                        "video.rendition.completed",
                        {
                            "job_id": job_id,
                            "video_id": video_id,
                            "height": h,
                            "bandwidth": details[h]["bandwidth"],
                        },
                    )
            return {
                "job_id": job_id,
                "renditions": [details[h] for h in heights],
                "gpu_index": None,
                "reused": True,
            }

        os.makedirs(canonical_output, exist_ok=True)
        for h in heights:
            stage = (
                f"Remuxing source {h}p video"
                if direct_mode
                else f"Transcoding {h}p video"
            )
            progress_tracker.start_task(
                video_id,
                f"transcode_{h}p",
                stage,
                job_id=job_id,
            )
        progress_started = True
        advance_job_status(job, models.JobStatus.transcoding.value)
        db.commit()

        local_source = ensure_local_source(job_id, source_url)

        fps = video.frame_rate or 30.0
        input_bytes = _file_size(local_source)
        complexity = video.complexity_score

        def on_progress(pct):
            for h in heights:
                stage = (
                    f"Remuxing source {h}p video"
                    if direct_mode
                    else f"Transcoding {h}p video"
                )
                progress_tracker.update_task(
                    video_id,
                    f"transcode_{h}p",
                    pct,
                    stage,
                    job_id=job_id,
                )

        attempt_root, attempt_output = _new_attempt_output(
            work_dir,
            _delivery_id(self),
        )
        # perf_counter is monotonic and therefore immune to host clock
        # corrections during a long encode.
        encode_started = time.perf_counter()

        used_gpu_index = None
        if direct_mode:
            source_rendition = norm_rend[0]
            source_height = int(source_rendition["height"])
            attempt_rendition = os.path.join(
                attempt_output,
                f"video_{source_height}p",
            )
            try:
                cmd = ffmpeg_utils.remux_h264_hls_command(
                    local_source,
                    attempt_rendition,
                    segment_duration=seg,
                    segment_format=seg_fmt,
                )
                ffmpeg_utils.run_cmd_with_progress(
                    cmd,
                    duration,
                    on_progress,
                    wall_timeout=_runtime_limit(
                        "CPU_FALLBACK_FFMPEG_TIMEOUT_SEC",
                        5400,
                    ),
                )
                _validate_direct_play_output(
                    attempt_rendition,
                    source_rendition,
                    seg_fmt,
                    duration,
                    fps,
                    seg,
                )
            except Exception as exc:
                logger.warning(
                    "[transcode_group] H.264 direct-play failed for job=%s; "
                    "requesting the normal encoded ladder: %s",
                    job_id,
                    exc,
                )
                raise DirectPlayFallbackRequired(
                    str(exc),
                    video_id,
                    source_height,
                ) from exc
        else:
            # Reserve one NVENC session per simultaneous rendition. The lease
            # and heartbeat live in this Celery pool child, so a hard child
            # death cannot be hidden by the healthy parent registration.
            try:
                gpu_lease = gpu_registry.acquire_gpu(
                    worker_id,
                    slots=len(norm_rend),
                    preferred_index=gpu_index,
                )
            except Exception as exc:
                logger.warning(
                    "[transcode_group] acquire_gpu failed: %s",
                    exc,
                )
                gpu_lease = None
            used_gpu_index = (
                gpu_lease.gpu_index if gpu_lease is not None else None
            )

            fallback_reason = None
            if gpu_lease is not None:
                lease_heartbeat = gpu_registry.GPULeaseHeartbeat(gpu_lease)
                gpu_timeout = _runtime_limit(
                    "GPU_FFMPEG_TIMEOUT_SEC",
                    5400,
                )
                gpu_deadline = (
                    time.monotonic() + gpu_timeout
                    if gpu_timeout > 0
                    else None
                )
                try:
                    lease_heartbeat.start()
                    try:
                        cmd = ffmpeg_utils.transcode_multi_command(
                            local_source,
                            attempt_output,
                            norm_rend,
                            fps,
                            gpu_index=used_gpu_index,
                            segment_duration=seg,
                            codec=codec,
                            segment_format=seg_fmt,
                            preset=preset,
                            require_gpu=True,
                            nvenc_profile=nvenc_profile,
                        )
                        ffmpeg_utils.run_cmd_with_progress(
                            cmd,
                            duration,
                            on_progress,
                            cancel_event=lease_heartbeat.lost_event,
                            wall_timeout=gpu_timeout,
                        )
                    except ffmpeg_utils.FFmpegError as exc:
                        if (
                            lease_heartbeat.lost_event.is_set()
                            or not ffmpeg_utils.is_nvdec_initialization_failure(
                                exc
                            )
                        ):
                            raise
                        logger.warning(
                            "[transcode_group] NVDEC initialization failed "
                            "for job=%s; retrying with software decode and "
                            "GPU scaling/NVENC: %s",
                            job_id,
                            exc,
                        )
                        _remove_rendition_directories(
                            attempt_output,
                            heights,
                        )
                        recovery_cmd = (
                            ffmpeg_utils.transcode_multi_command(
                                local_source,
                                attempt_output,
                                norm_rend,
                                fps,
                                gpu_index=used_gpu_index,
                                segment_duration=seg,
                                codec=codec,
                                segment_format=seg_fmt,
                                preset=preset,
                                require_gpu=True,
                                software_decode_gpu=True,
                                nvenc_profile=nvenc_profile,
                            )
                        )
                        if lease_heartbeat.lost_event.is_set():
                            raise ffmpeg_utils.FFmpegError(
                                "GPU reservation was lost before NVDEC "
                                "recovery could start"
                            )
                        ffmpeg_utils.run_cmd_with_progress(
                            recovery_cmd,
                            duration,
                            on_progress,
                            cancel_event=lease_heartbeat.lost_event,
                            wall_timeout=_remaining_wall_timeout(
                                gpu_timeout,
                                gpu_deadline,
                            ),
                        )
                except ffmpeg_utils.FFmpegError as exc:
                    logger.warning(
                        "[transcode_group] GPU multi-rendition failed for "
                        "job=%s, requesting bounded CPU replacement: %s",
                        job_id,
                        exc,
                    )
                    fallback_reason = str(exc)
                finally:
                    lease_heartbeat.stop()
                    try:
                        gpu_registry.release_gpu(gpu_lease)
                    except Exception:
                        logger.exception(
                            "[transcode_group] failed to release GPU lease=%s",
                            gpu_lease.lease_id,
                        )
                    gpu_lease = None
            else:
                fallback_reason = "no GPU lease was available"

            if fallback_reason is not None:
                raise BoundedCPUFallbackRequired(
                    fallback_reason,
                    duration,
                    video_id,
                )

        encode_finished = time.perf_counter()

        # Validate all attempt outputs before touching canonical paths.  Then
        # re-read job state so a late duplicate cannot regress a job that has
        # already advanced into packaging or publishing.
        _validate_rendition_set(
            attempt_output,
            norm_rend,
            seg_fmt,
            duration,
        )
        job, video = lock_current_job(db, job_id, allow_completed=True)
        current_status = str(job.status)
        if current_status in _ADVANCED_JOB_STATES:
            if not _canonical_renditions_reusable(
                db,
                video_id,
                canonical_output,
                norm_rend,
                seg_fmt,
                duration,
            ):
                raise RuntimeError(
                    f"job {job_id} advanced to {current_status}; "
                    "attempt output will not replace canonical renditions"
                )
        else:
            _promote_rendition_directories(
                attempt_output,
                canonical_output,
                norm_rend,
                seg_fmt,
                duration,
            )

        _rows, details = _prepare_rendition_rows(
            db,
            video_id,
            canonical_output,
            norm_rend,
            seg_fmt,
            duration,
        )
        if direct_mode:
            video.encoding_strategy = DIRECT_PLAY_COMPLETE
        encode_duration = encode_finished - encode_started
        measured_fps = _effective_fps(fps, duration, encode_duration)
        for r in norm_rend:
            h = r["height"]
            rdir = os.path.join(canonical_output, f"video_{h}p")
            _record_metric(
                db, job_id, f"{h}p", r["codec"], used_gpu_index, worker_id,
                fps=measured_fps,
                encode_duration_sec=encode_duration,
                input_bytes=input_bytes,
                output_bytes=_dir_size(rdir), complexity_score=complexity, commit=False,
            )
        # Rendition de-duplication/upserts and every metric are committed in
        # one transaction only after every canonical rendition validates.
        db.commit()

        results = []
        if current_status not in _ADVANCED_JOB_STATES:
            for h in heights:
                progress_tracker.complete_task(
                    video_id,
                    f"transcode_{h}p",
                    job_id=job_id,
                )
                publish_event(
                    "video.rendition.completed",
                    {
                        "job_id": job_id,
                        "video_id": video_id,
                        "height": h,
                        "bandwidth": details[h]["bandwidth"],
                    },
                )
        for h in heights:
            results.append(details[h])
        return {
            "job_id": job_id,
            "renditions": results,
            "gpu_index": used_gpu_index,
            "reused": current_status in _ADVANCED_JOB_STATES,
        }
    except StaleJobError:
        _rollback_quietly(db)
        raise
    except DirectPlayFallbackRequired:
        # Copy failure is a controlled replacement with the normal ladder.
        # Do not mark the source-height progress task failed.
        _rollback_quietly(db)
        raise
    except BoundedCPUFallbackRequired:
        # Replacement is a continuation of the same generation, not a failed
        # rendition.  Keep progress running and let the wrapper publish the
        # bounded CPU canvas.
        _rollback_quietly(db)
        raise
    except Exception as exc:
        _rollback_quietly(db)
        if progress_started and video_id:
            for r in (renditions or []):
                h = r["height"] if isinstance(r, dict) else r
                try:
                    progress_tracker.fail_task(
                        video_id,
                        f"transcode_{h}p",
                        str(exc),
                        job_id=job_id,
                    )
                except Exception:
                    pass
        raise
    finally:
        if attempt_root:
            _cleanup_attempt_root(attempt_root)
        db.close()


@celery_app.task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    max_retries=3,
)
def transcode_group(
    self,
    job_id: str,
    source_url: str,
    renditions: list,
    gpu_index: int = None,
    settings: dict = None,
) -> dict:
    """Run one single-pass delivery under the package-compatible job lock."""
    work_root = get_settings().WORK_DIR
    heights = sorted(
        {
            int(rendition)
            if isinstance(rendition, int)
            else int(rendition["height"])
            for rendition in (renditions or [])
        }
    )
    # Shared job ownership blocks package's exclusive lock while preserving
    # intended multi-GPU parallelism.  Ordered rendition locks serialize any
    # duplicate or overlapping group without deadlocking disjoint groups.
    with job_lock(
        work_root,
        job_id,
        purpose="transcode_group:job",
        shared=True,
    ):
        with ExitStack() as rendition_locks:
            for height in heights:
                rendition_locks.enter_context(
                    job_lock(
                        work_root,
                        f"{job_id}:rendition:{height}",
                        purpose=f"transcode_group:{height}p",
                    )
                )
            try:
                return _run_transcode_group(
                    self,
                    job_id,
                    source_url,
                    renditions,
                    gpu_index,
                    settings,
                )
            except DirectPlayFallbackRequired as exc:
                transitioned = _transition_direct_play_to_fallback(
                    job_id,
                    exc.video_id,
                )
                fallback_plan = (settings or {}).get(
                    "_video_direct_play",
                    {},
                )
                fallback_renditions = fallback_plan.get(
                    "fallback_renditions",
                    [],
                )
                fallback_tasks = [
                    f"transcode_{_rendition_height(rendition)}p"
                    for rendition in fallback_renditions
                ]
                try:
                    progress_replaced = progress_tracker.replace_task(
                        exc.video_id,
                        f"transcode_{exc.source_height}p",
                        fallback_tasks,
                        job_id=job_id,
                    )
                    if not progress_replaced:
                        logger.warning(
                            "[transcode_group] direct-play progress transition "
                            "was rejected for job=%s",
                            job_id,
                        )
                except Exception:
                    logger.exception(
                        "[transcode_group] could not replace direct-play "
                        "progress tasks for job=%s",
                        job_id,
                    )
                logger.warning(
                    "[transcode_group] replacing direct-play job=%s "
                    "with encoded renditions=%s durable_transition=%s: %s",
                    job_id,
                    [
                        _rendition_height(rendition)
                        for rendition in fallback_renditions
                    ],
                    transitioned,
                    exc,
                )
                return self.replace(
                    _direct_play_fallback_canvas(
                        job_id,
                        source_url,
                        settings or {},
                        video_id=exc.video_id,
                    )
                )
            except BoundedCPUFallbackRequired as exc:
                logger.warning(
                    "[transcode_group] replacing job=%s heights=%s with "
                    "bounded CPU chunks after GPU loss: %s",
                    job_id,
                    heights,
                    exc,
                )
                return self.replace(
                    _bounded_cpu_group_canvas(
                        job_id,
                        source_url,
                        renditions,
                        exc.duration,
                        settings or {},
                        video_id=exc.video_id,
                    )
                )
            except StaleJobError as exc:
                logger.info(
                    "[transcode_group] skipping stale job=%s: %s",
                    job_id,
                    exc,
                )
                return stale_result(job_id, "video")


def _run_transcode_chunk(
    self,
    job_id: str,
    source_url: str,
    renditions: list,
    start_sec: float,
    duration_sec: float,
    chunk_index: int,
    gpu_index: int = None,
    settings: dict = None,
    force_cpu: bool = False,
) -> dict:
    """Encode one capacity-bounded time-slice."""
    settings = settings or {}
    codec = settings.get("codec", "h264")
    seg = settings.get("segment_duration_sec", ffmpeg_utils.SEGMENT_DURATION)
    seg_fmt = settings.get("segment_format", "fmp4")
    nvenc_profile = settings.get("nvenc_profile")
    cpu_preset = settings.get("cpu_video_preset") or _cpu_fallback_settings(
        settings
    )["cpu_video_preset"]
    cpu_threads = _cpu_fallback_settings(settings)[
        "cpu_fallback_threads_per_task"
    ]
    worker_id = _worker_id()
    video_id = None

    db = SessionLocal()
    try:
        job, video = lock_current_job(db, job_id)
        video_id = str(job.video_id)
        advance_job_status(job, models.JobStatus.transcoding.value)
        db.commit()

        norm_rend = _normalize_renditions(renditions, video, codec)
        if not norm_rend:
            raise ValueError("transcode_chunk received no renditions")
        validate_bounded_chunk_codecs(norm_rend, codec)
        heights = [r["height"] for r in norm_rend]
        chunk_totals = {}
        for h in heights:
            task_name = f"transcode_{h}p"
            total_chunks = progress_tracker.task_chunk_count(
                settings,
                task_name,
            )
            chunk_totals[h] = total_chunks
            if total_chunks:
                progress_tracker.configure_chunked_task(
                    video_id,
                    task_name,
                    total_chunks,
                    stage=f"Transcoding {h}p video in chunks",
                    job_id=job_id,
                )
                progress_tracker.update_chunked_task(
                    video_id,
                    task_name,
                    int(chunk_index),
                    total_chunks,
                    0,
                    f"Transcoding {h}p video in chunks",
                    job_id=job_id,
                )
            else:
                progress_tracker.start_task(
                    video_id,
                    task_name,
                    f"Transcoding {h}p video",
                    job_id=job_id,
                )

        work_dir = os.path.join(get_settings().WORK_DIR, job_id)
        local_source = ensure_local_source(job_id, source_url)

        fps = video.frame_rate or 30.0
        input_bytes = _file_size(local_source)
        complexity = video.complexity_score
        chunk_dur = float(duration_sec) if duration_sec else 0.0
        if chunk_dur <= 0 or chunk_dur > CPU_FALLBACK_MAX_SLICE_SEC:
            raise ValueError(
                "transcode_chunk duration must be in "
                f"(0, {CPU_FALLBACK_MAX_SLICE_SEC}] seconds"
            )

        def on_progress(pct):
            for h in heights:
                task_name = f"transcode_{h}p"
                total_chunks = chunk_totals[h]
                if total_chunks:
                    progress_tracker.update_chunked_task(
                        video_id,
                        task_name,
                        int(chunk_index),
                        total_chunks,
                        pct,
                        f"Transcoding {h}p video in chunks",
                        job_id=job_id,
                    )
                else:
                    progress_tracker.update_task(
                        video_id,
                        task_name,
                        pct,
                        f"Transcoding {h}p video",
                        job_id=job_id,
                    )

        group_key = _chunk_group_key(norm_rend)
        chunk_dir = os.path.join(
            work_dir,
            "chunks",
            f"chunk_{int(chunk_index):05d}_{group_key}",
        )
        if os.path.isdir(chunk_dir):
            shutil.rmtree(chunk_dir)
        os.makedirs(chunk_dir, exist_ok=True)
        encode_started = time.perf_counter()

        gpu_lease = None
        used_gpu_index = None
        fallback_reason = None
        if not force_cpu:
            try:
                gpu_lease = gpu_registry.acquire_gpu(
                    worker_id,
                    slots=len(norm_rend),
                    preferred_index=gpu_index,
                )
            except Exception as exc:
                logger.warning("[transcode_chunk] acquire_gpu failed: %s", exc)
                fallback_reason = str(exc)
            if gpu_lease is None and fallback_reason is None:
                fallback_reason = "no GPU lease was available"
            used_gpu_index = (
                gpu_lease.gpu_index if gpu_lease is not None else None
            )

        if gpu_lease is not None:
            lease_heartbeat = gpu_registry.GPULeaseHeartbeat(gpu_lease)
            gpu_timeout = _runtime_limit(
                "GPU_FFMPEG_TIMEOUT_SEC",
                5400,
            )
            gpu_deadline = (
                time.monotonic() + gpu_timeout
                if gpu_timeout > 0
                else None
            )
            try:
                lease_heartbeat.start()
                try:
                    cmd = ffmpeg_utils.transcode_chunk_command(
                        local_source, chunk_dir, norm_rend,
                        float(start_sec), chunk_dur, fps,
                        gpu_index=used_gpu_index, segment_duration=seg, codec=codec, segment_format="ts",
                        require_gpu=True,
                        nvenc_profile=nvenc_profile,
                    )
                    ffmpeg_utils.run_cmd_with_progress(
                        cmd,
                        chunk_dur,
                        on_progress,
                        cancel_event=lease_heartbeat.lost_event,
                        wall_timeout=gpu_timeout,
                    )
                except ffmpeg_utils.FFmpegError as exc:
                    if (
                        lease_heartbeat.lost_event.is_set()
                        or not ffmpeg_utils.is_nvdec_initialization_failure(
                            exc
                        )
                    ):
                        raise
                    logger.warning(
                        "[transcode_chunk] NVDEC initialization failed for "
                        "job=%s chunk=%s; retrying with software decode and "
                        "GPU scaling/NVENC: %s",
                        job_id,
                        chunk_index,
                        exc,
                    )
                    shutil.rmtree(chunk_dir, ignore_errors=True)
                    if os.path.lexists(chunk_dir):
                        raise ffmpeg_utils.FFmpegError(
                            "could not remove partial chunk before GPU "
                            f"recovery: {chunk_dir}"
                        )
                    os.makedirs(chunk_dir, exist_ok=True)
                    recovery_cmd = ffmpeg_utils.transcode_chunk_command(
                        local_source, chunk_dir, norm_rend,
                        float(start_sec), chunk_dur, fps,
                        gpu_index=used_gpu_index,
                        segment_duration=seg,
                        codec=codec,
                        segment_format="ts",
                        require_gpu=True,
                        software_decode_gpu=True,
                        nvenc_profile=nvenc_profile,
                    )
                    if lease_heartbeat.lost_event.is_set():
                        raise ffmpeg_utils.FFmpegError(
                            "GPU reservation was lost before NVDEC recovery "
                            "could start"
                        )
                    ffmpeg_utils.run_cmd_with_progress(
                        recovery_cmd,
                        chunk_dur,
                        on_progress,
                        cancel_event=lease_heartbeat.lost_event,
                        wall_timeout=_remaining_wall_timeout(
                            gpu_timeout,
                            gpu_deadline,
                        ),
                    )
            except ffmpeg_utils.FFmpegError as exc:
                logger.warning(
                    "[transcode_chunk] GPU chunk failed for job=%s chunk=%s, "
                    "requesting bounded CPU replacement: %s",
                    job_id,
                    chunk_index,
                    exc,
                )
                fallback_reason = str(exc)
            finally:
                lease_heartbeat.stop()
                try:
                    gpu_registry.release_gpu(gpu_lease)
                except Exception:
                    logger.exception(
                        "[transcode_chunk] failed to release GPU lease=%s",
                        gpu_lease.lease_id,
                    )
                gpu_lease = None

        if fallback_reason is not None:
            raise BoundedCPUFallbackRequired(
                fallback_reason,
                chunk_dur,
                video_id,
            )

        if force_cpu:
            used_gpu_index = None
            cmd = ffmpeg_utils.transcode_chunk_command(
                local_source, chunk_dir, norm_rend,
                float(start_sec), chunk_dur, fps,
                gpu_index=None, segment_duration=seg, codec=codec, segment_format="ts",
                force_software=True,
                cpu_preset=cpu_preset,
                cpu_threads=cpu_threads,
                require_cpu_codec=True,
            )
            ffmpeg_utils.run_cmd_with_progress(
                cmd,
                chunk_dur,
                on_progress,
                wall_timeout=_runtime_limit(
                    "CPU_FALLBACK_FFMPEG_TIMEOUT_SEC",
                    5400,
                ),
            )

        encode_duration = time.perf_counter() - encode_started
        measured_fps = _effective_fps(fps, chunk_dur, encode_duration)
        source_duration = float(
            settings.get("_source_video_duration")
            or video.duration
            or 0.0
        )
        processed_input_bytes = (
            int(
                round(
                    input_bytes
                    * min(chunk_dur, source_duration)
                    / source_duration
                )
            )
            if source_duration > 0
            else input_bytes
        )

        rend_out = []
        for r in norm_rend:
            h = r["height"]
            rdir = os.path.join(chunk_dir, f"video_{h}p")
            rend_out.append({
                "height": h, "width": r["width"], "bitrate": r["bitrate"], "codec": r["codec"],
                "dir": _abspath(rdir),
                "playlist": _abspath(os.path.join(rdir, f"chunk_{chunk_index}.m3u8")),
            })

        lock_current_job(db, job_id)
        _record_metric(
            db,
            job_id,
            f"chunk_{chunk_index}_{group_key}",
            codec,
            used_gpu_index,
            worker_id,
            fps=measured_fps,
            encode_duration_sec=encode_duration,
            input_bytes=processed_input_bytes,
            output_bytes=_dir_size(chunk_dir), complexity_score=complexity, commit=True,
        )
        for h in heights:
            total_chunks = chunk_totals[h]
            if total_chunks:
                progress_tracker.update_chunked_task(
                    video_id,
                    f"transcode_{h}p",
                    int(chunk_index),
                    total_chunks,
                    100,
                    f"Transcoding {h}p video in chunks",
                    job_id=job_id,
                )
        return {
            "job_id": job_id,
            "chunk_index": chunk_index,
            "gpu_index": used_gpu_index,
            "renditions": rend_out,
        }
    except StaleJobError:
        _rollback_quietly(db)
        raise
    except BoundedCPUFallbackRequired:
        _rollback_quietly(db)
        raise
    except Exception as exc:
        if video_id:
            for r in (renditions or []):
                h = r["height"] if isinstance(r, dict) else r
                try:
                    progress_tracker.fail_task(
                        video_id,
                        f"transcode_{h}p",
                        str(exc),
                        job_id=job_id,
                    )
                except Exception:
                    pass
        raise
    finally:
        db.close()


@celery_app.task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    max_retries=3,
)
def transcode_chunk(
    self,
    job_id: str,
    source_url: str,
    renditions: list,
    start_sec: float,
    duration_sec: float,
    chunk_index: int,
    gpu_index: int = None,
    settings: dict = None,
    force_cpu: bool = False,
) -> dict:
    """Serialize a chunk delivery and coordinate it with package cleanup."""
    work_root = get_settings().WORK_DIR
    group_key = _chunk_group_key(renditions)
    try:
        with job_lock(
            work_root,
            job_id,
            purpose=f"transcode_chunk:{chunk_index}:job",
            shared=True,
        ):
            # Concat takes this lock exclusively, so it can never read a
            # group directory while a duplicate chunk delivery recreates it.
            with job_lock(
                work_root,
                f"{job_id}:chunks",
                purpose=f"transcode_chunk:{chunk_index}:concat-fence",
                shared=True,
            ):
                with job_lock(
                    work_root,
                    f"{job_id}:chunk:{int(chunk_index)}:{group_key}",
                    purpose=f"transcode_chunk:{chunk_index}:{group_key}",
                ):
                    return _run_transcode_chunk(
                        self,
                        job_id,
                        source_url,
                        renditions,
                        start_sec,
                        duration_sec,
                        chunk_index,
                        gpu_index,
                        settings,
                        force_cpu,
                    )
    except BoundedCPUFallbackRequired as exc:
        logger.warning(
            "[transcode_chunk] replacing job=%s chunk=%s with bounded CPU "
            "work after GPU loss: %s",
            job_id,
            chunk_index,
            exc,
        )
        return self.replace(
            _bounded_cpu_chunk_canvas(
                job_id,
                source_url,
                renditions,
                start_sec,
                exc.duration,
                chunk_index,
                settings or {},
            )
        )
    except StaleJobError as exc:
        logger.info(
            "[transcode_chunk] skipping stale job=%s chunk=%s: %s",
            job_id,
            chunk_index,
            exc,
        )
        return stale_result(job_id, "video_chunk")


@celery_app.task
def merge_chunk_results(
    results,
    job_id: str,
    chunk_index: int,
    expected_heights: list,
) -> dict:
    """Collapse one-rendition CPU replacements into the original chunk shape."""
    renditions = []
    seen_heights = set()
    for result in results or []:
        if not isinstance(result, dict):
            raise ValueError("CPU chunk replacement returned a non-object")
        if str(result.get("job_id")) != str(job_id):
            raise ValueError("CPU chunk replacement returned the wrong job")
        if int(result.get("chunk_index", -1)) != int(chunk_index):
            raise ValueError("CPU chunk replacement returned the wrong slice")
        for rendition in result.get("renditions") or []:
            height = int(rendition["height"])
            if height in seen_heights:
                raise ValueError(
                    f"CPU chunk replacement duplicated {height}p"
                )
            seen_heights.add(height)
            renditions.append(rendition)

    expected = {int(height) for height in expected_heights or []}
    if seen_heights != expected:
        raise ValueError(
            "CPU chunk replacement result mismatch: "
            f"expected={sorted(expected)} actual={sorted(seen_heights)}"
        )
    renditions.sort(key=lambda rendition: int(rendition["height"]), reverse=True)
    return {
        "job_id": job_id,
        "chunk_index": int(chunk_index),
        "gpu_index": None,
        "renditions": renditions,
    }


def _run_concat_segments(self, results, job_id: str, settings: dict = None) -> dict:
    """Stitch per-rendition chunk outputs into variant playlists and create Rendition rows."""
    settings = settings or {}
    segment_duration = settings.get(
        "segment_duration_sec",
        ffmpeg_utils.SEGMENT_DURATION,
    )
    worker_id = _worker_id()
    video_id = None

    db = SessionLocal()
    try:
        job, video = lock_current_job(db, job_id)
        video_id = str(job.video_id)
        advance_job_status(job, models.JobStatus.transcoding.value)
        db.commit()

        duration = (
            float(settings.get("_source_video_duration") or 0.0)
            or video.duration
            or 1.0
        )
        complexity = video.complexity_score
        work_dir = os.path.join(get_settings().WORK_DIR, job_id)
        out_base = os.path.join(work_dir, "output")
        os.makedirs(out_base, exist_ok=True)

        # Chunked path always uses MPEG-TS segments: TS concatenates cleanly
        # via the concat demuxer, unlike fMP4 fragments (no standalone moov).
        ext = "ts"
        is_fmp4 = False

        # Validate the complete chord contract before deleting or rebuilding
        # any canonical output. Missing, duplicate, replayed, and malformed
        # slices must fail closed rather than produce a plausible partial VOD.
        by_height, meta = _validated_chunk_results(
            results,
            job_id,
            settings,
        )
        chunk_root = os.path.realpath(os.path.join(work_dir, "chunks"))

        rendition_specs = []
        started = time.perf_counter()
        for h in sorted(by_height.keys()):
            items = sorted(by_height[h], key=lambda x: x[0])
            m = meta[h]
            rdir = os.path.join(out_base, f"video_{h}p")
            if os.path.isdir(rdir):
                shutil.rmtree(rdir)
            os.makedirs(rdir, exist_ok=True)
            out_playlist = os.path.join(rdir, "video.m3u8")

            # Build a concat list of every segment file across chunks (in order).
            list_path = os.path.join(rdir, "concat_list.txt")
            seg_lines = []
            if is_fmp4 and items:
                first_dir = items[0][1].get("dir", "")
                init_path = os.path.join(first_dir, "init.mp4")
                if os.path.exists(init_path):
                    seg_lines.append(f"file '{_abspath(init_path)}'")
            for _ci, rend in items:
                cdir = rend.get("dir", "")
                real_cdir = os.path.realpath(cdir)
                try:
                    contained = (
                        os.path.commonpath((chunk_root, real_cdir))
                        == chunk_root
                    )
                except ValueError:
                    contained = False
                if (
                    not contained
                    or os.path.islink(cdir)
                    or not os.path.isdir(real_cdir)
                ):
                    raise ValueError(
                        "concat_segments received an unsafe or missing "
                        f"chunk directory: {cdir!r}"
                    )
                seg_files = sorted(
                    (
                        name
                        for name in os.listdir(real_cdir)
                        if name.endswith(f".{ext}")
                        and not os.path.islink(
                            os.path.join(real_cdir, name)
                        )
                        and os.path.isfile(
                            os.path.join(real_cdir, name)
                        )
                    ),
                    key=_natural_key,
                )
                if not seg_files:
                    raise ValueError(
                        "concat_segments received a chunk without media "
                        f"segments: {real_cdir}"
                    )
                for sf in seg_files:
                    seg_lines.append(
                        f"file '{_abspath(os.path.join(real_cdir, sf))}'"
                    )
            with open(list_path, "w", encoding="utf-8") as fh:
                fh.write("\n".join(seg_lines) + "\n")

            cmd = ffmpeg_utils.concat_segments_command(
                list_path,
                out_playlist,
                codec=m["codec"],
                segment_duration=segment_duration,
            )
            ffmpeg_utils.run_cmd(cmd)

            rendition_specs.append(
                {
                    "height": h,
                    "width": m["width"],
                    "bitrate": m["bitrate"],
                    "codec": m["codec"],
                }
            )

        if not rendition_specs:
            raise ValueError("concat_segments received no usable chunk outputs")
        _validate_rendition_set(
            out_base,
            rendition_specs,
            "ts",
            duration,
        )

        # A retry can supersede this generation while concat is running.
        # Re-lock it before staging deterministic video-wide rows.
        lock_current_job(db, job_id)
        _rows, details = _prepare_rendition_rows(
            db,
            video_id,
            out_base,
            rendition_specs,
            "ts",
            duration,
        )
        for spec in rendition_specs:
            h = spec["height"]
            rdir = os.path.join(out_base, f"video_{h}p")
            _record_metric(
                db, job_id, f"concat_{h}p", spec["codec"], None, worker_id,
                fps=0.0,
                encode_duration_sec=time.perf_counter() - started,
                input_bytes=0, output_bytes=_dir_size(rdir),
                complexity_score=complexity, commit=False,
            )
        db.commit()

        rendition_rows = []
        for spec in rendition_specs:
            h = spec["height"]
            progress_tracker.complete_task(
                video_id,
                f"transcode_{h}p",
                job_id=job_id,
            )
            publish_event(
                "video.rendition.completed",
                {
                    "job_id": job_id,
                    "video_id": video_id,
                    "height": h,
                    "bandwidth": details[h]["bandwidth"],
                },
            )
            rendition_rows.append(details[h])
        return {"job_id": job_id, "renditions": rendition_rows}
    except StaleJobError:
        _rollback_quietly(db)
        raise
    finally:
        db.close()


@celery_app.task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    max_retries=3,
)
def concat_segments(self, results, job_id: str, settings: dict = None) -> dict:
    """Serialize concat output and coordinate with package cleanup."""
    work_root = get_settings().WORK_DIR
    try:
        with job_lock(
            work_root,
            job_id,
            purpose="concat_segments:job",
            shared=True,
        ):
            with job_lock(
                work_root,
                f"{job_id}:chunks",
                purpose="concat_segments:chunk-fence",
            ):
                with job_lock(
                    work_root,
                    f"{job_id}:concat",
                    purpose="concat_segments",
                ):
                    return _run_concat_segments(
                        self,
                        results,
                        job_id,
                        settings,
                    )
    except StaleJobError as exc:
        logger.info("[concat_segments] skipping stale job=%s: %s", job_id, exc)
        return stale_result(job_id, "video_concat")
