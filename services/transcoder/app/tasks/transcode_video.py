"""Transcode video renditions (GPU-grouped single-pass, chunked, per-rendition fallback)."""

import logging
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


class BoundedCPUFallbackRequired(RuntimeError):
    """Signal that the current GPU delivery must be replaced, not continued."""

    def __init__(self, reason: str, duration: float):
        super().__init__(reason)
        self.duration = float(duration)


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
    total = 0
    try:
        for name in os.listdir(directory):
            total += _file_size(os.path.join(directory, name))
    except OSError:
        pass
    return total


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

        task_name = f"transcode_{height}p"

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
            video.duration or 1.0,
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
                    f"Transcoding {height}p video",
                    job_id=job_id,
                )
                progress_started = True
            _rows, details = _prepare_rendition_rows(
                db,
                video_id,
                canonical_output,
                normalized,
                seg_fmt,
                video.duration or 1.0,
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
            f"Transcoding {height}p video",
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
                f"Transcoding {height}p video",
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
        try:
            cmd = ffmpeg_utils.transcode_video_command(
                local_source, attempt_rendition,
                width, height, bitrate, fps,
                force_gpu=use_gpu,
                segment_duration=seg_dur,
                preset=preset,
                codec=spec["codec"],
                segment_format=seg_fmt,
            )
            duration = video.duration or 1.0
            ffmpeg_utils.run_cmd_with_progress(cmd, duration, on_progress)
        except ffmpeg_utils.FFmpegError as exc:
            if use_gpu:
                logger.warning(
                    "GPU transcode failed for job=%s, falling back to CPU: %s",
                    job_id, exc,
                )
                _remove_rendition_directories(attempt_output, [height])
                cmd = ffmpeg_utils.transcode_video_command(
                    local_source, attempt_rendition,
                    width, height, bitrate, fps,
                    force_gpu=False,
                    segment_duration=seg_dur,
                    preset=preset,
                    codec=spec["codec"],
                    segment_format=seg_fmt,
                )
                duration = video.duration or 1.0
                ffmpeg_utils.run_cmd_with_progress(cmd, duration, on_progress)
            else:
                raise

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

        duration = video.duration or 1.0
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
            norm.append({
                "height": h, "width": w,
                "bitrate": r["bitrate"], "codec": r.get("codec", codec),
            })
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
            2.0,
            min(30.0, expected_duration * 0.01),
        )
        if playlist_duration < expected_duration - allowed_shortfall:
            raise RenditionValidationError(
                "rendition playlist duration is incomplete: "
                f"{playlist_duration:.3f}s of {expected_duration:.3f}s"
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
        shutil.rmtree(
            os.path.join(output_base, f"video_{int(height)}p"),
            ignore_errors=True,
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
        row.name = f"{height}p"
        row.height = height
        row.width = rendition["width"]
        row.video_bitrate = rendition["bitrate"]
        row.audio_bitrate = 0
        row.codec = rendition["codec"]
        row.profile = "high"
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


def _runtime_limit(name: str, default: float) -> float:
    try:
        return float(getattr(get_settings(), name))
    except (AttributeError, TypeError, ValueError):
        return float(default)


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


def _rollback_quietly(db) -> None:
    rollback = getattr(db, "rollback", None)
    if callable(rollback):
        try:
            rollback()
        except Exception:
            pass


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
    seg = settings.get("segment_duration_sec", ffmpeg_utils.SEGMENT_DURATION)
    seg_fmt = settings.get("segment_format", "fmp4")
    preset = settings.get("video_preset", "p6")
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
            video.duration or 1.0,
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
                video.duration or 1.0,
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
            progress_tracker.start_task(
                video_id,
                f"transcode_{h}p",
                f"Transcoding {h}p video",
                job_id=job_id,
            )
        progress_started = True
        advance_job_status(job, models.JobStatus.transcoding.value)
        db.commit()

        local_source = ensure_local_source(job_id, source_url)

        fps = video.frame_rate or 30.0
        duration = video.duration or 1.0
        input_bytes = _file_size(local_source)
        complexity = video.complexity_score

        def on_progress(pct):
            for h in heights:
                progress_tracker.update_task(
                    video_id,
                    f"transcode_{h}p",
                    pct,
                    f"Transcoding {h}p video",
                    job_id=job_id,
                )

        attempt_root, attempt_output = _new_attempt_output(
            work_dir,
            _delivery_id(self),
        )
        encode_started = time.time()

        # Reserve one NVENC session per simultaneous rendition. The lease and
        # its heartbeat live in this Celery pool child, so a hard child death
        # cannot be hidden by the healthy parent worker's registration.
        try:
            gpu_lease = gpu_registry.acquire_gpu(
                worker_id,
                slots=len(norm_rend),
                preferred_index=gpu_index,
            )
        except Exception as exc:
            logger.warning("[transcode_group] acquire_gpu failed: %s", exc)
            gpu_lease = None
        used_gpu_index = (
            gpu_lease.gpu_index if gpu_lease is not None else None
        )

        fallback_reason = None
        if gpu_lease is not None:
            lease_heartbeat = gpu_registry.GPULeaseHeartbeat(gpu_lease)
            try:
                lease_heartbeat.start()
                cmd = ffmpeg_utils.transcode_multi_command(
                    local_source, attempt_output, norm_rend, fps,
                    gpu_index=used_gpu_index, segment_duration=seg, codec=codec,
                    segment_format=seg_fmt, preset=preset, require_gpu=True,
                )
                ffmpeg_utils.run_cmd_with_progress(
                    cmd,
                    duration,
                    on_progress,
                    cancel_event=lease_heartbeat.lost_event,
                    wall_timeout=_runtime_limit(
                        "GPU_FFMPEG_TIMEOUT_SEC",
                        5400,
                    ),
                )
            except ffmpeg_utils.FFmpegError as exc:
                logger.warning(
                    "[transcode_group] GPU multi-rendition failed for job=%s, "
                    "requesting bounded CPU replacement: %s",
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
            raise BoundedCPUFallbackRequired(fallback_reason, duration)

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
        encode_duration = time.time() - encode_started
        for r in norm_rend:
            h = r["height"]
            rdir = os.path.join(canonical_output, f"video_{h}p")
            _record_metric(
                db, job_id, f"{h}p", r["codec"], used_gpu_index, worker_id,
                fps=0.0, encode_duration_sec=encode_duration, input_bytes=input_bytes,
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
        for h in heights:
            progress_tracker.start_task(
                video_id,
                f"transcode_{h}p",
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
                progress_tracker.update_task(
                    video_id,
                    f"transcode_{h}p",
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
            try:
                lease_heartbeat.start()
                cmd = ffmpeg_utils.transcode_chunk_command(
                    local_source, chunk_dir, norm_rend,
                    float(start_sec), chunk_dur, fps,
                    gpu_index=used_gpu_index, segment_duration=seg, codec=codec, segment_format="ts",
                    require_gpu=True,
                )
                ffmpeg_utils.run_cmd_with_progress(
                    cmd,
                    chunk_dur,
                    on_progress,
                    cancel_event=lease_heartbeat.lost_event,
                    wall_timeout=_runtime_limit(
                        "GPU_FFMPEG_TIMEOUT_SEC",
                        5400,
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
            raise BoundedCPUFallbackRequired(fallback_reason, chunk_dur)

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
            fps=0.0, encode_duration_sec=chunk_dur, input_bytes=input_bytes,
            output_bytes=_dir_size(chunk_dir), complexity_score=complexity, commit=True,
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
    codec_default = settings.get("codec", "h264")
    seg_fmt = settings.get("segment_format", "fmp4")
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

        duration = video.duration or 1.0
        complexity = video.complexity_score
        work_dir = os.path.join(get_settings().WORK_DIR, job_id)
        out_base = os.path.join(work_dir, "output")
        os.makedirs(out_base, exist_ok=True)

        # Chunked path always uses MPEG-TS segments: TS concatenates cleanly
        # via the concat demuxer, unlike fMP4 fragments (no standalone moov).
        ext = "ts"
        is_fmp4 = False

        # Group chunk results by rendition height, ordered by chunk_index.
        by_height = {}
        meta = {}
        for chunk_result in (results or []):
            if not isinstance(chunk_result, dict):
                continue
            ci = chunk_result.get("chunk_index", 0)
            for rend in chunk_result.get("renditions", []):
                h = rend["height"]
                by_height.setdefault(h, []).append((ci, rend))
                meta[h] = {
                    "width": rend.get("width"),
                    "bitrate": rend.get("bitrate"),
                    "codec": rend.get("codec", codec_default),
                }

        rendition_specs = []
        started = time.time()
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
                if not cdir or not os.path.isdir(cdir):
                    continue
                seg_files = sorted(
                    (f for f in os.listdir(cdir) if f.endswith(f".{ext}")),
                    key=_natural_key,
                )
                for sf in seg_files:
                    seg_lines.append(f"file '{_abspath(os.path.join(cdir, sf))}'")
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
                db, job_id, f"{h}p", spec["codec"], None, worker_id,
                fps=0.0, encode_duration_sec=time.time() - started,
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
