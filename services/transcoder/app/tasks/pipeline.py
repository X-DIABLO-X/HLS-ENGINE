"""End-to-end transcoding pipeline orchestration."""

import logging
import uuid
from datetime import datetime, timezone

from celery import Task, chord, group

from app.celery_app import celery_app
from app.config import get_settings
from app.db import SessionLocal
from app import ffmpeg_utils, gpu_registry, models
from app.transcode_safety import (
    CPU_FALLBACK_DEFAULT_SLICE_SEC,
    CPU_FALLBACK_MAX_RENDITIONS_PER_TASK,
    CPU_FALLBACK_MAX_TASKS,
    bounded_cpu_plan,
    validate_bounded_chunk_codecs,
)
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
from app.tasks.extract_audio import extract_audio
from app.tasks.extract_subtitles import extract_subtitles
from app.tasks.package import package
from app.tasks.probe import _run_probe
from app.tasks.thumbnail import thumbnail
from app.tasks.workspace_cleanup import request_workspace_cleanup
from app.tasks.transcode_video import (
    DIRECT_PLAY_PENDING,
    transcode_video,
    transcode_group,
    transcode_chunk,
    concat_segments,
)

logger = logging.getLogger(__name__)

GPU_VIDEO_QUEUE = "video"
CPU_VIDEO_QUEUE = "video_cpu"
CHUNK_TARGET_COUNT = 30
CHUNK_MIN_SLICE_SEC = 30.0
CHUNK_MAX_SLICE_SEC = 300.0


def _set_video_status(db, video_id: str, status: str) -> None:
    video = db.query(models.Video).filter(models.Video.id == video_id).first()
    if video:
        if video.status == "deleting":
            logger.info(
                "not changing tombstoned video=%s status to %s",
                video_id,
                status,
            )
            return
        video.status = status
        db.commit()
    publish_event("video.status_changed", {"video_id": video_id, "status": status})


def _run_probe_sync(job_id: str, source_url: str) -> dict:
    # Run the probe in-process. Calling probe.delay().get() from inside a task
    # is forbidden by Celery (raises E_WOULDBLOCK / "Never call result.get()
    # within a task!") because the worker would block waiting for a subtask it
    # cannot run. Invoking the plain function avoids the deadlock entirely.
    # run_pipeline owns the job-wide shared workspace lock for the entire
    # orchestration attempt. Calling the unlocked implementation avoids
    # recursively acquiring the same Windows advisory lock while retaining the
    # standalone probe task's normal locking wrapper.
    return _run_probe(job_id, source_url)


def _rendition_height(rendition) -> int:
    """Heights from int or dict rendition specs."""
    return rendition if isinstance(rendition, int) else rendition.get("height", 0)


def _partition_groups(renditions: list, gpu_count: int) -> list:
    """Split renditions into G balanced groups, largest renditions spread across GPUs.

    Sorts by height descending and round-robins into G buckets so the heaviest
    renditions land on different GPUs (1 GPU now, N GPUs later).
    """
    g = max(1, gpu_count or 1)
    ordered = sorted(renditions, key=_rendition_height, reverse=True)
    buckets = [[] for _ in range(g)]
    for i, r in enumerate(ordered):
        buckets[i % g].append(r)
    return [b for b in buckets if b]


def _live_gpu_indices(status: list) -> list:
    """Return unique GPU indices that currently have a heartbeat worker."""
    return list(_live_gpu_capacities(status))


def _live_gpu_capacities(status: list) -> dict:
    """Return ``{gpu_index: safe_session_capacity}`` for live workers.

    Duplicate registrations for one physical GPU use the smallest advertised
    capacity. A missing or malformed capacity is treated as one session so a
    bad status payload can reduce throughput but cannot oversubscribe NVENC.
    """
    capacities = {}
    for item in status or []:
        if item.get("worker_id") is None or item.get("index") is None:
            continue
        try:
            index = int(item["index"])
        except (TypeError, ValueError):
            continue
        try:
            capacity = max(1, int(item.get("capacity", 1)))
        except (TypeError, ValueError):
            capacity = 1
        if index in capacities:
            capacities[index] = min(capacities[index], capacity)
        else:
            capacities[index] = capacity
    return dict(sorted(capacities.items()))


def _partition_gpu_groups(renditions: list, status: list) -> list:
    """Assign deterministic, capacity-bounded rendition groups to live GPUs.

    Renditions are first spread across physical GPUs by descending height, as
    before. Each GPU bucket is then split so one grouped FFmpeg command never
    asks for more simultaneous NVENC sessions than that GPU advertised.
    Without a live GPU, one unpinned group is returned for the CPU queue.
    """
    ordered = sorted(renditions, key=_rendition_height, reverse=True)
    if not ordered:
        return []
    capacities = _live_gpu_capacities(status)
    if not capacities:
        return [(None, ordered)]

    indices = list(capacities)
    buckets = {index: [] for index in indices}
    for offset, rendition in enumerate(ordered):
        buckets[indices[offset % len(indices)]].append(rendition)

    routed_groups = []
    for index in indices:
        bucket = buckets[index]
        capacity = capacities[index]
        for offset in range(0, len(bucket), capacity):
            routed_groups.append(
                (index, bucket[offset : offset + capacity])
            )
    return routed_groups


def _video_queue(live_gpu_indices: list) -> str:
    """Route GPU work exclusively to NVENC workers, with explicit CPU fallback."""
    return GPU_VIDEO_QUEUE if live_gpu_indices else CPU_VIDEO_QUEUE


def _renditions_request_h264(renditions: list, default_codec: str) -> bool:
    """Reject direct play when any explicit rendition requests another codec."""
    if str(default_codec or "").strip().lower() != "h264":
        return False
    for rendition in renditions or []:
        if not isinstance(rendition, dict):
            continue
        requested = rendition.get("codec", default_codec)
        if str(requested or "").strip().lower() != "h264":
            return False
    return True


def _build_chunks(duration: float, chunk_duration: float) -> list:
    """Return [(start_sec, duration_sec), ...] covering [0, duration)."""
    if duration <= 0 or chunk_duration <= 0:
        return []
    chunks = []
    start = 0.0
    while start < duration:
        chunks.append((start, min(chunk_duration, duration - start)))
        start += chunk_duration
    return chunks


def _adaptive_chunk_duration(
    duration: float,
    base: float = 60.0,
    max_dur: float = CHUNK_MAX_SLICE_SEC,
) -> float:
    """Pick a chunk duration that balances parallelism vs overhead for long videos.

    Aim for roughly ``CHUNK_TARGET_COUNT`` tasks, while strictly bounding the
    media duration represented by any one late-acknowledged Celery delivery.
    This prevents a feature-length encode from approaching the broker's
    acknowledgement timeout even if a caller supplies an unsafe base value.
    """
    if duration <= 0:
        return min(max(float(base), CHUNK_MIN_SLICE_SEC), float(max_dur))
    safe_max = max(CHUNK_MIN_SLICE_SEC, float(max_dur))
    safe_base = min(max(float(base), CHUNK_MIN_SLICE_SEC), safe_max)
    target = duration / CHUNK_TARGET_COUNT  # e.g. ~30 chunks for a 2h movie
    rounded = round(target / 60.0) * 60.0
    return min(safe_max, max(safe_base, rounded))


def _cpu_fallback_plan(
    duration: float,
    renditions: list,
    settings: dict,
) -> tuple[list, list]:
    """Return one-rendition groups and bounded slices for CPU recovery.

    CPU fallback is a safety path, not the optional throughput-oriented chunk
    mode.  It is therefore always enabled when no GPU is live, clamps
    caller/Redis overrides to five minutes, and rejects an unbounded or
    broker-flooding plan before any task is published.
    """
    validate_bounded_chunk_codecs(
        renditions,
        settings.get("codec", "h264"),
    )
    return bounded_cpu_plan(
        duration,
        renditions,
        settings.get(
            "cpu_fallback_chunk_duration_sec",
            CPU_FALLBACK_DEFAULT_SLICE_SEC,
        ),
        rendition_height=_rendition_height,
    )


def _run_pipeline(
    self,
    job_id: str,
    source_url: str,
    video_id: str,
    version: str = "v1",
    renditions: list = None,
    audio_languages: list = None,
    subtitle_languages: list = None,
    settings: dict = None,
) -> dict:
    """Probe source, analyze per-title complexity, build a ladder, then fan out
    GPU-grouped transcode tasks (or chunked transcode + concat) in parallel with
    audio/subtitle/thumbnail, finishing with a packaging callback."""
    if not isinstance(settings, dict):
        raise ValueError(
            "pipeline settings snapshot is required; resolve defaults before "
            "publishing run_pipeline"
        )
    # Never consult mutable defaults during task execution. Celery autoretries
    # retain this serialized snapshot, so one job generation cannot change its
    # ladder, chunking, or direct-play mode between dispatch attempts.
    settings = dict(settings)
    codec = settings.get("codec", "h264")
    per_title = settings.get("per_title_encoding", True)
    chunked = settings.get("chunked_encoding", False)
    chunk_dur = settings.get("chunk_duration_sec", 60)
    chunk_min = settings.get("chunk_min_duration_sec", 600)

    db = SessionLocal()
    try:
        job, video = lock_current_job(db, job_id, allow_completed=True)
        if str(job.video_id) != str(video_id):
            raise StaleJobError(
                f"pipeline job {job_id} belongs to video {job.video_id}, "
                f"not {video_id}"
            )
        # A duplicate delivery must not regress or re-run an attempt that
        # already completed or successfully published its task graph.
        if job.status == models.JobStatus.completed.value:
            logger.info("[pipeline] job=%s already completed", job_id)
            return {
                "job_id": job_id,
                "video_id": video_id,
                "status": "completed",
            }
        if job.dispatch_count and job.dispatch_count > 0:
            logger.info("[pipeline] job=%s already dispatched", job_id)
            return {
                "job_id": job_id,
                "video_id": video_id,
                "status": "already_dispatched",
            }
        advance_job_status(job, models.JobStatus.probing.value)
        job.input_path = source_url
        video.status = "processing"
        db.commit()
        publish_event("job.created", {"job_id": job_id, "video_id": video_id, "source_url": source_url})
    finally:
        db.close()

    logger.info("[pipeline] probing job=%s", job_id)
    probe_result = _run_probe_sync(job_id, source_url)

    duration = probe_result.get("duration") or 0.0
    video_duration = probe_result.get("video_duration") or duration
    # Carry the video elementary-stream duration through the Celery graph.
    # Containers can legitimately have a longer audio tail; video validation
    # and chunk planning must not mistake that tail for missing frames.
    settings = dict(settings)
    settings["_source_video_duration"] = float(video_duration)
    src_height = probe_result.get("height") or 1080
    src_width = probe_result.get("width") or 1920
    direct_play_requested = bool(
        settings.get("video_passthrough_enabled", False)
    )
    direct_play_eligible, direct_play_reason = (
        ffmpeg_utils.h264_direct_play_eligibility(probe_result, codec)
    )
    if not _renditions_request_h264(renditions, codec):
        direct_play_eligible = False
        direct_play_reason = "an explicit rendition requests a non-h264 codec"
    direct_play = direct_play_requested and direct_play_eligible
    if direct_play_requested:
        logger.info(
            "[pipeline] H.264 direct-play job=%s eligible=%s reason=%s",
            job_id,
            direct_play,
            direct_play_reason,
        )

    # Adaptive chunk duration: scale chunk size with video length to keep the
    # number of chunks bounded and reduce concat/overhead, while still enabling
    # parallelism for long movies.
    use_chunked = (
        bool(chunked)
        and video_duration > chunk_min
        and video_duration > 0
    )
    chunks = []
    if use_chunked:
        chunk_dur = _adaptive_chunk_duration(
            video_duration,
            base=float(chunk_dur),
        )
        chunks = _build_chunks(video_duration, chunk_dur)
        logger.info(
            "[pipeline] adaptive chunk_dur=%.0f chunks=%d for duration=%.0f",
            chunk_dur,
            len(chunks),
            video_duration,
        )

    # The shared WORK_DIR volume means the probe's downloaded source is reusable.
    local_source = ensure_local_source(job_id, source_url)

    qualities = settings.get("qualities", [1080, 720, 480])

    # Direct play skips the content-analysis pass. Its encoded fallback uses
    # the normal configured ladder at neutral complexity.
    if direct_play:
        complexity = 0.5
        try:
            ladder = ffmpeg_utils.get_per_title_ladder(
                src_height,
                src_width,
                qualities,
                complexity,
                codec,
            )
        except Exception as exc:
            logger.warning(
                "[pipeline] direct-play fallback ladder failed; using "
                "the default ladder: %s",
                exc,
            )
            ladder = ffmpeg_utils.get_ladder_for_qualities(
                src_height,
                src_width,
                qualities,
            )
    elif per_title:
        try:
            complexity = ffmpeg_utils.analyze_complexity(
                local_source,
                video_duration,
            )
        except Exception as exc:
            logger.warning("[pipeline] complexity analysis failed for job=%s: %s", job_id, exc)
            complexity = 0.5
        try:
            ladder = ffmpeg_utils.get_per_title_ladder(
                src_height, src_width, qualities, complexity, codec,
            )
        except Exception as exc:
            logger.warning("[pipeline] get_per_title_ladder failed, using default ladder: %s", exc)
            ladder = ffmpeg_utils.get_ladder_for_qualities(src_height, src_width, qualities)
    else:
        complexity = 0.5
        ladder = ffmpeg_utils.get_ladder_for_qualities(src_height, src_width, qualities)

    # Persist the per-title analysis + complexity score on the video row.
    if per_title and not direct_play:
        try:
            db = SessionLocal()
            try:
                job, video = lock_current_job(db, job_id)
                video.complexity_score = complexity
                video.encoding_strategy = "per_title"
                analysis_id = str(
                    uuid.uuid5(
                        uuid.NAMESPACE_URL,
                        f"hls-engine:per-title:{video_id}:{job_id}",
                    )
                )
                analysis = (
                    db.query(models.PerTitleAnalysis)
                    .filter(models.PerTitleAnalysis.id == analysis_id)
                    .first()
                )
                if analysis is None:
                    analysis = models.PerTitleAnalysis(
                        id=analysis_id,
                        video_id=video_id,
                    )
                    db.add(analysis)
                analysis.complexity_score = complexity
                analysis.recommended_ladder = ladder
                analysis.analyzed_at = datetime.now(timezone.utc)
                db.commit()
            finally:
                db.close()
        except StaleJobError:
            raise
        except Exception as exc:
            logger.warning("[pipeline] failed to persist PerTitleAnalysis: %s", exc)

    # Caller-provided renditions override the computed ladder. Direct play
    # retains this complete ladder solely as its fail-safe replacement plan.
    normal_renditions = renditions if renditions else ladder
    if direct_play:
        selected_renditions = [
            {
                "height": int(probe_result["height"]),
                "width": int(probe_result["width"]),
                "bitrate": int(probe_result["video_bitrate"]),
                "codec": "h264",
                "profile": probe_result["video_profile"],
                "level": int(probe_result["video_level"]),
            }
        ]
    else:
        selected_renditions = normal_renditions

    audio_infos = probe_result.get("audio_tracks", [])
    if audio_languages:
        allowed_audio = {models.normalize_track_language(lang) for lang in audio_languages}
        audio_infos = [
            a
            for a in audio_infos
            if models.normalize_track_language(a.get("language")) in allowed_audio
        ]
    audio_infos = models.assign_track_identities(audio_infos, "audio_index")
    if audio_infos and not any(
        bool(a.get("default"))
        or bool(
            isinstance(a.get("disposition"), dict)
            and a["disposition"].get("default")
        )
        for a in audio_infos
    ):
        audio_infos[0]["default"] = True

    subtitle_infos = probe_result.get("subtitle_tracks", [])
    if subtitle_languages:
        allowed_subtitles = {
            models.normalize_track_language(lang) for lang in subtitle_languages
        }
        subtitle_infos = [
            s
            for s in subtitle_infos
            if models.normalize_track_language(s.get("language")) in allowed_subtitles
        ]
    subtitle_infos = models.assign_track_identities(
        subtitle_infos, "subtitle_index"
    )

    # Partition renditions across live GPU workers. The CPU worker deliberately
    # does not consume the GPU queue: sharing one queue caused Celery to send
    # expensive encodes to libx264 even while NVENC was idle.
    try:
        gpu_status = gpu_registry.get_gpu_status()
    except Exception:
        gpu_status = []
    live_gpu_indices = _live_gpu_indices(gpu_status)
    video_queue = _video_queue(live_gpu_indices)
    cpu_fallback_mode = not live_gpu_indices
    if direct_play:
        if cpu_fallback_mode:
            fallback_groups, fallback_chunks = _cpu_fallback_plan(
                video_duration,
                normal_renditions,
                settings,
            )
            fallback_routes = [
                {"gpu_index": None, "renditions": group_rend}
                for group_rend in fallback_groups
            ]
            fallback_use_chunked = True
            fallback_queue = CPU_VIDEO_QUEUE
            fallback_force_cpu = True
        else:
            if use_chunked:
                validate_bounded_chunk_codecs(normal_renditions, codec)
            fallback_routes = [
                {
                    "gpu_index": target_gpu,
                    "renditions": group_rend,
                }
                for target_gpu, group_rend in _partition_gpu_groups(
                    normal_renditions,
                    gpu_status,
                )
            ]
            fallback_chunks = chunks
            fallback_use_chunked = use_chunked
            fallback_queue = GPU_VIDEO_QUEUE
            fallback_force_cpu = False

        settings = dict(settings)
        settings["chunked_encoding"] = False
        settings["_video_direct_play"] = {
            "probe": {
                key: probe_result.get(key)
                for key in (
                    "duration",
                    "video_duration",
                    "width",
                    "height",
                    "video_codec",
                    "video_profile",
                    "video_level",
                    "video_pix_fmt",
                    "video_field_order",
                    "video_sample_aspect_ratio",
                    "video_rotation",
                    "video_bitrate",
                    "frame_rate",
                )
            },
            "fallback_renditions": normal_renditions,
            "fallback_routes": fallback_routes,
            "fallback_chunks": fallback_chunks,
            "fallback_use_chunked": fallback_use_chunked,
            "fallback_queue": fallback_queue,
            "fallback_force_cpu": fallback_force_cpu,
        }
        routed_groups = [(None, selected_renditions)]
        # Remux is CPU-routed orchestration/I/O work and never acquires NVENC.
        video_queue = CPU_VIDEO_QUEUE
        cpu_fallback_mode = False
        use_chunked = False
    elif cpu_fallback_mode:
        cpu_groups, chunks = _cpu_fallback_plan(
            video_duration,
            selected_renditions,
            settings,
        )
        routed_groups = [(None, group_rend) for group_rend in cpu_groups]
        # A CPU-only deployment must never fall through to the feature-length
        # grouped task, regardless of the optional CHUNKED_ENCODING setting.
        use_chunked = True
        chunk_dur = max(length for _start, length in chunks)
    else:
        if use_chunked:
            validate_bounded_chunk_codecs(
                selected_renditions,
                codec,
            )
        routed_groups = _partition_gpu_groups(
            selected_renditions,
            gpu_status,
        )

    rendition_task_names = [
        f"transcode_{height}p"
        for height in sorted(
            {_rendition_height(r) for r in selected_renditions},
            reverse=True,
        )
    ]
    if use_chunked:
        settings = progress_tracker.with_chunked_tasks(
            settings,
            rendition_task_names,
            len(chunks),
        )

    # Progress task names: one per rendition height (deduped) + audio/subtitle/thumbnail/package.
    task_names = ["probe"]
    task_names.extend(rendition_task_names)
    for audio in audio_infos:
        task_names.append(f"audio_{audio['track_id']}")
    for sub in subtitle_infos:
        task_names.append(f"subtitle_{sub['track_id']}")
    task_names.append("thumbnail")
    task_names.append("package")

    db = SessionLocal()
    try:
        _job, video = lock_current_job(db, job_id)
        if direct_play:
            # This generation-fenced row is the durable guard that prevents a
            # delayed direct remux from promoting a source rung after another
            # delivery has switched the job to the encoded fallback ladder.
            video.encoding_strategy = DIRECT_PLAY_PENDING
        progress_tracker.init_progress(
            video_id,
            len(task_names),
            task_names,
            job_id=job_id,
        )
        progress_tracker.complete_task(
            video_id,
            "probe",
            job_id=job_id,
        )
        if use_chunked:
            for task_name in rendition_task_names:
                progress_tracker.configure_chunked_task(
                    video_id,
                    task_name,
                    len(chunks),
                    stage=f"Preparing {len(chunks)} video chunks",
                    job_id=job_id,
                )
        db.commit()
    finally:
        db.close()

    # Build the chord header.
    header_sigs = []
    if use_chunked:
        chunk_sigs = []
        for target_gpu, group_rend in routed_groups:
            for ci, (start, dur) in enumerate(chunks):
                chunk_sigs.append(transcode_chunk.s(
                    job_id,
                    source_url,
                    group_rend,
                    start,
                    dur,
                    ci,
                    target_gpu,
                    settings,
                    cpu_fallback_mode,
                ).set(queue=video_queue))
        # Sub-chord: encode all chunks in parallel, then stitch per-rendition.
        video_sig = chord(group(*chunk_sigs), concat_segments.s(job_id, settings))
        header_sigs.append(video_sig)
    else:
        for target_gpu, group_rend in routed_groups:
            header_sigs.append(
                transcode_group.s(
                    job_id,
                    source_url,
                    group_rend,
                    target_gpu,
                    settings,
                ).set(queue=video_queue)
            )

    for audio in audio_infos:
        header_sigs.append(extract_audio.s(job_id, source_url, audio, settings))
    for sub in subtitle_infos:
        header_sigs.append(extract_subtitles.s(job_id, source_url, sub, settings))
    header_sigs.append(thumbnail.s(job_id, source_url, settings))

    # A header task that exhausts its own retries prevents the package body
    # from running. Attach an immutable errback so the DB/UI leave "processing"
    # immediately instead of waiting for the stuck-job watchdog.
    callback = package.s(job_id, source_url, version).on_error(
        on_pipeline_failure.si(video_id, job_id)
    )

    logger.info(
        "[pipeline] dispatching job=%s groups=%d routes=%s queue=%s "
        "chunked=%s cpu_fallback=%s renditions=%s",
        job_id,
        len(routed_groups),
        [
            {
                "gpu": target_gpu,
                "heights": [
                    _rendition_height(rendition)
                    for rendition in group_rend
                ],
            }
            for target_gpu, group_rend in routed_groups
        ],
        video_queue,
        use_chunked,
        cpu_fallback_mode,
        [_rendition_height(r) for r in selected_renditions],
    )

    # Serialize duplicate dispatch attempts on the Job row. Crucially, record
    # dispatch_count only after Celery accepts the chord. A broker failure now
    # rolls the transaction back and lets this autoretrying task publish again
    # instead of leaving a permanently "dispatched" job with no child tasks.
    db = SessionLocal()
    try:
        job, _video = lock_current_job(db, job_id, allow_completed=True)
        if job.status == models.JobStatus.completed.value:
            return {"job_id": job_id, "video_id": video_id, "status": "completed"}
        if job.dispatch_count and job.dispatch_count > 0:
            logger.info("[pipeline] job=%s already dispatched, skipping fan-out", job_id)
            return {
                "job_id": job_id,
                "video_id": video_id,
                "status": "already_dispatched",
            }

        chord(group(*header_sigs), callback).apply_async()
        job.dispatch_count = (job.dispatch_count or 0) + 1
        advance_job_status(job, models.JobStatus.queued.value)
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    return {"job_id": job_id, "video_id": video_id, "status": "dispatched"}


def _finalize_pipeline_failure(
    video_id: str,
    job_id: str,
    *,
    error_message: str,
    cleanup_trigger: str,
) -> None:
    """Generation-fenced final failure shared by dispatch and chord tasks."""
    db = SessionLocal()
    try:
        try:
            job, video = lock_current_job(db, job_id, allow_completed=True)
        except (StaleJobError, ValueError) as exc:
            logger.info(
                "[pipeline] ignoring delayed errback job=%s: %s",
                job_id,
                exc,
            )
            return
        if (
            job.status == models.JobStatus.completed.value
            or video.status == "ready"
        ):
            logger.info(
                "[pipeline] ignoring errback for completed job=%s",
                job_id,
            )
            return
        job.status = models.JobStatus.failed.value
        job.error_message = error_message
        video.status = "failed"
        db.commit()
        # Header tasks can still be unwinding when the first chord failure
        # arrives. The cleanup worker takes the job lock exclusively and
        # retries instead of racing those live mutators.
        side_effects = (
            (
                "workspace cleanup request",
                lambda: request_workspace_cleanup(
                    job_id,
                    trigger=cleanup_trigger,
                ),
            ),
            (
                "progress repair",
                lambda: progress_tracker.set_percent(
                    video_id,
                    0,
                    "Failed after retries",
                    job_id=job_id,
                ),
            ),
            (
                "status event",
                lambda: publish_event(
                    "video.status_changed",
                    {"video_id": video_id, "status": "failed"},
                ),
            ),
        )
        for description, action in side_effects:
            try:
                action()
            except Exception:
                # These systems may be the reason orchestration exhausted its
                # retries. Attempt every repair independently after the
                # generation-fenced database failure is durable.
                logger.exception(
                    "[pipeline] job=%s failed to perform %s",
                    job_id,
                    description,
                )
    finally:
        db.close()


class PipelineDispatchTask(Task):
    """Finalize a generation when orchestration itself exhausts retries."""

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        del task_id, einfo
        try:
            job_id = kwargs.get("job_id") if kwargs else None
            video_id = kwargs.get("video_id") if kwargs else None
            if job_id is None and len(args or ()) > 0:
                job_id = args[0]
            if video_id is None and len(args or ()) > 2:
                video_id = args[2]
            if job_id is None or video_id is None:
                logger.error(
                    "[pipeline] cannot finalize orchestration failure: "
                    "job/video arguments are missing"
                )
                return
            logger.error(
                "[pipeline] video=%s job=%s orchestration exhausted retries: %s",
                video_id,
                job_id,
                exc,
            )
            _finalize_pipeline_failure(
                str(video_id),
                str(job_id),
                error_message=(
                    "Pipeline orchestration exhausted retries "
                    f"({type(exc).__name__})"
                ),
                cleanup_trigger="pipeline-dispatch-failure",
            )
        except Exception:
            # Celery is already recording the terminal task failure. Never let
            # a secondary reporting/cleanup error hide that original cause.
            logger.exception(
                "[pipeline] failed to finalize orchestration failure"
            )


@celery_app.task(
    bind=True,
    base=PipelineDispatchTask,
    autoretry_for=(Exception,),
    retry_backoff=True,
    max_retries=2,
)
def run_pipeline(
    self,
    job_id: str,
    source_url: str,
    video_id: str,
    version: str = "v1",
    renditions: list = None,
    audio_languages: list = None,
    subtitle_languages: list = None,
    settings: dict = None,
) -> dict:
    """Claim one generation before any probe, download, or chord dispatch."""
    try:
        with job_lock(
            get_settings().WORK_DIR,
            job_id,
            purpose="pipeline-dispatch:job",
            shared=True,
        ):
            with job_lock(
                get_settings().WORK_DIR,
                f"{job_id}:pipeline",
                purpose="pipeline-dispatch",
            ):
                return _run_pipeline(
                    self,
                    job_id,
                    source_url,
                    video_id,
                    version,
                    renditions,
                    audio_languages,
                    subtitle_languages,
                    settings,
                )
    except StaleJobError as exc:
        logger.info("[pipeline] skipping stale delivery job=%s: %s", job_id, exc)
        return {
            **stale_result(job_id, "pipeline"),
            "video_id": video_id,
            "status": "superseded",
        }


@celery_app.task(bind=True)
def on_pipeline_failure(
    self, video_id: str, job_id: str, *args, **kwargs,
) -> None:
    del self, args, kwargs
    logger.error(
        "[pipeline] video=%s job=%s failed after task retries",
        video_id,
        job_id,
    )
    _finalize_pipeline_failure(
        video_id,
        job_id,
        error_message="Pipeline task exhausted retries",
        cleanup_trigger="pipeline-failure",
    )
