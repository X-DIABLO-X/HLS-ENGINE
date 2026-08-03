"""Package HLS master playlist and atomically publish to MinIO."""

import logging
import os
import shutil
import stat

import redis

from app.celery_app import celery_app
from app.config import get_settings
from app.db import SessionLocal
from app import ffmpeg_utils, models
from app.job_fencing import (
    StaleJobError,
    advance_job_status,
    lock_current_job,
    stale_result,
)
from app.job_lock import job_lock
from app.minio_client import (
    atomic_publish,
    published_prefix_exists,
    withhold_published_prefix,
)
from app.rabbitmq import publish_event
from app import progress as progress_tracker

logger = logging.getLogger(__name__)


def _cache_published_version(video_id: str, version: str, prefix: str) -> None:
    """Refresh delivery routing after an initial publish or safe redelivery."""
    try:
        client = redis.from_url(get_settings().REDIS_URL)
        pipeline = client.pipeline(transaction=True)
        pipeline.set(
            f"video:{video_id}:version",
            version,
            ex=60 * 60 * 24 * 30,
        )
        pipeline.set(
            f"video:{video_id}:prefix",
            prefix,
            ex=60 * 60 * 24 * 30,
        )
        pipeline.execute()
    except Exception as exc:
        logger.warning("failed to cache video version in redis: %s", exc)


def _remove_completed_work_dir(work_dir: str) -> None:
    """Best-effort removal of large, reproducible local job artifacts."""
    try:
        work_root = os.path.abspath(get_settings().WORK_DIR)
        candidate = os.path.abspath(work_dir)
        if (
            os.path.islink(work_root)
            or os.path.dirname(candidate) != work_root
        ):
            logger.error(
                "refusing to remove work directory outside configured root: %s",
                work_dir,
            )
            return
        if os.path.lexists(candidate):
            mode = os.lstat(candidate).st_mode
            if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
                logger.error(
                    "refusing to remove non-directory or symlink workspace: %s",
                    work_dir,
                )
                return
            if (
                os.path.dirname(os.path.realpath(candidate))
                != os.path.realpath(work_root)
            ):
                logger.error(
                    "refusing to remove workspace resolving outside root: %s",
                    work_dir,
                )
                return
            shutil.rmtree(candidate)
            logger.info("Removed completed work directory: %s", candidate)
    except Exception:
        # Publication is already durable and the database is ready. Cleanup
        # failure must be visible, but it must not redeliver the whole job.
        logger.exception("failed to remove completed work directory: %s", work_dir)


def _refresh_completed_progress(video_id: str, job_id: str) -> None:
    """Best-effort progress repair after the durable completion commit."""
    for description, action in (
        (
            "mark package progress complete",
            lambda: progress_tracker.complete_task(
                video_id,
                "package",
                job_id=job_id,
            ),
        ),
        (
            "set video progress to ready",
            lambda: progress_tracker.set_percent(
                video_id,
                100,
                "Ready",
                job_id=job_id,
            ),
        ),
    ):
        try:
            action()
        except Exception:
            # Redis progress is a reconstructable cache. A cache outage after
            # MinIO and PostgreSQL committed must not redeliver packaging.
            logger.exception(
                "failed to %s for completed video %s",
                description,
                video_id,
            )


def _publish_completion_event(event_type: str, payload: dict) -> None:
    """Best-effort event emission after the durable completion commit."""
    try:
        publish_event(event_type, payload)
    except Exception:
        # There is no transactional outbox in this service. Retrying the
        # package task cannot guarantee this event either, because a redelivery
        # correctly takes the completed-job fast path. Keep the durable result
        # authoritative and make the missed notification visible in logs.
        logger.exception(
            "failed to publish %s after package completion",
            event_type,
        )


def _rel_path(from_dir: str, target: str) -> str:
    root = os.path.realpath(from_dir)
    destination = os.path.realpath(
        target if os.path.isabs(target) else os.path.join(root, target)
    )
    try:
        contained = os.path.commonpath([root, destination]) == root
    except ValueError:
        contained = False
    if not contained:
        raise ValueError(f"HLS asset is outside output directory: {target}")
    relative = os.path.relpath(destination, root).replace("\\", "/")
    if relative.startswith("../") or "\r" in relative or "\n" in relative:
        raise ValueError(f"unsafe HLS asset path: {target}")
    return relative


def _media_names(tracks: list) -> list:
    """Return HLS-safe, group-unique names without dropping duplicate languages."""
    names = []
    used = set()
    language_counts = {}
    for track in tracks:
        language = models.normalize_track_language(
            getattr(track, "language", "und")
        )
        ordinal = language_counts.get(language, 0) + 1
        language_counts[language] = ordinal
        fallback = language.upper()
        if ordinal > 1:
            fallback = f"{fallback} {ordinal}"
        requested = models.hls_attribute(
            getattr(track, "name", None), fallback
        )
        candidate = requested
        suffix = 1
        while candidate.casefold() in used:
            suffix += 1
            candidate = models.suffixed_hls_name(requested, suffix)
        used.add(candidate.casefold())
        names.append(candidate)
    return names


def _write_master(
    master_path: str,
    output_dir: str,
    renditions: list,
    audio_tracks: list,
    subtitles: list,
) -> None:
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:6",
        "#EXT-X-INDEPENDENT-SEGMENTS",
    ]

    if audio_tracks:
        audio_names = _media_names(audio_tracks)
        default_idx = next(
            (
                idx
                for idx, track in enumerate(audio_tracks)
                if bool(getattr(track, "default", False))
            ),
            0,
        )
        for idx, track in enumerate(audio_tracks):
            lang = models.normalize_track_language(track.language)
            name = audio_names[idx]
            uri = models.hls_attribute(
                _rel_path(output_dir, track.playlist_path)
            )
            lines.append(
                f'#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",NAME="{name}",'
                f'DEFAULT={"YES" if idx == default_idx else "NO"},AUTOSELECT=YES,'
                f'LANGUAGE="{lang}",URI="{uri}"'
            )

    if subtitles:
        subtitle_names = _media_names(subtitles)
        for idx, track in enumerate(subtitles):
            lang = models.normalize_track_language(track.language)
            name = subtitle_names[idx]
            uri = models.hls_attribute(
                _rel_path(output_dir, track.playlist_path)
            )
            lines.append(
                f'#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs",NAME="{name}",'
                f'DEFAULT=NO,AUTOSELECT=YES,LANGUAGE="{lang}",URI="{uri}"'
            )

    for rendition in renditions:
        bw = rendition.bandwidth or 1
        res = f"{rendition.width}x{rendition.height}"
        rcodec = rendition.codec or "h264"
        try:
            codecs = ffmpeg_utils.codecs_string(rcodec, rendition.height or 1080)
        except Exception:
            codecs = "avc1.640029"
        if audio_tracks:
            # RFC 8216 requires CODECS to enumerate every media format present
            # in the Variant Stream, including the referenced external audio
            # rendition group. Audio extraction publishes AAC-LC.
            codecs = f"{codecs},mp4a.40.2"
        attrs = [f'BANDWIDTH={bw}', f'CODECS="{codecs}"', f'RESOLUTION={res}']
        if audio_tracks:
            attrs.append('AUDIO="audio"')
        if subtitles:
            attrs.append('SUBTITLES="subs"')
        lines.append(f'#EXT-X-STREAM-INF:{",".join(attrs)}')
        lines.append(_rel_path(output_dir, rendition.playlist_path))

    with open(master_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def _run_package(
    self,
    results,
    job_id: str,
    source_url: str,
    version: str,
    db=None,
) -> dict:
    logger.info("[package] job=%s", job_id)
    owns_session = db is None
    if owns_session:
        db = SessionLocal()
    cleanup_work_dir = False
    work_dir = os.path.join(get_settings().WORK_DIR, job_id)
    try:
        # Keep the Video row lock through validation, publication and the
        # completion commit. Manual/watchdog retry creation therefore either
        # wins before publication starts or waits and observes a ready video.
        job, video = lock_current_job(db, job_id, allow_completed=True)

        version = version or "v1"
        terminal_state_present = (
            job.status == models.JobStatus.completed.value
            or video.status == "ready"
        )
        if terminal_state_present:
            if not (
                job.status == models.JobStatus.completed.value
                and video.status == "ready"
                and job.output_prefix
            ):
                raise RuntimeError(
                    "package job has an inconsistent terminal state or no "
                    "published output prefix"
                )
            if not published_prefix_exists(job.output_prefix):
                raise RuntimeError(
                    "completed package job is missing its master commit marker"
                )
            _cache_published_version(video.id, version, job.output_prefix)
            _refresh_completed_progress(str(video.id), job_id)
            cleanup_work_dir = True
            return {
                "job_id": job_id,
                "output_prefix": job.output_prefix,
                "reused": True,
            }

        advance_job_status(job, models.JobStatus.packaging.value)
        flush = getattr(db, "flush", None)
        if callable(flush):
            flush()

        progress_tracker.start_task(
            video.id,
            "package",
            "Packaging HLS output",
            job_id=job_id,
        )
        progress_tracker.set_percent(
            video.id,
            95,
            "Packaging HLS output",
            job_id=job_id,
        )
        output_dir = os.path.join(work_dir, "output")
        os.makedirs(output_dir, exist_ok=True)

        duration = video.duration or 1.0

        # Ensure audio tracks are packaged as HLS
        audio_tracks = (
            db.query(models.AudioTrack)
            .filter(models.AudioTrack.video_id == job.video_id)
            .order_by(
                models.AudioTrack.playlist_path.asc(),
                models.AudioTrack.created_at.asc(),
            )
            .all()
        )
        for track in audio_tracks:
            if not track.playlist_path or not os.path.exists(track.playlist_path):
                identity = models.track_identity_from_path(track, "audio")
                playlist_dir = os.path.join(output_dir, f"audio_{identity}")
                cmd = ffmpeg_utils.package_audio_command(track.file_path, playlist_dir)
                ffmpeg_utils.run_cmd(cmd)
                track.playlist_path = os.path.join(
                    playlist_dir, "audio.m3u8"
                ).replace("\\", "/")
            if not track.bandwidth:
                track.bandwidth = ffmpeg_utils.compute_directory_bitrate(
                    os.path.dirname(track.playlist_path), duration
                )
        if callable(flush):
            flush()

        # Ensure subtitles are packaged as HLS
        subtitles = (
            db.query(models.Subtitle)
            .filter(models.Subtitle.video_id == job.video_id)
            .order_by(
                models.Subtitle.playlist_path.asc(),
                models.Subtitle.created_at.asc(),
            )
            .all()
        )
        for sub in subtitles:
            if not sub.playlist_path or not os.path.exists(sub.playlist_path):
                identity = models.track_identity_from_path(
                    sub, "subtitles"
                )
                playlist_dir = os.path.join(
                    output_dir, f"subtitles_{identity}"
                )
                sub.playlist_path = ffmpeg_utils.package_subtitle(
                    sub.file_path, playlist_dir, duration
                ).replace("\\", "/")
        if callable(flush):
            flush()

        # Video renditions
        renditions = (
            db.query(models.Rendition)
            .filter(models.Rendition.video_id == job.video_id)
            .order_by(models.Rendition.height.desc())
            .all()
        )

        master_path = os.path.join(output_dir, "master.m3u8")
        _write_master(master_path, output_dir, renditions, audio_tracks, subtitles)

        # Atomic publish to MinIO
        shard = video.id[:2].lower()
        final_prefix = f"{shard}/{video.id}/{version}/"
        advance_job_status(job, models.JobStatus.publishing.value)
        if callable(flush):
            flush()
        publish_prefix = atomic_publish(output_dir, final_prefix)
        if not published_prefix_exists(publish_prefix):
            raise RuntimeError(
                "HLS publication returned without a durable master commit "
                "marker"
            )

        job.status = models.JobStatus.completed.value
        job.output_prefix = publish_prefix
        job.progress = 100.0
        video.status = "ready"
        try:
            db.commit()
        except BaseException:
            # Storage became discoverable but PostgreSQL did not commit the
            # ready state. Fail closed until this late-acknowledged package
            # delivery retries and can commit both sides successfully.
            try:
                withhold_published_prefix(publish_prefix)
            except Exception as cleanup_exc:
                logger.critical(
                    "database completion failed and published master could "
                    "not be withheld for job=%s prefix=%s",
                    job_id,
                    publish_prefix,
                    exc_info=True,
                )
                raise RuntimeError(
                    "package completion failed and storage could not be "
                    "made undiscoverable"
                ) from cleanup_exc
            raise
        cleanup_work_dir = True

        _refresh_completed_progress(str(video.id), job_id)

        # Cache the published version so the CDN edge can resolve it.
        _cache_published_version(video.id, version, publish_prefix)

        _publish_completion_event(
            "video.status_changed",
            {"video_id": video.id, "status": "ready"},
        )
        _publish_completion_event(
            "job.completed",
            {
                "job_id": job_id,
                "video_id": video.id,
                "output_prefix": publish_prefix,
            },
        )
        return {"job_id": job_id, "output_prefix": publish_prefix}
    finally:
        if owns_session:
            db.close()
        if cleanup_work_dir:
            _remove_completed_work_dir(work_dir)


@celery_app.task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    max_retries=3,
)
def package(self, results, job_id: str, source_url: str, version: str) -> dict:
    """Package under job and video-wide publication locks."""
    work_root = get_settings().WORK_DIR
    with job_lock(
        work_root,
        job_id,
        purpose="package",
    ):
        # Resolve the immutable parent only after the job-exclusive lock is
        # held. Different job IDs for one video then serialize on this key.
        lookup = SessionLocal()
        try:
            job = lookup.query(models.Job).filter(models.Job.id == job_id).first()
            if not job:
                raise ValueError(f"Job {job_id} not found")
            video_id = str(job.video_id)
            with job_lock(
                work_root,
                f"video:{video_id}:publish",
                purpose="package:video",
            ):
                try:
                    return _run_package(
                        self,
                        results,
                        job_id,
                        source_url,
                        version,
                        db=lookup,
                    )
                except StaleJobError as exc:
                    logger.info(
                        "[package] skipping stale job=%s video=%s: %s",
                        job_id,
                        video_id,
                        exc,
                    )
                    # All tasks for this job are excluded by the job lock, so a
                    # superseded chord callback can safely reclaim its workspace.
                    _remove_completed_work_dir(
                        os.path.join(work_root, job_id)
                    )
                    return {
                        **stale_result(job_id, "package"),
                        "video_id": video_id,
                    }
        finally:
            lookup.close()
