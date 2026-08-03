"""Extract and package audio tracks."""

import logging
import os
import shutil

from app.celery_app import celery_app
from app.config import get_settings
from app.db import SessionLocal
from app import ffmpeg_utils, models
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

logger = logging.getLogger(__name__)

AUDIO_MAX_RETRIES = 5


def _reset_audio_output(playlist_dir: str) -> None:
    """Remove every artifact from a prior/partial muxer attempt."""
    if os.path.isdir(playlist_dir):
        shutil.rmtree(playlist_dir)
    os.makedirs(playlist_dir, exist_ok=True)


def _audio_retry_stage(language: str, completed_retries: int, max_retries: int) -> str:
    """Human-readable state for the retry that Celery is about to schedule."""
    next_retry = min(completed_retries + 1, max_retries)
    return f"Retrying audio ({language}) {next_retry}/{max_retries}"


def _validate_audio_output(playlist_dir: str, playlist_path: str) -> None:
    """Reject partial/stale audio playlists before persisting a track row."""
    root = os.path.abspath(playlist_dir)
    playlist = os.path.abspath(playlist_path)
    try:
        if os.path.commonpath((root, playlist)) != root:
            raise ValueError("audio playlist escapes its output directory")
    except ValueError:
        raise ValueError("audio playlist has an invalid path")
    if not os.path.isfile(playlist) or os.path.getsize(playlist) <= 0:
        raise ValueError("audio playlist is missing or empty")

    with open(playlist, "r", encoding="utf-8-sig") as handle:
        lines = [line.strip() for line in handle if line.strip()]
    if not lines or lines[0] != "#EXTM3U":
        raise ValueError("audio playlist has an invalid HLS header")
    if "#EXT-X-ENDLIST" not in lines:
        raise ValueError("audio playlist is incomplete (ENDLIST missing)")
    media = [line for line in lines if not line.startswith("#")]
    extinf_count = sum(line.startswith("#EXTINF:") for line in lines)
    if not media or extinf_count != len(media):
        raise ValueError("audio playlist has an incomplete segment list")
    for uri in media:
        if (
            not uri
            or "\\" in uri
            or "\r" in uri
            or "\n" in uri
            or "://" in uri
            or os.path.isabs(uri)
        ):
            raise ValueError(f"audio playlist has an unsafe media URI: {uri!r}")
        asset = os.path.abspath(os.path.join(root, uri))
        try:
            contained = os.path.commonpath((root, asset)) == root
        except ValueError:
            contained = False
        if not contained or not os.path.isfile(asset) or os.path.getsize(asset) <= 0:
            raise ValueError(f"audio playlist references a missing segment: {uri}")


def _run_extract_audio(self, job_id: str, source_url: str, audio_info: dict, settings: dict = None) -> dict:
    settings = settings or {}
    audio_info = dict(audio_info or {})
    audio_bitrate = f"{settings.get('audio_bitrate_kbps', 128)}k"
    audio_channels = settings.get("audio_channels", 2)
    seg_dur = settings.get("segment_duration_sec", ffmpeg_utils.SEGMENT_DURATION)
    lang = models.normalize_track_language(audio_info.get("language"))
    try:
        stream_index = max(0, int(audio_info.get("audio_index", 0)))
    except (TypeError, ValueError):
        stream_index = 0
    track_id = models.safe_track_identity(
        audio_info.get("track_id"), lang, stream_index
    )
    track_name = models.hls_attribute(
        audio_info.get("name"),
        models.track_display_name(audio_info, lang),
    )
    disposition = audio_info.get("disposition")
    is_default = bool(
        audio_info.get("default")
        or (isinstance(disposition, dict) and disposition.get("default"))
    )

    logger.info(
        "[extract_audio] job=%s lang=%s track=%s", job_id, lang, track_id
    )
    task_name = f"audio_{track_id}"
    db = SessionLocal()
    job = None
    existing_track = None
    try:
        job, _video = lock_current_job(db, job_id)
        advance_job_status(job, models.JobStatus.extracting.value)
        db.commit()
        video_id = str(job.video_id)
        progress_tracker.start_task(
            video_id,
            task_name,
            f"Extracting audio ({track_name})",
            job_id=job_id,
        )

        work_dir = os.path.join(get_settings().WORK_DIR, job_id)
        local_source = os.path.join(work_dir, "source.mp4")
        raw_dir = os.path.join(work_dir, "audio", track_id)
        raw_path = os.path.join(raw_dir, "audio.m4a")
        playlist_dir = os.path.join(work_dir, "output", f"audio_{track_id}")
        playlist_path = os.path.join(
            playlist_dir, "audio.m3u8"
        ).replace("\\", "/")
        row_id = models.deterministic_track_row_id(
            video_id, "audio", track_id
        )

        # Late acknowledgements can redeliver a task after its DB commit. Treat
        # an already-materialized playlist as success instead of creating a
        # duplicate stream row or re-encoding the entire track. Identity/path,
        # not language, distinguishes two streams with the same language.
        existing_track = (
            db.query(models.AudioTrack)
            .filter(
                models.AudioTrack.video_id == video_id,
                models.AudioTrack.id == row_id,
            )
            .first()
        )
        if existing_track is None:
            existing_track = (
                db.query(models.AudioTrack)
                .filter(
                    models.AudioTrack.video_id == video_id,
                    models.AudioTrack.playlist_path == playlist_path,
                )
                .order_by(models.AudioTrack.created_at.desc())
                .first()
            )
        if (
            existing_track
            and existing_track.playlist_path
            and os.path.exists(existing_track.playlist_path)
        ):
            job, _video = lock_current_job(db, job_id)
            existing_track = (
                db.query(models.AudioTrack)
                .filter(
                    models.AudioTrack.video_id == video_id,
                    models.AudioTrack.id == row_id,
                )
                .first()
            )
            if existing_track is None:
                raise RuntimeError(
                    "audio output exists without its current durable row"
                )
            existing_track.language = lang
            existing_track.name = track_name
            existing_track.default = is_default
            db.commit()
            progress_tracker.complete_task(
                video_id,
                task_name,
                job_id=job_id,
            )
            return {
                "type": "audio",
                "language": lang,
                "track_id": track_id,
                "name": track_name,
                "playlist": existing_track.playlist_path,
                "reused": True,
            }

        local_source = ensure_local_source(job_id, source_url)

        channels = audio_info.get("channels", audio_channels)
        source_codec = audio_info.get("codec_name", "aac")
        # Preserve any intentional audio delay encoded in the source metadata
        # (e.g. MP4 edit list / Matroska offset). parse_probe populated this
        # as delay_ms = audio.start_time - video.start_time.
        audio_delay_ms = audio_info.get("delay_ms") or 0.0

        os.makedirs(raw_dir, exist_ok=True)
        if os.path.exists(raw_path):
            os.remove(raw_path)

        duration = 0.0
        video = db.query(models.Video).filter(models.Video.id == video_id).first()
        if video and video.duration:
            duration = video.duration
        elif not video:
            raise ValueError(f"Video {job.video_id} not found in database")

        bitrate_kbps = int(audio_bitrate.replace("k", ""))

        # A failed FFmpeg attempt can leave a valid-looking playlist that
        # references incomplete segments. Every retry starts atomically clean.
        _reset_audio_output(playlist_dir)
        loudnorm = settings.get("loudnorm", True) and source_codec.lower() not in ("aac", "mp3")

        def on_progress(pct):
            progress_tracker.update_task(
                video_id,
                task_name,
                pct,
                f"Extracting audio ({track_name})",
                job_id=job_id,
            )

        combined_ok = False
        if True:
            try:
                cmd = ffmpeg_utils.combined_audio_command(
                    local_source, playlist_dir,
                    stream_index=stream_index, language=lang,
                    bitrate_kbps=bitrate_kbps, channels=channels,
                    segment_duration=seg_dur, loudnorm=loudnorm,
                    source_codec=source_codec,
                    audio_delay_ms=audio_delay_ms,
                )
                ffmpeg_utils.run_cmd_with_progress(cmd, duration, on_progress)
                combined_ok = True
            except Exception as exc:
                logger.warning(
                    "[extract_audio] combined command failed for job=%s lang=%s, "
                    "falling back to two-step: %s", job_id, lang, exc,
                )
                # The combined muxer can leave valid-looking old/high-numbered
                # segments. Recreate the entire tree before the fallback.
                _reset_audio_output(playlist_dir)

        if not combined_ok:
            cmd = ffmpeg_utils.extract_audio_command(
                local_source, raw_path,
                stream_index=stream_index, language=lang,
                bitrate=audio_bitrate, channels=audio_channels,
                audio_delay_ms=audio_delay_ms,
            )
            ffmpeg_utils.run_cmd_with_progress(cmd, duration, on_progress)
            cmd2 = ffmpeg_utils.package_audio_command(raw_path, playlist_dir, seg_dur)
            ffmpeg_utils.run_cmd(cmd2)

        _validate_audio_output(playlist_dir, playlist_path)
        bitrate_val = bitrate_kbps * 1000

        # Retry creation may supersede this job while FFmpeg is running. Lock
        # the generation again before any video-wide row is recreated.
        job, _video = lock_current_job(db, job_id)
        track = (
            db.query(models.AudioTrack)
            .filter(
                models.AudioTrack.video_id == video_id,
                models.AudioTrack.id == row_id,
            )
            .first()
        )
        if track is None:
            track = models.AudioTrack(
                id=row_id,
                video_id=video_id,
                language=lang,
            )
            db.add(track)
        track.language = lang
        track.name = track_name
        track.default = is_default
        track.codec = "aac"
        track.bitrate = bitrate_val
        track.channels = channels
        track.delay_ms = audio_delay_ms
        track.file_path = raw_path.replace("\\", "/")
        track.playlist_path = playlist_path
        db.commit()

        progress_tracker.complete_task(video_id, task_name, job_id=job_id)
        publish_event(
            "audio.extracted",
            {
                "job_id": job_id,
                "video_id": video_id,
                "language": lang,
                "track_id": track_id,
                "name": track_name,
            },
        )
        return {
            "type": "audio",
            "language": lang,
            "track_id": track_id,
            "name": track_name,
            "playlist": playlist_path,
        }
    except StaleJobError:
        raise
    except Exception as exc:
        if job:
            completed_retries = int(getattr(self.request, "retries", 0) or 0)
            max_retries = int(getattr(self, "max_retries", AUDIO_MAX_RETRIES))
            if completed_retries < max_retries:
                stage = _audio_retry_stage(lang, completed_retries, max_retries)
                progress_tracker.update_task(
                    job.video_id,
                    task_name,
                    0,
                    stage,
                    job_id=job_id,
                )
                logger.warning(
                    "[extract_audio] job=%s lang=%s failed; %s: %s",
                    job_id,
                    lang,
                    stage,
                    exc,
                )
            else:
                progress_tracker.fail_task(
                    job.video_id,
                    task_name,
                    str(exc),
                    job_id=job_id,
                )
                logger.error(
                    "[extract_audio] job=%s lang=%s exhausted %d retries: %s",
                    job_id,
                    lang,
                    max_retries,
                    exc,
                )
        raise
    finally:
        db.close()


@celery_app.task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=2,
    retry_backoff_max=60,
    retry_jitter=True,
    max_retries=AUDIO_MAX_RETRIES,
)
def extract_audio(
    self,
    job_id: str,
    source_url: str,
    audio_info: dict,
    settings: dict = None,
) -> dict:
    """Serialize one audio identity and coordinate with package cleanup."""
    info = dict(audio_info or {})
    language = models.normalize_track_language(info.get("language"))
    try:
        stream_index = max(0, int(info.get("audio_index", 0)))
    except (TypeError, ValueError):
        stream_index = 0
    track_id = models.safe_track_identity(
        info.get("track_id"),
        language,
        stream_index,
    )
    work_root = get_settings().WORK_DIR
    try:
        with job_lock(
            work_root,
            job_id,
            purpose=f"extract_audio:{track_id}:job",
            shared=True,
        ):
            with job_lock(
                work_root,
                f"{job_id}:audio:{track_id}",
                purpose=f"extract_audio:{track_id}",
            ):
                return _run_extract_audio(
                    self,
                    job_id,
                    source_url,
                    info,
                    settings,
                )
    except StaleJobError as exc:
        logger.info(
            "[extract_audio] skipping stale job=%s track=%s: %s",
            job_id,
            track_id,
            exc,
        )
        return stale_result(job_id, "audio")
