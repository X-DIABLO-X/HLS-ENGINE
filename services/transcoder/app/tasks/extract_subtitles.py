"""Extract and package subtitle tracks."""

import logging
import os
import shutil

from app.celery_app import celery_app
from app.config import get_settings
from app.db import SessionLocal
from app import ffmpeg_utils, models
from app.job_fencing import (
    StaleJobError,
    ensure_local_source,
    lock_current_job,
    stale_result,
)
from app.job_lock import job_lock
from app.rabbitmq import publish_event
from app import progress as progress_tracker

logger = logging.getLogger(__name__)


def _validate_subtitle_output(
    playlist_dir: str,
    playlist_path: str,
    raw_path: str,
) -> None:
    if not os.path.isfile(raw_path) or os.path.getsize(raw_path) <= 0:
        raise ValueError("subtitle extraction produced no WebVTT data")
    with open(raw_path, "r", encoding="utf-8-sig") as handle:
        header = handle.read(6)
    if header != "WEBVTT":
        raise ValueError("subtitle extraction produced invalid WebVTT")
    if not os.path.isfile(playlist_path) or os.path.getsize(playlist_path) <= 0:
        raise ValueError("subtitle playlist is missing or empty")
    with open(playlist_path, "r", encoding="utf-8-sig") as handle:
        lines = [line.strip() for line in handle if line.strip()]
    if (
        not lines
        or lines[0] != "#EXTM3U"
        or "#EXT-X-ENDLIST" not in lines
    ):
        raise ValueError("subtitle playlist is incomplete")
    if not any(line.startswith("#EXT-X-TARGETDURATION:") for line in lines):
        raise ValueError("subtitle playlist is missing target duration")
    media = [line for line in lines if not line.startswith("#")]
    if not media:
        raise ValueError("subtitle playlist contains no WebVTT segments")
    for entry in media:
        referenced = os.path.abspath(os.path.join(playlist_dir, entry))
        if not os.path.isfile(referenced) or os.path.getsize(referenced) <= 0:
            raise ValueError("subtitle playlist references a missing WebVTT")
        with open(referenced, "r", encoding="utf-8-sig") as handle:
            segment_header = handle.read(256)
        if (
            not segment_header.startswith("WEBVTT")
            or "X-TIMESTAMP-MAP=" not in segment_header
        ):
            raise ValueError("subtitle segment is missing its HLS timestamp map")


def _run_extract_subtitles(self, job_id: str, source_url: str, sub_info: dict, settings: dict = None) -> dict:
    settings = settings or {}
    sub_info = dict(sub_info or {})
    lang = models.normalize_track_language(sub_info.get("language"))
    try:
        stream_index = max(0, int(sub_info.get("subtitle_index", 0)))
    except (TypeError, ValueError):
        stream_index = 0
    track_id = models.safe_track_identity(
        sub_info.get("track_id"), lang, stream_index
    )
    track_name = models.hls_attribute(
        sub_info.get("name"),
        models.track_display_name(sub_info, lang),
    )

    logger.info(
        "[extract_subtitles] job=%s lang=%s track=%s",
        job_id,
        lang,
        track_id,
    )
    task_name = f"subtitle_{track_id}"
    db = SessionLocal()
    job = None
    existing_sub = None
    try:
        job, _video = lock_current_job(db, job_id)
        db.commit()
        video_id = str(job.video_id)

        progress_tracker.start_task(
            video_id,
            task_name,
            f"Extracting subtitles ({track_name})",
            job_id=job_id,
        )

        work_dir = os.path.join(get_settings().WORK_DIR, job_id)
        local_source = os.path.join(work_dir, "source.mp4")
        raw_dir = os.path.join(work_dir, "subtitles", track_id)
        raw_path = os.path.join(raw_dir, "subtitles.vtt")
        playlist_dir = os.path.join(
            work_dir, "output", f"subtitles_{track_id}"
        )
        playlist_path = os.path.join(
            playlist_dir, "subtitles.m3u8"
        ).replace("\\", "/")
        row_id = models.deterministic_track_row_id(
            video_id, "subtitle", track_id
        )

        existing_sub = (
            db.query(models.Subtitle)
            .filter(
                models.Subtitle.video_id == video_id,
                models.Subtitle.id == row_id,
            )
            .first()
        )
        if existing_sub is None:
            existing_sub = (
                db.query(models.Subtitle)
                .filter(
                    models.Subtitle.video_id == video_id,
                    models.Subtitle.playlist_path == playlist_path,
                )
                .order_by(models.Subtitle.created_at.desc())
                .first()
            )
        if (
            existing_sub
            and existing_sub.playlist_path
            and os.path.exists(existing_sub.playlist_path)
        ):
            job, _video = lock_current_job(db, job_id)
            existing_sub = (
                db.query(models.Subtitle)
                .filter(
                    models.Subtitle.video_id == video_id,
                    models.Subtitle.id == row_id,
                )
                .first()
            )
            if existing_sub is None:
                raise RuntimeError(
                    "subtitle output exists without its current durable row"
                )
            existing_sub.language = lang
            existing_sub.name = track_name
            db.commit()
            progress_tracker.complete_task(
                video_id,
                task_name,
                job_id=job_id,
            )
            return {
                "type": "subtitle",
                "language": lang,
                "track_id": track_id,
                "name": track_name,
                "playlist": existing_sub.playlist_path,
                "reused": True,
            }

        local_source = ensure_local_source(job_id, source_url)

        os.makedirs(raw_dir, exist_ok=True)
        if os.path.exists(raw_path):
            os.remove(raw_path)
        if os.path.isdir(playlist_dir):
            shutil.rmtree(playlist_dir)

        cmd = ffmpeg_utils.extract_subtitle_command(local_source, raw_path, stream_index)
        ffmpeg_utils.run_cmd(cmd)

        video = db.query(models.Video).filter(models.Video.id == video_id).first()
        duration = video.duration if video and video.duration else 0
        segment_format = settings.get(
            "segment_format", get_settings().HLS_SEGMENT_FORMAT
        )
        playlist_path = ffmpeg_utils.package_subtitle(
            raw_path,
            playlist_dir,
            duration,
            segment_format=segment_format,
        ).replace("\\", "/")
        _validate_subtitle_output(playlist_dir, playlist_path, raw_path)

        job, _video = lock_current_job(db, job_id)
        sub = (
            db.query(models.Subtitle)
            .filter(
                models.Subtitle.video_id == video_id,
                models.Subtitle.id == row_id,
            )
            .first()
        )
        if sub is None:
            sub = models.Subtitle(
                id=row_id,
                video_id=video_id,
                language=lang,
            )
            db.add(sub)
        sub.language = lang
        sub.name = track_name
        sub.format = "webvtt"
        sub.file_path = raw_path.replace("\\", "/")
        sub.playlist_path = playlist_path
        db.commit()

        progress_tracker.complete_task(video_id, task_name, job_id=job_id)
        publish_event(
            "subtitle.extracted",
            {
                "job_id": job_id,
                "video_id": video_id,
                "language": lang,
                "track_id": track_id,
                "name": track_name,
            },
        )
        return {
            "type": "subtitle",
            "language": lang,
            "track_id": track_id,
            "name": track_name,
            "playlist": playlist_path,
        }
    except StaleJobError:
        raise
    except Exception as exc:
        if job:
            progress_tracker.fail_task(
                job.video_id,
                task_name,
                str(exc),
                job_id=job_id,
            )
        raise
    finally:
        db.close()


@celery_app.task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    max_retries=3,
)
def extract_subtitles(
    self,
    job_id: str,
    source_url: str,
    sub_info: dict,
    settings: dict = None,
) -> dict:
    """Serialize one subtitle identity and coordinate with package cleanup."""
    info = dict(sub_info or {})
    language = models.normalize_track_language(info.get("language"))
    try:
        stream_index = max(0, int(info.get("subtitle_index", 0)))
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
            purpose=f"extract_subtitle:{track_id}:job",
            shared=True,
        ):
            with job_lock(
                work_root,
                f"{job_id}:subtitle:{track_id}",
                purpose=f"extract_subtitle:{track_id}",
            ):
                return _run_extract_subtitles(
                    self,
                    job_id,
                    source_url,
                    info,
                    settings,
                )
    except StaleJobError as exc:
        logger.info(
            "[extract_subtitles] skipping stale job=%s track=%s: %s",
            job_id,
            track_id,
            exc,
        )
        return stale_result(job_id, "subtitle")
