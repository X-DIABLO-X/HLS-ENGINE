import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from app import models
from app.tasks import transcode_video as transcode_tasks
from app.tasks.transcode_video import (
    RenditionValidationError,
    _canonical_renditions_reusable,
    _chunk_group_key,
    _deterministic_rendition_id,
    _prepare_rendition_rows,
    _promote_rendition_directories,
    _validate_rendition_directory,
)


def _write_fmp4_rendition(
    output_base: Path,
    height: int,
    *,
    marker: bytes = b"segment",
    endlist: bool = True,
    stale_tail: bool = False,
) -> Path:
    rendition_dir = output_base / f"video_{height}p"
    rendition_dir.mkdir(parents=True, exist_ok=True)
    (rendition_dir / "init.mp4").write_bytes(b"init")
    (rendition_dir / "00000.m4s").write_bytes(marker)
    playlist = [
        "#EXTM3U",
        "#EXT-X-VERSION:7",
        '#EXT-X-MAP:URI="init.mp4"',
        "#EXTINF:6.000000,",
        "00000.m4s",
    ]
    if endlist:
        playlist.append("#EXT-X-ENDLIST")
    (rendition_dir / "video.m3u8").write_text(
        "\n".join(playlist) + "\n",
        encoding="utf-8",
    )
    if stale_tail:
        (rendition_dir / "00999.m4s").write_bytes(b"stale")
    return rendition_dir


class _FakeQuery:
    def __init__(self, session, model):
        self.session = session
        self.model = model

    def filter(self, *_criteria):
        return self

    def first(self):
        if self.model is models.Job:
            return self.session.job
        if self.model is models.Video:
            return self.session.video
        return None

    def all(self):
        if self.model is models.Rendition:
            return list(self.session.rows)
        return []


class _FakeSession:
    def __init__(self, rows=None, job=None, video=None):
        self.rows = list(rows or [])
        self.job = job
        self.video = video
        self.added = []
        self.metrics = []
        self.deleted = []
        self.commit_count = 0
        self.rollback_count = 0

    def query(self, model):
        return _FakeQuery(self, model)

    def add(self, row):
        self.added.append(row)
        if isinstance(row, models.Rendition):
            self.rows.append(row)
        else:
            self.metrics.append(row)

    def delete(self, row):
        self.deleted.append(row)
        self.rows.remove(row)

    def commit(self):
        self.commit_count += 1

    def refresh(self, _row):
        pass

    def rollback(self):
        self.rollback_count += 1

    def close(self):
        pass


def _spec(height=720, width=1280, bitrate=3_000_000):
    return {
        "height": height,
        "width": width,
        "bitrate": bitrate,
        "codec": "h264",
    }


def _row(
    video_id: str,
    output_base: Path,
    *,
    height=720,
    width=1280,
    bitrate=3_000_000,
    row_id=None,
):
    rendition_dir = output_base / f"video_{height}p"
    return models.Rendition(
        id=row_id or f"row-{height}",
        video_id=video_id,
        name=f"{height}p",
        height=height,
        width=width,
        video_bitrate=bitrate,
        audio_bitrate=0,
        codec="h264",
        profile="high",
        segment_path=os.path.join(
            str(rendition_dir),
            "%05d.m4s",
        ).replace("\\", "/"),
        playlist_path=os.path.join(
            str(rendition_dir),
            "video.m3u8",
        ).replace("\\", "/"),
        bandwidth=1,
    )


class SinglePassIdempotencyTests(unittest.TestCase):
    def test_chunk_group_identity_is_stable_and_disjoint(self):
        self.assertEqual(
            _chunk_group_key([{"height": 720}, {"height": 1080}]),
            "720-1080",
        )
        self.assertEqual(_chunk_group_key([360]), "360")
        self.assertNotEqual(
            _chunk_group_key([{"height": 720}, {"height": 1080}]),
            _chunk_group_key([360]),
        )

    def test_completed_group_redelivery_reuses_db_after_local_cleanup(self):
        video_id = "video-completed"
        job_id = "job-completed"
        spec = _spec()
        job = SimpleNamespace(
            id=job_id,
            video_id=video_id,
            status=models.JobStatus.completed.value,
            output_prefix="vi/video-completed/v1/",
        )
        video = SimpleNamespace(
            id=video_id,
            status="ready",
            width=1920,
            height=1080,
            frame_rate=24.0,
            duration=6.0,
            complexity_score=0.5,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            canonical = Path(temp_dir) / job_id / "output"
            db = _FakeSession(
                [_row(video_id, canonical)],
                job=job,
                video=video,
            )
            with (
                patch.object(transcode_tasks, "SessionLocal", return_value=db),
                patch.object(
                    transcode_tasks,
                    "get_settings",
                    return_value=SimpleNamespace(WORK_DIR=temp_dir),
                ),
                patch.object(
                    transcode_tasks.gpu_registry,
                    "acquire_gpu",
                ) as acquire,
                patch.object(
                    transcode_tasks.ffmpeg_utils,
                    "transcode_multi_command",
                ) as command,
                patch.object(
                    transcode_tasks.progress_tracker,
                    "start_task",
                ) as start_task,
                patch.object(transcode_tasks, "publish_event") as publish,
            ):
                result = transcode_tasks.transcode_group.run(
                    job_id,
                    "minio://unused/source.mkv",
                    [spec],
                    None,
                    {"segment_format": "fmp4"},
                )

            self.assertFalse((Path(temp_dir) / job_id).exists())
            self.assertTrue((Path(temp_dir) / ".job-locks").is_dir())

        self.assertTrue(result["reused"])
        self.assertTrue(result["durable"])
        acquire.assert_not_called()
        command.assert_not_called()
        start_task.assert_not_called()
        publish.assert_not_called()

    def test_stale_delivery_after_packaging_does_not_reset_progress_or_encode(self):
        video_id = "video-packaging"
        job_id = "job-packaging"
        spec = _spec()
        job = SimpleNamespace(
            id=job_id,
            video_id=video_id,
            status=models.JobStatus.packaging.value,
        )
        video = SimpleNamespace(
            id=video_id,
            width=1920,
            height=1080,
            frame_rate=24.0,
            duration=6.0,
            complexity_score=0.5,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            work_dir = Path(temp_dir) / job_id
            canonical = work_dir / "output"
            _write_fmp4_rendition(canonical, 720)
            db = _FakeSession(
                [_row(video_id, canonical)],
                job=job,
                video=video,
            )
            with (
                patch.object(transcode_tasks, "SessionLocal", return_value=db),
                patch.object(
                    transcode_tasks,
                    "get_settings",
                    return_value=SimpleNamespace(WORK_DIR=temp_dir),
                ),
                patch.object(
                    transcode_tasks.gpu_registry,
                    "acquire_gpu",
                ) as acquire,
                patch.object(
                    transcode_tasks.ffmpeg_utils,
                    "transcode_multi_command",
                ) as command,
                patch.object(
                    transcode_tasks.progress_tracker,
                    "start_task",
                ) as start_task,
                patch.object(
                    transcode_tasks.progress_tracker,
                    "fail_task",
                ) as fail_task,
                patch.object(transcode_tasks, "publish_event") as publish,
            ):
                result = transcode_tasks.transcode_group.run(
                    job_id,
                    "minio://unused/source.mkv",
                    [spec],
                    None,
                    {"segment_format": "fmp4"},
                )

        self.assertTrue(result["reused"])
        start_task.assert_not_called()
        fail_task.assert_not_called()
        acquire.assert_not_called()
        command.assert_not_called()
        publish.assert_not_called()

    def test_group_redelivery_reuses_output_and_releases_gpu_once(self):
        video_id = "video-task"
        job_id = "job-task"
        specs = [
            _spec(),
            _spec(height=480, width=854, bitrate=1_500_000),
        ]
        job = SimpleNamespace(
            id=job_id,
            video_id=video_id,
            status=models.JobStatus.queued.value,
        )
        video = SimpleNamespace(
            id=video_id,
            width=1920,
            height=1080,
            frame_rate=24.0,
            duration=6.0,
            complexity_score=0.5,
        )
        db = _FakeSession(job=job, video=video)
        gpu_lease = transcode_tasks.gpu_registry.GPULease(
            gpu_index=0,
            lease_id="test-group-lease",
            worker_id="test-worker",
            slots=len(specs),
            managed=False,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            work_dir = Path(temp_dir) / job_id
            work_dir.mkdir(parents=True)
            (work_dir / "source.mp4").write_bytes(b"source")

            def fake_multi_command(_source, output_base, renditions, *_args, **_kwargs):
                for rendition in renditions:
                    _write_fmp4_rendition(
                        Path(output_base),
                        rendition["height"],
                    )
                return ["ffmpeg"]

            with (
                patch.object(transcode_tasks, "SessionLocal", return_value=db),
                patch.object(
                    transcode_tasks,
                    "get_settings",
                    return_value=SimpleNamespace(WORK_DIR=temp_dir),
                ),
                patch.object(
                    transcode_tasks,
                    "ensure_local_source",
                    return_value=str(work_dir / "source.mp4"),
                ),
                patch.object(
                    transcode_tasks.ffmpeg_utils,
                    "transcode_multi_command",
                    side_effect=fake_multi_command,
                ) as command,
                patch.object(
                    transcode_tasks.ffmpeg_utils,
                    "run_cmd_with_progress",
                ),
                patch.object(
                    transcode_tasks.gpu_registry,
                    "acquire_gpu",
                    return_value=gpu_lease,
                ) as acquire,
                patch.object(
                    transcode_tasks,
                    "_worker_id",
                    return_value="test-worker",
                ),
                patch.object(
                    transcode_tasks.gpu_registry,
                    "release_gpu",
                ) as release,
                patch.object(transcode_tasks.progress_tracker, "start_task"),
                patch.object(transcode_tasks.progress_tracker, "update_task"),
                patch.object(transcode_tasks.progress_tracker, "complete_task"),
                patch.object(transcode_tasks.progress_tracker, "fail_task"),
                patch.object(transcode_tasks, "publish_event"),
            ):
                first = transcode_tasks.transcode_group.run(
                    job_id,
                    "minio://unused/source.mkv",
                    specs,
                    None,
                    {"segment_format": "fmp4"},
                )
                commits_after_first = db.commit_count
                second = transcode_tasks.transcode_group.run(
                    job_id,
                    "minio://unused/source.mkv",
                    specs,
                    None,
                    {"segment_format": "fmp4"},
                )

        self.assertFalse(first["reused"])
        self.assertTrue(second["reused"])
        self.assertEqual(command.call_count, 1)
        acquire.assert_called_once_with(
            "test-worker",
            slots=len(specs),
            preferred_index=None,
        )
        release.assert_called_once_with(gpu_lease)
        self.assertEqual(commits_after_first, 2)
        self.assertEqual(db.commit_count, 3)
        self.assertEqual(len(db.rows), 2)
        self.assertEqual(
            {row.height for row in db.rows},
            {480, 720},
        )
        self.assertEqual(db.rollback_count, 0)

    def test_retry_reuses_valid_canonical_and_keeps_one_stable_row(self):
        video_id = "video-1"
        with tempfile.TemporaryDirectory() as temp_dir:
            canonical = Path(temp_dir) / "output"
            _write_fmp4_rendition(canonical, 720)
            db = _FakeSession()

            rows, _details = _prepare_rendition_rows(
                db,
                video_id,
                str(canonical),
                [_spec()],
                "fmp4",
                6.0,
            )
            first_id = rows[0].id
            db.commit()

            self.assertTrue(
                _canonical_renditions_reusable(
                    db,
                    video_id,
                    str(canonical),
                    [_spec()],
                    "fmp4",
                )
            )

            rows, _details = _prepare_rendition_rows(
                db,
                video_id,
                str(canonical),
                [_spec()],
                "fmp4",
                6.0,
            )
            db.commit()

        self.assertEqual(first_id, _deterministic_rendition_id(video_id, 720))
        self.assertEqual(rows[0].id, first_id)
        self.assertEqual(len(db.rows), 1)
        self.assertEqual(db.commit_count, 2)

    def test_promotion_replaces_whole_directory_and_drops_stale_tail(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            canonical = root / "output"
            attempts = root / "attempt" / "output"
            old_dir = _write_fmp4_rendition(
                canonical,
                720,
                marker=b"old",
                stale_tail=True,
            )
            _write_fmp4_rendition(
                attempts,
                720,
                marker=b"new",
            )

            with self.assertRaises(RenditionValidationError):
                _validate_rendition_directory(str(old_dir), ".m4s")

            _promote_rendition_directories(
                str(attempts),
                str(canonical),
                [_spec()],
                "fmp4",
            )

            promoted = canonical / "video_720p"
            self.assertEqual((promoted / "00000.m4s").read_bytes(), b"new")
            self.assertFalse((promoted / "00999.m4s").exists())
            _validate_rendition_directory(str(promoted), ".m4s")

    def test_endlist_does_not_make_a_short_partial_attempt_complete(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            rendition_dir = _write_fmp4_rendition(
                Path(temp_dir) / "output",
                720,
            )
            with self.assertRaisesRegex(
                RenditionValidationError,
                "duration is incomplete",
            ):
                _validate_rendition_directory(
                    str(rendition_dir),
                    ".m4s",
                    expected_duration=600.0,
                )

    def test_duplicate_rows_are_removed_without_an_internal_commit(self):
        video_id = "video-2"
        with tempfile.TemporaryDirectory() as temp_dir:
            canonical = Path(temp_dir) / "output"
            _write_fmp4_rendition(canonical, 720)
            stable_id = _deterministic_rendition_id(video_id, 720)
            stable = _row(
                video_id,
                canonical,
                row_id=stable_id,
            )
            duplicate = _row(
                video_id,
                canonical,
                row_id="ffffffff-ffff-ffff-ffff-ffffffffffff",
            )
            db = _FakeSession([duplicate, stable])

            rows, _details = _prepare_rendition_rows(
                db,
                video_id,
                str(canonical),
                [_spec()],
                "fmp4",
                6.0,
            )

            self.assertEqual(db.commit_count, 0)
            self.assertEqual(rows, [stable])
            self.assertEqual(db.rows, [stable])
            self.assertEqual(db.deleted, [duplicate])
            db.commit()

        self.assertEqual(db.commit_count, 1)

    def test_invalid_second_rendition_causes_no_row_mutation(self):
        video_id = "video-3"
        with tempfile.TemporaryDirectory() as temp_dir:
            canonical = Path(temp_dir) / "output"
            _write_fmp4_rendition(canonical, 720)
            _write_fmp4_rendition(canonical, 480, endlist=False)
            existing = _row(video_id, canonical)
            db = _FakeSession([existing])

            with self.assertRaises(RenditionValidationError):
                _prepare_rendition_rows(
                    db,
                    video_id,
                    str(canonical),
                    [
                        _spec(),
                        _spec(height=480, width=854, bitrate=1_500_000),
                    ],
                    "fmp4",
                    6.0,
                )

        self.assertEqual(db.rows, [existing])
        self.assertEqual(db.added, [])
        self.assertEqual(db.deleted, [])
        self.assertEqual(db.commit_count, 0)


if __name__ == "__main__":
    unittest.main()
