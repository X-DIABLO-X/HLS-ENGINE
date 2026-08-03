import os
import tempfile
import threading
import unittest
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from pydantic import ValidationError

from app import ffmpeg_utils, models
from app.config import Settings
from app.tasks import pipeline
from app.tasks import transcode_video as transcode_tasks


def _spec(height, width, bitrate):
    return {
        "height": height,
        "width": width,
        "bitrate": bitrate,
        "codec": "h264",
    }


FOUR_RUNGS = [
    _spec(1080, 1920, 6_000_000),
    _spec(720, 1280, 3_000_000),
    _spec(480, 854, 1_500_000),
    _spec(360, 640, 800_000),
]


def _walk_canvas(canvas):
    yield canvas
    tasks = getattr(canvas, "tasks", None)
    if tasks:
        for task in tasks:
            yield from _walk_canvas(task)
    body = getattr(canvas, "body", None)
    if body is not None:
        yield from _walk_canvas(body)


class _EmptyQuery:
    def filter(self, *_args):
        return self

    def all(self):
        return []


class _FakeSession:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0

    def query(self, _model):
        return _EmptyQuery()

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        pass


class _Heartbeat:
    def __init__(self):
        self.lost_event = threading.Event()

    def start(self):
        pass

    def stop(self):
        pass


class CPUFallbackSafetyTests(unittest.TestCase):
    def test_no_gpu_four_rung_movie_plan_is_capacity_bounded(self):
        groups, chunks = pipeline._cpu_fallback_plan(
            7177.832,
            FOUR_RUNGS,
            {"cpu_fallback_chunk_duration_sec": 120},
        )

        self.assertEqual(len(groups), 4)
        self.assertTrue(all(len(group) == 1 for group in groups))
        self.assertEqual([group[0]["height"] for group in groups], [1080, 720, 480, 360])
        self.assertEqual(len(chunks), 60)
        self.assertTrue(all(0 < duration <= 120 for _start, duration in chunks))
        self.assertAlmostEqual(sum(duration for _start, duration in chunks), 7177.832)
        self.assertLessEqual(
            len(groups) * len(chunks),
            pipeline.CPU_FALLBACK_MAX_TASKS,
        )

    def test_long_gpu_replacement_routes_every_slice_to_cpu(self):
        canvas = transcode_tasks._bounded_cpu_group_canvas(
            "job-1",
            "minio://uploads/source.mkv",
            FOUR_RUNGS,
            7177.832,
            {
                "cpu_fallback_chunk_duration_sec": 120,
                "cpu_video_preset": "veryfast",
            },
        )
        chunk_signatures = [
            signature
            for signature in _walk_canvas(canvas)
            if getattr(signature, "task", None)
            == transcode_tasks.transcode_chunk.name
        ]

        self.assertEqual(len(chunk_signatures), 240)
        for signature in chunk_signatures:
            self.assertEqual(signature.options.get("queue"), "video_cpu")
            self.assertEqual(len(signature.args[2]), 1)
            self.assertGreater(signature.args[4], 0)
            self.assertLessEqual(signature.args[4], 120)
            self.assertTrue(signature.args[8])
        self.assertEqual(canvas.body.task, transcode_tasks.concat_segments.name)
        self.assertEqual(canvas.body.options.get("queue"), "package")

    def test_group_gpu_failure_never_runs_feature_length_cpu_inside_task(self):
        job_id = "job-gpu-loss"
        video_id = "video-gpu-loss"
        job = SimpleNamespace(
            id=job_id,
            video_id=video_id,
            status=models.JobStatus.queued.value,
        )
        video = SimpleNamespace(
            id=video_id,
            status="processing",
            width=1920,
            height=1080,
            frame_rate=24.0,
            duration=7177.832,
            complexity_score=0.5,
        )
        db = _FakeSession()
        lease = transcode_tasks.gpu_registry.GPULease(
            gpu_index=0,
            lease_id="lease-1",
            worker_id="worker-1",
            slots=2,
            managed=False,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            source = Path(temp_dir) / "source.mkv"
            source.write_bytes(b"source")
            with (
                patch.object(transcode_tasks, "SessionLocal", return_value=db),
                patch.object(
                    transcode_tasks,
                    "get_settings",
                    return_value=SimpleNamespace(
                        WORK_DIR=temp_dir,
                        GPU_FFMPEG_TIMEOUT_SEC=5400,
                        CPU_VIDEO_PRESET="veryfast",
                    ),
                ),
                patch.object(
                    transcode_tasks,
                    "lock_current_job",
                    return_value=(job, video),
                ),
                patch.object(
                    transcode_tasks,
                    "ensure_local_source",
                    return_value=str(source),
                ),
                patch.object(
                    transcode_tasks.gpu_registry,
                    "acquire_gpu",
                    return_value=lease,
                ),
                patch.object(
                    transcode_tasks.gpu_registry,
                    "GPULeaseHeartbeat",
                    return_value=_Heartbeat(),
                ),
                patch.object(
                    transcode_tasks.gpu_registry,
                    "release_gpu",
                ) as release,
                patch.object(
                    transcode_tasks.ffmpeg_utils,
                    "transcode_multi_command",
                    return_value=["ffmpeg", "gpu"],
                ) as multi_command,
                patch.object(
                    transcode_tasks.ffmpeg_utils,
                    "run_cmd_with_progress",
                    side_effect=ffmpeg_utils.FFmpegError("lease lost"),
                ) as run,
                patch.object(
                    transcode_tasks.ffmpeg_utils,
                    "transcode_video_command",
                ) as forbidden_cpu_command,
                patch.object(transcode_tasks.progress_tracker, "start_task"),
                patch.object(transcode_tasks.progress_tracker, "update_task"),
                patch.object(transcode_tasks.progress_tracker, "fail_task") as fail_task,
                patch.object(transcode_tasks, "publish_event"),
                patch.object(transcode_tasks, "_worker_id", return_value="worker-1"),
            ):
                with self.assertRaises(
                    transcode_tasks.BoundedCPUFallbackRequired
                ) as raised:
                    transcode_tasks._run_transcode_group(
                        Mock(request=SimpleNamespace(id="delivery-1")),
                        job_id,
                        "minio://uploads/source.mkv",
                        FOUR_RUNGS[:2],
                        0,
                        {"segment_format": "fmp4", "video_preset": "p6"},
                    )

        self.assertAlmostEqual(raised.exception.duration, 7177.832)
        multi_command.assert_called_once()
        self.assertTrue(multi_command.call_args.kwargs["require_gpu"])
        self.assertEqual(run.call_args.kwargs["wall_timeout"], 5400)
        forbidden_cpu_command.assert_not_called()
        fail_task.assert_not_called()
        release.assert_called_once_with(lease)

    def test_failed_gpu_chunk_replacement_merges_one_rung_results(self):
        canvas = transcode_tasks._bounded_cpu_chunk_canvas(
            "job-2",
            "minio://uploads/source.mkv",
            FOUR_RUNGS[:2],
            240.0,
            120.0,
            2,
            {"cpu_video_preset": "veryfast"},
        )
        signatures = [
            signature
            for signature in _walk_canvas(canvas)
            if getattr(signature, "task", None)
            == transcode_tasks.transcode_chunk.name
        ]
        self.assertEqual(len(signatures), 2)
        self.assertTrue(all(len(signature.args[2]) == 1 for signature in signatures))
        self.assertTrue(all(signature.args[8] for signature in signatures))
        self.assertEqual(canvas.body.task, transcode_tasks.merge_chunk_results.name)

        merged = transcode_tasks.merge_chunk_results.run(
            [
                {
                    "job_id": "job-2",
                    "chunk_index": 2,
                    "renditions": [{"height": 720, "dir": "/tmp/720"}],
                },
                {
                    "job_id": "job-2",
                    "chunk_index": 2,
                    "renditions": [{"height": 1080, "dir": "/tmp/1080"}],
                },
            ],
            "job-2",
            2,
            [720, 1080],
        )
        self.assertEqual(
            [rendition["height"] for rendition in merged["renditions"]],
            [1080, 720],
        )
        self.assertIsNone(merged["gpu_index"])

    def test_cpu_preset_is_valid_even_if_a_gpu_token_leaks_in(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.object(
                ffmpeg_utils,
                "is_gpu_available",
                return_value=False,
            ) as gpu_probe:
                command = ffmpeg_utils.transcode_chunk_command(
                    "source.mkv",
                    os.path.join(temp_dir, "cpu-preset-test"),
                    [FOUR_RUNGS[1]],
                    0,
                    60,
                    24,
                    force_software=True,
                    cpu_preset="p6",
                )
            gpu_probe.assert_not_called()

        preset_index = command.index("-preset")
        self.assertEqual(command[preset_index + 1], "veryfast")
        self.assertNotIn("p6", command)
        threads_index = command.index("-threads")
        self.assertEqual(command[threads_index + 1], "2")
        filter_threads_index = command.index("-filter_complex_threads")
        self.assertEqual(command[filter_threads_index + 1], "2")
        with self.assertRaises(ValidationError):
            Settings(_env_file=None, CPU_VIDEO_PRESET="p6")

    def test_chunk_concat_uses_codec_appropriate_bitstream_filter(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            chunk_list = Path(temp_dir) / "chunks.txt"
            chunk_list.write_text("file '/tmp/chunk.ts'\n", encoding="utf-8")
            h264 = ffmpeg_utils.concat_segments_command(
                str(chunk_list),
                str(Path(temp_dir) / "h264" / "video.m3u8"),
                codec="h264",
                segment_duration=4,
            )
            hevc = ffmpeg_utils.concat_segments_command(
                str(chunk_list),
                str(Path(temp_dir) / "hevc" / "video.m3u8"),
                codec="hevc",
                segment_duration=4,
            )
            self.assertIn("h264_mp4toannexb", h264)
            self.assertIn("hevc_mp4toannexb", hevc)
            self.assertEqual(h264[h264.index("-hls_time") + 1], "4")
            with self.assertRaisesRegex(ValueError, "AV1 is not supported"):
                ffmpeg_utils.concat_segments_command(
                    str(chunk_list),
                    str(Path(temp_dir) / "av1" / "video.m3u8"),
                    codec="av1",
                    segment_duration=4,
                )
            with self.assertRaisesRegex(ValueError, "unsupported"):
                ffmpeg_utils.concat_segments_command(
                    str(chunk_list),
                    str(Path(temp_dir) / "unknown" / "video.m3u8"),
                    codec="vp9",
                )

    def test_cpu_thread_budget_is_bounded(self):
        self.assertEqual(Settings(_env_file=None).CPU_FALLBACK_THREADS_PER_TASK, 2)
        with self.assertRaises(ValidationError):
            Settings(_env_file=None, CPU_FALLBACK_THREADS_PER_TASK=0)

    def test_av1_chunk_fallback_fails_before_dispatch(self):
        av1_rungs = [dict(rendition, codec="av1") for rendition in FOUR_RUNGS]
        with self.assertRaisesRegex(ValueError, "supports only h264/hevc"):
            pipeline._cpu_fallback_plan(
                7177.832,
                av1_rungs,
                {
                    "codec": "av1",
                    "cpu_fallback_chunk_duration_sec": 120,
                },
            )
        with self.assertRaisesRegex(ValueError, "supports only h264/hevc"):
            transcode_tasks._bounded_cpu_group_canvas(
                "job-av1",
                "minio://uploads/source.mkv",
                av1_rungs,
                7177.832,
                {"codec": "av1"},
            )

    def test_fallback_never_substitutes_a_different_codec(self):
        av1_spec = dict(FOUR_RUNGS[1], codec="av1")
        hevc_spec = dict(FOUR_RUNGS[1], codec="hevc")
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.object(
                ffmpeg_utils,
                "_ffmpeg_encoders",
                return_value={"libx264"},
            ):
                with self.assertRaisesRegex(
                    ffmpeg_utils.FFmpegError,
                    "CPU encoder is unavailable",
                ):
                    ffmpeg_utils.transcode_chunk_command(
                        "source.mkv",
                        str(Path(temp_dir) / "cpu"),
                        [av1_spec],
                        0,
                        60,
                        24,
                        codec="av1",
                        force_software=True,
                        require_cpu_codec=True,
                    )

            with (
                patch.object(ffmpeg_utils, "is_gpu_available", return_value=True),
                patch.object(
                    ffmpeg_utils,
                    "_ffmpeg_encoders",
                    return_value={"h264_nvenc"},
                ),
                patch.object(
                    ffmpeg_utils,
                    "select_hwaccel_args",
                    return_value=[],
                ),
            ):
                with self.assertRaisesRegex(
                    ffmpeg_utils.FFmpegError,
                    "NVENC encoder is unavailable",
                ):
                    ffmpeg_utils.transcode_chunk_command(
                        "source.mkv",
                        str(Path(temp_dir) / "gpu"),
                        [hevc_spec],
                        0,
                        60,
                        24,
                        codec="hevc",
                        require_gpu=True,
                    )

    def test_encoder_discovery_accepts_hyphenated_ffmpeg_names(self):
        listing = subprocess.CompletedProcess(
            args=["ffmpeg", "-encoders"],
            returncode=0,
            stdout=(
                " V....D libaom-av1           libaom AV1\n"
                " V....D libx264               libx264 H.264\n"
            ),
            stderr="",
        )
        with (
            patch.object(ffmpeg_utils, "_ENCODERS_CACHE", None),
            patch.object(ffmpeg_utils, "run_cmd", return_value=listing),
        ):
            encoders = ffmpeg_utils._ffmpeg_encoders()

        self.assertIn("libaom-av1", encoders)

    def test_ffmpeg_deadline_must_leave_celery_cleanup_margin(self):
        with self.assertRaises(ValidationError):
            Settings(
                _env_file=None,
                CPU_FALLBACK_FFMPEG_TIMEOUT_SEC=6500,
                FFMPEG_TASK_RUNTIME_MARGIN_SEC=500,
                CELERY_TASK_SOFT_TIME_LIMIT_SEC=6900,
            )


if __name__ == "__main__":
    unittest.main()
