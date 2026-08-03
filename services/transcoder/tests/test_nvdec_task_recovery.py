import tempfile
import threading
import unittest
from contextlib import ExitStack, contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from app import ffmpeg_utils, models
from app.tasks import transcode_video as transcode_tasks


RENDITIONS = [
    {
        "height": 720,
        "width": 1280,
        "bitrate": 3_000_000,
        "codec": "h264",
    },
    {
        "height": 480,
        "width": 854,
        "bitrate": 1_500_000,
        "codec": "h264",
    },
]


class _FakeSession:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


class _Heartbeat:
    def __init__(self):
        self.lost_event = threading.Event()
        self.started = False
        self.stopped = False

    def start(self):
        self.started = True

    def stop(self):
        self.stopped = True


def _nvdec_initialization_error() -> ffmpeg_utils.FFmpegError:
    return ffmpeg_utils.FFmpegError(
        "[h264] cuvidCreateDecoder(...) failed -> "
        "CUDA_ERROR_OUT_OF_MEMORY: out of memory\n"
        "Failed setup for format cuda\n"
        "hwaccel initialisation returned error"
    )


def _rendition_details(renditions):
    return {
        int(rendition["height"]): {
            "height": int(rendition["height"]),
            "width": int(rendition["width"]),
            "bandwidth": int(rendition["bitrate"]),
            "playlist": f"/published/{rendition['height']}p/video.m3u8",
        }
        for rendition in renditions
    }


@contextmanager
def _group_environment(temp_dir: str, runner_side_effect):
    job_id = "job-nvdec-group"
    video_id = "video-nvdec-group"
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
        duration=60.0,
        complexity_score=0.5,
    )
    db = _FakeSession()
    lease = transcode_tasks.gpu_registry.GPULease(
        gpu_index=0,
        lease_id="lease-nvdec-group",
        worker_id="worker-nvdec",
        slots=len(RENDITIONS),
        managed=False,
    )
    heartbeat = _Heartbeat()
    source = Path(temp_dir) / "source.mkv"
    source.write_bytes(b"source")

    def prepare_rows(_db, _video_id, _output, renditions, *_args):
        return [], _rendition_details(renditions)

    with ExitStack() as stack:
        stack.enter_context(
            patch.object(transcode_tasks, "SessionLocal", return_value=db)
        )
        stack.enter_context(
            patch.object(
                transcode_tasks,
                "get_settings",
                return_value=SimpleNamespace(
                    WORK_DIR=temp_dir,
                    GPU_FFMPEG_TIMEOUT_SEC=42,
                ),
            )
        )
        stack.enter_context(
            patch.object(
                transcode_tasks,
                "lock_current_job",
                return_value=(job, video),
            )
        )
        stack.enter_context(
            patch.object(transcode_tasks, "advance_job_status")
        )
        stack.enter_context(
            patch.object(
                transcode_tasks,
                "ensure_local_source",
                return_value=str(source),
            )
        )
        stack.enter_context(
            patch.object(
                transcode_tasks,
                "_completed_rendition_results",
                return_value=None,
            )
        )
        stack.enter_context(
            patch.object(
                transcode_tasks,
                "_canonical_renditions_reusable",
                return_value=False,
            )
        )
        validate = stack.enter_context(
            patch.object(transcode_tasks, "_validate_rendition_set")
        )
        stack.enter_context(
            patch.object(transcode_tasks, "_promote_rendition_directories")
        )
        stack.enter_context(
            patch.object(
                transcode_tasks,
                "_prepare_rendition_rows",
                side_effect=prepare_rows,
            )
        )
        stack.enter_context(patch.object(transcode_tasks, "_record_metric"))
        stack.enter_context(
            patch.object(
                transcode_tasks.gpu_registry,
                "acquire_gpu",
                return_value=lease,
            )
        )
        stack.enter_context(
            patch.object(
                transcode_tasks.gpu_registry,
                "GPULeaseHeartbeat",
                return_value=heartbeat,
            )
        )
        release = stack.enter_context(
            patch.object(transcode_tasks.gpu_registry, "release_gpu")
        )
        command = stack.enter_context(
            patch.object(
                transcode_tasks.ffmpeg_utils,
                "transcode_multi_command",
                side_effect=[
                    ["ffmpeg", "nvdec"],
                    ["ffmpeg", "hybrid"],
                ],
            )
        )
        runner = stack.enter_context(
            patch.object(
                transcode_tasks.ffmpeg_utils,
                "run_cmd_with_progress",
                side_effect=runner_side_effect,
            )
        )
        stack.enter_context(
            patch.object(
                transcode_tasks,
                "_worker_id",
                return_value="worker-nvdec",
            )
        )
        for progress_method in (
            "start_task",
            "update_task",
            "complete_task",
            "fail_task",
        ):
            stack.enter_context(
                patch.object(
                    transcode_tasks.progress_tracker,
                    progress_method,
                )
            )
        stack.enter_context(patch.object(transcode_tasks, "publish_event"))

        yield SimpleNamespace(
            job_id=job_id,
            video_id=video_id,
            db=db,
            lease=lease,
            heartbeat=heartbeat,
            command=command,
            runner=runner,
            release=release,
            validate=validate,
        )


@contextmanager
def _chunk_environment(temp_dir: str, runner_side_effect):
    job_id = "job-nvdec-chunk"
    video_id = "video-nvdec-chunk"
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
        duration=60.0,
        complexity_score=0.5,
    )
    db = _FakeSession()
    lease = transcode_tasks.gpu_registry.GPULease(
        gpu_index=0,
        lease_id="lease-nvdec-chunk",
        worker_id="worker-nvdec",
        slots=len(RENDITIONS),
        managed=False,
    )
    heartbeat = _Heartbeat()
    source = Path(temp_dir) / "source.mkv"
    source.write_bytes(b"source")

    with ExitStack() as stack:
        stack.enter_context(
            patch.object(transcode_tasks, "SessionLocal", return_value=db)
        )
        stack.enter_context(
            patch.object(
                transcode_tasks,
                "get_settings",
                return_value=SimpleNamespace(
                    WORK_DIR=temp_dir,
                    GPU_FFMPEG_TIMEOUT_SEC=42,
                ),
            )
        )
        stack.enter_context(
            patch.object(
                transcode_tasks,
                "lock_current_job",
                return_value=(job, video),
            )
        )
        stack.enter_context(
            patch.object(transcode_tasks, "advance_job_status")
        )
        stack.enter_context(
            patch.object(
                transcode_tasks,
                "ensure_local_source",
                return_value=str(source),
            )
        )
        stack.enter_context(patch.object(transcode_tasks, "_record_metric"))
        stack.enter_context(
            patch.object(
                transcode_tasks.gpu_registry,
                "acquire_gpu",
                return_value=lease,
            )
        )
        stack.enter_context(
            patch.object(
                transcode_tasks.gpu_registry,
                "GPULeaseHeartbeat",
                return_value=heartbeat,
            )
        )
        release = stack.enter_context(
            patch.object(transcode_tasks.gpu_registry, "release_gpu")
        )
        command = stack.enter_context(
            patch.object(
                transcode_tasks.ffmpeg_utils,
                "transcode_chunk_command",
                side_effect=[
                    ["ffmpeg", "nvdec-chunk"],
                    ["ffmpeg", "hybrid-chunk"],
                ],
            )
        )
        runner = stack.enter_context(
            patch.object(
                transcode_tasks.ffmpeg_utils,
                "run_cmd_with_progress",
                side_effect=runner_side_effect,
            )
        )
        stack.enter_context(
            patch.object(
                transcode_tasks,
                "_worker_id",
                return_value="worker-nvdec",
            )
        )
        stack.enter_context(
            patch.object(
                transcode_tasks.progress_tracker,
                "task_chunk_count",
                return_value=0,
            )
        )
        for progress_method in (
            "start_task",
            "update_task",
            "configure_chunked_task",
            "update_chunked_task",
            "fail_task",
        ):
            stack.enter_context(
                patch.object(
                    transcode_tasks.progress_tracker,
                    progress_method,
                )
            )

        yield SimpleNamespace(
            job_id=job_id,
            video_id=video_id,
            db=db,
            lease=lease,
            heartbeat=heartbeat,
            command=command,
            runner=runner,
            release=release,
        )


class NVDECTaskRecoveryTests(unittest.TestCase):
    def test_group_uses_video_stream_duration_when_audio_tail_is_longer(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with _group_environment(temp_dir, [None]) as env:
                result = transcode_tasks._run_transcode_group(
                    Mock(request=SimpleNamespace(id="delivery-audio-tail")),
                    env.job_id,
                    "minio://uploads/source.mkv",
                    RENDITIONS,
                    0,
                    {
                        "segment_format": "fmp4",
                        "video_preset": "p6",
                        "_source_video_duration": 47.25,
                    },
                )

        self.assertEqual(result["gpu_index"], 0)
        env.validate.assert_called_once()
        self.assertEqual(env.validate.call_args.args[3], 47.25)

    def test_group_nvdec_failure_retries_hybrid_and_succeeds(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with _group_environment(
                temp_dir,
                [_nvdec_initialization_error(), None],
            ) as env:
                result = transcode_tasks._run_transcode_group(
                    Mock(request=SimpleNamespace(id="delivery-nvdec")),
                    env.job_id,
                    "minio://uploads/source.mkv",
                    RENDITIONS,
                    0,
                    {"segment_format": "fmp4", "video_preset": "p6"},
                )

        self.assertEqual(result["gpu_index"], 0)
        self.assertEqual(env.command.call_count, 2)
        self.assertNotIn(
            "software_decode_gpu",
            env.command.call_args_list[0].kwargs,
        )
        self.assertTrue(
            env.command.call_args_list[1].kwargs["software_decode_gpu"]
        )
        self.assertEqual(env.runner.call_count, 2)
        self.assertEqual(
            env.runner.call_args_list[1].args[0],
            ["ffmpeg", "hybrid"],
        )
        env.release.assert_called_once_with(env.lease)
        self.assertTrue(env.heartbeat.started)
        self.assertTrue(env.heartbeat.stopped)

    def test_group_hybrid_failure_requests_bounded_cpu_fallback(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with _group_environment(
                temp_dir,
                [
                    _nvdec_initialization_error(),
                    ffmpeg_utils.FFmpegError("hybrid GPU path failed"),
                ],
            ) as env:
                with self.assertRaises(
                    transcode_tasks.BoundedCPUFallbackRequired
                ) as raised:
                    transcode_tasks._run_transcode_group(
                        Mock(request=SimpleNamespace(id="delivery-nvdec")),
                        env.job_id,
                        "minio://uploads/source.mkv",
                        RENDITIONS,
                        0,
                        {"segment_format": "fmp4", "video_preset": "p6"},
                    )

        self.assertEqual(str(raised.exception), "hybrid GPU path failed")
        self.assertEqual(raised.exception.video_id, env.video_id)
        self.assertEqual(env.command.call_count, 2)
        self.assertTrue(
            env.command.call_args_list[1].kwargs["software_decode_gpu"]
        )
        self.assertEqual(env.runner.call_count, 2)
        env.release.assert_called_once_with(env.lease)

    def test_group_generic_gpu_failure_skips_hybrid_retry(self):
        generic_error = ffmpeg_utils.FFmpegError(
            "NVENC rejected an unsupported encoder option"
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            with _group_environment(
                temp_dir,
                [generic_error],
            ) as env:
                with self.assertRaises(
                    transcode_tasks.BoundedCPUFallbackRequired
                ) as raised:
                    transcode_tasks._run_transcode_group(
                        Mock(request=SimpleNamespace(id="delivery-generic")),
                        env.job_id,
                        "minio://uploads/source.mkv",
                        RENDITIONS,
                        0,
                        {"segment_format": "fmp4", "video_preset": "p6"},
                    )

        self.assertEqual(str(raised.exception), str(generic_error))
        self.assertEqual(env.command.call_count, 1)
        self.assertNotIn(
            "software_decode_gpu",
            env.command.call_args.kwargs,
        )
        self.assertEqual(env.runner.call_count, 1)
        env.release.assert_called_once_with(env.lease)

    def test_group_lost_lease_skips_hybrid_retry(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with _group_environment(
                temp_dir,
                [_nvdec_initialization_error()],
            ) as env:
                env.heartbeat.lost_event.set()
                with self.assertRaises(
                    transcode_tasks.BoundedCPUFallbackRequired
                ) as raised:
                    transcode_tasks._run_transcode_group(
                        Mock(request=SimpleNamespace(id="delivery-lost")),
                        env.job_id,
                        "minio://uploads/source.mkv",
                        RENDITIONS,
                        0,
                        {"segment_format": "fmp4", "video_preset": "p6"},
                    )

        self.assertIn("cuvidCreateDecoder", str(raised.exception))
        self.assertEqual(raised.exception.video_id, env.video_id)
        self.assertEqual(env.command.call_count, 1)
        self.assertNotIn(
            "software_decode_gpu",
            env.command.call_args.kwargs,
        )
        self.assertEqual(env.runner.call_count, 1)
        env.release.assert_called_once_with(env.lease)

    def test_group_lease_lost_while_building_recovery_skips_second_process(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with _group_environment(
                temp_dir,
                [_nvdec_initialization_error()],
            ) as env:
                def build_command(*_args, **kwargs):
                    if kwargs.get("software_decode_gpu"):
                        env.heartbeat.lost_event.set()
                        return ["ffmpeg", "hybrid"]
                    return ["ffmpeg", "nvdec"]

                env.command.side_effect = build_command
                with self.assertRaises(
                    transcode_tasks.BoundedCPUFallbackRequired
                ) as raised:
                    transcode_tasks._run_transcode_group(
                        Mock(request=SimpleNamespace(id="delivery-race")),
                        env.job_id,
                        "minio://uploads/source.mkv",
                        RENDITIONS,
                        0,
                        {"segment_format": "fmp4", "video_preset": "p6"},
                    )

        self.assertIn(
            "GPU reservation was lost before NVDEC recovery",
            str(raised.exception),
        )
        self.assertEqual(env.command.call_count, 2)
        self.assertEqual(env.runner.call_count, 1)
        env.release.assert_called_once_with(env.lease)

    def test_group_recovery_uses_remaining_primary_wall_deadline(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with _group_environment(
                temp_dir,
                [_nvdec_initialization_error(), None],
            ) as env:
                with patch.object(
                    transcode_tasks.time,
                    "monotonic",
                    side_effect=[100.0, 125.0],
                ) as monotonic:
                    result = transcode_tasks._run_transcode_group(
                        Mock(request=SimpleNamespace(id="delivery-deadline")),
                        env.job_id,
                        "minio://uploads/source.mkv",
                        RENDITIONS,
                        0,
                        {"segment_format": "fmp4", "video_preset": "p6"},
                    )

        self.assertEqual(result["gpu_index"], 0)
        self.assertEqual(monotonic.call_count, 2)
        primary_timeout = env.runner.call_args_list[0].kwargs["wall_timeout"]
        recovery_timeout = env.runner.call_args_list[1].kwargs["wall_timeout"]
        self.assertEqual(primary_timeout, 42.0)
        self.assertEqual(recovery_timeout, 17.0)
        self.assertLess(recovery_timeout, primary_timeout)
        self.assertEqual(env.command.call_count, 2)
        self.assertTrue(
            env.command.call_args_list[1].kwargs["software_decode_gpu"]
        )
        env.release.assert_called_once_with(env.lease)

    def test_chunk_nvdec_failure_retries_hybrid_and_succeeds(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with _chunk_environment(
                temp_dir,
                [_nvdec_initialization_error(), None],
            ) as env:
                result = transcode_tasks._run_transcode_chunk(
                    Mock(request=SimpleNamespace(id="delivery-chunk")),
                    env.job_id,
                    "minio://uploads/source.mkv",
                    RENDITIONS,
                    0.0,
                    60.0,
                    0,
                    0,
                    {},
                    False,
                )

        self.assertEqual(result["gpu_index"], 0)
        self.assertEqual(result["chunk_index"], 0)
        self.assertEqual(env.command.call_count, 2)
        self.assertNotIn(
            "software_decode_gpu",
            env.command.call_args_list[0].kwargs,
        )
        self.assertTrue(
            env.command.call_args_list[1].kwargs["software_decode_gpu"]
        )
        self.assertEqual(
            env.runner.call_args_list[1].args[0],
            ["ffmpeg", "hybrid-chunk"],
        )
        env.release.assert_called_once_with(env.lease)
        self.assertTrue(env.heartbeat.started)
        self.assertTrue(env.heartbeat.stopped)


if __name__ == "__main__":
    unittest.main()
