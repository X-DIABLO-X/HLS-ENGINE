import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from app import ffmpeg_utils, models, progress
from app.config import Settings
from app.tasks import pipeline
from app.tasks import transcode_video as transcode_tasks
from tests.test_transcode_single_pass_idempotency import (
    _FakeSession,
    _write_fmp4_rendition,
)


def _eligible_probe(**overrides):
    probe = {
        "duration": 60.0,
        "width": 1920,
        "height": 1080,
        "video_codec": "h264",
        "video_profile": "High",
        "video_level": 40,
        "video_pix_fmt": "yuv420p",
        "video_field_order": "progressive",
        "video_sample_aspect_ratio": "1:1",
        "video_rotation": 0.0,
        "video_bitrate": 5_000_000,
        "frame_rate": 24.0,
        "audio_tracks": [],
        "subtitle_tracks": [],
    }
    probe.update(overrides)
    return probe


def _write_ts_rendition(root: Path, durations=(3.0, 3.0)) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:6",
        "#EXT-X-INDEPENDENT-SEGMENTS",
        "#EXT-X-PLAYLIST-TYPE:VOD",
        f"#EXT-X-TARGETDURATION:{max(1, int(max(durations) + 0.999))}",
    ]
    for index, duration in enumerate(durations):
        name = f"{index:05d}.ts"
        (root / name).write_bytes(b"segment")
        lines.extend((f"#EXTINF:{duration:.3f},", name))
    lines.append("#EXT-X-ENDLIST")
    (root / "video.m3u8").write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )
    return root


class DirectPlayCompatibilityTests(unittest.TestCase):
    def test_only_conservative_h264_sources_are_eligible(self):
        eligible, reason = ffmpeg_utils.h264_direct_play_eligibility(
            _eligible_probe(),
            "h264",
        )

        self.assertTrue(eligible, reason)

    def test_missing_or_unsafe_metadata_fails_closed(self):
        cases = {
            "wrong_output_codec": ({}, "hevc"),
            "wrong_source_codec": ({"video_codec": "hevc"}, "h264"),
            "ten_bit": ({"video_pix_fmt": "yuv420p10le"}, "h264"),
            "interlaced": ({"video_field_order": "tt"}, "h264"),
            "unknown_field_order": ({"video_field_order": None}, "h264"),
            "anamorphic": (
                {"video_sample_aspect_ratio": "4:3"},
                "h264",
            ),
            "rotation": ({"video_rotation": 90}, "h264"),
            "odd_width": ({"width": 1919}, "h264"),
            "oversized": ({"width": 7680, "height": 4320}, "h264"),
            "high_frame_rate": ({"frame_rate": 120.0}, "h264"),
            "unsafe_profile": ({"video_profile": "High 10"}, "h264"),
            "missing_level": ({"video_level": None}, "h264"),
            "missing_bitrate": ({"video_bitrate": 0}, "h264"),
            "missing_duration": ({"duration": 0}, "h264"),
        }

        for case, (overrides, output_codec) in cases.items():
            with self.subTest(case=case):
                eligible, _reason = (
                    ffmpeg_utils.h264_direct_play_eligibility(
                        _eligible_probe(**overrides),
                        output_codec,
                    )
                )
                self.assertFalse(eligible)

    def test_probe_parser_carries_fail_closed_video_metadata(self):
        parsed = ffmpeg_utils.parse_probe(
            {
                "streams": [
                    {
                        "codec_type": "video",
                        "codec_name": "h264",
                        "profile": "High",
                        "level": 40,
                        "pix_fmt": "yuv420p",
                        "field_order": "progressive",
                        "sample_aspect_ratio": "1:1",
                        "width": 1920,
                        "height": 1080,
                        "bit_rate": "5000000",
                        "r_frame_rate": "24/1",
                        "tags": {
                            "DURATION": "00:00:59.500000000",
                        },
                    }
                ],
                "format": {"duration": "60.0"},
            }
        )

        self.assertEqual(parsed["video_profile"], "High")
        self.assertEqual(parsed["video_level"], 40)
        self.assertEqual(parsed["video_pix_fmt"], "yuv420p")
        self.assertEqual(parsed["video_field_order"], "progressive")
        self.assertEqual(parsed["video_sample_aspect_ratio"], "1:1")
        self.assertEqual(parsed["video_rotation"], 0.0)
        self.assertEqual(parsed["duration"], 60.0)
        self.assertEqual(parsed["video_duration"], 59.5)

    def test_setting_defaults_off(self):
        settings = Settings(_env_file=None)

        self.assertFalse(settings.VIDEO_PASSTHROUGH_ENABLED)
        self.assertFalse(
            progress.DEFAULT_SETTINGS["video_passthrough_enabled"]
        )


class DirectPlayCommandAndValidationTests(unittest.TestCase):
    def test_remux_command_copies_one_video_stream_and_normalizes_timestamps(self):
        with tempfile.TemporaryDirectory() as output:
            command = ffmpeg_utils.remux_h264_hls_command(
                "source.mkv",
                output,
                segment_duration=6,
                segment_format="fmp4",
            )

        self.assertEqual(command[command.index("-c:v") + 1], "copy")
        self.assertEqual(command[command.index("-map") + 1], "0:v:0")
        self.assertEqual(
            command[command.index("-avoid_negative_ts") + 1],
            "make_zero",
        )
        self.assertIn("independent_segments", command)
        self.assertNotIn("-vf", command)
        self.assertNotIn("-preset", command)

    def test_output_validation_checks_codec_duration_timestamps_and_keyframes(self):
        with tempfile.TemporaryDirectory() as output:
            rendition_dir = _write_ts_rendition(Path(output))
            playlist_probe = {
                "streams": [
                    {
                        "codec_type": "video",
                        "codec_name": "h264",
                        "profile": "High",
                        "level": 40,
                        "pix_fmt": "yuv420p",
                        "field_order": "progressive",
                        "sample_aspect_ratio": "1:1",
                        "width": 1920,
                        "height": 1080,
                    }
                ],
                "format": {"duration": "6.0"},
            }
            packets = [
                {"pts_time": "0.000", "dts_time": "0.000", "flags": "K_"},
                {"pts_time": "1.000", "dts_time": "1.000", "flags": "__"},
                {"pts_time": "3.000", "dts_time": "3.000", "flags": "K_"},
                {"pts_time": "4.000", "dts_time": "4.000", "flags": "__"},
            ]
            with (
                patch.object(
                    transcode_tasks.ffmpeg_utils,
                    "ffprobe",
                    return_value=playlist_probe,
                ),
                patch.object(
                    transcode_tasks.ffmpeg_utils,
                    "ffprobe_video_packets",
                    return_value=packets,
                ),
            ):
                result = transcode_tasks._validate_direct_play_output(
                    str(rendition_dir),
                    {
                        "height": 1080,
                        "width": 1920,
                        "bitrate": 5_000_000,
                        "codec": "h264",
                    },
                    "ts",
                    6.0,
                    24.0,
                )

        self.assertEqual(len(result["segment_paths"]), 2)

    def test_output_validation_rejects_negative_or_non_keyframe_start(self):
        bad_packet_sets = {
            "negative_timestamp": [
                {
                    "pts_time": "-0.100",
                    "dts_time": "-0.100",
                    "flags": "K_",
                }
            ],
            "non_keyframe_start": [
                {
                    "pts_time": "0.000",
                    "dts_time": "0.000",
                    "flags": "__",
                }
            ],
        }
        playlist_probe = {
            "streams": [
                {
                    "codec_type": "video",
                    "codec_name": "h264",
                    "profile": "High",
                    "level": 40,
                    "pix_fmt": "yuv420p",
                    "field_order": "progressive",
                    "sample_aspect_ratio": "1:1",
                    "width": 1920,
                    "height": 1080,
                }
            ],
            "format": {"duration": "3.0"},
        }
        for case, packets in bad_packet_sets.items():
            with self.subTest(case=case), tempfile.TemporaryDirectory() as output:
                rendition_dir = _write_ts_rendition(
                    Path(output),
                    durations=(3.0,),
                )
                with (
                    patch.object(
                        transcode_tasks.ffmpeg_utils,
                        "ffprobe",
                        return_value=playlist_probe,
                    ),
                    patch.object(
                        transcode_tasks.ffmpeg_utils,
                        "ffprobe_video_packets",
                        return_value=packets,
                    ),
                ):
                    with self.assertRaises(
                        transcode_tasks.RenditionValidationError
                    ):
                        transcode_tasks._validate_direct_play_output(
                            str(rendition_dir),
                            {
                                "height": 1080,
                                "width": 1920,
                                "bitrate": 5_000_000,
                                "codec": "h264",
                            },
                            "ts",
                            3.0,
                            24.0,
                        )

    def test_output_validation_rejects_sparse_source_keyframes(self):
        with tempfile.TemporaryDirectory() as output:
            rendition_dir = _write_ts_rendition(
                Path(output),
                durations=(15.0,),
            )
            playlist_probe = {
                "streams": [
                    {
                        "codec_type": "video",
                        "codec_name": "h264",
                        "profile": "High",
                        "level": 40,
                        "pix_fmt": "yuv420p",
                        "field_order": "progressive",
                        "sample_aspect_ratio": "1:1",
                        "width": 1920,
                        "height": 1080,
                    }
                ],
                "format": {"duration": "15.0"},
            }
            packets = [
                {
                    "pts_time": "0.000",
                    "dts_time": "0.000",
                    "flags": "K_",
                }
            ]
            with (
                patch.object(
                    transcode_tasks.ffmpeg_utils,
                    "ffprobe",
                    return_value=playlist_probe,
                ),
                patch.object(
                    transcode_tasks.ffmpeg_utils,
                    "ffprobe_video_packets",
                    return_value=packets,
                ),
            ):
                with self.assertRaisesRegex(
                    transcode_tasks.RenditionValidationError,
                    "oversized",
                ):
                    transcode_tasks._validate_direct_play_output(
                        str(rendition_dir),
                        {
                            "height": 1080,
                            "width": 1920,
                            "bitrate": 5_000_000,
                            "codec": "h264",
                        },
                        "ts",
                        15.0,
                        24.0,
                        6,
                    )


class DirectPlayTaskTests(unittest.TestCase):
    def _run_group(self, *, copy_failure=False, encoding_strategy=None):
        video_id = "video-direct"
        job_id = "job-direct"
        source_spec = {
            "height": 1080,
            "width": 1920,
            "bitrate": 5_000_000,
            "codec": "h264",
        }
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
            encoding_strategy=encoding_strategy,
        )
        db = _FakeSession(job=job, video=video)
        direct_plan = {
            "probe": _eligible_probe(duration=6.0),
            "fallback_renditions": [
                source_spec,
                {
                    "height": 720,
                    "width": 1280,
                    "bitrate": 3_000_000,
                    "codec": "h264",
                },
            ],
            "fallback_routes": [
                {
                    "gpu_index": 0,
                    "renditions": [source_spec],
                }
            ],
            "fallback_chunks": [],
            "fallback_use_chunked": False,
            "fallback_queue": "video",
            "fallback_force_cpu": False,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            source = Path(temp_dir) / "source.mkv"
            source.write_bytes(b"source")

            def remux_command(_source, output_dir, **_kwargs):
                _write_fmp4_rendition(Path(output_dir).parent, 1080)
                return ["ffmpeg", "copy"]

            run_side_effect = (
                ffmpeg_utils.FFmpegError("copy failed")
                if copy_failure
                else None
            )
            with (
                patch.object(transcode_tasks, "SessionLocal", return_value=db),
                patch.object(
                    transcode_tasks,
                    "get_settings",
                    return_value=SimpleNamespace(
                        WORK_DIR=temp_dir,
                        CPU_FALLBACK_FFMPEG_TIMEOUT_SEC=5400,
                    ),
                ),
                patch.object(
                    transcode_tasks,
                    "ensure_local_source",
                    return_value=str(source),
                ),
                patch.object(
                    transcode_tasks.ffmpeg_utils,
                    "remux_h264_hls_command",
                    side_effect=remux_command,
                ) as remux,
                patch.object(
                    transcode_tasks.ffmpeg_utils,
                    "run_cmd_with_progress",
                    side_effect=run_side_effect,
                ),
                patch.object(
                    transcode_tasks,
                    "_validate_direct_play_output",
                    return_value={},
                ) as validate_copy,
                patch.object(
                    transcode_tasks.gpu_registry,
                    "acquire_gpu",
                ) as acquire_gpu,
                patch.object(
                    transcode_tasks.ffmpeg_utils,
                    "transcode_multi_command",
                ) as encode,
                patch.object(transcode_tasks.progress_tracker, "start_task"),
                patch.object(transcode_tasks.progress_tracker, "update_task"),
                patch.object(transcode_tasks.progress_tracker, "complete_task"),
                patch.object(
                    transcode_tasks.progress_tracker,
                    "fail_task",
                ) as fail_task,
                patch.object(transcode_tasks, "publish_event"),
                patch.object(
                    transcode_tasks,
                    "_worker_id",
                    return_value="cpu-worker",
                ),
            ):
                expects_fallback = (
                    copy_failure
                    or encoding_strategy
                    == transcode_tasks.DIRECT_PLAY_FALLBACK
                )
                if expects_fallback:
                    with self.assertRaises(
                        transcode_tasks.DirectPlayFallbackRequired
                    ) as raised:
                        transcode_tasks._run_transcode_group(
                            Mock(request=SimpleNamespace(id="delivery-1")),
                            job_id,
                            "minio://unused/source.mkv",
                            [source_spec],
                            None,
                            {
                                "codec": "h264",
                                "segment_format": "fmp4",
                                "_video_direct_play": direct_plan,
                            },
                        )
                    result = raised.exception
                else:
                    result = transcode_tasks._run_transcode_group(
                        Mock(request=SimpleNamespace(id="delivery-1")),
                        job_id,
                        "minio://unused/source.mkv",
                        [source_spec],
                        None,
                        {
                            "codec": "h264",
                            "segment_format": "fmp4",
                            "_video_direct_play": direct_plan,
                        },
                    )

        return result, remux, validate_copy, acquire_gpu, encode, fail_task

    def test_real_group_remuxes_without_an_nvenc_lease(self):
        result, remux, validate_copy, acquire, encode, fail = (
            self._run_group()
        )

        self.assertEqual(result["gpu_index"], None)
        remux.assert_called_once()
        validate_copy.assert_called_once()
        acquire.assert_not_called()
        encode.assert_not_called()
        fail.assert_not_called()

    def test_failed_copy_requests_normal_ladder_without_marking_failure(self):
        result, _remux, validate_copy, acquire, encode, fail = (
            self._run_group(copy_failure=True)
        )

        self.assertIsInstance(
            result,
            transcode_tasks.DirectPlayFallbackRequired,
        )
        validate_copy.assert_not_called()
        acquire.assert_not_called()
        encode.assert_not_called()
        fail.assert_not_called()

    def test_delayed_direct_delivery_obeys_durable_fallback_intent(self):
        result, remux, validate_copy, acquire, encode, fail = self._run_group(
            encoding_strategy=transcode_tasks.DIRECT_PLAY_FALLBACK,
        )

        self.assertIsInstance(
            result,
            transcode_tasks.DirectPlayFallbackRequired,
        )
        self.assertIn("already committed", str(result))
        remux.assert_not_called()
        validate_copy.assert_not_called()
        acquire.assert_not_called()
        encode.assert_not_called()
        fail.assert_not_called()

    def test_direct_fallback_transition_is_durable_and_replayable(self):
        job = SimpleNamespace(
            id="job-direct",
            video_id="video-direct",
            status=models.JobStatus.queued.value,
        )
        video = SimpleNamespace(
            id="video-direct",
            status="processing",
            encoding_strategy=transcode_tasks.DIRECT_PLAY_PENDING,
        )
        db = _FakeSession(job=job, video=video)

        with (
            patch.object(transcode_tasks, "SessionLocal", return_value=db),
            patch.object(
                transcode_tasks,
                "lock_current_job",
                return_value=(job, video),
            ),
        ):
            claimed = transcode_tasks._transition_direct_play_to_fallback(
                job.id,
                video.id,
            )
            replayed = transcode_tasks._transition_direct_play_to_fallback(
                job.id,
                video.id,
            )

        self.assertTrue(claimed)
        self.assertFalse(replayed)
        self.assertEqual(
            video.encoding_strategy,
            transcode_tasks.DIRECT_PLAY_FALLBACK,
        )
        self.assertEqual(db.commit_count, 1)

    def test_direct_fallback_transition_rejects_unrelated_strategy(self):
        job = SimpleNamespace(
            id="job-direct",
            video_id="video-direct",
            status=models.JobStatus.queued.value,
        )
        video = SimpleNamespace(
            id="video-direct",
            status="processing",
            encoding_strategy="per_title",
        )
        db = _FakeSession(job=job, video=video)

        with (
            patch.object(transcode_tasks, "SessionLocal", return_value=db),
            patch.object(
                transcode_tasks,
                "lock_current_job",
                return_value=(job, video),
            ),
            self.assertRaisesRegex(
                RuntimeError,
                "cannot transition direct-play",
            ),
        ):
            transcode_tasks._transition_direct_play_to_fallback(
                job.id,
                video.id,
            )

        self.assertEqual(video.encoding_strategy, "per_title")
        self.assertEqual(db.commit_count, 0)
        self.assertEqual(db.rollback_count, 1)

    def test_failed_copy_canvas_disables_direct_play_on_gpu_replacement(self):
        fallback_renditions = [
            {
                "height": 1080,
                "width": 1920,
                "bitrate": 5_000_000,
                "codec": "h264",
            },
            {
                "height": 720,
                "width": 1280,
                "bitrate": 3_000_000,
                "codec": "h264",
            },
        ]
        settings = {
            "codec": "h264",
            "video_passthrough_enabled": True,
            "_video_direct_play": {
                "fallback_renditions": fallback_renditions,
                "fallback_routes": [
                    {
                        "gpu_index": 0,
                        "renditions": fallback_renditions,
                    }
                ],
                "fallback_chunks": [],
                "fallback_use_chunked": False,
                "fallback_queue": "video",
                "fallback_force_cpu": False,
            },
        }

        signature = transcode_tasks._direct_play_fallback_canvas(
            "job-1",
            "source.mkv",
            settings,
        )

        self.assertTrue(str(signature.task).endswith("transcode_group"))
        self.assertEqual(signature.options.get("queue"), "video")
        fallback_settings = signature.args[4]
        self.assertFalse(fallback_settings["video_passthrough_enabled"])
        self.assertNotIn("_video_direct_play", fallback_settings)


class _PipelineSession:
    def __init__(self, job):
        self.job = job

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


class DirectPlayPipelineRoutingTests(unittest.TestCase):
    def test_pipeline_copies_original_and_keeps_adaptive_ladder(self):
        job = SimpleNamespace(
            id="job-direct-route",
            video_id="video-direct-route",
            status=models.JobStatus.pending.value,
            dispatch_count=0,
            input_path=None,
        )
        video = SimpleNamespace(
            id=job.video_id,
            status="pending",
            complexity_score=None,
            encoding_strategy=None,
        )
        sessions = [_PipelineSession(job) for _ in range(4)]
        dispatched = Mock()
        chord_factory = Mock(return_value=dispatched)
        fallback_ladder = [
            {
                "height": 1080,
                "width": 1920,
                "bitrate": 5_000_000,
                "codec": "h264",
            },
            {
                "height": 720,
                "width": 1280,
                "bitrate": 3_000_000,
                "codec": "h264",
            },
        ]

        with (
            patch.object(
                pipeline,
                "SessionLocal",
                side_effect=sessions,
            ),
            patch.object(
                pipeline,
                "lock_current_job",
                side_effect=lambda *_args, **_kwargs: (job, video),
            ),
            patch.object(
                pipeline,
                "advance_job_status",
                side_effect=lambda target, status: setattr(
                    target,
                    "status",
                    status,
                ),
            ),
            patch.object(
                pipeline,
                "_run_probe_sync",
                return_value=_eligible_probe(duration=120.0),
            ),
            patch.object(
                pipeline,
                "ensure_local_source",
                return_value="/work/source.mkv",
            ),
            patch.object(
                pipeline.ffmpeg_utils,
                "get_per_title_ladder",
                return_value=fallback_ladder,
            ),
            patch.object(
                pipeline.ffmpeg_utils,
                "analyze_complexity",
            ) as analyze,
            patch.object(
                pipeline.gpu_registry,
                "get_gpu_status",
                return_value=[
                    {
                        "index": 0,
                        "worker_id": "gpu-0",
                        "capacity": 2,
                    }
                ],
            ),
            patch.object(pipeline, "publish_event"),
            patch.object(pipeline, "chord", chord_factory),
            patch.object(pipeline.progress_tracker, "init_progress"),
            patch.object(pipeline.progress_tracker, "complete_task"),
        ):
            result = pipeline._run_pipeline(
                Mock(),
                job.id,
                "minio://uploads/source.mkv",
                video.id,
                settings={
                    "codec": "h264",
                    "video_passthrough_enabled": True,
                    "per_title_encoding": True,
                    "chunked_encoding": True,
                    "chunk_duration_sec": 60,
                    "chunk_min_duration_sec": 60,
                    "qualities": [1080, 720],
                },
            )

        self.assertEqual(result["status"], "dispatched")
        analyze.assert_called_once()
        outer_header = chord_factory.call_args.args[0]
        original_signatures = [
            signature
            for signature in outer_header.tasks
            if str(getattr(signature, "task", "")).endswith(
                "transcode_video"
            )
        ]
        self.assertEqual(len(original_signatures), 1)
        original_signature = original_signatures[0]
        self.assertEqual(
            original_signature.options.get("queue"),
            pipeline.GPU_VIDEO_QUEUE,
        )
        original = original_signature.args[2]
        self.assertEqual(original["name"], "Original")
        self.assertTrue(original["is_original"])
        self.assertTrue(original["direct_play"])
        self.assertEqual((original["width"], original["height"]), (1920, 1080))
        self.assertEqual(video.encoding_strategy, "per_title")


if __name__ == "__main__":
    unittest.main()
