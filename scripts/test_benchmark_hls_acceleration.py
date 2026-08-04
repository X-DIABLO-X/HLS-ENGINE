import argparse
import json
import random
import sys
import tempfile
import unittest
from fractions import Fraction
from pathlib import Path
from unittest.mock import Mock, patch

from scripts import benchmark_hls_acceleration as benchmark


def _source_info(path: Path) -> benchmark.SourceInfo:
    stat = path.stat()
    return benchmark.SourceInfo(
        path=path,
        size_bytes=stat.st_size,
        mtime_ns=stat.st_mtime_ns,
        duration_seconds=7177.832,
        codec_name="h264",
        width=1920,
        height=804,
        fps=Fraction(24, 1),
    )


def _batch_result(
    command: benchmark.CommandSpec,
    *,
    returncode=0,
    stdout="",
    stderr="",
    wall_seconds=0.1,
) -> benchmark.BatchResult:
    return benchmark.BatchResult(
        wall_seconds=wall_seconds,
        processes=(
            benchmark.ProcessResult(
                label=command.label,
                args=command.args,
                returncode=returncode,
                wall_seconds=wall_seconds,
                stdout=stdout,
                stderr=stderr,
            ),
        ),
    )


class ClipAndMatrixTests(unittest.TestCase):
    def test_representative_clips_and_chunks_use_six_second_grid(self):
        clips = benchmark.select_aligned_clips(
            7177.832,
            clip_seconds=120,
            clip_count=2,
        )

        self.assertEqual(len(clips), 2)
        self.assertNotEqual(clips[0].start_seconds, clips[1].start_seconds)
        for clip in clips:
            self.assertEqual(clip.start_seconds % 6, 0)
            self.assertEqual(clip.duration_seconds, 120)

    def test_candidate_filter_accepts_repeated_and_comma_separated_ids(self):
        args = benchmark.parse_args(
            [
                "--source",
                "source.mkv",
                "--candidate",
                "grouped_nvenc_balanced,intel_qsv_480p",
                "--candidate",
                "hybrid_nvenc_720p_qsv_480p_parallel",
                "--candidate",
                "intel_qsv_480p",
            ]
        )

        requested = benchmark._candidate_filter_values(args)

        self.assertEqual(
            requested,
            (
                "grouped_nvenc_balanced",
                "intel_qsv_480p",
                "hybrid_nvenc_720p_qsv_480p_parallel",
            ),
        )
        candidates = tuple(
            benchmark.CandidatePlan(candidate_id, "test", {}, ())
            for candidate_id in (
                "grouped_nvenc_balanced",
                "intel_qsv_480p",
                "hybrid_nvenc_720p_qsv_480p_parallel",
                "unselected",
            )
        )
        selected = benchmark._filter_candidates(candidates, requested)
        self.assertEqual(
            [candidate.candidate_id for candidate in selected],
            list(requested),
        )
        with self.assertRaisesRegex(
            benchmark.HarnessError,
            "unknown candidate ID",
        ):
            benchmark._filter_candidates(candidates, ("missing",))

    def test_matrix_contains_every_required_candidate_and_parallel_shape(self):
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            source_path = root_path / "source.mkv"
            source_path.write_bytes(b"source")
            source = _source_info(source_path)
            clips = benchmark.select_aligned_clips(
                source.duration_seconds,
                clip_seconds=120,
                clip_count=1,
            )

            matrix = benchmark.build_candidate_matrix(
                ffmpeg="ffmpeg",
                ffprobe="ffprobe",
                python_executable=sys.executable,
                source=source,
                clips=clips,
                run_root=root_path / "run",
                temporal_chunks=4,
                nvenc_max_sessions=2,
            )

            by_id = {candidate.candidate_id: candidate for candidate in matrix}
            self.assertEqual(
                set(by_id),
                {
                    "grouped_nvenc_quality",
                    "grouped_nvenc_balanced",
                    "grouped_nvenc_turbo",
                    "single_nvenc_720p",
                    "single_nvenc_480p",
                    "separate_nvenc_renditions_parallel",
                    "temporal_nvenc_chunks_parallel",
                    "intel_qsv_480p",
                    "hybrid_nvenc_720p_qsv_480p_parallel",
                    "h264_stream_copy_direct_play",
                },
            )
            grouped_command = by_id[
                "grouped_nvenc_quality"
            ].clips[0].batches[0].commands[0].args
            self.assertLess(
                grouped_command.index("-t"),
                grouped_command.index("-i"),
                "the clip limit must apply to every grouped output",
            )

            quality_command = by_id[
                "grouped_nvenc_quality"
            ].clips[0].batches[0].commands[0].args
            quality_batch = by_id[
                "grouped_nvenc_quality"
            ].clips[0].batches[0]
            balanced_command = by_id[
                "grouped_nvenc_balanced"
            ].clips[0].batches[0].commands[0].args
            turbo_command = by_id[
                "grouped_nvenc_turbo"
            ].clips[0].batches[0].commands[0].args
            self.assertIn("split=2", " ".join(quality_command))
            filter_graph = quality_command[
                quality_command.index("-filter_complex") + 1
            ]
            self.assertIn(
                "scale_cuda=1280:720:"
                "force_original_aspect_ratio=decrease",
                filter_graph,
            )
            self.assertIn(
                "scale_cuda=854:480:"
                "force_original_aspect_ratio=decrease",
                filter_graph,
            )
            self.assertNotIn("interp_algo", filter_graph)
            self.assertNotIn("format=", filter_graph)
            self.assertEqual(
                [
                    (target.width, target.height)
                    for target in quality_batch.outputs
                ],
                [(1280, 536), (854, 358)],
            )
            for candidate in by_id.values():
                for clip_plan in candidate.clips:
                    for batch in clip_plan.batches:
                        for command in batch.commands:
                            if "-t" in command.args and "-i" in command.args:
                                self.assertLess(
                                    command.args.index("-t"),
                                    command.args.index("-i"),
                                    command.label,
                                )
            self.assertEqual(
                quality_command[quality_command.index("-preset") + 1],
                "p6",
            )
            self.assertEqual(
                balanced_command[balanced_command.index("-preset") + 1],
                "p4",
            )
            self.assertEqual(
                turbo_command[turbo_command.index("-preset") + 1],
                "p3",
            )
            self.assertIn("init.mp4", quality_command)
            self.assertIn("-hls_fmp4_init_filename", quality_command)
            self.assertNotIn("mpegts", quality_command)

            separate = by_id[
                "separate_nvenc_renditions_parallel"
            ].clips[0].batches[0]
            self.assertTrue(separate.parallel)
            self.assertEqual(len(separate.commands), 2)

            temporal_clip = by_id[
                "temporal_nvenc_chunks_parallel"
            ].clips[0]
            self.assertEqual(len(temporal_clip.batches), 4)
            self.assertTrue(
                all(batch.parallel for batch in temporal_clip.batches)
            )
            self.assertTrue(
                all(
                    len(batch.commands) <= 2
                    for batch in temporal_clip.batches
                )
            )
            self.assertEqual(
                sum(
                    len(batch.commands)
                    for batch in temporal_clip.batches
                ),
                8,
            )
            self.assertTrue(
                all(
                    not batch.validate_outputs
                    for batch in temporal_clip.batches
                )
            )
            for batch in temporal_clip.batches:
                chunk_labels = {
                    command.label.rsplit("-chunk-", 1)[1]
                    for command in batch.commands
                }
                self.assertEqual(len(chunk_labels), len(batch.commands))
            self.assertEqual(len(temporal_clip.stitches), 2)
            self.assertEqual(
                [
                    (stitch.output.width, stitch.output.height)
                    for stitch in temporal_clip.stitches
                ],
                [(1280, 536), (854, 358)],
            )

            qsv_command = by_id[
                "intel_qsv_480p"
            ].clips[0].batches[0].commands[0].args
            self.assertIn("services.transcoder.qsv_worker", qsv_command)
            qsv_target = by_id[
                "intel_qsv_480p"
            ].clips[0].batches[0].outputs[0]
            self.assertEqual(
                (qsv_target.width, qsv_target.height),
                (854, 358),
            )
            hybrid = by_id[
                "hybrid_nvenc_720p_qsv_480p_parallel"
            ]
            hybrid_batch = hybrid.clips[0].batches[0]
            self.assertTrue(hybrid_batch.parallel)
            self.assertEqual(len(hybrid_batch.commands), 2)
            self.assertEqual(
                [
                    (target.rendition_id, target.width, target.height)
                    for target in hybrid_batch.outputs
                ],
                [
                    ("720p", 1280, 536),
                    ("480p", 854, 358),
                ],
            )
            nvenc_command, qsv_command = (
                command.args for command in hybrid_batch.commands
            )
            self.assertEqual(
                nvenc_command[nvenc_command.index("-preset") + 1],
                "p4",
            )
            self.assertEqual(nvenc_command.count("h264_nvenc"), 1)
            self.assertIn("services.transcoder.qsv_worker", qsv_command)
            self.assertEqual(
                hybrid.settings["nvenc"]["sessions"],
                1,
            )
            single_filter = by_id[
                "single_nvenc_480p"
            ].clips[0].batches[0].commands[0].args
            single_filter = single_filter[
                single_filter.index("-vf") + 1
            ]
            self.assertEqual(
                single_filter,
                "setpts=PTS-STARTPTS,scale_cuda=854:480:"
                "force_original_aspect_ratio=decrease",
            )
            copy_command = by_id[
                "h264_stream_copy_direct_play"
            ].clips[0].batches[0].commands[0].args
            self.assertEqual(
                copy_command[copy_command.index("-c:v") + 1],
                "copy",
            )

    def test_nvenc_profiles_and_fmp4_mux_match_deployed_engine(self):
        rendition = benchmark.Rendition("720p", 1280, 720, 3000)
        common = [
            "-c:v", "h264_nvenc",
            "-preset", None,
            "-tune", "hq",
            "-profile:v", "high",
            "-rc", "vbr",
            "-b:v", "3000000",
            "-maxrate", "4500000",
            "-bufsize", "6000000",
            "-g", "144",
            "-keyint_min", "144",
            "-sc_threshold", "0",
            "-flags", "+cgop",
            "-force_key_frames", "expr:gte(t,n_forced*6)",
            "-cq", "23",
            "-multipass", None,
        ]
        profile_suffixes = {
            "quality": [
                "-spatial-aq", "1",
                "-temporal-aq", "1",
                "-bf", "2",
                "-2pass", "1",
                "-rc-lookahead", "32",
                "-no-scenecut", "1",
            ],
            "balanced": [
                "-temporal-aq", "1",
                "-bf", "2",
                "-rc-lookahead", "12",
                "-no-scenecut", "1",
            ],
            "turbo": [
                "-temporal-aq", "1",
                "-bf", "2",
                "-rc-lookahead", "0",
            ],
        }
        for name, suffix in profile_suffixes.items():
            with self.subTest(profile=name):
                profile = benchmark.NVENC_PROFILES[name]
                expected = list(common)
                expected[expected.index(None)] = profile.preset
                expected[expected.index(None)] = profile.multipass
                expected += suffix
                self.assertEqual(
                    benchmark._nvenc_args(
                        profile,
                        rendition,
                        Fraction(24, 1),
                    ),
                    expected,
                )

        playlist = Path("output") / "video.m3u8"
        self.assertEqual(
            benchmark._hls_args(playlist),
            [
                "-an", "-sn",
                "-hls_segment_type", "fmp4",
                "-hls_fmp4_init_filename", "init.mp4",
                "-hls_time", "6",
                "-hls_playlist_type", "vod",
                "-hls_flags", "independent_segments",
                "-hls_segment_filename",
                (Path("output") / "%05d.m4s").as_posix(),
                "-f", "hls", playlist.as_posix(),
            ],
        )

    def test_harness_encoder_and_mux_args_equal_current_core_builders(self):
        transcoder_root = (
            Path(__file__).resolve().parents[1] / "services" / "transcoder"
        )
        sys.path.insert(0, str(transcoder_root))
        try:
            from app import ffmpeg_utils as core_ffmpeg
        finally:
            sys.path.remove(str(transcoder_root))

        rendition = benchmark.Rendition("720p", 1280, 720, 3000)
        for name in ("quality", "balanced", "turbo"):
            with self.subTest(profile=name), patch.object(
                core_ffmpeg,
                "_configured_nvenc_profile",
                return_value=name,
            ):
                core_arguments = [
                    "-c:v",
                    "h264_nvenc",
                    *core_ffmpeg._nvenc_encode_args(
                        "h264_nvenc",
                        3_000_000,
                        144,
                        6,
                        "p6",
                        True,
                        2,
                        True,
                    ),
                ]
                harness_arguments = benchmark._nvenc_args(
                    benchmark.NVENC_PROFILES[name],
                    rendition,
                    Fraction(24, 1),
                )
                self.assertEqual(harness_arguments, core_arguments)

        playlist = Path("output") / "video.m3u8"
        segment_pattern = Path("output") / "%05d.m4s"
        core_mux_arguments = core_ffmpeg._hls_mux_args(
            6,
            "fmp4",
            str(playlist),
            str(segment_pattern),
        )
        core_mux_arguments[
            core_mux_arguments.index("-hls_segment_filename") + 1
        ] = segment_pattern.as_posix()
        core_mux_arguments[-1] = playlist.as_posix()
        self.assertEqual(
            benchmark._hls_args(playlist),
            core_mux_arguments,
        )

    def test_hls_paths_use_ffmpeg_portable_forward_slashes(self):
        with tempfile.TemporaryDirectory() as root:
            playlist = Path(root) / "rendition" / "video.m3u8"
            arguments = benchmark._hls_args(playlist)
            self.assertEqual(arguments[-1], playlist.as_posix())
            segment_pattern = arguments[
                arguments.index("-hls_segment_filename") + 1
            ]
            self.assertEqual(
                segment_pattern,
                (playlist.parent / "%05d.m4s").as_posix(),
            )


class DryRunTests(unittest.TestCase):
    def test_dry_run_filters_to_the_requested_candidate(self):
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            source_path = root_path / "source.mkv"
            source_path.write_bytes(b"source")
            source = _source_info(source_path)
            report_path = root_path / "report.json"
            requested_id = "hybrid_nvenc_720p_qsv_480p_parallel"
            candidates = (
                benchmark.CandidatePlan("unselected", "test", {}, ()),
                benchmark.CandidatePlan(requested_id, "test", {}, ()),
            )
            args = argparse.Namespace(
                source=source_path,
                report=report_path,
                work_root=root_path / "work",
                ffmpeg=sys.executable,
                ffprobe=sys.executable,
                python_executable=sys.executable,
                clip_seconds=120.0,
                clip_count=1,
                temporal_chunks=4,
                nvenc_max_sessions=2,
                candidate_order_seed=1,
                candidate=[requested_id],
                segment_format="fmp4",
                timeout_multiplier=5.0,
                execute=False,
            )
            with patch.object(
                benchmark,
                "inspect_ffmpeg",
                return_value={
                    "executable": sys.executable,
                    "version": "mock",
                    "libvmaf_available": False,
                    "quality_metric_path": "ssim_psnr",
                },
            ), patch.object(
                benchmark,
                "probe_source",
                return_value=source,
            ), patch.object(
                benchmark,
                "build_candidate_matrix",
                return_value=candidates,
            ):
                report = benchmark.run_harness(args, executor=Mock())

            self.assertEqual(
                [entry["candidate_id"] for entry in report["candidates"]],
                [requested_id],
            )
            self.assertEqual(
                report["benchmark"]["candidate_filter"],
                [requested_id],
            )

    def test_dry_run_uses_mocked_subprocesses_and_creates_no_media(self):
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            source = root_path / "source.mkv"
            source.write_bytes(b"source-bytes")
            report_path = root_path / "report.json"
            work_root = root_path / "work"
            executable = Path(sys.executable)
            executor = Mock()

            def run_batch(commands, *, parallel, timeout_seconds):
                self.assertFalse(parallel)
                command = commands[0]
                if command.label == "ffmpeg-version":
                    return _batch_result(
                        command,
                        stdout="ffmpeg version mock-1.0\n",
                    )
                if command.label == "ffmpeg-filters":
                    return _batch_result(
                        command,
                        stdout="... libvmaf ...\n",
                    )
                if command.label == "probe-source":
                    return _batch_result(
                        command,
                        stdout=json.dumps(
                            {
                                "streams": [
                                    {
                                        "codec_name": "h264",
                                        "width": 1920,
                                        "height": 804,
                                        "avg_frame_rate": "24/1",
                                        "r_frame_rate": "24/1",
                                    }
                                ],
                                "format": {"duration": "7177.832"},
                            }
                        ),
                    )
                self.fail(f"unexpected mocked command: {command.label}")

            executor.run_batch.side_effect = run_batch
            args = argparse.Namespace(
                source=source,
                report=report_path,
                work_root=work_root,
                ffmpeg=str(executable),
                ffprobe=str(executable),
                python_executable=str(executable),
                clip_seconds=120.0,
                clip_count=2,
                temporal_chunks=4,
                nvenc_max_sessions=2,
                candidate_order_seed=7,
                segment_format="fmp4",
                timeout_multiplier=5.0,
                execute=False,
            )

            report = benchmark.run_harness(args, executor=executor)

            self.assertEqual(report["status"], "planned")
            self.assertEqual(report["mode"], "dry-run")
            self.assertEqual(len(report["candidates"]), 10)
            original_order = [
                "grouped_nvenc_quality",
                "grouped_nvenc_balanced",
                "grouped_nvenc_turbo",
                "single_nvenc_720p",
                "single_nvenc_480p",
                "separate_nvenc_renditions_parallel",
                "temporal_nvenc_chunks_parallel",
                "intel_qsv_480p",
                "hybrid_nvenc_720p_qsv_480p_parallel",
                "h264_stream_copy_direct_play",
            ]
            random.Random(7).shuffle(original_order)
            self.assertEqual(
                report["benchmark"]["candidate_order"],
                {
                    "strategy": "deterministic-randomized",
                    "seed": 7,
                    "candidate_ids": original_order,
                },
            )
            self.assertEqual(
                [
                    candidate["candidate_id"]
                    for candidate in report["candidates"]
                ],
                original_order,
            )
            self.assertTrue(report["cleanup"]["source_preserved"])
            self.assertTrue(source.is_file())
            self.assertFalse(any(work_root.glob("run-*")))
            stored = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(stored["ffmpeg"]["quality_metric_path"], "libvmaf")
            self.assertTrue(
                all(
                    candidate["cleanup"]["reason"]
                    == "dry-run created no media"
                    for candidate in stored["candidates"]
                )
            )


class ValidationAndQualityTests(unittest.TestCase):
    def test_playlist_validation_records_actual_media_data(self):
        with tempfile.TemporaryDirectory() as root:
            output = Path(root)
            playlist = output / "video.m3u8"
            (output / "init.mp4").write_bytes(b"i" * 8)
            (output / "00000.m4s").write_bytes(b"a" * 10)
            (output / "00001.m4s").write_bytes(b"b" * 12)
            playlist.write_text(
                "#EXTM3U\n"
                "#EXT-X-INDEPENDENT-SEGMENTS\n"
                '#EXT-X-MAP:URI="init.mp4"\n'
                "#EXTINF:6.0,\n"
                "00000.m4s\n"
                "#EXTINF:2.0,\n"
                "00001.m4s\n"
                "#EXT-X-ENDLIST\n",
                encoding="utf-8",
            )
            executor = Mock()

            def run_batch(commands, *, parallel, timeout_seconds):
                command = commands[0]
                return _batch_result(
                    command,
                    stdout=json.dumps(
                        {
                            "streams": [
                                {
                                    "codec_name": "h264",
                                    "width": 854,
                                    "height": 480,
                                    "avg_frame_rate": "24/1",
                                }
                            ],
                            "format": {"duration": "8.0"},
                        }
                    ),
                )

            executor.run_batch.side_effect = run_batch
            target = benchmark.OutputTarget(
                playlist=playlist,
                rendition_id="480p",
                width=854,
                height=480,
                source_start_seconds=0,
                expected_duration_seconds=8,
            )

            result = benchmark.validate_output(
                target,
                ffprobe="ffprobe",
                executor=executor,
                fps=Fraction(24, 1),
            )

            self.assertEqual(result["status"], "passed")
            self.assertEqual(
                result["playlist_validation"]["segment_durations_seconds"],
                [6.0, 2.0],
            )
            self.assertEqual(result["actual"]["codec_name"], "h264")
            self.assertEqual(
                result["playlist_validation"]["initialization_uri"],
                "init.mp4",
            )
            self.assertGreater(result["output_bytes"], 30)

    def test_nvenc_geometry_is_even_aspect_preserved_and_inside_box(self):
        with tempfile.TemporaryDirectory() as root:
            output = Path(root)
            playlist = output / "video.m3u8"
            (output / "init.mp4").write_bytes(b"init")
            (output / "00000.m4s").write_bytes(b"segment")
            playlist.write_text(
                "#EXTM3U\n"
                "#EXT-X-INDEPENDENT-SEGMENTS\n"
                '#EXT-X-MAP:URI="init.mp4"\n'
                "#EXTINF:6,\n"
                "00000.m4s\n"
                "#EXT-X-ENDLIST\n",
                encoding="utf-8",
            )
            executor = Mock()
            executor.run_batch.return_value = _batch_result(
                benchmark.CommandSpec("probe", ("ffprobe",)),
                stdout=json.dumps(
                    {
                        "streams": [
                            {
                                "codec_name": "h264",
                                "width": 854,
                                "height": 358,
                                "avg_frame_rate": "24/1",
                            }
                        ],
                        "format": {"duration": "6.0"},
                    }
                ),
            )
            target = benchmark.OutputTarget(
                playlist=playlist,
                rendition_id="480p",
                width=854,
                height=358,
                source_start_seconds=0,
                expected_duration_seconds=6,
                box_width=854,
                box_height=480,
                source_width=1920,
                source_height=804,
            )

            result = benchmark.validate_output(
                target,
                ffprobe="ffprobe",
                executor=executor,
                fps=Fraction(24, 1),
            )

            self.assertEqual(result["status"], "passed")
            self.assertTrue(
                result["geometry_validation"]["dimensions_are_even"]
            )
            self.assertTrue(result["geometry_validation"]["inside_box"])
            self.assertLess(
                result["geometry_validation"]["relative_aspect_error"],
                0.005,
            )

    def test_temporal_stitch_moves_chunks_into_complete_final_playlist(self):
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            chunks = []
            original_media = []
            for index, start in enumerate((0, 6)):
                chunk_dir = root_path / "chunks" / f"chunk_{index:02d}"
                chunk_dir.mkdir(parents=True)
                initialization = chunk_dir / "init.mp4"
                segment = chunk_dir / "00000.m4s"
                initialization.write_bytes(b"init" + bytes([index]))
                segment.write_bytes(b"segment" + bytes([index]))
                original_media.extend([initialization, segment])
                playlist = chunk_dir / "video.m3u8"
                playlist.write_text(
                    "#EXTM3U\n"
                    "#EXT-X-INDEPENDENT-SEGMENTS\n"
                    '#EXT-X-MAP:URI="init.mp4"\n'
                    "#EXTINF:6,\n"
                    "00000.m4s\n"
                    "#EXT-X-ENDLIST\n",
                    encoding="utf-8",
                )
                chunks.append(
                    benchmark.OutputTarget(
                        playlist=playlist,
                        rendition_id="480p",
                        width=854,
                        height=358,
                        source_start_seconds=start,
                        expected_duration_seconds=6,
                    )
                )
            final_target = benchmark.OutputTarget(
                playlist=(
                    root_path / "final" / "480p" / "video.m3u8"
                ),
                rendition_id="480p",
                width=854,
                height=358,
                source_start_seconds=0,
                expected_duration_seconds=12,
            )
            plan = benchmark.StitchPlan(
                label="stitch",
                chunks=tuple(chunks),
                output=final_target,
            )

            result = benchmark.stitch_hls_chunks(plan)

            self.assertEqual(result["moved_files"], 4)
            self.assertTrue(all(not path.exists() for path in original_media))
            manifest = final_target.playlist.read_text(encoding="utf-8")
            self.assertEqual(manifest.count("#EXT-X-DISCONTINUITY\n"), 1)
            self.assertEqual(manifest.count("#EXT-X-MAP:"), 2)
            parsed = benchmark._parse_playlist(final_target.playlist)
            self.assertEqual(parsed["segment_count"], 2)
            self.assertEqual(parsed["playlist_duration_seconds"], 12)
            self.assertEqual(len(parsed["initialization_uris"]), 2)

    def test_quality_falls_back_to_ssim_and_psnr(self):
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            source_path = root_path / "source.mkv"
            playlist = root_path / "index.m3u8"
            source_path.write_bytes(b"source")
            playlist.write_text("#EXTM3U\n", encoding="utf-8")
            source = _source_info(source_path)
            target = benchmark.OutputTarget(
                playlist=playlist,
                rendition_id="480p",
                width=854,
                height=480,
                source_start_seconds=600,
                expected_duration_seconds=120,
            )
            executor = Mock()

            def run_batch(commands, *, parallel, timeout_seconds):
                command = commands[0]
                return _batch_result(
                    command,
                    stderr=(
                        "[Parsed_ssim] SSIM Y:0.98 U:0.99 V:0.99 "
                        "All:0.985 (18.2)\n"
                        "[Parsed_psnr] PSNR y:40 u:43 v:43 "
                        "average:41.25 min:30 max:50\n"
                    ),
                )

            executor.run_batch.side_effect = run_batch
            result = benchmark.measure_quality(
                target,
                source=source,
                ffmpeg="ffmpeg",
                executor=executor,
                metric_path="ssim_psnr",
            )

            self.assertEqual(result["status"], "passed")
            self.assertEqual(result["metric_path"], "ssim_psnr")
            self.assertAlmostEqual(result["ssim"], 0.985)
            self.assertAlmostEqual(result["psnr_db"], 41.25)
            graph = result["command"][result["command"].index("-filter_complex") + 1]
            self.assertIn("ssim", graph)
            self.assertIn("psnr", graph)

    def test_quality_uses_vmaf_when_libvmaf_is_available(self):
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            source_path = root_path / "source.mkv"
            playlist = root_path / "index.m3u8"
            source_path.write_bytes(b"source")
            playlist.write_text("#EXTM3U\n", encoding="utf-8")
            source = _source_info(source_path)
            target = benchmark.OutputTarget(
                playlist=playlist,
                rendition_id="720p",
                width=1280,
                height=720,
                source_start_seconds=300,
                expected_duration_seconds=60,
            )
            executor = Mock()

            def run_batch(commands, *, parallel, timeout_seconds):
                return _batch_result(
                    commands[0],
                    stderr="[Parsed_libvmaf] VMAF score: 93.456000\n",
                )

            executor.run_batch.side_effect = run_batch
            result = benchmark.measure_quality(
                target,
                source=source,
                ffmpeg="ffmpeg",
                executor=executor,
                metric_path="libvmaf",
            )

            self.assertEqual(result["status"], "passed")
            self.assertEqual(result["metric_path"], "libvmaf")
            self.assertAlmostEqual(result["vmaf"], 93.456)
            graph = result["command"][result["command"].index("-filter_complex") + 1]
            self.assertIn("libvmaf", graph)

    def test_direct_copy_is_marked_lossless_without_quality_subprocess(self):
        executor = Mock()
        target = benchmark.OutputTarget(
            playlist=Path("copy.m3u8"),
            rendition_id="source",
            width=1920,
            height=804,
            source_start_seconds=0,
            expected_duration_seconds=120,
            direct_copy=True,
        )
        source = benchmark.SourceInfo(
            path=Path("source.mkv"),
            size_bytes=1,
            mtime_ns=1,
            duration_seconds=120,
            codec_name="h264",
            width=1920,
            height=804,
            fps=Fraction(24, 1),
        )

        result = benchmark.measure_quality(
            target,
            source=source,
            ffmpeg="ffmpeg",
            executor=executor,
            metric_path="libvmaf",
        )

        self.assertEqual(result["metric_path"], "lossless-by-copy")
        executor.run_batch.assert_not_called()


class HybridExecutionTests(unittest.TestCase):
    def test_hybrid_runs_both_encoders_in_parallel_and_validates_both(self):
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            source_path = root_path / "source.mkv"
            source_path.write_bytes(b"source")
            source = _source_info(source_path)
            run_root = root_path / "run"
            matrix = benchmark.build_candidate_matrix(
                ffmpeg="ffmpeg",
                ffprobe="ffprobe",
                python_executable=sys.executable,
                source=source,
                clips=(benchmark.Clip(0, 0, 120),),
                run_root=run_root,
                temporal_chunks=4,
                nvenc_max_sessions=2,
            )
            candidate = next(
                item
                for item in matrix
                if item.candidate_id
                == "hybrid_nvenc_720p_qsv_480p_parallel"
            )
            executor = Mock()

            def run_batch(commands, *, parallel, timeout_seconds):
                self.assertTrue(parallel)
                self.assertEqual(len(commands), 2)
                return benchmark.BatchResult(
                    wall_seconds=0.25,
                    processes=tuple(
                        benchmark.ProcessResult(
                            label=command.label,
                            args=command.args,
                            returncode=0,
                            wall_seconds=0.25,
                            stdout="",
                            stderr="",
                        )
                        for command in commands
                    ),
                )

            executor.run_batch.side_effect = run_batch
            validation = {
                "status": "passed",
                "output_bytes": 10,
                "playlist_validation": {
                    "playlist_duration_seconds": 120,
                },
            }
            quality = {
                "status": "passed",
                "metric_path": "libvmaf",
                "vmaf": 95.0,
            }
            cleanup = {
                "attempted": True,
                "deleted": True,
                "source_preserved": True,
                "error": None,
            }
            with patch.object(
                benchmark,
                "validate_output",
                return_value=validation,
            ) as validate, patch.object(
                benchmark,
                "measure_quality",
                return_value=quality,
            ) as measure, patch.object(
                benchmark,
                "cleanup_owned_tree",
                return_value=cleanup,
            ):
                result = benchmark.run_candidate(
                    candidate,
                    source=source,
                    ffmpeg="ffmpeg",
                    ffprobe="ffprobe",
                    executor=executor,
                    metric_path="libvmaf",
                    run_root=run_root,
                    timeout_multiplier=5,
                )

            self.assertEqual(result["status"], "passed")
            self.assertEqual(validate.call_count, 2)
            self.assertEqual(measure.call_count, 2)
            self.assertEqual(
                [
                    call.args[0].rendition_id
                    for call in validate.call_args_list
                ],
                ["720p", "480p"],
            )
            self.assertEqual(result["encode_wall_seconds"], 0.25)
            self.assertEqual(result["source_seconds_processed"], 120)
            self.assertEqual(result["output_seconds_processed"], 240)
            self.assertTrue(result["cleanup"]["deleted"])
            self.assertTrue(source_path.exists())


class TemporalExecutionTests(unittest.TestCase):
    def test_temporal_candidate_validates_only_stitched_final_outputs(self):
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            source_path = root_path / "source.mkv"
            source_path.write_bytes(b"source")
            source = _source_info(source_path)
            run_root = root_path / "run"
            matrix = benchmark.build_candidate_matrix(
                ffmpeg="ffmpeg",
                ffprobe="ffprobe",
                python_executable=sys.executable,
                source=source,
                clips=(benchmark.Clip(0, 0, 120),),
                run_root=run_root,
                temporal_chunks=4,
                nvenc_max_sessions=2,
            )
            candidate = next(
                item
                for item in matrix
                if item.candidate_id
                == "temporal_nvenc_chunks_parallel"
            )
            executor = Mock()

            def run_batch(commands, *, parallel, timeout_seconds):
                return benchmark.BatchResult(
                    wall_seconds=0.2,
                    processes=tuple(
                        benchmark.ProcessResult(
                            label=command.label,
                            args=command.args,
                            returncode=0,
                            wall_seconds=0.2,
                            stdout="",
                            stderr="",
                        )
                        for command in commands
                    ),
                )

            executor.run_batch.side_effect = run_batch
            validation = {
                "status": "passed",
                "output_bytes": 10,
                "playlist_validation": {
                    "playlist_duration_seconds": 120,
                },
            }
            quality = {
                "status": "passed",
                "metric_path": "libvmaf",
                "vmaf": 95.0,
            }
            cleanup = {
                "attempted": True,
                "deleted": True,
                "source_preserved": True,
                "error": None,
            }
            with patch.object(
                benchmark,
                "stitch_hls_chunks",
                return_value={"moved_files": 1},
            ) as stitch, patch.object(
                benchmark,
                "validate_output",
                return_value=validation,
            ) as validate, patch.object(
                benchmark,
                "measure_quality",
                return_value=quality,
            ) as measure, patch.object(
                benchmark,
                "cleanup_owned_tree",
                return_value=cleanup,
            ):
                result = benchmark.run_candidate(
                    candidate,
                    source=source,
                    ffmpeg="ffmpeg",
                    ffprobe="ffprobe",
                    executor=executor,
                    metric_path="libvmaf",
                    run_root=run_root,
                    timeout_multiplier=5,
                )

            self.assertEqual(result["status"], "passed")
            self.assertEqual(stitch.call_count, 2)
            self.assertEqual(validate.call_count, 2)
            self.assertEqual(measure.call_count, 2)
            validated_targets = [
                call.args[0] for call in validate.call_args_list
            ]
            self.assertTrue(
                all(
                    "final" in target.playlist.parts
                    for target in validated_targets
                )
            )
            self.assertTrue(
                all(
                    "chunks" not in target.playlist.parts
                    for target in validated_targets
                )
            )
            self.assertGreaterEqual(result["encode_wall_seconds"], 0.8)


class CleanupAndExecutorTests(unittest.TestCase):
    def test_failed_candidate_media_is_removed_but_source_is_preserved(self):
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            source_path = root_path / "source.mkv"
            source_path.write_bytes(b"source")
            source = _source_info(source_path)
            run_root = root_path / "owned" / "run-id"
            run_root.mkdir(parents=True)
            candidate = benchmark.CandidatePlan(
                candidate_id="failed-candidate",
                category="test",
                settings={},
                clips=(
                    benchmark.ClipPlan(
                        benchmark.Clip(0, 0, 6),
                        (
                            benchmark.BatchPlan(
                                "failed",
                                (
                                    benchmark.CommandSpec(
                                        "failed-process",
                                        ("ffmpeg",),
                                    ),
                                ),
                                (),
                                False,
                            ),
                        ),
                    ),
                ),
            )
            executor = Mock()

            def run_batch(commands, *, parallel, timeout_seconds):
                candidate_root = run_root / "failed-candidate"
                (candidate_root / "partial.ts").write_bytes(b"partial")
                return _batch_result(
                    commands[0],
                    returncode=9,
                    stderr="simulated failure",
                )

            executor.run_batch.side_effect = run_batch
            result = benchmark.run_candidate(
                candidate,
                source=source,
                ffmpeg="ffmpeg",
                ffprobe="ffprobe",
                executor=executor,
                metric_path="ssim_psnr",
                run_root=run_root,
                timeout_multiplier=5,
            )

            self.assertEqual(result["status"], "failed")
            self.assertTrue(result["cleanup"]["deleted"])
            self.assertGreater(result["cleanup"]["bytes_removed"], 0)
            self.assertFalse((run_root / "failed-candidate").exists())
            self.assertEqual(source_path.read_bytes(), b"source")

    def test_candidate_cleanup_failure_overrides_a_skipped_status(self):
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            source_path = root_path / "source.mkv"
            source_path.write_bytes(b"source")
            source = _source_info(source_path)
            source = benchmark.SourceInfo(
                **{**source.__dict__, "codec_name": "hevc"}
            )
            candidate = benchmark.CandidatePlan(
                candidate_id="copy",
                category="stream_copy",
                settings={},
                clips=(),
            )
            failed_cleanups = (
                {
                    "attempted": True,
                    "deleted": False,
                    "source_preserved": True,
                    "error": "simulated deletion failure",
                },
                {
                    "attempted": True,
                    "deleted": True,
                    "source_preserved": False,
                    "error": "simulated source check failure",
                },
            )
            for cleanup in failed_cleanups:
                with self.subTest(cleanup=cleanup), patch.object(
                    benchmark,
                    "cleanup_owned_tree",
                    return_value=dict(cleanup),
                ):
                    result = benchmark.run_candidate(
                        candidate,
                        source=source,
                        ffmpeg="ffmpeg",
                        ffprobe="ffprobe",
                        executor=Mock(),
                        metric_path="ssim_psnr",
                        run_root=root_path / "run",
                        timeout_multiplier=5,
                    )

                    self.assertEqual(result["status"], "failed")
                    self.assertEqual(
                        result["status_before_cleanup"],
                        "skipped",
                    )
                    self.assertIn("cleanup_error", result)

    def test_run_cleanup_failure_forces_failed_report(self):
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            source = root_path / "source.mkv"
            source.write_bytes(b"source")
            executable = Path(sys.executable)
            executor = Mock()

            def run_batch(commands, *, parallel, timeout_seconds):
                command = commands[0]
                if command.label == "ffmpeg-version":
                    return _batch_result(
                        command,
                        stdout="ffmpeg version mock-1.0\n",
                    )
                if command.label == "ffmpeg-filters":
                    return _batch_result(command, stdout="")
                if command.label == "probe-source":
                    return _batch_result(
                        command,
                        stdout=json.dumps(
                            {
                                "streams": [
                                    {
                                        "codec_name": "h264",
                                        "width": 1920,
                                        "height": 804,
                                        "avg_frame_rate": "24/1",
                                    }
                                ],
                                "format": {"duration": "7177.832"},
                            }
                        ),
                    )
                self.fail(f"unexpected command: {command.label}")

            executor.run_batch.side_effect = run_batch
            cleanups = (
                {
                    "attempted": True,
                    "deleted": False,
                    "source_preserved": True,
                    "error": "simulated run cleanup failure",
                },
                {
                    "attempted": True,
                    "deleted": True,
                    "source_preserved": False,
                    "error": "simulated source check failure",
                },
            )
            for index, cleanup in enumerate(cleanups):
                report_path = root_path / f"report-{index}.json"
                args = argparse.Namespace(
                    source=source,
                    report=report_path,
                    work_root=root_path / f"work-{index}",
                    ffmpeg=str(executable),
                    ffprobe=str(executable),
                    python_executable=str(executable),
                    clip_seconds=120.0,
                    clip_count=1,
                    temporal_chunks=4,
                    nvenc_max_sessions=2,
                    candidate_order_seed=1,
                    segment_format="fmp4",
                    timeout_multiplier=5.0,
                    execute=True,
                )
                with self.subTest(cleanup=cleanup), patch.object(
                    benchmark,
                    "build_candidate_matrix",
                    return_value=(),
                ), patch.object(
                    benchmark,
                    "cleanup_owned_tree",
                    return_value=dict(cleanup),
                ):
                    result = benchmark.run_harness(
                        args,
                        executor=executor,
                    )

                self.assertEqual(result["status"], "failed")
                if not cleanup["deleted"]:
                    self.assertIn("cleanup_error", result)
                else:
                    self.assertIn("source_integrity_error", result)
                self.assertEqual(
                    json.loads(
                        report_path.read_text(encoding="utf-8")
                    )["status"],
                    "failed",
                )

    @patch.object(benchmark.subprocess, "Popen")
    def test_executor_launches_mocked_process_without_shell(self, popen):
        process = Mock()
        process.returncode = 0
        process.communicate.return_value = ("progress=end\n", "")
        process.poll.return_value = 0
        popen.return_value = process
        executor = benchmark.SubprocessExecutor()
        spec = benchmark.CommandSpec("mock", ("ffmpeg", "-version"))

        result = executor.run_batch(
            [spec],
            parallel=True,
            timeout_seconds=5,
        )

        self.assertEqual(result.processes[0].returncode, 0)
        called_args, called_kwargs = popen.call_args
        self.assertEqual(called_args[0], ["ffmpeg", "-version"])
        self.assertNotIn("shell", called_kwargs)

    def test_cleanup_refuses_tree_that_contains_source(self):
        with tempfile.TemporaryDirectory() as root:
            owner = Path(root) / "owner"
            target = owner / "candidate"
            target.mkdir(parents=True)
            source = target / "source.mkv"
            source.write_bytes(b"source")

            outcome = benchmark.cleanup_owned_tree(
                target,
                owner_root=owner,
                source=source,
            )

            self.assertFalse(outcome["deleted"])
            self.assertIn("contains the source", outcome["error"])
            self.assertTrue(source.exists())


if __name__ == "__main__":
    unittest.main()
