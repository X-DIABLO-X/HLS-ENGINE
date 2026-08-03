import json
import os
import tempfile
import unittest
from fractions import Fraction
from pathlib import Path

from qsv_worker import (
    EncodeJob,
    EncodingFailedError,
    JobConfigurationError,
    ProcessResult,
    QsvAdapter,
    QsvUnavailableError,
    RenditionSettings,
    SourceRange,
)


class _FakeRunner:
    def __init__(
        self,
        *,
        qsv_available=True,
        encode_succeeds=True,
        unsafe_playlist=False,
        first_packet_keyframe=True,
        discontinuous_packets=False,
        mutate_source=False,
    ):
        self.qsv_available = qsv_available
        self.encode_succeeds = encode_succeeds
        self.unsafe_playlist = unsafe_playlist
        self.first_packet_keyframe = first_packet_keyframe
        self.discontinuous_packets = discontinuous_packets
        self.mutate_source = mutate_source
        self.calls = []

    def __call__(self, args, timeout=None):
        args = [str(value) for value in args]
        self.calls.append((args, timeout))
        if "-f" in args and "lavfi" in args:
            return ProcessResult(
                tuple(args),
                0 if self.qsv_available else 1,
                stderr="" if self.qsv_available else "no QSV device",
            )

        if Path(args[0]).name.lower().startswith("ffprobe"):
            target = Path(args[-1])
            if "-show_packets" in args:
                segment_index = int(target.stem.rsplit("_", 1)[1])
                if segment_index == 0:
                    first_dts = 0.0
                    declared_duration = 6.0
                else:
                    first_dts = 6.5 if self.discontinuous_packets else 6.0
                    declared_duration = 2.0
                final_dts = first_dts + declared_duration - (1.0 / 24.0)
                payload = {
                    "packets": [
                        {
                            "pts_time": str(first_dts),
                            "dts_time": str(first_dts),
                            "duration_time": str(1.0 / 24.0),
                            "flags": (
                                "K_"
                                if self.first_packet_keyframe
                                else "__"
                            ),
                        },
                        {
                            "pts_time": str(final_dts),
                            "dts_time": str(final_dts),
                            "duration_time": str(1.0 / 24.0),
                            "flags": "__",
                        },
                    ]
                }
            elif target.suffix == ".m3u8":
                payload = {
                    "streams": [
                        {
                            "codec_name": "h264",
                            "width": 854,
                            "height": 358,
                            "avg_frame_rate": "24/1",
                        }
                    ],
                    "format": {"duration": "8.0"},
                }
            else:
                payload = {
                    "streams": [
                        {
                            "codec_name": "h264",
                            "width": 1920,
                            "height": 804,
                            "pix_fmt": "yuv420p",
                            "avg_frame_rate": "24/1",
                            "r_frame_rate": "24/1",
                        }
                    ],
                    "format": {"duration": "7177.832"},
                }
            return ProcessResult(tuple(args), 0, stdout=json.dumps(payload))

        if not self.encode_succeeds:
            return ProcessResult(
                tuple(args),
                9,
                stderr="simulated QSV encode failure",
            )

        playlist = Path(args[-1])
        playlist.parent.mkdir(parents=True, exist_ok=True)
        (playlist.parent / "segment_00000.ts").write_bytes(b"segment-zero")
        (playlist.parent / "segment_00001.ts").write_bytes(b"segment-one")
        second_uri = (
            "../escape.ts" if self.unsafe_playlist else "segment_00001.ts"
        )
        playlist.write_text(
            "#EXTM3U\n"
            "#EXT-X-VERSION:6\n"
            "#EXT-X-INDEPENDENT-SEGMENTS\n"
            "#EXTINF:6.000000,\n"
            "segment_00000.ts\n"
            "#EXTINF:2.000000,\n"
            f"{second_uri}\n"
            "#EXT-X-ENDLIST\n",
            encoding="utf-8",
        )
        if self.mutate_source:
            source = Path(args[args.index("-i") + 1])
            source.write_bytes(b"mutated-source")
        return ProcessResult(
            tuple(args),
            0,
            stdout=(
                "frame=192\n"
                "out_time_ms=8000000\n"
                "speed=8.0x\n"
                "progress=end\n"
            ),
        )


def _settings(**overrides):
    values = {
        "rendition_id": "480p",
        "width": 854,
        "height": 480,
        "bitrate_kbps": 1000,
        "maxrate_kbps": 1070,
        "bufsize_kbps": 2000,
        "fps": None,
    }
    values.update(overrides)
    return RenditionSettings(**values)


class QsvWorkerTests(unittest.TestCase):
    def test_command_uses_full_qsv_path_and_six_second_keyframes(self):
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            source = root_path / "source.mkv"
            source.write_bytes(b"input")
            job = EncodeJob(
                source=source,
                output_dir=root_path / "result",
                source_range=SourceRange(12, 8),
                rendition=_settings(fps=Fraction(24, 1)),
            ).normalized()
            adapter = QsvAdapter("ffmpeg", "ffprobe", runner=_FakeRunner())
            staging = root_path / ".staging.partial"

            command = adapter.build_encode_command(
                job,
                staging,
                Fraction(24, 1),
                (854, 358),
            )

            self.assertIn("qsv=hw", command)
            self.assertEqual(command[command.index("-hwaccel") + 1], "qsv")
            self.assertEqual(
                command[command.index("-hwaccel_output_format") + 1],
                "qsv",
            )
            self.assertEqual(
                command[command.index("-vf") + 1],
                "vpp_qsv=w=854:h=358",
            )
            self.assertEqual(command[command.index("-g") + 1], "144")
            self.assertEqual(
                command[command.index("-force_key_frames") + 1],
                "expr:gte(t,n_forced*6)",
            )
            self.assertEqual(
                command[command.index("-hls_time") + 1],
                "6",
            )
            self.assertEqual(
                command[command.index("-hls_flags") + 1],
                "independent_segments+temp_file",
            )

    def test_qsv_probe_fails_when_device_cannot_encode(self):
        adapter = QsvAdapter(
            "ffmpeg",
            "ffprobe",
            runner=_FakeRunner(qsv_available=False),
        )
        with self.assertRaisesRegex(QsvUnavailableError, "no QSV device"):
            adapter.probe_qsv()

    def test_job_rejects_odd_dimensions_and_unaligned_start(self):
        with tempfile.TemporaryDirectory() as root:
            source = Path(root) / "source.mkv"
            source.write_bytes(b"input")
            odd_job = EncodeJob(
                source=source,
                output_dir=Path(root) / "odd",
                source_range=SourceRange(0, 8),
                rendition=_settings(width=853),
            ).normalized()
            with self.assertRaisesRegex(
                JobConfigurationError,
                "even width",
            ):
                odd_job.validate()

            unaligned_job = EncodeJob(
                source=source,
                output_dir=Path(root) / "unaligned",
                source_range=SourceRange(5, 8),
                rendition=_settings(),
            ).normalized()
            with self.assertRaisesRegex(
                JobConfigurationError,
                "must align",
            ):
                unaligned_job.validate()

    def test_success_publishes_manifest_and_checksummed_artifacts(self):
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            source = root_path / "source.mkv"
            source.write_bytes(b"input")
            output = root_path / "output"
            runner = _FakeRunner()
            adapter = QsvAdapter("ffmpeg", "ffprobe", runner=runner)
            job = EncodeJob(
                source=source,
                output_dir=output,
                source_range=SourceRange(0, 8),
                rendition=_settings(),
            )

            manifest = adapter.run(job)

            self.assertEqual(manifest["status"], "complete")
            self.assertEqual(manifest["backend"], "intel_qsv")
            self.assertEqual(manifest["rendition"]["gop_frames"], 144)
            self.assertEqual(manifest["rendition"]["width"], 854)
            self.assertEqual(manifest["rendition"]["height"], 358)
            self.assertEqual(
                manifest["rendition"]["bounding_box_height"],
                480,
            )
            self.assertEqual(manifest["hls"]["segment_count"], 2)
            packet_validation = manifest["hls"]["packet_validation"]
            self.assertTrue(packet_validation["validated"])
            self.assertEqual(len(packet_validation["segments"]), 2)
            self.assertTrue(
                packet_validation["segments"][0]["first_packet_keyframe"]
            )
            self.assertTrue(
                manifest["source"]["identity_verified_unchanged"]
            )
            self.assertEqual(
                manifest["ffmpeg"]["final_progress"]["progress"],
                "end",
            )
            self.assertTrue((output / "manifest.json").is_file())
            self.assertTrue((output / "index.m3u8").is_file())
            stored = json.loads(
                (output / "manifest.json").read_text(encoding="utf-8")
            )
            artifact_names = {
                artifact["path"] for artifact in stored["artifacts"]
            }
            self.assertIn("segment_00000.ts", artifact_names)
            self.assertIn("ffmpeg.stderr.log", artifact_names)
            for artifact in stored["artifacts"]:
                self.assertEqual(len(artifact["sha256"]), 64)
            self.assertEqual(
                list(root_path.glob("*.partial")),
                [],
            )
            self.assertEqual(
                list(root_path.glob("*.qsv.lock")),
                [],
            )

    def test_encode_failure_removes_partial_output(self):
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            source = root_path / "source.mkv"
            source.write_bytes(b"input")
            output = root_path / "output"
            adapter = QsvAdapter(
                "ffmpeg",
                "ffprobe",
                runner=_FakeRunner(encode_succeeds=False),
            )
            job = EncodeJob(
                source=source,
                output_dir=output,
                source_range=SourceRange(0, 8),
                rendition=_settings(),
            )

            with self.assertRaisesRegex(
                EncodingFailedError,
                "simulated QSV encode failure",
            ):
                adapter.run(job)

            self.assertFalse(output.exists())
            self.assertEqual(
                list(root_path.glob("*.partial")),
                [],
            )
            self.assertEqual(
                list(root_path.glob("*.qsv.lock")),
                [],
            )

    def test_playlist_path_traversal_is_rejected_and_cleaned(self):
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            source = root_path / "source.mkv"
            source.write_bytes(b"input")
            output = root_path / "output"
            adapter = QsvAdapter(
                "ffmpeg",
                "ffprobe",
                runner=_FakeRunner(unsafe_playlist=True),
            )
            job = EncodeJob(
                source=source,
                output_dir=output,
                source_range=SourceRange(0, 8),
                rendition=_settings(),
            )

            with self.assertRaisesRegex(
                EncodingFailedError,
                "unsafe local segment URI",
            ):
                adapter.run(job)

            self.assertFalse(output.exists())
            self.assertEqual(list(root_path.glob("*.partial")), [])

    def test_actual_segment_first_packet_must_be_a_keyframe(self):
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            source = root_path / "source.mkv"
            source.write_bytes(b"input")
            output = root_path / "output"
            adapter = QsvAdapter(
                "ffmpeg",
                "ffprobe",
                runner=_FakeRunner(first_packet_keyframe=False),
            )
            job = EncodeJob(
                source=source,
                output_dir=output,
                source_range=SourceRange(0, 8),
                rendition=_settings(),
            )

            with self.assertRaisesRegex(
                EncodingFailedError,
                "actual keyframe",
            ):
                adapter.run(job)

            self.assertFalse(output.exists())
            self.assertEqual(list(root_path.glob("*.partial")), [])

    def test_actual_segment_packet_timeline_must_be_continuous(self):
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            source = root_path / "source.mkv"
            source.write_bytes(b"input")
            output = root_path / "output"
            adapter = QsvAdapter(
                "ffmpeg",
                "ffprobe",
                runner=_FakeRunner(discontinuous_packets=True),
            )
            job = EncodeJob(
                source=source,
                output_dir=output,
                source_range=SourceRange(0, 8),
                rendition=_settings(),
            )

            with self.assertRaisesRegex(
                EncodingFailedError,
                "timeline is discontinuous",
            ):
                adapter.run(job)

            self.assertFalse(output.exists())
            self.assertEqual(list(root_path.glob("*.partial")), [])

    def test_source_identity_change_prevents_publish(self):
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            source = root_path / "source.mkv"
            source.write_bytes(b"input")
            output = root_path / "output"
            adapter = QsvAdapter(
                "ffmpeg",
                "ffprobe",
                runner=_FakeRunner(mutate_source=True),
            )
            job = EncodeJob(
                source=source,
                output_dir=output,
                source_range=SourceRange(0, 8),
                rendition=_settings(),
            )

            with self.assertRaisesRegex(
                EncodingFailedError,
                "source identity changed",
            ):
                adapter.run(job)

            self.assertFalse(output.exists())
            self.assertEqual(list(root_path.glob("*.partial")), [])

    def test_dead_owner_lock_is_recovered_without_unlink_window(self):
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            source = root_path / "source.mkv"
            source.write_bytes(b"input")
            output = root_path / "output"
            lock_path = root_path / ".output.qsv.lock"
            lock_path.write_text(
                json.dumps(
                    {
                        "pid": 2_147_483_647,
                        "created_at": "2020-01-01T00:00:00Z",
                        "output_dir": str(output.resolve()),
                    }
                ),
                encoding="utf-8",
            )
            adapter = QsvAdapter("ffmpeg", "ffprobe", runner=_FakeRunner())
            job = EncodeJob(
                source=source,
                output_dir=output,
                source_range=SourceRange(0, 8),
                rendition=_settings(),
            )

            manifest = adapter.run(job)

            self.assertTrue(
                manifest["output_lock"]["stale_lock_recovered"]
            )
            self.assertFalse(lock_path.exists())

    def test_live_owner_lock_is_not_stolen_or_removed(self):
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            source = root_path / "source.mkv"
            source.write_bytes(b"input")
            output = root_path / "output"
            lock_path = root_path / ".output.qsv.lock"
            original_lock = json.dumps(
                {
                    "pid": os.getpid(),
                    "created_at": "2026-08-03T00:00:00Z",
                    "output_dir": str(output.resolve()),
                }
            )
            lock_path.write_text(original_lock, encoding="utf-8")
            adapter = QsvAdapter("ffmpeg", "ffprobe", runner=_FakeRunner())
            job = EncodeJob(
                source=source,
                output_dir=output,
                source_range=SourceRange(0, 8),
                rendition=_settings(),
            )

            with self.assertRaisesRegex(
                JobConfigurationError,
                "another QSV attempt holds",
            ):
                adapter.run(job)

            self.assertEqual(
                lock_path.read_text(encoding="utf-8"),
                original_lock,
            )
            self.assertFalse(output.exists())

    def test_existing_output_directory_is_never_overwritten(self):
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            source = root_path / "source.mkv"
            source.write_bytes(b"input")
            output = root_path / "output"
            output.mkdir()
            sentinel = output / "keep.txt"
            sentinel.write_text("keep", encoding="utf-8")
            adapter = QsvAdapter("ffmpeg", "ffprobe", runner=_FakeRunner())
            job = EncodeJob(
                source=source,
                output_dir=output,
                source_range=SourceRange(0, 8),
                rendition=_settings(),
            )

            with self.assertRaisesRegex(
                JobConfigurationError,
                "refusing to overwrite",
            ):
                adapter.run(job)

            self.assertEqual(sentinel.read_text(encoding="utf-8"), "keep")


if __name__ == "__main__":
    unittest.main()
