import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from app import ffmpeg_utils
from app.tasks.package import _write_master
from app.tasks import transcode_video as transcode_tasks
from app.tasks.transcode_video import (
    _canonical_renditions_reusable,
    _prepare_rendition_rows,
)
from tests.test_transcode_single_pass_idempotency import (
    _FakeSession,
    _write_fmp4_rendition,
)
from tests.test_video_passthrough import _write_ts_rendition


def _direct_spec(profile="High", level=40):
    return {
        "height": 804,
        "width": 1920,
        "bitrate": 2_500_000,
        "codec": "h264",
        "profile": profile,
        "level": level,
    }


class DirectPlayCodecsTests(unittest.TestCase):
    def test_rfc6381_uses_actual_direct_profile_and_level(self):
        cases = (
            ("Baseline", 30, "avc1.42001e"),
            ("Constrained Baseline", 31, "avc1.42e01f"),
            ("Main", 32, "avc1.4d0020"),
            ("High", 40, "avc1.640028"),
            ("High", 52, "avc1.640034"),
        )

        for profile, level, expected in cases:
            with self.subTest(profile=profile, level=level):
                persisted = ffmpeg_utils.direct_h264_profile_metadata(
                    profile,
                    level,
                )
                self.assertEqual(
                    ffmpeg_utils.codecs_string(
                        "h264",
                        804,
                        persisted,
                    ),
                    expected,
                )

    def test_encoded_rendition_metadata_remains_height_derived_high(self):
        self.assertEqual(
            ffmpeg_utils.codecs_string("h264", 1080, "high"),
            "avc1.640029",
        )
        # Historically this argument was ignored for encoded renditions.
        self.assertEqual(
            ffmpeg_utils.codecs_string("h264", 1080, "main"),
            "avc1.640029",
        )

    def test_invalid_persisted_direct_metadata_fails_closed(self):
        for value in (
            "direct-h264:high:not-a-level",
            "direct-h264:high:99",
            "direct-h264:high 10:40",
        ):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    ffmpeg_utils.codecs_string("h264", 1080, value)

    def test_direct_metadata_is_persisted_and_part_of_reuse_identity(self):
        video_id = "video-direct-codecs"
        with tempfile.TemporaryDirectory() as temp_dir:
            canonical = Path(temp_dir) / "output"
            _write_fmp4_rendition(canonical, 804)
            db = _FakeSession()

            rows, _details = _prepare_rendition_rows(
                db,
                video_id,
                str(canonical),
                [_direct_spec()],
                "fmp4",
                6.0,
            )
            db.commit()

            self.assertEqual(rows[0].profile, "direct-h264:high:40")
            self.assertTrue(
                _canonical_renditions_reusable(
                    db,
                    video_id,
                    str(canonical),
                    [_direct_spec()],
                    "fmp4",
                    6.0,
                )
            )
            self.assertFalse(
                _canonical_renditions_reusable(
                    db,
                    video_id,
                    str(canonical),
                    [_direct_spec(profile="Main")],
                    "fmp4",
                    6.0,
                )
            )

    def test_master_advertises_exact_movie_high_level_40(self):
        with tempfile.TemporaryDirectory() as output_dir:
            playlist_dir = os.path.join(output_dir, "video_804p")
            os.makedirs(playlist_dir)
            playlist_path = os.path.join(playlist_dir, "video.m3u8")
            Path(playlist_path).write_text("#EXTM3U\n", encoding="utf-8")
            master_path = os.path.join(output_dir, "master.m3u8")
            rendition = SimpleNamespace(
                bandwidth=2_500_000,
                width=1920,
                height=804,
                codec="h264",
                profile=ffmpeg_utils.direct_h264_profile_metadata(
                    "High",
                    40,
                ),
                playlist_path=playlist_path,
            )

            with patch(
                "app.tasks.package.ffmpeg_utils.ffprobe",
                return_value={
                    "streams": [
                        {
                            "codec_type": "video",
                            "width": 1920,
                            "height": 804,
                        }
                    ]
                },
            ):
                _write_master(
                    master_path,
                    output_dir,
                    [rendition],
                    [],
                    [],
                )
            master = Path(master_path).read_text(encoding="utf-8")

        self.assertIn('CODECS="avc1.640028"', master)
        self.assertIn("RESOLUTION=1920x804", master)

    def test_master_rejects_malformed_persisted_direct_metadata(self):
        with tempfile.TemporaryDirectory() as output_dir:
            playlist_path = os.path.join(output_dir, "video.m3u8")
            Path(playlist_path).write_text("#EXTM3U\n", encoding="utf-8")
            master_path = os.path.join(output_dir, "master.m3u8")
            rendition = SimpleNamespace(
                bandwidth=2_500_000,
                width=1920,
                height=804,
                codec="h264",
                profile="direct-h264:high:not-a-level",
                playlist_path=playlist_path,
            )

            with patch(
                "app.tasks.package.ffmpeg_utils.ffprobe",
                return_value={
                    "streams": [
                        {
                            "codec_type": "video",
                            "width": 1920,
                            "height": 804,
                        }
                    ]
                },
            ), self.assertRaisesRegex(
                ValueError,
                "invalid persisted direct-play H.264",
            ):
                _write_master(
                    master_path,
                    output_dir,
                    [rendition],
                    [],
                    [],
                )

            self.assertFalse(os.path.exists(master_path))

    def test_master_uses_actual_scaled_frames_not_rendition_boxes(self):
        with tempfile.TemporaryDirectory() as output_dir:
            renditions = []
            actual_by_path = {}
            for name, box, actual in (
                ("720p", (1280, 720), (1280, 536)),
                ("480p", (854, 480), (854, 358)),
            ):
                playlist_dir = os.path.join(output_dir, f"video_{name}")
                os.makedirs(playlist_dir)
                playlist_path = os.path.join(
                    playlist_dir,
                    "video.m3u8",
                )
                Path(playlist_path).write_text(
                    "#EXTM3U\n",
                    encoding="utf-8",
                )
                actual_by_path[playlist_path] = actual
                renditions.append(
                    SimpleNamespace(
                        bandwidth=2_500_000,
                        width=box[0],
                        height=box[1],
                        codec="h264",
                        profile="high",
                        playlist_path=playlist_path,
                    )
                )

            def probe(path):
                width, height = actual_by_path[path]
                return {
                    "streams": [
                        {
                            "codec_type": "video",
                            "width": width,
                            "height": height,
                        }
                    ]
                }

            master_path = os.path.join(output_dir, "master.m3u8")
            with patch(
                "app.tasks.package.ffmpeg_utils.ffprobe",
                side_effect=probe,
            ):
                _write_master(
                    master_path,
                    output_dir,
                    renditions,
                    [],
                    [],
                )
            master = Path(master_path).read_text(encoding="utf-8")

        self.assertIn("RESOLUTION=1280x536", master)
        self.assertIn("RESOLUTION=854x358", master)
        self.assertNotIn("RESOLUTION=1280x720", master)
        self.assertNotIn("RESOLUTION=854x480", master)

    def test_master_fails_closed_when_actual_dimensions_cannot_be_probed(self):
        with tempfile.TemporaryDirectory() as output_dir:
            playlist_path = os.path.join(output_dir, "video.m3u8")
            Path(playlist_path).write_text("#EXTM3U\n", encoding="utf-8")
            rendition = SimpleNamespace(
                bandwidth=1,
                width=1280,
                height=720,
                codec="h264",
                profile="high",
                playlist_path=playlist_path,
            )
            master_path = os.path.join(output_dir, "master.m3u8")

            with patch(
                "app.tasks.package.ffmpeg_utils.ffprobe",
                return_value={"streams": []},
            ), self.assertRaisesRegex(RuntimeError, "no video stream"):
                _write_master(
                    master_path,
                    output_dir,
                    [rendition],
                    [],
                    [],
                )

            self.assertFalse(os.path.exists(master_path))

    def test_remux_validation_fences_profile_and_level_metadata(self):
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
                    "height": 804,
                }
            ],
            "format": {"duration": "3.0"},
        }
        packets = [
            {
                "pts_time": "0.000",
                "dts_time": "0.000",
                "flags": "K_",
            }
        ]
        mismatches = (
            (_direct_spec(profile="Main"), "profile changed"),
            (_direct_spec(level=41), "level changed"),
        )

        for rendition, error in mismatches:
            with self.subTest(error=error), tempfile.TemporaryDirectory() as output:
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
                    with self.assertRaisesRegex(
                        transcode_tasks.RenditionValidationError,
                        error,
                    ):
                        transcode_tasks._validate_direct_play_output(
                            str(rendition_dir),
                            rendition,
                            "ts",
                            3.0,
                            24.0,
                        )


if __name__ == "__main__":
    unittest.main()
