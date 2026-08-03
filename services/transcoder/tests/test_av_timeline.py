import os
import tempfile
import unittest

from app import ffmpeg_utils


class AudioTimelineTests(unittest.TestCase):
    def test_video_codec_start_offset_does_not_trim_synced_audio(self):
        parsed = ffmpeg_utils.parse_probe(
            {
                "format": {"start_time": "0.000", "duration": "60"},
                "streams": [
                    {"codec_type": "video", "start_time": "0.105"},
                    {
                        "codec_type": "audio",
                        "start_time": "0.000",
                        "sample_rate": "48000",
                        "tags": {"encoder_delay": "1024"},
                    },
                ],
            }
        )

        self.assertEqual(parsed["video_start_time"], 0.105)
        self.assertEqual(parsed["audio_tracks"][0]["delay_ms"], 0.0)

    def test_authored_audio_offset_is_measured_from_presentation_start(self):
        parsed = ffmpeg_utils.parse_probe(
            {
                "format": {"start_time": "1.000", "duration": "60"},
                "streams": [
                    {"codec_type": "video", "start_time": "1.105"},
                    {"codec_type": "audio", "start_time": "1.500"},
                ],
            }
        )

        self.assertEqual(parsed["audio_tracks"][0]["delay_ms"], 500.0)


class SubtitlePackagingTests(unittest.TestCase):
    def test_packages_short_hls_webvtt_segments_with_timestamp_maps(self):
        with tempfile.TemporaryDirectory() as root:
            source = os.path.join(root, "source.vtt")
            output = os.path.join(root, "output")
            with open(source, "w", encoding="utf-8") as handle:
                handle.write(
                    "WEBVTT\n\n"
                    "00:05.500 --> 00:06.500\n"
                    "Crosses a segment boundary\n\n"
                    "00:08.000 --> 00:09.000\n"
                    "Second cue\n"
                )

            playlist = ffmpeg_utils.package_subtitle(
                source, output, duration=12, segment_duration=6
            )

            with open(playlist, "r", encoding="utf-8") as handle:
                manifest = handle.read()
            self.assertIn("#EXT-X-TARGETDURATION:6", manifest)
            self.assertEqual(manifest.count("#EXTINF:"), 2)
            self.assertIn("subtitles_00000.vtt", manifest)
            self.assertIn("subtitles_00001.vtt", manifest)

            with open(
                os.path.join(output, "subtitles_00000.vtt"), encoding="utf-8"
            ) as handle:
                first = handle.read()
            self.assertIn("WEBVTT\nX-TIMESTAMP-MAP=", first)
            self.assertIn("MPEGTS:0", first)
            self.assertIn("00:00:05.500 --> 00:00:06.000", first)

            with open(
                os.path.join(output, "subtitles_00001.vtt"), encoding="utf-8"
            ) as handle:
                second = handle.read()
            self.assertIn("MPEGTS:540000", second)
            self.assertIn("00:00:00.000 --> 00:00:00.500", second)
            self.assertIn("00:00:02.000 --> 00:00:03.000", second)

    def test_uses_mpegts_start_offset_for_transport_stream_renditions(self):
        with tempfile.TemporaryDirectory() as root:
            source = os.path.join(root, "source.vtt")
            output = os.path.join(root, "output")
            with open(source, "w", encoding="utf-8") as handle:
                handle.write("WEBVTT\n\n00:00.000 --> 00:01.000\nCue\n")

            ffmpeg_utils.package_subtitle(
                source,
                output,
                duration=6,
                segment_format="ts",
            )

            with open(
                os.path.join(output, "subtitles_00000.vtt"), encoding="utf-8"
            ) as handle:
                self.assertIn("MPEGTS:126000", handle.read())


if __name__ == "__main__":
    unittest.main()
