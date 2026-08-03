import os
import tempfile
import unittest

from app.tasks.extract_audio import (
    _reset_audio_output,
    _validate_audio_output,
)


class AudioArtifactHygieneTests(unittest.TestCase):
    def test_fallback_reset_removes_every_partial_segment(self):
        with tempfile.TemporaryDirectory() as root:
            output = os.path.join(root, "audio_eng")
            os.makedirs(output)
            for name in (
                "audio.m3u8",
                "00001.aac",
                "00999.aac",
                "init.mp4",
            ):
                with open(os.path.join(output, name), "wb") as handle:
                    handle.write(b"partial")

            _reset_audio_output(output)

            self.assertTrue(os.path.isdir(output))
            self.assertEqual(os.listdir(output), [])

    def test_playlist_validation_rejects_missing_and_stale_segments(self):
        with tempfile.TemporaryDirectory() as output:
            playlist = os.path.join(output, "audio.m3u8")
            with open(playlist, "w", encoding="utf-8") as handle:
                handle.write(
                    "#EXTM3U\n"
                    "#EXTINF:6.0,\n"
                    "00001.aac\n"
                    "#EXTINF:6.0,\n"
                    "00999.aac\n"
                    "#EXT-X-ENDLIST\n"
                )
            with open(os.path.join(output, "00001.aac"), "wb") as handle:
                handle.write(b"media")

            with self.assertRaisesRegex(ValueError, "missing segment"):
                _validate_audio_output(output, playlist)

    def test_complete_audio_playlist_is_accepted(self):
        with tempfile.TemporaryDirectory() as output:
            playlist = os.path.join(output, "audio.m3u8")
            with open(playlist, "w", encoding="utf-8") as handle:
                handle.write(
                    "#EXTM3U\n"
                    "#EXTINF:6.0,\n"
                    "00001.aac\n"
                    "#EXT-X-ENDLIST\n"
                )
            with open(os.path.join(output, "00001.aac"), "wb") as handle:
                handle.write(b"media")

            _validate_audio_output(output, playlist)


if __name__ == "__main__":
    unittest.main()
