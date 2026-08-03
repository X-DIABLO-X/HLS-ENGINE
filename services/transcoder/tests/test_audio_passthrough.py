import os
import tempfile
import unittest
from unittest.mock import patch

from app.config import Settings
from app.ffmpeg_utils import combined_audio_command


def _option_value(args, option):
    return args[args.index(option) + 1]


class AudioPassthroughCommandTests(unittest.TestCase):
    def _command(self, **overrides):
        options = {
            "stream_index": 0,
            "bitrate_kbps": 128,
            "channels": 2,
            "loudnorm": False,
            "source_codec": "aac",
            "source_profile": "LC",
            "audio_delay_ms": 0.0,
            "aac_passthrough_enabled": True,
            "source_channels": 2,
            "source_sample_rate": "48000",
        }
        options.update(overrides)
        with tempfile.TemporaryDirectory() as output_dir:
            return combined_audio_command(
                "source.mkv",
                output_dir,
                **options,
            )

    def test_eligible_aac_uses_stream_copy_without_encode_or_filters(self):
        cmd = self._command()

        self.assertEqual(_option_value(cmd, "-c:a"), "copy")
        for option in ("-af", "-b:a", "-ac", "-ar"):
            self.assertNotIn(option, cmd)
        self.assertEqual(_option_value(cmd, "-muxdelay"), "0")

    def test_passthrough_is_disabled_by_default(self):
        cmd = self._command(aac_passthrough_enabled=False)

        self.assertEqual(_option_value(cmd, "-c:a"), "aac")
        self.assertIn("-af", cmd)
        self.assertEqual(_option_value(cmd, "-ar"), "48000")

    def test_loudnorm_is_applied_when_aac_must_be_encoded(self):
        cmd = self._command(loudnorm=True)

        self.assertEqual(_option_value(cmd, "-c:a"), "aac")
        self.assertIn(
            "loudnorm=I=-16:LRA=11:TP=-1.5:linear=true",
            _option_value(cmd, "-af"),
        )

    def test_every_required_transform_forces_existing_encode_path(self):
        cases = {
            "not_aac": {"source_codec": "ac3"},
            "unknown_profile": {"source_profile": None},
            "he_aac": {"source_profile": "HE-AAC"},
            "loudnorm": {"loudnorm": True},
            "channel_conversion": {
                "source_channels": 6,
                "channels": 2,
            },
            "sample_rate_conversion": {"source_sample_rate": "44100"},
            "positive_delay": {"audio_delay_ms": 105.0},
            "negative_delay": {"audio_delay_ms": -105.0},
            "unknown_channels": {"source_channels": None},
            "unknown_sample_rate": {"source_sample_rate": None},
        }

        for case, overrides in cases.items():
            with self.subTest(case=case):
                cmd = self._command(**overrides)
                self.assertEqual(_option_value(cmd, "-c:a"), "aac")
                self.assertIn("-af", cmd)
                self.assertEqual(_option_value(cmd, "-b:a"), "128k")
                self.assertEqual(_option_value(cmd, "-ac"), "2")
                self.assertEqual(_option_value(cmd, "-ar"), "48000")

    def test_feature_setting_defaults_off_and_parses_explicit_enable(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(
                Settings(_env_file=None).AAC_PASSTHROUGH_ENABLED
            )
            self.assertTrue(
                Settings(
                    _env_file=None,
                    AAC_PASSTHROUGH_ENABLED=True,
                ).AAC_PASSTHROUGH_ENABLED
            )


if __name__ == "__main__":
    unittest.main()
