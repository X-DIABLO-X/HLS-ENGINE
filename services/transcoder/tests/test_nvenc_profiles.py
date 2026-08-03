import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from pydantic import ValidationError

from app.config import Settings
from app.ffmpeg_utils import _nvenc_encode_args


def _option_value(args, option):
    return args[args.index(option) + 1]


class NvencProfileTests(unittest.TestCase):
    def _args(
        self,
        profile,
        *,
        preset="fast",
        lookahead=True,
        aq=True,
    ):
        settings = SimpleNamespace(NVENC_PROFILE=profile)
        with patch(
            "app.ffmpeg_utils.get_settings",
            return_value=settings,
        ):
            return _nvenc_encode_args(
                "h264_nvenc",
                bitrate=4_000_000,
                gop=180,
                seg=6,
                preset=preset,
                lookahead=lookahead,
                bf=2,
                aq=aq,
            )

    def test_profile_is_opt_in_and_unset_preserves_legacy_arguments(self):
        args = self._args(None)

        self.assertEqual(
            args,
            [
                "-preset", "p4",
                "-tune", "hq",
                "-profile:v", "high",
                "-rc", "vbr",
                "-b:v", "4000000",
                "-maxrate", "6000000",
                "-bufsize", "8000000",
                "-g", "180",
                "-keyint_min", "180",
                "-sc_threshold", "0",
                "-flags", "+cgop",
                "-force_key_frames", "expr:gte(t,n_forced*6)",
                "-cq", "23",
                "-multipass", "fullres",
                "-spatial-aq", "1",
                "-temporal-aq", "1",
                "-bf", "2",
                "-2pass", "1",
                "-rc-lookahead", "32",
                "-no-scenecut", "1",
            ],
        )

    def test_quality_profile_selects_current_quality_tuning(self):
        args = self._args(
            "quality",
            preset="p3",
            lookahead=False,
            aq=False,
        )

        self.assertEqual(_option_value(args, "-preset"), "p6")
        self.assertEqual(_option_value(args, "-multipass"), "fullres")
        self.assertEqual(_option_value(args, "-rc-lookahead"), "32")
        self.assertEqual(_option_value(args, "-spatial-aq"), "1")
        self.assertEqual(_option_value(args, "-temporal-aq"), "1")
        self.assertEqual(_option_value(args, "-2pass"), "1")
        self.assertEqual(_option_value(args, "-no-scenecut"), "1")

    def test_balanced_profile_uses_quarter_resolution_multipass(self):
        args = self._args("balanced")

        self.assertEqual(_option_value(args, "-preset"), "p4")
        self.assertEqual(_option_value(args, "-multipass"), "qres")
        self.assertEqual(_option_value(args, "-rc-lookahead"), "12")
        self.assertEqual(_option_value(args, "-temporal-aq"), "1")
        self.assertNotIn("-spatial-aq", args)
        self.assertNotIn("-2pass", args)
        self.assertEqual(_option_value(args, "-no-scenecut"), "1")

    def test_turbo_profile_is_single_pass_with_no_lookahead(self):
        args = self._args("turbo")

        self.assertEqual(_option_value(args, "-preset"), "p3")
        self.assertEqual(_option_value(args, "-multipass"), "disabled")
        self.assertEqual(_option_value(args, "-rc-lookahead"), "0")
        self.assertEqual(_option_value(args, "-temporal-aq"), "1")
        self.assertNotIn("-spatial-aq", args)
        self.assertNotIn("-2pass", args)
        self.assertNotIn("-no-scenecut", args)

    def test_explicit_job_snapshot_overrides_worker_environment(self):
        with patch(
            "app.ffmpeg_utils.get_settings",
            return_value=SimpleNamespace(NVENC_PROFILE="quality"),
        ):
            args = _nvenc_encode_args(
                "h264_nvenc",
                bitrate=4_000_000,
                gop=180,
                seg=6,
                preset="p6",
                lookahead=True,
                bf=2,
                aq=True,
                nvenc_profile="turbo",
            )

        self.assertEqual(_option_value(args, "-preset"), "p3")
        self.assertEqual(_option_value(args, "-multipass"), "disabled")
        self.assertEqual(_option_value(args, "-rc-lookahead"), "0")

    def test_explicit_unset_snapshot_ignores_worker_environment(self):
        with patch(
            "app.ffmpeg_utils.get_settings",
            return_value=SimpleNamespace(NVENC_PROFILE="turbo"),
        ):
            args = _nvenc_encode_args(
                "h264_nvenc",
                bitrate=4_000_000,
                gop=180,
                seg=6,
                preset="p6",
                lookahead=True,
                bf=2,
                aq=True,
                nvenc_profile=None,
            )

        self.assertEqual(_option_value(args, "-preset"), "p6")
        self.assertEqual(_option_value(args, "-multipass"), "fullres")
        self.assertEqual(_option_value(args, "-rc-lookahead"), "32")

    def test_settings_accept_only_named_profiles_and_default_to_unset(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(Settings(_env_file=None).NVENC_PROFILE)
            with patch.dict(
                os.environ,
                {"NVENC_PROFILE": "   "},
                clear=True,
            ):
                self.assertIsNone(
                    Settings(_env_file=None).NVENC_PROFILE
                )
            self.assertIsNone(
                Settings(
                    _env_file=None,
                    NVENC_PROFILE="   ",
                ).NVENC_PROFILE
            )
            for profile in ("quality", "balanced", "turbo"):
                with self.subTest(profile=profile):
                    self.assertEqual(
                        Settings(
                            _env_file=None,
                            NVENC_PROFILE=profile,
                        ).NVENC_PROFILE,
                        profile,
                    )
            with self.assertRaises(ValidationError):
                Settings(_env_file=None, NVENC_PROFILE="fastest")


if __name__ == "__main__":
    unittest.main()
