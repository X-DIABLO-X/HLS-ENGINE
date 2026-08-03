import unittest
from unittest.mock import patch

from app import ffmpeg_utils


class LadderGeometryTests(unittest.TestCase):
    def test_cinematic_source_fits_standard_hls_bounding_boxes(self):
        ladder = ffmpeg_utils.get_ladder_for_qualities(
            804,
            1920,
            [720, 480],
        )

        self.assertEqual(
            [(rung["width"], rung["height"]) for rung in ladder],
            [(1280, 720), (854, 480)],
        )

    def test_narrower_source_keeps_its_aspect_derived_width(self):
        ladder = ffmpeg_utils.get_ladder_for_qualities(
            1080,
            1440,
            [720, 480],
        )

        self.assertEqual(
            [(rung["width"], rung["height"]) for rung in ladder],
            [(960, 720), (640, 480)],
        )

    def test_default_ladder_uses_the_same_width_caps(self):
        ladder = ffmpeg_utils.get_ladder(804, 1920)

        self.assertEqual(
            [(rung["width"], rung["height"]) for rung in ladder],
            [(1280, 720), (854, 480), (640, 360)],
        )

    def test_gpu_scalers_force_encoder_safe_even_dimensions(self):
        for scaler in ("scale_cuda", "scale_npp"):
            with self.subTest(scaler=scaler), patch.object(
                ffmpeg_utils,
                "_gpu_scaler",
                return_value=scaler,
            ):
                filter_chain = ffmpeg_utils._scale_filter_for(
                    True,
                    854,
                    480,
                )

            self.assertIn(
                "force_original_aspect_ratio=decrease",
                filter_chain,
            )
            self.assertIn("force_divisible_by=2", filter_chain)


if __name__ == "__main__":
    unittest.main()
