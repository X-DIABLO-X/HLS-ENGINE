import subprocess
import tempfile
import unittest
from unittest.mock import patch

from app import ffmpeg_utils


class NVDECInitializationFailureTests(unittest.TestCase):
    def test_matches_cuvid_decoder_creation_failure_from_ffmpeg_8(self):
        error = ffmpeg_utils.FFmpegError(
            "decoder->cvdl->cuvidCreateDecoder(&decoder->decoder, &cuinfo) "
            "failed -> CUDA_ERROR_OUT_OF_MEMORY: out of memory\n"
            "Failed setup for format cuda: hwaccel initialisation returned error."
        )

        self.assertTrue(
            ffmpeg_utils.is_nvdec_initialization_failure(error)
        )

    def test_matches_stderr_attribute_and_nvcuvid_load_failures(self):
        error = subprocess.CalledProcessError(
            1,
            ["ffmpeg"],
            stderr=(
                "cuvidCreateDecoder(&decoder, &info) failed -> "
                "CUDA_ERROR_NOT_SUPPORTED"
            ),
        )

        self.assertTrue(
            ffmpeg_utils.is_nvdec_initialization_failure(error)
        )
        self.assertTrue(
            ffmpeg_utils.is_nvdec_initialization_failure(
                "Cannot load libnvcuvid.so.1"
            )
        )

    def test_does_not_match_generic_cuda_or_encoder_failures(self):
        non_nvdec_failures = [
            "nvEncOpenEncodeSessionEx failed: CUDA_ERROR_OUT_OF_MEMORY",
            "scale_cuda failed: CUDA_ERROR_OUT_OF_MEMORY",
            "cuvidCreateDecoder succeeded",
            (
                "Failed setup for format cuda: "
                "hwaccel initialization returned error"
            ),
            (
                "cuvidCreateDecoder succeeded\n"
                "scale_cuda failed: CUDA_ERROR_OUT_OF_MEMORY"
            ),
            (
                "cuvidCreateDecoder succeeded; scale_cuda failed: "
                "CUDA_ERROR_OUT_OF_MEMORY"
            ),
            "FFmpeg stalled (no progress for 300s)",
        ]

        for failure in non_nvdec_failures:
            with self.subTest(failure=failure):
                self.assertFalse(
                    ffmpeg_utils.is_nvdec_initialization_failure(failure)
                )


class SoftwareDecodeGPUCommandTests(unittest.TestCase):
    RENDITIONS = [
        {"width": 1280, "height": 720, "bitrate": 3_000_000},
        {"width": 854, "height": 480, "bitrate": 1_500_000},
    ]

    def _capability_patches(self):
        return (
            patch.object(
                ffmpeg_utils,
                "_ffmpeg_encoders",
                return_value={"h264_nvenc"},
            ),
            patch.object(
                ffmpeg_utils,
                "_ffmpeg_filters",
                return_value={"scale_cuda", "hwupload_cuda"},
            ),
            patch.object(
                ffmpeg_utils,
                "_configured_nvenc_profile",
                return_value=None,
            ),
        )

    def test_multi_hybrid_uses_cpu_decode_then_cuda_upload_and_nvenc(self):
        encoder_patch, filter_patch, profile_patch = (
            self._capability_patches()
        )
        with (
            tempfile.TemporaryDirectory() as output_dir,
            encoder_patch,
            filter_patch,
            profile_patch,
            patch.object(ffmpeg_utils, "is_gpu_available") as gpu_probe,
        ):
            command = ffmpeg_utils.transcode_multi_command(
                "source.mkv",
                output_dir,
                self.RENDITIONS,
                24.0,
                gpu_index=2,
                require_gpu=True,
                software_decode_gpu=True,
            )

        gpu_probe.assert_not_called()
        self.assertNotIn("-hwaccel", command)
        self.assertEqual(
            command[command.index("-init_hw_device") + 1],
            "cuda=gpu:2",
        )
        self.assertEqual(
            command[command.index("-filter_hw_device") + 1],
            "gpu",
        )
        graph = command[command.index("-filter_complex") + 1]
        self.assertIn(
            "[0:v]setpts=PTS-STARTPTS,format=nv12,hwupload_cuda,"
            "split=2[in0][in1]",
            graph,
        )
        self.assertIn(
            "[in0]scale_cuda=1280:720:"
            "force_original_aspect_ratio=decrease:"
            "force_divisible_by=2[v0]",
            graph,
        )
        self.assertIn(
            "[in1]scale_cuda=854:480:"
            "force_original_aspect_ratio=decrease:"
            "force_divisible_by=2[v1]",
            graph,
        )
        self.assertEqual(command.count("h264_nvenc"), 2)

    def test_required_gpu_default_decode_also_skips_runtime_probe(self):
        encoder_patch, filter_patch, profile_patch = (
            self._capability_patches()
        )
        with (
            tempfile.TemporaryDirectory() as output_dir,
            encoder_patch,
            filter_patch,
            profile_patch,
            patch.object(ffmpeg_utils, "is_gpu_available") as gpu_probe,
        ):
            command = ffmpeg_utils.transcode_multi_command(
                "source.mkv",
                output_dir,
                self.RENDITIONS,
                24.0,
                gpu_index=0,
                require_gpu=True,
            )

        gpu_probe.assert_not_called()
        self.assertIn("-hwaccel", command)
        self.assertIn("-hwaccel_output_format", command)
        self.assertNotIn("-init_hw_device", command)

    def test_chunk_hybrid_reuses_the_same_gpu_upload_path(self):
        encoder_patch, filter_patch, profile_patch = (
            self._capability_patches()
        )
        with (
            tempfile.TemporaryDirectory() as output_dir,
            encoder_patch,
            filter_patch,
            profile_patch,
            patch.object(ffmpeg_utils, "is_gpu_available") as gpu_probe,
        ):
            command = ffmpeg_utils.transcode_chunk_command(
                "source.mkv",
                output_dir,
                self.RENDITIONS,
                120.0,
                60.0,
                24.0,
                gpu_index=1,
                segment_format="ts",
                require_gpu=True,
                software_decode_gpu=True,
            )

        gpu_probe.assert_not_called()
        self.assertNotIn("-hwaccel", command)
        self.assertLess(
            command.index("-init_hw_device"),
            command.index("-ss"),
        )
        self.assertEqual(
            command[command.index("-init_hw_device") + 1],
            "cuda=gpu:1",
        )
        graph = command[command.index("-filter_complex") + 1]
        self.assertIn(
            "format=nv12,hwupload_cuda,split=2",
            graph,
        )
        self.assertEqual(command.count("h264_nvenc"), 2)

    def test_hybrid_mode_fails_early_without_required_filters(self):
        with (
            tempfile.TemporaryDirectory() as output_dir,
            patch.object(
                ffmpeg_utils,
                "_ffmpeg_encoders",
                return_value={"h264_nvenc"},
            ),
            patch.object(
                ffmpeg_utils,
                "_ffmpeg_filters",
                return_value={"scale_cuda"},
            ),
            patch.object(ffmpeg_utils, "is_gpu_available") as gpu_probe,
        ):
            with self.assertRaisesRegex(
                ffmpeg_utils.FFmpegError,
                "hwupload_cuda",
            ):
                ffmpeg_utils.transcode_multi_command(
                    "source.mkv",
                    output_dir,
                    self.RENDITIONS,
                    24.0,
                    require_gpu=True,
                    software_decode_gpu=True,
                )

        gpu_probe.assert_not_called()


if __name__ == "__main__":
    unittest.main()
