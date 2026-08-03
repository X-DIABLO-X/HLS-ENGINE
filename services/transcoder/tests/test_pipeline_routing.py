import unittest

from app.config import Settings
from app.tasks.pipeline import (
    CHUNK_MAX_SLICE_SEC,
    CPU_VIDEO_QUEUE,
    GPU_VIDEO_QUEUE,
    _adaptive_chunk_duration,
    _build_chunks,
    _live_gpu_indices,
    _partition_gpu_groups,
    _video_queue,
    on_pipeline_failure,
)


class PipelineRoutingTests(unittest.TestCase):
    def test_live_gpu_workers_select_gpu_queue(self):
        status = [
            {"index": 0, "worker_id": "gpu-a"},
            {"index": 1, "worker_id": "gpu-b"},
            {"index": 2, "worker_id": None},
        ]

        indices = _live_gpu_indices(status)

        self.assertEqual(indices, [0, 1])
        self.assertEqual(_video_queue(indices), GPU_VIDEO_QUEUE)

    def test_missing_gpu_worker_selects_cpu_fallback_queue(self):
        indices = _live_gpu_indices([{"index": 0, "worker_id": None}])

        self.assertEqual(indices, [])
        self.assertEqual(_video_queue(indices), CPU_VIDEO_QUEUE)

    def test_four_rungs_split_at_capacity_three_and_stay_on_gpu(self):
        status = [
            {
                "index": 0,
                "worker_id": "gpu-a",
                "capacity": 3,
                "in_use": 0,
            }
        ]

        routed = _partition_gpu_groups(
            [360, 1080, 480, 720],
            status,
        )

        self.assertEqual(
            routed,
            [
                (0, [1080, 720, 480]),
                (0, [360]),
            ],
        )
        self.assertTrue(all(len(group) <= 3 for _gpu, group in routed))
        self.assertTrue(all(gpu == 0 for gpu, _group in routed))
        self.assertEqual(
            _video_queue(_live_gpu_indices(status)),
            GPU_VIDEO_QUEUE,
        )

    def test_pipeline_failure_errback_can_be_immutable(self):
        signature = on_pipeline_failure.si("video-1", "job-1")

        self.assertTrue(signature.immutable)
        self.assertEqual(signature.args, ("video-1", "job-1"))

    def test_explicit_chunk_opt_in_is_bounded_and_routes_to_gpu(self):
        duration = 7177.832
        settings = Settings(
            _env_file=None,
            CHUNKED_ENCODING=True,
            CHUNK_DURATION_SEC=60,
        )

        self.assertTrue(settings.CHUNKED_ENCODING)
        chunk_duration = _adaptive_chunk_duration(
            duration,
            base=settings.CHUNK_DURATION_SEC,
        )
        chunks = _build_chunks(duration, chunk_duration)

        self.assertEqual(chunk_duration, 240)
        self.assertEqual(len(chunks), 30)
        self.assertTrue(all(0 < length <= CHUNK_MAX_SLICE_SEC for _, length in chunks))
        self.assertAlmostEqual(sum(length for _, length in chunks), duration)
        self.assertAlmostEqual(chunks[-1][0] + chunks[-1][1], duration)
        self.assertEqual(_video_queue([settings.GPU_INDEX]), GPU_VIDEO_QUEUE)

    def test_unsafe_chunk_base_is_clamped_to_five_minutes(self):
        self.assertEqual(
            _adaptive_chunk_duration(7200, base=3600),
            CHUNK_MAX_SLICE_SEC,
        )


if __name__ == "__main__":
    unittest.main()
