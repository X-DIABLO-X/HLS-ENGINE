"""Integration coverage for the Redis Lua progress state machine."""

import os
import unittest
from unittest.mock import patch
from uuid import uuid4

import redis

from app import progress


REDIS_INTEGRATION_URL = os.environ.get("HLS_REDIS_INTEGRATION_URL", "")


@unittest.skipUnless(
    REDIS_INTEGRATION_URL,
    "set HLS_REDIS_INTEGRATION_URL to run Redis integration tests",
)
class ProgressRedisIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.client = redis.from_url(REDIS_INTEGRATION_URL)
        self.client.ping()
        self.addCleanup(self.client.close)

        self.video_id = f"integration-{uuid4()}"
        self.key = f"video:{self.video_id}:progress"
        self.addCleanup(self.client.delete, self.key)

        redis_patcher = patch.object(
            progress,
            "_redis",
            return_value=self.client,
        )
        redis_patcher.start()
        self.addCleanup(redis_patcher.stop)

    def test_stale_generation_is_rejected_and_ttl_is_preserved(self):
        progress.init_progress(
            self.video_id,
            1,
            ["transcode_720p"],
            job_id="job-old",
        )
        progress.update_task(
            self.video_id,
            "transcode_720p",
            75,
            job_id="job-old",
        )

        progress.init_progress(
            self.video_id,
            1,
            ["transcode_720p"],
            job_id="job-current",
        )
        initial_ttl = self.client.ttl(self.key)
        initial_updated_at = self.client.hget(self.key, "updated_at")

        progress.update_task(
            self.video_id,
            "transcode_720p",
            99,
            stage="stale task",
            job_id="job-old",
        )
        progress.set_percent(
            self.video_id,
            99,
            stage="stale overall",
            job_id="job-old",
        )
        progress.set_stage(
            self.video_id,
            "stale stage",
            job_id="job-old",
        )

        state = progress.get_progress(self.video_id)
        self.assertEqual(state["job_id"], "job-current")
        self.assertEqual(state["percent"], 0)
        self.assertEqual(state["stage"], "Initializing")
        self.assertEqual(
            state["tasks"]["transcode_720p"],
            {"status": "pending", "percent": 0},
        )
        self.assertEqual(
            self.client.hget(self.key, "updated_at"),
            initial_updated_at,
        )
        self.assertGreater(initial_ttl, 0)
        self.assertLessEqual(initial_ttl, 86_400)
        self.assertGreater(self.client.ttl(self.key), 0)

    def test_duplicate_and_out_of_order_chunks_are_monotonic(self):
        progress.init_progress(
            self.video_id,
            1,
            ["transcode_720p"],
            job_id="job-current",
        )
        self.assertTrue(progress.configure_chunked_task(
            self.video_id,
            "transcode_720p",
            3,
            job_id="job-current",
        ))

        progress.update_chunked_task(
            self.video_id,
            "transcode_720p",
            2,
            3,
            100,
            job_id="job-current",
        )
        after_out_of_order = progress.get_progress(self.video_id)
        progress.update_chunked_task(
            self.video_id,
            "transcode_720p",
            2,
            3,
            10,
            job_id="job-current",
        )
        after_duplicate = progress.get_progress(self.video_id)

        self.assertEqual(
            after_duplicate["tasks"],
            after_out_of_order["tasks"],
        )
        self.assertEqual(
            after_duplicate["percent"],
            after_out_of_order["percent"],
        )
        self.assertGreaterEqual(
            float(after_duplicate["updated_at"]),
            float(after_out_of_order["updated_at"]),
        )
        self.assertEqual(
            after_duplicate["tasks"]["transcode_720p"]["chunks"],
            {"completed": 1, "total": 3},
        )

        for chunk_index, chunk_percent in ((0, 80), (1, 100), (0, 100)):
            progress.update_chunked_task(
                self.video_id,
                "transcode_720p",
                chunk_index,
                3,
                chunk_percent,
                job_id="job-current",
            )

        before_completion = progress.get_progress(self.video_id)
        task = before_completion["tasks"]["transcode_720p"]
        self.assertEqual(task["percent"], 99)
        self.assertEqual(task["chunks"], {"completed": 3, "total": 3})
        self.assertEqual(before_completion["percent"], 99)
        self.assertEqual(
            int(self.client.hget(
                self.key,
                "_chunk:sum:transcode_720p",
            )),
            300,
        )

        progress.complete_task(
            self.video_id,
            "transcode_720p",
            job_id="job-current",
        )
        completed = progress.get_progress(self.video_id)
        self.assertEqual(completed["percent"], 100)
        self.assertEqual(completed["completed_tasks"], 1)
        self.assertEqual(
            completed["tasks"]["transcode_720p"]["status"],
            "completed",
        )

    def test_accepted_mutation_refreshes_ttl_but_stale_update_does_not(self):
        progress.init_progress(
            self.video_id,
            1,
            ["transcode_720p"],
            job_id="job-current",
        )
        self.client.expire(self.key, 60)

        progress.update_task(
            self.video_id,
            "transcode_720p",
            25,
            job_id="job-current",
        )

        self.assertGreater(self.client.ttl(self.key), 86_000)

        self.client.expire(self.key, 60)
        progress.update_task(
            self.video_id,
            "transcode_720p",
            90,
            job_id="job-stale",
        )

        stale_ttl = self.client.ttl(self.key)
        self.assertGreater(stale_ttl, 0)
        self.assertLessEqual(stale_ttl, 60)
        self.assertEqual(
            progress.get_progress(self.video_id)["tasks"][
                "transcode_720p"
            ]["percent"],
            25,
        )

    def test_task_replacement_is_generation_fenced_and_idempotent(self):
        progress.init_progress(
            self.video_id,
            1,
            ["gpu_group"],
            job_id="job-current",
        )
        progress.update_task(
            self.video_id,
            "gpu_group",
            55,
            job_id="job-current",
        )

        progress.replace_task(
            self.video_id,
            "gpu_group",
            ["stale_720p", "stale_480p"],
            job_id="job-old",
        )
        stale_state = progress.get_progress(self.video_id)
        self.assertEqual(stale_state["total_tasks"], 1)
        self.assertEqual(set(stale_state["tasks"]), {"gpu_group"})
        self.assertEqual(stale_state["tasks"]["gpu_group"]["percent"], 55)

        replacements = ["cpu_720p", "cpu_480p"]
        progress.replace_task(
            self.video_id,
            "gpu_group",
            replacements,
            job_id="job-current",
        )
        progress.update_task(
            self.video_id,
            "cpu_720p",
            70,
            job_id="job-current",
        )
        progress.replace_task(
            self.video_id,
            "gpu_group",
            replacements,
            job_id="job-current",
        )

        state = progress.get_progress(self.video_id)
        self.assertEqual(state["total_tasks"], 2)
        self.assertEqual(set(state["tasks"]), set(replacements))
        self.assertEqual(state["tasks"]["cpu_720p"]["percent"], 70)
        self.assertEqual(
            state["tasks"]["cpu_480p"],
            {"status": "pending", "percent": 0},
        )

    def test_replacement_requires_live_source_and_blocks_resurrection(self):
        progress.init_progress(
            self.video_id,
            1,
            ["direct"],
            job_id="job-current",
        )
        initial = progress.get_progress(self.video_id)
        self.assertFalse(progress.replace_task(
            self.video_id,
            "missing",
            ["fallback"],
            job_id="job-current",
        ))
        self.assertEqual(progress.get_progress(self.video_id), initial)

        progress.complete_task(
            self.video_id,
            "direct",
            job_id="job-current",
        )
        completed = progress.get_progress(self.video_id)
        self.assertFalse(progress.replace_task(
            self.video_id,
            "direct",
            ["fallback"],
            job_id="job-current",
        ))
        self.assertEqual(progress.get_progress(self.video_id), completed)

        progress.init_progress(
            self.video_id,
            1,
            ["direct"],
            job_id="job-current",
        )
        self.assertTrue(progress.replace_task(
            self.video_id,
            "direct",
            ["fallback"],
            job_id="job-current",
        ))
        replaced = progress.get_progress(self.video_id)
        self.assertFalse(progress.replace_task(
            self.video_id,
            "direct",
            ["other"],
            job_id="job-current",
        ))
        progress.update_task(
            self.video_id,
            "direct",
            90,
            stage="delayed direct delivery",
            job_id="job-current",
        )

        self.assertEqual(progress.get_progress(self.video_id), replaced)
        self.assertEqual(set(replaced["tasks"]), {"fallback"})


if __name__ == "__main__":
    unittest.main()
