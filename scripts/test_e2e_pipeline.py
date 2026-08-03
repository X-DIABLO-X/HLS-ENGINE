import unittest
from unittest.mock import patch

import httpx

from scripts import e2e_pipeline


def response(status: int, *, retry_after: str | None = None) -> httpx.Response:
    headers = {"Retry-After": retry_after} if retry_after is not None else {}
    return httpx.Response(
        status,
        headers=headers,
        request=httpx.Request("DELETE", "http://test/videos/video-id"),
    )


def job_response(status: int, payload=None) -> httpx.Response:
    return httpx.Response(
        status,
        json=payload,
        request=httpx.Request("GET", "http://test/jobs/job-id"),
    )


class FakeClient:
    def __init__(self, deletes, lookups):
        self.deletes = list(deletes)
        self.lookups = list(lookups)

    def delete(self, _url, *, headers, timeout):
        result = self.deletes.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    def get(self, _url, *, headers, timeout):
        return self.lookups.pop(0)


class DeleteVideoWithRetryTests(unittest.TestCase):
    def test_retries_conflict_service_failure_and_timeout(self):
        request = httpx.Request("DELETE", "http://test/videos/video-id")
        client = FakeClient(
            [
                response(409, retry_after="0"),
                response(503, retry_after="0"),
                httpx.ReadTimeout("timed out", request=request),
                response(204),
            ],
            [response(404)],
        )
        result = e2e_pipeline.delete_video_with_retry(
            client,
            "http://test/videos/video-id",
            {"Authorization": "Bearer token"},
            timeout_seconds=5,
            initial_backoff_seconds=0,
        )
        self.assertTrue(result["passed"])
        self.assertEqual(result["attempts"], 4)
        self.assertEqual(result["lookup_status"], 404)
        self.assertGreaterEqual(result["seconds"], 0)

    def test_resumes_when_lookup_still_sees_tombstone(self):
        client = FakeClient(
            [response(204), response(404)],
            [response(200), response(404)],
        )
        result = e2e_pipeline.delete_video_with_retry(
            client,
            "http://test/videos/video-id",
            {},
            timeout_seconds=5,
            initial_backoff_seconds=0,
        )
        self.assertTrue(result["passed"])
        self.assertEqual(result["attempts"], 2)

    def test_nonretryable_response_fails_immediately(self):
        client = FakeClient([response(500)], [])
        with self.assertRaises(RuntimeError):
            e2e_pipeline.delete_video_with_retry(
                client,
                "http://test/videos/video-id",
                {},
                timeout_seconds=5,
                initial_backoff_seconds=0,
            )


class WorkspaceCleanupVerificationTests(unittest.TestCase):
    def test_waits_for_active_workspace_then_verifies_exact_absence(self):
        client = FakeClient(
            [],
            [
                job_response(
                    200,
                    {
                        "status": "transcoding",
                        "workspace": {
                            "state": "present",
                            "exists": True,
                            "path": "/tmp/hls-work/job-id",
                        },
                        "workspaces": [
                            {
                                "job_id": "job-id",
                                "job_status": "transcoding",
                                "state": "present",
                                "exists": True,
                                "path": "/tmp/hls-work/job-id",
                            }
                        ],
                    },
                ),
                job_response(
                    200,
                    {
                        "status": "failed",
                        "workspace": {
                            "state": "absent",
                            "exists": False,
                            "path": "/tmp/hls-work/job-id",
                        },
                        "workspaces": [
                            {
                                "job_id": "job-id",
                                "job_status": "failed",
                                "state": "absent",
                                "exists": False,
                                "path": "/tmp/hls-work/job-id",
                            }
                        ],
                    },
                ),
            ],
        )
        with patch.object(e2e_pipeline.time, "sleep"):
            result = e2e_pipeline.wait_for_workspace_cleanup(
                client,
                "http://test/jobs/job-id",
                {},
                timeout_seconds=5,
                poll_seconds=0.1,
            )

        self.assertTrue(result["passed"])
        self.assertEqual(result["attempts"], 2)
        self.assertEqual(result["workspace"]["state"], "absent")

    def test_missing_database_row_is_not_mistaken_for_removed_workspace(self):
        client = FakeClient(
            [],
            [
                job_response(404, {"detail": "not created"}),
                job_response(
                    200,
                    {
                        "status": "failed",
                        "workspace": {
                            "state": "absent",
                            "exists": False,
                            "path": "/tmp/hls-work/job-id",
                        },
                        "workspaces": [
                            {
                                "job_id": "job-id",
                                "job_status": "failed",
                                "state": "absent",
                                "exists": False,
                                "path": "/tmp/hls-work/job-id",
                            }
                        ],
                    },
                ),
            ],
        )
        with patch.object(e2e_pipeline.time, "sleep"):
            result = e2e_pipeline.wait_for_workspace_cleanup(
                client,
                "http://test/jobs/job-id",
                {},
                timeout_seconds=5,
                poll_seconds=0.1,
            )

        self.assertEqual(result["attempts"], 2)
        self.assertEqual(result["job_status"], "failed")

    def test_waits_for_every_retry_generation_workspace(self):
        primary_absent = {
            "job_id": "initial-job",
            "job_status": "failed",
            "state": "absent",
            "exists": False,
            "path": "/tmp/hls-work/initial-job",
        }
        replacement_present = {
            "job_id": "replacement-job",
            "job_status": "transcoding",
            "state": "present",
            "exists": True,
            "path": "/tmp/hls-work/replacement-job",
        }
        replacement_absent = {
            **replacement_present,
            "job_status": "completed",
            "state": "absent",
            "exists": False,
        }
        client = FakeClient(
            [],
            [
                job_response(
                    200,
                    {
                        "status": "failed",
                        "workspace": primary_absent,
                        "workspaces": [
                            primary_absent,
                            replacement_present,
                        ],
                    },
                ),
                job_response(
                    200,
                    {
                        "status": "failed",
                        "workspace": primary_absent,
                        "workspaces": [
                            primary_absent,
                            replacement_absent,
                        ],
                    },
                ),
            ],
        )
        with patch.object(e2e_pipeline.time, "sleep"):
            result = e2e_pipeline.wait_for_workspace_cleanup(
                client,
                "http://test/jobs/initial-job",
                {},
                timeout_seconds=5,
                poll_seconds=0.1,
            )

        self.assertEqual(result["attempts"], 2)
        self.assertEqual(len(result["workspaces"]), 2)
        self.assertTrue(
            all(not item["exists"] for item in result["workspaces"])
        )

    def test_absent_pending_workspace_is_not_terminal_cleanup(self):
        absent = {
            "job_id": "job-id",
            "state": "absent",
            "exists": False,
            "path": "/tmp/hls-work/job-id",
        }
        client = FakeClient(
            [],
            [
                job_response(
                    200,
                    {
                        "status": "queued",
                        "workspace": absent,
                        "workspaces": [
                            {**absent, "job_status": "queued"}
                        ],
                    },
                ),
                job_response(
                    200,
                    {
                        "status": "failed",
                        "workspace": absent,
                        "workspaces": [
                            {**absent, "job_status": "failed"}
                        ],
                    },
                ),
            ],
        )
        with patch.object(e2e_pipeline.time, "sleep"):
            result = e2e_pipeline.wait_for_workspace_cleanup(
                client,
                "http://test/jobs/job-id",
                {},
                timeout_seconds=5,
                poll_seconds=0.1,
            )

        self.assertEqual(result["attempts"], 2)
        self.assertEqual(result["workspaces"][0]["job_status"], "failed")

    def test_unsafe_workspace_fails_closed(self):
        client = FakeClient(
            [],
            [
                job_response(
                    200,
                    {
                        "status": "failed",
                        "workspace": {
                            "state": "unsafe",
                            "exists": True,
                            "error": "symlink",
                        },
                        "workspaces": [
                            {
                                "job_id": "job-id",
                                "job_status": "failed",
                                "state": "unsafe",
                                "exists": True,
                                "error": "symlink",
                            }
                        ],
                    },
                )
            ],
        )
        with self.assertRaisesRegex(RuntimeError, "unsafe workspace"):
            e2e_pipeline.wait_for_workspace_cleanup(
                client,
                "http://test/jobs/job-id",
                {},
                timeout_seconds=5,
            )

    def test_ingestion_job_id_is_stable_and_object_scoped(self):
        first = e2e_pipeline.ingestion_job_id(
            "video-id",
            "uploads-raw",
            "ab/video/source.mkv",
        )
        repeated = e2e_pipeline.ingestion_job_id(
            "video-id",
            "uploads-raw",
            "ab/video/source.mkv",
        )
        other = e2e_pipeline.ingestion_job_id(
            "video-id",
            "uploads-raw",
            "ab/video/other.mkv",
        )
        self.assertEqual(first, repeated)
        self.assertNotEqual(first, other)


if __name__ == "__main__":
    unittest.main()
