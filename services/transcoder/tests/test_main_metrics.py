import unittest
from types import SimpleNamespace
from unittest.mock import patch

from app import main


class GpuMetricsTests(unittest.TestCase):
    def test_idle_registered_gpu_worker_is_reported_active(self):
        settings = SimpleNamespace(
            TRANSCODER_METRICS_ENABLED=True,
            CELERY_BROKER_URL="memory://",
        )
        status = [
            {
                "index": 0,
                "worker_id": "gpu-worker-1",
                "worker_ids": ["gpu-worker-1"],
                "capacity": 2,
                "in_use": 0,
            }
        ]

        with (
            patch.object(main, "get_settings", return_value=settings),
            patch.object(main.subprocess, "run") as run,
            patch.object(main.gpu_registry, "get_gpu_status", return_value=status),
            patch.object(main.gpu_workers_active, "set") as set_active,
        ):
            run.return_value.returncode = 1
            main._poll_gpu_metrics()

        set_active.assert_called_once_with(1)

    def test_orphaned_lease_without_registered_worker_is_not_active(self):
        settings = SimpleNamespace(
            TRANSCODER_METRICS_ENABLED=True,
            CELERY_BROKER_URL="memory://",
        )
        status = [
            {
                "index": 0,
                "worker_id": None,
                "worker_ids": [],
                "capacity": 2,
                "in_use": 1,
            }
        ]

        with (
            patch.object(main, "get_settings", return_value=settings),
            patch.object(main.subprocess, "run") as run,
            patch.object(main.gpu_registry, "get_gpu_status", return_value=status),
            patch.object(main.gpu_workers_active, "set") as set_active,
        ):
            run.return_value.returncode = 1
            main._poll_gpu_metrics()

        set_active.assert_called_once_with(0)


if __name__ == "__main__":
    unittest.main()
