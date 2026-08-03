import os
import unittest
from unittest.mock import Mock, patch

from app import worker_health


class _QueueStatus:
    def __init__(self, consumers):
        self.consumer_count = consumers


class _Connection:
    def __init__(self, consumers):
        self.consumers = consumers
        self.released = False
        self.ensured = False

    def ensure_connection(self, max_retries):
        self.ensured = max_retries == 0

    def channel(self):
        return self

    def queue_declare(self, *, queue, passive):
        if not passive or queue not in self.consumers:
            raise RuntimeError("queue unavailable")
        return _QueueStatus(self.consumers[queue])

    def release(self):
        self.released = True


class _Redis:
    def __init__(self, *, heartbeat=100.0, ttl=90, member=True):
        self.heartbeat = heartbeat
        self.live_ttl = ttl
        self.member = member

    def ping(self):
        return True

    def hgetall(self, _key):
        return {
            b"worker_type": b"gpu",
            b"capacity": b"3",
            b"last_heartbeat": str(self.heartbeat).encode(),
        }

    def sismember(self, _key, _member):
        return self.member

    def ttl(self, _key):
        return self.live_ttl


class WorkerHealthTests(unittest.TestCase):
    def test_queue_names_are_exact_deduplicated_and_safe(self):
        self.assertEqual(
            worker_health.queue_names("video, package,video"),
            ["video", "package"],
        )
        for invalid in ("", "video,,package", "../video", "video queue"):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                worker_health.queue_names(invalid)

    def test_broker_health_requires_a_consumer_on_every_queue(self):
        healthy = _Connection({"video": 1, "package": 2})
        worker_health.check_broker_consumers(
            ["video", "package"],
            connection_factory=lambda *args, **kwargs: healthy,
        )
        self.assertTrue(healthy.ensured)
        self.assertTrue(healthy.released)

        missing = _Connection({"video": 1, "package": 0})
        with self.assertRaisesRegex(RuntimeError, "no live consumer"):
            worker_health.check_broker_consumers(
                ["video", "package"],
                connection_factory=lambda *args, **kwargs: missing,
            )
        self.assertTrue(missing.released)

    def test_gpu_health_requires_fresh_registered_capacity(self):
        with patch.dict(os.environ, {"GPU_WORKER_ROLE": "true"}):
            worker_health.check_gpu_registration(
                _Redis(heartbeat=100.0),
                worker_id="gpu-worker",
                now=120.0,
            )
            with self.assertRaisesRegex(RuntimeError, "stale"):
                worker_health.check_gpu_registration(
                    _Redis(heartbeat=1.0),
                    worker_id="gpu-worker",
                    now=120.0,
                )
            with self.assertRaisesRegex(RuntimeError, "live registry"):
                worker_health.check_gpu_registration(
                    _Redis(member=False),
                    worker_id="gpu-worker",
                    now=120.0,
                )

    def test_main_fails_closed_and_passes_only_after_all_checks(self):
        client = Mock()
        with (
            patch.object(worker_health, "redis_client", return_value=client),
            patch.object(worker_health, "check_redis") as check_redis,
            patch.object(
                worker_health,
                "check_broker_consumers",
            ) as check_broker,
            patch.object(worker_health, "check_gpu_registration") as check_gpu,
            patch.dict(os.environ, {"CELERY_QUEUE": "video"}),
        ):
            self.assertEqual(worker_health.main(), 0)
            check_redis.assert_called_once_with(client)
            check_broker.assert_called_once_with(["video"])
            check_gpu.assert_called_once_with(client)

            check_broker.side_effect = RuntimeError("broker down")
            self.assertEqual(worker_health.main(), 1)


if __name__ == "__main__":
    unittest.main()
