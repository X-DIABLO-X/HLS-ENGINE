import os
import tempfile
import threading
import time
import unittest
from unittest.mock import MagicMock, patch

from app import celery_app as celery_module
from app import gpu_registry
from app.tasks import pipeline


class _RegistryRedis:
    """Small stateful fake for registration discovery/cleanup tests."""

    def __init__(self, hashes=None, workers=None, lease_usage=None):
        self.hashes = hashes or {}
        self.workers = set(workers or ())
        self.lease_usage = lease_usage or {}
        self.eval_calls = []
        self.expirations = {}

    def smembers(self, key):
        if key == gpu_registry._WORKER_SET:
            return set(self.workers)
        return set()

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def srem(self, key, value):
        if key == gpu_registry._WORKER_SET:
            self.workers.discard(value)

    def ttl(self, key):
        return self.expirations.get(key, -1)

    def expire(self, key, ttl):
        self.expirations[key] = ttl
        return True

    def scan_iter(self, match=None, count=None):
        prefix = gpu_registry._WORKER_KEY.split("{worker}", 1)[0]
        return iter([key for key in list(self.hashes) if key.startswith(prefix)])

    def eval(self, script, numkeys, *args):
        self.eval_calls.append((script, numkeys, args))
        if script == gpu_registry._REGISTER_SCRIPT:
            (
                worker_key,
                worker_set,
                _count_key,
                worker_id,
                index,
                capacity,
                now,
                ttl,
                registration_id,
            ) = args
            record = self.hashes.get(worker_key)
            existing_id = (
                record.get("registration_id", "")
                if record is not None
                else None
            )
            if (
                existing_id is not None
                and existing_id != registration_id
            ):
                return 0
            self.hashes[worker_key] = {
                "worker_type": "gpu",
                "index": str(index),
                "capacity": str(capacity),
                "last_heartbeat": str(now),
                "registration_id": str(registration_id),
            }
            if worker_set == gpu_registry._WORKER_SET:
                self.workers.add(worker_id)
            self.expirations[worker_key] = int(ttl)
            return 1
        if script == gpu_registry._HEARTBEAT_SCRIPT:
            (
                worker_key,
                worker_set,
                worker_id,
                now,
                ttl,
                registration_id,
            ) = args
            record = self.hashes.get(worker_key, {})
            if record.get("worker_type") != "gpu":
                self.srem(worker_set, worker_id)
                return 0
            if record.get("registration_id", "") != registration_id:
                return 0
            record["last_heartbeat"] = str(now)
            self.workers.add(worker_id)
            self.expirations[worker_key] = int(ttl)
            return 1
        if script == gpu_registry._UNREGISTER_SCRIPT:
            (
                worker_key,
                worker_set,
                worker_id,
                registration_id,
            ) = args
            record = self.hashes.get(worker_key, {})
            if record.get("registration_id", "") != registration_id:
                return 0
            self.hashes.pop(worker_key, None)
            self.srem(worker_set, worker_id)
            return 1
        if script == gpu_registry._REMOVE_STALE_SCRIPT:
            worker_key, worker_set, worker_id, stale_before = args
            record = self.hashes.get(worker_key, {})
            try:
                last = float(record.get("last_heartbeat", 0) or 0)
            except (TypeError, ValueError):
                last = 0
            if record.get("worker_type") != "gpu" or last < float(stale_before):
                self.hashes.pop(worker_key, None)
                self.srem(worker_set, worker_id)
                return 1
            return 0
        if script == gpu_registry._REMOVE_ORPHAN_SCRIPT:
            worker_key, worker_set, worker_id = args
            if worker_id not in self.workers:
                self.hashes.pop(worker_key, None)
                return 1
            return 0
        if script == gpu_registry._LEASE_USAGE_SCRIPT:
            leases_key, _weights_key = args
            return self.lease_usage.get(leases_key, 0)
        raise AssertionError("unexpected script")


class _LeaseRedis:
    """Stateful model of the task-lease Lua scripts."""

    def __init__(self, now=1_000.0):
        self.now = float(now)
        self.leases = {}
        self.weights = {}
        self.eval_calls = []
        self.fail_scripts = set()

    def advance(self, seconds):
        self.now += float(seconds)

    def _prune(self, leases_key, weights_key):
        leases = self.leases.setdefault(leases_key, {})
        weights = self.weights.setdefault(weights_key, {})
        expired = [
            lease_id
            for lease_id, expires_at in leases.items()
            if expires_at <= self.now
        ]
        for lease_id in expired:
            leases.pop(lease_id, None)
            weights.pop(lease_id, None)

    def eval(self, script, numkeys, *args):
        self.eval_calls.append((script, numkeys, args))
        if script in self.fail_scripts:
            raise ConnectionError("redis unavailable")
        if script == gpu_registry._ACQUIRE_SCRIPT:
            (
                leases_key,
                weights_key,
                lease_id,
                capacity,
                requested,
                ttl,
            ) = args
            self._prune(leases_key, weights_key)
            leases = self.leases.setdefault(leases_key, {})
            weights = self.weights.setdefault(weights_key, {})
            used = sum(int(weights.get(active_id, 1)) for active_id in leases)
            if int(requested) < 1 or used + int(requested) > int(capacity):
                return 0
            leases[lease_id] = self.now + float(ttl)
            weights[lease_id] = int(requested)
            return 1
        if script == gpu_registry._REFRESH_LEASE_SCRIPT:
            leases_key, weights_key, lease_id, ttl = args
            self._prune(leases_key, weights_key)
            leases = self.leases.setdefault(leases_key, {})
            if lease_id not in leases:
                return 0
            leases[lease_id] = self.now + float(ttl)
            return 1
        if script == gpu_registry._RELEASE_SCRIPT:
            leases_key, weights_key, lease_id = args
            leases = self.leases.setdefault(leases_key, {})
            weights = self.weights.setdefault(weights_key, {})
            removed = lease_id in leases
            leases.pop(lease_id, None)
            weights.pop(lease_id, None)
            return int(removed)
        if script == gpu_registry._LEASE_USAGE_SCRIPT:
            leases_key, weights_key = args
            self._prune(leases_key, weights_key)
            leases = self.leases.setdefault(leases_key, {})
            weights = self.weights.setdefault(weights_key, {})
            return sum(int(weights.get(active_id, 1)) for active_id in leases)
        if script == gpu_registry._HEARTBEAT_SCRIPT:
            return 1
        raise AssertionError("unexpected script")


def _lease_patches(client, capacity=3):
    return (
        patch.object(gpu_registry, "_enabled", return_value=True),
        patch.object(gpu_registry, "_redis", return_value=client),
        patch.object(gpu_registry, "get_gpu_count", return_value=1),
        patch.object(
            gpu_registry,
            "_worker_registrations",
            return_value={
                0: {
                    "worker_id": "worker-a",
                    "worker_ids": ["worker-a"],
                    "capacity": capacity,
                }
            },
        ),
    )


class GPURegistryLifecycleTests(unittest.TestCase):
    def test_register_atomically_sets_bounded_worker_and_set_ttls(self):
        client = MagicMock()
        client.eval.return_value = 1

        with (
            patch.object(gpu_registry, "_enabled", return_value=True),
            patch.object(gpu_registry, "_redis", return_value=client),
            patch.object(gpu_registry.time, "time", return_value=1234.5),
        ):
            registered = gpu_registry.register_worker(2, "worker-a", 3)

        self.assertTrue(registered)
        call = client.eval.call_args.args
        self.assertEqual(call[0], gpu_registry._REGISTER_SCRIPT)
        self.assertEqual(
            call[1:5],
            (3, "gpu:worker:worker-a", "gpu:workers", "gpu:count"),
        )
        self.assertEqual(call[5:9], ("worker-a", 2, 3, 1234.5))
        self.assertEqual(call[9], gpu_registry.WORKER_TTL)
        self.assertEqual(call[10], "")
        self.assertIn("EXPIRE', worker_key, ttl", gpu_registry._REGISTER_SCRIPT)
        self.assertIn("EXPIRE', worker_set, ttl", gpu_registry._REGISTER_SCRIPT)

    def test_heartbeat_refreshes_registration_only_and_cannot_recreate_it(self):
        client = MagicMock()
        client.eval.side_effect = [1, 0]

        with (
            patch.object(gpu_registry, "_enabled", return_value=True),
            patch.object(gpu_registry, "_redis", return_value=client),
            patch.object(gpu_registry.time, "time", return_value=5678.0),
        ):
            self.assertTrue(gpu_registry.heartbeat("worker-a"))
            self.assertFalse(gpu_registry.heartbeat("expired-worker"))

        first = client.eval.call_args_list[0].args
        self.assertEqual(first[0], gpu_registry._HEARTBEAT_SCRIPT)
        self.assertEqual(
            first[1:],
            (
                2,
                "gpu:worker:worker-a",
                "gpu:workers",
                "worker-a",
                5678.0,
                gpu_registry.WORKER_TTL,
                "",
            ),
        )
        type_check = gpu_registry._HEARTBEAT_SCRIPT.index(
            "HGET', worker_key, 'worker_type'"
        )
        timestamp_write = gpu_registry._HEARTBEAT_SCRIPT.index(
            "HSET', worker_key, 'last_heartbeat'"
        )
        self.assertLess(type_check, timestamp_write)
        self.assertNotIn("lease", gpu_registry._HEARTBEAT_SCRIPT.lower())
        self.assertNotIn("counter", gpu_registry._HEARTBEAT_SCRIPT.lower())

    def test_stale_and_legacy_hashes_are_deleted_not_only_unindexed(self):
        now = time.time()
        client = _RegistryRedis(
            workers={"live", "stale", "legacy", "expired"},
            hashes={
                "gpu:worker:live": {
                    "worker_type": "gpu",
                    "index": "0",
                    "capacity": "3",
                    "last_heartbeat": str(now),
                },
                "gpu:worker:stale": {
                    "worker_type": "gpu",
                    "index": "1",
                    "capacity": "2",
                    "last_heartbeat": str(now - gpu_registry.WORKER_TTL - 1),
                },
                "gpu:worker:legacy": {
                    "index": "2",
                    "capacity": "1",
                    "last_heartbeat": str(now),
                },
                "gpu:worker:orphan": {
                    "worker_type": "gpu",
                    "index": "3",
                    "capacity": "1",
                    "last_heartbeat": str(now),
                },
            },
        )

        with patch.object(gpu_registry.time, "time", return_value=now):
            registrations = gpu_registry._worker_registrations(client)

        self.assertEqual(
            registrations,
            {
                0: {
                    "worker_id": "live",
                    "worker_ids": ["live"],
                    "capacity": 3,
                }
            },
        )
        self.assertEqual(client.workers, {"live"})
        self.assertNotIn("gpu:worker:stale", client.hashes)
        self.assertNotIn("gpu:worker:legacy", client.hashes)
        self.assertNotIn("gpu:worker:orphan", client.hashes)
        self.assertEqual(
            client.expirations["gpu:worker:live"],
            gpu_registry.WORKER_TTL,
        )

    def test_shared_gpu_registrations_use_conservative_minimum_capacity(self):
        now = time.time()
        client = _RegistryRedis(
            workers={"worker-a", "worker-b"},
            hashes={
                "gpu:worker:worker-a": {
                    "worker_type": "gpu",
                    "index": "0",
                    "capacity": "3",
                    "last_heartbeat": str(now),
                },
                "gpu:worker:worker-b": {
                    "worker_type": "gpu",
                    "index": "0",
                    "capacity": "8",
                    "last_heartbeat": str(now),
                },
            },
        )

        with patch.object(gpu_registry.time, "time", return_value=now):
            registrations = gpu_registry._worker_registrations(client)

        self.assertEqual(registrations[0]["capacity"], 3)
        self.assertEqual(
            registrations[0]["worker_ids"],
            ["worker-a", "worker-b"],
        )

    def test_live_registration_keeps_gpu_routing_and_expired_one_does_not(self):
        now = time.time()
        client = _RegistryRedis(
            workers={"live"},
            hashes={
                "gpu:worker:live": {
                    "worker_type": "gpu",
                    "index": "0",
                    "capacity": "3",
                    "last_heartbeat": str(now),
                }
            },
            lease_usage={"gpu:leases:0": 1},
        )
        with (
            patch.object(gpu_registry, "_enabled", return_value=True),
            patch.object(gpu_registry, "_redis", return_value=client),
            patch.object(gpu_registry, "get_gpu_count", return_value=1),
            patch.object(gpu_registry.time, "time", return_value=now),
        ):
            status = gpu_registry.get_gpu_status()

        self.assertEqual(status[0]["worker_id"], "live")
        self.assertEqual(status[0]["in_use"], 1)
        self.assertEqual(pipeline._live_gpu_indices(status), [0])
        self.assertEqual(pipeline._video_queue([0]), pipeline.GPU_VIDEO_QUEUE)

        client.hashes["gpu:worker:live"]["last_heartbeat"] = str(
            now - gpu_registry.WORKER_TTL - 1
        )
        with (
            patch.object(gpu_registry, "_enabled", return_value=True),
            patch.object(gpu_registry, "_redis", return_value=client),
            patch.object(gpu_registry, "get_gpu_count", return_value=1),
            patch.object(gpu_registry.time, "time", return_value=now),
        ):
            status = gpu_registry.get_gpu_status()

        self.assertIsNone(status[0]["worker_id"])
        self.assertEqual(pipeline._live_gpu_indices(status), [])
        self.assertEqual(pipeline._video_queue([]), pipeline.CPU_VIDEO_QUEUE)

    def test_unregister_never_releases_child_owned_task_leases(self):
        client = MagicMock()
        client.eval.return_value = 1

        with (
            patch.object(gpu_registry, "_enabled", return_value=True),
            patch.object(gpu_registry, "_redis", return_value=client),
        ):
            self.assertTrue(gpu_registry.unregister_worker("worker-a"))

        call = client.eval.call_args.args
        self.assertEqual(
            call,
            (
                gpu_registry._UNREGISTER_SCRIPT,
                2,
                "gpu:worker:worker-a",
                "gpu:workers",
                "worker-a",
                "",
            ),
        )
        self.assertNotIn("lease", gpu_registry._UNREGISTER_SCRIPT.lower())

    def test_expired_hash_recovers_after_fresh_healthy_probe(self):
        now = time.time()
        worker_key = "gpu:worker:worker-a"
        client = _RegistryRedis(
            workers={"worker-a"},
            hashes={
                worker_key: {
                    "worker_type": "gpu",
                    "index": "0",
                    "capacity": "2",
                    "last_heartbeat": str(now),
                    "registration_id": "registration-a",
                }
            },
        )
        probe = MagicMock(return_value=True)

        with (
            patch.object(gpu_registry, "_enabled", return_value=True),
            patch.object(gpu_registry, "_redis", return_value=client),
            patch.object(gpu_registry.time, "time", return_value=now),
        ):
            self.assertTrue(
                gpu_registry.heartbeat("worker-a", "registration-a")
            )
            # Redis expires only the bounded worker hash; a stale set member
            # may remain until the next heartbeat/status cleanup.
            client.hashes.pop(worker_key)
            recovered = gpu_registry.refresh_or_recover_worker(
                0,
                "worker-a",
                2,
                "registration-a",
                gpu_probe=probe,
            )

        self.assertTrue(recovered)
        probe.assert_called_once_with()
        self.assertEqual(
            client.hashes[worker_key]["registration_id"],
            "registration-a",
        )
        self.assertIn("worker-a", client.workers)
        self.assertEqual(
            sum(
                script == gpu_registry._REGISTER_SCRIPT
                for script, _numkeys, _args in client.eval_calls
            ),
            1,
        )

    def test_expired_hash_probe_failure_does_not_recreate_registration(self):
        client = _RegistryRedis(workers={"worker-a"})
        probe = MagicMock(return_value=False)

        with (
            patch.object(gpu_registry, "_enabled", return_value=True),
            patch.object(gpu_registry, "_redis", return_value=client),
        ):
            recovered = gpu_registry.refresh_or_recover_worker(
                0,
                "worker-a",
                2,
                "registration-a",
                gpu_probe=probe,
            )

        self.assertFalse(recovered)
        probe.assert_called_once_with()
        self.assertNotIn("gpu:worker:worker-a", client.hashes)
        self.assertFalse(
            any(
                script == gpu_registry._REGISTER_SCRIPT
                for script, _numkeys, _args in client.eval_calls
            )
        )

    def test_shutdown_during_probe_cannot_recreate_registration(self):
        alive = {"value": True}

        def probe():
            alive["value"] = False
            return True

        with (
            patch.object(
                gpu_registry,
                "heartbeat",
                return_value=False,
            ) as heartbeat,
            patch.object(gpu_registry, "register_worker") as register,
        ):
            recovered = gpu_registry.refresh_or_recover_worker(
                0,
                "worker-a",
                2,
                "registration-a",
                gpu_probe=probe,
                should_continue=lambda: alive["value"],
            )

        self.assertFalse(recovered)
        heartbeat.assert_called_once_with("worker-a", "registration-a")
        register.assert_not_called()

    def test_registration_token_fences_old_helper_after_replacement(self):
        now = time.time()
        worker_key = "gpu:worker:worker-a"
        client = _RegistryRedis(
            workers={"worker-a"},
            hashes={
                worker_key: {
                    "worker_type": "gpu",
                    "index": "0",
                    "capacity": "2",
                    "last_heartbeat": str(now),
                    "registration_id": "new-registration",
                }
            },
        )

        with (
            patch.object(gpu_registry, "_enabled", return_value=True),
            patch.object(gpu_registry, "_redis", return_value=client),
        ):
            self.assertFalse(
                gpu_registry.heartbeat("worker-a", "old-registration")
            )
            self.assertFalse(
                gpu_registry.unregister_worker(
                    "worker-a",
                    "old-registration",
                )
            )
            self.assertFalse(
                gpu_registry.register_worker(
                    0,
                    "worker-a",
                    2,
                    "old-registration",
                )
            )
            self.assertFalse(gpu_registry.heartbeat("worker-a"))
            self.assertFalse(gpu_registry.unregister_worker("worker-a"))
            self.assertFalse(
                gpu_registry.register_worker(0, "worker-a", 2)
            )

        self.assertEqual(
            client.hashes[worker_key]["registration_id"],
            "new-registration",
        )
        self.assertIn("worker-a", client.workers)

    def test_hard_child_death_expires_lease_while_parent_remains_healthy(self):
        client = _LeaseRedis()
        enabled, redis_client, gpu_count, registrations = _lease_patches(
            client,
            capacity=1,
        )
        with enabled, redis_client, gpu_count, registrations:
            first = gpu_registry.acquire_gpu("worker-a")
            self.assertIsNotNone(first)
            leases_key = gpu_registry._LEASES_KEY.format(index=0)
            original_expiry = client.leases[leases_key][first.lease_id]

            # This models the still-healthy Celery parent after its pool child
            # was hard-killed: registration heartbeat must not touch the lease.
            self.assertTrue(gpu_registry.heartbeat("worker-a"))
            self.assertEqual(
                client.leases[leases_key][first.lease_id],
                original_expiry,
            )

            client.advance(gpu_registry.LEASE_TTL + 1)
            replacement = gpu_registry.acquire_gpu("worker-a")

        self.assertIsNotNone(replacement)
        self.assertNotEqual(first.lease_id, replacement.lease_id)

    def test_normal_long_encode_refreshes_until_clean_release(self):
        client = _LeaseRedis()
        enabled, redis_client, gpu_count, registrations = _lease_patches(
            client,
            capacity=2,
        )
        with enabled, redis_client, gpu_count, registrations:
            lease = gpu_registry.acquire_gpu("worker-a", slots=2)
            self.assertIsNotNone(lease)
            client.advance(gpu_registry.LEASE_TTL - 10)
            self.assertTrue(gpu_registry.refresh_gpu_lease(lease))

            # The original expiry has passed, but the task-owned refresh keeps
            # a competing grouped encode out.
            client.advance(20)
            self.assertIsNone(gpu_registry.acquire_gpu("worker-a", slots=1))
            self.assertTrue(gpu_registry.release_gpu(lease))
            self.assertIsNotNone(gpu_registry.acquire_gpu("worker-a", slots=2))

    def test_lease_heartbeat_runs_in_owner_and_stops_cleanly(self):
        lease = gpu_registry.GPULease(
            gpu_index=0,
            lease_id="lease-heartbeat",
            worker_id="worker-a",
        )
        refreshed = threading.Event()

        def refresh(_lease):
            refreshed.set()
            return True

        with patch.object(
            gpu_registry,
            "refresh_gpu_lease",
            side_effect=refresh,
        ) as refresh_call:
            heartbeat = gpu_registry.GPULeaseHeartbeat(
                lease,
                interval=0.01,
            ).start()
            self.assertTrue(refreshed.wait(1))
            heartbeat.stop()
            calls_after_stop = refresh_call.call_count
            time.sleep(0.04)

        self.assertFalse(heartbeat.lost_event.is_set())
        self.assertEqual(refresh_call.call_count, calls_after_stop)

    def test_redis_failure_fails_closed_and_notifies_running_encode(self):
        client = _LeaseRedis()
        client.fail_scripts.add(gpu_registry._ACQUIRE_SCRIPT)
        enabled, redis_client, gpu_count, registrations = _lease_patches(
            client,
            capacity=3,
        )
        with enabled, redis_client, gpu_count, registrations:
            self.assertIsNone(gpu_registry.acquire_gpu("worker-a"))

        lease = gpu_registry.GPULease(
            gpu_index=0,
            lease_id="lease-lost",
            worker_id="worker-a",
        )
        with patch.object(
            gpu_registry,
            "refresh_gpu_lease",
            return_value=False,
        ):
            heartbeat = gpu_registry.GPULeaseHeartbeat(
                lease,
                interval=0.01,
            ).start()
            self.assertTrue(heartbeat.lost_event.wait(1))
            heartbeat.stop()

    def test_weighted_leases_never_oversubscribe_encoder_capacity(self):
        client = _LeaseRedis()
        enabled, redis_client, gpu_count, registrations = _lease_patches(
            client,
            capacity=3,
        )
        with enabled, redis_client, gpu_count, registrations:
            two_slots = gpu_registry.acquire_gpu("worker-a", slots=2)
            self.assertIsNotNone(two_slots)
            self.assertIsNone(gpu_registry.acquire_gpu("worker-a", slots=2))
            one_slot = gpu_registry.acquire_gpu("worker-a", slots=1)
            self.assertIsNotNone(one_slot)
            self.assertIsNone(gpu_registry.acquire_gpu("worker-a", slots=1))
            self.assertEqual(gpu_registry._lease_usage(client, 0), 3)

            self.assertTrue(gpu_registry.release_gpu(two_slots))
            replacement = gpu_registry.acquire_gpu("worker-a", slots=2)
            self.assertIsNotNone(replacement)
            self.assertEqual(gpu_registry._lease_usage(client, 0), 3)

            # Exact release cannot accidentally remove the other task.
            self.assertTrue(gpu_registry.release_gpu(one_slot))
            self.assertEqual(gpu_registry._lease_usage(client, 0), 2)

    def test_non_owner_process_cannot_refresh_or_release_lease(self):
        lease = gpu_registry.GPULease(
            gpu_index=0,
            lease_id="foreign-lease",
            worker_id="worker-a",
            owner_pid=os.getpid() + 1,
        )
        client = MagicMock()
        with patch.object(gpu_registry, "_redis", return_value=client):
            self.assertFalse(gpu_registry.refresh_gpu_lease(lease))
            self.assertFalse(gpu_registry.release_gpu(lease))
        client.eval.assert_not_called()

    def test_celery_clean_shutdown_unregisters_only_gpu_role(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            stop_file = os.path.join(temp_dir, "registration.stop")

            def unregister_after_stop_marker(*_args):
                self.assertTrue(os.path.isfile(stop_file))
                return True

            with (
                patch.dict(
                    os.environ,
                    {
                        "GPU_WORKER_ROLE": "true",
                        "GPU_REGISTRATION_ID": "registration-a",
                        "GPU_REGISTRATION_STOP_FILE": stop_file,
                    },
                ),
                patch.object(
                    celery_module.settings,
                    "GPU_WORKER_ID",
                    "worker-a",
                ),
                patch.object(
                    celery_module.gpu_registry,
                    "unregister_worker",
                    side_effect=unregister_after_stop_marker,
                ) as unregister,
            ):
                celery_module.unregister_gpu_worker_on_shutdown()
        unregister.assert_called_once_with("worker-a", "registration-a")

        with (
            patch.dict(os.environ, {"GPU_WORKER_ROLE": "false"}),
            patch.object(
                celery_module.gpu_registry,
                "unregister_worker",
            ) as unregister,
        ):
            celery_module.unregister_gpu_worker_on_shutdown()
        unregister.assert_not_called()


if __name__ == "__main__":
    unittest.main()
