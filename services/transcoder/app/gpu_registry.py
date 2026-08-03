"""Redis-backed GPU session pool with graceful passthrough fallback.

Each GPU can hold up to ``capacity`` concurrent encoding sessions. Atomic,
weighted, expiring leases let multiple workers share a GPU without
overloading it. Redis outages fail closed while the registry is enabled;
``GPU_REGISTRY_ENABLED=false`` is the explicit single-GPU passthrough mode.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import logging
import os
import subprocess
import threading
import time
import uuid
from typing import Callable, Optional

import redis

from app.config import get_settings

logger = logging.getLogger(__name__)

# Redis key layout -----------------------------------------------------------
#   gpu:leases:<index>     -> sorted set {lease_id: expires_at}
#   gpu:lease-weights:<index> -> hash {lease_id: NVENC sessions}
#   gpu:worker:<worker_id> -> hash {index, capacity, last_heartbeat}
#   gpu:workers            -> set of registered worker_ids
#   gpu:count              -> highest observed (gpu_index + 1); count fallback
_LEASES_KEY = "gpu:leases:{index}"
_LEASE_WEIGHTS_KEY = "gpu:lease-weights:{index}"
_WORKER_KEY = "gpu:worker:{worker}"
_WORKER_SET = "gpu:workers"
_COUNT_KEY = "gpu:count"

# Lease / heartbeat cadence. GPU reservations belong to the Celery pool child
# that runs FFmpeg. The task-owned heartbeat disappears with that child, while
# the parent worker's independent registration may remain healthy. This makes a
# hard-killed child release capacity through expiry instead of letting the
# parent refresh a phantom reservation forever.
LEASE_TTL = 60
LEASE_HEARTBEAT_INTERVAL = 15
HEARTBEAT_INTERVAL = 15
WORKER_TTL = LEASE_TTL * 2

# Short probe cache so we do not shell out to nvidia-smi on every status call.
_GPU_COUNT_CACHE: dict = {"value": 0, "ts": 0.0}
_GPU_COUNT_CACHE_TTL = 30.0

_client = None

# Register the worker hash and its membership as one operation. The hash TTL is
# the source of truth for liveness; the set TTL is a final bound on stale set
# members when there are no live workers left to prune them.
_REGISTER_SCRIPT = """
local worker_key = KEYS[1]
local worker_set = KEYS[2]
local count_key = KEYS[3]
local worker_id = ARGV[1]
local idx = tonumber(ARGV[2])
local capacity = tonumber(ARGV[3])
local now = ARGV[4]
local ttl = tonumber(ARGV[5])
local registration_id = ARGV[6] or ''

if redis.call('EXISTS', worker_key) == 1 then
    local existing_id =
        redis.call('HGET', worker_key, 'registration_id') or ''
    if existing_id ~= registration_id then
        return 0
    end
end

redis.call(
    'HSET', worker_key,
    'index', idx,
    'capacity', capacity,
    'worker_type', 'gpu',
    'last_heartbeat', now,
    'registration_id', registration_id
)
redis.call('EXPIRE', worker_key, ttl)
redis.call('SADD', worker_set, worker_id)
redis.call('EXPIRE', worker_set, ttl)

local observed_count = tonumber(redis.call('GET', count_key) or 0)
local wanted_count = idx + 1
if observed_count < wanted_count then
    redis.call('SET', count_key, wanted_count)
end
return 1
"""

# Heartbeat only a registration that still exists and is typed as a GPU
# worker. Registration liveness is deliberately independent from task leases:
# a parent Celery worker must never refresh a GPU slot owned by a dead pool
# child. This check and HSET are atomic so a late heartbeat cannot recreate a
# partial hash after clean shutdown or expiry.
_HEARTBEAT_SCRIPT = """
local worker_key = KEYS[1]
local worker_set = KEYS[2]
local worker_id = ARGV[1]
local now = ARGV[2]
local worker_ttl = tonumber(ARGV[3])
local registration_id = ARGV[4] or ''

if redis.call('HGET', worker_key, 'worker_type') ~= 'gpu' then
    redis.call('SREM', worker_set, worker_id)
    return 0
end
local existing_id =
    redis.call('HGET', worker_key, 'registration_id') or ''
if existing_id ~= registration_id then
    return 0
end

redis.call('HSET', worker_key, 'last_heartbeat', now)
redis.call('EXPIRE', worker_key, worker_ttl)
redis.call('SADD', worker_set, worker_id)
redis.call('EXPIRE', worker_set, worker_ttl)
return 1
"""

# A graceful parent-worker shutdown removes only its registration. Task leases
# are independent and can be released by a live child or expire after a hard
# child/worker death.
_UNREGISTER_SCRIPT = """
local worker_key = KEYS[1]
local worker_set = KEYS[2]
local worker_id = ARGV[1]
local registration_id = ARGV[2] or ''

local existing_id =
    redis.call('HGET', worker_key, 'registration_id') or ''
if existing_id ~= registration_id then
    return 0
end
redis.call('SREM', worker_set, worker_id)
redis.call('DEL', worker_key)
return 1
"""

# Remove legacy, malformed, or stale registrations without racing a heartbeat
# that refreshed the same worker after the caller read its hash.
_REMOVE_STALE_SCRIPT = """
local worker_key = KEYS[1]
local worker_set = KEYS[2]
local worker_id = ARGV[1]
local stale_before = tonumber(ARGV[2])
local worker_type = redis.call('HGET', worker_key, 'worker_type')
local last = tonumber(redis.call('HGET', worker_key, 'last_heartbeat') or 0)

if worker_type ~= 'gpu' or last < stale_before then
    redis.call('SREM', worker_set, worker_id)
    redis.call('DEL', worker_key)
    return 1
end
return 0
"""

# Delete a legacy/orphan hash only if it is still absent from the authoritative
# worker set at execution time. Registering and pruning are both atomic, so a
# concurrent healthy registration always wins regardless of command order.
_REMOVE_ORPHAN_SCRIPT = """
local worker_key = KEYS[1]
local worker_set = KEYS[2]
local worker_id = ARGV[1]

if redis.call('SISMEMBER', worker_set, worker_id) == 0 then
    redis.call('DEL', worker_key)
    return 1
end
return 0
"""

# Weighted, expiring task leases. One grouped FFmpeg command can open several
# NVENC encoders simultaneously, so capacity is counted in encoder sessions,
# not Celery task count.
_ACQUIRE_SCRIPT = """
local leases = KEYS[1]
local weights = KEYS[2]
local lease_id = ARGV[1]
local cap = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
local now_parts = redis.call('TIME')
local now = tonumber(now_parts[1]) + (tonumber(now_parts[2]) / 1000000)

local expired = redis.call('ZRANGEBYSCORE', leases, '-inf', now)
if #expired > 0 then
    redis.call('ZREMRANGEBYSCORE', leases, '-inf', now)
    redis.call('HDEL', weights, unpack(expired))
end

local used = 0
local active = redis.call('ZRANGE', leases, 0, -1)
for _, active_id in ipairs(active) do
    used = used + tonumber(redis.call('HGET', weights, active_id) or 1)
end

if requested < 1 or cap < 1 or (used + requested) > cap then
    return 0
end

redis.call('ZADD', leases, now + ttl, lease_id)
redis.call('HSET', weights, lease_id, requested)
return 1
"""

# Only the pool child that knows the unguessable lease id can refresh or
# release it. Refresh refuses to resurrect an already-expired lease.
_REFRESH_LEASE_SCRIPT = """
local leases = KEYS[1]
local weights = KEYS[2]
local lease_id = ARGV[1]
local ttl = tonumber(ARGV[2])
local now_parts = redis.call('TIME')
local now = tonumber(now_parts[1]) + (tonumber(now_parts[2]) / 1000000)
local expires_at = tonumber(redis.call('ZSCORE', leases, lease_id) or 0)

if expires_at <= now then
    redis.call('ZREM', leases, lease_id)
    redis.call('HDEL', weights, lease_id)
    return 0
end

redis.call('ZADD', leases, 'XX', now + ttl, lease_id)
return 1
"""

_RELEASE_SCRIPT = """
local leases = KEYS[1]
local weights = KEYS[2]
local lease_id = ARGV[1]
local removed = redis.call('ZREM', leases, lease_id)
redis.call('HDEL', weights, lease_id)
if redis.call('ZCARD', leases) == 0 then
    redis.call('DEL', leases)
    redis.call('DEL', weights)
end
return removed
"""

_LEASE_USAGE_SCRIPT = """
local leases = KEYS[1]
local weights = KEYS[2]
local now_parts = redis.call('TIME')
local now = tonumber(now_parts[1]) + (tonumber(now_parts[2]) / 1000000)
local expired = redis.call('ZRANGEBYSCORE', leases, '-inf', now)
if #expired > 0 then
    redis.call('ZREMRANGEBYSCORE', leases, '-inf', now)
    redis.call('HDEL', weights, unpack(expired))
end

local used = 0
local active = redis.call('ZRANGE', leases, 0, -1)
for _, lease_id in ipairs(active) do
    used = used + tonumber(redis.call('HGET', weights, lease_id) or 1)
end
if #active == 0 then
    redis.call('DEL', leases)
    redis.call('DEL', weights)
end
return used
"""


def _redis() -> Optional[redis.Redis]:
    """Return a cached Redis client, or None if Redis is unreachable."""
    global _client
    if _client is not None:
        return _client
    try:
        settings = get_settings()
        _client = redis.from_url(
            settings.REDIS_URL,
            socket_timeout=1.0,
            socket_connect_timeout=1.0,
            decode_responses=True,
        )
    except Exception as exc:  # malformed URL / config import failure
        logger.debug("gpu_registry: redis client init failed: %s", exc)
        _client = None
    return _client


def _enabled() -> bool:
    try:
        return bool(get_settings().GPU_REGISTRY_ENABLED)
    except Exception:
        return False


def _detect_gpu_count() -> int:
    """Best-effort GPU count via nvidia-smi; 0 if unavailable."""
    try:
        out = subprocess.run(
            ["nvidia-smi", "--query-gpu=index", "--format=csv,noheader,nounits"],
            capture_output=True,
            text=True,
            timeout=3,
        )
        if out.returncode == 0:
            lines = [ln.strip() for ln in out.stdout.splitlines() if ln.strip()]
            if lines:
                return len(lines)
    except Exception as exc:
        logger.debug("gpu_registry: nvidia-smi count probe failed: %s", exc)
    return 0


def get_gpu_count() -> int:
    """Total GPUs available for scheduling (always >= 1)."""
    now = time.time()
    cached = _GPU_COUNT_CACHE.get("value", 0)
    if cached and now - _GPU_COUNT_CACHE.get("ts", 0.0) < _GPU_COUNT_CACHE_TTL:
        return cached
    count = _detect_gpu_count()
    if not count:
        client = _redis()
        if client is not None:
            try:
                raw = client.get(_COUNT_KEY)
                if raw is not None:
                    count = int(raw)
            except Exception:
                count = 0
    if not count:
        count = 1  # single-GPU passthrough
    _GPU_COUNT_CACHE["value"] = count
    _GPU_COUNT_CACHE["ts"] = now
    return count


def register_worker(
    gpu_index: int,
    worker_id: str,
    capacity: int,
    registration_id: Optional[str] = None,
) -> bool:
    """Register a worker against its pinned GPU index.

    Returns ``True`` only when Redis durably accepted the bounded registration.
    """
    if not _enabled():
        logger.info(
            "gpu_registry: disabled; skipping register_worker(gpu=%s, worker=%s)",
            gpu_index,
            worker_id,
        )
        return False
    try:
        gpu_index = int(gpu_index)
        capacity = int(capacity)
    except (TypeError, ValueError):
        logger.error(
            "gpu_registry: invalid registration gpu=%r capacity=%r",
            gpu_index,
            capacity,
        )
        return False
    if gpu_index < 0 or capacity < 1:
        logger.error(
            "gpu_registry: refusing registration gpu=%s capacity=%s",
            gpu_index,
            capacity,
        )
        return False
    client = _redis()
    if client is None:
        return False
    owner_id = str(registration_id or "")
    try:
        registered = client.eval(
            _REGISTER_SCRIPT,
            3,
            _WORKER_KEY.format(worker=worker_id),
            _WORKER_SET,
            _COUNT_KEY,
            worker_id,
            gpu_index,
            capacity,
            time.time(),
            WORKER_TTL,
            owner_id,
        )
        if registered:
            logger.info(
                "gpu_registry: registered worker=%s gpu=%s capacity=%s",
                worker_id,
                gpu_index,
                capacity,
            )
        else:
            logger.warning(
                "gpu_registry: registration refused for worker=%s because "
                "another live registration owns that ID",
                worker_id,
            )
        _prune_orphan_worker_hashes(client)
        return bool(registered)
    except Exception as exc:
        logger.warning("gpu_registry: register_worker failed (degraded mode): %s", exc)
        return False


def heartbeat(
    worker_id: str,
    registration_id: Optional[str] = None,
) -> bool:
    """Refresh only the live parent-worker registration.

    Missing/expired registrations are never recreated by a heartbeat. The
    caller must pass the NVENC probe and call :func:`register_worker` again.
    Task-owned GPU leases are intentionally untouched.
    """
    if not _enabled():
        return False
    client = _redis()
    if client is None:
        return False
    try:
        refreshed = client.eval(
            _HEARTBEAT_SCRIPT,
            2,
            _WORKER_KEY.format(worker=worker_id),
            _WORKER_SET,
            worker_id,
            time.time(),
            WORKER_TTL,
            str(registration_id or ""),
        )
        return int(refreshed or 0) == 1
    except Exception as exc:
        logger.debug("gpu_registry: heartbeat failed: %s", exc)
        return False


def unregister_worker(
    worker_id: str,
    registration_id: Optional[str] = None,
) -> bool:
    """Remove one parent worker's exact registration after clean shutdown."""
    if not _enabled():
        return False
    client = _redis()
    if client is None:
        return False
    try:
        removed = client.eval(
            _UNREGISTER_SCRIPT,
            2,
            _WORKER_KEY.format(worker=worker_id),
            _WORKER_SET,
            worker_id,
            str(registration_id or ""),
        )
        if removed:
            logger.info("gpu_registry: unregistered worker=%s", worker_id)
        return bool(removed)
    except Exception as exc:
        logger.debug("gpu_registry: unregister_worker failed: %s", exc)
        return False


def refresh_or_recover_worker(
    gpu_index: int,
    worker_id: str,
    capacity: int,
    registration_id: str,
    *,
    gpu_probe: Callable[[], bool],
    should_continue: Callable[[], bool] = lambda: True,
) -> bool:
    """Refresh a worker registration or safely recover an expired hash.

    A normal heartbeat remains non-creating. Recovery is allowed only after a
    fresh caller-supplied GPU/NVENC probe and a second liveness check, so an
    exiting helper cannot recreate a ghost registration after a slow probe.
    The registration token also prevents an old helper from refreshing,
    replacing, or unregistering a newer worker instance with the same ID.
    """
    if not should_continue():
        return False
    if heartbeat(worker_id, registration_id):
        return True
    if not should_continue():
        return False
    try:
        gpu_healthy = bool(gpu_probe())
    except Exception as exc:
        logger.warning(
            "gpu_registry: recovery probe failed worker=%s: %s",
            worker_id,
            exc,
        )
        return False
    if not gpu_healthy or not should_continue():
        return False
    return register_worker(
        gpu_index,
        worker_id,
        capacity,
        registration_id,
    )


@dataclass(frozen=True)
class GPULease:
    """One process-owned, weighted GPU reservation."""

    gpu_index: int
    lease_id: str
    worker_id: str
    slots: int = 1
    managed: bool = True
    owner_pid: int = field(default_factory=os.getpid)


def refresh_gpu_lease(lease: GPULease) -> bool:
    """Refresh an unexpired lease from its owning pool child only."""
    if not lease.managed:
        return True
    if lease.owner_pid != os.getpid():
        logger.error(
            "gpu_registry: process %s refused to refresh lease=%s owned by pid=%s",
            os.getpid(),
            lease.lease_id,
            lease.owner_pid,
        )
        return False
    client = _redis()
    if client is None:
        return False
    try:
        refreshed = client.eval(
            _REFRESH_LEASE_SCRIPT,
            2,
            _LEASES_KEY.format(index=lease.gpu_index),
            _LEASE_WEIGHTS_KEY.format(index=lease.gpu_index),
            lease.lease_id,
            LEASE_TTL,
        )
        return bool(refreshed)
    except Exception as exc:
        logger.warning(
            "gpu_registry: lease refresh failed gpu=%s lease=%s: %s",
            lease.gpu_index,
            lease.lease_id,
            exc,
        )
        return False


class GPULeaseHeartbeat:
    """Refresh one lease from the Celery pool child that owns the encode.

    The daemon thread exists only inside that child process. A hard child death
    therefore stops refreshes even when the long-lived parent worker continues
    heartbeating its registration.
    """

    def __init__(
        self,
        lease: GPULease,
        interval: float = LEASE_HEARTBEAT_INTERVAL,
    ) -> None:
        self.lease = lease
        self.interval = max(0.01, float(interval))
        self.stop_event = threading.Event()
        self.lost_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> "GPULeaseHeartbeat":
        if not self.lease.managed:
            return self
        if self.lease.owner_pid != os.getpid():
            self.lost_event.set()
            return self
        if self._thread is not None and self._thread.is_alive():
            return self
        self._thread = threading.Thread(
            target=self._run,
            name=f"gpu-lease-{self.lease.gpu_index}-{self.lease.lease_id[-8:]}",
            daemon=True,
        )
        self._thread.start()
        return self

    def _run(self) -> None:
        while not self.stop_event.wait(self.interval):
            if self.lease.owner_pid != os.getpid() or not refresh_gpu_lease(
                self.lease
            ):
                self.lost_event.set()
                logger.error(
                    "gpu_registry: lease lost gpu=%s lease=%s owner_pid=%s",
                    self.lease.gpu_index,
                    self.lease.lease_id,
                    self.lease.owner_pid,
                )
                return

    def stop(self) -> None:
        self.stop_event.set()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=max(1.0, self.interval + 1.0))
            if thread.is_alive():
                logger.warning(
                    "gpu_registry: lease heartbeat did not stop promptly "
                    "gpu=%s lease=%s",
                    self.lease.gpu_index,
                    self.lease.lease_id,
                )


def acquire_gpu(
    worker_id: Optional[str] = None,
    timeout: float = 0.0,
    *,
    slots: int = 1,
    preferred_index: Optional[int] = None,
) -> Optional[GPULease]:
    """Atomically claim weighted NVENC capacity on one registered GPU.

    Redis failure is fail-closed while the registry is enabled: callers receive
    ``None`` and can use CPU fallback without risking GPU oversubscription.
    Explicitly disabling the registry returns an unmanaged single-GPU lease.
    """
    try:
        requested_slots = int(slots)
    except (TypeError, ValueError) as exc:
        raise ValueError("GPU lease slots must be a positive integer") from exc
    if requested_slots < 1:
        raise ValueError("GPU lease slots must be a positive integer")

    wid = worker_id or "anon"
    if not _enabled():
        index = int(preferred_index) if preferred_index is not None else 0
        return GPULease(
            gpu_index=index,
            lease_id=f"unmanaged:{wid}:{os.getpid()}:{uuid.uuid4().hex}",
            worker_id=wid,
            slots=requested_slots,
            managed=False,
        )
    client = _redis()
    if client is None:
        logger.warning("gpu_registry: Redis unavailable; GPU acquire failed closed")
        return None
    count = get_gpu_count()
    regs = _worker_registrations(client)
    if preferred_index is None:
        candidates = [idx for idx in range(count) if idx in regs]
    else:
        requested_index = int(preferred_index)
        candidates = [requested_index] if requested_index in regs else []
    if not candidates:
        logger.warning(
            "gpu_registry: no live registered GPU is eligible for worker=%s",
            wid,
        )
        return None

    deadline = time.monotonic() + timeout if timeout and timeout > 0 else None
    lease_id = f"{wid}:{os.getpid()}:{uuid.uuid4().hex}"
    while True:
        for idx in candidates:
            cap = max(1, int(regs[idx]["capacity"]))
            if requested_slots > cap:
                continue
            try:
                ok = client.eval(
                    _ACQUIRE_SCRIPT,
                    2,
                    _LEASES_KEY.format(index=idx),
                    _LEASE_WEIGHTS_KEY.format(index=idx),
                    lease_id,
                    cap,
                    requested_slots,
                    LEASE_TTL,
                )
            except Exception as exc:
                logger.warning(
                    "gpu_registry: acquire script failed closed: %s",
                    exc,
                )
                return None
            if ok:
                lease = GPULease(
                    gpu_index=idx,
                    lease_id=lease_id,
                    worker_id=wid,
                    slots=requested_slots,
                )
                logger.info(
                    "gpu_registry: acquired gpu=%s slots=%s capacity=%s "
                    "worker=%s lease=%s owner_pid=%s",
                    idx,
                    requested_slots,
                    cap,
                    wid,
                    lease_id,
                    lease.owner_pid,
                )
                return lease
        if deadline is None or time.monotonic() >= deadline:
            return None
        time.sleep(0.2)


def release_gpu(lease: GPULease) -> bool:
    """Release exactly one process-owned lease without touching other tasks."""
    if not isinstance(lease, GPULease):
        raise TypeError("release_gpu requires a GPULease")
    if not lease.managed:
        return True
    if lease.owner_pid != os.getpid():
        logger.error(
            "gpu_registry: process %s refused to release lease=%s owned by pid=%s",
            os.getpid(),
            lease.lease_id,
            lease.owner_pid,
        )
        return False
    client = _redis()
    if client is None:
        return False
    try:
        removed = client.eval(
            _RELEASE_SCRIPT,
            2,
            _LEASES_KEY.format(index=lease.gpu_index),
            _LEASE_WEIGHTS_KEY.format(index=lease.gpu_index),
            lease.lease_id,
        )
        logger.info(
            "gpu_registry: released gpu=%s slots=%s worker=%s lease=%s",
            lease.gpu_index,
            lease.slots,
            lease.worker_id,
            lease.lease_id,
        )
        return bool(removed)
    except Exception as exc:
        logger.warning("gpu_registry: release_gpu failed: %s", exc)
        return False


def _lease_usage(client, gpu_index: int) -> int:
    return int(
        client.eval(
            _LEASE_USAGE_SCRIPT,
            2,
            _LEASES_KEY.format(index=gpu_index),
            _LEASE_WEIGHTS_KEY.format(index=gpu_index),
        )
        or 0
    )


def get_free_gpu_count() -> int:
    """Approximate number of free, registered NVENC session slots."""
    if not _enabled():
        return 1
    client = _redis()
    if client is None:
        return 0
    count = get_gpu_count()
    regs = _worker_registrations(client)
    free = 0
    try:
        for idx in range(count):
            if idx not in regs:
                continue
            cap = max(1, int(regs[idx]["capacity"]))
            cur = _lease_usage(client, idx)
            free += max(0, cap - cur)
    except Exception:
        return 0
    return free


def _default_capacity() -> int:
    try:
        return max(1, int(get_settings().NVENC_MAX_SESSIONS))
    except Exception:
        return 3


def _prune_orphan_worker_hashes(client) -> None:
    """Remove legacy hashes that have no corresponding worker-set member."""
    try:
        prefix = _WORKER_KEY.split("{worker}", 1)[0]
        for worker_key in client.scan_iter(
            match=_WORKER_KEY.format(worker="*"),
            count=100,
        ):
            if isinstance(worker_key, bytes):
                worker_key = worker_key.decode("utf-8", errors="strict")
            worker_key = str(worker_key)
            worker_id = worker_key[len(prefix):]
            if not worker_id:
                continue
            client.eval(
                _REMOVE_ORPHAN_SCRIPT,
                2,
                worker_key,
                _WORKER_SET,
                worker_id,
            )
    except Exception as exc:
        logger.debug("gpu_registry: orphan registration cleanup failed: %s", exc)


def _worker_registrations(client) -> dict:
    """Map GPU index to conservative capacity and live worker IDs.

    Registrations whose last heartbeat is older than ``WORKER_TTL`` are
    ignored and atomically removed so a restarted container does not leave
    stale capacity behind. If replicas disagree about a shared GPU's capacity,
    the minimum wins so a bad replica cannot permit oversubscription.
    """
    regs: dict = {}
    try:
        stale_threshold = time.time() - WORKER_TTL
        for wid in list(client.smembers(_WORKER_SET)):
            h = client.hgetall(_WORKER_KEY.format(worker=wid))
            if not h:
                client.srem(_WORKER_SET, wid)
                continue
            # Older startup code registered every Celery worker, including
            # CPU-only containers. Ignore and clean any untyped registration;
            # only a worker that passed the NVENC probe writes worker_type=gpu.
            if h.get("worker_type") != "gpu":
                client.eval(
                    _REMOVE_STALE_SCRIPT,
                    2,
                    _WORKER_KEY.format(worker=wid),
                    _WORKER_SET,
                    wid,
                    stale_threshold,
                )
                continue
            try:
                last = float(h.get("last_heartbeat", 0) or 0)
                index = int(h.get("index", 0))
                capacity = int(h.get("capacity", _default_capacity()))
            except (TypeError, ValueError):
                last = 0
            if last < stale_threshold:
                client.eval(
                    _REMOVE_STALE_SCRIPT,
                    2,
                    _WORKER_KEY.format(worker=wid),
                    _WORKER_SET,
                    wid,
                    stale_threshold,
                )
                continue
            # Bound registrations created by older releases that did not set a
            # TTL. Current register/heartbeat calls already keep this refreshed.
            worker_key = _WORKER_KEY.format(worker=wid)
            ttl = client.ttl(worker_key)
            if ttl == -2:
                client.srem(_WORKER_SET, wid)
                continue
            if ttl == -1 or ttl > WORKER_TTL:
                client.expire(worker_key, WORKER_TTL)
            capacity = max(1, capacity)
            existing = regs.get(index)
            if existing is None:
                regs[index] = {
                    "worker_id": wid,
                    "worker_ids": [wid],
                    "capacity": capacity,
                }
            else:
                worker_ids = sorted(set(existing["worker_ids"] + [wid]))
                existing["worker_ids"] = worker_ids
                existing["worker_id"] = worker_ids[0]
                existing["capacity"] = min(existing["capacity"], capacity)
    except Exception:
        pass
    _prune_orphan_worker_hashes(client)
    return regs


def get_gpu_status() -> list:
    """Per-GPU status: ``[{index, worker_id, capacity, in_use}]``.

    ``in_use`` is the number of active sessions on that GPU.
    """
    if not _enabled():
        return []
    client = _redis()
    if client is None:
        return []
    try:
        count = get_gpu_count()
        regs = _worker_registrations(client)
        default_capacity = _default_capacity()
        status = []
        for idx in range(count):
            reg = regs.get(idx, {})
            cap = reg.get("capacity", default_capacity)
            in_use = _lease_usage(client, idx)
            status.append(
                {
                    "index": idx,
                    "worker_id": reg.get("worker_id"),
                    "worker_ids": reg.get("worker_ids", []),
                    "capacity": cap,
                    "in_use": in_use,
                }
            )
        return status
    except Exception as exc:
        logger.debug("gpu_registry: get_gpu_status failed: %s", exc)
        return []
