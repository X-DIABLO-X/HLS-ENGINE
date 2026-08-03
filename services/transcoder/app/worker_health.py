"""Fail-closed Celery worker health without legacy pidbox queues."""

from __future__ import annotations

import os
import re
import socket
import sys
import time
from collections.abc import Callable
from typing import Any

from kombu import Connection
import redis

from app.config import get_settings


_QUEUE_NAME = re.compile(r"^[A-Za-z0-9_.-]+$")


def queue_names(raw: str | None = None) -> list[str]:
    """Return the worker's exact, validated queue set."""
    value = os.environ.get("CELERY_QUEUE", "celery") if raw is None else raw
    queues: list[str] = []
    for candidate in value.split(","):
        candidate = candidate.strip()
        if not candidate or not _QUEUE_NAME.fullmatch(candidate):
            raise ValueError(f"invalid Celery queue name: {candidate!r}")
        if candidate not in queues:
            queues.append(candidate)
    if not queues:
        raise ValueError("worker must consume at least one queue")
    return queues


def check_broker_consumers(
    queues: list[str],
    *,
    connection_factory: Callable[..., Any] = Connection,
) -> None:
    """Prove that RabbitMQ sees a live consumer for every assigned queue."""
    settings = get_settings()
    connection = connection_factory(
        settings.CELERY_BROKER_URL,
        connect_timeout=3,
        heartbeat=0,
    )
    try:
        connection.ensure_connection(max_retries=0)
        channel = connection.channel()
        for queue in queues:
            status = channel.queue_declare(queue=queue, passive=True)
            if int(getattr(status, "consumer_count", 0) or 0) < 1:
                raise RuntimeError(f"Celery queue has no live consumer: {queue}")
    finally:
        connection.release()


def redis_client() -> redis.Redis:
    settings = get_settings()
    return redis.from_url(
        settings.REDIS_URL,
        socket_connect_timeout=2,
        socket_timeout=2,
    )


def check_redis(client: redis.Redis) -> None:
    if not client.ping():
        raise RuntimeError("Redis ping failed")


def _decoded_hash(values: dict[Any, Any]) -> dict[str, str]:
    def decode(value: Any) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8", "strict")
        return str(value)

    return {decode(key): decode(value) for key, value in values.items()}


def check_gpu_registration(
    client: redis.Redis,
    *,
    worker_id: str | None = None,
    now: float | None = None,
) -> None:
    """Require a fresh, capacity-bearing registration for a GPU worker."""
    settings = get_settings()
    if os.environ.get("GPU_WORKER_ROLE", "").lower() != "true":
        return
    identity = worker_id or settings.GPU_WORKER_ID or socket.gethostname()
    worker_key = f"gpu:worker:{identity}"
    values = _decoded_hash(client.hgetall(worker_key) or {})
    if values.get("worker_type") != "gpu":
        raise RuntimeError("GPU worker registration is absent or malformed")
    if not client.sismember("gpu:workers", identity):
        raise RuntimeError("GPU worker is missing from the live registry")
    if int(values.get("capacity", "0")) < 1:
        raise RuntimeError("GPU worker has no advertised NVENC capacity")
    observed_at = time.time() if now is None else now
    last_heartbeat = float(values.get("last_heartbeat", "0"))
    if observed_at - last_heartbeat > 60:
        raise RuntimeError("GPU worker registration heartbeat is stale")
    if int(client.ttl(worker_key)) <= 0:
        raise RuntimeError("GPU worker registration has no live TTL")


def main() -> int:
    try:
        client = redis_client()
        check_redis(client)
        check_broker_consumers(queue_names())
        check_gpu_registration(client)
    except Exception as exc:
        print(f"worker health failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print("worker health passed", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
