#!/usr/bin/env bash
set -e

QUEUE_NAME=${CELERY_QUEUE:-celery}
CONCURRENCY=${CELERY_CONCURRENCY:-2}
LOG_LEVEL=${CELERY_LOG_LEVEL:-info}
GPU_WORKER_ROLE=${GPU_WORKER_ROLE:-false}
CELERY_BIN=${CELERY_BIN:-celery}

echo "============================================"
echo " Starting Celery worker"
echo "  queue      : $QUEUE_NAME"
echo "  concurrency: $CONCURRENCY"
echo "  log level  : $LOG_LEVEL"
echo "============================================"

# Ensure Python can resolve the 'app' package regardless of CWD
export PYTHONPATH="/app:${PYTHONPATH:-}"

# GPU: eagerly detect and log at startup so the container logs show whether
# NVENC is usable without waiting for the first transcoding task.
python - <<'PYGPUCHECK' 2>/tmp/gpu-detect.log || true
import logging, sys
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
try:
    from app.ffmpeg_utils import is_gpu_available
    is_gpu_available()
except Exception as exc:
    print(f"[gpu-detect] warning: {exc}", flush=True)
PYGPUCHECK

echo "GPU detection log (if any):"
cat /tmp/gpu-detect.log || true
echo "============================================"

# ---------------------------------------------------------------------------
# GPU registry: only the dedicated NVENC worker may advertise GPU capacity.
# Registering a CPU worker here made the scheduler send video jobs to an empty
# GPU queue whenever the actual GPU worker was offline.
# ---------------------------------------------------------------------------
if [[ "${GPU_WORKER_ROLE,,}" == "true" ]]; then
GPU_REGISTRATION_ID=${GPU_REGISTRATION_ID:-$(python -c 'import uuid; print(uuid.uuid4())')}
GPU_REGISTRATION_STOP_FILE=${GPU_REGISTRATION_STOP_FILE:-/tmp/gpu-registration-${GPU_REGISTRATION_ID}.stop}
export GPU_REGISTRATION_ID
export GPU_REGISTRATION_STOP_FILE

python - <<'PYREG' 2>/tmp/gpu-register.log || true
import logging, os, socket, sys
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
try:
    from app.config import get_settings
    from app import gpu_registry
    from app.ffmpeg_utils import is_gpu_available
    if not is_gpu_available():
        print("[gpu-registry] NVENC unavailable; registration skipped", flush=True)
        raise SystemExit(0)
    s = get_settings()
    worker_id = s.GPU_WORKER_ID or socket.gethostname()
    registered = gpu_registry.register_worker(
        gpu_index=s.GPU_INDEX,
        worker_id=worker_id,
        capacity=s.NVENC_MAX_SESSIONS,
        registration_id=os.environ["GPU_REGISTRATION_ID"],
    )
    if registered:
        print(f"[gpu-registry] registered worker={worker_id} gpu={s.GPU_INDEX} capacity={s.NVENC_MAX_SESSIONS}", flush=True)
    else:
        print(f"[gpu-registry] registration deferred worker={worker_id}", flush=True)
except Exception as exc:
    print(f"[gpu-registry] register failed (degraded mode): {exc}", flush=True)
PYREG
cat /tmp/gpu-register.log || true

# Background heartbeat loop so this GPU worker's registration/locks stay
# alive. It is intentionally not detached: Celery replaces this wrapper as the
# container's primary process below (beneath Docker's tiny init when enabled),
# so the helper can never keep a dead worker container alive. Task-owned GPU
# leases are refreshed inside pool children and expire independently.
python - <<'PYHB' >/tmp/gpu-heartbeat.log 2>&1 &
import os, socket, sys, time
worker_id = None
was_registered = False
try:
    from app.config import get_settings
    from app import gpu_registry
    from app.ffmpeg_utils import detect_gpu
    s = get_settings()
    worker_id = s.GPU_WORKER_ID or socket.gethostname()
    registration_id = os.environ["GPU_REGISTRATION_ID"]
    stop_file = os.environ["GPU_REGISTRATION_STOP_FILE"]
    interval = 15
    worker_parent_pid = os.getppid()
    parent_alive = lambda: (
        os.getppid() == worker_parent_pid
        and not os.path.exists(stop_file)
    )
    while parent_alive():
        try:
            active = gpu_registry.refresh_or_recover_worker(
                gpu_index=s.GPU_INDEX,
                worker_id=worker_id,
                capacity=s.NVENC_MAX_SESSIONS,
                registration_id=registration_id,
                # detect_gpu performs a fresh bounded NVENC encode probe.
                gpu_probe=detect_gpu,
                should_continue=parent_alive,
            )
            was_registered = active or was_registered
        except Exception:
            pass
        time.sleep(interval)
except Exception:
    pass
finally:
    if worker_id and was_registered:
        try:
            gpu_registry.unregister_worker(worker_id, registration_id)
        except Exception:
            pass
PYHB
else
  echo "[gpu-registry] CPU worker role; GPU registration skipped"
fi

# `exec` is a liveness invariant, not an optimization. Celery replaces the
# wrapper as the primary process, so any Celery exit becomes a container exit
# and Compose's restart policy can recover it. Never use `celery & wait`.
exec "$CELERY_BIN" -A app.celery_app worker \
  -Q "$QUEUE_NAME" \
  -c "$CONCURRENCY" \
  --without-mingle \
  --without-gossip \
  --loglevel="$LOG_LEVEL" \
  -n "worker@%h"
