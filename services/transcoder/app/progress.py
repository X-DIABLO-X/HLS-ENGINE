"""Redis-based progress tracking for video processing."""

import json
import logging
import time
from typing import Optional

import redis

from app.config import get_settings

logger = logging.getLogger(__name__)

_GENERATION_HSET_SCRIPT = """
local current = redis.call('HGET', KEYS[1], 'job_id')
if current and current ~= '' then
  if ARGV[1] == '' or current ~= ARGV[1] then
    return 0
  end
elseif ARGV[1] ~= '' then
  return 0
end
for i = 2, #ARGV, 2 do
  redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1])
end
return 1
"""

_INIT_GENERATION_SCRIPT = """
redis.call('DEL', KEYS[1])
for i = 1, #ARGV, 2 do
  redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1])
end
redis.call('EXPIRE', KEYS[1], 86400)
return 1
"""


def _redis():
    return redis.from_url(get_settings().REDIS_URL)


def _redis_value(value) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return str(value)


def _generation_hset(
    video_id: str,
    job_id: Optional[str],
    mapping: dict,
) -> bool:
    """Atomically mutate progress only for the current job generation."""
    r = _redis()
    args = [str(job_id or "")]
    for key, value in mapping.items():
        args.extend((str(key), _redis_value(value)))
    return bool(
        r.eval(
            _GENERATION_HSET_SCRIPT,
            1,
            f"video:{video_id}:progress",
            *args,
        )
    )


def init_progress(
    video_id: str,
    total_tasks: int,
    task_names: list,
    job_id: Optional[str] = None,
) -> None:
    """Initialize the progress hash for a video."""
    r = _redis()
    tasks = {name: json.dumps({"status": "pending", "percent": 0}) for name in task_names}
    mapping = {
        "job_id": str(job_id or ""),
        "percent": 0,
        "stage": "Initializing",
        "total_tasks": total_tasks,
        "completed_tasks": 0,
        "updated_at": time.time(),
        **{f"task:{name}": val for name, val in tasks.items()},
    }
    args = []
    for key, value in mapping.items():
        args.extend((str(key), _redis_value(value)))
    r.eval(
        _INIT_GENERATION_SCRIPT,
        1,
        f"video:{video_id}:progress",
        *args,
    )


def start_task(
    video_id: str,
    task_name: str,
    stage: str,
    job_id: Optional[str] = None,
) -> None:
    """Mark a task as started."""
    _generation_hset(video_id, job_id, {
        "stage": stage,
        "updated_at": time.time(),
        f"task:{task_name}": json.dumps({"status": "running", "percent": 0}),
    })


def update_task(
    video_id: str,
    task_name: str,
    percent: int,
    stage: Optional[str] = None,
    job_id: Optional[str] = None,
) -> None:
    """Update a task's progress percentage."""
    mapping = {
        f"task:{task_name}": json.dumps({"status": "running", "percent": percent}),
        "updated_at": time.time(),
    }
    if stage:
        mapping["stage"] = stage
    if _generation_hset(video_id, job_id, mapping):
        _recompute_overall(video_id, job_id=job_id)


def complete_task(
    video_id: str,
    task_name: str,
    job_id: Optional[str] = None,
) -> None:
    """Mark a task as completed and recompute overall progress."""
    if _generation_hset(video_id, job_id, {
        f"task:{task_name}": json.dumps({"status": "completed", "percent": 100}),
        "updated_at": time.time(),
    }):
        _recompute_overall(video_id, job_id=job_id)


def fail_task(
    video_id: str,
    task_name: str,
    error: str = "",
    job_id: Optional[str] = None,
) -> None:
    """Mark a task as failed."""
    if _generation_hset(video_id, job_id, {
        f"task:{task_name}": json.dumps({"status": "failed", "percent": 0, "error": error}),
        "stage": "Failed",
        "updated_at": time.time(),
    }):
        _recompute_overall(video_id, job_id=job_id)


def set_stage(
    video_id: str,
    stage: str,
    job_id: Optional[str] = None,
) -> None:
    """Set the current processing stage label."""
    _generation_hset(video_id, job_id, {
        "stage": stage,
        "updated_at": time.time(),
    })


def set_percent(
    video_id: str,
    percent: int,
    stage: Optional[str] = None,
    job_id: Optional[str] = None,
) -> None:
    """Set the overall percentage directly."""
    mapping = {"percent": percent, "updated_at": time.time()}
    if stage:
        mapping["stage"] = stage
    _generation_hset(video_id, job_id, mapping)


def _recompute_overall(
    video_id: str,
    job_id: Optional[str] = None,
) -> None:
    """Recompute the overall percentage from individual tasks."""
    r = _redis()
    key = f"video:{video_id}:progress"
    data = r.hgetall(key)
    if not data:
        return
    total = int(data.get(b"total_tasks", b"1"))
    completed = 0
    total_percent = 0
    for k, v in data.items():
        k = k.decode() if isinstance(k, bytes) else k
        if k.startswith("task:"):
            task = json.loads(v)
            if task.get("status") == "completed":
                completed += 1
                total_percent += 100
            else:
                total_percent += task.get("percent", 0)
    overall = int(total_percent / total) if total > 0 else 0
    _generation_hset(video_id, job_id, {
        "percent": overall,
        "completed_tasks": completed,
        "updated_at": time.time(),
    })


def get_progress(video_id: str) -> dict:
    """Read the full progress state for a video."""
    r = _redis()
    data = r.hgetall(f"video:{video_id}:progress")
    if not data:
        return {"percent": 0, "stage": "pending", "total_tasks": 0, "completed_tasks": 0, "tasks": {}}
    result = {}
    tasks = {}
    for k, v in data.items():
        k = k.decode() if isinstance(k, bytes) else k
        v = v.decode() if isinstance(v, bytes) else v
        if k.startswith("task:"):
            name = k[5:]
            tasks[name] = json.loads(v)
        else:
            result[k] = v
    result["tasks"] = tasks
    result["percent"] = int(result.get("percent", 0))
    result["total_tasks"] = int(result.get("total_tasks", 0))
    result["completed_tasks"] = int(result.get("completed_tasks", 0))
    return result


# ---- Default transcoding settings ----

DEFAULT_SETTINGS = {
    "qualities": [1080, 720, 480],
    "audio_bitrate_kbps": 128,
    "audio_channels": 2,
    "segment_duration_sec": 6,
    "video_preset": "medium",
    "cpu_video_preset": "veryfast",
    "cpu_fallback_threads_per_task": 2,
    "force_cpu": False,
    "segment_format": "fmp4",
    "codec": "h264",
    # Single-pass remains the safe default until chunk retries are fenced from
    # overwriting outputs belonging to another pipeline attempt.
    "chunked_encoding": False,
    "chunk_duration_sec": 60,
    "chunk_min_duration_sec": 600,
    "cpu_fallback_chunk_duration_sec": 120,
    "per_title_encoding": True,
    "loudnorm": True,
    "trickplay": True,
}

QUALITY_LADDER = {
    2160: {"bitrate": 14000000, "label": "4K (2160p)"},
    1080: {"bitrate": 6000000, "label": "Full HD (1080p)"},
    720: {"bitrate": 3000000, "label": "HD (720p)"},
    480: {"bitrate": 1500000, "label": "SD (480p)"},
    360: {"bitrate": 800000, "label": "Low (360p)"},
    240: {"bitrate": 400000, "label": "Very Low (240p)"},
    144: {"bitrate": 200000, "label": "Audio-only (144p)"},
}


def get_default_settings() -> dict:
    """Read default transcoding settings.

    Built-in ``DEFAULT_SETTINGS`` are overlaid with values from
    ``app.config.get_settings()`` so environment flips take effect, then any
    Redis-stored overrides are applied on top (Redis wins, preserving the old
    override semantics). ``get_settings`` is imported lazily here to avoid any
    import cycle between ``progress`` and ``config``.
    """
    settings = DEFAULT_SETTINGS.copy()
    try:
        from app.config import get_settings as _get_settings
        cfg = _get_settings()
        _CONFIG_KEYS = (
            ("segment_format", "HLS_SEGMENT_FORMAT", "fmp4"),
            ("codec", "VIDEO_CODEC", "h264"),
            ("chunked_encoding", "CHUNKED_ENCODING", False),
            ("chunk_duration_sec", "CHUNK_DURATION_SEC", 60),
            ("chunk_min_duration_sec", "CHUNK_MIN_DURATION_SEC", 600),
            (
                "cpu_fallback_chunk_duration_sec",
                "CPU_FALLBACK_CHUNK_DURATION_SEC",
                120,
            ),
            ("cpu_video_preset", "CPU_VIDEO_PRESET", "veryfast"),
            (
                "cpu_fallback_threads_per_task",
                "CPU_FALLBACK_THREADS_PER_TASK",
                2,
            ),
            ("per_title_encoding", "PER_TITLE_ENCODING", True),
            ("loudnorm", "LOUDNORM", True),
            ("trickplay", "TRICKPLAY", True),
        )
        for key, attr, default in _CONFIG_KEYS:
            settings[key] = getattr(cfg, attr, default)
    except Exception as exc:
        logger.warning("could not read config settings for defaults: %s", exc)

    r = _redis()
    raw = r.get("transcoding:defaults")
    if raw:
        try:
            stored = json.loads(raw)
            if isinstance(stored, dict):
                settings.update(stored)
        except Exception:
            pass
    return settings


def set_default_settings(settings: dict) -> None:
    """Write default transcoding settings to Redis."""
    r = _redis()
    r.set("transcoding:defaults", json.dumps(settings))
