"""Redis-based progress tracking for video processing."""

import json
import logging
import time
from typing import Optional

import redis

from app.config import get_settings

logger = logging.getLogger(__name__)

CHUNK_TOTALS_SETTING = "_progress_task_chunks"

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
redis.call('EXPIRE', KEYS[1], 86400)
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

_REPLACE_TASK_SCRIPT = """
-- HLS_REPLACE_TASK_V4
local current = redis.call('HGET', KEYS[1], 'job_id')
if current and current ~= '' then
  if ARGV[1] == '' or current ~= ARGV[1] then
    return 0
  end
elseif ARGV[1] ~= '' then
  return 0
end
local old_field = 'task:' .. ARGV[2]
local replacement_field = '_replacement:' .. ARGV[2]
local prior_replacement = redis.call('HGET', KEYS[1], replacement_field)
if prior_replacement then
  if prior_replacement == ARGV[4] then
    return 2
  end
  return -3
end
local old_raw = redis.call('HGET', KEYS[1], old_field)
if not old_raw then
  return -1
end
local old_ok, old_task = pcall(cjson.decode, old_raw)
if not old_ok or type(old_task) ~= 'table' then
  return -1
end
if old_task['status'] == 'completed' then
  return -2
end
local replacement_keeps_old_name = false
for i = 5, #ARGV - 1 do
  if 'task:' .. ARGV[i] == old_field then
    replacement_keeps_old_name = true
  end
end
local removed = 0
if not replacement_keeps_old_name then
  removed = redis.call('HDEL', KEYS[1], old_field)
end
local added = 0
for i = 5, #ARGV - 1 do
  local field = 'task:' .. ARGV[i]
  if redis.call('HEXISTS', KEYS[1], field) == 0 then
    added = added + 1
    redis.call('HSET', KEYS[1], field, ARGV[3])
  end
end
local total = tonumber(redis.call('HGET', KEYS[1], 'total_tasks') or '0')
redis.call('HSET', KEYS[1], 'total_tasks', math.max(0, total - removed + added))
redis.call('HSET', KEYS[1], replacement_field, ARGV[4])
redis.call('HSET', KEYS[1], 'updated_at', ARGV[#ARGV])
redis.call('EXPIRE', KEYS[1], 86400)
return 1
"""

_TASK_UPDATE_SCRIPT = """
-- HLS_TASK_UPDATE_V2
local current = redis.call('HGET', KEYS[1], 'job_id')
if current and current ~= '' then
  if ARGV[1] == '' or current ~= ARGV[1] then
    return 0
  end
elseif ARGV[1] ~= '' then
  return 0
end
local field = 'task:' .. ARGV[2]
local raw = redis.call('HGET', KEYS[1], field)
if not raw then
  return -1
end
local ok, existing = pcall(cjson.decode, raw)
if not ok or type(existing) ~= 'table' then
  return -1
end
if existing['status'] == 'completed' and ARGV[3] ~= 'completed' then
  return 2
end
local requested = math.max(0, math.min(100, tonumber(ARGV[4]) or 0))
if ARGV[3] == 'running' then
  requested = math.min(99, requested)
end
local prior = math.max(0, math.min(100, tonumber(existing['percent']) or 0))
local payload = {
  status = ARGV[3],
  percent = math.max(prior, requested)
}
if existing['chunks'] ~= nil then
  payload['chunks'] = existing['chunks']
end
if ARGV[3] == 'failed' and ARGV[6] ~= '' then
  payload['error'] = ARGV[6]
end
redis.call('HSET', KEYS[1], field, cjson.encode(payload))
if ARGV[5] ~= '' then
  redis.call('HSET', KEYS[1], 'stage', ARGV[5])
end
redis.call('HSET', KEYS[1], 'updated_at', ARGV[7])
redis.call('EXPIRE', KEYS[1], 86400)
return 1
"""

_RECOMPUTE_OVERALL_SCRIPT = """
-- HLS_RECOMPUTE_OVERALL_V1
local current = redis.call('HGET', KEYS[1], 'job_id')
if current and current ~= '' then
  if ARGV[1] == '' or current ~= ARGV[1] then
    return 0
  end
elseif ARGV[1] ~= '' then
  return 0
end
local total = tonumber(redis.call('HGET', KEYS[1], 'total_tasks') or '0')
local completed = 0
local total_percent = 0
local values = redis.call('HGETALL', KEYS[1])
for i = 1, #values, 2 do
  if string.sub(values[i], 1, 5) == 'task:' then
    local ok, task = pcall(cjson.decode, values[i + 1])
    if ok and type(task) == 'table' then
      local task_percent = math.max(
        0,
        math.min(100, tonumber(task['percent']) or 0)
      )
      if task['status'] == 'completed' then
        completed = completed + 1
        task_percent = 100
      end
      total_percent = total_percent + task_percent
    end
  end
end
local computed = 0
if total > 0 then
  computed = math.floor(total_percent / total)
end
computed = math.max(0, math.min(100, computed))
local prior = tonumber(redis.call('HGET', KEYS[1], 'percent') or '0')
local overall = math.max(prior, computed)
redis.call('HSET', KEYS[1], 'percent', overall)
redis.call('HSET', KEYS[1], 'completed_tasks', completed)
redis.call('HSET', KEYS[1], 'updated_at', ARGV[2])
redis.call('EXPIRE', KEYS[1], 86400)
return overall
"""

_SET_PERCENT_SCRIPT = """
-- HLS_SET_PERCENT_V1
local current = redis.call('HGET', KEYS[1], 'job_id')
if current and current ~= '' then
  if ARGV[1] == '' or current ~= ARGV[1] then
    return 0
  end
elseif ARGV[1] ~= '' then
  return 0
end
local requested = math.max(0, math.min(100, tonumber(ARGV[2]) or 0))
local prior = tonumber(redis.call('HGET', KEYS[1], 'percent') or '0')
redis.call('HSET', KEYS[1], 'percent', math.max(prior, requested))
if ARGV[3] ~= '' then
  redis.call('HSET', KEYS[1], 'stage', ARGV[3])
end
redis.call('HSET', KEYS[1], 'updated_at', ARGV[4])
redis.call('EXPIRE', KEYS[1], 86400)
return 1
"""

_CONFIGURE_CHUNKED_TASK_SCRIPT = """
-- HLS_CONFIGURE_CHUNKED_TASK_V1
local current = redis.call('HGET', KEYS[1], 'job_id')
if current and current ~= '' then
  if ARGV[1] == '' or current ~= ARGV[1] then
    return 0
  end
elseif ARGV[1] ~= '' then
  return 0
end
local task_field = 'task:' .. ARGV[2]
local raw = redis.call('HGET', KEYS[1], task_field)
if not raw then
  return -2
end
local total = math.floor(tonumber(ARGV[3]) or 0)
if total < 1 or total > 10000 then
  return -3
end
local total_field = '_chunk:total:' .. ARGV[2]
local existing_total = tonumber(redis.call('HGET', KEYS[1], total_field) or '0')
if existing_total ~= 0 and existing_total ~= total then
  return -1
end
local ok, task = pcall(cjson.decode, raw)
if not ok or type(task) ~= 'table' then
  task = {}
end
if task['status'] == 'completed' then
  return 2
end
local base_field = '_chunk:base:' .. ARGV[2]
local sum_field = '_chunk:sum:' .. ARGV[2]
local completed_field = '_chunk:completed:' .. ARGV[2]
if existing_total == 0 then
  local base = math.max(
    0,
    math.min(99, tonumber(task['percent']) or 0)
  )
  redis.call('HSET', KEYS[1], total_field, total)
  redis.call('HSET', KEYS[1], base_field, base)
  redis.call('HSET', KEYS[1], sum_field, 0)
  redis.call('HSET', KEYS[1], completed_field, 0)
end
local completed = tonumber(redis.call('HGET', KEYS[1], completed_field) or '0')
local prior = math.max(0, math.min(99, tonumber(task['percent']) or 0))
local payload = {
  status = 'running',
  percent = prior,
  chunks = {completed = completed, total = total}
}
redis.call('HSET', KEYS[1], task_field, cjson.encode(payload))
if ARGV[4] ~= '' then
  redis.call('HSET', KEYS[1], 'stage', ARGV[4])
end
redis.call('HSET', KEYS[1], 'updated_at', ARGV[5])
redis.call('EXPIRE', KEYS[1], 86400)
return 1
"""

_UPDATE_CHUNKED_TASK_SCRIPT = """
-- HLS_UPDATE_CHUNKED_TASK_V1
local current = redis.call('HGET', KEYS[1], 'job_id')
if current and current ~= '' then
  if ARGV[1] == '' or current ~= ARGV[1] then
    return 0
  end
elseif ARGV[1] ~= '' then
  return 0
end
local task_field = 'task:' .. ARGV[2]
local raw = redis.call('HGET', KEYS[1], task_field)
if not raw then
  return -2
end
local total = math.floor(tonumber(ARGV[4]) or 0)
if total < 1 or total > 10000 then
  return -3
end
local chunk_index = math.floor(tonumber(ARGV[3]) or -1)
if chunk_index < 0 or chunk_index >= total then
  return -4
end
local total_field = '_chunk:total:' .. ARGV[2]
local configured_total = tonumber(redis.call('HGET', KEYS[1], total_field) or '0')
if configured_total ~= total then
  return -1
end
local ok, task = pcall(cjson.decode, raw)
if not ok or type(task) ~= 'table' then
  task = {}
end
if task['status'] == 'completed' then
  return 2
end
local value_field = '_chunk:value:' .. ARGV[2] .. ':' .. chunk_index
local old_value = tonumber(redis.call('HGET', KEYS[1], value_field) or '0')
local new_value = math.max(
  old_value,
  math.max(0, math.min(100, tonumber(ARGV[5]) or 0))
)
local sum_field = '_chunk:sum:' .. ARGV[2]
local completed_field = '_chunk:completed:' .. ARGV[2]
local progress_sum = tonumber(redis.call('HGET', KEYS[1], sum_field) or '0')
progress_sum = math.min(total * 100, progress_sum + new_value - old_value)
local completed = tonumber(redis.call('HGET', KEYS[1], completed_field) or '0')
if old_value < 100 and new_value >= 100 then
  completed = math.min(total, completed + 1)
end
redis.call('HSET', KEYS[1], value_field, new_value)
redis.call('HSET', KEYS[1], sum_field, progress_sum)
redis.call('HSET', KEYS[1], completed_field, completed)
local base = tonumber(
  redis.call('HGET', KEYS[1], '_chunk:base:' .. ARGV[2]) or '0'
)
local aggregate = base + math.floor(
  ((100 - base) * progress_sum) / (total * 100)
)
aggregate = math.min(99, aggregate)
aggregate = math.max(
  aggregate,
  math.max(0, math.min(99, tonumber(task['percent']) or 0))
)
local payload = {
  status = 'running',
  percent = aggregate,
  chunks = {completed = completed, total = total}
}
redis.call('HSET', KEYS[1], task_field, cjson.encode(payload))
if ARGV[6] ~= '' then
  redis.call('HSET', KEYS[1], 'stage', ARGV[6])
end
redis.call('HSET', KEYS[1], 'updated_at', ARGV[7])
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


def _update_task_state(
    video_id: str,
    task_name: str,
    status: str,
    percent: int,
    *,
    stage: Optional[str] = None,
    error: str = "",
    job_id: Optional[str] = None,
) -> int:
    """Atomically update one conceptual task without lowering its progress."""
    return int(
        _redis().eval(
            _TASK_UPDATE_SCRIPT,
            1,
            f"video:{video_id}:progress",
            str(job_id or ""),
            str(task_name),
            str(status),
            int(percent),
            str(stage or ""),
            str(error or ""),
            str(time.time()),
        )
    )


def start_task(
    video_id: str,
    task_name: str,
    stage: str,
    job_id: Optional[str] = None,
) -> None:
    """Mark a task as started."""
    _update_task_state(
        video_id,
        task_name,
        "running",
        0,
        stage=stage,
        job_id=job_id,
    )


def update_task(
    video_id: str,
    task_name: str,
    percent: int,
    stage: Optional[str] = None,
    job_id: Optional[str] = None,
) -> None:
    """Update a task's progress percentage."""
    if _update_task_state(
        video_id,
        task_name,
        "running",
        percent,
        stage=stage,
        job_id=job_id,
    ) == 1:
        _recompute_overall(video_id, job_id=job_id)


def complete_task(
    video_id: str,
    task_name: str,
    job_id: Optional[str] = None,
) -> None:
    """Mark a task as completed and recompute overall progress."""
    if _update_task_state(
        video_id,
        task_name,
        "completed",
        100,
        job_id=job_id,
    ) == 1:
        _recompute_overall(video_id, job_id=job_id)


def fail_task(
    video_id: str,
    task_name: str,
    error: str = "",
    job_id: Optional[str] = None,
) -> None:
    """Mark a task as failed."""
    if _update_task_state(
        video_id,
        task_name,
        "failed",
        0,
        stage="Failed",
        error=error,
        job_id=job_id,
    ) == 1:
        _recompute_overall(video_id, job_id=job_id)


def replace_task(
    video_id: str,
    old_task_name: str,
    new_task_names: list,
    job_id: Optional[str] = None,
) -> bool:
    """Atomically replace one live task with fallback task identities.

    Exact replays are idempotent. Missing, completed, malformed, stale, or
    conflicting source tasks fail closed and cannot create new progress fields.
    """
    names = list(dict.fromkeys(str(name) for name in new_task_names if name))
    if not names:
        return False
    pending = json.dumps({"status": "pending", "percent": 0})
    replacement_signature = json.dumps(names, separators=(",", ":"))
    r = _redis()
    result = int(
        r.eval(
            _REPLACE_TASK_SCRIPT,
            1,
            f"video:{video_id}:progress",
            str(job_id or ""),
            str(old_task_name),
            pending,
            replacement_signature,
            *names,
            str(time.time()),
        )
    )
    if result == 1:
        _recompute_overall(video_id, job_id=job_id)
        return True
    if result == 2:
        return True
    logger.warning(
        "rejected progress replacement video=%s job=%s task=%s result=%s",
        video_id,
        job_id,
        old_task_name,
        result,
    )
    return False


def with_chunked_tasks(
    settings: dict,
    task_names: list,
    total_chunks: int,
) -> dict:
    """Return JSON-safe task settings carrying the expected chunk cardinality."""
    total = int(total_chunks)
    if total < 1:
        raise ValueError("total_chunks must be positive")
    result = dict(settings or {})
    existing = result.get(CHUNK_TOTALS_SETTING) or {}
    totals = {
        str(name): int(value)
        for name, value in dict(existing).items()
    }
    for name in task_names:
        totals[str(name)] = total
    result[CHUNK_TOTALS_SETTING] = totals
    return result


def task_chunk_count(settings: dict, task_name: str) -> int:
    """Read one task's configured chunk count after Celery JSON round-trips."""
    raw = (settings or {}).get(CHUNK_TOTALS_SETTING) or {}
    try:
        total = int(dict(raw).get(str(task_name), 0))
    except (TypeError, ValueError):
        return 0
    return max(0, total)


def configure_chunked_task(
    video_id: str,
    task_name: str,
    total_chunks: int,
    stage: Optional[str] = None,
    job_id: Optional[str] = None,
) -> bool:
    """Register a retry-safe chunk aggregate for one rendition task."""
    result = int(
        _redis().eval(
            _CONFIGURE_CHUNKED_TASK_SCRIPT,
            1,
            f"video:{video_id}:progress",
            str(job_id or ""),
            str(task_name),
            int(total_chunks),
            str(stage or ""),
            str(time.time()),
        )
    )
    if result < 0:
        raise ValueError(
            f"invalid chunk progress configuration for {task_name}: "
            f"total={total_chunks}, result={result}"
        )
    return result > 0


def update_chunked_task(
    video_id: str,
    task_name: str,
    chunk_index: int,
    total_chunks: int,
    percent: int,
    stage: Optional[str] = None,
    job_id: Optional[str] = None,
) -> bool:
    """Advance one chunk and atomically fold it into rendition progress."""
    result = int(
        _redis().eval(
            _UPDATE_CHUNKED_TASK_SCRIPT,
            1,
            f"video:{video_id}:progress",
            str(job_id or ""),
            str(task_name),
            int(chunk_index),
            int(total_chunks),
            int(percent),
            str(stage or ""),
            str(time.time()),
        )
    )
    if result < 0:
        raise ValueError(
            f"invalid chunk progress update for {task_name}: "
            f"chunk={chunk_index}/{total_chunks}, result={result}"
        )
    if result == 1:
        _recompute_overall(video_id, job_id=job_id)
    return result > 0


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
    _redis().eval(
        _SET_PERCENT_SCRIPT,
        1,
        f"video:{video_id}:progress",
        str(job_id or ""),
        int(percent),
        str(stage or ""),
        str(time.time()),
    )


def _recompute_overall(
    video_id: str,
    job_id: Optional[str] = None,
) -> None:
    """Atomically recompute overall progress without allowing regression."""
    _redis().eval(
        _RECOMPUTE_OVERALL_SCRIPT,
        1,
        f"video:{video_id}:progress",
        str(job_id or ""),
        str(time.time()),
    )


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
        elif k.startswith(("_chunk:", "_replacement:")):
            continue
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
    "nvenc_profile": None,
    "video_passthrough_enabled": False,
    # Single-pass remains the safe default until chunk retries are fenced from
    # overwriting outputs belonging to another pipeline attempt.
    "chunked_encoding": False,
    "chunk_duration_sec": 60,
    "chunk_min_duration_sec": 600,
    "cpu_fallback_chunk_duration_sec": 120,
    "per_title_encoding": True,
    "loudnorm": True,
    "aac_passthrough_enabled": False,
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
            ("nvenc_profile", "NVENC_PROFILE", None),
            (
                "video_passthrough_enabled",
                "VIDEO_PASSTHROUGH_ENABLED",
                False,
            ),
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
            (
                "aac_passthrough_enabled",
                "AAC_PASSTHROUGH_ENABLED",
                False,
            ),
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
