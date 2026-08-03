"""Capacity invariants shared by pipeline dispatch and task replacement."""

import math
from typing import Any, Callable


CPU_FALLBACK_MIN_SLICE_SEC = 30.0
CPU_FALLBACK_MAX_SLICE_SEC = 300.0
CPU_FALLBACK_DEFAULT_SLICE_SEC = 120.0
CPU_FALLBACK_MAX_RENDITIONS_PER_TASK = 1
CPU_FALLBACK_MAX_TASKS = 4096
BOUNDED_CHUNK_CODECS = frozenset({"h264", "hevc"})


def validate_bounded_chunk_codecs(
    renditions: list,
    default_codec: str,
) -> None:
    """Fail before dispatch when TS chunk concat cannot preserve the codec."""
    codecs = set()
    for rendition in renditions or []:
        codec = (
            rendition.get("codec", default_codec)
            if isinstance(rendition, dict)
            else default_codec
        )
        codecs.add(str(codec or "h264").lower())
    unsupported = codecs - BOUNDED_CHUNK_CODECS
    if unsupported:
        raise ValueError(
            "bounded chunk encoding supports only h264/hevc; unsupported "
            f"codec(s): {', '.join(sorted(unsupported))}"
        )


def build_chunks(duration: float, chunk_duration: float) -> list:
    """Cover ``[0, duration)`` with positive, non-overlapping slices."""
    if duration <= 0 or chunk_duration <= 0:
        return []
    chunks = []
    start = 0.0
    while start < duration:
        chunks.append((start, min(chunk_duration, duration - start)))
        start += chunk_duration
    return chunks


def bounded_cpu_plan(
    duration: float,
    renditions: list,
    requested_slice: Any,
    *,
    rendition_height: Callable[[Any], int],
) -> tuple[list, list]:
    """Build a bounded, one-rendition CPU task plan.

    Rejecting invalid duration/cardinality is safer than silently reverting to
    a feature-length task or flooding the broker with an unbounded canvas.
    """
    try:
        media_duration = float(duration)
    except (TypeError, ValueError):
        media_duration = 0.0
    if not math.isfinite(media_duration) or media_duration <= 0:
        raise ValueError(
            "bounded CPU fallback requires a positive finite probed duration"
        )

    try:
        slice_duration = float(requested_slice)
    except (TypeError, ValueError):
        slice_duration = CPU_FALLBACK_DEFAULT_SLICE_SEC
    slice_duration = min(
        CPU_FALLBACK_MAX_SLICE_SEC,
        max(CPU_FALLBACK_MIN_SLICE_SEC, slice_duration),
    )

    groups = [
        [rendition]
        for rendition in sorted(
            renditions or [],
            key=rendition_height,
            reverse=True,
        )
    ]
    if not groups:
        raise ValueError("bounded CPU fallback received no renditions")
    if any(
        len(group) > CPU_FALLBACK_MAX_RENDITIONS_PER_TASK
        for group in groups
    ):
        raise ValueError("CPU fallback group exceeds its rendition capacity")

    chunks = build_chunks(media_duration, slice_duration)
    task_count = len(groups) * len(chunks)
    if task_count > CPU_FALLBACK_MAX_TASKS:
        raise ValueError(
            "bounded CPU fallback would create "
            f"{task_count} tasks (limit {CPU_FALLBACK_MAX_TASKS})"
        )
    return groups, chunks
