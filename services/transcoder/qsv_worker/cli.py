"""Command-line entry point for the isolated Windows QSV adapter."""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

from .adapter import (
    EncodeJob,
    JobConfigurationError,
    QsvAdapter,
    QsvWorkerError,
    RenditionSettings,
    SourceRange,
    parse_fraction,
    resolve_executable,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m qsv_worker",
        description=(
            "Standalone Intel Quick Sync H.264/HLS proof-of-concept worker"
        ),
    )
    parser.add_argument(
        "--ffmpeg",
        default="ffmpeg",
        help="FFmpeg executable or absolute path",
    )
    parser.add_argument(
        "--ffprobe",
        default="ffprobe",
        help="ffprobe executable or absolute path",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    probe = subparsers.add_parser(
        "probe",
        help="verify that a real Intel QSV H.264 encode can start",
    )
    probe.add_argument(
        "--timeout-seconds",
        type=float,
        default=20.0,
    )

    encode = subparsers.add_parser(
        "encode",
        help="encode one aligned source range to an isolated HLS directory",
    )
    encode.add_argument("--source", type=Path, required=True)
    encode.add_argument("--output-dir", type=Path, required=True)
    encode.add_argument("--start-seconds", type=float, required=True)
    encode.add_argument("--duration-seconds", type=float, required=True)
    encode.add_argument("--rendition-id", default="qsv")
    encode.add_argument("--width", type=int, required=True)
    encode.add_argument("--height", type=int, required=True)
    encode.add_argument("--bitrate-kbps", type=int, required=True)
    encode.add_argument("--maxrate-kbps", type=int)
    encode.add_argument("--bufsize-kbps", type=int)
    encode.add_argument(
        "--preset",
        default="veryfast",
        choices=(
            "veryfast",
            "faster",
            "fast",
            "medium",
            "slow",
            "slower",
            "veryslow",
        ),
    )
    encode.add_argument(
        "--low-power",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    encode.add_argument(
        "--profile",
        choices=("baseline", "main", "high"),
        default="high",
    )
    encode.add_argument("--b-frames", type=int, default=2)
    encode.add_argument(
        "--segment-seconds",
        type=float,
        default=6.0,
    )
    encode.add_argument(
        "--fps",
        help=(
            "optional rational frame rate such as 24 or 24000/1001; "
            "defaults to ffprobe"
        ),
    )
    encode.add_argument("--timeout-seconds", type=float)
    return parser


def _json_dump(payload: dict, handle) -> None:
    json.dump(payload, handle, indent=2, sort_keys=True)
    handle.write("\n")


def _adapter(args) -> QsvAdapter:
    return QsvAdapter(
        ffmpeg=resolve_executable(args.ffmpeg),
        ffprobe=resolve_executable(args.ffprobe),
    )


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        adapter = _adapter(args)
        if args.command == "probe":
            if (
                not math.isfinite(args.timeout_seconds)
                or args.timeout_seconds <= 0
            ):
                raise JobConfigurationError(
                    "timeout_seconds must be finite and greater than 0"
                )
            payload = {
                "schema_version": 1,
                "status": "available",
                **adapter.probe_qsv(args.timeout_seconds),
            }
            _json_dump(payload, sys.stdout)
            return 0

        bitrate = args.bitrate_kbps
        maxrate = args.maxrate_kbps
        if maxrate is None:
            maxrate = math.ceil(bitrate * 1.07)
        bufsize = args.bufsize_kbps
        if bufsize is None:
            bufsize = bitrate * 2
        fps = parse_fraction(args.fps) if args.fps else None
        rendition = RenditionSettings(
            rendition_id=args.rendition_id,
            width=args.width,
            height=args.height,
            bitrate_kbps=bitrate,
            maxrate_kbps=maxrate,
            bufsize_kbps=bufsize,
            preset=args.preset,
            low_power=args.low_power,
            profile=args.profile,
            b_frames=args.b_frames,
            segment_seconds=args.segment_seconds,
            fps=fps,
        )
        job = EncodeJob(
            source=args.source,
            output_dir=args.output_dir,
            source_range=SourceRange(
                start_seconds=args.start_seconds,
                duration_seconds=args.duration_seconds,
            ),
            rendition=rendition,
        )
        if args.timeout_seconds is not None and (
            not math.isfinite(args.timeout_seconds)
            or args.timeout_seconds <= 0
        ):
            raise JobConfigurationError(
                "timeout_seconds must be finite and greater than 0"
            )
        manifest = adapter.run(job, timeout=args.timeout_seconds)
        _json_dump(manifest, sys.stdout)
        return 0
    except QsvWorkerError as exc:
        _json_dump(exc.as_dict(), sys.stderr)
        return exc.exit_code
    except KeyboardInterrupt:
        payload = {
            "schema_version": 1,
            "status": "failed",
            "error": {
                "code": "interrupted",
                "message": "QSV job interrupted; child process was terminated",
            },
        }
        _json_dump(payload, sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
