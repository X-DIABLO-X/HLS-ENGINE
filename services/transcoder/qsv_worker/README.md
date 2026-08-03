# Intel QSV proof-of-concept worker

This package is an isolated Windows-native experiment. It is not imported by
the API or Celery workers and does not change the production FFmpeg pipeline.

It performs the following operations:

- verifies QSV by encoding one generated frame, rather than trusting the
  presence of `h264_qsv` in `ffmpeg -encoders`;
- seeks to a six-second-aligned source range;
- uses QSV hardware decode, `vpp_qsv` scaling, and H.264 QSV encoding;
- treats `--width` and `--height` as a bounding box, preserves the source
  aspect ratio, and rounds the fitted output to even NV12 dimensions;
- forces IDR/keyframes on the six-second HLS grid;
- writes MPEG-TS HLS segments to a unique staging directory;
- validates the playlist, segment presence, durations, codec, and dimensions;
- probes every segment's actual video packets to verify that its first packet
  is a keyframe and that PTS/DTS timelines are finite, monotonic, and continuous
  across segment boundaries;
- records the source file identity before work and refuses publication if its
  file ID, size, or modification/change timestamps differ afterward;
- publishes a new output directory only after validation;
- emits a JSON manifest containing timings, settings, checksums, and the
  reproducible FFmpeg command;
- refuses to overwrite an existing output directory and removes partial output
  after a failed attempt;
- holds an operating-system file lock for the attempt, rejects live owners, and
  safely reuses a lock left by a provably dead process without an unlink/create
  race.

## Run on Windows

From `services/transcoder`:

```powershell
python -m qsv_worker probe
```

Example two-minute 480p range:

```powershell
python -m qsv_worker encode `
  --source "D:\media\source.mkv" `
  --output-dir "D:\qsv-poc\movie-480p-chunk-0000" `
  --start-seconds 0 `
  --duration-seconds 120 `
  --rendition-id 480p `
  --width 854 `
  --height 480 `
  --bitrate-kbps 1000 `
  --maxrate-kbps 1070 `
  --bufsize-kbps 2000
```

The output directory must not already exist. Successful output contains
`index.m3u8`, `segment_*.ts`, FFmpeg logs, and `manifest.json`.

The dimensions are maximum bounds, not a request to stretch the picture. For
an example 1920x804 source, the `854x480` box produces `854x358`
square-pixel video. The fitted dimensions and the original bounding box are
both recorded in `manifest.json`.

Lock recovery is deliberately fail-closed. A lock is recovered only when its
recorded local PID is no longer running (or its process-start marker proves
that the PID was recycled). A live, malformed, inaccessible, or foreign-host
lock is left untouched and the new attempt exits with code `2`.

## Exit codes

- `0`: success
- `2`: invalid job or unsafe output target
- `3`: FFmpeg/ffprobe missing or QSV unavailable
- `4`: encode, timeout, or validation failure
- `130`: interrupted

Failure details are emitted as JSON on standard error.

## Intentional limitations

- This is a local filesystem adapter, not a network service.
- It does not claim jobs, refresh leases, or perform generation fencing.
- It does not upload artifacts to object storage.
- It does not update PostgreSQL or publish RabbitMQ events.
- It does not combine the QSV output with the NVENC rendition.
- It does not replace the current video encoder selection logic.
- It validates structure and packet continuity, but quality acceptance still
  requires the planned VMAF/size comparison against the existing profile.

Those concerns must be added in the orchestration layer before this adapter can
participate in the live pipeline.
