# HLS-ENGINE

HLS-ENGINE is a self-hosted video ingestion, transcoding, packaging, and
playback stack. Upload a source video and the engine produces adaptive HLS
renditions, preserves multiple audio and subtitle tracks, stores the result in
S3-compatible object storage, and serves it through a caching edge.

The project runs as a Docker Compose stack and supports CPU transcoding by
default, with an optional NVIDIA NVENC worker.

> [!IMPORTANT]
> HLS-ENGINE is under active development. It is designed with production
> concerns in mind, but operators are responsible for security review,
> capacity planning, backups, monitoring, and recovery testing before using it
> for critical workloads.

## Features

- Managed, resumable multipart uploads for large source files
- Adaptive HLS output with configurable, aspect-preserving 360p, 480p, 720p,
  and 1080p bounding boxes
- Multiple audio tracks and WebVTT subtitle tracks
- CPU transcoding with optional NVIDIA NVENC acceleration
- Retry-aware Celery workers with bounded FFmpeg shutdown and stall detection
- Fail-closed publishing: media is validated and the master playlist is
  published last
- Signed playback URLs, byte-range requests, CORS, and Nginx edge caching
- Dashboard and HLS.js-based player with quality, audio, and subtitle controls
- PostgreSQL metadata, Redis state, RabbitMQ jobs, and MinIO object storage
- Prometheus metrics and provisioned Grafana dashboards
- Exact-prefix media and exact-video edge-cache cleanup when a video is deleted

## How it works

```text
Browser
  |
  +--> Nginx edge --> Next.js dashboard
  |               \-> Go API gateway --> auth / upload / metadata / analytics
  |
  \--> signed HLS request --> CDN origin --> MinIO

Upload --> raw MinIO object --> RabbitMQ event --> Celery pipeline
       --> FFmpeg renditions/audio/subtitles --> validated HLS package --> MinIO
```

HLS objects are stored beneath prefixes sharded by the first two characters of
the video UUID. The transcoder withholds `master.m3u8` until all referenced
assets and media playlists have been uploaded successfully.

Deletion is a resumable saga. PostgreSQL first commits a `deleting` tombstone.
The metadata service then makes an authenticated internal request to the Nginx
cache purger. The purger durably revokes that exact canonical video UUID before
acknowledging the request. Every HLS request crosses the revocation guard
before an edge-cache lookup, so previously signed and previously cached URLs
fail closed immediately. The bounded background reconciler then parses the
first request-path UUID from each Nginx cache key and removes exact target
entries. It coalesces all revoked videos into each complete inventory and keeps
running periodically, so even a delayed origin response that fills the cache
after deletion is eventually reclaimed. A sibling video's path and query are
not touched.

Only after cache revocation succeeds does deletion remove and verify the exact
raw, HLS, and thumbnail prefixes, and then remove the row in a fresh
transaction. Physical cache convergence remains observable through the
internal purger status and health endpoints and continues independently of the
DELETE caller. Upload commits and deletes share a per-video database fence, so a
multipart completion cannot recreate raw media after cleanup. Revocation,
storage, and database failures leave the durable status as `deleting`, return
`Retry-After`, and are safe to retry. Transcoder work directories remain
worker-owned. Packaging removes completed work only after durable publish and
database commit. Failed, timed-out, orphaned, and superseded job directories
are reclaimed by a periodic Celery reaper which takes the same job-exclusive
lock as packaging and re-checks durable generation state before removing one
real, direct child of `WORK_DIR`. The metadata service never cross-deletes
worker paths.

## Requirements

- Docker Engine or Docker Desktop
- Docker Compose v2 (`docker compose`)
- Git
- Enough local storage for the source, transcoder workspace, and HLS output

NVIDIA acceleration additionally requires a supported NVIDIA driver and the
NVIDIA Container Toolkit (or an equivalent working Docker GPU runtime).

## Quick start

1. Clone the repository:

   ```bash
   git clone https://github.com/X-DIABLO-X/HLS-ENGINE.git
   cd HLS-ENGINE
   ```

2. Create your local environment file:

   ```bash
   cp .env.example .env
   ```

   On PowerShell, use `Copy-Item .env.example .env`.

3. Start the CPU-capable stack:

   ```bash
   docker compose up --build -d
   docker compose ps
   ```

4. Open the dashboard at <http://localhost>.

The copied `.env.example` exposes these local development interfaces:

| Interface | Address |
|---|---|
| Dashboard and API edge | <http://localhost> |
| Frontend directly | <http://localhost:3000> |
| RabbitMQ management | <http://localhost:15672> |
| MinIO console | <http://localhost:9001> |
| Prometheus | <http://localhost:9090> |
| Grafana | <http://localhost:3001> |

If you change a port in `.env`, use that value instead. Initial local
credentials are also documented in `.env.example`.

Follow service logs with:

```bash
docker compose logs -f
```

Stop the stack without deleting stored data:

```bash
docker compose down
```

`docker compose down -v` permanently removes the database, object storage,
queues, metrics, and transcoder workspace volumes.

## NVIDIA GPU mode

After confirming that Docker can access the host GPU, start the optional NVENC
worker:

```bash
docker compose --profile gpu up --build -d
docker compose --profile gpu ps
```

The GPU worker consumes the dedicated video queue. When no healthy GPU worker
is registered, video work is automatically split into one-rendition CPU tasks
covering at most `CPU_FALLBACK_CHUNK_DURATION_SEC` of source media (120 seconds
by default). Each software encoder is capped by
`CPU_FALLBACK_THREADS_PER_TASK` (two threads by default), so concurrent CPU
workers do not each claim every host core. If an acquired GPU or its lease is
lost mid-encode, the GPU task is replaced by the same bounded CPU plan; it
never continues a feature-length libx264 encode inside the original
late-acknowledged task. Bounded chunk/CPU recovery supports H.264 and HEVC.
AV1 remains on the single-pass GPU path because FFmpeg cannot reliably demux
concatenated AV1 MPEG-TS chunks as video; a missing/lost GPU therefore fails
AV1 closed before publishing partial output. Keep
`TRANSCODER_GPU_CONCURRENCY=1` unless the host and encoder session limits have
been measured for the intended rendition set.

### Acceleration controls

The GPU image builds FFmpeg 8.1.2 from the checksum-pinned official release
archive. Its image build verifies the version, `h264_nvenc`, and `scale_cuda`
before the worker is published. NVIDIA driver libraries still come from the
host through the container runtime.

Rendition dimensions are maximum bounding boxes, not dimensions that stretch
the source. Rungs above the source height are omitted, widths are capped, and
the scaler preserves the display aspect ratio inside these boxes:

| Nominal rung | Maximum box |
|---|---:|
| 1080p | 1920x1080 |
| 720p | 1280x720 |
| 480p | 854x480 |
| 360p | 640x360 |

For example, the benchmark's 1920x804 cinematic source produced 1280x536 and
854x358 video inside the nominal 720p and 480p boxes.

`NVENC_PROFILE` selects an explicit speed/quality tradeoff. Leaving it unset
preserves the existing audited command:

| Profile | NVENC settings | Intended use |
|---|---|---|
| `turbo` | p3, single pass, lookahead 0 | Lowest encoding latency |
| `balanced` | p4, quarter-resolution multipass, lookahead 12 | General speed/quality tradeoff |
| `quality` | p6, full-resolution multipass, lookahead 32, spatial AQ | Highest compression effort |

The production GPU path groups renditions into one FFmpeg process. Tests on
the RTX 4050 Laptop GPU found that separate rendition processes and temporal
chunks were slower than a grouped command: they time-share the same saturated
NVENC block while adding demux, CUDA-context, boundary, and stitching
overhead. A container or sandbox isolates a job but does not add another
physical encoder. Temporal chunks remain useful for bounded CPU recovery and
can improve speed when dispatched to genuinely separate GPUs or hosts.

Both passthrough optimizations are strict, opt-in, and fail safe:

- `VIDEO_PASSTHROUGH_ENABLED=true` permits a single source-resolution H.264
  remux only for progressive 8-bit `yuv420p`, square-pixel, unrotated sources
  with supported profile, level, dimensions, frame rate, duration, and
  bitrate metadata. It never creates a mixed copy/encode ladder. An ineligible
  source uses the normal encoded ladder; a remux or validation failure
  atomically replaces the copy attempt with that complete ladder.
- `AAC_PASSTHROUGH_ENABLED=true` copies only 48 kHz AAC-LC with the requested
  channel count when no loudness, resampling, delay, or trim operation is
  required. Every other track, and every failed copy attempt, follows the
  validated AAC encode path.

The Windows-native
[Intel QSV worker](services/transcoder/qsv_worker/README.md) is a proof of
concept only. It is not connected to Celery, storage, database state, or the
production Compose pipeline. Its measured 480p quality was substantially below
NVENC at the tested settings, so hybrid QSV/NVENC operation is not enabled in
production.

The isolated acceleration matrix, limitations, projected encode-only samples,
and reproduction commands are in [BENCHMARK.md](BENCHMARK.md).

## Configuration

Runtime configuration comes from `.env`; `.env.example` is the canonical
reference. At minimum, review:

- `AUTH_JWT_SECRET`, `SIGNED_URL_SECRET`, and service credentials
- `NGINX_HTTP_PORT` and public API/HLS base URLs
- `UPLOAD_MAX_FILE_SIZE_BYTES` and `UPLOAD_CHUNK_SIZE_BYTES`
- `DEFAULT_RENDITIONS`, codec, segment, and worker concurrency settings
- `CPU_FALLBACK_CHUNK_DURATION_SEC`, `CPU_FALLBACK_THREADS_PER_TASK`, and the
  valid `CPU_VIDEO_PRESET`
- `CELERY_TASK_*` and `RABBITMQ_CONSUMER_TIMEOUT_MS`
- MinIO buckets, endpoints, and access credentials

The checked-in values containing `change-me` are for isolated local
development only. Replace every placeholder before exposing the stack to a
network. The RabbitMQ consumer timeout must remain greater than the Celery hard
task limit plus its acknowledgement safety margin. Optional GPU chunking stays
disabled by default; bounded CPU recovery is independent and always engages
when no GPU is live. FFmpeg wall-clock limits must retain their configured
margin below the Celery soft limit.

See [SECURITY.md](SECURITY.md) for deployment guidance.

## Validation

With the stack running, exercise the complete upload-to-playback pipeline:

```bash
python scripts/e2e_pipeline.py \
  --base-url http://localhost \
  --media /path/to/video.mkv \
  --timeout-seconds 14400 \
  --delete-after-test \
  --report test-run.e2e.json
```

The runner validates authentication, multipart upload, processing state,
master and media playlists, track counts, segment range requests, and cleanup.
`--delete-after-test` removes the generated database record and stored media;
it can wait through the worker's two-hour hard-timeout window, proves the exact
`/tmp/hls-work/{job-id}` directory for every retry/watchdog generation absent
through the authenticated job API, then retries the resumable media deletion
and verifies a 404. The original source file is identified before the run and
is never deleted or modified.
Use `--cleanup-timeout-seconds` to override the 9,000-second safety bound.

Contributor test commands are listed in
[CONTRIBUTING.md](CONTRIBUTING.md).
Measured feature-length performance, bottleneck analysis, and cleanup results
are documented in [BENCHMARK.md](BENCHMARK.md).

## Services

| Service | Implementation | Responsibility |
|---|---|---|
| `frontend` | Next.js, TypeScript | Dashboard, upload UI, and HLS player |
| `gateway` | Go | Authentication middleware and API routing |
| `auth` | Go | User, JWT, refresh-token, and API-key flows |
| `upload` | Go | Fenced multipart uploads and completion events |
| `metadata` | Go | Video metadata, tracks, renditions, progress, and deletion |
| `cdn-origin` | Go | Signed HLS access and MinIO origin reads |
| `nginx-cache-purger` | Go | Internal authenticated cache revocation and exact-video purge |
| `analytics` | Go | Playback event ingestion and aggregation |
| `transcoder-api` | Python, FastAPI | Job status, retries, and metrics |
| `transcoder-event-worker` | Python | Upload-event ingestion and job dispatch |
| `transcoder-cpu-worker` | Celery, FFmpeg | CPU video, audio, subtitles, thumbnails, and packaging |
| `transcoder-gpu-worker` | Celery, FFmpeg/NVENC | Optional GPU video transcoding (`gpu` profile) |
| `postgres`, `redis`, `rabbitmq`, `minio` | Infrastructure | Durable metadata, state, queues, and media |
| `nginx` | Nginx | Public edge, guarded HLS cache, CORS, and range handling |
| `prometheus`, `grafana` | Observability | Metrics collection and dashboards |

## Repository layout

```text
HLS-ENGINE/
|-- docker-compose.yml
|-- docker-compose.prod.yml
|-- .env.example
|-- frontend/                 # Next.js dashboard and player
|-- infra/                    # Nginx, PostgreSQL, RabbitMQ, metrics
|-- scripts/                  # End-to-end and fault-recovery checks
|-- services/
|   |-- analytics/            # Go playback analytics
|   |-- auth/                 # Go authentication service
|   |-- cdn-origin/           # Go HLS origin
|   |-- gateway/              # Go API gateway
|   |-- internal/             # Shared Go packages
|   |-- metadata/             # Go metadata service
|   |-- transcoder/           # Python/Celery/FFmpeg pipeline
|   `-- upload/               # Go upload service
`-- shared/                   # Shared event and type definitions
```

## Contributing

Bug reports and focused pull requests are welcome. Read
[CONTRIBUTING.md](CONTRIBUTING.md) before making a change. Please report
security issues privately as described in [SECURITY.md](SECURITY.md).

## License

HLS-ENGINE source code is available under the [MIT License](LICENSE).
Third-party software, codecs, container images, and media retain their own
licenses and terms. In particular, the accelerated GPU image contains a
GPL-enabled FFmpeg build; distributors should follow the
[transcoder image licensing and source guidance](services/transcoder/THIRD_PARTY_NOTICES.md).
