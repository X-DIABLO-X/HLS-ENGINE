# Performance and reliability benchmark

This benchmark exercises the complete public upload-to-delete path with a
feature-length source. It is intended as a reproducible reference, not a
guarantee for other hardware or media.

## Test profile

Run date: 2026-08-03

| Item | Value |
|---|---|
| Host | Windows 11, Docker Desktop |
| CPU | Intel Core i5-13500HX, 14 cores / 20 logical processors |
| Memory | 31.7 GiB |
| GPU | NVIDIA GeForce RTX 4050 Laptop GPU, 6 GiB |
| Source | 7,177.832 seconds, 1920x804, H.264 at 24 fps |
| Tracks | 2 AAC audio, 3 SubRip subtitle |
| Input size | 2,584,532,929 bytes |
| Output | 2 adaptive video variants, 2 audio tracks, 3 subtitle tracks |

The runner performed a resumable multipart upload, waited for transcoding and
publishing, checked every playlist, requested a byte range, verified signed
and unsigned playback behavior, deleted the media, and then verified storage,
cache, queue, database, Redis, lock, and workspace cleanup. Resource observers
sampled the host, Docker services, and NVIDIA GPU throughout the run.

## Current optimized encoded result

The final unattended encoded run used the FFmpeg 8.1.2 GPU image, the grouped
720p + 480p turbo profile, and the exact feature-length source described
above. It became playable in **14m 02.641s** and completed playback checks,
workspace cleanup, media deletion, and delete verification in **14m 05.891s**.

| Milestone or stage | Wall time |
|---|---:|
| Resumable upload transfer | 44.391 s |
| Source setup and probe | about 30.2 s |
| Two audio tracks, concurrent with video | 262.792 s / 372.419 s |
| Grouped 720p + 480p NVENC encode | 740.063 s |
| Validated HLS publish | 21.453 s |
| Ready for playback | 842.641 s |
| Full test including deletion | 845.891 s |

There were no task retries, worker or container restarts, progress
regressions, NVDEC/CUDA errors, recovery attempts, or CPU-video fallbacks.
All seven playlists, signed access, unsigned denial, byte ranges, source
preservation, queue drain, exact workspace cleanup, and media deletion passed.
Video encoding was 87.8% of time-to-ready and remains the critical path.

The previous unattended hardened result became playable in 42m 10.141s.
The optimized encoded run is therefore **28m 07.500s faster (66.70%)** on this
machine. A separate clean turbo run completed the video encode in 660.869s,
so sustained full-source video time has varied from about 11m 01s to 12m 20s.
The slower final result is used as the authoritative release figure.

## Historical publishing comparison

| Milestone or stage | Baseline | Hardened pipeline | Change |
|---|---:|---:|---:|
| Upload | 32.703 s | 74.312 s | Local storage variance; slower |
| Source preparation | 53.652 s | 101.248 s | Local storage variance; slower |
| NVENC video encode | 2,177.144 s | 2,209.154 s | 1.47% slower |
| Validated HLS publish | 672.280 s | 145.193 s | 78.40% faster |
| Ready for playback | 2,940.672 s | 2,530.141 s | 410.531 s faster |
| Full test including deletion | Not measured | 2,544.672 s | Passed |

This older comparison isolated the fail-closed publisher improvement.
Publishing fell from 11m 12.280s to 2m 25.193s because staged assets were
uploaded concurrently to final keys, exact inventory was verified, and the
master playlist was committed last. The optimized run above includes that
publisher plus the newer grouped encoder and turbo settings.

## Bottleneck

Video encoding is the remaining critical-path bottleneck. During the final
740.063s encode, 366 GPU samples showed:

| Metric | Mean | Median | 95th percentile | Maximum |
|---|---:|---:|---:|---:|
| NVENC encoder utilization | 27.16% | 27% | 37% | 51% |
| General GPU utilization | 10.33% | 9% | 18% | 43% |
| Decoder utilization | 30.11% | 30% | 42% | 68% |
| GPU memory | 271.96 MiB | 273 MiB | 273 MiB | 273 MiB |
| Temperature | 70.31 C | 70 C | 73 C | 76 C |
| Power | 15.68 W | 15.60 W | 23.07 W | 32.16 W |

The CPU audio jobs overlapped the first half of video encoding. While they
ran, the CPU worker averaged 263.95% CPU, encoder utilization averaged 25.98%,
and decoder utilization averaged 29.04%. After audio completed, CPU-worker
use fell to 1.60%, while encoder and decoder utilization rose to 28.37% and
31.20%. This indicates secondary CPU, storage, and laptop power contention,
but not enough idle NVENC capacity for same-GPU process fan-out to overcome
duplicate demux/decode and launch costs.

The earlier quality profile did saturate the NVENC block at 99-100%. The turbo
profile intentionally trades some encoder work for speed, so its utilization
figures are not contradictory. On this hardware, one grouped FFmpeg process
feeding both renditions was faster than separate rendition processes or
temporal chunks. Separate containers or sandboxes still share the same
physical encoder.

Source download and publishing remain sensitive to Docker Desktop's virtual
disk. Operators should leave several gigabytes of transient headroom and use
fast local or attached storage for worker scratch space.

## Isolated acceleration matrix

This section is a **sample-based optimization comparison**, distinct from the
feature-length end-to-end run above.

The committed
[sanitized benchmark summary](benchmark-results/2026-08-03-summary.json)
records the measurements and SHA-256 identities of the excluded raw local
reports. The matrix sampled two deterministic 120-second ranges beginning at
2,352 and 4,704 seconds. It used the same source and machine described above,
two renditions with six-second HLS segments, and at most two simultaneous
NVENC sessions. VMAF was measured against a resolution-matched source
reference.

The samples used Windows-host FFmpeg 8.1.1 as recorded in the reports. The
production GPU container builds checksum-pinned FFmpeg 8.1.2 and verifies
`h264_nvenc` and `scale_cuda` during the image build. The matrix compares
scheduling and encoder settings; the current result above is the separate
FFmpeg 8.1.2 container end-to-end validation.

| Candidate | Process and hardware shape | Encode wall time for 240 s sampled source | Source realtime | Mean VMAF, 720p / 480p | Linear full-source projection |
|---|---|---:|---:|---:|---:|
| Grouped quality | One FFmpeg, two NVENC outputs; p6/fullres/lookahead 32 | 18.386 s | 13.05x | 93.74 / 94.16 | 9m 09.9s |
| Grouped balanced | One FFmpeg, two NVENC outputs; p4/qres/lookahead 12 | 12.180 s | 19.70x | 93.58 / 94.11 | 6m 04.3s |
| Grouped turbo | One FFmpeg, two NVENC outputs; p3/single-pass/lookahead 0 | 9.894 s | 24.26x | 93.57 / 94.10 | 4m 55.9s |
| Separate renditions | Two parallel FFmpeg/NVENC processes; quality profile | 18.709 s | 12.83x | 93.74 / 94.16 | 9m 19.6s |
| Temporal chunks | Four chunks per clip, eight FFmpeg launches, two concurrent NVENC sessions; quality profile | 22.740 s | 10.55x | 93.77 / 94.19 | 11m 20.1s |
| Single 720p | One FFmpeg/NVENC output; quality profile | 12.606 s | 19.04x | 93.74 / n/a | 6m 17.0s |
| Single 480p | One FFmpeg/NVENC output; quality profile | 7.722 s | 31.08x | n/a / 94.16 | 3m 51.0s |
| H.264 stream copy | One source-resolution remux, no ABR encode | 0.495 s | 484.50x | Lossless by copy | 14.8s |
| Intel QSV 480p POC | One Windows-native QSV process; veryfast/low-power | 17.501 s | 13.71x | n/a / 62.83 | 8m 43.4s |
| Hybrid NVENC + QSV POC | Parallel NVENC 720p balanced and QSV 480p | 16.979 s | 14.14x | 93.58 / 62.83 | 8m 27.8s |

The final column is explicitly a **linear encode-only projection**:
7,177.832 source seconds divided by the measured sample realtime factor. It
does not include upload, source preparation, audio, subtitles, thumbnails,
quality analysis, object-storage publishing, playback validation, retries, or
deletion. It also cannot predict behavior across unsampled scenes or a long,
thermally sustained run. These values must not be presented as measured
full-pipeline completion times.

The grouped command was the best dual-rendition NVENC shape. Compared with
grouped quality, two separate same-GPU rendition processes were 1.76% slower,
and temporal chunking was 23.68% slower. Both approaches duplicate setup and
demux/decode work, and chunks add boundary and HLS assembly overhead. The full
run's 99-100% encoder utilization explains why extra processes could not create
more throughput: separate containers or sandboxes still time-share the same
physical NVENC block.

Turbo reduced sampled encode time by 46.19% versus quality, and balanced
reduced it by 33.75%, with only small VMAF differences on these two clips.
That quality result is media-specific; profile selection still requires
representative content and playback testing.

Direct H.264 stream copy was much faster because it did not encode. Production
use remains opt-in through `VIDEO_PASSTHROUGH_ENABLED`, with strict codec,
pixel-format, scan, aspect, rotation, profile/level, geometry, frame-rate,
duration, and bitrate checks. It publishes one source-resolution rendition,
never a mixed copy/encode ladder. Ineligibility selects the normal ladder, and
any copy or output-validation failure replaces the attempt with the complete
encoded ladder. `AAC_PASSTHROUGH_ENABLED` is likewise opt-in: only compatible
48 kHz AAC-LC with matching channels and no required loudness, resampling,
delay, or trim work is copied; all other cases use the AAC encoder.

The QSV adapter proved that a second hardware family could run, but it did not
improve this workload. Standalone QSV 480p was slower than NVENC 480p and its
mean VMAF was 62.83 versus 94.16. The hybrid was also slower than grouped
balanced NVENC while inheriting that low-quality 480p output. The adapter is a
local Windows proof of concept: it has no Celery lease/generation integration,
object-store publication, database updates, or event flow. It is not enabled
in the production pipeline.

## Cleanup and fail-closed checks

The final encoded run passed all of these checks:

- All seven HLS playlists were valid; every referenced asset existed.
- Signed playback and byte-range requests succeeded; unsigned access failed.
- The master playlist was exposed only after the staged package was complete.
- Deletion revoked cached playback before removing persistent records.
- A previously signed URL failed after deletion.
- Raw upload, HLS output, thumbnails, database rows, Redis upload state,
  multipart state, job locks, and the exact worker workspace all converged to
  zero.
- RabbitMQ queues ended with no ready or unacknowledged jobs.
- All services stayed healthy with no restart, redelivery, consumer timeout,
  or serious application-log event.
- The original source's byte size, SHA-256 digest, and modification time were
  unchanged.

The acceleration harness also removes each candidate's generated media before
starting the next candidate. All ten candidates reported successful cleanup.
The final run-owned directory was removed, while the source still existed with
the same 2,584,532,929-byte size and modification timestamp before and after
both matrix reports. The run-level cleanup recorded zero additional bytes
because candidate-level cleanup had already reclaimed the media. The harness
refuses to delete an owner root or any cleanup tree containing the source.

The engine now uses bounded task timeouts, late acknowledgements, process-tree
termination, generation fencing, exact-prefix deletion, durable cache
revocation, resumable cleanup, stale-workspace recovery, and fail-closed
publishing. These controls make retries and common crash/failure paths safe;
they do not replace deployment-specific capacity planning, backups, monitoring,
or failure testing.

## Reproduce

To reproduce the measured full end-to-end workflow with the stack running, use
the maintained runner:

```bash
python scripts/e2e_pipeline.py \
  --base-url http://localhost \
  --media /path/to/video.mkv \
  --timeout-seconds 14400 \
  --delete-after-test \
  --report test-run.e2e.json
```

The generated report and media are local test artifacts and are excluded from
version control. `--delete-after-test` removes generated media while leaving
the supplied source untouched.

On a Windows host with FFmpeg/NVENC available, reproduce the nine-candidate
primary matrix with:

```powershell
python scripts/benchmark_hls_acceleration.py `
  --source "D:\path\to\feature-length-source.mkv" `
  --report ".benchmarks\hls-acceleration-results-v2.json" `
  --work-root ".benchmarks\hls-acceleration-work" `
  --clip-seconds 120 `
  --clip-count 2 `
  --temporal-chunks 4 `
  --nvenc-max-sessions 2 `
  --candidate-order-seed 20260803 `
  --segment-format fmp4 `
  --candidate "grouped_nvenc_quality,grouped_nvenc_balanced,grouped_nvenc_turbo" `
  --candidate "single_nvenc_720p,single_nvenc_480p,separate_nvenc_renditions_parallel" `
  --candidate "temporal_nvenc_chunks_parallel,h264_stream_copy_direct_play,intel_qsv_480p" `
  --execute
```

Run the distinct-hardware experiment separately so its contention and quality
results remain identifiable:

```powershell
python scripts/benchmark_hls_acceleration.py `
  --source "D:\path\to\feature-length-source.mkv" `
  --report ".benchmarks\hls-acceleration-hybrid.json" `
  --work-root ".benchmarks\hls-acceleration-work" `
  --clip-seconds 120 `
  --clip-count 2 `
  --temporal-chunks 4 `
  --nvenc-max-sessions 2 `
  --candidate-order-seed 20260803 `
  --segment-format fmp4 `
  --candidate hybrid_nvenc_720p_qsv_480p_parallel `
  --execute
```

The benchmark defaults to a non-executing plan; `--execute` is the explicit
opt-in for media work. Candidate and run cleanup are fail-closed, and the input
source is never an owned cleanup target.

## Image licensing

HLS-ENGINE application source remains MIT-licensed. The GPU image's FFmpeg
8.1.2 build enables GPL components including x264 and x265, so the resulting
FFmpeg executables are GPL-2.0-or-later rather than MIT. The build does not
enable FFmpeg's `nonfree` option. Anyone distributing a built image must follow
the source, notice, and package-inventory guidance in
[the transcoder third-party notice](services/transcoder/THIRD_PARTY_NOTICES.md).
