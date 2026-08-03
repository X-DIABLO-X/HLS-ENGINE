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

## Results

| Milestone or stage | Baseline | Hardened pipeline | Change |
|---|---:|---:|---:|
| Upload | 32.703 s | 74.312 s | Local storage variance; slower |
| Source preparation | 53.652 s | 101.248 s | Local storage variance; slower |
| NVENC video encode | 2,177.144 s | 2,209.154 s | 1.47% slower |
| Validated HLS publish | 672.280 s | 145.193 s | 78.40% faster |
| Ready for playback | 2,940.672 s | 2,530.141 s | 410.531 s faster |
| Full test including deletion | Not measured | 2,544.672 s | Passed |

The hardened run became playable in **42m 10.141s** and completed all cleanup
checks in **42m 24.672s**. Compared with the usable baseline ready time of
49m 00.672s, readiness improved by **6m 50.531s (13.96%)**.

The largest improvement is publishing. The baseline serialized uploads and
made a complete duplicate copy. The hardened publisher uploads its staged
assets concurrently to their final keys, verifies the exact inventory, and
commits the master playlist last. Publishing fell from 11m 12.280s to
2m 25.193s, a 4.63x speedup.

The baseline reached a valid ready state, but its runner's later cleanup check
was invalidated by manual cache intervention during diagnosis. The hardened
result is the unattended, fully passing comparison run.

## Bottleneck

Video encoding is the remaining critical-path bottleneck. It consumed about
87.3% of time-to-ready. During the encode interval, 1,100 GPU samples showed:

| Metric | Median | 95th percentile | Maximum |
|---|---:|---:|---:|
| NVENC encoder utilization | 99% | 100% | 100% |
| General GPU utilization | 8% | 10% | 37% |
| Decoder utilization | 17% | 23% | 57% |
| GPU memory | 763 MiB | 763 MiB | 763 MiB |
| Temperature | 71 C | 72 C | 72 C |
| Power | 13.84 W | 14.18 W | 15.25 W |

The encoder was saturated and temperature remained stable; the observer did
not directly sample NVIDIA throttle-reason flags. Faster single-job completion
therefore requires a faster encoder, a deliberately tested
multi-session/rendition strategy, or different quality/codec settings.
Increasing ordinary GPU compute allocation is unlikely to help this workload.

Source download and publishing were the next material costs. Their throughput
varied with Docker Desktop's local virtual disk. The transcoder workspace
peaked near 7.2 GB. Host free space fell from 9.26 GiB to a 4.29 GiB minimum,
a transient 4.97 GiB drop. That drop was 54.98% smaller than the baseline
run's 11.04 GiB transient drop, although the runs began with different free
space. The Docker data filesystem retained more than 900 GiB free. Operators
should leave substantially more host headroom than this test machine had.

## Cleanup and fail-closed checks

The final run passed all of these checks:

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

The engine now uses bounded task timeouts, late acknowledgements, process-tree
termination, generation fencing, exact-prefix deletion, durable cache
revocation, resumable cleanup, stale-workspace recovery, and fail-closed
publishing. These controls make retries and common crash/failure paths safe;
they do not replace deployment-specific capacity planning, backups, monitoring,
or failure testing.

## Reproduce

With the stack running, use the maintained end-to-end runner:

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
