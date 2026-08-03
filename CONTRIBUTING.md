# Contributing to HLS-ENGINE

Thank you for helping improve HLS-ENGINE. Focused bug fixes, tests,
documentation, and well-scoped features are welcome.

## Before you start

- Search existing issues and pull requests before opening a duplicate.
- Use an issue to discuss behavior changes or large features before investing
  in an implementation.
- Do not report vulnerabilities in a public issue. Follow
  [SECURITY.md](SECURITY.md) instead.
- Keep pull requests focused. Unrelated refactors make media-pipeline changes
  much harder to review safely.

## Development setup

The easiest way to run the complete system is Docker Compose:

```bash
cp .env.example .env
docker compose up --build -d
docker compose ps
```

On PowerShell, use `Copy-Item .env.example .env`.

CPU transcoding is available in the default stack. To develop against an
NVIDIA worker, install a working Docker GPU runtime and add
`--profile gpu` to the Compose commands.

Never commit `.env`, credentials, tokens, source media, generated HLS output,
or benchmark logs.

## Test your change

Run the checks for every area you touched.

### Go services

Each Go service is its own module. Run:

```bash
(cd services/internal && go test ./...)
(cd services/auth && go test ./...)
(cd services/upload && go test ./...)
(cd services/metadata && go test ./...)
(cd services/cdn-origin && go test ./...)
(cd services/gateway && go test ./...)
(cd services/analytics && go test ./...)
```

Format changed Go files with `gofmt` before committing.

### Transcoder

```bash
cd services/transcoder
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt pytest
python -m pytest -q
```

On PowerShell, activate the environment with
`.venv\Scripts\Activate.ps1`.

Transcoder changes should include a regression test for retries, duplicate
delivery, process termination, track identity, or publishing behavior when
that behavior is affected.

### Frontend

```bash
cd frontend
npm ci
npm run lint
npm run type-check
npm run build
```

### Compose configuration

Validate every supported topology:

```bash
docker compose config --quiet
docker compose --profile gpu config --quiet
docker compose -f docker-compose.yml -f docker-compose.prod.yml config --quiet
```

### End-to-end pipeline

With the stack running:

```bash
python scripts/e2e_pipeline.py \
  --base-url http://localhost \
  --media /path/to/test-video.mkv \
  --delete-after-test \
  --report test-run.e2e.json
```

Use media you are permitted to process. Keep `--delete-after-test` enabled
unless the generated objects are needed for a specific investigation.

## Pipeline invariants

Changes to the media path require extra care. Preserve these properties:

- A retried or duplicate job must not corrupt or append to a prior attempt.
- A job is not marked ready until its durable master playlist can be read.
- Referenced assets are uploaded before media playlists, and the master
  playlist is published last.
- Partial publishing failures must leave the master playlist unavailable.
- FFmpeg child processes must be terminated and reaped on cancellation,
  timeout, worker shutdown, and ordinary failure.
- Broker acknowledgement timeouts must remain longer than worker hard limits.
- Video deletion must target only the exact video's sharded prefixes.
- Video deletion must commit a durable `deleting` tombstone before external
  cleanup; upload completion and deletion must share the per-video fence.
- Cleanup and final-commit failures must leave a retryable tombstone, never a
  ready row whose media has already been removed.
- Generated work directories are removed only after durable publication and
  database commit. They remain worker-owned; metadata deletion must not remove
  paths from another service.

Tests that deliberately interrupt processing should also verify that queues,
processes, object prefixes, records, and workspace files are left in a
retryable state.

## Pull request checklist

- Describe the problem, the chosen approach, and user-visible behavior.
- Link the relevant issue when one exists.
- Add or update tests for changed behavior.
- Update `.env.example` and documentation for configuration changes.
- Include the commands you ran and their results.
- Call out migrations, compatibility risks, resource impact, and operational
  rollout steps.
- Confirm that no secrets, private media, generated output, or personal paths
  are included.

By contributing, you agree that your contribution is licensed under the
project's [MIT License](LICENSE).
