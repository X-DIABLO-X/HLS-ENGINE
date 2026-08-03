# Analytics Service

The analytics service ingests playback events from the HLS-ENGINE frontend
player, stores them in PostgreSQL, maintains live counters in Redis, and serves
aggregated metrics for dashboards.

## Endpoints

| Method | Path                              | Description                              |
|--------|-----------------------------------|------------------------------------------|
| POST   | `/api/v1/analytics/events`        | Ingest a single event or a batch         |
| GET    | `/api/v1/analytics/videos/:id`    | Aggregated metrics for a video           |
| GET    | `/api/v1/analytics/dashboard`     | Global platform metrics                  |
| GET    | `/health`                         | Liveness probe                           |
| GET    | `/ready`                          | Readiness probe                          |
| GET    | `/metrics`                        | Prometheus metrics                       |

## Environment variables

| Variable                  | Default                                                             | Description                            |
|---------------------------|---------------------------------------------------------------------|----------------------------------------|
| `ANALYTICS_PORT`          | `8080`                                                              | HTTP port                              |
| `DATABASE_URL`            | `postgres://postgres:postgres@localhost:5432/hlsengine?sslmode=disable` | PostgreSQL DSN                      |
| `REDIS_URL`               | `localhost:6379`                                                    | Redis address                          |
| `REDIS_PASSWORD`          | ``                                                                  | Redis password                         |
| `LOG_LEVEL`               | `info`                                                              | JSON log level                         |
| `METRICS_PATH`            | `/metrics`                                                          | Prometheus scrape path                 |
| `ANALYTICS_ROLLUP_HOURLY` | `5m`                                                                | Interval between hourly rollups        |
| `ANALYTICS_ROLLUP_DAILY`  | `1h`                                                                | Interval between daily rollups         |

## Database schema

Apply `schema.sql` to the analytics database. It creates:

- `analytics_events` — raw playback events
- `analytics_hourly` — hourly materialised rollups
- `analytics_daily` — daily materialised rollups

## Event types

Common event types emitted by the player:

- `play`
- `pause`
- `heartbeat`
- `buffering`
- `quality_switch`
- `error`
- `ended`

## Build and run locally

```bash
cd services/analytics
go mod tidy
go run .
```

## Docker

```bash
cd services/analytics
docker build -t hls-engine/analytics:latest .
docker run -p 8080:8080 hls-engine/analytics:latest
```

## Logging

The service uses the shared logger in `pkg/logger/`, which emits structured JSON
logs and propagates `X-Trace-ID` / `X-Span-ID` headers.
