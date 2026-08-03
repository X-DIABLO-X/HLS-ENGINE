# HLS-ENGINE Observability

This document describes the observability stack for HLS-ENGINE: metrics, logs,
traces, dashboards, and alerting. It covers both the platform-wide setup and the
conventions each service should follow.

## Stack overview

| Layer           | Technology                 | Purpose                                          |
|-----------------|----------------------------|--------------------------------------------------|
| Metrics         | Prometheus                 | Collect counters, gauges, histograms, summaries  |
| Visualisation   | Grafana                    | Dashboards and ad-hoc queries                    |
| Alerting        | Prometheus Alertmanager    | Route alerts to Slack/PagerDuty/email            |
| Logs            | Structured JSON on stdout  | Centralised by Fluent Bit / Loki / CloudWatch    |
| Traces          | OpenTelemetry (future)     | Distributed request correlation                  |
| Service metrics | Prometheus Go client       | Per-service instrumentation                      |

## Accessing Prometheus and Grafana

When running the local Docker Compose stack managed by the infra agent:

| Service     | URL                                    | Notes                              |
|-------------|----------------------------------------|------------------------------------|
| Prometheus  | http://localhost:9090                  | Query metrics, graph, and rules    |
| Grafana     | http://localhost:3000                  | Default credentials `admin/admin`  |
| Alertmanager| http://localhost:9093                  | Silences and alert routing         |

In production, expose these endpoints through an authenticated reverse proxy or
Vercel/private network. Do not publish Prometheus, Grafana, or Alertmanager to
the public internet without authentication.

### Provisioning the Grafana dashboard

The dashboard is stored as JSON at:

```text
infra/grafana/dashboards/hls-engine.json
```

Mount or import this file into Grafana. If using Docker Compose, place it under
`/etc/grafana/provisioning/dashboards/` or configure a dashboard provider that
loads JSON from `infra/grafana/dashboards`.

### Loading Prometheus alert rules

Reference `infra/prometheus/alerts.yml` from `prometheus.yml`:

```yaml
rule_files:
  - /etc/prometheus/alerts.yml
```

## Key metrics exposed by each service

All services must expose `/metrics` using the Prometheus client and should also
expose `/health` and `/ready` for load-balancer and orchestrator probes.

### Analytics service (`services/analytics/`)

| Metric                                              | Type      | Description                                    |
|-----------------------------------------------------|-----------|------------------------------------------------|
| `analytics_http_requests_total`                     | Counter   | Requests by method, route, and status          |
| `analytics_http_request_duration_seconds`           | Histogram | Request latency by method and route            |
| `analytics_events_ingested_total`                   | Counter   | Playback events ingested by event type         |
| `analytics_concurrent_viewers`                      | Gauge     | Estimated concurrent playback sessions         |

HTTP endpoints:

- `POST /api/v1/analytics/events` — ingest player events
- `GET  /api/v1/analytics/videos/:id` — per-video aggregated metrics
- `GET  /api/v1/analytics/dashboard` — global metrics
- `GET  /health`, `/ready`, `/metrics`

### Gateway, Auth, Upload, Metadata, CDN-Origin services

Future Go services should expose at least:

| Metric                              | Type      | Description                                  |
|-------------------------------------|-----------|----------------------------------------------|
| `{service}_http_requests_total`     | Counter   | Requests by method, route, status            |
| `{service}_http_request_duration_seconds` | Histogram | Request latency distribution           |
| `{service}_active_connections`      | Gauge     | Active HTTP connections                      |

### Transcoding pipeline

| Metric                              | Type      | Description                                  |
|-------------------------------------|-----------|----------------------------------------------|
| `transcoding_queue_depth`           | Gauge     | Number of jobs waiting to be processed       |
| `transcode_duration_seconds`        | Histogram | Total time to transcode a video              |
| `transcode_frames_total`            | Counter   | Frames encoded by codec/rendition            |
| `transcoder_jobs_failed_total`      | Counter   | Failed transcode/packaging jobs              |

### CDN / Nginx edge

| Metric                              | Type      | Description                                  |
|-------------------------------------|-----------|----------------------------------------------|
| `cdn_cache_hits_total`              | Counter   | Cache hits at the edge                       |
| `cdn_cache_misses_total`            | Counter   | Cache misses at the edge                     |
| `nginx_http_requests_total`         | Counter   | Raw edge requests (if using nginx-exporter)  |

### Message queue

| Metric                              | Type      | Description                                  |
|-------------------------------------|-----------|----------------------------------------------|
| `rabbitmq_queue_messages`           | Gauge     | Ready messages per queue                     |
| `rabbitmq_queue_messages_unacked`   | Gauge     | Unacknowledged messages per queue            |

## Logging format

All services emit **structured JSON logs to stdout**. The shared logger pattern
lives in `services/analytics/pkg/logger/` and can be copied to other services.

### Default JSON log fields

```json
{
  "time": "2024-05-01T12:34:56.789Z",
  "level": "INFO",
  "msg": "events ingested",
  "service": "analytics",
  "trace_id": "a1b2c3d4e5f6...",
  "span_id": "b2c3d4e5f6a1...",
  "handler": "ingest",
  "accepted": 42,
  "errors": 0
}
```

### Required fields

| Field      | Description                                                          |
|------------|----------------------------------------------------------------------|
| `time`     | ISO 8601 timestamp (provided by `slog` JSON handler)                 |
| `level`    | `DEBUG`, `INFO`, `WARN`, `ERROR`                                     |
| `msg`      | Human-readable log message                                           |
| `service`  | Service name (set by each service when initialising the logger)      |
| `trace_id` | Correlation ID propagated across requests                            |
| `span_id`  | Current request/operation span ID                                    |

### Optional common fields

- `video_id`
- `user_session_id`
- `event_type`
- `route`, `method`, `status`
- `duration_ms`
- `error` (on error logs)

## Trace propagation

HLS-ENGINE uses lightweight trace IDs until full OpenTelemetry is adopted.

### Headers

| Header        | Purpose                                           |
|---------------|---------------------------------------------------|
| `X-Trace-ID`  | Correlates a single end-to-end request            |
| `X-Span-ID`   | Identifies the current hop / span                 |

### Behaviour

1. The frontend or gateway creates a `X-Trace-ID` at request start.
2. Every downstream service copies the headers into its log context.
3. Services may create a new `X-Span-ID` for each hop.
4. The analytics service echoes `X-Trace-ID` in responses.

Example curl that includes trace headers:

```bash
curl -X POST http://localhost:8080/api/v1/analytics/events \
  -H "Content-Type: application/json" \
  -H "X-Trace-ID: trace-abc-123" \
  -H "X-Span-ID: span-analytics-1" \
  -d '{
    "event_type": "play",
    "video_id": "vid-123",
    "user_session_id": "session-456",
    "event_time": "2024-05-01T12:34:56Z"
  }'
```

## Alerting rules

Example Prometheus rules are in `infra/prometheus/alerts.yml`. They cover:

- High HTTP error rate
- High request latency
- Service down
- Transcoding queue depth
- Slow transcode jobs
- Low CDN cache hit ratio
- Concurrent viewer spikes
- Stopped analytics ingestion

Configure Alertmanager to route `critical` alerts to on-call and `warning`
alerts to Slack/email.

## Dashboard panels

The `HLS-ENGINE Overview` dashboard (`infra/grafana/dashboards/hls-engine.json`)
includes:

- Requests per second
- Error rate
- CDN cache hit ratio
- Transcoding queue depth
- Average transcode time
- Concurrent viewers

## Local development checklist

1. Start Prometheus, Grafana, PostgreSQL, Redis, and the analytics service.
2. Open http://localhost:9090/targets and verify all scrape targets are `UP`.
3. Open http://localhost:3000 and import `hls-engine.json`.
4. Send test events to `POST /api/v1/analytics/events`.
5. Confirm `analytics_events_ingested_total` and `analytics_concurrent_viewers`
   appear in Prometheus and the Grafana dashboard.
