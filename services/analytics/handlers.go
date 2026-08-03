package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"hls-engine/services/analytics/pkg/logger"
)

// Event represents a single playback event emitted by the frontend player.
type Event struct {
	EventID          string          `json:"event_id"`
	EventTime        time.Time       `json:"event_time"`
	VideoID          string          `json:"video_id"`
	UserSessionID    string          `json:"user_session_id"`
	EventType        string          `json:"event_type"`
	Quality          string          `json:"quality,omitempty"`
	BufferDurationMs int64           `json:"buffer_duration_ms,omitempty"`
	ErrorCode        string          `json:"error_code,omitempty"`
	ErrorMessage     string          `json:"error_message,omitempty"`
	PositionMs       int64           `json:"position_ms,omitempty"`
	DurationMs       int64           `json:"duration_ms,omitempty"`
	Payload          json.RawMessage `json:"payload,omitempty"`
	TraceID          string          `json:"trace_id,omitempty"`
	SpanID           string          `json:"span_id,omitempty"`
}

// IngestResponse is returned by the events endpoint.
type IngestResponse struct {
	Accepted int      `json:"accepted"`
	Errors   int      `json:"errors"`
	TraceID  string   `json:"trace_id"`
	Details  []string `json:"details,omitempty"`
}

func ingestHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		log := logger.WithKV(ctx, "handler", "ingest")

		body, err := readLimitedBody(r, 1<<20)
		if err != nil {
			respondError(ctx, w, http.StatusBadRequest, "cannot read body")
			return
		}

		var events []Event
		trimmed := strings.TrimSpace(string(body))
		if strings.HasPrefix(trimmed, "[") {
			if err := json.Unmarshal(body, &events); err != nil {
				respondError(ctx, w, http.StatusBadRequest, "invalid json array")
				return
			}
		} else {
			var single Event
			if err := json.Unmarshal(body, &single); err != nil {
				respondError(ctx, w, http.StatusBadRequest, "invalid json event")
				return
			}
			events = []Event{single}
		}

		accepted := 0
		errorsCount := 0
		var details []string

		for _, e := range events {
			if err := normalizeEvent(&e, ctx); err != nil {
				errorsCount++
				details = append(details, err.Error())
				continue
			}
			if err := store.SaveEvent(ctx, e); err != nil {
				log.ErrorContext(ctx, "failed to save event", "event_id", e.EventID, "error", err)
				errorsCount++
				details = append(details, "database error")
				continue
			}

			eventsIngestedTotal.WithLabelValues(e.EventType).Inc()

			// Track live concurrent viewers in Redis.
			switch e.EventType {
			case "play", "heartbeat":
				_ = store.RecordActiveSession(ctx, e.VideoID, e.UserSessionID)
			case "pause", "ended", "error":
				_ = store.RemoveActiveSession(ctx, e.VideoID, e.UserSessionID)
			}
			accepted++
		}

		// Update the live gauge from Redis after each batch.
		if cv, err := store.CountConcurrentViewers(ctx); err == nil {
			concurrentViewers.Set(float64(cv))
		}

		resp := IngestResponse{
			Accepted: accepted,
			Errors:   errorsCount,
			TraceID:  traceID(ctx),
			Details:  details,
		}
		respondJSON(ctx, w, http.StatusAccepted, resp)
		log.InfoContext(ctx, "events ingested", "accepted", accepted, "errors", errorsCount)
	}
}

func videoMetricsHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		videoID := chi.URLParam(r, "id")
		if videoID == "" {
			respondError(ctx, w, http.StatusBadRequest, "missing video id")
			return
		}
		since := parseSince(r, 24*time.Hour)
		m, err := store.GetVideoMetrics(ctx, videoID, since)
		if err != nil {
			logger.WithKV(ctx, "handler", "video_metrics").ErrorContext(ctx, "query failed", "error", err)
			respondError(ctx, w, http.StatusInternalServerError, "query failed")
			return
		}
		respondJSON(ctx, w, http.StatusOK, m)
	}
}

func dashboardHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		since := parseSince(r, 24*time.Hour)
		d, err := store.GetDashboardMetrics(ctx, since)
		if err != nil {
			logger.WithKV(ctx, "handler", "dashboard").ErrorContext(ctx, "query failed", "error", err)
			respondError(ctx, w, http.StatusInternalServerError, "query failed")
			return
		}
		respondJSON(ctx, w, http.StatusOK, d)
	}
}

func healthHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if err := store.HealthCheck(ctx); err != nil {
			respondError(ctx, w, http.StatusServiceUnavailable, "unhealthy")
			return
		}
		respondJSON(ctx, w, http.StatusOK, map[string]string{"status": "healthy"})
	}
}

func readyHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if _, err := store.RowCount(ctx); err != nil {
			respondError(ctx, w, http.StatusServiceUnavailable, "not ready")
			return
		}
		respondJSON(ctx, w, http.StatusOK, map[string]string{"status": "ready"})
	}
}

func normalizeEvent(e *Event, ctx context.Context) error {
	if e.EventType == "" {
		return errors.New("event_type is required")
	}
	if e.VideoID == "" {
		return errors.New("video_id is required")
	}
	if e.UserSessionID == "" {
		return errors.New("user_session_id is required")
	}
	if e.EventTime.IsZero() {
		e.EventTime = time.Now().UTC()
	} else {
		e.EventTime = e.EventTime.UTC()
	}
	if e.EventID == "" {
		e.EventID = logger.NewRequestID()
	}
	if e.TraceID == "" {
		e.TraceID = traceID(ctx)
	}
	if e.SpanID == "" {
		e.SpanID = logger.NewRequestID()
	}
	return nil
}

func respondJSON(ctx context.Context, w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		logger.WithTrace(ctx).ErrorContext(ctx, "failed to encode response", "error", err)
	}
}

func respondError(ctx context.Context, w http.ResponseWriter, code int, message string) {
	respondJSON(ctx, w, code, map[string]interface{}{
		"error":    message,
		"trace_id": traceID(ctx),
		"status":   code,
	})
}

func traceID(ctx context.Context) string {
	return logger.TraceID(ctx)
}

func readLimitedBody(r *http.Request, limit int64) ([]byte, error) {
	return io.ReadAll(io.LimitReader(r.Body, limit))
}

func parseSince(r *http.Request, defaultWindow time.Duration) time.Time {
	if v := r.URL.Query().Get("since"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			return t.UTC()
		}
	}
	return time.Now().UTC().Add(-defaultWindow)
}
