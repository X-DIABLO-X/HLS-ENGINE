// Package logger provides a small, shared structured JSON logger for the
// analytics service. Other services can copy the same pattern so that the whole
// platform emits a single, queryable log format.
package logger

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"os"
	"sync"
)

type ctxKey string

const (
	traceIDKey ctxKey = "trace_id"
	spanIDKey  ctxKey = "span_id"
)

var (
	once     sync.Once
	instance *slog.Logger
)

// Init configures the global JSON logger. Safe to call multiple times.
func Init(level string) {
	once.Do(func() {
		var lv slog.Level
		switch level {
		case "debug":
			lv = slog.LevelDebug
		case "warn":
			lv = slog.LevelWarn
		case "error":
			lv = slog.LevelError
		default:
			lv = slog.LevelInfo
		}

		h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level:       lv,
			AddSource:   false,
			ReplaceAttr: nil,
		})
		instance = slog.New(h)
		slog.SetDefault(instance)
	})
}

// L returns the configured logger, initialising it with INFO level if necessary.
func L() *slog.Logger {
	if instance == nil {
		Init("info")
	}
	return instance
}

// TraceID extracts the trace ID from ctx.
func TraceID(ctx context.Context) string {
	if v, ok := ctx.Value(traceIDKey).(string); ok {
		return v
	}
	return ""
}

// SpanID extracts the span ID from ctx.
func SpanID(ctx context.Context) string {
	if v, ok := ctx.Value(spanIDKey).(string); ok {
		return v
	}
	return ""
}

// WithTrace returns a logger enriched with trace_id and span_id from ctx.
func WithTrace(ctx context.Context) *slog.Logger {
	log := L()
	if tid := TraceID(ctx); tid != "" {
		log = log.With(slog.String("trace_id", tid))
	}
	if sid := SpanID(ctx); sid != "" {
		log = log.With(slog.String("span_id", sid))
	}
	return log
}

// WithKV returns a logger enriched with trace/span IDs and arbitrary key/value pairs.
func WithKV(ctx context.Context, args ...any) *slog.Logger {
	return WithTrace(ctx).With(args...)
}

// NewRequestID returns a cryptographically random 16-byte hex ID suitable for
// trace IDs, span IDs, or event IDs.
func NewRequestID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "unknown"
	}
	return hex.EncodeToString(b)
}

// Middleware injects trace_id and span_id into the request context. It creates
// new IDs when the client did not supply them and echoes the trace ID in the
// response header for log correlation.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tid := r.Header.Get("X-Trace-ID")
		if tid == "" {
			tid = NewRequestID()
		}
		sid := r.Header.Get("X-Span-ID")
		if sid == "" {
			sid = NewRequestID()
		}

		ctx := context.WithValue(r.Context(), traceIDKey, tid)
		ctx = context.WithValue(ctx, spanIDKey, sid)

		w.Header().Set("X-Trace-ID", tid)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
