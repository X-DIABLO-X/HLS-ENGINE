package main

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	httpRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "analytics_http_requests_total",
			Help: "Total HTTP requests handled by the analytics service.",
		},
		[]string{"method", "route", "status"},
	)

	httpRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "analytics_http_request_duration_seconds",
			Help:    "HTTP request latency distribution for the analytics service.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "route"},
	)

	eventsIngestedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "analytics_events_ingested_total",
			Help: "Total playback events ingested by type.",
		},
		[]string{"event_type"},
	)

	concurrentViewers = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "analytics_concurrent_viewers",
			Help: "Estimated number of concurrent playback sessions.",
		},
	)
)

// metricsHandler exposes Prometheus metrics on the configured path.
func metricsHandler() http.Handler {
	return promhttp.Handler()
}

// instrumentHandler records request counts and durations.
func instrumentHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(ww, r)

		status := strconv.Itoa(ww.statusCode)
		route := r.URL.Path
		httpRequestsTotal.WithLabelValues(r.Method, route, status).Inc()
		httpRequestDuration.WithLabelValues(r.Method, route).Observe(time.Since(start).Seconds())
	})
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
	written    bool
}

func (rw *responseWriter) WriteHeader(code int) {
	if !rw.written {
		rw.statusCode = code
		rw.ResponseWriter.WriteHeader(code)
		rw.written = true
	}
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	if !rw.written {
		rw.WriteHeader(http.StatusOK)
	}
	return rw.ResponseWriter.Write(b)
}
