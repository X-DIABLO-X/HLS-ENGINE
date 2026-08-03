package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"hls-engine/services/analytics/pkg/logger"
)

func main() {
	cfg := loadConfig()
	logger.Init(cfg.LogLevel)
	log := logger.L()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := NewStore(ctx, cfg)
	if err != nil {
		log.Error("failed to connect to stores", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	StartAggregator(ctx, store, cfg)

	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(logger.Middleware)
	r.Use(instrumentHandler)

	r.Get("/health", healthHandler(store))
	r.Get("/ready", readyHandler(store))
	r.Get(cfg.MetricsPath, promhttp.Handler().ServeHTTP)

	r.Route("/api/v1/analytics", func(r chi.Router) {
		r.Post("/events", ingestHandler(store))
		r.Get("/videos/{id}", videoMetricsHandler(store))
		r.Get("/dashboard", dashboardHandler(store))
	})

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      r,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	go func() {
		log.Info("analytics service starting", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server failed", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("shutdown error", "error", err)
	}
}

// traceIDKey is only referenced for context lookup in helpers; the canonical
// accessor is logger.TraceID.
type traceIDKey struct{}

var _ = traceIDKey{}

// compile-time guard to ensure the main package compiles when handlers.go does
// not reference traceIDKey directly.
func init() {
	_ = fmt.Sprintf
}
