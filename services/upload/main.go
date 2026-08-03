package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"hls-engine/internal/config"
	"hls-engine/internal/db"
	"hls-engine/internal/logger"
	"hls-engine/internal/middleware"
	"hls-engine/internal/minio"
	"hls-engine/internal/rabbitmq"
	"hls-engine/internal/redis"
	"hls-engine/internal/videolease"
	"hls-engine/upload/internal/handler"
)

type uploadVideoLeaseManager struct {
	pool *pgxpool.Pool
}

func (m uploadVideoLeaseManager) TryAcquire(
	ctx context.Context,
	videoID string,
) (handler.VideoLease, error) {
	return videolease.TryAcquireShared(ctx, m.pool, videoID)
}

func main() {
	cfg := config.Load()
	cfg.ServiceName = "upload"
	log := logger.WithService("upload")

	pool, err := db.NewPool(
		cfg.PostgresDSN,
		cfg.PostgresMaxOpen,
		cfg.PostgresMaxIdle,
	)
	if err != nil {
		log.Fatal().Err(err).Msg("postgres connection failed")
	}
	defer pool.Close()

	mc, err := minio.New(cfg.MinioEndpoint, cfg.MinioAccessKey, cfg.MinioSecretKey, cfg.MinioBucket, cfg.MinioUseSSL)
	if err != nil {
		log.Fatal().Err(err).Msg("minio connection failed")
	}
	if err := mc.EnsureBucket(context.Background()); err != nil {
		log.Fatal().Err(err).Msg("minio bucket ensure failed")
	}

	rmq, err := rabbitmq.New(cfg.RabbitmqURL)
	if err != nil {
		log.Fatal().Err(err).Msg("rabbitmq connection failed")
	}
	defer rmq.Close()
	if err := rmq.DeclareQueue("upload.completed", true); err != nil {
		log.Fatal().Err(err).Msg("declare queue failed")
	}

	rc := redis.New(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)

	h := handler.New(
		log,
		mc,
		rmq,
		rc,
		uploadVideoLeaseManager{pool: pool},
	)

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Logger(log))
	r.Use(middleware.Recoverer)
	r.Use(middleware.CORS)

	r.Get("/health", health)
	r.Get("/ready", ready(rc, pool))

	r.Post("/api/v1/videos/{id}/upload", h.MultipartUpload)
	r.Post("/api/v1/videos/{id}/upload/presign", h.PresignUpload)
	r.Post("/api/v1/videos/{id}/upload/session", h.CreateMultipartSession)
	r.Get("/api/v1/videos/{id}/upload/session/{sessionID}", h.MultipartSessionStatus)
	r.Put("/api/v1/videos/{id}/upload/session/{sessionID}/parts/{partNumber}", h.UploadMultipartPart)
	r.Post("/api/v1/videos/{id}/upload/session/{sessionID}/complete", h.CompleteMultipartSession)
	r.Post("/api/v1/videos/{id}/upload/session/{sessionID}/abort", h.AbortMultipartSession)

	port := cfg.HTTPPort
	if port == "" {
		port = "8080"
	}
	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      r,
		ReadTimeout:  10 * time.Minute,
		WriteTimeout: 10 * time.Minute,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Info().Str("addr", srv.Addr).Msg("upload service starting")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("upload listen failed")
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	log.Info().Msg("upload service stopped")
}

func health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"healthy"}`))
}

func ready(rc *redis.Client, pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := rc.Ping(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"not ready"}`))
			return
		}
		if err := pool.Ping(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"not ready"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	}
}
