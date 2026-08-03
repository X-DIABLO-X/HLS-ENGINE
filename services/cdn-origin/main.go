package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	"hls-engine/cdn-origin/internal/hls"
	"hls-engine/internal/config"
	"hls-engine/internal/logger"
	"hls-engine/internal/middleware"
	"hls-engine/internal/minio"
	ir "hls-engine/internal/redis"
)

type server struct {
	cfg   config.Config
	log   zerolog.Logger
	minio *minio.Client
	hls   *hls.Service
}

func main() {
	cfg := config.Load()
	cfg.ServiceName = "cdn-origin"
	log := logger.WithService("cdn-origin")

	mc, err := minio.New(cfg.MinioEndpoint, cfg.MinioAccessKey, cfg.MinioSecretKey, cfg.MinioBucket, cfg.MinioUseSSL)
	if err != nil {
		log.Fatal().Err(err).Msg("minio connection failed")
	}
	if err := mc.EnsureBucket(context.Background()); err != nil {
		log.Fatal().Err(err).Msg("minio bucket ensure failed")
	}

	rc := ir.New(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)

	baseURL := getEnv("CDN_BASE_URL", "http://localhost:8080")
	srv := &server{
		cfg:   cfg,
		log:   log,
		minio: mc,
		hls:   hls.New(mc, rc, cfg.JWTHMACSecret, baseURL),
	}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Logger(log))
	r.Use(middleware.Recoverer)
	r.Use(middleware.CORS)

	r.Get("/health", srv.health)
	r.Get("/ready", srv.ready)

	r.Get("/hls/{videoID}/master.m3u8", srv.masterManifest)
	r.Get("/hls/{videoID}/{rendition}/{file:.+\\.m3u8}", srv.variantManifest)
	r.Get("/hls/{videoID}/{rendition}/{file:.+\\.ts}", srv.segment)
	r.Get("/hls/{videoID}/{rendition}/{file:.+\\.m4s}", srv.segment)
	r.Get("/hls/{videoID}/{rendition}/{file:.+\\.mp4}", srv.segment)
	r.Get("/hls/{videoID}/{rendition}/{file:.+\\.aac}", srv.segment)
	r.Get("/hls/{videoID}/{rendition}/{file:.+\\.vtt}", srv.segment)

	port := cfg.HTTPPort
	if port == "" {
		port = "8080"
	}
	httpSrv := &http.Server{
		Addr:         ":" + port,
		Handler:      r,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Info().Str("addr", httpSrv.Addr).Msg("cdn-origin service starting")
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("cdn-origin listen failed")
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(ctx)
	log.Info().Msg("cdn-origin service stopped")
}

func (s *server) health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"healthy"}`))
}

func (s *server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if _, err := s.minio.Client.ListBuckets(ctx); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"not ready"}`))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ready"}`))
}

func (s *server) masterManifest(w http.ResponseWriter, r *http.Request) {
	videoID := chi.URLParam(r, "videoID")
	token := r.URL.Query().Get("token")
	expiry := time.Now().Add(s.cfg.JWTSegmentTTL)
	body, contentType, err := s.hls.RewriteMasterManifest(r.Context(), videoID, token, expiry)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusForbidden)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "private, max-age=60")
	_, _ = w.Write([]byte(body))
}

func (s *server) variantManifest(w http.ResponseWriter, r *http.Request) {
	videoID := chi.URLParam(r, "videoID")
	rendition := chi.URLParam(r, "rendition")
	filePath := chi.URLParam(r, "file")
	variantPath := rendition + "/" + filePath
	token := r.URL.Query().Get("token")
	expiry := time.Now().Add(s.cfg.JWTSegmentTTL)
	body, contentType, err := s.hls.RewriteVariantManifest(r.Context(), videoID, variantPath, token, expiry)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusForbidden)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "private, max-age=60")
	_, _ = w.Write([]byte(body))
}

func (s *server) segment(w http.ResponseWriter, r *http.Request) {
	videoID := chi.URLParam(r, "videoID")
	rendition := chi.URLParam(r, "rendition")
	filePath := chi.URLParam(r, "file")
	token := r.URL.Query().Get("token")
	if err := s.hls.ProxySegment(r.Context(), w, r, videoID, rendition+"/"+filePath, token); err != nil {
		s.log.Error().Err(err).Msg("segment proxy failed")
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusNotFound)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
