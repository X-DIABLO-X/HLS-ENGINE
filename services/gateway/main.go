package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	"hls-engine/gateway/internal/proxy"
	"hls-engine/internal/config"
	"hls-engine/internal/jwt"
	"hls-engine/internal/logger"
	"hls-engine/internal/middleware"
)

func main() {
	cfg := config.Load()
	log := logger.WithService("gateway")

	jm := jwt.NewManager(cfg.JWTSecret, cfg.JWTAccessTTL, cfg.JWTRefreshTTL)

	// Build proxy registry from environment. Defaults point to Kubernetes-style service names.
	registry, err := proxy.NewRegistry(map[string]string{
		"/api/v1/auth/":                getEnv("AUTH_SERVICE_URL", "http://auth:8080"),
		"/api/v1/videos":               getEnv("METADATA_SERVICE_URL", "http://metadata:8080"),
		"/api/v1/catalog/":             getEnv("METADATA_SERVICE_URL", "http://metadata:8080"),
		"/api/v1/embed/":               getEnv("METADATA_SERVICE_URL", "http://metadata:8080"),
		"/api/v1/transcoding-settings": getEnv("METADATA_SERVICE_URL", "http://metadata:8080"),
		"/api/v1/upload/":              getEnv("UPLOAD_SERVICE_URL", "http://upload:8080"),
		"/api/v1/analytics/":           getEnv("ANALYTICS_SERVICE_URL", "http://analytics:8080"),
		"/hls/":                        getEnv("CDN_ORIGIN_SERVICE_URL", "http://cdn-origin:8080"),
	})
	if err != nil {
		log.Fatal().Err(err).Msg("failed to build proxy registry")
	}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Logger(log))
	r.Use(middleware.Recoverer)
	r.Use(middleware.CORS)

	// Health endpoints
	r.Get("/health", healthHandler(&log))
	r.Get("/ready", readyHandler(&log))

	// Public auth proxy
	r.Handle("/api/v1/auth/*", http.HandlerFunc(registry.Handler))

	// Public embeds are capabilities addressed by opaque, revocable share IDs.
	r.Get("/api/v1/embed/{shareID}", http.HandlerFunc(registry.Handler))

	// Upload routes go to upload service
	uploadProxy := httputil.NewSingleHostReverseProxy(mustParseURL(getEnv("UPLOAD_SERVICE_URL", "http://upload:8080")))
	// Flush immediately so chunked upload parts stream through without buffering
	uploadProxy.FlushInterval = -1

	// Transcoder API routes use /api/v1 prefix; strip it before forwarding.
	transcoderURL := mustParseURL(getEnv("TRANSCODER_API_SERVICE_URL", "http://transcoder-api:8080"))
	transcoderProxy := httputil.NewSingleHostReverseProxy(transcoderURL)
	oldDirector := transcoderProxy.Director
	transcoderProxy.Director = func(req *http.Request) {
		oldDirector(req)
		req.URL.Path = strings.TrimPrefix(req.URL.Path, "/api/v1")
		req.URL.RawPath = ""
	}

	r.Group(func(uploadRouter chi.Router) {
		uploadRouter.Use(authMiddleware(jm, cfg.APIKeyHeader, &log))
		uploadRouter.Post("/api/v1/videos/{id}/upload", proxyHandler(uploadProxy))
		uploadRouter.Post("/api/v1/videos/{id}/upload/presign", proxyHandler(uploadProxy))
		uploadRouter.Post("/api/v1/videos/{id}/upload/session", proxyHandler(uploadProxy))
		uploadRouter.Get("/api/v1/videos/{id}/upload/session/{sessionID}", proxyHandler(uploadProxy))
		uploadRouter.Put("/api/v1/videos/{id}/upload/session/{sessionID}/parts/{partNumber}", proxyHandler(uploadProxy))
		uploadRouter.Post("/api/v1/videos/{id}/upload/session/{sessionID}/complete", proxyHandler(uploadProxy))
		uploadRouter.Post("/api/v1/videos/{id}/upload/session/{sessionID}/abort", proxyHandler(uploadProxy))
	})

	// Authenticated metadata routes
	r.Group(func(protected chi.Router) {
		protected.Use(authMiddleware(jm, cfg.APIKeyHeader, &log))
		protected.Get("/api/v1/videos", http.HandlerFunc(registry.Handler))
		protected.Post("/api/v1/videos", http.HandlerFunc(registry.Handler))
		protected.Get("/api/v1/videos/{id}", http.HandlerFunc(registry.Handler))
		protected.Put("/api/v1/videos/{id}", http.HandlerFunc(registry.Handler))
		protected.Delete("/api/v1/videos/{id}", http.HandlerFunc(registry.Handler))
		protected.Patch("/api/v1/videos/{id}/status", http.HandlerFunc(registry.Handler))
		protected.Get("/api/v1/videos/{id}/manifest", http.HandlerFunc(registry.Handler))
		protected.Post("/api/v1/videos/{id}/renditions", http.HandlerFunc(registry.Handler))
		protected.Get("/api/v1/videos/{id}/renditions", http.HandlerFunc(registry.Handler))
		protected.Post("/api/v1/videos/{id}/audio", http.HandlerFunc(registry.Handler))
		protected.Get("/api/v1/videos/{id}/audio", http.HandlerFunc(registry.Handler))
		protected.Post("/api/v1/videos/{id}/subtitles", http.HandlerFunc(registry.Handler))
		protected.Get("/api/v1/videos/{id}/subtitles", http.HandlerFunc(registry.Handler))
		protected.Get("/api/v1/videos/{id}/progress", http.HandlerFunc(registry.Handler))
		protected.Post("/api/v1/videos/{id}/share", http.HandlerFunc(registry.Handler))
		protected.Handle("/api/v1/catalog/*", http.HandlerFunc(registry.Handler))
		protected.Post("/api/v1/videos/{id}/retry", proxyHandler(transcoderProxy))
		protected.Get("/api/v1/transcoding-settings", http.HandlerFunc(registry.Handler))
		protected.Put("/api/v1/transcoding-settings", http.HandlerFunc(registry.Handler))
		protected.Get("/api/v1/jobs/{id}", proxyHandler(transcoderProxy))
		protected.Handle("/api/v1/analytics/*", http.HandlerFunc(registry.Handler))
	})

	// HLS delivery can be public or token-validated by cdn-origin; proxy transparently.
	r.Handle("/hls/*", http.HandlerFunc(registry.Handler))

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
		log.Info().Str("addr", srv.Addr).Msg("gateway starting")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("gateway listen failed")
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Error().Err(err).Msg("gateway shutdown error")
	}
	log.Info().Msg("gateway stopped")
}

func healthHandler(log *zerolog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"healthy"}`))
	}
}

func readyHandler(log *zerolog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	}
}

func authMiddleware(jm *jwt.Manager, apiKeyHeader string, log *zerolog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Identity headers are an internal trust boundary. Never allow a
			// client to smuggle another creator's identity to an upstream.
			r.Header.Del("X-Authenticated-User-ID")
			r.Header.Del("X-Authenticated-Role")
			r.Header.Del("X-Authenticated-API-Key")
			authHeader := r.Header.Get("Authorization")
			apiKey := r.Header.Get(apiKeyHeader)

			if authHeader != "" {
				parts := strings.SplitN(authHeader, " ", 2)
				if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
					http.Error(w, `{"error":"invalid authorization header"}`, http.StatusUnauthorized)
					return
				}
				claims, err := jm.Validate(parts[1])
				if err != nil {
					http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusUnauthorized)
					return
				}
				if claims.Type != "access" {
					http.Error(w, `{"error":"invalid token type"}`, http.StatusUnauthorized)
					return
				}
				r.Header.Set("X-Authenticated-User-ID", claims.UserID)
				r.Header.Set("X-Authenticated-Role", claims.Role)
				ctx := context.WithValue(r.Context(), "claims", claims)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}

			if apiKey != "" {
				valid, err := validateAPIKey(r.Context(), apiKey)
				if err != nil || !valid {
					http.Error(w, `{"error":"invalid api key"}`, http.StatusUnauthorized)
					return
				}
				r.Header.Set("X-Authenticated-API-Key", "true")
				next.ServeHTTP(w, r)
				return
			}

			http.Error(w, `{"error":"missing authorization"}`, http.StatusUnauthorized)
		})
	}
}

func validateAPIKey(ctx context.Context, key string) (bool, error) {
	authURL := getEnv("AUTH_SERVICE_URL", "http://auth:8080")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, authURL+"/api/v1/auth/apikeys/validate?key="+url.QueryEscape(key), nil)
	if err != nil {
		return false, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func mustParseURL(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil {
		panic(err)
	}
	return u
}

func proxyHandler(proxy *httputil.ReverseProxy) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		proxy.ServeHTTP(w, r)
	}
}
