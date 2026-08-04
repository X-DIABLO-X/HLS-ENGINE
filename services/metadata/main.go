package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"hls-engine/internal/config"
	"hls-engine/internal/db"
	"hls-engine/internal/jwt"
	"hls-engine/internal/logger"
	"hls-engine/internal/middleware"
	internalminio "hls-engine/internal/minio"
	internalredis "hls-engine/internal/redis"
	"hls-engine/metadata/internal/repository"
)

type server struct {
	cfg          config.Config
	log          zerolog.Logger
	pool         *pgxpool.Pool
	repo         *repository.Repository
	redis        *internalredis.Client
	videoDeleter videoDeleter
}

func main() {
	cfg := config.Load()
	cfg.ServiceName = "metadata"
	log := logger.WithService("metadata")

	pool, err := db.NewPool(cfg.PostgresDSN, cfg.PostgresMaxOpen, cfg.PostgresMaxIdle)
	if err != nil {
		log.Fatal().Err(err).Msg("postgres connection failed")
	}
	defer pool.Close()

	rc := internalredis.New(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)

	rawStore, err := internalminio.New(
		cfg.MinioEndpoint,
		cfg.MinioAccessKey,
		cfg.MinioSecretKey,
		getEnv("MINIO_RAW_BUCKET", "uploads-raw"),
		cfg.MinioUseSSL,
	)
	if err != nil {
		log.Fatal().Err(err).Msg("create raw object store client")
	}
	hlsStore, err := internalminio.New(
		cfg.MinioEndpoint,
		cfg.MinioAccessKey,
		cfg.MinioSecretKey,
		getEnv("MINIO_HLS_BUCKET", "hls-output"),
		cfg.MinioUseSSL,
	)
	if err != nil {
		log.Fatal().Err(err).Msg("create HLS object store client")
	}
	thumbnailStore, err := internalminio.New(
		cfg.MinioEndpoint,
		cfg.MinioAccessKey,
		cfg.MinioSecretKey,
		getEnv("MINIO_THUMBNAILS_BUCKET", "thumbnails"),
		cfg.MinioUseSSL,
	)
	if err != nil {
		log.Fatal().Err(err).Msg("create thumbnail object store client")
	}
	for _, objectStore := range []struct {
		name   string
		client *internalminio.Client
	}{
		{name: "raw", client: rawStore},
		{name: "HLS", client: hlsStore},
		{name: "thumbnail", client: thumbnailStore},
	} {
		ensureCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := objectStore.client.EnsureBucket(ensureCtx)
		cancel()
		if err != nil {
			log.Fatal().Err(err).Str("store", objectStore.name).Msg("ensure object store bucket")
		}
	}

	cachePurgeTimeout, err := time.ParseDuration(
		getEnv("NGINX_CACHE_PURGE_TIMEOUT", "10s"),
	)
	if err != nil {
		log.Fatal().Err(err).Msg("parse NGINX_CACHE_PURGE_TIMEOUT")
	}
	cachePurger, err := newHTTPVideoCachePurger(
		getEnv("NGINX_CACHE_PURGER_URL", "http://nginx-cache-purger:8080"),
		os.Getenv("NGINX_CACHE_PURGE_SECRET"),
		cachePurgeTimeout,
	)
	if err != nil {
		log.Fatal().Err(err).Msg("configure nginx cache purger")
	}

	repo := repository.New(pool)
	if err := repo.MigrateCreatorCatalog(
		context.Background(),
		getEnv("LEGACY_CATALOG_OWNER_ID", "b47be6c6-4214-4f5c-b522-bf3afd0cf582"),
	); err != nil {
		log.Fatal().Err(err).Msg("migrate creator catalog")
	}
	srv := &server{
		cfg:   cfg,
		log:   log,
		pool:  pool,
		repo:  repo,
		redis: rc,
		videoDeleter: newVideoDeletionService(
			repo,
			cachePurger,
			rawStore,
			hlsStore,
			thumbnailStore,
			rc,
		),
	}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Logger(log))
	r.Use(middleware.Recoverer)
	r.Use(middleware.CORS)

	r.Get("/health", srv.health)
	r.Get("/ready", srv.ready)

	// Video CRUD + search
	r.Get("/api/v1/videos", srv.listVideos)
	r.Post("/api/v1/videos", srv.createVideo)
	r.Get("/api/v1/videos/{id}", srv.getVideo)
	r.Put("/api/v1/videos/{id}", srv.updateVideo)
	r.Delete("/api/v1/videos/{id}", srv.deleteVideo)
	r.Patch("/api/v1/videos/{id}/status", srv.updateStatus)
	r.Get("/api/v1/videos/{id}/manifest", srv.getManifest)
	// Public embed endpoint: returns a signed manifest URL for iframe embedding.
	// No auth middleware — the signed token is the credential. This lets third
	// party sites embed <iframe src="…/embed/{id}"> without a user session.
	r.Post("/api/v1/videos/{id}/share", srv.updateVideoShare)
	r.Get("/api/v1/catalog/titles", srv.listTitles)
	r.Post("/api/v1/catalog/titles", srv.createTitle)
	r.Get("/api/v1/catalog/titles/{id}", srv.getTitle)
	r.Put("/api/v1/catalog/titles/{id}", srv.updateTitle)
	r.Delete("/api/v1/catalog/titles/{id}", srv.deleteTitle)
	r.Post("/api/v1/catalog/titles/{id}/seasons", srv.createSeason)
	r.Get("/api/v1/catalog/titles/{id}/seasons", srv.listSeasons)
	r.Post("/api/v1/catalog/titles/{id}/playables", srv.createPlayable)
	r.Get("/api/v1/catalog/titles/{id}/playables", srv.listPlayables)
	r.Delete("/api/v1/catalog/playables/{id}", srv.deletePlayable)
	r.Post("/api/v1/catalog/playables/{id}/publish", srv.publishPlayable)
	r.Post("/api/v1/catalog/playables/{id}/unpublish", srv.unpublishPlayable)
	r.Post("/api/v1/catalog/playables/{id}/share/rotate", srv.rotatePlayableShare)
	r.Get("/api/v1/embed/{shareID}", srv.getSharedEmbed)

	// Renditions
	r.Post("/api/v1/videos/{id}/renditions", srv.createRendition)
	r.Get("/api/v1/videos/{id}/renditions", srv.listRenditions)

	// Audio tracks
	r.Post("/api/v1/videos/{id}/audio", srv.createAudioTrack)
	r.Get("/api/v1/videos/{id}/audio", srv.listAudioTracks)

	// Subtitles
	r.Post("/api/v1/videos/{id}/subtitles", srv.createSubtitle)
	r.Get("/api/v1/videos/{id}/subtitles", srv.listSubtitles)

	// Progress + Settings
	r.Get("/api/v1/videos/{id}/progress", srv.getProgress)
	r.Get("/api/v1/transcoding-settings", srv.getTranscodingSettings)
	r.Put("/api/v1/transcoding-settings", srv.updateTranscodingSettings)

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
		log.Info().Str("addr", httpSrv.Addr).Msg("metadata service starting")
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("metadata listen failed")
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(ctx)
	log.Info().Msg("metadata service stopped")
}

func (s *server) health(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, map[string]string{"status": "healthy"})
}

func (s *server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.pool.Ping(ctx); err != nil {
		respondJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not ready"})
		return
	}
	respondJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

type createVideoReq struct {
	Title       string   `json:"title"`
	Description string   `json:"description"`
	Tags        []string `json:"tags"`
}

func (s *server) createVideo(w http.ResponseWriter, r *http.Request) {
	ownerID, ok := creatorID(w, r)
	if !ok {
		return
	}
	var req createVideoReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid json")
		return
	}
	v, err := s.repo.CreateVideo(r.Context(), ownerID, req.Title, req.Description, req.Tags)
	if err != nil {
		s.log.Error().Err(err).Msg("create video failed")
		respondError(w, http.StatusInternalServerError, "internal error")
		return
	}
	respondJSON(w, http.StatusCreated, v)
}

func (s *server) getVideo(w http.ResponseWriter, r *http.Request) {
	ownerID, ok := creatorID(w, r)
	if !ok {
		return
	}
	id := chi.URLParam(r, "id")
	v, err := s.repo.GetOwnedVideo(r.Context(), id, ownerID)
	if err != nil {
		if err == repository.ErrNotFound {
			respondError(w, http.StatusNotFound, "video not found")
			return
		}
		respondError(w, http.StatusInternalServerError, "internal error")
		return
	}
	respondJSON(w, http.StatusOK, v)
}

func (s *server) listVideos(w http.ResponseWriter, r *http.Request) {
	ownerID, ok := creatorID(w, r)
	if !ok {
		return
	}
	search := r.URL.Query().Get("search")
	status := r.URL.Query().Get("status")
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	pageSize, _ := strconv.Atoi(r.URL.Query().Get("page_size"))
	if pageSize == 0 {
		pageSize, _ = strconv.Atoi(r.URL.Query().Get("pageSize"))
	}
	if page == 0 {
		page = 1
	}
	if pageSize == 0 {
		pageSize = 20
	}
	videos, total, err := s.repo.ListOwnedVideos(r.Context(), ownerID, search, status, page, pageSize)
	if err != nil {
		s.log.Error().Err(err).Msg("list videos failed")
		respondError(w, http.StatusInternalServerError, "internal error")
		return
	}
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"videos":   videos,
		"page":     page,
		"pageSize": pageSize,
		"total":    total,
	})
}

type updateVideoReq struct {
	Title       string   `json:"title,omitempty"`
	Description string   `json:"description,omitempty"`
	Tags        []string `json:"tags,omitempty"`
}

func (s *server) updateVideo(w http.ResponseWriter, r *http.Request) {
	ownerID, ok := creatorID(w, r)
	if !ok {
		return
	}
	id := chi.URLParam(r, "id")
	if _, err := s.repo.GetOwnedVideo(r.Context(), id, ownerID); err != nil {
		respondOwnedError(w, err)
		return
	}
	var req updateVideoReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid json")
		return
	}
	updates := map[string]interface{}{}
	if req.Title != "" {
		updates["title"] = req.Title
	}
	if req.Description != "" {
		updates["description"] = req.Description
	}
	if req.Tags != nil {
		updates["tags"] = req.Tags
	}
	v, err := s.repo.UpdateVideo(r.Context(), id, updates)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "internal error")
		return
	}
	respondJSON(w, http.StatusOK, v)
}

func (s *server) deleteVideo(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if s.repo != nil {
		ownerID, ok := creatorID(w, r)
		if !ok {
			return
		}
		if _, err := s.repo.GetOwnedVideo(r.Context(), id, ownerID); err != nil {
			respondOwnedError(w, err)
			return
		}
		linked, err := s.repo.VideoIsLinked(r.Context(), id)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal error")
			return
		}
		if linked {
			respondError(w, http.StatusConflict, "video is linked to catalog content")
			return
		}
	}
	err := s.videoDeleter.Delete(r.Context(), id)
	switch {
	case err == nil:
		w.WriteHeader(http.StatusNoContent)
	case errors.Is(err, repository.ErrNotFound):
		respondError(w, http.StatusNotFound, "video not found")
	case errors.Is(err, repository.ErrVideoActive):
		w.Header().Set("Retry-After", "1")
		respondError(w, http.StatusConflict, "video has active processing jobs")
	case errors.Is(err, repository.ErrVideoDeletionInProgress):
		w.Header().Set("Retry-After", "1")
		respondError(w, http.StatusConflict, "video deletion is already in progress")
	case errors.Is(err, errVideoDeletionRetryable):
		s.log.Error().Err(err).Str("video_id", id).Msg("video deletion incomplete")
		w.Header().Set("Retry-After", "1")
		respondError(w, http.StatusServiceUnavailable, "video deletion incomplete; retry later")
	default:
		s.log.Error().Err(err).Str("video_id", id).Msg("delete video failed")
		respondError(w, http.StatusInternalServerError, "internal error")
	}
}

type updateStatusReq struct {
	Status string `json:"status"`
}

func (s *server) updateStatus(w http.ResponseWriter, r *http.Request) {
	ownerID, ok := creatorID(w, r)
	if !ok {
		return
	}
	id := chi.URLParam(r, "id")
	if _, err := s.repo.GetOwnedVideo(r.Context(), id, ownerID); err != nil {
		respondOwnedError(w, err)
		return
	}
	var req updateStatusReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if err := s.repo.UpdateStatus(r.Context(), id, req.Status); err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			respondError(w, http.StatusNotFound, "video not found")
			return
		}
		if errors.Is(err, repository.ErrVideoDeletionInProgress) {
			w.Header().Set("Retry-After", "1")
			respondError(w, http.StatusConflict, "video is being deleted")
			return
		}
		respondError(w, http.StatusInternalServerError, "internal error")
		return
	}
	respondJSON(w, http.StatusOK, map[string]string{"status": req.Status})
}

func (s *server) getManifest(w http.ResponseWriter, r *http.Request) {
	ownerID, ok := creatorID(w, r)
	if !ok {
		return
	}
	id := chi.URLParam(r, "id")
	video, err := s.repo.GetOwnedVideo(r.Context(), id, ownerID)
	if err != nil {
		if err == repository.ErrNotFound {
			respondError(w, http.StatusNotFound, "video not found")
			return
		}
		respondError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if video.Status != "ready" {
		respondError(w, http.StatusConflict, "video not ready")
		return
	}

	manifestPath := "/hls/" + id + "/"
	secret := []byte(getEnv("JWT_HMAC_SECRET", "change-me-signed-url-hmac-secret"))
	token := jwt.BuildSignedTokenPrefix(secret, manifestPath, time.Now().Add(1*time.Hour))
	fullURL := "/hls/" + id + "/master.m3u8?token=" + url.QueryEscape(token)

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"url":       fullURL,
		"token":     token,
		"expiresAt": time.Now().Add(1 * time.Hour).UTC().Format(time.RFC3339),
	})
}

// getEmbedManifest returns a long-lived signed manifest URL plus basic video
// metadata so the public embed page can render the player without a session.
// The endpoint itself is public (no auth middleware); the signed token is the
// only credential needed to fetch HLS segments from cdn-origin.
func (s *server) getEmbedManifest(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	video, err := s.repo.GetVideo(r.Context(), id)
	if err != nil {
		if err == repository.ErrNotFound {
			respondError(w, http.StatusNotFound, "video not found")
			return
		}
		respondError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if video.Status != "ready" {
		respondError(w, http.StatusConflict, "video not ready")
		return
	}

	manifestPath := "/hls/" + id + "/"
	secret := []byte(getEnv("JWT_HMAC_SECRET", "change-me-signed-url-hmac-secret"))
	// Embed tokens last 24h so embedded players keep working without a
	// backend round-trip for refresh; the page re-fetches on each load.
	token := jwt.BuildSignedTokenPrefix(secret, manifestPath, time.Now().Add(24*time.Hour))
	fullURL := "/hls/" + id + "/master.m3u8?token=" + url.QueryEscape(token)

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"id":        video.ID,
		"title":     video.Title,
		"status":    video.Status,
		"duration":  video.Duration,
		"url":       fullURL,
		"token":     token,
		"expiresAt": time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339),
	})
}

type createRenditionReq struct {
	Name       string `json:"name"`
	IsOriginal bool   `json:"is_original"`
	Codec      string `json:"codec"`
	Bandwidth  int    `json:"bandwidth"`
	Width      int    `json:"width"`
	Height     int    `json:"height"`
}

func (s *server) createRendition(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.ownedVideoOrReject(w, r); !ok {
		return
	}
	id := chi.URLParam(r, "id")
	var req createRenditionReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid json")
		return
	}
	rend, err := s.repo.CreateRendition(r.Context(), id, req.Name, req.Codec, req.IsOriginal, req.Bandwidth, req.Width, req.Height)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "internal error")
		return
	}
	respondJSON(w, http.StatusCreated, rend)
}

func (s *server) listRenditions(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.ownedVideoOrReject(w, r); !ok {
		return
	}
	id := chi.URLParam(r, "id")
	list, err := s.repo.ListRenditions(r.Context(), id)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "internal error")
		return
	}
	respondJSON(w, http.StatusOK, list)
}

type createAudioReq struct {
	Language string  `json:"language"`
	Name     string  `json:"name"`
	Codec    string  `json:"codec"`
	Default  bool    `json:"default"`
	DelayMs  float64 `json:"delay_ms"`
}

func (s *server) createAudioTrack(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.ownedVideoOrReject(w, r); !ok {
		return
	}
	id := chi.URLParam(r, "id")
	var req createAudioReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid json")
		return
	}
	t, err := s.repo.CreateAudioTrack(r.Context(), id, req.Language, req.Name, req.Codec, req.Default, req.DelayMs)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "internal error")
		return
	}
	respondJSON(w, http.StatusCreated, t)
}

func (s *server) listAudioTracks(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.ownedVideoOrReject(w, r); !ok {
		return
	}
	id := chi.URLParam(r, "id")
	list, err := s.repo.ListAudioTracks(r.Context(), id)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "internal error")
		return
	}
	respondJSON(w, http.StatusOK, list)
}

type createSubtitleReq struct {
	Language string `json:"language"`
	Name     string `json:"name"`
	Format   string `json:"format"`
}

func (s *server) createSubtitle(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.ownedVideoOrReject(w, r); !ok {
		return
	}
	id := chi.URLParam(r, "id")
	var req createSubtitleReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid json")
		return
	}
	sub, err := s.repo.CreateSubtitle(r.Context(), id, req.Language, req.Name, req.Format)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "internal error")
		return
	}
	respondJSON(w, http.StatusCreated, sub)
}

func (s *server) listSubtitles(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.ownedVideoOrReject(w, r); !ok {
		return
	}
	id := chi.URLParam(r, "id")
	list, err := s.repo.ListSubtitles(r.Context(), id)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "internal error")
		return
	}
	respondJSON(w, http.StatusOK, list)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// ---- Progress ----

func (s *server) getProgress(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.ownedVideoOrReject(w, r); !ok {
		return
	}
	id := chi.URLParam(r, "id")
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	key := fmt.Sprintf("video:%s:progress", id)
	data, err := s.redis.HGetAll(ctx, key)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "redis error")
		return
	}
	if len(data) == 0 {
		respondJSON(w, http.StatusOK, map[string]interface{}{
			"percent":         0,
			"stage":           "pending",
			"total_tasks":     0,
			"completed_tasks": 0,
			"tasks":           map[string]interface{}{},
		})
		return
	}

	result := map[string]interface{}{}
	tasks := map[string]interface{}{}
	for k, v := range data {
		if len(k) > 5 && k[:5] == "task:" {
			var task map[string]interface{}
			if err := json.Unmarshal([]byte(v), &task); err == nil {
				tasks[k[5:]] = task
			}
		} else {
			result[k] = v
		}
	}
	result["tasks"] = tasks
	if p, ok := result["percent"]; ok {
		if pStr, ok := p.(string); ok {
			if n, err := strconv.Atoi(pStr); err == nil {
				result["percent"] = n
			}
		}
	}
	if t, ok := result["total_tasks"]; ok {
		if tStr, ok := t.(string); ok {
			if n, err := strconv.Atoi(tStr); err == nil {
				result["total_tasks"] = n
			}
		}
	}
	if c, ok := result["completed_tasks"]; ok {
		if cStr, ok := c.(string); ok {
			if n, err := strconv.Atoi(cStr); err == nil {
				result["completed_tasks"] = n
			}
		}
	}
	respondJSON(w, http.StatusOK, result)
}

// ---- Transcoding Settings ----

var defaultTranscodingSettings = map[string]interface{}{
	"qualities":            []int{1080, 720, 480},
	"audio_bitrate_kbps":   128,
	"audio_channels":       2,
	"segment_duration_sec": 6,
	"video_preset":         "medium",
	"force_cpu":            false,
}

func (s *server) getTranscodingSettings(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	raw, err := s.redis.Get(ctx, "transcoding:defaults")
	if err == nil && raw != "" {
		var settings map[string]interface{}
		if err := json.Unmarshal([]byte(raw), &settings); err == nil {
			respondJSON(w, http.StatusOK, settings)
			return
		}
	}
	respondJSON(w, http.StatusOK, defaultTranscodingSettings)
}

func (s *server) updateTranscodingSettings(w http.ResponseWriter, r *http.Request) {
	var settings map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&settings); err != nil {
		respondError(w, http.StatusBadRequest, "invalid json")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	b, _ := json.Marshal(settings)
	if err := s.redis.Set(ctx, "transcoding:defaults", string(b), 0); err != nil {
		respondError(w, http.StatusInternalServerError, "redis error")
		return
	}
	respondJSON(w, http.StatusOK, settings)
}

func respondJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func respondError(w http.ResponseWriter, status int, msg string) {
	respondJSON(w, status, map[string]string{"error": msg})
}
