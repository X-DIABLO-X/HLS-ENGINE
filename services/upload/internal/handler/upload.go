package handler

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"hls-engine/internal/minio"
	"hls-engine/internal/rabbitmq"
	internalredis "hls-engine/internal/redis"
)

const (
	maxUploadSize = 10 << 30 // 10 GiB
)

var allowedVideoExtensions = map[string]struct{}{
	".mp4":  {},
	".mov":  {},
	".mkv":  {},
	".avi":  {},
	".webm": {},
	".mxf":  {},
	".ts":   {},
}

// Handler handles upload requests.
type Handler struct {
	log         zerolog.Logger
	minio       *minio.Client
	rmq         *rabbitmq.Client
	redis       *internalredis.Client
	bucket      string
	videoLeases VideoLeaseManager
}

func New(
	log zerolog.Logger,
	mc *minio.Client,
	rmq *rabbitmq.Client,
	rc *internalredis.Client,
	videoLeases VideoLeaseManager,
) *Handler {
	return &Handler{
		log:         log,
		minio:       mc,
		rmq:         rmq,
		redis:       rc,
		bucket:      mc.Bucket,
		videoLeases: videoLeases,
	}
}

func (h *Handler) PresignUpload(w http.ResponseWriter, r *http.Request) {
	// A direct-to-object-store URL remains writable after this service has
	// checked PostgreSQL, so it cannot participate in the upload/delete fence.
	// Fail closed and direct clients to the managed multipart session API.
	respondError(
		w,
		http.StatusNotImplemented,
		"presigned uploads are disabled; use the managed upload session API",
	)
}

func (h *Handler) MultipartUpload(w http.ResponseWriter, r *http.Request) {
	videoID := chi.URLParam(r, "id")
	if videoID == "" {
		respondError(w, http.StatusBadRequest, "video id required")
		return
	}
	videoLease, canonicalVideoID, err := h.acquireVideoWrite(
		r.Context(),
		videoID,
	)
	if err != nil {
		respondVideoWriteError(w, err)
		return
	}
	defer releaseVideoLease(videoLease)
	videoID = canonicalVideoID

	idemp := r.Header.Get("X-Idempotency-Key")
	if idemp != "" {
		cached, err := h.redis.Get(r.Context(), idempotencyKey(idemp))
		if err == nil && cached != "" {
			respondJSON(w, http.StatusOK, json.RawMessage(cached))
			return
		}
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxUploadSize)

	mr, err := r.MultipartReader()
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid multipart form")
		return
	}

	var filePart *multipart.Part
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			respondError(w, http.StatusBadRequest, "failed to read multipart")
			return
		}
		if part.FormName() == "file" {
			filePart = part
			break
		}
		part.Close()
	}
	if filePart == nil {
		respondError(w, http.StatusBadRequest, "file required")
		return
	}
	defer filePart.Close()

	filename := filePart.FileName()
	ext := strings.ToLower(filepath.Ext(filename))
	if !isAllowedVideoExtension(ext) {
		respondError(w, http.StatusUnsupportedMediaType, "unsupported file type")
		return
	}

	contentType := filePart.Header.Get("Content-Type")
	if contentType == "" {
		contentType = mime.TypeByExtension(ext)
	}

	objectName := fmt.Sprintf("raw/%s/%s%s", videoID, uuid.NewString(), ext)
	reader := bufio.NewReaderSize(filePart, 1<<20)

	size, err := h.minio.PutObject(r.Context(), objectName, reader, -1, contentType)
	if err != nil {
		h.log.Error().Err(err).Msg("minio put failed")
		respondError(w, http.StatusInternalServerError, "storage error")
		return
	}

	record := map[string]interface{}{
		"id":           uuid.NewString(),
		"video_id":     videoID,
		"object_name":  objectName,
		"filename":     filename,
		"size":         size,
		"content_type": contentType,
		"bucket":       h.bucket,
		"uploaded_at":  time.Now().UTC(),
	}
	if err := h.publishUploadCompleted(r.Context(), record); err != nil {
		h.log.Error().Err(err).Msg("publish upload.completed failed")
		w.Header().Set("Retry-After", "1")
		respondError(w, http.StatusServiceUnavailable, "upload stored but event dispatch failed; retry later")
		return
	}

	if idemp != "" {
		b, _ := json.Marshal(record)
		_ = h.redis.Set(r.Context(), idempotencyKey(idemp), string(b), 24*time.Hour)
	}
	respondJSON(w, http.StatusCreated, record)
}

func (h *Handler) publishUploadCompleted(ctx context.Context, record map[string]interface{}) error {
	body, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return h.rmq.PublishJSON(ctx, "", "upload.completed", body)
}

func idempotencyKey(k string) string {
	return fmt.Sprintf("idempotency:%s", k)
}

func isAllowedVideoExtension(ext string) bool {
	_, ok := allowedVideoExtensions[ext]
	return ok
}

func respondJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func respondError(w http.ResponseWriter, status int, msg string) {
	respondJSON(w, status, map[string]string{"error": msg})
}
