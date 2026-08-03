package handler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"

	"hls-engine/internal/videolease"
)

var errVideoDeleting = errors.New("video is being deleted")

// VideoLease is the upload service's narrow view of the shared PostgreSQL
// advisory lease. Keeping this as an interface makes upload/delete races
// failure-injectable in unit tests.
type VideoLease interface {
	VideoStatus(ctx context.Context) (string, error)
	Release(ctx context.Context) error
}

type VideoLeaseManager interface {
	TryAcquire(ctx context.Context, videoID string) (VideoLease, error)
}

func (h *Handler) acquireVideoWrite(
	ctx context.Context,
	videoID string,
) (VideoLease, string, error) {
	parsed, err := uuid.Parse(videoID)
	if err != nil {
		return nil, "", videolease.ErrVideoNotFound
	}
	canonicalID := parsed.String()
	if h.videoLeases == nil {
		return nil, "", errors.New("video lease manager is unavailable")
	}

	lease, err := h.videoLeases.TryAcquire(ctx, canonicalID)
	if err != nil {
		return nil, canonicalID, err
	}
	status, err := lease.VideoStatus(ctx)
	if err != nil {
		releaseVideoLease(lease)
		return nil, canonicalID, err
	}
	if status == "deleting" {
		releaseVideoLease(lease)
		return nil, canonicalID, errVideoDeleting
	}
	return lease, canonicalID, nil
}

func respondVideoWriteError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, videolease.ErrBusy):
		w.Header().Set("Retry-After", "1")
		respondError(w, http.StatusConflict, "video mutation is already in progress")
	case errors.Is(err, errVideoDeleting):
		w.Header().Set("Retry-After", "1")
		respondError(w, http.StatusConflict, "video is being deleted")
	case errors.Is(err, videolease.ErrVideoNotFound):
		respondError(w, http.StatusNotFound, "video not found")
	default:
		w.Header().Set("Retry-After", "1")
		respondError(w, http.StatusServiceUnavailable, "video state unavailable")
	}
}

func releaseVideoLease(lease VideoLease) {
	if lease == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = lease.Release(ctx)
}

func (h *Handler) invalidateMultipartSession(
	ctx context.Context,
	record multipartSessionRecord,
) error {
	var failures []error
	if record.UploadID != "" && record.ObjectName != "" {
		if err := h.minio.AbortMultipartUpload(
			ctx,
			record.ObjectName,
			record.UploadID,
		); err != nil {
			failures = append(failures, fmt.Errorf("abort upload: %w", err))
		}
	}
	if err := h.redis.Delete(
		ctx,
		multipartSessionKey(record.SessionID),
		multipartCompletionLockKey(record.SessionID),
	); err != nil {
		failures = append(failures, fmt.Errorf("delete session state: %w", err))
	}
	return errors.Join(failures...)
}
