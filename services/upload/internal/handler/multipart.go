package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	minioSDK "github.com/minio/minio-go/v7"

	"hls-engine/internal/videolease"
)

const (
	// Completed records intentionally remain for one bounded retry window.
	// Retaining the deterministic EventID lets a client safely replay
	// /complete after a timeout without completing storage twice or publishing
	// a new logical upload event. Every state write refreshes this TTL; Redis
	// removes the record automatically after the 24-hour recovery window.
	multipartSessionTTL        = 24 * time.Hour
	multipartCompletionLockTTL = 5 * time.Minute
	defaultPartSize            = 8 << 20 // 8 MiB - small enough for reliable browser uploads
	minMultipartPart           = 5 << 20 // 5 MiB - S3 multipart minimum
	maxMultipartParts          = 10000
	maxSessionRequestSize      = 64 << 10
	maxUploadFilenameLength    = 1024
	maxUploadContentTypeLength = 255

	multipartStatusCreated        = "created"
	multipartStatusCompleting     = "completing"
	multipartStatusPendingEvent   = "completed_pending_event"
	multipartStatusEventPublished = "event_published"
)

var errInvalidMultipartParts = errors.New("invalid multipart parts")

type multipartSessionRequest struct {
	Filename    string `json:"filename"`
	Size        int64  `json:"size"`
	ContentType string `json:"content_type"`
}

type multipartSessionResponse struct {
	SessionID   string `json:"session_id"`
	VideoID     string `json:"video_id"`
	UploadID    string `json:"upload_id"`
	ObjectName  string `json:"object_name"`
	Filename    string `json:"filename"`
	ContentType string `json:"content_type"`
	Size        int64  `json:"size"`
	PartSize    int64  `json:"part_size"`
	Status      string `json:"status"`
	Parts       []int  `json:"parts,omitempty"`
	ExpiresIn   int    `json:"expires_in"`
}

type multipartSessionRecord struct {
	SessionID     string     `json:"session_id"`
	VideoID       string     `json:"video_id"`
	UploadID      string     `json:"upload_id"`
	ObjectName    string     `json:"object_name"`
	Filename      string     `json:"filename"`
	ContentType   string     `json:"content_type"`
	Size          int64      `json:"size"`
	PartSize      int64      `json:"part_size"`
	Status        string     `json:"status"`
	CreatedAt     time.Time  `json:"created_at"`
	EventID       string     `json:"event_id,omitempty"`
	CompletedSize int64      `json:"completed_size,omitempty"`
	CompletedAt   *time.Time `json:"completed_at,omitempty"`
}

type uploadedPart struct {
	PartNumber   int       `json:"part_number"`
	ETag         string    `json:"etag"`
	Size         int64     `json:"size"`
	LastModified time.Time `json:"last_modified"`
}

func (h *Handler) CreateMultipartSession(w http.ResponseWriter, r *http.Request) {
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

	r.Body = http.MaxBytesReader(w, r.Body, maxSessionRequestSize)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	var req multipartSessionRequest
	if err := decoder.Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		respondError(w, http.StatusBadRequest, "request body must contain one json object")
		return
	}
	if req.Filename == "" {
		respondError(w, http.StatusBadRequest, "filename required")
		return
	}
	if len(req.Filename) > maxUploadFilenameLength {
		respondError(w, http.StatusBadRequest, "filename too long")
		return
	}
	if req.Size <= 0 || req.Size > maxUploadSize {
		respondError(w, http.StatusBadRequest, "size must be between 1 byte and 10 GiB")
		return
	}
	if len(req.ContentType) > maxUploadContentTypeLength {
		respondError(w, http.StatusBadRequest, "content type too long")
		return
	}

	ext := strings.ToLower(filepath.Ext(req.Filename))
	if !isAllowedVideoExtension(ext) {
		respondError(w, http.StatusUnsupportedMediaType, "unsupported file type")
		return
	}

	contentType := req.ContentType
	if contentType == "" {
		contentType = mime.TypeByExtension(ext)
		if contentType == "" {
			contentType = "application/octet-stream"
		}
	}

	partSize := int64(defaultPartSize)
	minNeeded := 1 + (req.Size-1)/maxMultipartParts
	if minNeeded > partSize {
		partSize = alignTo(minNeeded, minMultipartPart)
	}
	if partSize < minMultipartPart {
		partSize = minMultipartPart
	}
	if _, err := multipartPartCount(req.Size, partSize); err != nil {
		respondError(w, http.StatusBadRequest, "invalid multipart layout")
		return
	}

	sessionID := uuid.NewString()
	objectName := fmt.Sprintf("raw/%s/%s%s", videoID, sessionID, ext)

	initOpts := minioSDK.PutObjectOptions{ContentType: contentType}
	uploadID, err := h.minio.NewMultipartUpload(r.Context(), objectName, initOpts)
	if err != nil {
		h.log.Error().Err(err).Msg("init multipart upload failed")
		respondError(w, http.StatusInternalServerError, "storage error")
		return
	}

	record := multipartSessionRecord{
		SessionID:   sessionID,
		VideoID:     videoID,
		UploadID:    uploadID,
		ObjectName:  objectName,
		Filename:    req.Filename,
		ContentType: contentType,
		Size:        req.Size,
		PartSize:    partSize,
		Status:      multipartStatusCreated,
		CreatedAt:   time.Now().UTC(),
	}
	if err := h.saveMultipartSession(r.Context(), record); err != nil {
		_ = h.minio.AbortMultipartUpload(r.Context(), objectName, uploadID)
		h.log.Error().Err(err).Msg("save multipart session failed")
		respondError(w, http.StatusInternalServerError, "session storage error")
		return
	}

	respondJSON(w, http.StatusCreated, multipartSessionResponse{
		SessionID:   sessionID,
		VideoID:     videoID,
		UploadID:    uploadID,
		ObjectName:  objectName,
		Filename:    req.Filename,
		ContentType: contentType,
		Size:        req.Size,
		PartSize:    partSize,
		Status:      record.Status,
		ExpiresIn:   int(multipartSessionTTL.Seconds()),
	})
}

func (h *Handler) MultipartSessionStatus(w http.ResponseWriter, r *http.Request) {
	_, record, ok := h.loadMultipartSession(w, r)
	if !ok {
		return
	}

	var partNumbers []int
	if record.Status == multipartStatusCreated {
		parts, err := h.listMultipartParts(r.Context(), record.ObjectName, record.UploadID)
		if err != nil {
			h.log.Error().Err(err).Msg("list multipart parts failed")
			respondError(w, http.StatusInternalServerError, "storage error")
			return
		}

		partNumbers = make([]int, 0, len(parts))
		for _, part := range parts {
			partNumbers = append(partNumbers, part.PartNumber)
		}
	}

	respondJSON(w, http.StatusOK, multipartSessionResponse{
		SessionID:   record.SessionID,
		VideoID:     record.VideoID,
		UploadID:    record.UploadID,
		ObjectName:  record.ObjectName,
		Filename:    record.Filename,
		ContentType: record.ContentType,
		Size:        record.Size,
		PartSize:    record.PartSize,
		Status:      record.Status,
		Parts:       partNumbers,
		ExpiresIn:   int(multipartSessionTTL.Seconds()),
	})
}

func (h *Handler) UploadMultipartPart(w http.ResponseWriter, r *http.Request) {
	_, record, ok := h.loadMultipartSession(w, r)
	if !ok {
		return
	}
	videoLease, _, err := h.acquireVideoWrite(
		r.Context(),
		record.VideoID,
	)
	if err != nil {
		if errors.Is(err, errVideoDeleting) ||
			errors.Is(err, videolease.ErrVideoNotFound) {
			if cleanupErr := h.invalidateMultipartSession(
				r.Context(),
				record,
			); cleanupErr != nil {
				h.log.Warn().
					Err(cleanupErr).
					Str("session_id", record.SessionID).
					Msg("failed to invalidate fenced multipart session")
			}
		}
		respondVideoWriteError(w, err)
		return
	}
	defer releaseVideoLease(videoLease)
	if record.Status != multipartStatusCreated {
		respondError(w, http.StatusConflict, "upload session is not accepting parts")
		return
	}

	partNumber, err := strconv.Atoi(chi.URLParam(r, "partNumber"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid part number")
		return
	}

	expectedSize, err := multipartPartSize(record.Size, record.PartSize, partNumber)
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid part number")
		return
	}
	if r.ContentLength < 0 {
		respondError(w, http.StatusLengthRequired, "content-length required")
		return
	}
	if r.ContentLength > expectedSize {
		respondError(w, http.StatusRequestEntityTooLarge, "part exceeds expected size")
		return
	}
	if r.ContentLength < expectedSize {
		respondError(w, http.StatusBadRequest, "part size does not match expected size")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, expectedSize)
	part, err := h.minio.PutObjectPart(
		r.Context(),
		record.ObjectName,
		record.UploadID,
		partNumber,
		io.LimitReader(r.Body, expectedSize),
		expectedSize,
		minioSDK.PutObjectPartOptions{},
	)
	if err != nil {
		h.log.Error().Err(err).Int("part_number", partNumber).Msg("multipart part upload failed")
		respondError(w, http.StatusInternalServerError, "storage error")
		return
	}
	if part.Size != expectedSize {
		h.log.Error().
			Int("part_number", partNumber).
			Int64("expected_size", expectedSize).
			Int64("stored_size", part.Size).
			Msg("multipart part size mismatch after upload")
		respondError(w, http.StatusInternalServerError, "stored part size mismatch")
		return
	}

	respondJSON(w, http.StatusOK, uploadedPart{
		PartNumber:   part.PartNumber,
		ETag:         part.ETag,
		Size:         part.Size,
		LastModified: part.LastModified,
	})
}

func (h *Handler) CompleteMultipartSession(w http.ResponseWriter, r *http.Request) {
	_, record, ok := h.loadMultipartSession(w, r)
	if !ok {
		return
	}
	videoLease, _, err := h.acquireVideoWrite(
		r.Context(),
		record.VideoID,
	)
	if err != nil {
		if errors.Is(err, errVideoDeleting) ||
			errors.Is(err, videolease.ErrVideoNotFound) {
			if cleanupErr := h.invalidateMultipartSession(
				r.Context(),
				record,
			); cleanupErr != nil {
				h.log.Warn().
					Err(cleanupErr).
					Str("session_id", record.SessionID).
					Msg("failed to invalidate fenced multipart completion")
			}
		}
		respondVideoWriteError(w, err)
		return
	}
	defer releaseVideoLease(videoLease)

	if record.Status != multipartStatusEventPublished {
		locked, err := h.redis.SetNX(
			r.Context(),
			multipartCompletionLockKey(record.SessionID),
			uuid.NewString(),
			multipartCompletionLockTTL,
		)
		if err != nil {
			h.log.Error().Err(err).Msg("failed to acquire multipart completion lock")
			w.Header().Set("Retry-After", "1")
			respondError(w, http.StatusServiceUnavailable, "session storage unavailable")
			return
		}
		if !locked {
			w.Header().Set("Retry-After", "1")
			respondError(w, http.StatusConflict, "upload completion is already in progress")
			return
		}
		defer h.releaseMultipartCompletionLock(record.SessionID)
	}

	switch record.Status {
	case multipartStatusEventPublished:
		recordMap, err := h.multipartCompletionRecord(record)
		if err != nil {
			h.log.Error().Err(err).Msg("invalid completed multipart session state")
			respondError(w, http.StatusInternalServerError, "corrupt session state")
			return
		}
		respondJSON(w, http.StatusOK, recordMap)
		return
	case multipartStatusPendingEvent:
		h.publishCompletedMultipart(w, r, &record)
		return
	case multipartStatusCreated:
		parts, err := h.listMultipartParts(r.Context(), record.ObjectName, record.UploadID)
		if err != nil {
			h.log.Error().Err(err).Msg("list multipart parts failed")
			w.Header().Set("Retry-After", "1")
			respondError(w, http.StatusServiceUnavailable, "storage unavailable while validating upload")
			return
		}
		if err := validateMultipartParts(record, parts); err != nil {
			respondError(w, http.StatusBadRequest, "uploaded parts do not match the session")
			return
		}

		completedAt := time.Now().UTC()
		record.EventID = uuid.NewString()
		record.CompletedAt = &completedAt
		record.CompletedSize = record.Size
		record.Status = multipartStatusCompleting
		if err := h.saveMultipartSession(r.Context(), record); err != nil {
			h.log.Error().Err(err).Msg("failed to mark multipart session as completing")
			w.Header().Set("Retry-After", "1")
			respondError(w, http.StatusServiceUnavailable, "session storage unavailable")
			return
		}
	case multipartStatusCompleting:
		if record.EventID == "" || record.CompletedAt == nil {
			h.log.Error().Str("session_id", record.SessionID).Msg("multipart completing state is incomplete")
			respondError(w, http.StatusInternalServerError, "corrupt session state")
			return
		}
	default:
		respondError(w, http.StatusConflict, "upload session cannot be completed")
		return
	}

	completedSize, err := h.ensureMultipartCompleted(r.Context(), record)
	if err != nil {
		h.log.Error().Err(err).Str("session_id", record.SessionID).Msg("complete multipart upload failed")
		if errors.Is(err, errInvalidMultipartParts) {
			respondError(w, http.StatusBadRequest, "uploaded parts do not match the session")
			return
		}
		w.Header().Set("Retry-After", "1")
		respondError(w, http.StatusServiceUnavailable, "storage unavailable while completing upload")
		return
	}

	record.CompletedSize = completedSize
	record.Status = multipartStatusPendingEvent
	if err := h.saveMultipartSession(r.Context(), record); err != nil {
		h.log.Error().Err(err).Msg("failed to persist completed multipart session")
		w.Header().Set("Retry-After", "1")
		respondError(w, http.StatusServiceUnavailable, "upload completed but session state could not be saved")
		return
	}

	h.publishCompletedMultipart(w, r, &record)
}

func (h *Handler) AbortMultipartSession(w http.ResponseWriter, r *http.Request) {
	_, record, ok := h.loadMultipartSession(w, r)
	if !ok {
		return
	}
	if record.Status != multipartStatusCreated {
		respondError(w, http.StatusConflict, "upload session can no longer be aborted")
		return
	}
	if err := h.minio.AbortMultipartUpload(r.Context(), record.ObjectName, record.UploadID); err != nil {
		h.log.Error().Err(err).Msg("abort multipart upload failed")
		respondError(w, http.StatusInternalServerError, "storage error")
		return
	}
	_ = h.redis.Delete(r.Context(), multipartSessionKey(record.SessionID))
	respondJSON(w, http.StatusOK, map[string]string{"status": "aborted"})
}

func (h *Handler) ensureMultipartCompleted(ctx context.Context, record multipartSessionRecord) (int64, error) {
	if info, err := h.minio.StatObject(ctx, record.ObjectName, minioSDK.StatObjectOptions{}); err == nil {
		if info.Size != record.Size {
			return 0, fmt.Errorf("%w: completed object size is %d, want %d", errInvalidMultipartParts, info.Size, record.Size)
		}
		return info.Size, nil
	}

	parts, err := h.listMultipartParts(ctx, record.ObjectName, record.UploadID)
	if err != nil {
		return 0, fmt.Errorf("list multipart parts: %w", err)
	}
	if err := validateMultipartParts(record, parts); err != nil {
		return 0, err
	}

	completeParts := make([]minioSDK.CompletePart, 0, len(parts))
	for _, part := range parts {
		completeParts = append(completeParts, minioSDK.CompletePart{
			PartNumber: part.PartNumber,
			ETag:       part.ETag,
		})
	}

	result, err := h.minio.CompleteMultipartUpload(
		ctx,
		record.ObjectName,
		record.UploadID,
		completeParts,
		minioSDK.PutObjectOptions{ContentType: record.ContentType},
	)
	if err != nil {
		// A concurrent request or a process restart may observe NoSuchUpload
		// after the object was already committed. Verify the final object before
		// deciding completion failed.
		info, statErr := h.minio.StatObject(ctx, record.ObjectName, minioSDK.StatObjectOptions{})
		if statErr == nil {
			if info.Size != record.Size {
				return 0, fmt.Errorf("%w: completed object size is %d, want %d", errInvalidMultipartParts, info.Size, record.Size)
			}
			return info.Size, nil
		}
		return 0, fmt.Errorf("complete multipart upload: %w", err)
	}

	completedSize := result.Size
	if completedSize == 0 {
		info, statErr := h.minio.StatObject(ctx, record.ObjectName, minioSDK.StatObjectOptions{})
		if statErr != nil {
			return 0, fmt.Errorf("stat completed multipart object: %w", statErr)
		}
		completedSize = info.Size
	}
	if completedSize != record.Size {
		return 0, fmt.Errorf("%w: completed object size is %d, want %d", errInvalidMultipartParts, completedSize, record.Size)
	}
	return completedSize, nil
}

func (h *Handler) publishCompletedMultipart(w http.ResponseWriter, r *http.Request, record *multipartSessionRecord) {
	recordMap, err := h.multipartCompletionRecord(*record)
	if err != nil {
		h.log.Error().Err(err).Msg("invalid completed multipart session state")
		respondError(w, http.StatusInternalServerError, "corrupt session state")
		return
	}

	if err := h.publishUploadCompleted(r.Context(), recordMap); err != nil {
		h.log.Error().Err(err).Str("session_id", record.SessionID).Msg("publish upload.completed failed")
		w.Header().Set("Retry-After", "1")
		respondError(w, http.StatusServiceUnavailable, "upload completed but event dispatch failed; retry completion")
		return
	}

	record.Status = multipartStatusEventPublished
	if err := h.saveMultipartSession(r.Context(), *record); err != nil {
		// The event was dispatched, so returning failure would invite a duplicate
		// retry. Keep the deterministic event ID in the response and surface the
		// state persistence problem in logs for reconciliation.
		h.log.Error().Err(err).Str("session_id", record.SessionID).Msg("event published but completion state was not saved")
	}
	respondJSON(w, http.StatusOK, recordMap)
}

func (h *Handler) multipartCompletionRecord(record multipartSessionRecord) (map[string]interface{}, error) {
	if record.EventID == "" || record.CompletedAt == nil || record.CompletedSize <= 0 {
		return nil, errors.New("missing completion event state")
	}
	return map[string]interface{}{
		"id":           record.EventID,
		"video_id":     record.VideoID,
		"object_name":  record.ObjectName,
		"filename":     record.Filename,
		"size":         record.CompletedSize,
		"content_type": record.ContentType,
		"bucket":       h.bucket,
		"uploaded_at":  record.CompletedAt.UTC(),
	}, nil
}

func (h *Handler) saveMultipartSession(ctx context.Context, record multipartSessionRecord) error {
	b, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return h.redis.Set(ctx, multipartSessionKey(record.SessionID), string(b), multipartSessionTTL)
}

func (h *Handler) loadMultipartSession(w http.ResponseWriter, r *http.Request) (string, multipartSessionRecord, bool) {
	sessionID := chi.URLParam(r, "sessionID")
	if sessionID == "" {
		respondError(w, http.StatusBadRequest, "session id required")
		return "", multipartSessionRecord{}, false
	}

	value, err := h.redis.Get(r.Context(), multipartSessionKey(sessionID))
	if err != nil || value == "" {
		respondError(w, http.StatusNotFound, "upload session not found")
		return "", multipartSessionRecord{}, false
	}

	var record multipartSessionRecord
	if err := json.Unmarshal([]byte(value), &record); err != nil {
		respondError(w, http.StatusInternalServerError, "corrupt session state")
		return "", multipartSessionRecord{}, false
	}
	return sessionID, record, true
}

func (h *Handler) listMultipartParts(ctx context.Context, objectName, uploadID string) ([]uploadedPart, error) {
	parts := make([]uploadedPart, 0, 8)
	marker := 0
	for {
		result, err := h.minio.ListObjectParts(ctx, objectName, uploadID, marker, 1000)
		if err != nil {
			return nil, err
		}
		for _, part := range result.ObjectParts {
			parts = append(parts, uploadedPart{
				PartNumber:   part.PartNumber,
				ETag:         part.ETag,
				Size:         part.Size,
				LastModified: part.LastModified,
			})
		}
		if !result.IsTruncated {
			break
		}
		marker = result.NextPartNumberMarker
		if marker <= 0 {
			break
		}
	}
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})
	return parts, nil
}

func multipartSessionKey(sessionID string) string {
	return fmt.Sprintf("upload:multipart:%s", sessionID)
}

func multipartCompletionLockKey(sessionID string) string {
	return fmt.Sprintf("upload:multipart:%s:completion-lock", sessionID)
}

func (h *Handler) releaseMultipartCompletionLock(sessionID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := h.redis.Delete(ctx, multipartCompletionLockKey(sessionID)); err != nil {
		h.log.Warn().Err(err).Str("session_id", sessionID).Msg("failed to release multipart completion lock")
	}
}

func multipartPartCount(size, partSize int64) (int, error) {
	if size <= 0 || size > maxUploadSize {
		return 0, fmt.Errorf("%w: invalid upload size", errInvalidMultipartParts)
	}
	if partSize < minMultipartPart {
		return 0, fmt.Errorf("%w: invalid part size", errInvalidMultipartParts)
	}

	count := 1 + (size-1)/partSize
	if count <= 0 || count > maxMultipartParts {
		return 0, fmt.Errorf("%w: part count %d exceeds limit", errInvalidMultipartParts, count)
	}
	return int(count), nil
}

func multipartPartSize(size, partSize int64, partNumber int) (int64, error) {
	count, err := multipartPartCount(size, partSize)
	if err != nil {
		return 0, err
	}
	if partNumber < 1 || partNumber > count {
		return 0, fmt.Errorf("%w: part number %d outside 1..%d", errInvalidMultipartParts, partNumber, count)
	}
	if partNumber < count {
		return partSize, nil
	}
	return size - int64(count-1)*partSize, nil
}

func validateMultipartParts(record multipartSessionRecord, parts []uploadedPart) error {
	count, err := multipartPartCount(record.Size, record.PartSize)
	if err != nil {
		return err
	}
	if len(parts) != count {
		return fmt.Errorf("%w: found %d parts, want %d", errInvalidMultipartParts, len(parts), count)
	}

	for i, part := range parts {
		expectedNumber := i + 1
		if part.PartNumber != expectedNumber {
			return fmt.Errorf("%w: found part number %d, want %d", errInvalidMultipartParts, part.PartNumber, expectedNumber)
		}
		expectedSize, err := multipartPartSize(record.Size, record.PartSize, expectedNumber)
		if err != nil {
			return err
		}
		if part.Size != expectedSize {
			return fmt.Errorf("%w: part %d has size %d, want %d", errInvalidMultipartParts, part.PartNumber, part.Size, expectedSize)
		}
		if part.ETag == "" {
			return fmt.Errorf("%w: part %d has no etag", errInvalidMultipartParts, part.PartNumber)
		}
	}
	return nil
}

func alignTo(size int64, multiple int64) int64 {
	if multiple <= 0 {
		return size
	}
	rem := size % multiple
	if rem == 0 {
		return size
	}
	return size + multiple - rem
}
