package handler

import (
	"errors"
	"testing"
	"time"
)

func TestMultipartPartLayout(t *testing.T) {
	t.Parallel()

	const mebibyte = int64(1 << 20)
	size := int64(20) * mebibyte
	partSize := int64(8) * mebibyte

	count, err := multipartPartCount(size, partSize)
	if err != nil {
		t.Fatalf("multipartPartCount() error = %v", err)
	}
	if count != 3 {
		t.Fatalf("multipartPartCount() = %d, want 3", count)
	}

	wantSizes := []int64{8 * mebibyte, 8 * mebibyte, 4 * mebibyte}
	for partNumber, want := range wantSizes {
		got, err := multipartPartSize(size, partSize, partNumber+1)
		if err != nil {
			t.Fatalf("multipartPartSize(part %d) error = %v", partNumber+1, err)
		}
		if got != want {
			t.Fatalf("multipartPartSize(part %d) = %d, want %d", partNumber+1, got, want)
		}
	}
}

func TestMultipartPartLayoutRejectsInvalidValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		size       int64
		partSize   int64
		partNumber int
	}{
		{name: "zero upload", size: 0, partSize: defaultPartSize, partNumber: 1},
		{name: "oversized upload", size: maxUploadSize + 1, partSize: defaultPartSize, partNumber: 1},
		{name: "undersized nominal part", size: 1, partSize: minMultipartPart - 1, partNumber: 1},
		{name: "zero part number", size: defaultPartSize, partSize: defaultPartSize, partNumber: 0},
		{name: "part number past end", size: defaultPartSize, partSize: defaultPartSize, partNumber: 2},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := multipartPartSize(tt.size, tt.partSize, tt.partNumber)
			if !errors.Is(err, errInvalidMultipartParts) {
				t.Fatalf("multipartPartSize() error = %v, want errInvalidMultipartParts", err)
			}
		})
	}
}

func TestValidateMultipartParts(t *testing.T) {
	t.Parallel()

	record := multipartSessionRecord{
		Size:     20 << 20,
		PartSize: 8 << 20,
	}
	valid := []uploadedPart{
		{PartNumber: 1, Size: 8 << 20, ETag: "one"},
		{PartNumber: 2, Size: 8 << 20, ETag: "two"},
		{PartNumber: 3, Size: 4 << 20, ETag: "three"},
	}
	if err := validateMultipartParts(record, valid); err != nil {
		t.Fatalf("validateMultipartParts(valid) error = %v", err)
	}

	tests := []struct {
		name  string
		parts []uploadedPart
	}{
		{name: "missing part", parts: valid[:2]},
		{name: "wrong order", parts: []uploadedPart{valid[1], valid[0], valid[2]}},
		{name: "wrong size", parts: []uploadedPart{valid[0], valid[1], {PartNumber: 3, Size: 3 << 20, ETag: "three"}}},
		{name: "missing etag", parts: []uploadedPart{valid[0], valid[1], {PartNumber: 3, Size: 4 << 20}}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if err := validateMultipartParts(record, tt.parts); !errors.Is(err, errInvalidMultipartParts) {
				t.Fatalf("validateMultipartParts() error = %v, want errInvalidMultipartParts", err)
			}
		})
	}
}

func TestAllowedVideoExtensionRequiresExactMatch(t *testing.T) {
	t.Parallel()

	for _, ext := range []string{".mp4", ".mov", ".mkv", ".avi", ".webm", ".mxf", ".ts"} {
		if !isAllowedVideoExtension(ext) {
			t.Fatalf("isAllowedVideoExtension(%q) = false, want true", ext)
		}
	}
	for _, ext := range []string{"", ".m", ".mp", ".exe", "mp4"} {
		if isAllowedVideoExtension(ext) {
			t.Fatalf("isAllowedVideoExtension(%q) = true, want false", ext)
		}
	}
}

func TestMultipartCompletionRecordUsesPersistedEventIdentity(t *testing.T) {
	t.Parallel()

	completedAt := time.Date(2026, time.August, 3, 12, 0, 0, 0, time.UTC)
	record := multipartSessionRecord{
		VideoID:       "video-id",
		ObjectName:    "raw/video-id/session.mp4",
		Filename:      "video.mp4",
		ContentType:   "video/mp4",
		EventID:       "event-id",
		CompletedSize: 1234,
		CompletedAt:   &completedAt,
	}
	h := &Handler{bucket: "uploads-raw"}

	got, err := h.multipartCompletionRecord(record)
	if err != nil {
		t.Fatalf("multipartCompletionRecord() error = %v", err)
	}
	if got["id"] != "event-id" {
		t.Fatalf("event id = %v, want event-id", got["id"])
	}
	if got["size"] != int64(1234) {
		t.Fatalf("size = %v, want 1234", got["size"])
	}
	if got["uploaded_at"] != completedAt {
		t.Fatalf("uploaded_at = %v, want %v", got["uploaded_at"], completedAt)
	}
}
