package minio

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	minioSDK "github.com/minio/minio-go/v7"
)

func TestValidateDeletePrefix(t *testing.T) {
	t.Parallel()

	valid := []string{
		"raw/4c106c1b-0859-4b16-a053-210f52264ead/",
		"4c/4c106c1b-0859-4b16-a053-210f52264ead/",
	}
	for _, prefix := range valid {
		prefix := prefix
		t.Run("valid_"+prefix, func(t *testing.T) {
			t.Parallel()
			if err := validateDeletePrefix(prefix); err != nil {
				t.Fatalf("validateDeletePrefix(%q) returned %v", prefix, err)
			}
		})
	}

	invalid := []string{
		"",
		"/",
		"raw/",
		"/raw/video/",
		"raw/video",
		"raw//video/",
		"raw/../video/",
		`raw\video\`,
	}
	for _, prefix := range invalid {
		prefix := prefix
		t.Run("invalid_"+prefix, func(t *testing.T) {
			t.Parallel()
			err := validateDeletePrefix(prefix)
			if !errors.Is(err, ErrUnsafeDeletePrefix) {
				t.Fatalf("validateDeletePrefix(%q) error = %v, want ErrUnsafeDeletePrefix", prefix, err)
			}
		})
	}
}

func TestObjectWithinDeletePrefixRequiresDirectoryBoundary(t *testing.T) {
	t.Parallel()

	prefix := "raw/4c106c1b-0859-4b16-a053-210f52264ead/"
	if !objectWithinDeletePrefix(prefix, prefix+"source.mp4") {
		t.Fatal("expected object below exact prefix to match")
	}
	if objectWithinDeletePrefix(prefix, "raw/4c106c1b-0859-4b16-a053-210f52264ead-other/source.mp4") {
		t.Fatal("sibling video prefix must not match")
	}
}

func TestPartitionDeleteBatchesUsesS3MultiDeleteLimit(t *testing.T) {
	t.Parallel()

	objects := make([]minioSDK.ObjectInfo, 5001)
	for index := range objects {
		objects[index].Key = fmt.Sprintf("prefix/object-%04d", index)
	}
	batches := partitionDeleteBatches(objects, deleteObjectBatchSize)
	if len(batches) != 6 {
		t.Fatalf("batch count = %d, want 6", len(batches))
	}
	for index, batch := range batches {
		if len(batch) > deleteObjectBatchSize {
			t.Fatalf("batch %d has %d objects", index, len(batch))
		}
	}
	if len(batches[5]) != 1 {
		t.Fatalf("last batch size = %d, want 1", len(batches[5]))
	}
}

func TestRunBoundedLimitsConcurrencyAndCollectsFailures(t *testing.T) {
	t.Parallel()

	items := make([]int, 12)
	var active atomic.Int32
	var peak atomic.Int32
	err := runBounded(items, 3, func(_ int) error {
		current := active.Add(1)
		for {
			previous := peak.Load()
			if current <= previous || peak.CompareAndSwap(previous, current) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		active.Add(-1)
		return errors.New("injected deletion failure")
	})
	if err == nil {
		t.Fatal("runBounded unexpectedly discarded failures")
	}
	if got := peak.Load(); got < 2 || got > 3 {
		t.Fatalf("peak concurrency = %d, want 2..3", got)
	}
}
