package minio

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	minioSDK "github.com/minio/minio-go/v7"
)

func TestDeletePrefixIntegration(t *testing.T) {
	endpoint := os.Getenv("TEST_MINIO_ENDPOINT")
	accessKey := os.Getenv("TEST_MINIO_ACCESS_KEY")
	secretKey := os.Getenv("TEST_MINIO_SECRET_KEY")
	if endpoint == "" || accessKey == "" || secretKey == "" {
		t.Skip("TEST_MINIO_* integration settings are not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	bucket := "hls-delete-" + strings.ReplaceAll(uuid.NewString(), "-", "")
	client, err := New(endpoint, accessKey, secretKey, bucket, false)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := client.EnsureBucket(ctx); err != nil {
		t.Fatalf("EnsureBucket: %v", err)
	}
	var exactUploadID, siblingUploadID, prefix, siblingPrefix string
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			30*time.Second,
		)
		defer cleanupCancel()
		if exactUploadID != "" && prefix != "" {
			_ = client.AbortMultipartUpload(
				cleanupCtx,
				prefix+"incomplete.bin",
				exactUploadID,
			)
		}
		if siblingUploadID != "" && siblingPrefix != "" {
			_ = client.AbortMultipartUpload(
				cleanupCtx,
				siblingPrefix+"incomplete.bin",
				siblingUploadID,
			)
		}
		if prefix != "" {
			_ = client.DeletePrefix(cleanupCtx, prefix)
		}
		if siblingPrefix != "" {
			_ = client.DeletePrefix(cleanupCtx, siblingPrefix)
		}
		_ = client.Client.RemoveBucket(cleanupCtx, bucket)
	})

	videoID := uuid.NewString()
	prefix = "raw/" + videoID + "/"
	siblingPrefix = "raw/" + videoID + "-sibling/"

	// More than one S3 multi-delete batch exercises bounded batch dispatch.
	objects := make([]int, deleteObjectBatchSize+1)
	if err := runBounded(objects, deleteMaxWorkers, func(index int) error {
		key := fmt.Sprintf("%sobject-%04d.bin", prefix, index)
		_, err := client.PutObject(
			ctx,
			key,
			bytes.NewReader([]byte{byte(index)}),
			1,
			"application/octet-stream",
		)
		return err
	}); err != nil {
		t.Fatalf("seed exact-prefix objects: %v", err)
	}
	siblingObject := siblingPrefix + "keep.bin"
	if _, err := client.PutObject(
		ctx,
		siblingObject,
		bytes.NewReader([]byte("keep")),
		4,
		"application/octet-stream",
	); err != nil {
		t.Fatalf("seed sibling object: %v", err)
	}

	exactUploadID, err = client.NewMultipartUpload(
		ctx,
		prefix+"incomplete.bin",
		minioSDK.PutObjectOptions{},
	)
	if err != nil || exactUploadID == "" {
		t.Fatalf("seed exact incomplete upload: id=%q err=%v", exactUploadID, err)
	}
	siblingUploadID, err = client.NewMultipartUpload(
		ctx,
		siblingPrefix+"incomplete.bin",
		minioSDK.PutObjectOptions{},
	)
	if err != nil || siblingUploadID == "" {
		t.Fatalf(
			"seed sibling incomplete upload: id=%q err=%v",
			siblingUploadID,
			err,
		)
	}
	for _, upload := range []struct {
		key string
		id  string
	}{
		{key: prefix + "incomplete.bin", id: exactUploadID},
		{key: siblingPrefix + "incomplete.bin", id: siblingUploadID},
	} {
		if _, err := client.PutObjectPart(
			ctx,
			upload.key,
			upload.id,
			1,
			bytes.NewReader([]byte("part")),
			4,
			minioSDK.PutObjectPartOptions{},
		); err != nil {
			t.Fatalf("seed incomplete upload part %q: %v", upload.key, err)
		}
	}
	exactUploadListed := false
	seededUploads, err := client.listIncompleteUploads(ctx, prefix)
	if err != nil {
		t.Fatalf("list seeded incomplete upload: %v", err)
	}
	for _, upload := range seededUploads {
		if upload.UploadID == exactUploadID {
			exactUploadListed = true
		}
	}
	if !exactUploadListed {
		t.Fatal("seeded exact-prefix multipart upload was not listed")
	}

	deleteStarted := time.Now()
	if err := client.DeletePrefix(ctx, prefix); err != nil {
		t.Fatalf("DeletePrefix: %v", err)
	}
	t.Logf(
		"deleted %d objects plus one multipart upload in %s",
		len(objects),
		time.Since(deleteStarted).Round(time.Millisecond),
	)
	for object := range client.Client.ListObjects(
		ctx,
		bucket,
		minioSDK.ListObjectsOptions{
			Prefix:       prefix,
			Recursive:    true,
			WithVersions: true,
		},
	) {
		t.Fatalf("exact-prefix object remained: %#v", object)
	}
	remainingUploads, err := client.listIncompleteUploads(ctx, prefix)
	if err != nil {
		t.Fatalf("verify exact-prefix multipart uploads: %v", err)
	}
	if len(remainingUploads) > 0 {
		t.Fatalf("exact-prefix multipart upload remained: %#v", remainingUploads[0])
	}
	if _, err := client.StatObject(
		ctx,
		siblingObject,
		minioSDK.StatObjectOptions{},
	); err != nil {
		t.Fatalf("sibling object was removed: %v", err)
	}

	if _, err := client.ListObjectParts(
		ctx,
		prefix+"incomplete.bin",
		exactUploadID,
		0,
		1000,
	); err == nil {
		t.Fatal("exact-prefix multipart upload still accepts part listing")
	}
	if _, err := client.ListObjectParts(
		ctx,
		siblingPrefix+"incomplete.bin",
		siblingUploadID,
		0,
		1000,
	); err != nil {
		t.Fatalf("sibling multipart upload was removed: %v", err)
	}
	if err := client.DeletePrefix(ctx, siblingPrefix); err != nil {
		t.Fatalf("cleanup sibling prefix: %v", err)
	}
}
