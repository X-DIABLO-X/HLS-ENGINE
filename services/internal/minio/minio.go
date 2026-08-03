package minio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var ErrUnsafeDeletePrefix = errors.New("unsafe object delete prefix")

const (
	deleteObjectBatchSize = 1000
	deleteMaxWorkers      = 4
)

// Client wraps the MinIO SDK with helpers.
type Client struct {
	Client *minio.Client
	Core   *minio.Core
	Bucket string
}

func New(endpoint, accessKey, secretKey, bucket string, useSSL bool) (*Client, error) {
	mc, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("create minio client: %w", err)
	}
	core, err := minio.NewCore(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("create minio core client: %w", err)
	}
	return &Client{Client: mc, Core: core, Bucket: bucket}, nil
}

func (c *Client) EnsureBucket(ctx context.Context) error {
	exists, err := c.Client.BucketExists(ctx, c.Bucket)
	if err != nil {
		return fmt.Errorf("check bucket: %w", err)
	}
	if !exists {
		if err := c.Client.MakeBucket(ctx, c.Bucket, minio.MakeBucketOptions{}); err != nil {
			return fmt.Errorf("make bucket: %w", err)
		}
	}
	return nil
}

func (c *Client) PutObject(ctx context.Context, objectName string, reader io.Reader, size int64, contentType string) (int64, error) {
	info, err := c.Client.PutObject(ctx, c.Bucket, objectName, reader, size, minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		return 0, err
	}
	return info.Size, nil
}

func (c *Client) GetObject(ctx context.Context, objectName string, opts minio.GetObjectOptions) (*minio.Object, error) {
	return c.Client.GetObject(ctx, c.Bucket, objectName, opts)
}

func (c *Client) StatObject(ctx context.Context, objectName string, opts minio.StatObjectOptions) (minio.ObjectInfo, error) {
	return c.Client.StatObject(ctx, c.Bucket, objectName, opts)
}

func (c *Client) PresignedPutURL(ctx context.Context, objectName string, expiry time.Duration) (string, error) {
	u, err := c.Client.PresignedPutObject(ctx, c.Bucket, objectName, expiry)
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

func (c *Client) PresignedGetURL(ctx context.Context, objectName string, expiry time.Duration) (string, error) {
	u, err := c.Client.PresignedGetObject(ctx, c.Bucket, objectName, expiry, nil)
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

func (c *Client) NewMultipartUpload(ctx context.Context, objectName string, opts minio.PutObjectOptions) (string, error) {
	return c.Core.NewMultipartUpload(ctx, c.Bucket, objectName, opts)
}

func (c *Client) PutObjectPart(ctx context.Context, objectName, uploadID string, partID int, data io.Reader, size int64, opts minio.PutObjectPartOptions) (minio.ObjectPart, error) {
	return c.Core.PutObjectPart(ctx, c.Bucket, objectName, uploadID, partID, data, size, opts)
}

func (c *Client) ListObjectParts(ctx context.Context, objectName, uploadID string, partNumberMarker, maxParts int) (minio.ListObjectPartsResult, error) {
	return c.Core.ListObjectParts(ctx, c.Bucket, objectName, uploadID, partNumberMarker, maxParts)
}

func (c *Client) CompleteMultipartUpload(ctx context.Context, objectName, uploadID string, parts []minio.CompletePart, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	return c.Core.CompleteMultipartUpload(ctx, c.Bucket, objectName, uploadID, parts, opts)
}

func (c *Client) AbortMultipartUpload(ctx context.Context, objectName, uploadID string) error {
	return c.Core.AbortMultipartUpload(ctx, c.Bucket, objectName, uploadID)
}

// DeletePrefix permanently removes every object version below an exact,
// directory-like prefix. The prefix guard deliberately rejects bucket-wide
// and ambiguous values before MinIO is contacted.
func (c *Client) DeletePrefix(ctx context.Context, prefix string) error {
	if err := validateDeletePrefix(prefix); err != nil {
		return err
	}

	incompleteUploads, err := c.listIncompleteUploads(ctx, prefix)
	if err != nil {
		return err
	}
	if err := runBounded(
		incompleteUploads,
		deleteMaxWorkers,
		func(upload minio.ObjectMultipartInfo) error {
			if err := c.Core.AbortMultipartUpload(
				ctx,
				c.Bucket,
				upload.Key,
				upload.UploadID,
			); err != nil {
				return fmt.Errorf(
					"abort incomplete upload %q in bucket %q: %w",
					upload.Key,
					c.Bucket,
					err,
				)
			}
			return nil
		},
	); err != nil {
		return err
	}

	objectVersions := make([]minio.ObjectInfo, 0, deleteObjectBatchSize)
	objects := c.Client.ListObjects(ctx, c.Bucket, minio.ListObjectsOptions{
		Prefix:       prefix,
		Recursive:    true,
		WithVersions: true,
	})
	for object := range objects {
		if object.Err != nil {
			return fmt.Errorf("list bucket %q prefix %q: %w", c.Bucket, prefix, object.Err)
		}
		if !objectWithinDeletePrefix(prefix, object.Key) {
			return fmt.Errorf(
				"%w: listed object %q escaped prefix %q",
				ErrUnsafeDeletePrefix,
				object.Key,
				prefix,
			)
		}
		objectVersions = append(objectVersions, object)
	}

	batches := partitionDeleteBatches(objectVersions, deleteObjectBatchSize)
	if err := runBounded(
		batches,
		deleteMaxWorkers,
		func(batch []minio.ObjectInfo) error {
			objectsCh := make(chan minio.ObjectInfo, len(batch))
			for _, object := range batch {
				objectsCh <- object
			}
			close(objectsCh)

			var failures []error
			for removeErr := range c.Client.RemoveObjects(
				ctx,
				c.Bucket,
				objectsCh,
				minio.RemoveObjectsOptions{},
			) {
				failures = append(failures, fmt.Errorf(
					"remove object %q version %q from bucket %q: %w",
					removeErr.ObjectName,
					removeErr.VersionID,
					c.Bucket,
					removeErr.Err,
				))
			}
			return errors.Join(failures...)
		},
	); err != nil {
		return err
	}

	// A concurrent writer or a paginated-list race must keep the database row
	// intact. Verify the exact prefix is empty and let a later DELETE retry.
	remainingIncomplete, err := c.listIncompleteUploads(ctx, prefix)
	if err != nil {
		return fmt.Errorf("verify incomplete uploads: %w", err)
	}
	if len(remainingIncomplete) > 0 {
		return fmt.Errorf(
			"verify bucket %q prefix %q: incomplete upload %q remains",
			c.Bucket,
			prefix,
			remainingIncomplete[0].Key,
		)
	}

	remaining := c.Client.ListObjects(ctx, c.Bucket, minio.ListObjectsOptions{
		Prefix:       prefix,
		Recursive:    true,
		WithVersions: true,
	})
	for object := range remaining {
		if object.Err != nil {
			return fmt.Errorf("verify bucket %q prefix %q: %w", c.Bucket, prefix, object.Err)
		}
		if objectWithinDeletePrefix(prefix, object.Key) {
			return fmt.Errorf(
				"verify bucket %q prefix %q: object %q remains",
				c.Bucket,
				prefix,
				object.Key,
			)
		}
		return fmt.Errorf(
			"%w: verification object %q escaped prefix %q",
			ErrUnsafeDeletePrefix,
			object.Key,
			prefix,
		)
	}

	return nil
}

func (c *Client) listIncompleteUploads(
	ctx context.Context,
	prefix string,
) ([]minio.ObjectMultipartInfo, error) {
	// Some MinIO releases return an empty page when a ListMultipartUploads
	// prefix is supplied, even though a bucket-wide request returns the
	// uploads. List bucket-wide with the S3 maximum, paginate, then enforce
	// the exact directory boundary locally.
	const maxUploads = 1000
	var (
		uploads        []minio.ObjectMultipartInfo
		keyMarker      string
		uploadIDMarker string
	)
	for {
		result, err := c.Core.ListMultipartUploads(
			ctx,
			c.Bucket,
			"",
			keyMarker,
			uploadIDMarker,
			"",
			maxUploads,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"list incomplete uploads in bucket %q prefix %q: %w",
				c.Bucket,
				prefix,
				err,
			)
		}
		for _, upload := range result.Uploads {
			if objectWithinDeletePrefix(prefix, upload.Key) {
				uploads = append(uploads, upload)
			}
		}
		if !result.IsTruncated {
			return uploads, nil
		}
		if result.NextKeyMarker == keyMarker &&
			result.NextUploadIDMarker == uploadIDMarker {
			return nil, fmt.Errorf(
				"list incomplete uploads in bucket %q prefix %q did not advance",
				c.Bucket,
				prefix,
			)
		}
		keyMarker = result.NextKeyMarker
		uploadIDMarker = result.NextUploadIDMarker
	}
}

func partitionDeleteBatches(
	objects []minio.ObjectInfo,
	batchSize int,
) [][]minio.ObjectInfo {
	if batchSize <= 0 || len(objects) == 0 {
		return nil
	}
	batches := make([][]minio.ObjectInfo, 0, 1+(len(objects)-1)/batchSize)
	for start := 0; start < len(objects); start += batchSize {
		end := start + batchSize
		if end > len(objects) {
			end = len(objects)
		}
		batches = append(batches, objects[start:end])
	}
	return batches
}

func runBounded[T any](
	items []T,
	maxWorkers int,
	run func(T) error,
) error {
	if len(items) == 0 {
		return nil
	}
	if maxWorkers < 1 {
		maxWorkers = 1
	}
	if maxWorkers > len(items) {
		maxWorkers = len(items)
	}

	work := make(chan T)
	failures := make(chan error, len(items))
	var workers sync.WaitGroup
	workers.Add(maxWorkers)
	for worker := 0; worker < maxWorkers; worker++ {
		go func() {
			defer workers.Done()
			for item := range work {
				if err := run(item); err != nil {
					failures <- err
				}
			}
		}()
	}
	for _, item := range items {
		work <- item
	}
	close(work)
	workers.Wait()
	close(failures)

	var joined []error
	for err := range failures {
		joined = append(joined, err)
	}
	return errors.Join(joined...)
}

func validateDeletePrefix(prefix string) error {
	if prefix == "" ||
		strings.HasPrefix(prefix, "/") ||
		!strings.HasSuffix(prefix, "/") ||
		strings.Contains(prefix, `\`) {
		return fmt.Errorf("%w: %q", ErrUnsafeDeletePrefix, prefix)
	}

	trimmed := strings.TrimSuffix(prefix, "/")
	if path.Clean(trimmed) != trimmed {
		return fmt.Errorf("%w: %q", ErrUnsafeDeletePrefix, prefix)
	}

	segments := strings.Split(trimmed, "/")
	if len(segments) < 2 {
		return fmt.Errorf("%w: %q", ErrUnsafeDeletePrefix, prefix)
	}
	for _, segment := range segments {
		if segment == "" || segment == "." || segment == ".." {
			return fmt.Errorf("%w: %q", ErrUnsafeDeletePrefix, prefix)
		}
	}
	return nil
}

func objectWithinDeletePrefix(prefix, objectName string) bool {
	return strings.HasPrefix(objectName, prefix)
}
