package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	internalminio "hls-engine/internal/minio"
	"hls-engine/metadata/internal/repository"
)

var (
	errVideoResourceCleanup   = errors.New("video resource cleanup failed")
	errVideoDeletionRetryable = errors.New("video deletion can be retried")
)

type prefixDeleter interface {
	DeletePrefix(ctx context.Context, prefix string) error
}

type redisKeyDeleter interface {
	Delete(ctx context.Context, keys ...string) error
	DeleteMultipartSessionsForVideo(ctx context.Context, videoID string) error
}

type videoDeletionRepository interface {
	BeginVideoDeletion(
		ctx context.Context,
		id string,
	) (repository.VideoDeletion, error)
}

type videoDeleter interface {
	Delete(ctx context.Context, videoID string) error
}

type videoDeletionService struct {
	repo       videoDeletionRepository
	cache      videoCachePurger
	raw        prefixDeleter
	hls        prefixDeleter
	thumbnails prefixDeleter
	redis      redisKeyDeleter
}

type videoStoragePrefixes struct {
	Raw        string
	HLS        string
	Thumbnails string
}

func newVideoDeletionService(
	repo videoDeletionRepository,
	cache videoCachePurger,
	raw, hls, thumbnails *internalminio.Client,
	redis redisKeyDeleter,
) *videoDeletionService {
	return &videoDeletionService{
		repo:       repo,
		cache:      cache,
		raw:        raw,
		hls:        hls,
		thumbnails: thumbnails,
		redis:      redis,
	}
}

func (s *videoDeletionService) Delete(
	ctx context.Context,
	videoID string,
) (resultErr error) {
	canonicalID, err := canonicalVideoID(videoID)
	if err != nil {
		return repository.ErrNotFound
	}

	deletion, err := s.repo.BeginVideoDeletion(ctx, canonicalID)
	if err != nil {
		switch {
		case errors.Is(err, repository.ErrNotFound),
			errors.Is(err, repository.ErrVideoActive),
			errors.Is(err, repository.ErrVideoDeletionInProgress):
			return err
		default:
			return fmt.Errorf(
				"%w: commit deletion tombstone: %v",
				errVideoDeletionRetryable,
				err,
			)
		}
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if releaseErr := deletion.Release(releaseCtx); releaseErr != nil {
			wrapped := fmt.Errorf(
				"%w: release deletion lease: %v",
				errVideoDeletionRetryable,
				releaseErr,
			)
			if resultErr == nil {
				resultErr = wrapped
			} else {
				resultErr = errors.Join(resultErr, wrapped)
			}
		}
	}()

	if err := s.cleanupResources(ctx, canonicalID); err != nil {
		return fmt.Errorf(
			"%w: %w: %v",
			errVideoDeletionRetryable,
			errVideoResourceCleanup,
			err,
		)
	}
	if err := deletion.Finalize(ctx); err != nil {
		if errors.Is(err, repository.ErrVideoActive) {
			return err
		}
		return fmt.Errorf(
			"%w: finalize tombstoned video: %v",
			errVideoDeletionRetryable,
			err,
		)
	}
	return nil
}

func (s *videoDeletionService) cleanupResources(ctx context.Context, videoID string) error {
	prefixes, err := storagePrefixesForVideo(videoID)
	if err != nil {
		return err
	}

	keys := videoRedisKeys(videoID)

	// Revocation is the fail-closed boundary. The purger durably writes a
	// per-video tombstone and independently enqueues bounded background disk
	// convergence; nginx checks the tombstone before every cache lookup. Do not
	// remove origin objects until the durable acknowledgement succeeds:
	// otherwise a previously cached signed URL could remain playable.
	if err := s.cache.Purge(ctx, videoID); err != nil {
		return fmt.Errorf("purge nginx HLS cache: %w", err)
	}

	operations := []struct {
		name string
		run  func() error
	}{
		{
			name: "raw objects",
			run:  func() error { return s.raw.DeletePrefix(ctx, prefixes.Raw) },
		},
		{
			name: "HLS objects",
			run:  func() error { return s.hls.DeletePrefix(ctx, prefixes.HLS) },
		},
		{
			name: "thumbnail objects",
			run: func() error {
				return s.thumbnails.DeletePrefix(ctx, prefixes.Thumbnails)
			},
		},
		{
			name: "Redis state",
			run: func() error {
				keyErr := s.redis.Delete(ctx, keys...)
				sessionErr := s.redis.DeleteMultipartSessionsForVideo(
					ctx,
					videoID,
				)
				return errors.Join(keyErr, sessionErr)
			},
		},
	}

	failures := make([]error, len(operations))
	var wait sync.WaitGroup
	wait.Add(len(operations))
	for index := range operations {
		index := index
		go func() {
			defer wait.Done()
			if err := operations[index].run(); err != nil {
				failures[index] = fmt.Errorf(
					"delete %s: %w",
					operations[index].name,
					err,
				)
			}
		}()
	}
	wait.Wait()
	return errors.Join(failures...)
}

func canonicalVideoID(videoID string) (string, error) {
	parsed, err := uuid.Parse(strings.TrimSpace(videoID))
	if err != nil {
		return "", fmt.Errorf("invalid video ID: %w", err)
	}
	return parsed.String(), nil
}

func storagePrefixesForVideo(videoID string) (videoStoragePrefixes, error) {
	canonicalID, err := canonicalVideoID(videoID)
	if err != nil {
		return videoStoragePrefixes{}, err
	}
	shard := canonicalID[:2]
	return videoStoragePrefixes{
		Raw:        fmt.Sprintf("raw/%s/", canonicalID),
		HLS:        fmt.Sprintf("%s/%s/", shard, canonicalID),
		Thumbnails: fmt.Sprintf("%s/%s/", shard, canonicalID),
	}, nil
}

func videoRedisKeys(videoID string) []string {
	return []string{
		fmt.Sprintf("video:%s:progress", videoID),
		fmt.Sprintf("video:%s:version", videoID),
		fmt.Sprintf("video:%s:prefix", videoID),
	}
}
