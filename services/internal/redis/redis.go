package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	multipartSessionPrefix    = "upload:multipart:"
	multipartSessionScanCount = int64(128)
	redisDeleteBatchSize      = 256
)

// Client wraps go-redis Client with common helpers.
type Client struct {
	client *redis.Client
}

func New(addr, password string, db int) *Client {
	return &Client{
		client: redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: password,
			DB:       db,
		}),
	}
}

func (c *Client) Close() error {
	return c.client.Close()
}

func (c *Client) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

func (c *Client) Get(ctx context.Context, key string) (string, error) {
	return c.client.Get(ctx, key).Result()
}

func (c *Client) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	return c.client.Set(ctx, key, value, ttl).Err()
}

func (c *Client) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	return c.client.SetNX(ctx, key, value, ttl).Result()
}

func (c *Client) Delete(ctx context.Context, keys ...string) error {
	return c.client.Del(ctx, keys...).Err()
}

// DeleteMultipartSessionsForVideo removes completed and incomplete multipart
// session state for a tombstoned video. Session records normally live for 24
// hours so clients can safely replay completion after a lost response. Once a
// video is being deleted that replay contract is no longer useful, and the
// video mutation lease guarantees no target session can be created or updated
// while this incremental SCAN is running.
func (c *Client) DeleteMultipartSessionsForVideo(
	ctx context.Context,
	videoID string,
) error {
	parsedVideoID, err := uuid.Parse(videoID)
	if err != nil || parsedVideoID.String() != videoID {
		return fmt.Errorf("invalid canonical video ID %q", videoID)
	}

	keys := make([]string, 0, 2)
	iterator := c.client.Scan(
		ctx,
		0,
		multipartSessionPrefix+"*",
		multipartSessionScanCount,
	).Iterator()
	for iterator.Next(ctx) {
		key := iterator.Val()
		if !isMultipartSessionRecordKey(key) {
			continue
		}

		value, err := c.client.Get(ctx, key).Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return fmt.Errorf("read multipart session %q: %w", key, err)
		}
		recordVideoID, err := multipartSessionVideoID(value)
		if err != nil {
			return fmt.Errorf("decode multipart session %q: %w", key, err)
		}
		if recordVideoID != videoID {
			continue
		}
		keys = append(keys, key, key+":completion-lock")
	}
	if err := iterator.Err(); err != nil {
		return fmt.Errorf("scan multipart sessions: %w", err)
	}

	for start := 0; start < len(keys); start += redisDeleteBatchSize {
		end := min(start+redisDeleteBatchSize, len(keys))
		if err := c.client.Del(ctx, keys[start:end]...).Err(); err != nil {
			return fmt.Errorf("delete multipart sessions: %w", err)
		}
	}
	return nil
}

func isMultipartSessionRecordKey(key string) bool {
	sessionID, ok := strings.CutPrefix(key, multipartSessionPrefix)
	if !ok {
		return false
	}
	parsed, err := uuid.Parse(sessionID)
	return err == nil && parsed.String() == sessionID
}

func multipartSessionVideoID(value string) (string, error) {
	var record struct {
		VideoID string `json:"video_id"`
	}
	if err := json.Unmarshal([]byte(value), &record); err != nil {
		return "", err
	}
	parsed, err := uuid.Parse(record.VideoID)
	if err != nil || parsed.String() != record.VideoID {
		return "", errors.New("record has invalid video_id")
	}
	return record.VideoID, nil
}

func (c *Client) BlacklistToken(ctx context.Context, tokenID string, ttl time.Duration) error {
	key := fmt.Sprintf("token:blacklist:%s", tokenID)
	return c.client.Set(ctx, key, "1", ttl).Err()
}

func (c *Client) IsTokenBlacklisted(ctx context.Context, tokenID string) (bool, error) {
	key := fmt.Sprintf("token:blacklist:%s", tokenID)
	n, err := c.client.Exists(ctx, key).Result()
	return n > 0, err
}

func (c *Client) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return c.client.HGetAll(ctx, key).Result()
}

func (c *Client) HSet(ctx context.Context, key string, values ...interface{}) error {
	return c.client.HSet(ctx, key, values...).Err()
}

func (c *Client) Client() *redis.Client {
	return c.client
}
