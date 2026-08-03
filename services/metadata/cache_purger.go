package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

type videoCachePurger interface {
	Purge(ctx context.Context, videoID string) error
}

type httpVideoCachePurger struct {
	baseURL string
	secret  string
	client  *http.Client
}

type cacheRevocationAck struct {
	VideoID            string `json:"video_id"`
	Revoked            bool   `json:"revoked"`
	State              string `json:"state"`
	RequiredGeneration uint64 `json:"required_generation"`
}

func newHTTPVideoCachePurger(
	rawBaseURL string,
	secret string,
	timeout time.Duration,
) (*httpVideoCachePurger, error) {
	parsed, err := url.Parse(strings.TrimSpace(rawBaseURL))
	if err != nil {
		return nil, fmt.Errorf("parse cache purger URL: %w", err)
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") ||
		parsed.Host == "" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("cache purger URL must be an HTTP(S) origin without credentials, query, or fragment")
	}
	if len(secret) < 32 {
		return nil, errors.New("NGINX_CACHE_PURGE_SECRET must contain at least 32 characters")
	}
	// Metadata's public HTTP server has a fixed 60s write timeout. Revocation
	// only writes the durable guard tombstone and enqueues asynchronous disk
	// convergence, so this internal call must remain comfortably below it.
	if timeout <= 0 || timeout > 30*time.Second {
		return nil, errors.New("cache purge timeout must be between 0 and 30s")
	}
	return &httpVideoCachePurger{
		baseURL: strings.TrimRight(parsed.String(), "/"),
		secret:  secret,
		client: &http.Client{
			Timeout: timeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

func (p *httpVideoCachePurger) Purge(ctx context.Context, videoID string) error {
	canonicalID, err := canonicalVideoID(videoID)
	if err != nil {
		return err
	}
	endpoint, err := url.Parse(p.baseURL)
	if err != nil {
		return err
	}
	endpoint.Path = path.Join(endpoint.Path, "v1/purge", canonicalID)

	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		endpoint.String(),
		nil,
	)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+p.secret)
	response, err := p.client.Do(request)
	if err != nil {
		return fmt.Errorf("call nginx cache purger: %w", err)
	}
	defer response.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(response.Body, 4<<10))
	if readErr != nil {
		return fmt.Errorf("read nginx cache purge response: %w", readErr)
	}
	if response.StatusCode != http.StatusAccepted {
		return fmt.Errorf(
			"nginx cache purger returned %s: %s",
			response.Status,
			strings.TrimSpace(string(body)),
		)
	}
	var acknowledgement cacheRevocationAck
	if err := json.Unmarshal(body, &acknowledgement); err != nil {
		return fmt.Errorf("decode nginx cache revocation acknowledgement: %w", err)
	}
	if acknowledgement.VideoID != canonicalID ||
		!acknowledgement.Revoked ||
		acknowledgement.RequiredGeneration == 0 {
		return fmt.Errorf(
			"nginx cache purger returned an invalid acknowledgement for %s",
			canonicalID,
		)
	}
	switch acknowledgement.State {
	case "pending", "converged", "retrying":
	default:
		return fmt.Errorf(
			"nginx cache purger returned invalid state %q",
			acknowledgement.State,
		)
	}
	return nil
}
