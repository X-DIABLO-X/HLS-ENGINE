package hls

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	miniogo "github.com/minio/minio-go/v7"

	"hls-engine/internal/jwt"
	"hls-engine/internal/minio"
	ir "hls-engine/internal/redis"
)

// Service handles HLS manifest rewriting and segment validation.
type Service struct {
	minio   *minio.Client
	redis   *ir.Client
	secret  []byte
	baseURL string
}

func New(mc *minio.Client, rc *ir.Client, secret []byte, baseURL string) *Service {
	return &Service{
		minio:   mc,
		redis:   rc,
		secret:  secret,
		baseURL: strings.TrimSuffix(baseURL, "/"),
	}
}

// shard returns the first two characters of the video ID, lower-cased.
func shard(videoID string) string {
	if len(videoID) < 2 {
		return strings.ToLower(videoID)
	}
	return strings.ToLower(videoID[:2])
}

// currentVersion resolves the published version for a video.
// It checks Redis first, then falls back to v1.
func (s *Service) currentVersion(ctx context.Context, videoID string) string {
	if s.redis == nil {
		return "v1"
	}
	v, err := s.redis.Get(ctx, fmt.Sprintf("video:%s:version", videoID))
	if err != nil || v == "" {
		return "v1"
	}
	return v
}

// objectPrefix returns the MinIO object key prefix for the video.
func (s *Service) objectPrefix(ctx context.Context, videoID string) string {
	return fmt.Sprintf("%s/%s/%s/", shard(videoID), videoID, s.currentVersion(ctx, videoID))
}

// ValidateToken checks a signed token for the given path.
// Accepts exact-path tokens and prefix-scoped tokens.
func (s *Service) ValidateToken(path, token string) bool {
	_, ok := s.validateTokenExpiry(path, token)
	return ok
}

func (s *Service) validateTokenExpiry(path, token string) (time.Time, bool) {
	now := time.Now()
	expiry, ok := jwt.VerifyURL(s.secret, path, token, now)
	if ok {
		return expiry, true
	}
	return jwt.VerifyURLPrefix(s.secret, path, token, now)
}

// GenerateToken creates a signed token for a path valid until expiry.
func (s *Service) GenerateToken(path string, expiry time.Time) string {
	return jwt.BuildSignedToken(s.secret, path, expiry)
}

// RewriteMasterManifest fetches the master manifest and rewrites variant URLs with signed tokens.
func (s *Service) RewriteMasterManifest(ctx context.Context, videoID, token string, expiry time.Time) (string, string, error) {
	prefix := s.objectPrefix(ctx, videoID)
	objectName := prefix + "master.m3u8"
	basePath := "/hls/" + videoID + "/"

	inboundExpiry, ok := s.validateTokenExpiry(basePath, token)
	if token == "" || !ok {
		return "", "", fmt.Errorf("invalid token")
	}
	if expiry.After(inboundExpiry) {
		expiry = inboundExpiry
	}
	// Issue a single prefix token covering all resources for this video.
	scopedToken := jwt.BuildSignedTokenPrefix(s.secret, basePath, expiry)
	obj, err := s.minio.GetObject(ctx, objectName, miniogo.GetObjectOptions{})
	if err != nil {
		return "", "", err
	}
	defer obj.Close()

	contentType := "application/vnd.apple.mpegurl"
	var out bytes.Buffer
	scanner := bufio.NewScanner(obj)
	for scanner.Scan() {
		line := scanner.Text()
		// Rewrite URI="..." attributes inside tag lines (audio/subtitle media).
		if strings.HasPrefix(line, "#EXT-X-MEDIA:") {
			rewritten := rewriteMediaURI(line, videoID, s.baseURL, scopedToken)
			out.WriteString(rewritten + "\n")
			continue
		}
		if strings.HasPrefix(line, "#") || strings.TrimSpace(line) == "" {
			out.WriteString(line + "\n")
			continue
		}
		// line is a relative variant playlist path like video_720p/video.m3u8
		variantPath := path.Clean("/hls/" + videoID + "/" + line)
		rewritten := fmt.Sprintf("%s%s?token=%s", s.baseURL, variantPath, url.QueryEscape(scopedToken))
		out.WriteString(rewritten + "\n")
	}
	if err := scanner.Err(); err != nil {
		return "", "", err
	}
	return out.String(), contentType, nil
}

// rewriteMediaURI replaces the URI attribute inside an EXT-X-MEDIA tag with an
// absolute URL using the provided token.
func rewriteMediaURI(line, videoID, baseURL, token string) string {
	const prefix = "URI=\""
	idx := strings.Index(line, prefix)
	if idx == -1 {
		return line
	}
	start := idx + len(prefix)
	end := strings.Index(line[start:], "\"")
	if end == -1 {
		return line
	}
	end += start
	rel := line[start:end]
	mediaPath := path.Clean("/hls/" + videoID + "/" + rel)
	abs := fmt.Sprintf("%s%s?token=%s", baseURL, mediaPath, url.QueryEscape(token))
	return line[:start] + abs + line[end:]
}

// RewriteVariantManifest rewrites segment URLs inside a variant playlist.
func (s *Service) RewriteVariantManifest(ctx context.Context, videoID, variantPath, token string, expiry time.Time) (string, string, error) {
	prefix := s.objectPrefix(ctx, videoID)
	fullPath := "/hls/" + videoID + "/" + variantPath
	if token == "" || !s.ValidateToken(fullPath, token) {
		return "", "", fmt.Errorf("invalid token")
	}
	objectName := prefix + variantPath
	obj, err := s.minio.GetObject(ctx, objectName, miniogo.GetObjectOptions{})
	if err != nil {
		return "", "", err
	}
	defer obj.Close()

	// hls.js does not preserve query parameters when resolving relative segment
	// URLs against the variant URL, so embed absolute signed URLs directly.
	renditionDir := path.Dir(variantPath)
	var out bytes.Buffer
	scanner := bufio.NewScanner(obj)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#EXT-X-MAP:") {
			rewritten := rewriteMapURI(line, videoID, renditionDir, s.baseURL, token)
			out.WriteString(rewritten + "\n")
			continue
		}
		if strings.HasPrefix(line, "#") || strings.TrimSpace(line) == "" {
			out.WriteString(line + "\n")
			continue
		}
		segPath := path.Clean("/hls/" + videoID + "/" + renditionDir + "/" + line)
		abs := fmt.Sprintf("%s%s?token=%s", s.baseURL, segPath, url.QueryEscape(token))
		out.WriteString(abs + "\n")
	}
	if err := scanner.Err(); err != nil {
		return "", "", err
	}
	return out.String(), "application/vnd.apple.mpegurl", nil
}

// rewriteMapURI replaces the URI="..." attribute inside an EXT-X-MAP tag with an
// absolute signed URL.
func rewriteMapURI(line, videoID, renditionDir, baseURL, token string) string {
	const prefix = "URI=\""
	idx := strings.Index(line, prefix)
	if idx == -1 {
		return line
	}
	start := idx + len(prefix)
	end := strings.Index(line[start:], "\"")
	if end == -1 {
		return line
	}
	end += start
	rel := line[start:end]
	segPath := path.Clean("/hls/" + videoID + "/" + renditionDir + "/" + rel)
	abs := fmt.Sprintf("%s%s?token=%s", baseURL, segPath, url.QueryEscape(token))
	return line[:start] + abs + line[end:]
}

// ProxySegment proxies a segment from MinIO supporting Range headers.
func (s *Service) ProxySegment(ctx context.Context, w http.ResponseWriter, r *http.Request, videoID, segPath, token string) error {
	prefix := s.objectPrefix(ctx, videoID)
	fullPath := "/hls/" + videoID + "/" + segPath
	if token == "" || !s.ValidateToken(fullPath, token) {
		return fmt.Errorf("invalid segment token")
	}
	objectName := prefix + segPath
	info, err := s.minio.StatObject(ctx, objectName, miniogo.StatObjectOptions{})
	if err != nil {
		return err
	}

	opts := miniogo.GetObjectOptions{}
	status := http.StatusOK
	contentLength := info.Size
	if rng := r.Header.Get("Range"); rng != "" {
		start, end, ok := parseRange(rng, info.Size)
		if !ok {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", info.Size))
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return nil
		}
		if err := opts.SetRange(start, end); err != nil {
			return err
		}
		status = http.StatusPartialContent
		contentLength = end - start + 1
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, info.Size))
	}
	obj, err := s.minio.GetObject(ctx, objectName, opts)
	if err != nil {
		return err
	}
	defer obj.Close()

	w.Header().Set("Content-Type", info.ContentType)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Cache-Control", "private, max-age=300")
	w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
	w.WriteHeader(status)
	_, err = io.Copy(w, obj)
	return err
}

func parseRange(rng string, size int64) (int64, int64, bool) {
	if size <= 0 {
		return 0, 0, false
	}
	rng = strings.TrimSpace(rng)
	if !strings.HasPrefix(rng, "bytes=") {
		return 0, 0, false
	}
	rng = strings.TrimPrefix(rng, "bytes=")
	if strings.Contains(rng, ",") {
		return 0, 0, false
	}
	parts := strings.SplitN(rng, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}

	if parts[0] == "" {
		suffixLength, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil || suffixLength <= 0 {
			return 0, 0, false
		}
		if suffixLength > size {
			suffixLength = size
		}
		return size - suffixLength, size - 1, true
	}

	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || start < 0 || start >= size {
		return 0, 0, false
	}
	end := size - 1
	if parts[1] != "" {
		end, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil || end < start {
			return 0, 0, false
		}
		if end >= size {
			end = size - 1
		}
	}
	return start, end, true
}
