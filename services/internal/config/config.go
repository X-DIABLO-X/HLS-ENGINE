package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds common service configuration.
type Config struct {
	ServiceName string
	HTTPPort    string
	LogLevel    string

	PostgresDSN     string
	PostgresMaxOpen int
	PostgresMaxIdle int

	RedisAddr     string
	RedisPassword string
	RedisDB       int

	MinioEndpoint  string
	MinioAccessKey string
	MinioSecretKey string
	MinioUseSSL    bool
	MinioBucket    string

	RabbitmqURL string

	JWTSecret     string
	JWTAccessTTL  time.Duration
	JWTRefreshTTL time.Duration
	JWTSegmentTTL time.Duration
	JWTHMACSecret []byte

	APIKeyHeader string
}

func Load() Config {
	return Config{
		ServiceName: getEnv("SERVICE_NAME", "hls-service"),
		HTTPPort:    getEnv("HTTP_PORT", "8080"),
		LogLevel:    getEnv("LOG_LEVEL", "info"),

		PostgresDSN:     getEnv("POSTGRES_DSN", "postgres://postgres:postgres@localhost:5432/hls?sslmode=disable"),
		PostgresMaxOpen: getEnvInt("POSTGRES_MAX_OPEN", 25),
		PostgresMaxIdle: getEnvInt("POSTGRES_MAX_IDLE", 5),

		RedisAddr:     getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword: getEnv("REDIS_PASSWORD", ""),
		RedisDB:       getEnvInt("REDIS_DB", 0),

		MinioEndpoint:  getEnv("MINIO_ENDPOINT", "localhost:9000"),
		MinioAccessKey: getEnv("MINIO_ACCESS_KEY", "minioadmin"),
		MinioSecretKey: getEnv("MINIO_SECRET_KEY", "minioadmin"),
		MinioUseSSL:    getEnvBool("MINIO_USE_SSL", false),
		MinioBucket:    getEnv("MINIO_BUCKET", "hls-videos"),

		RabbitmqURL: getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),

		JWTSecret:     getEnv("JWT_SECRET", "change-me-in-production"),
		JWTAccessTTL:  getEnvDuration("JWT_ACCESS_TTL", 24*time.Hour),
		JWTRefreshTTL: getEnvDuration("JWT_REFRESH_TTL", 7*24*time.Hour),
		JWTSegmentTTL: getEnvDuration("JWT_SEGMENT_TTL", 1*time.Hour),
		JWTHMACSecret: []byte(getEnv("JWT_HMAC_SECRET", "hls-segment-signing-secret")),

		APIKeyHeader: getEnv("API_KEY_HEADER", "X-API-Key"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func getEnvBool(key string, fallback bool) bool {
	v := strings.ToLower(os.Getenv(key))
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}
