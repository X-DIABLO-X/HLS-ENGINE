package main

import (
	"os"
	"time"
)

// Config is populated from environment variables.
type Config struct {
	Port          string
	DBURL         string
	RedisURL      string
	RedisPassword string
	LogLevel      string
	MetricsPath   string
	ReadTimeout   time.Duration
	WriteTimeout  time.Duration
	RollupHourly  time.Duration
	RollupDaily   time.Duration
}

func loadConfig() Config {
	return Config{
		Port:          getEnv("ANALYTICS_PORT", "8080"),
		DBURL:         getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/hlsengine?sslmode=disable"),
		RedisURL:      getEnv("REDIS_URL", "localhost:6379"),
		RedisPassword: getEnv("REDIS_PASSWORD", ""),
		LogLevel:      getEnv("LOG_LEVEL", "info"),
		MetricsPath:   getEnv("METRICS_PATH", "/metrics"),
		ReadTimeout:   getEnvDuration("ANALYTICS_READ_TIMEOUT", 5*time.Second),
		WriteTimeout:  getEnvDuration("ANALYTICS_WRITE_TIMEOUT", 10*time.Second),
		RollupHourly:  getEnvDuration("ANALYTICS_ROLLUP_HOURLY", 5*time.Minute),
		RollupDaily:   getEnvDuration("ANALYTICS_ROLLUP_DAILY", 1*time.Hour),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
