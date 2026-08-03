package main

import (
	"context"
	"log/slog"
	"time"

	"hls-engine/services/analytics/pkg/logger"
)

// StartAggregator runs background hourly and daily rollup loops. In a
// replicated deployment only the instance that holds the Redis lease performs
// work, so multiple analytics containers can be started safely.
func StartAggregator(ctx context.Context, store *Store, cfg Config) {
	go rollupLoop(ctx, store, cfg.RollupHourly, "analytics:rollup:hourly", time.Hour)
	go rollupLoop(ctx, store, cfg.RollupDaily, "analytics:rollup:daily", 24*time.Hour)
}

func rollupLoop(ctx context.Context, store *Store, interval time.Duration, leaseKey string, windowSize time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log := logger.L().With("component", "aggregator", "window", windowSize.String())

	// Run once at startup for the previous completed window.
	runRollup(ctx, store, leaseKey, windowSize, log)

	for {
		select {
		case <-ctx.Done():
			log.Info("aggregator stopped")
			return
		case <-ticker.C:
			runRollup(ctx, store, leaseKey, windowSize, log)
		}
	}
}

func runRollup(ctx context.Context, store *Store, leaseKey string, windowSize time.Duration, log *slog.Logger) {
	// TTL is twice the loop interval so transient crashes do not stall rollups.
	ok, err := store.AcquireLease(ctx, leaseKey, 2*windowSize)
	if err != nil || !ok {
		return
	}

	now := time.Now().UTC()
	end := now.Truncate(windowSize)
	start := end.Add(-windowSize)

	var rollupFn func(context.Context, time.Time, time.Time) error
	if windowSize == time.Hour {
		rollupFn = store.RollupHourly
	} else {
		rollupFn = store.RollupDaily
	}

	if err := rollupFn(ctx, start, end); err != nil {
		log.Error("rollup failed", "start", start, "end", end, "error", err)
		return
	}
	log.Info("rollup completed", "start", start, "end", end)
}
