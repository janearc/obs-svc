// Command obs-svc-agg is the aggregator daemon: it consumes Protobuf-over-Kafka
// fleet events, holds all aggregated state, and serves snapshots. Per the
// architecture record it boots UNHEALTHY and becomes healthy only once it has
// connected to its inputs.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"obs-svc/pkg/agg"
	"obs-svc/pkg/consumer"
	"obs-svc/pkg/httpapi"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	brokers := splitEnv("OBS_KAFKA_BROKERS", "kafka:9092")
	topics := splitEnv("OBS_KAFKA_TOPICS", consumer.TopicBackups+","+consumer.TopicHeartbeats)
	group := getEnv("OBS_CONSUMER_GROUP", "obs-svc-agg")
	addr := getEnv("OBS_HTTP_ADDR", ":8090")

	state := agg.New() // cold boot: UNHEALTHY until the consumer connects

	srv := &http.Server{Addr: addr, Handler: httpapi.New(state).Mux()}
	go func() {
		slog.Info("obs-svc-agg http listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server failed", "err", err)
		}
	}()

	// Consume with exponential backoff so a Kafka outage at cold boot self-heals
	// (Kafka replay on reconnect), per the idempotent-telemetry/backoff invariant.
	go func() {
		backoff := time.Second
		for ctx.Err() == nil {
			err := consumer.Run(ctx, brokers, group, topics, state)
			if err == nil || ctx.Err() != nil {
				return
			}
			slog.Error("consumer stopped; retrying", "err", err, "backoff", backoff)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func splitEnv(key, def string) []string {
	return strings.Split(getEnv(key, def), ",")
}
