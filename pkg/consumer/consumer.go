// Package consumer ingests fleet events from Kafka into the aggregator. It is
// the read side of the fleet's Confluent-SR protobuf convention (producers like
// delightd are the write side).
package consumer

import (
	"context"
	"errors"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	delightv1 "obs-svc/gen/go/delight/v1"
	observabilityv1 "obs-svc/gen/go/observability/v1"
	"obs-svc/pkg/agg"
	"obs-svc/pkg/sr"
)

// Topic names are the fleet's locked Kafka naming. The record type is determined
// by the topic (the SR framing carries the schema id, but obs-svc-agg resolves
// the Go type from the topic rather than a registry lookup; a registry-driven
// resolver is a documented follow-up).
const (
	TopicBackups    = "delight.events"
	TopicHeartbeats = "observability.heartbeat"
)

// Run consumes the given topics and folds each record into the aggregator,
// dispatching by topic to the right message type. Auto-commit is disabled and
// offsets are committed only AFTER a fetch is processed, per the architecture's
// "commit after successful processing" rule, so a crash mid-batch replays rather
// than loses. Blocks until ctx is cancelled or the client errors.
func Run(ctx context.Context, brokers []string, group string, topics []string, a *agg.Aggregator) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		// Cold boot relies on Kafka replay: a new group reads from the start of
		// retained history rather than only new events. Once the group has
		// committed offsets, it resumes from those.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return err
	}
	defer cl.Close()

	if err := cl.Ping(ctx); err != nil {
		return err
	}
	// Connected to our input -> leave the cold-boot UNHEALTHY state.
	a.MarkHealthy()
	slog.Info("obs-svc-agg consuming", "brokers", brokers, "topics", topics, "group", group)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if err := processFetches(a, fetches); err != nil {
			return err
		}

		// Commit only after the batch is folded into state.
		if err := cl.CommitUncommittedOffsets(ctx); err != nil {
			slog.Error("offset commit failed", "err", err)
		}
	}
}

// processFetches folds one poll's worth of records into state. A transport fetch
// error (other than a clean context cancellation) is returned so the caller can
// tear the client down and let the daemon's backoff loop reconnect; per-record
// decode failures are handled and swallowed by handleRecord so a poison pill
// does not stall the partition. Separated from Run so the fold/error semantics
// are unit-testable without a live broker.
func processFetches(a *agg.Aggregator, fetches kgo.Fetches) error {
	var fatal error
	fetches.EachError(func(t string, p int32, e error) {
		if errors.Is(e, context.Canceled) {
			return
		}
		slog.Error("fetch error", "topic", t, "partition", p, "err", e)
		fatal = e
	})
	if fatal != nil {
		return fatal
	}

	fetches.EachRecord(func(rec *kgo.Record) { handleRecord(a, rec) })
	return nil
}

// handleRecord decodes one record and dispatches it by topic to the right
// message type. A bad record is logged and skipped (and will be committed past)
// rather than stalling the partition; durable dead-lettering is a documented
// follow-up.
func handleRecord(a *agg.Aggregator, rec *kgo.Record) {
	frame, err := sr.Decode(rec.Value)
	if err != nil {
		slog.Warn("skipping undecodable record", "topic", rec.Topic, "offset", rec.Offset, "err", err)
		return
	}
	switch rec.Topic {
	case TopicHeartbeats:
		handleHeartbeat(a, frame.Payload, rec.Offset)
	case TopicBackups:
		handleBackup(a, frame.Payload, rec.Offset)
	default:
		// An unsubscribed topic should be impossible, but a record from a topic
		// we do not model is skipped rather than mis-parsed.
		slog.Warn("skipping record from unmodeled topic", "topic", rec.Topic, "offset", rec.Offset)
	}
}

func handleBackup(a *agg.Aggregator, payload []byte, offset int64) {
	var ev delightv1.BackupEvent
	if err := proto.Unmarshal(payload, &ev); err != nil {
		slog.Warn("skipping unparseable BackupEvent", "offset", offset, "err", err)
		return
	}
	a.IngestBackup(&ev)
	slog.Info("ingested backup event",
		"project", ev.GetProjectName(),
		"success", ev.GetSuccess(),
		"bytes_after", ev.GetBytesAfter(),
		"duration_ms", ev.GetDurationMilliseconds(),
	)
}

func handleHeartbeat(a *agg.Aggregator, payload []byte, offset int64) {
	var hb observabilityv1.ServiceHealthHeartbeat
	if err := proto.Unmarshal(payload, &hb); err != nil {
		slog.Warn("skipping unparseable ServiceHealthHeartbeat", "offset", offset, "err", err)
		return
	}
	a.IngestHeartbeat(&hb)
	slog.Info("ingested heartbeat",
		"service", hb.GetServiceName(),
		"reported_state", hb.GetCurrentState().String(),
	)
}
