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
	"obs-svc/pkg/agg"
	"obs-svc/pkg/sr"
)

// Run consumes `topic` and folds each delight.v1.BackupEvent into the
// aggregator. Auto-commit is disabled and offsets are committed only AFTER a
// fetch is processed, per the architecture's "commit after successful
// processing" rule, so a crash mid-batch replays rather than loses. Blocks until
// ctx is cancelled or the client errors.
func Run(ctx context.Context, brokers []string, group, topic string, a *agg.Aggregator) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
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
	slog.Info("obs-svc-agg consuming", "brokers", brokers, "topic", topic, "group", group)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
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

		// Commit only after the batch is folded into state.
		if err := cl.CommitUncommittedOffsets(ctx); err != nil {
			slog.Error("offset commit failed", "err", err)
		}
	}
}

// handleRecord decodes one record. A bad record is logged and skipped (and will
// be committed past) rather than stalling the partition; durable dead-lettering
// is a documented follow-up.
func handleRecord(a *agg.Aggregator, rec *kgo.Record) {
	frame, err := sr.Decode(rec.Value)
	if err != nil {
		slog.Warn("skipping undecodable record", "offset", rec.Offset, "err", err)
		return
	}
	var ev delightv1.BackupEvent
	if err := proto.Unmarshal(frame.Payload, &ev); err != nil {
		slog.Warn("skipping unparseable BackupEvent", "offset", rec.Offset, "err", err)
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
