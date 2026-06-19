package consumer

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	delightv1 "obs-svc/gen/go/delight/v1"
	"obs-svc/pkg/agg"
)

// frame mimics a producer encoding a first-message type in Confluent SR framing.
func frame(t *testing.T, schemaID int32, m proto.Message) []byte {
	t.Helper()
	payload, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out := []byte{0x00}
	var id [4]byte
	binary.BigEndian.PutUint32(id[:], uint32(schemaID))
	out = append(out, id[:]...)
	out = append(out, 0x00) // message-index: first message in file
	return append(out, payload...)
}

// TestRun_PingFailureLeavesUnhealthy verifies the cold-boot invariant: if the
// consumer cannot reach Kafka, Run returns the ping error and the aggregator is
// never marked healthy (so /health stays 503 and the daemon's backoff loop
// retries). We use an unroutable broker and a short deadline so the dial fails
// deterministically without a live cluster.
func TestRun_PingFailureLeavesUnhealthy(t *testing.T) {
	a := agg.New()
	ctx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
	defer cancel()

	// 127.0.0.1:1 is reserved/unroutable for a Kafka broker; Ping fails.
	err := Run(ctx, []string{"127.0.0.1:1"}, "obs-svc-agg-test", "delight.events", a)
	if err == nil {
		t.Fatal("expected Run to return an error when the broker is unreachable")
	}
	if a.Healthy() {
		t.Error("aggregator must stay UNHEALTHY when Kafka is unreachable")
	}
}

// fetchesWith builds a kgo.Fetches carrying the given records under one
// topic/partition, mirroring the shape PollFetches hands back.
func fetchesWith(topic string, recs ...*kgo.Record) kgo.Fetches {
	return kgo.Fetches{{
		Topics: []kgo.FetchTopic{{
			Topic:      topic,
			Partitions: []kgo.FetchPartition{{Partition: 0, Records: recs}},
		}},
	}}
}

func TestProcessFetches_FoldsRecords(t *testing.T) {
	a := agg.New()
	recs := []*kgo.Record{
		{Offset: 0, Value: frame(t, 9, &delightv1.BackupEvent{ProjectName: "paling", Success: true})},
		{Offset: 1, Value: frame(t, 9, &delightv1.BackupEvent{ProjectName: "paling", Success: false})},
		{Offset: 2, Value: []byte{0x01}}, // undecodable: skipped, not fatal
	}
	if err := processFetches(a, fetchesWith("delight.events", recs...)); err != nil {
		t.Fatalf("processFetches returned error: %v", err)
	}
	snap := a.Snapshot()
	if snap.TotalEvents != 2 {
		t.Errorf("total_events = %d, want 2 (poison pill must be skipped)", snap.TotalEvents)
	}
	p := snap.Backups["paling"]
	if p == nil || p.Successes != 1 || p.Failures != 1 {
		t.Errorf("paling rollup wrong: %+v", p)
	}
}

func TestProcessFetches_PropagatesFatalFetchError(t *testing.T) {
	a := agg.New()
	fetches := kgo.NewErrFetch(errFatal)
	if err := processFetches(a, fetches); err == nil {
		t.Error("expected a fetch transport error to propagate so the client is rebuilt")
	}
}

// errFatal stands in for a non-cancellation transport error on a partition.
var errFatal = context.DeadlineExceeded

func TestProcessFetches_IgnoresContextCancellation(t *testing.T) {
	a := agg.New()
	// A clean context cancellation on a partition is shutdown, not a fault: it
	// must not be returned as a fatal error.
	fetches := kgo.Fetches{{
		Topics: []kgo.FetchTopic{{
			Topic:      "delight.events",
			Partitions: []kgo.FetchPartition{{Partition: 0, Err: context.Canceled}},
		}},
	}}
	if err := processFetches(a, fetches); err != nil {
		t.Errorf("context cancellation must not be a fatal fetch error, got: %v", err)
	}
}

func TestHandleRecord_IngestsValidBackupEvent(t *testing.T) {
	a := agg.New()
	rec := &kgo.Record{
		Offset: 1,
		Value:  frame(t, 9, &delightv1.BackupEvent{ProjectName: "paling", Success: true, BytesAfter: 400, DurationMilliseconds: 5}),
	}
	handleRecord(a, rec)

	snap := a.Snapshot()
	if snap.TotalEvents != 1 {
		t.Fatalf("total_events = %d, want 1", snap.TotalEvents)
	}
	p := snap.Backups["paling"]
	if p == nil || p.Successes != 1 || p.LastBytesAfter != 400 {
		t.Fatalf("paling rollup wrong: %+v", p)
	}
}

func TestHandleRecord_SkipsUndecodableFrame(t *testing.T) {
	a := agg.New()
	// Too short / bad magic: sr.Decode rejects it; record is skipped, not panics.
	handleRecord(a, &kgo.Record{Offset: 2, Value: []byte{0x01, 0x02}})
	if a.Snapshot().TotalEvents != 0 {
		t.Error("an undecodable record must not be ingested")
	}
}

func TestHandleRecord_SkipsUnparseablePayload(t *testing.T) {
	a := agg.New()
	// Valid SR frame, but the payload is not a valid BackupEvent wire message.
	// Field 1 (project_name, string) with a length that overruns the buffer.
	bad := []byte{0x00, 0, 0, 0, 9, 0x00, 0x0a, 0x7f}
	handleRecord(a, &kgo.Record{Offset: 3, Value: bad})
	if a.Snapshot().TotalEvents != 0 {
		t.Error("an unparseable payload must not be ingested")
	}
}
