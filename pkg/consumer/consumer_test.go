package consumer

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	delightv1 "obs-svc/gen/go/delight/v1"
	observabilityv1 "obs-svc/gen/go/observability/v1"
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

func fetchesWith(recs ...*kgo.Record) kgo.Fetches {
	parts := map[string][]*kgo.Record{}
	for _, r := range recs {
		parts[r.Topic] = append(parts[r.Topic], r)
	}
	var topics []kgo.FetchTopic
	for topic, rs := range parts {
		topics = append(topics, kgo.FetchTopic{
			Topic:      topic,
			Partitions: []kgo.FetchPartition{{Partition: 0, Records: rs}},
		})
	}
	return kgo.Fetches{{Topics: topics}}
}

func TestRun_PingFailureLeavesUnhealthy(t *testing.T) {
	a := agg.New()
	ctx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
	defer cancel()
	err := Run(ctx, []string{"127.0.0.1:1"}, "obs-svc-agg-test", []string{TopicBackups}, a)
	if err == nil {
		t.Fatal("expected Run to error when the broker is unreachable")
	}
	if a.Healthy() {
		t.Error("aggregator must stay UNHEALTHY when Kafka is unreachable")
	}
}

func TestProcessFetches_DispatchesByTopic(t *testing.T) {
	a := agg.NewWithHysteresis(3)
	recs := []*kgo.Record{
		{Topic: TopicBackups, Offset: 0, Value: frame(t, 9, &delightv1.BackupEvent{ProjectName: "paling", Success: true})},
		{Topic: TopicHeartbeats, Offset: 0, Value: frame(t, 7, &observabilityv1.ServiceHealthHeartbeat{ServiceName: "delightd", CurrentState: observabilityv1.HealthState_HEALTH_STATE_GREEN})},
	}
	if err := processFetches(a, fetchesWith(recs...)); err != nil {
		t.Fatalf("processFetches: %v", err)
	}
	snap := a.Snapshot()
	if snap.Backups["paling"] == nil || snap.Backups["paling"].Successes != 1 {
		t.Errorf("backup not folded: %+v", snap.Backups)
	}
	if snap.Services["delightd"] == nil || snap.Services["delightd"].State != "GREEN" {
		t.Errorf("heartbeat not folded: %+v", snap.Services)
	}
}

func TestProcessFetches_PropagatesFatalFetchError(t *testing.T) {
	if err := processFetches(agg.New(), kgo.NewErrFetch(context.DeadlineExceeded)); err == nil {
		t.Error("expected a transport fetch error to propagate")
	}
}

func TestProcessFetches_IgnoresContextCancellation(t *testing.T) {
	fetches := kgo.Fetches{{Topics: []kgo.FetchTopic{{
		Topic:      TopicBackups,
		Partitions: []kgo.FetchPartition{{Partition: 0, Err: context.Canceled}},
	}}}}
	if err := processFetches(agg.New(), fetches); err != nil {
		t.Errorf("context cancellation must not be fatal, got: %v", err)
	}
}

func TestHandleRecord_SkipsUndecodableFrame(t *testing.T) {
	a := agg.New()
	handleRecord(a, &kgo.Record{Topic: TopicBackups, Offset: 1, Value: []byte{0x01, 0x02}})
	if a.Snapshot().TotalEvents != 0 {
		t.Error("undecodable record must not be ingested")
	}
}

func TestHandleRecord_SkipsUnmodeledTopic(t *testing.T) {
	a := agg.New()
	// A valid SR frame on a topic we do not model is skipped, not mis-parsed.
	handleRecord(a, &kgo.Record{Topic: "some.other.topic", Offset: 1, Value: frame(t, 1, &delightv1.BackupEvent{ProjectName: "x"})})
	if a.Snapshot().TotalEvents != 0 {
		t.Error("record from an unmodeled topic must not be ingested")
	}
}

func TestHandleBackup_SkipsUnparseable(t *testing.T) {
	a := agg.New()
	handleBackup(a, []byte{0x0a, 0x7f}, 0) // string field claims 0x7f bytes; overruns
	if a.Snapshot().TotalEvents != 0 {
		t.Error("unparseable BackupEvent must not be ingested")
	}
}

func TestHandleHeartbeat_SkipsUnparseable(t *testing.T) {
	a := agg.New()
	handleHeartbeat(a, []byte{0x0a, 0x7f}, 0)
	if a.Snapshot().TotalEvents != 0 {
		t.Error("unparseable heartbeat must not be ingested")
	}
}
