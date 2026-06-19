package agg

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	delightv1 "obs-svc/gen/go/delight/v1"
)

func TestAggregator_IngestUsesEventTimestamp(t *testing.T) {
	a := New()
	ts := time.Date(2026, 6, 19, 4, 24, 31, 0, time.UTC)
	a.IngestBackup(&delightv1.BackupEvent{
		ProjectName: "paling",
		Success:     true,
		Timestamp:   timestamppb.New(ts),
	})
	got := a.Snapshot().Backups["paling"].LastSeen
	if !got.Equal(ts) {
		t.Errorf("LastSeen = %s, want the event timestamp %s", got, ts)
	}
}

func TestAggregator_ColdBootUnhealthy(t *testing.T) {
	a := New()
	if a.Healthy() {
		t.Error("a freshly constructed aggregator must report UNHEALTHY")
	}
	a.MarkHealthy()
	if !a.Healthy() {
		t.Error("expected healthy after MarkHealthy")
	}
}

func TestAggregator_IngestBackupRollup(t *testing.T) {
	a := New()
	a.IngestBackup(&delightv1.BackupEvent{ProjectName: "paling", Success: true, BytesAfter: 100, DurationMilliseconds: 7})
	a.IngestBackup(&delightv1.BackupEvent{ProjectName: "paling", Success: false})
	a.IngestBackup(&delightv1.BackupEvent{ProjectName: "delightd", Success: true})

	snap := a.Snapshot()
	if snap.TotalEvents != 3 {
		t.Errorf("total_events = %d, want 3", snap.TotalEvents)
	}
	p := snap.Backups["paling"]
	if p == nil || p.Total != 2 || p.Successes != 1 || p.Failures != 1 {
		t.Fatalf("paling rollup wrong: %+v", p)
	}
	if p.LastSuccess {
		t.Error("paling last_success should reflect the most recent (failed) event")
	}
	if len(snap.Backups) != 2 {
		t.Errorf("expected 2 projects, got %d", len(snap.Backups))
	}
}

func TestSnapshot_IsACopy(t *testing.T) {
	a := New()
	a.IngestBackup(&delightv1.BackupEvent{ProjectName: "x", Success: true})
	snap := a.Snapshot()
	snap.Backups["x"].Total = 999 // mutate the copy
	if a.Snapshot().Backups["x"].Total != 1 {
		t.Error("Snapshot must return a copy; live state was mutated")
	}
}
