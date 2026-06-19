package agg

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	delightv1 "obs-svc/gen/go/delight/v1"
	observabilityv1 "obs-svc/gen/go/observability/v1"
)

func hb(service string, state observabilityv1.HealthState) *observabilityv1.ServiceHealthHeartbeat {
	return &observabilityv1.ServiceHealthHeartbeat{ServiceName: service, CurrentState: state}
}

func TestAggregator_IngestUsesEventTimestamp(t *testing.T) {
	a := New()
	ts := time.Date(2026, 6, 19, 4, 24, 31, 0, time.UTC)
	a.IngestBackup(&delightv1.BackupEvent{ProjectName: "paling", Success: true, Timestamp: timestamppb.New(ts)})
	if got := a.Snapshot().Backups["paling"].LastSeen; !got.Equal(ts) {
		t.Errorf("LastSeen = %s, want %s", got, ts)
	}
}

func TestAggregator_IngestHeartbeatDebouncesState(t *testing.T) {
	a := NewWithHysteresis(3)
	a.IngestHeartbeat(hb("delightd", observabilityv1.HealthState_HEALTH_STATE_RED))
	// One green report must NOT recover with a threshold of 3; the debounced
	// state stays RED while last_reported shows what the service actually said.
	a.IngestHeartbeat(hb("delightd", observabilityv1.HealthState_HEALTH_STATE_GREEN))

	sh := a.Snapshot().Services["delightd"]
	if sh == nil || sh.State != "RED" {
		t.Fatalf("debounced state = %v, want RED", sh)
	}
	if sh.LastReported != "GREEN" {
		t.Errorf("last_reported = %q, want GREEN", sh.LastReported)
	}
	if sh.HeartbeatCount != 2 {
		t.Errorf("heartbeat_count = %d, want 2", sh.HeartbeatCount)
	}
}

func TestAggregator_HeartbeatUsesTimestampElseNow(t *testing.T) {
	a := New()
	ts := time.Date(2026, 6, 19, 5, 0, 0, 0, time.UTC)
	a.IngestHeartbeat(&observabilityv1.ServiceHealthHeartbeat{
		ServiceName: "paling", CurrentState: observabilityv1.HealthState_HEALTH_STATE_GREEN,
		UptimeSeconds: 42, InternalLoadMetric: 7, Timestamp: timestamppb.New(ts),
	})
	sh := a.Snapshot().Services["paling"]
	if !sh.LastSeen.Equal(ts) || sh.UptimeSeconds != 42 || sh.LoadMetric != 7 {
		t.Errorf("heartbeat fields wrong: %+v", sh)
	}

	// No timestamp -> falls back to now (just assert it is set).
	a.IngestHeartbeat(hb("nots", observabilityv1.HealthState_HEALTH_STATE_GREEN))
	if a.Snapshot().Services["nots"].LastSeen.IsZero() {
		t.Error("missing-timestamp heartbeat must default LastSeen to now")
	}
}

func TestAggregator_FleetRollupWorstWins(t *testing.T) {
	a := NewWithHysteresis(3)
	a.IngestHeartbeat(hb("a", observabilityv1.HealthState_HEALTH_STATE_GREEN))
	a.IngestHeartbeat(hb("b", observabilityv1.HealthState_HEALTH_STATE_YELLOW))
	a.IngestHeartbeat(hb("c", observabilityv1.HealthState_HEALTH_STATE_RED))

	fleet := a.Snapshot().Fleet
	if fleet.Overall != "RED" {
		t.Errorf("overall = %q, want RED (worst wins)", fleet.Overall)
	}
	if fleet.ActiveNodes != 3 || fleet.DegradedNodes != 2 {
		t.Errorf("fleet counts wrong: %+v", fleet)
	}

	// An exhausted service is terminal and outranks RED.
	a.IngestHeartbeat(hb("d", observabilityv1.HealthState_HEALTH_STATE_EXHAUSTED))
	fleet = a.Snapshot().Fleet
	if fleet.Overall != "EXHAUSTED" || fleet.ExhaustedNodes != 1 {
		t.Errorf("after exhaustion: %+v, want overall EXHAUSTED", fleet)
	}
}

func TestAggregator_FleetRollupEmptyIsUnspecified(t *testing.T) {
	if got := New().Snapshot().Fleet.Overall; got != "UNSPECIFIED" {
		t.Errorf("empty fleet overall = %q, want UNSPECIFIED", got)
	}
}

func TestAggregator_ServicesSnapshotIsACopy(t *testing.T) {
	a := New()
	a.IngestHeartbeat(hb("x", observabilityv1.HealthState_HEALTH_STATE_GREEN))
	snap := a.Snapshot()
	snap.Services["x"].State = "MUTATED"
	if a.Snapshot().Services["x"].State != "GREEN" {
		t.Error("Snapshot must deep-copy services; live state was mutated")
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
