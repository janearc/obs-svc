package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	delightv1 "obs-svc/gen/go/delight/v1"
	observabilityv1 "obs-svc/gen/go/observability/v1"
	"obs-svc/pkg/agg"
)

func TestHealth_UnhealthyUntilMarked(t *testing.T) {
	a := agg.New()
	s := New(a)

	rr := httptest.NewRecorder()
	s.Mux().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("cold boot: code = %d, want 503", rr.Code)
	}

	a.MarkHealthy()
	rr = httptest.NewRecorder()
	s.Mux().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rr.Code != http.StatusOK {
		t.Errorf("after healthy: code = %d, want 200", rr.Code)
	}
}

func TestState_ReflectsIngest(t *testing.T) {
	a := agg.New()
	a.MarkHealthy()
	a.IngestBackup(&delightv1.BackupEvent{ProjectName: "paling", Success: true})
	s := New(a)

	rr := httptest.NewRecorder()
	s.Mux().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/state", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200", rr.Code)
	}
	var snap agg.Snapshot
	if err := json.Unmarshal(rr.Body.Bytes(), &snap); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !snap.Healthy || snap.TotalEvents != 1 || snap.Backups["paling"].Successes != 1 {
		t.Errorf("unexpected state: %+v", snap)
	}
}

func TestMetrics_Exposition(t *testing.T) {
	a := agg.New()
	a.MarkHealthy()
	a.IngestBackup(&delightv1.BackupEvent{ProjectName: "paling", Success: false})
	s := New(a)

	rr := httptest.NewRecorder()
	s.Mux().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rr.Body.String()
	if !strings.Contains(body, "obs_svc_healthy 1") {
		t.Errorf("missing healthy metric:\n%s", body)
	}
	if !strings.Contains(body, `obs_svc_backup_failures_total{project="paling"} 1`) {
		t.Errorf("missing failures metric:\n%s", body)
	}
}

func TestMetrics_HealthExposition(t *testing.T) {
	a := agg.NewWithHysteresis(3)
	a.MarkHealthy()
	a.IngestHeartbeat(&observabilityv1.ServiceHealthHeartbeat{
		ServiceName: "delightd", CurrentState: observabilityv1.HealthState_HEALTH_STATE_RED,
	})
	s := New(a)

	rr := httptest.NewRecorder()
	s.Mux().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rr.Body.String()

	for _, want := range []string{
		`obs_svc_service_health{service="delightd",state="RED"} 1`,
		`obs_svc_service_heartbeats_total{service="delightd"} 1`,
		`obs_svc_fleet_overall{state="RED"} 1`,
		"obs_svc_fleet_active_nodes 1",
		"obs_svc_fleet_degraded_nodes 1",
		"obs_svc_fleet_exhausted_nodes 0",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing metric %q in:\n%s", want, body)
		}
	}
}

func TestState_IncludesServicesAndFleet(t *testing.T) {
	a := agg.NewWithHysteresis(3)
	a.MarkHealthy()
	a.IngestHeartbeat(&observabilityv1.ServiceHealthHeartbeat{
		ServiceName: "paling", CurrentState: observabilityv1.HealthState_HEALTH_STATE_YELLOW,
	})
	s := New(a)

	rr := httptest.NewRecorder()
	s.Mux().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/state", nil))
	var snap agg.Snapshot
	if err := json.Unmarshal(rr.Body.Bytes(), &snap); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if snap.Services["paling"].State != "YELLOW" {
		t.Errorf("service state = %q, want YELLOW", snap.Services["paling"].State)
	}
	if snap.Fleet.Overall != "YELLOW" || snap.Fleet.DegradedNodes != 1 {
		t.Errorf("fleet rollup wrong: %+v", snap.Fleet)
	}
}
