package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	delightv1 "obs-svc/gen/go/delight/v1"
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
