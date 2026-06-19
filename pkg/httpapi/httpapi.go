// Package httpapi is obs-svc-agg's read surface: liveness, prometheus metrics,
// and the aggregated state snapshot. (The widget will consume a 2s gRPC feed per
// the architecture record; this JSON surface is the inspectable equivalent for
// now and for debugging.)
package httpapi

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"obs-svc/pkg/agg"
)

type Server struct {
	agg *agg.Aggregator
}

func New(a *agg.Aggregator) *Server { return &Server{agg: a} }

func (s *Server) Mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	mux.HandleFunc("GET /state", s.handleState)
	return mux
}

// handleHealth returns 200 once healthy, 503 during the cold-boot UNHEALTHY
// window -- so the fleet (and Traefik) can gate on a genuine readiness signal.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if !s.agg.Healthy() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unhealthy"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.agg.Snapshot())
}

// handleMetrics emits a minimal Prometheus exposition derived from the snapshot.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	snap := s.agg.Snapshot()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	healthy := 0
	if snap.Healthy {
		healthy = 1
	}
	fmt.Fprintf(w, "obs_svc_healthy %d\n", healthy)
	fmt.Fprintf(w, "obs_svc_events_total %d\n", snap.TotalEvents)
	for project, st := range snap.Backups {
		fmt.Fprintf(w, "obs_svc_backups_total{project=%q} %d\n", project, st.Total)
		fmt.Fprintf(w, "obs_svc_backup_failures_total{project=%q} %d\n", project, st.Failures)
	}

	// Per-service debounced health: the state label is the state-machine output
	// (post-hysteresis), so a flapping service does not flicker the series.
	for service, sh := range snap.Services {
		fmt.Fprintf(w, "obs_svc_service_health{service=%q,state=%q} 1\n", service, sh.State)
		fmt.Fprintf(w, "obs_svc_service_heartbeats_total{service=%q} %d\n", service, sh.HeartbeatCount)
	}
	// Fleet rollup.
	fmt.Fprintf(w, "obs_svc_fleet_overall{state=%q} 1\n", snap.Fleet.Overall)
	fmt.Fprintf(w, "obs_svc_fleet_active_nodes %d\n", snap.Fleet.ActiveNodes)
	fmt.Fprintf(w, "obs_svc_fleet_degraded_nodes %d\n", snap.Fleet.DegradedNodes)
	fmt.Fprintf(w, "obs_svc_fleet_exhausted_nodes %d\n", snap.Fleet.ExhaustedNodes)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		slog.Error("failed to encode response", "error", err)
	}
}
