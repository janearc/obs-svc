// Package agg holds obs-svc's aggregated fleet state. Per the architecture
// record, ALL state lives in the daemon (the widget is stateless), and the
// daemon presents UNHEALTHY on a cold boot until it has wired up its inputs.
package agg

import (
	"sync"
	"time"

	delightv1 "obs-svc/gen/go/delight/v1"
	observabilityv1 "obs-svc/gen/go/observability/v1"
	"obs-svc/pkg/health"
)

// BackupStat is the rolling per-project view assembled from delight.v1
// BackupEvents.
type BackupStat struct {
	Project        string    `json:"project"`
	Total          int       `json:"total"`
	Successes      int       `json:"successes"`
	Failures       int       `json:"failures"`
	LastSuccess    bool      `json:"last_success"`
	LastBytesAfter uint64    `json:"last_bytes_after"`
	LastDurationMs uint32    `json:"last_duration_ms"`
	LastSeen       time.Time `json:"last_seen"`
}

// ServiceHealth is the debounced per-service view assembled from
// observability.v1.ServiceHealthHeartbeats. State is the output of the health
// state machine (with hysteresis), distinct from the raw last-reported state.
type ServiceHealth struct {
	Service        string    `json:"service"`
	State          string    `json:"state"`
	LastReported   string    `json:"last_reported"`
	UptimeSeconds  uint32    `json:"uptime_seconds"`
	LoadMetric     uint32    `json:"load_metric"`
	HeartbeatCount int       `json:"heartbeat_count"`
	LastSeen       time.Time `json:"last_seen"`
}

// FleetHealth is the rolled-up health of the service fleet, derived from the
// per-service debounced states.
type FleetHealth struct {
	Overall        string `json:"overall"`
	ActiveNodes    int    `json:"active_nodes"`
	DegradedNodes  int    `json:"degraded_nodes"`
	ExhaustedNodes int    `json:"exhausted_nodes"`
}

// Snapshot is the JSON view served at /state. It is a copy; callers never touch
// the live maps.
type Snapshot struct {
	Healthy     bool                      `json:"healthy"`
	TotalEvents int                       `json:"total_events"`
	Backups     map[string]*BackupStat    `json:"backups"`
	Services    map[string]*ServiceHealth `json:"services"`
	Fleet       FleetHealth               `json:"fleet"`
}

// serviceHealth pairs the live debouncing state machine with the rendered view
// served to clients. The machine carries the hysteresis counters; the view is
// the deep-copyable projection.
type serviceHealth struct {
	machine *health.Machine
	view    ServiceHealth
}

// Aggregator is the in-memory state, safe for concurrent reads (HTTP) and one
// writer (the consumer loop).
type Aggregator struct {
	mu          sync.RWMutex
	healthy     bool
	totalEvents int
	backups     map[string]*BackupStat
	services    map[string]*serviceHealth
	// greenStreak is the hysteresis threshold handed to each new service's
	// machine (consecutive greens to snap back to GREEN).
	greenStreak int
}

func New() *Aggregator {
	return NewWithHysteresis(health.DefaultGreenStreak)
}

// NewWithHysteresis constructs an aggregator whose per-service health machines
// require greenStreak consecutive GREEN heartbeats to recover to GREEN.
func NewWithHysteresis(greenStreak int) *Aggregator {
	return &Aggregator{
		backups:     make(map[string]*BackupStat),
		services:    make(map[string]*serviceHealth),
		greenStreak: greenStreak,
	}
}

// MarkHealthy flips the daemon out of its cold-boot UNHEALTHY state. Called once
// the consumer has connected to its inputs. (The architecture's full gate is a
// successful Traefik poll; that discovery step is a documented follow-up.)
func (a *Aggregator) MarkHealthy() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.healthy = true
}

func (a *Aggregator) Healthy() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.healthy
}

// IngestBackup folds one BackupEvent into the per-project rolling state.
func (a *Aggregator) IngestBackup(ev *delightv1.BackupEvent) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.totalEvents++
	s := a.backups[ev.GetProjectName()]
	if s == nil {
		s = &BackupStat{Project: ev.GetProjectName()}
		a.backups[ev.GetProjectName()] = s
	}
	s.Total++
	if ev.GetSuccess() {
		s.Successes++
	} else {
		s.Failures++
	}
	s.LastSuccess = ev.GetSuccess()
	s.LastBytesAfter = ev.GetBytesAfter()
	s.LastDurationMs = ev.GetDurationMilliseconds()
	if ts := ev.GetTimestamp(); ts != nil {
		s.LastSeen = ts.AsTime()
	} else {
		s.LastSeen = time.Now().UTC()
	}
}

// IngestHeartbeat folds one ServiceHealthHeartbeat through the per-service
// health state machine. The stored State is the debounced output (with
// hysteresis), not the raw reported state; LastReported preserves what the
// service actually claimed for observability.
func (a *Aggregator) IngestHeartbeat(hb *observabilityv1.ServiceHealthHeartbeat) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.totalEvents++
	name := hb.GetServiceName()
	sh := a.services[name]
	if sh == nil {
		sh = &serviceHealth{
			machine: health.NewMachine(a.greenStreak),
			view:    ServiceHealth{Service: name},
		}
		a.services[name] = sh
	}

	state := sh.machine.Observe(hb.GetCurrentState())
	sh.view.State = state.String()
	sh.view.LastReported = health.StateOf(hb.GetCurrentState()).String()
	sh.view.UptimeSeconds = hb.GetUptimeSeconds()
	sh.view.LoadMetric = hb.GetInternalLoadMetric()
	sh.view.HeartbeatCount++
	if ts := hb.GetTimestamp(); ts != nil {
		sh.view.LastSeen = ts.AsTime()
	} else {
		sh.view.LastSeen = time.Now().UTC()
	}
}

// Snapshot returns a deep copy of the current state for serialization.
func (a *Aggregator) Snapshot() Snapshot {
	a.mu.RLock()
	defer a.mu.RUnlock()

	backups := make(map[string]*BackupStat, len(a.backups))
	for k, v := range a.backups {
		cp := *v
		backups[k] = &cp
	}

	services := make(map[string]*ServiceHealth, len(a.services))
	for k, v := range a.services {
		cp := v.view
		services[k] = &cp
	}

	return Snapshot{
		Healthy:     a.healthy,
		TotalEvents: a.totalEvents,
		Backups:     backups,
		Services:    services,
		Fleet:       a.rollupFleet(),
	}
}

// rollupFleet derives the fleet-wide health from the per-service debounced
// states, read straight off the live machines (no string round-trip). Overall
// health is the worst severity observed across active services: any EXHAUSTED
// service drives overall to EXHAUSTED (terminal outranks RED), else the max of
// GREEN/YELLOW/RED, else UNSPECIFIED when no service has reported. Callers hold
// a.mu.
func (a *Aggregator) rollupFleet() FleetHealth {
	fh := FleetHealth{Overall: health.StateUnspecified.String()}
	worst := health.StateUnspecified
	for _, sh := range a.services {
		fh.ActiveNodes++
		switch sh.machine.State() {
		case health.StateExhausted:
			fh.ExhaustedNodes++
		case health.StateYellow, health.StateRed:
			fh.DegradedNodes++
		}
		if sh.machine.State().Severity() > worst.Severity() {
			worst = sh.machine.State()
		}
	}
	if fh.ActiveNodes > 0 {
		fh.Overall = worst.String()
	}
	return fh
}
