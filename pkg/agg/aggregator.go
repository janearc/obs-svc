// Package agg holds obs-svc's aggregated fleet state. Per the architecture
// record, ALL state lives in the daemon (the widget is stateless), and the
// daemon presents UNHEALTHY on a cold boot until it has wired up its inputs.
package agg

import (
	"sync"
	"time"

	delightv1 "obs-svc/gen/go/delight/v1"
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

// Snapshot is the JSON view served at /state. It is a copy; callers never touch
// the live maps.
type Snapshot struct {
	Healthy     bool                   `json:"healthy"`
	TotalEvents int                    `json:"total_events"`
	Backups     map[string]*BackupStat `json:"backups"`
}

// Aggregator is the in-memory state, safe for concurrent reads (HTTP) and one
// writer (the consumer loop).
type Aggregator struct {
	mu          sync.RWMutex
	healthy     bool
	totalEvents int
	backups     map[string]*BackupStat
}

func New() *Aggregator {
	return &Aggregator{backups: make(map[string]*BackupStat)}
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

// Snapshot returns a deep copy of the current state for serialization.
func (a *Aggregator) Snapshot() Snapshot {
	a.mu.RLock()
	defer a.mu.RUnlock()

	backups := make(map[string]*BackupStat, len(a.backups))
	for k, v := range a.backups {
		cp := *v
		backups[k] = &cp
	}
	return Snapshot{Healthy: a.healthy, TotalEvents: a.totalEvents, Backups: backups}
}
