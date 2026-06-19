// Package health is obs-svc-agg's per-service health state machine. It debounces
// the self-reported state on each ServiceHealthHeartbeat into a stable observed
// state, applying asymmetric hysteresis: degradation is immediate (a single bad
// heartbeat is believed), recovery toward GREEN requires a run of consecutive
// healthy heartbeats so a flapping service does not oscillate the fleet view.
//
// The machine is explicit, per the engineering standard that forbids sprawling
// if/else health logic: states are an enum, transitions are evaluated against a
// documented table, and EXHAUSTED is terminal (a fully-consumed quota does not
// recover on its own).
package health

import (
	observabilityv1 "obs-svc/gen/go/observability/v1"
)

// State is the debounced, observed health of one service. It mirrors the
// observability.v1.HealthState contract; we keep a local type so the state
// machine's invariants live in this package rather than leaking the generated
// enum's zero-value semantics across the codebase.
type State int

const (
	// StateUnspecified is the cold-boot state before any heartbeat is folded.
	StateUnspecified State = iota
	StateGreen
	StateYellow
	StateRed
	// StateExhausted is terminal: quota fully consumed. No heartbeat moves a
	// service out of it.
	StateExhausted
)

// DefaultGreenStreak is the hysteresis threshold: the number of consecutive
// GREEN heartbeats required to snap a degraded (YELLOW/RED) service back to
// GREEN. Degradation has no such gate — it is applied on the first heartbeat.
const DefaultGreenStreak = 3

// Severity orders states for worst-wins fleet rollups. EXHAUSTED (terminal)
// outranks RED; UNSPECIFIED is the floor.
func (s State) Severity() int {
	switch s {
	case StateGreen:
		return 1
	case StateYellow:
		return 2
	case StateRed:
		return 3
	case StateExhausted:
		return 4
	default:
		return 0
	}
}

func (s State) String() string {
	switch s {
	case StateGreen:
		return "GREEN"
	case StateYellow:
		return "YELLOW"
	case StateRed:
		return "RED"
	case StateExhausted:
		return "EXHAUSTED"
	default:
		return "UNSPECIFIED"
	}
}

// StateOf maps a wire HealthState to the local State, so callers can render the
// raw reported state with the same clean vocabulary as the debounced state
// (e.g. "GREEN", not the proto's "HEALTH_STATE_GREEN").
func StateOf(h observabilityv1.HealthState) State { return fromProto(h) }

// fromProto maps the wire enum to a local State. An UNSPECIFIED or unknown wire
// value is treated as UNSPECIFIED and handled as a no-op observation by Observe.
func fromProto(h observabilityv1.HealthState) State {
	switch h {
	case observabilityv1.HealthState_HEALTH_STATE_GREEN:
		return StateGreen
	case observabilityv1.HealthState_HEALTH_STATE_YELLOW:
		return StateYellow
	case observabilityv1.HealthState_HEALTH_STATE_RED:
		return StateRed
	case observabilityv1.HealthState_HEALTH_STATE_EXHAUSTED:
		return StateExhausted
	default:
		return StateUnspecified
	}
}

// Machine debounces heartbeats for a single service. It is not safe for
// concurrent use; the aggregator owns one Machine per service behind its lock.
type Machine struct {
	// state is the current debounced state.
	state State
	// greenStreak counts consecutive GREEN reports while the machine is
	// degraded; it drives the snap-back hysteresis.
	greenStreak int
	// greenThreshold is the consecutive-green count required to recover to GREEN.
	greenThreshold int
}

// NewMachine returns a cold-boot machine (UNSPECIFIED) using the given recovery
// hysteresis threshold. A threshold <= 0 falls back to DefaultGreenStreak so a
// misconfiguration cannot make recovery instantaneous (which would defeat the
// anti-flap purpose).
func NewMachine(greenThreshold int) *Machine {
	if greenThreshold <= 0 {
		greenThreshold = DefaultGreenStreak
	}
	return &Machine{state: StateUnspecified, greenThreshold: greenThreshold}
}

// State returns the current debounced state.
func (m *Machine) State() State { return m.state }

// Observe folds one heartbeat's self-reported state into the machine and returns
// the resulting debounced state. The transition rules, in evaluation order:
//
//  1. EXHAUSTED is terminal: once reached, every later heartbeat is a no-op.
//  2. A reported EXHAUSTED snaps to EXHAUSTED from any state (quota is gone;
//     there is nothing to debounce).
//  3. An UNSPECIFIED report is treated as missing data: it neither degrades nor
//     advances recovery, and it breaks any in-progress green streak so a gap in
//     healthy reports does not silently count toward recovery.
//  4. A GREEN report from an already-GREEN (or cold UNSPECIFIED) machine snaps
//     to GREEN immediately — there is nothing to debounce on the way up from
//     clean state.
//  5. A GREEN report from a degraded machine (YELLOW/RED) increments the green
//     streak; only once the streak reaches the threshold does it recover to
//     GREEN. The streak resets on recovery.
//  6. A YELLOW or RED report degrades immediately and resets the green streak;
//     bad news is believed at once.
func (m *Machine) Observe(reported observabilityv1.HealthState) State {
	want := fromProto(reported)

	// rule 1: terminal.
	if m.state == StateExhausted {
		return m.state
	}
	// rule 2: exhaustion is immediate and terminal.
	if want == StateExhausted {
		m.state = StateExhausted
		m.greenStreak = 0
		return m.state
	}
	// rule 3: no data -> hold state, break the recovery streak.
	if want == StateUnspecified {
		m.greenStreak = 0
		return m.state
	}
	// rule 4: clean -> GREEN with no debounce.
	if want == StateGreen && (m.state == StateGreen || m.state == StateUnspecified) {
		m.state = StateGreen
		m.greenStreak = 0
		return m.state
	}
	// rule 5: recovery from a degraded state requires the hysteresis streak.
	if want == StateGreen {
		m.greenStreak++
		if m.greenStreak >= m.greenThreshold {
			m.state = StateGreen
			m.greenStreak = 0
		}
		return m.state
	}
	// rule 6: degradation (YELLOW/RED) is immediate.
	m.state = want
	m.greenStreak = 0
	return m.state
}
