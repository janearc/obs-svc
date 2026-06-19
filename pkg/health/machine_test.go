package health

import (
	"testing"

	observabilityv1 "obs-svc/gen/go/observability/v1"
)

const (
	unspecified = observabilityv1.HealthState_HEALTH_STATE_UNSPECIFIED
	green       = observabilityv1.HealthState_HEALTH_STATE_GREEN
	yellow      = observabilityv1.HealthState_HEALTH_STATE_YELLOW
	red         = observabilityv1.HealthState_HEALTH_STATE_RED
	exhausted   = observabilityv1.HealthState_HEALTH_STATE_EXHAUSTED
)

func TestMachine_ColdBootIsUnspecified(t *testing.T) {
	m := NewMachine(3)
	if m.State() != StateUnspecified {
		t.Errorf("cold boot state = %s, want UNSPECIFIED", m.State())
	}
}

func TestMachine_FirstGreenSnapsToGreen(t *testing.T) {
	m := NewMachine(3)
	// From clean (UNSPECIFIED) there is nothing to debounce on the way up.
	if got := m.Observe(green); got != StateGreen {
		t.Errorf("first green = %s, want GREEN", got)
	}
}

func TestMachine_DegradationIsImmediate(t *testing.T) {
	m := NewMachine(3)
	m.Observe(green)
	if got := m.Observe(yellow); got != StateYellow {
		t.Errorf("yellow report = %s, want immediate YELLOW", got)
	}
	if got := m.Observe(red); got != StateRed {
		t.Errorf("red report = %s, want immediate RED", got)
	}
}

func TestMachine_RecoveryRequiresHysteresis(t *testing.T) {
	m := NewMachine(3)
	m.Observe(red) // degrade

	// Two greens are not enough to recover with a threshold of 3.
	if got := m.Observe(green); got != StateRed {
		t.Errorf("1st green: state = %s, want still RED", got)
	}
	if got := m.Observe(green); got != StateRed {
		t.Errorf("2nd green: state = %s, want still RED", got)
	}
	// The third consecutive green snaps back.
	if got := m.Observe(green); got != StateGreen {
		t.Errorf("3rd green: state = %s, want GREEN", got)
	}
}

func TestMachine_DegradationDuringRecoveryResetsStreak(t *testing.T) {
	m := NewMachine(3)
	m.Observe(red)
	m.Observe(green) // streak = 1
	m.Observe(green) // streak = 2
	if got := m.Observe(yellow); got != StateYellow {
		t.Errorf("yellow mid-recovery = %s, want YELLOW", got)
	}
	// Streak must have reset: it now takes three fresh greens again.
	m.Observe(green)
	m.Observe(green)
	if got := m.State(); got != StateYellow {
		t.Errorf("after 2 greens post-reset = %s, want still YELLOW", got)
	}
	if got := m.Observe(green); got != StateGreen {
		t.Errorf("3rd green post-reset = %s, want GREEN", got)
	}
}

func TestMachine_UnspecifiedReportHoldsStateAndBreaksStreak(t *testing.T) {
	m := NewMachine(3)
	m.Observe(red)
	m.Observe(green) // streak = 1
	m.Observe(green) // streak = 2
	// A gap (no data) must not count toward recovery and must break the streak.
	if got := m.Observe(unspecified); got != StateRed {
		t.Errorf("unspecified report = %s, want held at RED", got)
	}
	if got := m.Observe(green); got != StateRed {
		t.Errorf("green after gap = %s, want RED (streak was reset)", got)
	}
}

func TestMachine_ExhaustedIsTerminal(t *testing.T) {
	m := NewMachine(3)
	m.Observe(green)
	if got := m.Observe(exhausted); got != StateExhausted {
		t.Errorf("exhausted report = %s, want EXHAUSTED", got)
	}
	// No later heartbeat — not even a run of greens — leaves EXHAUSTED.
	for i := 0; i < 5; i++ {
		if got := m.Observe(green); got != StateExhausted {
			t.Fatalf("green #%d after exhaustion = %s, want still EXHAUSTED", i, got)
		}
	}
	if got := m.Observe(red); got != StateExhausted {
		t.Errorf("red after exhaustion = %s, want still EXHAUSTED", got)
	}
}

func TestMachine_ExhaustedSnapsFromAnyState(t *testing.T) {
	for _, start := range []observabilityv1.HealthState{green, yellow, red} {
		m := NewMachine(3)
		m.Observe(start)
		if got := m.Observe(exhausted); got != StateExhausted {
			t.Errorf("from %s: exhausted = %s, want EXHAUSTED", start, got)
		}
	}
}

func TestMachine_RepeatedGreenStaysGreenWithoutStreak(t *testing.T) {
	m := NewMachine(3)
	m.Observe(green)
	// Already GREEN: further greens are a no-op snap, not streak accumulation.
	for i := 0; i < 4; i++ {
		if got := m.Observe(green); got != StateGreen {
			t.Fatalf("steady green #%d = %s, want GREEN", i, got)
		}
	}
}

func TestNewMachine_NonPositiveThresholdFallsBack(t *testing.T) {
	m := NewMachine(0)
	m.Observe(red)
	// Must still require DefaultGreenStreak greens, not recover instantly.
	for i := 0; i < DefaultGreenStreak-1; i++ {
		if got := m.Observe(green); got != StateRed {
			t.Fatalf("green #%d = %s, want still RED under fallback threshold", i, got)
		}
	}
	if got := m.Observe(green); got != StateGreen {
		t.Errorf("final green = %s, want GREEN", got)
	}
}

func TestState_StringAndSeverity(t *testing.T) {
	cases := []struct {
		s   State
		str string
		sev int
	}{
		{StateUnspecified, "UNSPECIFIED", 0},
		{StateGreen, "GREEN", 1},
		{StateYellow, "YELLOW", 2},
		{StateRed, "RED", 3},
		{StateExhausted, "EXHAUSTED", 4},
		{State(99), "UNSPECIFIED", 0},
	}
	for _, c := range cases {
		if c.s.String() != c.str {
			t.Errorf("State(%d).String() = %q, want %q", c.s, c.s.String(), c.str)
		}
		if c.s.Severity() != c.sev {
			t.Errorf("State(%d).Severity() = %d, want %d", c.s, c.s.Severity(), c.sev)
		}
	}
}

func TestFromProto_UnknownIsUnspecified(t *testing.T) {
	if got := fromProto(observabilityv1.HealthState(42)); got != StateUnspecified {
		t.Errorf("unknown proto state = %s, want UNSPECIFIED", got)
	}
}

func TestStateOf_MapsWireToLocal(t *testing.T) {
	cases := map[observabilityv1.HealthState]State{
		green:       StateGreen,
		yellow:      StateYellow,
		red:         StateRed,
		exhausted:   StateExhausted,
		unspecified: StateUnspecified,
	}
	for in, want := range cases {
		if got := StateOf(in); got != want {
			t.Errorf("StateOf(%s) = %s, want %s", in, got, want)
		}
	}
}
