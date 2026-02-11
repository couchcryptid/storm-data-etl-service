package domain

import "github.com/jonboulle/clockwork"

// clock is a package-level time source so tests can freeze time via SetClock.
// Production code uses the real clock; tests inject a fake for deterministic output.
var clock = clockwork.NewRealClock()

// SetClock swaps the time source for enrichment. Pass nil to reset to real time.
func SetClock(c clockwork.Clock) {
	if c == nil {
		clock = clockwork.NewRealClock()
		return
	}
	clock = c
}
