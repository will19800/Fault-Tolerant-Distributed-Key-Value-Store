package raft

import "time"

// Timer is the portion of time.Timer used by the Raft event loop.
type Timer interface {
	C() <-chan time.Time
	Reset(time.Duration)
	Stop() bool
}

// Clock creates timers. Tests can inject manually fired timers.
type Clock interface {
	NewTimer(time.Duration) Timer
}

type systemClock struct{}

func (systemClock) NewTimer(duration time.Duration) Timer {
	return &systemTimer{timer: time.NewTimer(duration)}
}

type systemTimer struct {
	timer *time.Timer
}

func (t *systemTimer) C() <-chan time.Time {
	return t.timer.C
}

func (t *systemTimer) Reset(duration time.Duration) {
	t.timer.Reset(duration)
}

func (t *systemTimer) Stop() bool {
	return t.timer.Stop()
}
