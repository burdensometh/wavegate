package clock

import (
	"sync"
	"time"
)

type Clock interface {
	Now() time.Time
}

type Real struct{}

func (Real) Now() time.Time { return time.Now().UTC() }

type Manual struct {
	mu  sync.Mutex
	now time.Time
}

func NewManual(at time.Time) *Manual { return &Manual{now: at.UTC()} }

func (m *Manual) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

func (m *Manual) Advance(delta time.Duration) {
	if delta < 0 {
		panic("manual clock cannot move backwards")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = m.now.Add(delta)
}
