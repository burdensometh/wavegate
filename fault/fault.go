package fault

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"wavegate/discovery"
	"wavegate/traffic"
	"wavegate/v1"
)

var ErrLostReceipt = errors.New("apply receipt dropped after commit")

// WindowStore is the verdict source a Windows wrapper can poll for seals.
type WindowStore interface {
	Verdict(v1.WindowKey, v1.WindowPolicy, time.Time) (v1.WindowVerdict, error)
	Inventory() (version uint64, sealed int, err error)
}

// Windows traps the first Verdict call that durably seals a window. The trap
// runs after the seal commits but before the caller can persist the decision
// that consumes the verdict, which is the crash boundary under test.
type Windows struct {
	store WindowStore
	mu    sync.Mutex
	trap  func(v1.WindowVerdict)
}

func NewWindows(store WindowStore) *Windows { return &Windows{store: store} }

// TrapAfterNextSeal installs a one-shot hook for the next freshly sealed window.
func (w *Windows) TrapAfterNextSeal(trap func(v1.WindowVerdict)) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.trap = trap
}

func (w *Windows) Verdict(key v1.WindowKey, policy v1.WindowPolicy, now time.Time) (v1.WindowVerdict, error) {
	w.mu.Lock()
	armed := w.trap != nil
	w.mu.Unlock()
	if !armed {
		return w.store.Verdict(key, policy, now)
	}
	_, before, err := w.store.Inventory()
	if err != nil {
		return v1.WindowVerdict{}, err
	}
	verdict, err := w.store.Verdict(key, policy, now)
	if err != nil {
		return v1.WindowVerdict{}, err
	}
	_, after, err := w.store.Inventory()
	if err != nil {
		return v1.WindowVerdict{}, err
	}
	if after > before {
		w.mu.Lock()
		trap := w.trap
		w.trap = nil
		w.mu.Unlock()
		trap(verdict)
	}
	return verdict, nil
}

// Plane drops an armed receipt after the enclosed atomic mutation succeeds.
type Plane struct {
	traffic.Plane
	mu    sync.Mutex
	armed bool
}

func (p *Plane) DropNextReceipt() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.armed = true
}

func (p *Plane) Apply(ctx context.Context, request traffic.ApplyRequest) (v1.TrafficBinding, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	binding, err := p.Plane.Apply(ctx, request)
	if err == nil && p.armed {
		p.armed = false
		return v1.TrafficBinding{}, ErrLostReceipt
	}
	return binding, err
}

type Source struct {
	discovery.Source
	mu         sync.Mutex
	paused     bool
	changed    chan struct{}
	generation uint64
	active     map[*stream]context.CancelFunc
}

func NewSource(source discovery.Source) *Source {
	return &Source{Source: source, changed: make(chan struct{}), active: make(map[*stream]context.CancelFunc)}
}

func (s *Source) Pause(paused bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if paused != s.paused {
		s.paused = paused
		close(s.changed)
		s.changed = make(chan struct{})
	}
}

func (s *Source) Disconnect() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.generation++
	for _, cancel := range s.active {
		cancel()
	}
}

func (s *Source) Watch(ctx context.Context, version uint64) (discovery.Stream, error) {
	ctx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	generation := s.generation
	s.mu.Unlock()
	upstream, err := s.Source.Watch(ctx, version)
	if err != nil {
		cancel()
		return nil, err
	}
	result := &stream{source: s, upstream: upstream, ctx: ctx}
	s.mu.Lock()
	if generation != s.generation {
		cancel()
	}
	s.active[result] = cancel
	s.mu.Unlock()
	return result, nil
}

type stream struct {
	source   *Source
	upstream discovery.Stream
	ctx      context.Context
}

func (s *stream) Next(ctx context.Context) (v1.EndpointEvent, error) {
	event, err := s.upstream.Next(s.ctx)
	if err != nil {
		if s.ctx.Err() != nil && ctx.Err() == nil {
			return v1.EndpointEvent{}, io.EOF
		}
		return event, err
	}
	for {
		s.source.mu.Lock()
		paused, changed := s.source.paused, s.source.changed
		s.source.mu.Unlock()
		if s.ctx.Err() != nil {
			return v1.EndpointEvent{}, io.EOF
		}
		if !paused {
			return event, nil
		}
		select {
		case <-ctx.Done():
			return v1.EndpointEvent{}, ctx.Err()
		case <-s.ctx.Done():
			return v1.EndpointEvent{}, io.EOF
		case <-changed:
		}
	}
}

func (s *stream) Close() error {
	s.source.mu.Lock()
	if cancel, exists := s.source.active[s]; exists {
		cancel()
		delete(s.source.active, s)
	}
	s.source.mu.Unlock()
	return s.upstream.Close()
}
