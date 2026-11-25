package rollout

import (
	"context"
	"fmt"
	"sync"
	"time"

	"wavegate/clock"
	"wavegate/discovery"
	"wavegate/journal"
	"wavegate/quota"
	"wavegate/traffic"
	"wavegate/v1"
)

type Observations interface {
	Snapshot(v1.Service) discovery.Snapshot
}

type Windows interface {
	Verdict(v1.WindowKey, v1.WindowPolicy, time.Time) (v1.WindowVerdict, error)
}

type EndpointFence interface {
	WithReadyRule(v1.Rule, func() error) error
}

type Engine struct {
	mu          sync.Mutex
	specs       *Specs
	index       Observations
	fence       EndpointFence
	plane       traffic.Plane
	windows     Windows
	journal     *journal.Store
	book        *quota.Book
	clock       clock.Clock
	drain       time.Duration
	drainInUse  func(v1.Service, v1.Target) bool
	generations map[string]uint64
}

type Dependencies struct {
	Specs        *Specs
	Index        Observations
	Fence        EndpointFence
	Plane        traffic.Plane
	Windows      Windows
	Journal      *journal.Store
	Clock        clock.Clock
	DefaultQuota quota.Limit
	Quotas       map[string]quota.Limit
	Drain        time.Duration
	DrainInUse   func(v1.Service, v1.Target) bool
}

func New(deps Dependencies) (*Engine, error) {
	if deps.Specs == nil || deps.Index == nil || deps.Fence == nil || deps.Plane == nil ||
		deps.Windows == nil || deps.Journal == nil || deps.Clock == nil || deps.Drain < 0 {
		return nil, v1.Invalid("rollout dependencies or drain interval")
	}
	states, err := deps.Journal.States()
	if err != nil {
		return nil, err
	}
	var reservations []v1.NamespaceReservation
	for _, state := range states {
		if state.Reservation != nil {
			reservations = append(reservations, *state.Reservation)
		}
	}
	book, err := quota.New(deps.DefaultQuota, deps.Quotas, reservations)
	if err != nil {
		return nil, err
	}
	return &Engine{
		specs: deps.Specs, index: deps.Index, fence: deps.Fence, plane: deps.Plane, windows: deps.Windows,
		journal: deps.Journal, book: book, clock: deps.Clock, drain: deps.Drain,
		generations: make(map[string]uint64),
		drainInUse:  deps.DrainInUse,
	}, nil
}

func (e *Engine) Usage(namespace string) quota.Usage { return e.book.Usage(namespace) }

// Reconcile advances at most one durable decision. Independent releases can be
// triggered concurrently. The engine serializes reservation and journal mutations.
func (e *Engine) Reconcile(ctx context.Context, namespace, name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	view, err := e.specs.Get(namespace, name)
	if err != nil {
		return err
	}
	spec := view.Spec
	now := e.clock.Now().UTC()
	state, exists, err := e.journal.State(spec.UID)
	if err != nil {
		return err
	}
	if !exists {
		state = v1.ReleaseState{Spec: spec, Status: v1.ReleaseStatus{
			APIVersion: v1.Version, Service: spec.Service, ReleaseUID: spec.UID,
			ObservedVersion: spec.ResourceVersion, Phase: v1.Waiting, Step: -1, Conditions: []v1.Condition{},
		}}
		setCondition(&state.Status, now, "Progressing", false, "ReleaseObserved")
		return e.commit(now, state, "ReleaseObserved", "DesiredStateRead", nil)
	}
	if err := e.specs.Publish(state.Status); err != nil {
		return err
	}
	if spec.ResourceVersion != state.Spec.ResourceVersion {
		state.Spec = spec
		state.Status.ObservedVersion = spec.ResourceVersion
		return e.commit(now, state, "DesiredObserved", "ResourceVersionChanged", state.Reservation)
	}
	binding, exists, err := e.plane.Observe(spec.Service)
	if err != nil {
		return err
	}
	snapshot := e.index.Snapshot(spec.Service)
	if state.Intent != nil {
		return e.execute(ctx, now, state, binding, snapshot)
	}
	if v1.Terminal(state.Status.Phase) {
		return e.terminal(now, state, binding, snapshot)
	}
	if state.Binding != nil {
		if !exists || binding.ResourceVersion != state.Binding.ResourceVersion || binding.OwnerUID != spec.UID ||
			binding.Operation != state.Binding.Operation || !traffic.SameRule(binding.Rule, state.Binding.Rule) {
			return fmt.Errorf("%w: live binding diverged from committed decision for %s", v1.ErrConflict, spec.Service)
		}
	} else if exists && binding.OwnerUID == spec.UID {
		return fmt.Errorf("%w: traffic owner has no committed or prepared decision for %s", v1.ErrConflict, spec.Service)
	}
	return e.advance(now, state, binding, snapshot)
}

func (e *Engine) commit(at time.Time, state v1.ReleaseState, kind, reason string, previous *v1.NamespaceReservation) error {
	record, err := e.journal.Commit(e.journal.Sequence(), at, kind, reason, state)
	if err != nil {
		return err
	}
	switch {
	case previous == nil && record.State.Reservation != nil:
		if err := e.book.Install(*record.State.Reservation); err != nil {
			return err
		}
	case previous != nil && record.State.Reservation == nil:
		if err := e.book.Release(*previous); err != nil {
			return err
		}
	}
	return e.specs.Publish(record.State.Status)
}

func (e *Engine) condition(at time.Time, state v1.ReleaseState, kind string, status bool, reason string) error {
	if !setCondition(&state.Status, at, kind, status, reason) {
		return nil
	}
	return e.commit(at, state, "ConditionChanged", reason, state.Reservation)
}

func setCondition(status *v1.ReleaseStatus, at time.Time, kind string, value bool, reason string) bool {
	for index, previous := range status.Conditions {
		if previous.Type != kind {
			continue
		}
		if previous.Status == value && previous.Reason == reason {
			return false
		}
		if previous.Status == value {
			at = previous.LastTransitionTime
		}
		status.Conditions[index] = v1.Condition{Type: kind, Status: value, Reason: reason, LastTransitionTime: at}
		return true
	}
	status.Conditions = append(status.Conditions, v1.Condition{Type: kind, Status: value, Reason: reason, LastTransitionTime: at})
	return true
}
