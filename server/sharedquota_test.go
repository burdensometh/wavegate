package server

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"wavegate/clock"
	"wavegate/config"
	"wavegate/quota"
	"wavegate/v1"
)

func hasReason(state v1.ReleaseState, kind, reason string) bool {
	for _, item := range state.Status.Conditions {
		if item.Type == kind && item.Reason == reason {
			return true
		}
	}
	return false
}

func reconcileUntil(t *testing.T, app *App, spec v1.ReleaseSpec, ready func(v1.ReleaseState) bool) v1.ReleaseState {
	t.Helper()
	ctx := context.Background()
	var state v1.ReleaseState
	for i := 0; i < 20; i++ {
		if err := app.Engine.Reconcile(ctx, spec.Service.Namespace, spec.Name); err != nil {
			t.Fatalf("%s reconcile: %v", spec.Key(), err)
		}
		next, exists, err := app.Journal.State(spec.UID)
		if err != nil || !exists {
			t.Fatalf("%s state: %v exists=%v", spec.Key(), err, exists)
		}
		state = next
		if ready(state) {
			return state
		}
	}
	t.Fatalf("%s did not reach the expected state; phase=%s", spec.Key(), state.Status.Phase)
	return state
}

func sharedPoolConfig(dir string) config.Config {
	cfg := config.Default()
	cfg.StateDirectory = dir
	cfg.DefaultQuota = quota.Limit{Releases: 4, CandidateUnits: 4}
	cfg.SharedQuota = quota.Shared{CandidateUnits: 1, Namespaces: []string{"shop", "payments"}}
	return cfg
}

func sharedPoolSpecs() (holder, waiter v1.ReleaseSpec) {
	holder = testRelease()
	waiter = testRelease()
	waiter.Name, waiter.UID = "waiting-rollout", "rollout-2"
	waiter.Service = v1.Service{Namespace: "payments", Name: "cart"}
	return holder, waiter
}

func installSharedPoolFixtures(t *testing.T, app *App, specs ...v1.ReleaseSpec) {
	t.Helper()
	ctx := context.Background()
	for index, spec := range specs {
		putEndpoint(t, app, spec.Service, spec.Stable, fmt.Sprintf("stable-%d", index), "http://127.0.0.1:49160", 0)
		putEndpoint(t, app, spec.Service, spec.Candidate, fmt.Sprintf("candidate-%d", index), "http://127.0.0.1:49161", 0)
		if _, err := app.Specs.Put(spec); err != nil {
			t.Fatal(err)
		}
	}
	list, err := app.Catalog.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Index.Replace(list); err != nil {
		t.Fatal(err)
	}
}

// Two member namespaces race for the last shared candidate unit. The engine
// mutex serializes their reconciles, so exactly one reservation is granted;
// the loser waits with Admitted=false (SharedBudget).
func TestConcurrentSharedBudgetAdmitsAtMostOne(t *testing.T) {
	cfg := sharedPoolConfig(t.TempDir())
	ctx := context.Background()
	app, err := Open(cfg, Options{Clock: clock.NewManual(restartTime())})
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	holder, waiter := sharedPoolSpecs()
	outside := testRelease()
	outside.Name, outside.UID = "outside-rollout", "rollout-3"
	outside.Service = v1.Service{Namespace: "other", Name: "app"}
	specs := []v1.ReleaseSpec{holder, waiter, outside}
	installSharedPoolFixtures(t, app, specs...)
	var workers sync.WaitGroup
	for _, spec := range specs {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := 0; i < 14; i++ {
				if err := app.Engine.Reconcile(ctx, spec.Service.Namespace, spec.Name); err != nil {
					t.Errorf("%s: %v", spec.Key(), err)
					return
				}
			}
		}()
	}
	workers.Wait()
	shop, payments := app.Engine.Usage("shop"), app.Engine.Usage("payments")
	if shop.Shared == nil || payments.Shared == nil {
		t.Fatalf("member namespaces must see the shared pool: %+v %+v", shop, payments)
	}
	if shop.Releases+payments.Releases != 1 || shop.Shared.CandidateUnits != 1 ||
		payments.Shared.CandidateUnits != 1 || shop.Shared.Limit != 1 {
		t.Fatalf("shared unit granted more than once: shop=%+v payments=%+v", shop, payments)
	}
	if len(shop.Shared.Reservations) != 1 || shop.Shared.Reservations[0] != payments.Shared.Reservations[0] {
		t.Fatalf("members disagree about the shared reservation: %+v %+v", shop.Shared, payments.Shared)
	}
	for _, spec := range []v1.ReleaseSpec{holder, waiter} {
		state, ok, err := app.Journal.State(spec.UID)
		if err != nil || !ok {
			t.Fatalf("%s state: %v", spec.Key(), err)
		}
		if spec.UID == shop.Shared.Reservations[0].ReleaseUID {
			if state.Reservation == nil || state.Status.Phase != v1.Progressing {
				t.Fatalf("%s owns the shared unit but is not progressing: %+v", spec.Key(), state)
			}
		} else if state.Reservation != nil || state.Binding != nil || state.Status.Phase != v1.Waiting ||
			!hasReason(state, "Admitted", "SharedBudget") {
			t.Fatalf("%s did not wait on the shared budget: %+v", spec.Key(), state)
		}
	}
	// A namespace outside the pool is not blocked by the exhausted pool.
	if usage := app.Engine.Usage("other"); usage.Shared != nil || usage.Releases != 1 {
		t.Fatalf("non-member namespace affected by the shared pool: %+v", usage)
	}
}

// Granted shares and waiting results are recomputed from the decision records
// after a restart, and only the holder's terminal transition returns its share.
func TestSharedBudgetRecoveredAndReturnedAfterRestart(t *testing.T) {
	cfg := sharedPoolConfig(t.TempDir())
	ctx := context.Background()
	app, err := Open(cfg, Options{Clock: clock.NewManual(restartTime())})
	if err != nil {
		t.Fatal(err)
	}
	holder, waiter := sharedPoolSpecs()
	installSharedPoolFixtures(t, app, holder, waiter)
	granted := reconcileUntil(t, app, holder, func(state v1.ReleaseState) bool { return state.Reservation != nil })
	blocked := reconcileUntil(t, app, waiter, func(state v1.ReleaseState) bool {
		return hasReason(state, "Admitted", "SharedBudget")
	})
	if blocked.Reservation != nil {
		t.Fatalf("waiter holds a reservation: %+v", blocked.Reservation)
	}
	if usage := app.Engine.Usage("payments"); usage.Shared == nil || usage.Shared.CandidateUnits != 1 {
		t.Fatalf("shared usage before restart = %+v", usage)
	}
	if err := app.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(cfg, Options{Clock: clock.NewManual(restartTime())})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	usage := reopened.Engine.Usage("payments")
	if usage.Shared == nil || usage.Shared.CandidateUnits != 1 || usage.Shared.Limit != 1 ||
		len(usage.Shared.Reservations) != 1 || usage.Shared.Reservations[0] != *granted.Reservation {
		t.Fatalf("shared grant not recomputed from records: %+v", usage.Shared)
	}
	list, err := reopened.Catalog.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Index.Replace(list); err != nil {
		t.Fatal(err)
	}
	stillWaiting := reconcileUntil(t, reopened, waiter, func(state v1.ReleaseState) bool {
		return hasReason(state, "Admitted", "SharedBudget")
	})
	if stillWaiting.Reservation != nil {
		t.Fatalf("waiter admitted while the shared unit is still held: %+v", stillWaiting.Reservation)
	}

	// Only the holder reaching a terminal phase returns the shared unit.
	view, err := reopened.Specs.Get(holder.Service.Namespace, holder.Name)
	if err != nil {
		t.Fatal(err)
	}
	aborted := view.Spec
	aborted.Abort = true
	if _, err := reopened.Specs.Put(aborted); err != nil {
		t.Fatal(err)
	}
	reconcileUntil(t, reopened, holder, func(state v1.ReleaseState) bool { return state.Status.Phase == v1.Aborted })
	if usage := reopened.Engine.Usage("payments"); usage.Shared == nil || usage.Shared.CandidateUnits != 0 {
		t.Fatalf("terminal holder did not return the shared unit: %+v", usage.Shared)
	}
	reconcileUntil(t, reopened, waiter, func(state v1.ReleaseState) bool { return state.Reservation != nil })
}
