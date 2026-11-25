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

func TestParallelReconcileKeepsNamespaceAndServiceReservations(t *testing.T) {
	cfg := config.Default()
	cfg.StateDirectory = t.TempDir()
	cfg.DefaultQuota = quota.Limit{Releases: 1, CandidateUnits: 1}
	ctx := context.Background()
	app, err := Open(cfg, Options{Clock: clock.NewManual(restartTime())})
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	base := testRelease()
	other, isolated := base, base
	other.Name, other.UID, other.Service.Name = "another-rollout", "rollout-2", "other-service"
	isolated.Name, isolated.UID, isolated.Service.Namespace = "isolated-rollout", "rollout-3", "isolated"
	specs := []v1.ReleaseSpec{base, other, isolated}
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
	home := app.Engine.Usage(base.Service.Namespace)
	peer := app.Engine.Usage(isolated.Service.Namespace)
	if home.Releases != 1 || home.CandidateUnits != 1 || peer.Releases != 1 || peer.Reservations[0].ReleaseUID != isolated.UID {
		t.Fatalf("home=%+v isolated=%+v", home, peer)
	}
	for _, spec := range []v1.ReleaseSpec{base, other} {
		state, ok, err := app.Journal.State(spec.UID)
		if err != nil || !ok {
			t.Fatalf("%s state: %v", spec.Key(), err)
		}
		if spec.UID == home.Reservations[0].ReleaseUID {
			if state.Reservation == nil || state.Status.Phase != v1.Progressing {
				t.Fatalf("%s owns quota but is not progressing: %+v", spec.Key(), state)
			}
		} else if state.Reservation != nil || state.Binding != nil || state.Status.Phase != v1.Waiting {
			t.Fatalf("%s served traffic without a reservation: %+v", spec.Key(), state)
		}
	}
}
