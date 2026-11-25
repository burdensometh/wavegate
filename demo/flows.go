package demo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"wavegate/fault"
	"wavegate/v1"
)

func (s *scenario) drive(ctx context.Context, fixture Fixture) error {
	primary, queued, isolated := fixture.Primary, fixture.Queued, fixture.Isolated
	first, err := s.until(ctx, primary, "stable-only binding", func(state v1.ReleaseState) bool {
		return state.Binding != nil && state.Binding.Rule.CandidateWeight == 0
	})
	if err != nil {
		return err
	}
	steady, err := s.probe(ctx, primary, 0, 0)
	if err != nil {
		return err
	}
	s.observe("progress", "stable-only", &steady, nil)
	active, err := s.until(ctx, primary, "first weighted step", weighted(0, first.Status.BindingVersion))
	if err != nil {
		return err
	}
	if _, err := s.until(ctx, queued, "same-namespace quota blocked", func(state v1.ReleaseState) bool {
		return condition(state, "Admitted", "NamespaceBudget")
	}); err != nil {
		return err
	}
	if _, err := s.until(ctx, isolated, "isolated weighted step", weighted(0, 0)); err != nil {
		return err
	}
	subjectQuota, err := s.inspectQuota(ctx, primary, 1)
	if err != nil {
		return err
	}
	peerQuota, err := s.inspectQuota(ctx, isolated, 1)
	if err != nil {
		return err
	}
	s.observe("isolation", "queued-and-admitted", nil, []any{subjectQuota, peerQuota})
	failing, err := s.probe(ctx, isolated, isolated.Steps[0], isolated.Steps[0])
	if err != nil {
		return err
	}
	s.observe("abort", "candidate-errors", &failing, nil)
	primary, err = s.pauseAndResume(ctx, primary, active.Status.BindingVersion)
	if err != nil {
		return err
	}
	active, _, err = s.app.Journal.State(primary.UID)
	if err != nil {
		return err
	}
	firstPass, err := s.probe(ctx, primary, primary.Steps[0], 0)
	if err != nil {
		return err
	}
	s.observe("progress", "first-step", &firstPass, nil)
	s.clock.Advance(max(primary.Window.Duration(), isolated.Window.Duration()))
	if _, err := s.until(ctx, isolated, "metrics rollback", phase(v1.Aborted)); err != nil {
		return err
	}
	postFailure, err := s.probe(ctx, isolated, 0, 0)
	if err != nil {
		return err
	}
	s.observe("abort", "metrics-rollback", &postFailure, nil)
	if err := s.exportWindow(ctx, isolated, failing.BindingVersion, "failed"); err != nil {
		return err
	}
	restarted, err := s.restartAfterApply(ctx, primary, active.Status.BindingVersion)
	if err != nil {
		return err
	}
	if err := s.exportWindow(ctx, primary, firstPass.BindingVersion, "first-pass"); err != nil {
		return err
	}
	secondPass, err := s.probe(ctx, primary, primary.Steps[1], 0)
	if err != nil {
		return err
	}
	s.observe("progress", "second-step", &secondPass, nil)
	s.clock.Advance(primary.Window.Duration())
	if _, err := s.until(ctx, primary, "promotion", phase(v1.Promoted)); err != nil {
		return err
	}
	promoted, err := s.probe(ctx, primary, 0, 0)
	if err != nil {
		return err
	}
	binding, err := s.traffic(ctx, primary)
	if err != nil {
		return err
	}
	if binding.Rule.Stable != primary.Candidate || restarted != secondPass.BindingVersion {
		return fmt.Errorf("promotion or replay did not preserve the expected traffic binding")
	}
	s.observe("progress", "promoted", &promoted, binding.Rule)
	if err := s.exportWindow(ctx, primary, secondPass.BindingVersion, "second-pass"); err != nil {
		return err
	}
	if err := s.manualAbort(ctx, queued); err != nil {
		return err
	}
	for _, spec := range []v1.ReleaseSpec{primary, queued, isolated} {
		if err := s.exportRelease(ctx, spec); err != nil {
			return err
		}
	}
	return nil
}

func (s *scenario) pauseAndResume(ctx context.Context, spec v1.ReleaseSpec, version uint64) (v1.ReleaseSpec, error) {
	spec.Paused = true
	spec, err := s.put(ctx, spec)
	if err != nil {
		return spec, err
	}
	if _, err := s.until(ctx, spec, "manual pause", phase(v1.Paused)); err != nil {
		return spec, err
	}
	probe, err := s.probe(ctx, spec, spec.Steps[0], 0)
	if err != nil {
		return spec, err
	}
	s.clock.Advance(spec.Window.Duration() + time.Second)
	if err := s.reconcile(ctx, spec); err != nil {
		return spec, err
	}
	live, err := s.traffic(ctx, spec)
	if err != nil {
		return spec, err
	}
	if live.ResourceVersion != version {
		return spec, fmt.Errorf("pause changed traffic binding from %d to %d", version, live.ResourceVersion)
	}
	s.observe("progress", "manually-paused", &probe, nil)
	spec.Paused = false
	spec, err = s.put(ctx, spec)
	if err != nil {
		return spec, err
	}
	_, err = s.until(ctx, spec, "fresh window after resume", weighted(0, version))
	return spec, err
}

func (s *scenario) restartAfterApply(ctx context.Context, spec v1.ReleaseSpec, prior uint64) (uint64, error) {
	prepared, err := s.until(ctx, spec, "second step intent", func(state v1.ReleaseState) bool {
		return state.Intent != nil && state.Intent.Kind == v1.WeightStep && state.Intent.Step == 1
	})
	if err != nil {
		return 0, err
	}
	s.fault.DropNextReceipt()
	err = s.reconcile(ctx, spec)
	if !errors.Is(err, fault.ErrLostReceipt) {
		return 0, fmt.Errorf("expected a dropped apply receipt, got %v", err)
	}
	live, err := s.traffic(ctx, spec)
	if err != nil {
		return 0, err
	}
	if live.Operation != prepared.Intent.Operation || live.ResourceVersion <= prior {
		return 0, fmt.Errorf("receipt drop did not occur after traffic apply")
	}
	if err := s.close(); err != nil {
		return 0, err
	}
	if err := s.open(); err != nil {
		return 0, err
	}
	if err := s.sync(ctx); err != nil {
		return 0, err
	}
	recovered, err := s.until(ctx, spec, "second step recovered", weighted(1, prior))
	if err != nil {
		return 0, err
	}
	if recovered.Status.BindingVersion != live.ResourceVersion || recovered.Binding.Operation != live.Operation {
		return 0, fmt.Errorf("recovery applied the second step more than once")
	}
	s.observe("resume", "recovered-unacknowledged-apply", nil, struct {
		Operation      string `json:"operation"`
		BindingVersion uint64 `json:"bindingVersion"`
	}{live.Operation, live.ResourceVersion})
	return live.ResourceVersion, nil
}

func (s *scenario) manualAbort(ctx context.Context, spec v1.ReleaseSpec) error {
	if _, err := s.until(ctx, spec, "queue released after promotion", weighted(0, 0)); err != nil {
		return err
	}
	spec.Abort = true
	spec, err := s.put(ctx, spec)
	if err != nil {
		return err
	}
	if _, err := s.until(ctx, spec, "manual rollback", phase(v1.Aborted)); err != nil {
		return err
	}
	probe, err := s.probe(ctx, spec, 0, 0)
	if err != nil {
		return err
	}
	usage, err := s.inspectQuota(ctx, spec, 0)
	if err != nil {
		return err
	}
	s.observe("abort", "manual-rollback", &probe, usage)
	return nil
}
