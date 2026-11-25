package demo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"time"

	"wavegate/clock"
	"wavegate/config"
	"wavegate/fault"
	"wavegate/journal"
	"wavegate/metrics"
	"wavegate/quota"
	"wavegate/server"
	"wavegate/traffic"
	"wavegate/v1"
)

type observation struct {
	Flow    string `json:"flow"`
	Phase   string `json:"phase,omitempty"`
	Probe   *Probe `json:"probe,omitempty"`
	Details any    `json:"details,omitempty"`
}

type report struct {
	APIVersion   string        `json:"apiVersion"`
	Observations []observation `json:"observations"`
}

type scenario struct {
	app      *server.App
	api      *httptest.Server
	backends []*httptest.Server
	fault    *fault.Plane
	clock    *clock.Manual
	cfg      config.Config
	out      string
	report   report
}

func Run(ctx context.Context, fixturePath, output string, stdout io.Writer) (result error) {
	fixture, err := readFixture(fixturePath)
	if err != nil {
		return err
	}
	if err := os.Mkdir(output, 0o750); err != nil {
		return err
	}
	s := &scenario{
		clock: clock.NewManual(time.Date(2026, 11, 25, 8, 0, 0, 0, time.UTC)),
		cfg:   config.Default(), out: output, report: report{APIVersion: v1.Version},
	}
	s.cfg.StateDirectory = filepath.Join(output, "state")
	s.cfg.DefaultQuota = quota.Limit{Releases: 1, CandidateUnits: 2}
	s.cfg.JournalRetention = 64
	defer func() {
		result = errors.Join(result, s.close())
		for _, backend := range s.backends {
			backend.Close()
		}
	}()
	if err := s.open(); err != nil {
		return err
	}
	if err := s.activateEndpoints(ctx, fixture); err != nil {
		return err
	}
	for _, spec := range []*v1.ReleaseSpec{&fixture.Primary, &fixture.Queued, &fixture.Isolated} {
		*spec, err = s.put(ctx, *spec)
		if err != nil {
			return err
		}
	}
	if err := s.drive(ctx, fixture); err != nil {
		return err
	}
	if err := s.exportTimeline(ctx); err != nil {
		return err
	}
	if err := s.write("report.v1.json", s.report); err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(s.report)
}

func (s *scenario) open() error {
	app, err := server.Open(s.cfg, server.Options{
		Clock: s.clock,
		WrapPlane: func(plane traffic.Plane) traffic.Plane {
			s.fault = &fault.Plane{Plane: plane}
			return s.fault
		},
	})
	if err != nil {
		return err
	}
	s.app, s.api = app, httptest.NewServer(app.Handler())
	return nil
}

func (s *scenario) close() error {
	if s.api != nil {
		s.api.Close()
		s.api = nil
	}
	if s.app == nil {
		return nil
	}
	err := s.app.Close()
	s.app = nil
	return err
}

func (s *scenario) reconcile(ctx context.Context, spec v1.ReleaseSpec) error {
	return s.app.Engine.Reconcile(ctx, spec.Service.Namespace, spec.Name)
}

func (s *scenario) until(ctx context.Context, spec v1.ReleaseSpec, want string, ready func(v1.ReleaseState) bool) (v1.ReleaseState, error) {
	var state v1.ReleaseState
	for i := 0; i < 40; i++ {
		if err := s.reconcile(ctx, spec); err != nil {
			return state, fmt.Errorf("%s reconcile: %w", spec.Key(), err)
		}
		next, exists, err := s.app.Journal.State(spec.UID)
		if err != nil {
			return state, err
		}
		state = next
		if exists && ready(state) {
			return state, nil
		}
	}
	return state, fmt.Errorf("%s did not reach %s; current phase=%s step=%d", spec.Key(), want, state.Status.Phase, state.Status.Step)
}

func phase(want string) func(v1.ReleaseState) bool {
	return func(state v1.ReleaseState) bool { return state.Status.Phase == want }
}

func condition(state v1.ReleaseState, kind, reason string) bool {
	for _, item := range state.Status.Conditions {
		if item.Type == kind && item.Reason == reason {
			return true
		}
	}
	return false
}

func weighted(step int, after uint64) func(v1.ReleaseState) bool {
	return func(state v1.ReleaseState) bool {
		return state.Status.Phase == v1.Progressing && state.Status.Step == step &&
			state.Status.BindingVersion > after && state.Intent == nil
	}
}

func (s *scenario) observe(flow, phase string, probe *Probe, details any) {
	s.report.Observations = append(s.report.Observations, observation{Flow: flow, Phase: phase, Probe: probe, Details: details})
}

func (s *scenario) write(name string, value any) error {
	file, err := os.OpenFile(filepath.Join(s.out, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o640)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return errors.Join(encoder.Encode(value), file.Close())
}

func (s *scenario) exportRelease(ctx context.Context, spec v1.ReleaseSpec) error {
	receipt, err := call[journal.Receipt](ctx, s.api, http.MethodGet, "/v1/releases/"+spec.Key()+"/receipt", nil)
	if err != nil {
		return err
	}
	if err := journal.VerifyReceipt(receipt); err != nil {
		return err
	}
	if err := s.write(spec.UID+".receipt.v1.json", receipt); err != nil {
		return err
	}
	view, err := s.get(ctx, spec)
	if err != nil {
		return err
	}
	return s.write(spec.UID+".release.v1.json", view)
}

func (s *scenario) exportWindow(ctx context.Context, spec v1.ReleaseSpec, binding uint64, label string) error {
	path := fmt.Sprintf("/v1/samples/%s/%d", spec.Service, binding)
	evidence, err := call[metrics.EvidenceDocument](ctx, s.api, http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	if err := metrics.VerifyEvidence(evidence); err != nil {
		return err
	}
	return s.write(label+".window.v1.json", evidence)
}

func (s *scenario) inspectQuota(ctx context.Context, spec v1.ReleaseSpec, expected int) (quota.Usage, error) {
	usage, err := call[quota.Usage](ctx, s.api, http.MethodGet, "/v1/namespaces/"+spec.Service.Namespace+"/quota", nil)
	if err == nil && usage.Releases != expected {
		err = fmt.Errorf("namespace %s quota expected %d releases, got %d", usage.Namespace, expected, usage.Releases)
	}
	return usage, err
}
