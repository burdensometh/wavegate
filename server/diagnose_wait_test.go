package server

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"wavegate/clock"
	"wavegate/config"
	"wavegate/journal"
	"wavegate/quota"
	"wavegate/v1"
)

// TestDiagnoseWaitingCauses builds three small durable scenarios. The stable
// revision case is closed and reopened from the same state directory to prove
// that its evidence survives a process restart.
func TestDiagnoseWaitingCauses(t *testing.T) {
	ctx := context.Background()
	root := os.Getenv("WAVEGATE_DIAG_OUT")
	if root == "" {
		root = t.TempDir()
	} else if err := os.MkdirAll(root, 0o750); err != nil {
		t.Fatal(err)
	}

	budgetEvidence := diagBudgetCase(t, ctx, filepath.Join(root, "budget"))
	notReadyEvidence := diagNotReadyCase(t, ctx, filepath.Join(root, "not-ready"))
	mismatchEvidence := diagMismatchCase(t, ctx, root)

	report := map[string]any{
		"apiVersion":             v1.Version,
		"budget":                 budgetEvidence,
		"endpointNotReady":       notReadyEvidence,
		"stableRevisionMismatch": mismatchEvidence,
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "evidence.v1.json"), data, 0o640); err != nil {
		t.Fatal(err)
	}
	t.Logf("diagnostic evidence written to %s", filepath.Join(root, "evidence.v1.json"))
}

type diagHarness struct {
	t   *testing.T
	ctx context.Context
	cfg config.Config
	clk *clock.Manual
	app *App
	dir string
}

func diagOpen(t *testing.T, ctx context.Context, dir string, limit quota.Limit) *diagHarness {
	t.Helper()
	cfg := config.Default()
	cfg.StateDirectory = dir
	cfg.DefaultQuota = limit
	cfg.Quotas = map[string]quota.Limit{}
	cfg.DrainMS = 0
	clk := clock.NewManual(time.Date(2026, 11, 25, 8, 0, 0, 0, time.UTC))
	app, err := Open(cfg, Options{Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	h := &diagHarness{t: t, ctx: ctx, cfg: cfg, clk: clk, app: app, dir: dir}
	h.sync()
	return h
}

func (h *diagHarness) close() {
	h.t.Helper()
	if err := h.app.Close(); err != nil {
		h.t.Fatal(err)
	}
}

func (h *diagHarness) restart() RecoveryReport {
	h.t.Helper()
	h.close()
	app, err := Open(h.cfg, Options{Clock: h.clk})
	if err != nil {
		h.t.Fatal(err)
	}
	h.app = app
	h.sync()
	return app.Recovery
}

func (h *diagHarness) sync() {
	h.t.Helper()
	list, err := h.app.Catalog.List(h.ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	if err := h.app.Index.Replace(list); err != nil {
		h.t.Fatal(err)
	}
}

func (h *diagHarness) endpoint(service v1.Service, revision, uid string, ready bool) {
	h.t.Helper()
	_, err := h.app.Catalog.Put(v1.ReplicaObservation{
		APIVersion: v1.Version,
		Service:    service,
		Node:       "node-a",
		Name:       uid,
		UID:        uid,
		Revision:   revision,
		Address:    "http://127.0.0.1:49151",
		Ready:      ready,
	})
	if err != nil {
		h.t.Fatal(err)
	}
}

func (h *diagHarness) put(spec v1.ReleaseSpec) v1.ReleaseSpec {
	h.t.Helper()
	view, err := h.app.Specs.Put(spec)
	if err != nil {
		h.t.Fatal(err)
	}
	return view.Spec
}

func (h *diagHarness) reconcile(spec v1.ReleaseSpec) {
	h.t.Helper()
	if err := h.app.Engine.Reconcile(h.ctx, spec.Service.Namespace, spec.Name); err != nil {
		h.t.Fatalf("reconcile %s: %v", spec.Key(), err)
	}
}

func (h *diagHarness) until(spec v1.ReleaseSpec, want func(v1.ReleaseState) bool) v1.ReleaseState {
	h.t.Helper()
	var state v1.ReleaseState
	for i := 0; i < 40; i++ {
		h.reconcile(spec)
		next, exists, err := h.app.Journal.State(spec.UID)
		if err != nil {
			h.t.Fatal(err)
		}
		state = next
		if exists && want(state) {
			return state
		}
	}
	h.t.Fatalf("release %s did not reach desired state; phase=%s step=%d conditions=%+v intent=%+v",
		spec.Key(), state.Status.Phase, state.Status.Step, state.Status.Conditions, state.Intent)
	return state
}

func diagPolicy() v1.WindowPolicy {
	return v1.WindowPolicy{
		Seconds:               1,
		MinSamples:            1,
		MaxErrorBPS:           500,
		MaxErrorDeltaBPS:      500,
		MaxMeanLatencyDeltaMS: 50,
	}
}

func diagCondition(state v1.ReleaseState, kind string) *v1.Condition {
	for i := range state.Status.Conditions {
		if state.Status.Conditions[i].Type == kind {
			return &state.Status.Conditions[i]
		}
	}
	return nil
}

func diagReadyUIDs(h *diagHarness, service v1.Service, revision string) []string {
	var uids []string
	for _, endpoint := range h.app.Index.Snapshot(service).Ready {
		if endpoint.Revision == revision {
			uids = append(uids, endpoint.UID)
		}
	}
	return uids
}

func diagBudgetCase(t *testing.T, ctx context.Context, dir string) map[string]any {
	t.Helper()
	h := diagOpen(t, ctx, dir, quota.Limit{Releases: 1, CandidateUnits: 1})
	defer h.close()

	firstService := v1.Service{Namespace: "shop", Name: "pay"}
	first := v1.ReleaseSpec{
		APIVersion: v1.Version, Service: firstService, Name: "pay-r1", UID: "budget-first-uid",
		Stable: "rev-a", Candidate: "rev-b", Steps: []int{10}, Window: diagPolicy(), MinReady: 1,
	}
	first = h.put(first)
	h.until(first, func(state v1.ReleaseState) bool { return state.Reservation != nil })

	blockedService := v1.Service{Namespace: "shop", Name: "checkout"}
	blocked := v1.ReleaseSpec{
		APIVersion: v1.Version, Service: blockedService, Name: "checkout-r2", UID: "budget-blocked-uid",
		Stable: "rev-a", Candidate: "rev-c", Steps: []int{10}, Window: diagPolicy(), MinReady: 1,
	}
	h.endpoint(blockedService, "rev-a", "budget-stable", true)
	h.endpoint(blockedService, "rev-c", "budget-candidate", true)
	h.sync()
	blocked = h.put(blocked)
	state := h.until(blocked, func(state v1.ReleaseState) bool {
		condition := diagCondition(state, "Admitted")
		return condition != nil && condition.Reason == "NamespaceBudget"
	})
	binding, exists, err := h.app.Plane.Observe(blockedService)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatalf("budget-blocked release unexpectedly installed a binding: %+v", binding)
	}
	return map[string]any{
		"expectedDiscriminator": "Admitted=False/NamespaceBudget; quota is exhausted; no binding for this service",
		"quota":                 h.app.Engine.Usage("shop"),
		"condition":             diagCondition(state, "Admitted"),
		"readyStableUIDs":       diagReadyUIDs(h, blockedService, "rev-a"),
		"readyCandidateUIDs":    diagReadyUIDs(h, blockedService, "rev-c"),
		"trafficBindingExists":  false,
	}
}

func diagNotReadyCase(t *testing.T, ctx context.Context, dir string) map[string]any {
	t.Helper()
	h := diagOpen(t, ctx, dir, quota.Limit{Releases: 5, CandidateUnits: 10})
	defer h.close()

	service := v1.Service{Namespace: "shop", Name: "search"}
	spec := v1.ReleaseSpec{
		APIVersion: v1.Version, Service: service, Name: "search-r1", UID: "not-ready-uid",
		Stable: "rev-a", Candidate: "rev-b", Steps: []int{10}, Window: diagPolicy(), MinReady: 1,
	}
	h.endpoint(service, "rev-a", "ready-stable-uid", true)
	h.endpoint(service, "rev-b", "not-ready-candidate-uid", false)
	h.sync()
	spec = h.put(spec)
	h.until(spec, func(state v1.ReleaseState) bool { return state.Binding != nil })
	state := h.until(spec, func(state v1.ReleaseState) bool {
		condition := diagCondition(state, "Progressing")
		return condition != nil && condition.Reason == "CandidateBelowMinimum"
	})
	binding, exists, err := h.app.Plane.Observe(service)
	if err != nil || !exists {
		t.Fatalf("expected stable-only binding, exists=%v err=%v", exists, err)
	}
	if binding.Rule.CandidateWeight != 0 || len(binding.Rule.CandidateTargets) != 0 {
		t.Fatalf("not-ready candidate received routes: %+v", binding.Rule)
	}
	list, err := h.app.Catalog.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return map[string]any{
		"expectedDiscriminator": "Admitted=True and budget remains; Progressing=False/CandidateBelowMinimum; live weight stays zero",
		"quota":                 h.app.Engine.Usage("shop"),
		"condition":             diagCondition(state, "Progressing"),
		"endpoints":             list.Items,
		"readyStableUIDs":       diagReadyUIDs(h, service, "rev-a"),
		"readyCandidateUIDs":    diagReadyUIDs(h, service, "rev-b"),
		"trafficBinding":        binding,
	}
}

func diagMismatchCase(t *testing.T, ctx context.Context, root string) map[string]any {
	t.Helper()
	blockedStateDir := filepath.Join(root, "mismatch-blocked", "state")
	h := diagOpen(t, ctx, blockedStateDir, quota.Limit{Releases: 5, CandidateUnits: 10})
	defer func() { h.close() }()

	service := v1.Service{Namespace: "shop", Name: "checkout"}
	old := v1.ReleaseSpec{
		APIVersion: v1.Version, Service: service, Name: "checkout-r1", UID: "checkout-r1-owner-uid",
		Stable: "rev-2026-11-13", Candidate: "rev-2026-11-20", Steps: []int{10},
		Window: diagPolicy(), MinReady: 1,
	}
	h.endpoint(service, old.Stable, "checkout-old-stable", true)
	h.endpoint(service, old.Candidate, "checkout-promoted-canary", true)
	h.sync()
	old = h.put(old)

	weighted := h.until(old, func(state v1.ReleaseState) bool {
		return state.Intent == nil && state.Window != nil && state.Status.BindingVersion != 0
	})
	weightedBinding, exists, err := h.app.Plane.Observe(service)
	if err != nil || !exists {
		t.Fatalf("weighted binding missing: exists=%v err=%v", exists, err)
	}
	for _, sample := range []v1.Sample{
		{
			APIVersion: v1.Version, Service: service, BindingVersion: weighted.Status.BindingVersion,
			ID: "diag-stable-sample", Role: v1.StableRole, At: weightedBinding.AppliedAt, LatencyMS: 1,
		},
		{
			APIVersion: v1.Version, Service: service, BindingVersion: weighted.Status.BindingVersion,
			ID: "diag-candidate-sample", Role: v1.CandidateRole, At: weightedBinding.AppliedAt, LatencyMS: 1,
		},
	} {
		if _, err := h.app.Metrics.Add(sample); err != nil {
			t.Fatal(err)
		}
	}
	h.clk.Advance(old.Window.Duration() + time.Second)
	promoted := h.until(old, func(state v1.ReleaseState) bool {
		return state.Status.Phase == v1.Promoted
	})
	lastBinding, exists, err := h.app.Plane.Observe(service)
	if err != nil || !exists {
		t.Fatalf("promoted binding missing: exists=%v err=%v", exists, err)
	}
	if lastBinding.OwnerUID != old.UID || lastBinding.Rule.Stable != old.Candidate || lastBinding.Rule.CandidateWeight != 0 {
		t.Fatalf("unexpected last committed binding: %+v", lastBinding)
	}

	// The next release names the pre-promotion revision as its stable baseline.
	wrong := v1.ReleaseSpec{
		APIVersion: v1.Version, Service: service, Name: "checkout-r2-wrong-baseline", UID: "checkout-r2-wrong-uid",
		Stable: old.Stable, Candidate: "rev-2026-11-27", Steps: []int{10},
		Window: diagPolicy(), MinReady: 1,
	}
	h.endpoint(service, wrong.Candidate, "checkout-next-canary-ready", true)
	h.sync()
	wrong = h.put(wrong)
	beforeRestart := h.until(wrong, func(state v1.ReleaseState) bool {
		condition := diagCondition(state, "Admitted")
		return condition != nil && condition.Reason == "StableBindingMismatch"
	})

	ownerKey := ""
	releases, err := h.app.Specs.List()
	if err != nil {
		t.Fatal(err)
	}
	for _, view := range releases.Items {
		if view.Spec.UID == lastBinding.OwnerUID {
			ownerKey = view.Spec.Key()
		}
	}
	oldReceipt, err := h.app.Journal.Receipt(old.UID, h.clk.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.VerifyReceipt(oldReceipt); err != nil {
		t.Fatal(err)
	}
	receiptData, err := json.MarshalIndent(oldReceipt, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "old-owner.receipt.v1.json"), receiptData, 0o640); err != nil {
		t.Fatal(err)
	}
	receiptBefore := map[string]any{
		"releaseUid":         oldReceipt.ReleaseUID,
		"lastDecision":       oldReceipt.LastDecision,
		"digest":             oldReceipt.Digest,
		"decisionSequences":  diagReceiptSequences(oldReceipt),
		"verifyBeforeChange": "OK",
	}

	bindingBeforeRestart := lastBinding
	recovery := h.restart()
	afterRestartReconcileCount := 3
	for i := 0; i < afterRestartReconcileCount; i++ {
		h.reconcile(wrong)
	}
	afterRestart, exists, err := h.app.Journal.State(wrong.UID)
	if err != nil || !exists {
		t.Fatalf("wrong successor state missing after restart: exists=%v err=%v", exists, err)
	}
	bindingAfterRestart, exists, err := h.app.Plane.Observe(service)
	if err != nil || !exists {
		t.Fatalf("binding missing after restart: exists=%v err=%v", exists, err)
	}
	if !reflect.DeepEqual(bindingBeforeRestart, bindingAfterRestart) {
		t.Fatalf("binding changed across restart:\nbefore=%+v\nafter=%+v", bindingBeforeRestart, bindingAfterRestart)
	}
	afterCondition := diagCondition(afterRestart, "Admitted")
	if afterCondition == nil || afterCondition.Reason != "StableBindingMismatch" {
		t.Fatalf("mismatch did not reproduce after restart: %+v", afterRestart.Status.Conditions)
	}
	blockedQuota := h.app.Engine.Usage(service.Namespace)

	rewrite := wrong
	rewrite.Stable = bindingAfterRestart.Rule.Stable
	_, rewriteErr := h.app.Specs.Put(rewrite)
	if rewriteErr == nil || !errors.Is(rewriteErr, v1.ErrInvalid) {
		t.Fatalf("same-name stable rewrite should be rejected as immutable, got %v", rewriteErr)
	}

	// Freeze the still-blocked, restarted state, then work on a copy so the
	// external inspect command can revisit the exact StableBindingMismatch scene.
	h.close()
	successorStateDir := filepath.Join(root, "mismatch-successor", "state")
	diagCopyDir(t, blockedStateDir, successorStateDir)
	successorCfg := h.cfg
	successorCfg.StateDirectory = successorStateDir
	successorApp, err := Open(successorCfg, Options{Clock: h.clk})
	if err != nil {
		t.Fatal(err)
	}
	h = &diagHarness{t: t, ctx: ctx, cfg: successorCfg, clk: h.clk, app: successorApp, dir: successorStateDir}
	h.sync()

	// Correct successor declaration: copy stable from the actual live binding.
	correct := v1.ReleaseSpec{
		APIVersion: v1.Version, Service: service, Name: "checkout-r2-successor", UID: "checkout-r2-successor-uid",
		Stable: bindingAfterRestart.Rule.Stable, Candidate: wrong.Candidate, Steps: []int{10},
		Window: diagPolicy(), MinReady: 1,
	}
	correct = h.put(correct)
	correctState := h.until(correct, func(state v1.ReleaseState) bool {
		return state.Binding != nil && state.Binding.OwnerUID == correct.UID &&
			state.Binding.Rule.CandidateWeight == correct.Steps[0]
	})
	newBinding, exists, err := h.app.Plane.Observe(service)
	if err != nil || !exists {
		t.Fatalf("correct successor binding missing: exists=%v err=%v", exists, err)
	}
	if newBinding.ResourceVersion <= lastBinding.ResourceVersion || newBinding.OwnerUID != correct.UID {
		t.Fatalf("successor did not CAS from the old binding: old=%+v new=%+v", lastBinding, newBinding)
	}
	receiptAfter, err := h.app.Journal.Receipt(old.UID, h.clk.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.VerifyReceipt(receiptAfter); err != nil {
		t.Fatal(err)
	}
	oldReceiptUnchanged := reflect.DeepEqual(oldReceipt, receiptAfter)
	if !oldReceiptUnchanged {
		t.Fatalf("old receipt changed after successor declaration:\nbefore=%+v\nafter=%+v", oldReceipt, receiptAfter)
	}

	return map[string]any{
		"expectedDiscriminator":      "budget remains and candidate is ready, but Admitted=False/StableBindingMismatch against the last committed binding",
		"service":                    service,
		"quotaWhileBlocked":          blockedQuota,
		"quotaAfterCorrectSuccessor": h.app.Engine.Usage(service.Namespace),
		"readyCandidateUIDs":         diagReadyUIDs(h, service, wrong.Candidate),
		"wrongSuccessor": map[string]any{
			"name":      wrong.Name,
			"uid":       wrong.UID,
			"stable":    wrong.Stable,
			"candidate": wrong.Candidate,
			"condition": diagCondition(beforeRestart, "Admitted"),
			"sameNameStableRewriteRejected": map[string]any{
				"attemptedStable": bindingAfterRestart.Rule.Stable,
				"error":           rewriteErr.Error(),
			},
		},
		"lastActuallyCommittedBinding": lastBinding,
		"owner": map[string]any{
			"uid":          lastBinding.OwnerUID,
			"releaseKey":   ownerKey,
			"phase":        promoted.Status.Phase,
			"lastDecision": promoted.Status.DecisionSequence,
		},
		"restart": map[string]any{
			"stateDirectory":           blockedStateDir,
			"successorStateDirectory":  successorStateDir,
			"inspectRecovery":          recovery,
			"bindingResourceVersion":   bindingAfterRestart.ResourceVersion,
			"bindingOperation":         bindingAfterRestart.Operation,
			"bindingOwnerUid":          bindingAfterRestart.OwnerUID,
			"conditionAfterReconciles": afterCondition,
			"bindingDeepUnchanged":     true,
		},
		"correctSuccessorDeclaration": map[string]any{
			"action":       "PUT a new release name/UID with stable copied from lastActuallyCommittedBinding.rule.stable; immutable old release is not updated",
			"name":         correct.Name,
			"uid":          correct.UID,
			"stable":       correct.Stable,
			"candidate":    correct.Candidate,
			"phase":        correctState.Status.Phase,
			"step":         correctState.Status.Step,
			"newBinding":   newBinding,
			"ownerChanged": newBinding.OwnerUID == correct.UID,
		},
		"oldReceipt": map[string]any{
			"file":                          filepath.Join(root, "old-owner.receipt.v1.json"),
			"before":                        receiptBefore,
			"deepUnchangedAfterSuccessor":   oldReceiptUnchanged,
			"verifyAfterSuccessor":          "OK",
			"oldBindingStillInReceipt":      diagReceiptHasBinding(oldReceipt, lastBinding),
			"successorAbsentFromOldReceipt": !diagReceiptHasUID(oldReceipt, correct.UID),
		},
	}
}

func diagCopyDir(t *testing.T, source, destination string) {
	t.Helper()
	err := filepath.WalkDir(source, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o750)
		}
		if entry.Name() == ".wavegate.lock" {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, info.Mode().Perm())
	})
	if err != nil {
		t.Fatal(err)
	}
}

func diagReceiptSequences(receipt journal.Receipt) []uint64 {
	sequences := make([]uint64, 0, len(receipt.Decisions))
	for _, record := range receipt.Decisions {
		sequences = append(sequences, record.Sequence)
	}
	return sequences
}

func diagReceiptHasBinding(receipt journal.Receipt, binding v1.TrafficBinding) bool {
	for _, record := range receipt.Decisions {
		if committed := record.State.Binding; committed != nil &&
			committed.ResourceVersion == binding.ResourceVersion &&
			committed.Operation == binding.Operation &&
			committed.OwnerUID == binding.OwnerUID {
			return true
		}
	}
	return false
}

func diagReceiptHasUID(receipt journal.Receipt, uid string) bool {
	for _, record := range receipt.Decisions {
		if record.State.Spec.UID == uid {
			return true
		}
	}
	return false
}
