package v1

import "testing"

func TestReadinessPhaseResolution(t *testing.T) {
	cases := []struct {
		name  string
		input ReplicaObservation
		want  string
	}{
		{"v1 ready", ReplicaObservation{Ready: true}, PhaseServing},
		{"v1 not ready", ReplicaObservation{Ready: false}, PhaseUnknown},
		{"explicit serving", ReplicaObservation{Ready: false, Phase: PhaseServing}, PhaseServing},
		{"explicit terminating", ReplicaObservation{Ready: true, Phase: PhaseTerminating}, PhaseTerminating},
		{"explicit unknown", ReplicaObservation{Ready: true, Phase: PhaseUnknown}, PhaseUnknown},
	}
	for _, tc := range cases {
		if got := tc.input.ReadinessPhase(); got != tc.want {
			t.Errorf("%s: ReadinessPhase()=%q want %q", tc.name, got, tc.want)
		}
		if tc.want != PhaseServing && tc.input.Serving() {
			t.Errorf("%s: Serving()=true for %q", tc.name, tc.want)
		}
		if tc.want == PhaseTerminating && !tc.input.Terminating() {
			t.Errorf("%s: Terminating()=false", tc.name)
		}
	}
}

func TestNormalizedKeepsV1Shape(t *testing.T) {
	legacy := ReplicaObservation{APIVersion: Version, Ready: true}
	if got := legacy.Normalized(); got != legacy {
		t.Fatalf("v1 observation rewritten: %+v", got)
	}
	explicit := ReplicaObservation{APIVersion: VersionV2, Phase: PhaseServing, Ready: false}
	if got := explicit.Normalized(); !got.Ready {
		t.Fatalf("explicit phase did not normalize Ready: %+v", got)
	}
	terminating := ReplicaObservation{APIVersion: VersionV2, Phase: PhaseTerminating, Ready: true}
	if got := terminating.Normalized(); got.Ready {
		t.Fatalf("terminating phase did not normalize Ready: %+v", got)
	}
}

func TestEndpointVersionFences(t *testing.T) {
	for _, version := range []string{Version, VersionV2} {
		if err := CheckEndpointVersion(version); err != nil {
			t.Errorf("endpoint version %q rejected: %v", version, err)
		}
	}
	if err := CheckEndpointVersion("wavegate.io/v3"); err == nil {
		t.Fatal("unknown endpoint version accepted")
	}
}

func TestV2ValidationRequiresKnownPhase(t *testing.T) {
	base := func() ReplicaObservation {
		return ReplicaObservation{
			APIVersion: VersionV2, Service: Service{Namespace: "shop", Name: "api"},
			Node: "node-a", Name: "pod", UID: "uid", Revision: "rev", Address: "http://127.0.0.1:8080",
		}
	}
	cases := []string{"", "Ready", "serving"}
	for _, phase := range cases {
		endpoint := base()
		endpoint.Phase = phase
		if err := endpoint.Validate(); err == nil {
			t.Errorf("v2 phase %q accepted", phase)
		}
	}
	for _, phase := range []string{PhaseUnknown, PhaseServing, PhaseTerminating} {
		endpoint := base()
		endpoint.Phase = phase
		if err := endpoint.Validate(); err != nil {
			t.Errorf("v2 phase %q rejected: %v", phase, err)
		}
	}
}
