package v1

// Endpoint readiness phases extend the historical Ready boolean.
//
// A wavegate.io/v1 observation keeps the original boolean meaning: an empty
// Phase maps to Serving when Ready is true and Unknown otherwise. A
// wavegate.io/v2 observation always carries an explicit, validated phase so
// that "unknown", "serving" and "terminating" stay distinguishable. Whenever
// Phase is present it is authoritative, which lets a newer source be observed
// through the old wire shape without being reinterpreted by the legacy flag.
const (
	PhaseUnknown     = "Unknown"
	PhaseServing     = "Serving"
	PhaseTerminating = "Terminating"
)

// VersionV2 is the endpoint resource version that requires an explicit phase.
// Release, traffic, sample and journal documents remain on Version.
const VersionV2 = "wavegate.io/v2"

// KnownReadiness reports whether phase is one of the explicit readiness phases.
func KnownReadiness(phase string) bool {
	switch phase {
	case PhaseUnknown, PhaseServing, PhaseTerminating:
		return true
	}
	return false
}

// CheckEndpointVersion accepts either endpoint resource version. Non-endpoint
// documents keep using the strict CheckVersion.
func CheckEndpointVersion(value string) error {
	if value != Version && value != VersionV2 {
		return Invalid("endpoint apiVersion must be " + Version + " or " + VersionV2)
	}
	return nil
}

// ReadinessPhase resolves an observation to a single readiness phase. An
// explicit Phase wins; otherwise the legacy boolean decides.
func (r ReplicaObservation) ReadinessPhase() string {
	if r.Phase != "" {
		return r.Phase
	}
	if r.Ready {
		return PhaseServing
	}
	return PhaseUnknown
}

// Serving reports whether a new request may be sent to this endpoint. An
// unknown or terminating endpoint never serves.
func (r ReplicaObservation) Serving() bool { return r.ReadinessPhase() == PhaseServing }

// Terminating reports an endpoint that is winding down. It may still be
// observed and drained, but it never takes new traffic and never displaces a
// newer UID at the same node/name.
func (r ReplicaObservation) Terminating() bool { return r.ReadinessPhase() == PhaseTerminating }

// Normalized keeps the legacy Ready boolean consistent with an explicit phase.
// It leaves a v1 observation untouched so its original wire representation and
// boolean result are preserved across persistence and watch replay.
func (r ReplicaObservation) Normalized() ReplicaObservation {
	if r.Phase != "" {
		r.Ready = r.Phase == PhaseServing
	}
	return r
}
