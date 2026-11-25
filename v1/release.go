package v1

import "slices"

func SameReleaseDefinition(a, b ReleaseSpec) bool {
	return a.APIVersion == b.APIVersion && a.Service == b.Service && a.Name == b.Name &&
		a.UID == b.UID && a.Stable == b.Stable && a.Candidate == b.Candidate &&
		a.Window == b.Window && a.MinReady == b.MinReady && slices.Equal(a.Steps, b.Steps)
}
