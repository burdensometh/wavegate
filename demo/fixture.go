package demo

import (
	"os"

	"wavegate/v1"
)

type Fixture struct {
	APIVersion       string                  `json:"apiVersion"`
	Primary          v1.ReleaseSpec          `json:"primary"`
	Queued           v1.ReleaseSpec          `json:"queued"`
	Isolated         v1.ReleaseSpec          `json:"isolated"`
	Endpoints        []v1.ReplicaObservation `json:"endpoints"`
	FailingEndpoints []string                `json:"failingEndpoints"`
}

func readFixture(path string) (Fixture, error) {
	var fixture Fixture
	file, err := os.Open(path)
	if err != nil {
		return fixture, err
	}
	defer file.Close()
	if err := v1.Decode(file, &fixture); err != nil {
		return fixture, err
	}
	if err := v1.CheckVersion(fixture.APIVersion); err != nil {
		return fixture, err
	}
	for _, spec := range []v1.ReleaseSpec{fixture.Primary, fixture.Queued, fixture.Isolated} {
		if err := spec.Validate(); err != nil {
			return fixture, err
		}
		if spec.ResourceVersion != 0 || spec.Abort || spec.Paused {
			return fixture, v1.Invalid("sample releases must be new and running")
		}
	}
	a, b, c := fixture.Primary, fixture.Queued, fixture.Isolated
	if len(a.Steps) != 2 || a.Stable != b.Stable || a.Stable != c.Stable || a.Candidate != b.Candidate || a.Candidate != c.Candidate ||
		a.Service.Namespace != b.Service.Namespace || a.Service.Namespace == c.Service.Namespace || a.Service == b.Service {
		return fixture, v1.Invalid("demo fixture requires two same-namespace releases and an isolated release")
	}
	seen := make(map[string]bool)
	keys := make(map[string]bool)
	for _, endpoint := range fixture.Endpoints {
		if err := endpoint.Validate(); err != nil {
			return fixture, err
		}
		if endpoint.ResourceVersion != 0 || seen[endpoint.UID] || keys[endpoint.Key()] {
			return fixture, v1.Invalid("sample endpoints must have unique new identities")
		}
		seen[endpoint.UID], keys[endpoint.Key()] = true, true
	}
	for _, uid := range fixture.FailingEndpoints {
		if !seen[uid] {
			return fixture, v1.Invalid("fixture failure references an absent endpoint")
		}
	}
	return fixture, nil
}
