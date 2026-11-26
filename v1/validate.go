package v1

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"time"
)

var (
	ErrInvalid     = errors.New("invalid resource")
	ErrConflict    = errors.New("resource version conflict")
	ErrNotFound    = errors.New("resource not found")
	ErrExpired     = errors.New("resource version expired")
	ErrUnavailable = errors.New("observation unavailable")
	label          = regexp.MustCompile("^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$")
	token          = regexp.MustCompile("^[A-Za-z0-9][A-Za-z0-9_.-]{0,95}$")
)

func Invalid(field string) error {
	return fmt.Errorf("%w: %s", ErrInvalid, field)
}

func ValidLabel(value string) bool { return label.MatchString(value) }
func ValidToken(value string) bool { return token.MatchString(value) }

func CheckVersion(value string) error {
	if value != Version {
		return Invalid("apiVersion must be " + Version)
	}
	return nil
}

func (s Service) Validate() error {
	if !ValidLabel(s.Namespace) || !ValidLabel(s.Name) {
		return Invalid("service namespace and name must be DNS labels")
	}
	return nil
}

func (r ReleaseSpec) Validate() error {
	if err := CheckVersion(r.APIVersion); err != nil {
		return err
	}
	if err := r.Service.Validate(); err != nil {
		return err
	}
	if !ValidLabel(r.Name) || !ValidToken(r.UID) {
		return Invalid("release name or uid")
	}
	if !ValidToken(r.Stable) || !ValidToken(r.Candidate) || r.Stable == r.Candidate {
		return Invalid("stable and candidate must be distinct revisions")
	}
	if len(r.Steps) == 0 || len(r.Steps) > 16 {
		return Invalid("steps must contain between 1 and 16 weights")
	}
	last := 0
	for _, weight := range r.Steps {
		if weight <= last || weight >= 100 {
			return Invalid("steps must increase strictly within 1..99")
		}
		last = weight
	}
	if r.MinReady < 1 || r.MinReady > 1000 {
		return Invalid("minReady must be within 1..1000")
	}
	if err := validateDomains(r.Domains); err != nil {
		return err
	}
	return r.Window.Validate()
}

func validateDomains(domains []DomainSpec) error {
	if len(domains) > 64 {
		return Invalid("a release may declare at most 64 node domains")
	}
	owner := make(map[string]string)
	names := make(map[string]bool)
	for _, domain := range domains {
		if !ValidLabel(domain.Name) || len(domain.Nodes) == 0 || len(domain.Nodes) > 1000 {
			return Invalid("node domain name or members")
		}
		if names[domain.Name] {
			return Invalid("duplicate node domain")
		}
		names[domain.Name] = true
		for _, node := range domain.Nodes {
			if !ValidLabel(node) {
				return Invalid("node domain member")
			}
			if previous, exists := owner[node]; exists && previous != domain.Name {
				return Invalid("node belongs to more than one domain")
			}
			owner[node] = domain.Name
		}
	}
	return nil
}

func (p WindowPolicy) Validate() error {
	if p.Seconds < 1 || p.Seconds > 3600 || p.MinSamples < 1 || p.MinSamples > 10000 {
		return Invalid("window duration or sample minimum")
	}
	if p.MaxErrorBPS < 0 || p.MaxErrorBPS > 10000 || p.MaxErrorDeltaBPS < 0 || p.MaxErrorDeltaBPS > 10000 {
		return Invalid("window error bounds")
	}
	if p.MaxMeanLatencyDeltaMS < 0 || p.MaxMeanLatencyDeltaMS > 600000 {
		return Invalid("window latency bound")
	}
	return nil
}

func (p WindowPolicy) Duration() time.Duration { return time.Duration(p.Seconds) * time.Second }

func (r ReplicaObservation) Validate() error {
	if err := CheckVersion(r.APIVersion); err != nil {
		return err
	}
	if err := r.Service.Validate(); err != nil {
		return err
	}
	if !ValidLabel(r.Node) || !ValidLabel(r.Name) || !ValidToken(r.UID) || !ValidToken(r.Revision) {
		return Invalid("endpoint identity or revision")
	}
	address, err := url.Parse(r.Address)
	if err != nil || address.Scheme != "http" || address.User != nil ||
		address.Fragment != "" || address.RawQuery != "" || address.ForceQuery || (address.Path != "" && address.Path != "/") {
		return Invalid("address must be an HTTP origin")
	}
	ip := net.ParseIP(address.Hostname())
	if ip == nil || !ip.IsLoopback() {
		return Invalid("fake endpoints require a numeric loopback address")
	}
	port, err := strconv.Atoi(address.Port())
	if err != nil || port < 1 || port > 65535 {
		return Invalid("address requires a valid port")
	}
	return nil
}

func (r Rule) Validate() error {
	if err := r.Service.Validate(); err != nil {
		return err
	}
	if !ValidToken(r.Stable) || r.CandidateWeight < 0 || r.CandidateWeight >= 100 {
		return Invalid("rule revision or weight")
	}
	if r.Candidate == "" {
		if r.CandidateWeight != 0 || len(r.CandidateTargets) != 0 {
			return Invalid("candidate route without a revision")
		}
	} else if !ValidToken(r.Candidate) || r.Candidate == r.Stable {
		return Invalid("candidate revision")
	}
	if (len(r.DrainTargets) == 0) != r.DrainUntil.IsZero() {
		return Invalid("incomplete drain window")
	}
	if err := validateDomainRoutes(r); err != nil {
		return err
	}
	if len(r.DomainRoutes) == 0 && r.CandidateWeight > 0 &&
		(len(r.StableTargets) == 0 || len(r.CandidateTargets) == 0) {
		return Invalid("a weighted rule needs both target groups")
	}
	seen := map[string]bool{}
	for _, group := range [][]Target{r.StableTargets, r.CandidateTargets, r.DrainTargets} {
		for _, target := range group {
			if !ValidLabel(target.Node) || !ValidLabel(target.Name) || !ValidToken(target.UID) || seen[target.Key()+"/"+target.UID] {
				return Invalid("invalid or duplicate target")
			}
			seen[target.Key()+"/"+target.UID] = true
		}
	}
	return nil
}

func validateDomainRoutes(r Rule) error {
	names := make(map[string]bool, len(r.DomainRoutes))
	sorted := true
	for index, route := range r.DomainRoutes {
		if !ValidLabel(route.Name) || route.CandidateWeight < 0 || route.CandidateWeight >= 100 {
			return Invalid("domain route name or weight")
		}
		if index > 0 && r.DomainRoutes[index-1].Name >= route.Name {
			sorted = false
		}
		names[route.Name] = true
	}
	if !sorted {
		return Invalid("domain routes must be sorted with unique names")
	}
	if len(r.DomainRoutes) == 0 {
		for _, group := range [][]Target{r.StableTargets, r.CandidateTargets, r.DrainTargets} {
			for _, target := range group {
				if target.Domain != "" {
					return Invalid("target names a domain but the rule has no domain routes")
				}
			}
		}
		return nil
	}
	candidateByDomain := make(map[string]int, len(r.DomainRoutes))
	stableByDomain := make(map[string]int, len(r.DomainRoutes))
	for _, group := range [][]Target{r.StableTargets, r.CandidateTargets, r.DrainTargets} {
		for _, target := range group {
			if !ValidLabel(target.Domain) || !names[target.Domain] {
				return Invalid("domain routed target requires a declared domain")
			}
		}
	}
	for _, target := range r.CandidateTargets {
		candidateByDomain[target.Domain]++
	}
	for _, target := range r.StableTargets {
		stableByDomain[target.Domain]++
	}
	// A domain that carries the step's candidate weight must also carry that
	// domain's own stable and ready candidate targets; a domain with no candidates
	// stays at zero and keeps serving its own stable revision. Every non-zero
	// route must agree on the same step weight.
	stepWeight := 0
	for _, route := range r.DomainRoutes {
		if route.CandidateWeight == 0 {
			continue
		}
		if stepWeight == 0 {
			stepWeight = route.CandidateWeight
		} else if route.CandidateWeight != stepWeight {
			return Invalid("domain routes disagree on the step weight")
		}
		if candidateByDomain[route.Name] == 0 {
			return Invalid("weighted domain route has no candidate target")
		}
		if stableByDomain[route.Name] == 0 {
			return Invalid("weighted domain route has no stable target")
		}
	}
	if r.CandidateWeight != stepWeight {
		return Invalid("rule candidate weight disagrees with its domain routes")
	}
	return nil
}

// HasCandidateTraffic reports whether the rule serves any candidate traffic,
// either across the whole service or through one of its node domains.
func (r Rule) HasCandidateTraffic() bool {
	if r.CandidateWeight > 0 {
		return true
	}
	for _, route := range r.DomainRoutes {
		if route.CandidateWeight > 0 {
			return true
		}
	}
	return false
}

// StepWeight is the candidate weight the rule currently serves for a step. For
// a domain-scoped rule this is the weight shared by its routed domains.
func (r Rule) StepWeight() int {
	if r.CandidateWeight > 0 {
		return r.CandidateWeight
	}
	for _, route := range r.DomainRoutes {
		if route.CandidateWeight > 0 {
			return route.CandidateWeight
		}
	}
	return 0
}

// DomainReady reports whether any of the rule's domains carries candidate
// targets satisfying the release's minimum ready count.
func (r Rule) DomainReady(minReady int) bool {
	candidateByDomain := make(map[string]int, len(r.DomainRoutes))
	stableByDomain := make(map[string]bool, len(r.DomainRoutes))
	for _, target := range r.StableTargets {
		stableByDomain[target.Domain] = true
	}
	for _, target := range r.CandidateTargets {
		candidateByDomain[target.Domain]++
	}
	for _, route := range r.DomainRoutes {
		if stableByDomain[route.Name] && candidateByDomain[route.Name] >= minReady {
			return true
		}
	}
	return false
}

func (s Sample) Validate() error {
	if err := CheckVersion(s.APIVersion); err != nil {
		return err
	}
	if err := s.Service.Validate(); err != nil {
		return err
	}
	if s.BindingVersion == 0 || !ValidToken(s.ID) || s.At.IsZero() || s.LatencyMS < 0 || s.LatencyMS > 600000 {
		return Invalid("sample binding, id, time, or latency")
	}
	if s.Role != StableRole && s.Role != CandidateRole {
		return Invalid("sample role")
	}
	return nil
}

func (w WindowKey) Validate() error {
	if err := w.Service.Validate(); err != nil {
		return err
	}
	if w.BindingVersion == 0 || w.Start.IsZero() || !w.End.After(w.Start) || w.End.Sub(w.Start) > time.Hour {
		return Invalid("window binding or range")
	}
	return nil
}
