package traffic

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"wavegate/internal/statefile"
	"wavegate/v1"
)

type ApplyRequest struct {
	OwnerUID string
	Intent   v1.Intent
	At       time.Time
}

type Plane interface {
	Observe(v1.Service) (v1.TrafficBinding, bool, error)
	Apply(context.Context, ApplyRequest) (v1.TrafficBinding, error)
}

type document struct {
	APIVersion string                       `json:"apiVersion"`
	Version    uint64                       `json:"resourceVersion"`
	Bindings   map[string]v1.TrafficBinding `json:"bindings"`
}

type Store struct {
	mu        sync.RWMutex
	path      string
	doc       document
	uncertain error
}

func Open(path string) (*Store, error) {
	store := &Store{path: path, doc: document{APIVersion: v1.Version, Bindings: make(map[string]v1.TrafficBinding)}}
	var recovered document
	if found, err := statefile.Read(path, &recovered); err != nil {
		return nil, err
	} else if found {
		store.doc = recovered
	}
	if err := store.validate(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *Store) validate() error {
	if err := v1.CheckVersion(s.doc.APIVersion); err != nil {
		return err
	}
	if s.doc.Bindings == nil {
		return v1.Invalid("traffic bindings inventory")
	}
	seen := make(map[uint64]bool)
	for key, binding := range s.doc.Bindings {
		if err := ValidateBinding(binding); err != nil {
			return err
		}
		if key != binding.Rule.Service.String() || binding.ResourceVersion > s.doc.Version || seen[binding.ResourceVersion] {
			return v1.Invalid("traffic binding identity or high water mark")
		}
		seen[binding.ResourceVersion] = true
	}
	if s.doc.Version > 0 && !seen[s.doc.Version] {
		return v1.Invalid("traffic high water mark has no binding")
	}
	return nil
}

func ValidateBinding(binding v1.TrafficBinding) error {
	if err := v1.CheckVersion(binding.APIVersion); err != nil {
		return err
	}
	if !v1.ValidToken(binding.OwnerUID) || !v1.ValidToken(binding.Operation) || binding.AppliedAt.IsZero() || binding.ResourceVersion == 0 {
		return v1.Invalid("traffic binding ownership, operation or version")
	}
	if err := validateIdentity(binding.Identity, binding.Operation, binding.OwnerUID, binding.Rule.Service); err != nil {
		return err
	}
	return binding.Rule.Validate()
}

// validateIdentity checks a composite operation identity when present. Legacy
// bindings and intents carry a nil identity and keep relying on the bare
// operation token. A present identity must be internally consistent: its
// external flow is the binding's operation and its service/owner match the
// rule service and the binding owner, which is what scopes acknowledgements.
func validateIdentity(identity *v1.OperationIdentity, operation, owner string, service v1.Service) error {
	if identity == nil {
		return nil
	}
	if err := identity.Validate(); err != nil {
		return err
	}
	if identity.External != operation || identity.OwnerUID != owner || identity.Service != service {
		return v1.Invalid("traffic operation identity does not match its binding")
	}
	return nil
}

func (s *Store) Observe(service v1.Service) (v1.TrafficBinding, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.uncertain != nil {
		return v1.TrafficBinding{}, false, s.uncertain
	}
	binding, exists := s.doc.Bindings[service.String()]
	binding.Rule = cloneRule(binding.Rule)
	return binding, exists, nil
}

func (s *Store) Bindings() ([]v1.TrafficBinding, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.uncertain != nil {
		return nil, s.uncertain
	}
	list := make([]v1.TrafficBinding, 0, len(s.doc.Bindings))
	for _, binding := range s.doc.Bindings {
		binding.Rule = cloneRule(binding.Rule)
		list = append(list, binding)
	}
	slices.SortFunc(list, func(a, b v1.TrafficBinding) int {
		return cmp.Compare(a.Rule.Service.String(), b.Rule.Service.String())
	})
	return list, nil
}

// WithCurrent admits a request against the binding it was routed from.
// Callers acquiring an endpoint fence take that fence first.
func (s *Store) WithCurrent(service v1.Service, version uint64, admit func()) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.uncertain != nil {
		return s.uncertain
	}
	if version == 0 || s.doc.Bindings[service.String()].ResourceVersion != version {
		return v1.ErrConflict
	}
	admit()
	return nil
}

func (s *Store) Apply(ctx context.Context, request ApplyRequest) (v1.TrafficBinding, error) {
	if err := request.Intent.Rule.Validate(); err != nil {
		return v1.TrafficBinding{}, err
	}
	if !v1.ValidToken(request.OwnerUID) || !v1.ValidToken(request.Intent.Operation) || request.At.IsZero() {
		return v1.TrafficBinding{}, v1.Invalid("apply operation, owner or time")
	}
	if err := validateIdentity(request.Intent.Identity, request.Intent.Operation, request.OwnerUID, request.Intent.Rule.Service); err != nil {
		return v1.TrafficBinding{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.uncertain != nil {
		return v1.TrafficBinding{}, s.uncertain
	}
	if err := ctx.Err(); err != nil {
		return v1.TrafficBinding{}, err
	}
	key := request.Intent.Rule.Service.String()
	current := s.doc.Bindings[key]
	sameOperation := v1.SameOperation(current.Identity, current.Operation, request.Intent.Identity, request.Intent.Operation)
	switch {
	case sameOperation:
		// SameOperation already proves the service (the map key) and, for the
		// composite form, owner and external flow. A reused flow with another
		// owner or rule is a conflict rather than an acknowledgement.
		if current.OwnerUID == request.OwnerUID && SameRule(current.Rule, request.Intent.Rule) {
			current.Rule = cloneRule(current.Rule)
			return current, nil
		}
		return v1.TrafficBinding{}, fmt.Errorf("%w: operation reused for another owner or rule", v1.ErrConflict)
	case current.Operation == request.Intent.Operation:
		// The external flow already exists on this service but under a different
		// composite identity (or on the other side of the v1/v2 upgrade). It must
		// never be acknowledged as the request's applied result.
		return v1.TrafficBinding{}, fmt.Errorf("%w: external flow is bound to another operation identity", v1.ErrConflict)
	case current.ResourceVersion != request.Intent.ExpectedVersion:
		return v1.TrafficBinding{}, v1.ErrConflict
	}
	if current.ResourceVersion > 0 && current.OwnerUID != request.OwnerUID {
		if current.Rule.CandidateWeight != 0 || current.Rule.Stable != request.Intent.Rule.Stable ||
			request.Intent.Rule.CandidateWeight != 0 {
			return v1.TrafficBinding{}, fmt.Errorf("%w: cannot replace an active traffic owner", v1.ErrConflict)
		}
	}
	if s.doc.Version == ^uint64(0) {
		return v1.TrafficBinding{}, v1.Invalid("traffic version exhausted")
	}
	next := document{
		APIVersion: v1.Version, Version: s.doc.Version + 1,
		Bindings: make(map[string]v1.TrafficBinding, len(s.doc.Bindings)+1),
	}
	for k, binding := range s.doc.Bindings {
		next.Bindings[k] = binding
	}
	var identity *v1.OperationIdentity
	if request.Intent.Identity != nil {
		cloned := *request.Intent.Identity
		identity = &cloned
	}
	binding := v1.TrafficBinding{
		APIVersion: v1.Version, OwnerUID: request.OwnerUID, ResourceVersion: next.Version,
		Operation: request.Intent.Operation, Identity: identity,
		AppliedAt: request.At.UTC(), Rule: cloneRule(request.Intent.Rule),
	}
	next.Bindings[key] = binding
	renamed, err := statefile.Write(s.path, next)
	if err != nil {
		if renamed {
			s.uncertain = fmt.Errorf("%w: traffic write outcome uncertain: %v", v1.ErrUnavailable, err)
		}
		return v1.TrafficBinding{}, err
	}
	s.doc = next
	binding.Rule = cloneRule(binding.Rule)
	return binding, nil
}
