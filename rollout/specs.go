package rollout

import (
	"cmp"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"sync"

	"wavegate/internal/statefile"
	"wavegate/v1"
)

type ReleaseView struct {
	Spec   v1.ReleaseSpec    `json:"spec"`
	Status *v1.ReleaseStatus `json:"status,omitempty"`
}

type ReleaseList struct {
	APIVersion      string        `json:"apiVersion"`
	ResourceVersion uint64        `json:"resourceVersion"`
	Items           []ReleaseView `json:"items"`
}

type specDocument struct {
	APIVersion      string                 `json:"apiVersion"`
	ResourceVersion uint64                 `json:"resourceVersion"`
	Items           map[string]ReleaseView `json:"items"`
}

type Specs struct {
	mu        sync.RWMutex
	path      string
	doc       specDocument
	uids      map[string]string
	notify    chan struct{}
	uncertain error
}

func OpenSpecs(path string) (*Specs, error) {
	specs := &Specs{
		path: path, doc: specDocument{APIVersion: v1.Version, Items: make(map[string]ReleaseView)},
		uids: make(map[string]string), notify: make(chan struct{}, 1),
	}
	var recovered specDocument
	if found, err := statefile.Read(path, &recovered); err != nil {
		return nil, err
	} else if found {
		specs.doc = recovered
	}
	if err := specs.restoreIndex(); err != nil {
		return nil, err
	}
	return specs, nil
}

func (s *Specs) restoreIndex() error {
	if err := v1.CheckVersion(s.doc.APIVersion); err != nil {
		return err
	}
	if s.doc.Items == nil {
		return v1.Invalid("release inventory absent")
	}
	seen := map[uint64]bool{}
	for key, view := range s.doc.Items {
		spec := view.Spec
		if err := spec.Validate(); err != nil {
			return err
		}
		if key != spec.Key() || spec.ResourceVersion == 0 || spec.ResourceVersion > s.doc.ResourceVersion || seen[spec.ResourceVersion] {
			return v1.Invalid("release inventory key or resource version")
		}
		if _, exists := s.uids[spec.UID]; exists {
			return v1.Invalid("release UID reused")
		}
		if view.Status != nil {
			if err := validateStatus(spec, *view.Status); err != nil {
				return err
			}
		}
		s.uids[spec.UID] = key
		seen[spec.ResourceVersion] = true
	}
	if s.doc.ResourceVersion > 0 && !seen[s.doc.ResourceVersion] {
		return v1.Invalid("release high water mark is missing")
	}
	return nil
}

func (s *Specs) Notify() <-chan struct{} { return s.notify }

func (s *Specs) List() (ReleaseList, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.uncertain != nil {
		return ReleaseList{}, s.uncertain
	}
	result := ReleaseList{APIVersion: v1.Version, ResourceVersion: s.doc.ResourceVersion, Items: make([]ReleaseView, 0, len(s.doc.Items))}
	for _, view := range s.doc.Items {
		result.Items = append(result.Items, view)
	}
	slices.SortFunc(result.Items, func(a, b ReleaseView) int { return cmp.Compare(a.Spec.ResourceVersion, b.Spec.ResourceVersion) })
	return v1.Copy(result)
}

func (s *Specs) Get(namespace, name string) (ReleaseView, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.uncertain != nil {
		return ReleaseView{}, s.uncertain
	}
	item, exists := s.doc.Items[namespace+"/"+name]
	if !exists {
		return ReleaseView{}, v1.ErrNotFound
	}
	return v1.Copy(item)
}

func (s *Specs) Put(spec v1.ReleaseSpec) (ReleaseView, error) {
	if err := spec.Validate(); err != nil {
		return ReleaseView{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.uncertain != nil {
		return ReleaseView{}, s.uncertain
	}
	key := spec.Key()
	current, exists := s.doc.Items[key]
	if current.Spec.ResourceVersion != spec.ResourceVersion {
		return ReleaseView{}, v1.ErrConflict
	}
	if exists {
		if !v1.SameReleaseDefinition(current.Spec, spec) || (current.Spec.Abort && !spec.Abort) {
			return ReleaseView{}, v1.Invalid("release definition is immutable and abort is irreversible")
		}
		if current.Spec.Paused == spec.Paused && current.Spec.Abort == spec.Abort {
			return v1.Copy(current)
		}
	}
	if previousKey, exists := s.uids[spec.UID]; exists && previousKey != key {
		return ReleaseView{}, fmt.Errorf("%w: release UID already exists", v1.ErrConflict)
	}
	if s.doc.ResourceVersion == ^uint64(0) {
		return ReleaseView{}, v1.Invalid("release version exhausted")
	}
	spec.Steps = slices.Clone(spec.Steps)
	spec.ResourceVersion = s.doc.ResourceVersion + 1
	next := s.doc
	next.ResourceVersion = spec.ResourceVersion
	view := ReleaseView{Spec: spec, Status: current.Status}
	if err := s.commit(next, key, view); err != nil {
		return ReleaseView{}, err
	}
	s.uids[spec.UID] = key
	select {
	case s.notify <- struct{}{}:
	default:
	}
	return v1.Copy(view)
}

func validateStatus(spec v1.ReleaseSpec, status v1.ReleaseStatus) error {
	if err := v1.CheckVersion(status.APIVersion); err != nil {
		return err
	}
	if status.ReleaseUID != spec.UID || status.Service != spec.Service || status.ObservedVersion == 0 ||
		status.ObservedVersion > spec.ResourceVersion || status.DecisionSequence == 0 || !v1.KnownPhase(status.Phase) ||
		status.Step < -1 || status.Step >= len(spec.Steps) {
		return v1.Invalid("release status identity, cursor or phase")
	}
	return nil
}

func (s *Specs) Publish(status v1.ReleaseStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.uncertain != nil {
		return s.uncertain
	}
	key, exists := s.uids[status.ReleaseUID]
	if !exists {
		return v1.ErrNotFound
	}
	view := s.doc.Items[key]
	if err := validateStatus(view.Spec, status); err != nil {
		return err
	}
	if previous := view.Status; previous != nil {
		if status.DecisionSequence < previous.DecisionSequence || status.ObservedVersion < previous.ObservedVersion {
			return v1.ErrConflict
		}
		if status.DecisionSequence == previous.DecisionSequence {
			if reflect.DeepEqual(*previous, status) {
				return nil
			}
			return fmt.Errorf("%w: status changed at the same decision sequence", v1.ErrConflict)
		}
	}
	copied, err := v1.Copy(status)
	if err != nil {
		return err
	}
	view.Status = &copied
	return s.commit(s.doc, key, view)
}

// WithVersion serializes a bounded traffic apply against writes to desired state.
func (s *Specs) WithVersion(spec v1.ReleaseSpec, apply func() error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.uncertain != nil {
		return s.uncertain
	}
	current := s.doc.Items[spec.Key()].Spec
	if current.UID != spec.UID || current.ResourceVersion != spec.ResourceVersion {
		return v1.ErrConflict
	}
	return apply()
}

func (s *Specs) commit(next specDocument, key string, view ReleaseView) error {
	next.Items = maps.Clone(next.Items)
	next.Items[key] = view
	renamed, err := statefile.Write(s.path, next)
	if err != nil {
		if renamed {
			s.uncertain = fmt.Errorf("%w: release write outcome uncertain: %v", v1.ErrUnavailable, err)
		}
		return err
	}
	s.doc = next
	return nil
}
