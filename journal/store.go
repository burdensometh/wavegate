package journal

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"wavegate/internal/statefile"
	"wavegate/v1"
)

type checkpoint struct {
	Sequence uint64                       `json:"sequence"`
	Head     string                       `json:"head,omitempty"`
	States   map[string]v1.DecisionRecord `json:"states"`
	Digest   string                       `json:"digest"`
}

type document struct {
	APIVersion string              `json:"apiVersion"`
	Checkpoint checkpoint          `json:"checkpoint"`
	Records    []v1.DecisionRecord `json:"records"`
}

type Store struct {
	mu     sync.RWMutex
	path   string
	retain int
	doc    document
	states map[string]v1.DecisionRecord
	// retained indexes doc.Records by release UID as ascending sequences, so a
	// single-release receipt reads only its own records instead of scanning
	// every retained decision. It is derived state: replay rebuilds it and
	// Commit keeps it in sync with compaction.
	retained  map[string][]uint64
	sequence  uint64
	head      string
	changed   chan struct{}
	uncertain error
}

func Open(path string, retain int) (*Store, error) {
	if retain < 2 || retain > 10000 {
		return nil, v1.Invalid("journal retention must be within 2..10000")
	}
	store := &Store{
		path: path, retain: retain, doc: document{
			APIVersion: v1.Version,
			Checkpoint: checkpoint{States: make(map[string]v1.DecisionRecord)},
			Records:    []v1.DecisionRecord{},
		},
		retained: make(map[string][]uint64),
		changed:  make(chan struct{}),
	}
	emptyDigest, err := digestCheckpoint(store.doc.Checkpoint)
	if err != nil {
		return nil, err
	}
	store.doc.Checkpoint.Digest = emptyDigest
	var recovered document
	if found, err := statefile.Read(path, &recovered); err != nil {
		return nil, err
	} else if found {
		store.doc = recovered
	}
	if err := store.replay(); err != nil {
		return nil, fmt.Errorf("verify journal: %w", err)
	}
	return store, nil
}

func digestCheckpoint(value checkpoint) (string, error) {
	value.Digest = ""
	return v1.Digest(value)
}

func digestRecord(record v1.DecisionRecord) (string, error) {
	record.Digest = ""
	return v1.Digest(record)
}

func (s *Store) replay() error {
	if err := v1.CheckVersion(s.doc.APIVersion); err != nil {
		return err
	}
	base := s.doc.Checkpoint
	digest, err := digestCheckpoint(base)
	if err != nil {
		return err
	}
	if digest != base.Digest || (base.Sequence == 0 && (base.Head != "" || len(base.States) != 0)) ||
		(base.Sequence > 0 && !v1.ValidDigest(base.Head)) || base.States == nil {
		return v1.Invalid("journal checkpoint digest or anchor")
	}
	s.states = maps.Clone(base.States)
	s.sequence, s.head = base.Sequence, base.Head
	foundAnchor := base.Sequence == 0
	seen := map[uint64]bool{}
	for uid, record := range base.States {
		if uid != record.State.Spec.UID || record.Sequence > base.Sequence || seen[record.Sequence] {
			return v1.Invalid("journal checkpoint inventory")
		}
		if err := validateRecord(record); err != nil {
			return err
		}
		seen[record.Sequence] = true
		if record.Sequence == base.Sequence && record.Digest == base.Head {
			foundAnchor = true
		}
	}
	if !foundAnchor {
		return v1.Invalid("journal checkpoint anchor is absent")
	}
	for _, record := range s.doc.Records {
		if record.Sequence != s.sequence+1 || record.PreviousDigest != s.head {
			return v1.Invalid("journal record chain")
		}
		if err := validateRecord(record); err != nil {
			return err
		}
		if err := validateTransition(s.states[record.State.Spec.UID], record); err != nil {
			return err
		}
		s.states[record.State.Spec.UID] = record
		s.retained[record.State.Spec.UID] = append(s.retained[record.State.Spec.UID], record.Sequence)
		s.sequence, s.head = record.Sequence, record.Digest
	}
	return nil
}

func (s *Store) Sequence() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sequence
}

func (s *Store) State(uid string) (v1.ReleaseState, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.uncertain != nil {
		return v1.ReleaseState{}, false, s.uncertain
	}
	record, found := s.states[uid]
	if !found {
		return v1.ReleaseState{}, false, nil
	}
	state, err := v1.Copy(record.State)
	return state, true, err
}

func (s *Store) States() ([]v1.ReleaseState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.uncertain != nil {
		return nil, s.uncertain
	}
	states := make([]v1.ReleaseState, 0, len(s.states))
	for _, record := range s.states {
		states = append(states, record.State)
	}
	slices.SortFunc(states, func(a, b v1.ReleaseState) int { return cmp.Compare(a.Spec.UID, b.Spec.UID) })
	return v1.Copy(states)
}

func (s *Store) Commit(expected uint64, at time.Time, kind, reason string, state v1.ReleaseState) (v1.DecisionRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.uncertain != nil {
		return v1.DecisionRecord{}, s.uncertain
	}
	if expected != s.sequence || expected == ^uint64(0) {
		return v1.DecisionRecord{}, v1.ErrConflict
	}
	state.Status.DecisionSequence = s.sequence + 1
	state, err := v1.Copy(state)
	if err != nil {
		return v1.DecisionRecord{}, err
	}
	record := v1.DecisionRecord{
		APIVersion: v1.Version, Sequence: s.sequence + 1, At: at.UTC(), Kind: kind, Reason: reason,
		State: state, PreviousDigest: s.head,
	}
	record.Digest, err = digestRecord(record)
	if err != nil {
		return v1.DecisionRecord{}, err
	}
	if err := validateRecord(record); err != nil {
		return v1.DecisionRecord{}, err
	}
	if err := validateTransition(s.states[state.Spec.UID], record); err != nil {
		return v1.DecisionRecord{}, err
	}
	next := s.doc
	next.Records = append(slices.Clone(next.Records), record)
	var compacted []v1.DecisionRecord
	if len(next.Records) > s.retain {
		count := len(next.Records) - s.retain
		next.Checkpoint, err = fold(next.Checkpoint, next.Records[:count])
		if err != nil {
			return v1.DecisionRecord{}, err
		}
		compacted = next.Records[:count]
		next.Records = slices.Clone(next.Records[count:])
	}
	renamed, err := statefile.Write(s.path, next)
	if err != nil {
		if renamed {
			s.uncertain = fmt.Errorf("%w: journal outcome uncertain: %v", v1.ErrUnavailable, err)
			s.signal()
		}
		return v1.DecisionRecord{}, err
	}
	s.doc = next
	s.sequence, s.head = record.Sequence, record.Digest
	s.states[state.Spec.UID] = record
	for _, dropped := range compacted {
		uid := dropped.State.Spec.UID
		if kept := s.retained[uid]; len(kept) <= 1 {
			delete(s.retained, uid)
		} else {
			s.retained[uid] = kept[1:]
		}
	}
	s.retained[state.Spec.UID] = append(s.retained[state.Spec.UID], record.Sequence)
	s.signal()
	return v1.Copy(record)
}

func (s *Store) signal() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func fold(base checkpoint, records []v1.DecisionRecord) (checkpoint, error) {
	base.States = maps.Clone(base.States)
	for _, record := range records {
		base.States[record.State.Spec.UID] = record
		base.Sequence, base.Head = record.Sequence, record.Digest
	}
	digest, err := digestCheckpoint(base)
	base.Digest = digest
	return base, err
}

type Page struct {
	APIVersion         string              `json:"apiVersion"`
	After              uint64              `json:"after"`
	Through            uint64              `json:"through"`
	CheckpointSequence uint64              `json:"checkpointSequence"`
	Records            []v1.DecisionRecord `json:"records"`
}

func (s *Store) Events(after uint64, limit int) (Page, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.uncertain != nil {
		return Page{}, s.uncertain
	}
	if limit < 1 || limit > 1000 {
		return Page{}, v1.Invalid("decision page limit")
	}
	page := Page{APIVersion: v1.Version, After: after, Through: after, CheckpointSequence: s.doc.Checkpoint.Sequence, Records: []v1.DecisionRecord{}}
	if after < page.CheckpointSequence {
		return page, v1.ErrExpired
	}
	if after > s.sequence {
		return page, v1.ErrConflict
	}
	start := int(after - page.CheckpointSequence)
	end := min(start+limit, len(s.doc.Records))
	page.Records = slices.Clone(s.doc.Records[start:end])
	page.Through = page.After + uint64(len(page.Records))
	return v1.Copy(page)
}
