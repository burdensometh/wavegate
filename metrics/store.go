package metrics

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	"wavegate/internal/statefile"
	"wavegate/v1"
)

const MaxBucket = 100000

type StoredWindow struct {
	Policy   v1.WindowPolicy  `json:"policy"`
	Verdict  v1.WindowVerdict `json:"verdict"`
	SealedAt time.Time        `json:"sealedAt"`
}

type storedSample struct {
	Version uint64    `json:"resourceVersion"`
	Sample  v1.Sample `json:"sample"`
}

type sampleDocument struct {
	APIVersion string                             `json:"apiVersion"`
	Version    uint64                             `json:"resourceVersion"`
	Buckets    map[string]map[string]storedSample `json:"buckets"`
	Windows    map[string]StoredWindow            `json:"windows"`
}

type Store struct {
	mu        sync.RWMutex
	path      string
	doc       sampleDocument
	uncertain error
	// orders lists every stored sample per bucket in resourceVersion order. It
	// is rebuilt from the persisted document on open and extended in lockstep
	// with Add, so incremental polls can read only the tail of the bucket.
	orders       map[string][]storedSample
	accumulators map[string]*windowAccumulator
}

func Open(path string) (*Store, error) {
	store := &Store{
		path: path, doc: sampleDocument{APIVersion: v1.Version, Buckets: make(map[string]map[string]storedSample), Windows: make(map[string]StoredWindow)},
	}
	var recovered sampleDocument
	if found, err := statefile.Read(path, &recovered); err != nil {
		return nil, err
	} else if found {
		store.doc = recovered
	}
	if err := store.validate(); err != nil {
		return nil, err
	}
	store.rebuildIndexes()
	return store, nil
}

// rebuildIndexes reconstructs the in-memory append order and resets window
// accumulators. Persisted sealed windows are validated separately by
// validate; only unsealed buckets need accumulators, which are created lazily
// on the next Verdict poll.
func (s *Store) rebuildIndexes() {
	s.orders = make(map[string][]storedSample, len(s.doc.Buckets))
	for key, bucket := range s.doc.Buckets {
		// Sealed buckets accept no further samples and are served from the
		// stored verdict, so they never need an append order or accumulator.
		if _, sealed := s.doc.Windows[key]; sealed {
			continue
		}
		order := make([]storedSample, 0, len(bucket))
		for _, stored := range bucket {
			order = append(order, stored)
		}
		slices.SortFunc(order, func(a, b storedSample) int { return cmp.Compare(a.Version, b.Version) })
		s.orders[key] = order
	}
	s.accumulators = make(map[string]*windowAccumulator)
}

func bucketKey(service v1.Service, version uint64) string {
	return service.String() + "/" + strconv.FormatUint(version, 10)
}

func (s *Store) validate() error {
	if err := v1.CheckVersion(s.doc.APIVersion); err != nil {
		return err
	}
	if s.doc.Buckets == nil || s.doc.Windows == nil {
		return v1.Invalid("sample document inventory")
	}
	seen := make(map[uint64]bool)
	for key, bucket := range s.doc.Buckets {
		if len(bucket) == 0 || len(bucket) > MaxBucket {
			return v1.Invalid("invalid sample bucket size")
		}
		for id, stored := range bucket {
			sample := stored.Sample
			if err := sample.Validate(); err != nil {
				return err
			}
			if id != sample.ID || key != bucketKey(sample.Service, sample.BindingVersion) ||
				stored.Version == 0 || stored.Version > s.doc.Version || seen[stored.Version] {
				return v1.Invalid("sample inventory identity or version")
			}
			seen[stored.Version] = true
		}
	}
	if uint64(len(seen)) != s.doc.Version {
		return v1.Invalid("sample version sequence")
	}
	for key, sealed := range s.doc.Windows {
		stored := sealed.Verdict
		if err := CheckVerdict(stored); err != nil {
			return err
		}
		if key != bucketKey(stored.Key.Service, stored.Key.BindingVersion) ||
			stored.Result == v1.Pending || stored.SampleVersion > s.doc.Version || sealed.SealedAt.Before(stored.Key.End) {
			return v1.Invalid("invalid sealed window")
		}
		samples := materialize(s.doc.Buckets[key], stored.SampleVersion)
		verdict, err := Evaluate(stored.Key, sealed.Policy, stored.SampleVersion, samples, sealed.SealedAt)
		if err != nil {
			return err
		}
		if verdict != stored {
			return v1.Invalid("sealed verdict differs from its samples")
		}
	}
	return nil
}

func (s *Store) Add(sample v1.Sample) (bool, error) {
	sample.At = sample.At.UTC()
	if err := sample.Validate(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.uncertain != nil {
		return false, s.uncertain
	}
	key := bucketKey(sample.Service, sample.BindingVersion)
	bucket := s.doc.Buckets[key]
	if previous, exists := bucket[sample.ID]; exists {
		if previous.Sample == sample {
			return false, nil
		}
		return false, fmt.Errorf("%w: changed sample with same id", v1.ErrConflict)
	}
	if _, exists := s.doc.Windows[key]; exists {
		return false, fmt.Errorf("%w: sample belongs to a sealed binding", v1.ErrExpired)
	}
	if len(bucket) >= MaxBucket || s.doc.Version == ^uint64(0) {
		return false, v1.Invalid("sample inventory limit reached")
	}
	next := s.doc
	next.Buckets = maps.Clone(next.Buckets)
	bucket = maps.Clone(bucket)
	if bucket == nil {
		bucket = make(map[string]storedSample)
	}
	next.Version++
	bucket[sample.ID] = storedSample{Version: next.Version, Sample: sample}
	next.Buckets[key] = bucket
	if err := s.commit(next); err != nil {
		return false, err
	}
	s.orders[key] = append(s.orders[key], storedSample{Version: next.Version, Sample: sample})
	return true, nil
}

func materialize(bucket map[string]storedSample, through uint64) []v1.Sample {
	samples := make([]v1.Sample, 0, len(bucket))
	for _, stored := range bucket {
		if stored.Version <= through {
			samples = append(samples, stored.Sample)
		}
	}
	return samples
}

func (s *Store) Verdict(key v1.WindowKey, policy v1.WindowPolicy, now time.Time) (v1.WindowVerdict, error) {
	key.Start, key.End = key.Start.UTC(), key.End.UTC()
	now = now.UTC()
	if err := key.Validate(); err != nil {
		return v1.WindowVerdict{}, err
	}
	if err := policy.Validate(); err != nil {
		return v1.WindowVerdict{}, err
	}
	if policy.Duration() != key.End.Sub(key.Start) {
		return v1.WindowVerdict{}, v1.Invalid("window and policy durations disagree")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.uncertain != nil {
		return v1.WindowVerdict{}, s.uncertain
	}
	bucket := bucketKey(key.Service, key.BindingVersion)
	if sealed, exists := s.doc.Windows[bucket]; exists {
		if sealed.Verdict.Key != key || sealed.Policy != policy {
			return v1.WindowVerdict{}, fmt.Errorf("%w: sealed window identity or policy changed", v1.ErrConflict)
		}
		return sealed.Verdict, nil
	}
	accumulator := s.accumulators[bucket]
	if accumulator == nil || accumulator.key != key || accumulator.policy != policy {
		// First poll for this window, or the controller opened a fresh window
		// for the same binding: rebuild from the complete inventory once.
		accumulator = newWindowAccumulator(key, policy)
		if _, err := accumulator.absorb(s.orders[bucket]); err != nil {
			return v1.WindowVerdict{}, err
		}
		s.accumulators[bucket] = accumulator
	} else if _, err := accumulator.absorb(s.orders[bucket]); err != nil {
		return v1.WindowVerdict{}, err
	}
	verdict, err := accumulator.verdict(s.doc.Version, now)
	if err != nil || verdict.Result == v1.Pending {
		return verdict, err
	}
	// Sealing is irreversible: reconstruct the verdict from the raw samples
	// with the full evaluator so an incremental state can never seal a result
	// that full evaluation would not have produced.
	full, err := Evaluate(key, policy, s.doc.Version, materialize(s.doc.Buckets[bucket], s.doc.Version), now)
	if err != nil {
		return v1.WindowVerdict{}, err
	}
	if full != verdict {
		delete(s.accumulators, bucket)
		return v1.WindowVerdict{}, v1.Invalid("incremental window diverged from full evaluation")
	}
	next := s.doc
	next.Windows = maps.Clone(next.Windows)
	next.Windows[bucket] = StoredWindow{Policy: policy, Verdict: verdict, SealedAt: now}
	if err := s.commit(next); err != nil {
		return v1.WindowVerdict{}, err
	}
	// The binding is sealed; its accumulator and order tail are no longer
	// needed, and Add rejects any further samples for this bucket.
	delete(s.accumulators, bucket)
	delete(s.orders, bucket)
	return verdict, nil
}

func (s *Store) Evidence(service v1.Service, version uint64) ([]v1.Sample, *StoredWindow, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.uncertain != nil {
		return nil, nil, s.uncertain
	}
	key := bucketKey(service, version)
	samples := materialize(s.doc.Buckets[key], s.doc.Version)
	sealed, exists := s.doc.Windows[key]
	if exists {
		end := 0
		for _, sample := range samples {
			window := sealed.Verdict.Key
			if !sample.At.Before(window.Start) && sample.At.Before(window.End) {
				samples[end] = sample
				end++
			}
		}
		samples = samples[:end]
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i].ID < samples[j].ID })
	if !exists {
		return samples, nil, nil
	}
	return samples, &sealed, nil
}

func (s *Store) Inventory() (version uint64, sealed int, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.uncertain != nil {
		return 0, 0, s.uncertain
	}
	return s.doc.Version, len(s.doc.Windows), nil
}

func (s *Store) CheckSealed(verdict v1.WindowVerdict) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.uncertain != nil {
		return s.uncertain
	}
	key := bucketKey(verdict.Key.Service, verdict.Key.BindingVersion)
	if sealed, exists := s.doc.Windows[key]; !exists || sealed.Verdict != verdict {
		return fmt.Errorf("%w: decision verdict is absent from its sealed window", v1.ErrConflict)
	}
	return nil
}

func (s *Store) commit(doc sampleDocument) error {
	renamed, err := statefile.Write(s.path, doc)
	if err != nil {
		if renamed {
			s.uncertain = fmt.Errorf("%w: sample write outcome uncertain: %v", v1.ErrUnavailable, err)
		}
		return err
	}
	s.doc = doc
	return nil
}
