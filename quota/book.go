package quota

import (
	"cmp"
	"fmt"
	"slices"
	"sync"
	"time"

	"wavegate/v1"
)

type Limit struct {
	Releases       int `json:"releases"`
	CandidateUnits int `json:"candidateUnits"`
}

func (l Limit) Validate() error {
	if l.Releases < 0 || l.Releases > 10000 || l.CandidateUnits < 0 || l.CandidateUnits > 1000000 {
		return v1.Invalid("namespace quota bounds")
	}
	return nil
}

type Usage struct {
	APIVersion     string                    `json:"apiVersion"`
	Namespace      string                    `json:"namespace"`
	Limit          Limit                     `json:"limit"`
	Releases       int                       `json:"releases"`
	CandidateUnits int                       `json:"candidateUnits"`
	Reservations   []v1.NamespaceReservation `json:"reservations"`
}

type Book struct {
	mu       sync.RWMutex
	base     Limit
	limits   map[string]Limit
	services map[v1.Service]v1.NamespaceReservation
	uids     map[string]v1.Service
}

func New(base Limit, limits map[string]Limit, recovered []v1.NamespaceReservation) (*Book, error) {
	if err := base.Validate(); err != nil {
		return nil, err
	}
	book := &Book{
		base: base, limits: make(map[string]Limit, len(limits)),
		services: make(map[v1.Service]v1.NamespaceReservation), uids: make(map[string]v1.Service),
	}
	for namespace, limit := range limits {
		if !v1.ValidLabel(namespace) {
			return nil, v1.Invalid("quota namespace")
		}
		if err := limit.Validate(); err != nil {
			return nil, err
		}
		book.limits[namespace] = limit
	}
	for _, reservation := range recovered {
		if err := book.Install(reservation); err != nil {
			return nil, fmt.Errorf("restore quota reservation: %w", err)
		}
	}
	return book, nil
}

func Validate(reservation v1.NamespaceReservation) error {
	if err := v1.CheckVersion(reservation.APIVersion); err != nil {
		return err
	}
	if err := reservation.Service.Validate(); err != nil {
		return err
	}
	if !v1.ValidToken(reservation.ReleaseUID) || reservation.Units < 1 || reservation.Units > 1000 ||
		reservation.ResourceVersion == 0 || reservation.AcquiredAt.IsZero() {
		return v1.Invalid("reservation identity, units, or acquisition time")
	}
	return nil
}

func (b *Book) usage(namespace string) Usage {
	limit, exists := b.limits[namespace]
	if !exists {
		limit = b.base
	}
	usage := Usage{APIVersion: v1.Version, Namespace: namespace, Limit: limit, Reservations: []v1.NamespaceReservation{}}
	for service, reservation := range b.services {
		if service.Namespace == namespace {
			usage.CandidateUnits += reservation.Units
			usage.Reservations = append(usage.Reservations, reservation)
		}
	}
	usage.Releases = len(usage.Reservations)
	return usage
}

func (b *Book) Usage(namespace string) Usage {
	b.mu.RLock()
	defer b.mu.RUnlock()
	usage := b.usage(namespace)
	slices.SortFunc(usage.Reservations, func(a, b v1.NamespaceReservation) int {
		return cmp.Compare(a.Service.Name, b.Service.Name)
	})
	return usage
}

// Plan creates a reservation that becomes live only after the journal commit.
func (b *Book) Plan(spec v1.ReleaseSpec, version uint64, at time.Time) (v1.NamespaceReservation, string) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if previous, exists := b.services[spec.Service]; exists {
		if previous.ReleaseUID == spec.UID {
			return previous, ""
		}
		return v1.NamespaceReservation{}, "ServiceReserved"
	}
	usage := b.usage(spec.Service.Namespace)
	if usage.Releases >= usage.Limit.Releases || usage.CandidateUnits+spec.MinReady > usage.Limit.CandidateUnits {
		return v1.NamespaceReservation{}, "NamespaceBudget"
	}
	return v1.NamespaceReservation{
		APIVersion: v1.Version, Service: spec.Service, ReleaseUID: spec.UID,
		ResourceVersion: version, Units: spec.MinReady, AcquiredAt: at.UTC(),
	}, ""
}

func (b *Book) Install(reservation v1.NamespaceReservation) error {
	if err := Validate(reservation); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if owner, exists := b.uids[reservation.ReleaseUID]; exists && owner != reservation.Service {
		return fmt.Errorf("%w: reservation UID belongs to another service", v1.ErrConflict)
	}
	if previous, exists := b.services[reservation.Service]; exists && previous != reservation {
		return fmt.Errorf("%w: service already reserved", v1.ErrConflict)
	}
	b.services[reservation.Service] = reservation
	b.uids[reservation.ReleaseUID] = reservation.Service
	return nil
}

func (b *Book) Release(reservation v1.NamespaceReservation) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if current, exists := b.services[reservation.Service]; !exists || current != reservation {
		return fmt.Errorf("%w: release does not match live reservation", v1.ErrConflict)
	}
	delete(b.services, reservation.Service)
	delete(b.uids, reservation.ReleaseUID)
	return nil
}
