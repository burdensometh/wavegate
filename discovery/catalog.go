package discovery

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"sync"

	"wavegate/internal/statefile"
	"wavegate/v1"
)

type catalogDocument struct {
	APIVersion string                           `json:"apiVersion"`
	Version    uint64                           `json:"resourceVersion"`
	Items      map[string]v1.ReplicaObservation `json:"items"`
	UIDKeys    map[string]string                `json:"uidKeys"`
	History    []v1.EndpointEvent               `json:"history"`
}

// Catalog holds the fake API's authoritative endpoint resources.
type Catalog struct {
	mu        sync.RWMutex
	path      string
	limit     int
	doc       catalogDocument
	changed   chan struct{}
	uncertain error
}

func OpenCatalog(path string, historyLimit int) (*Catalog, error) {
	if historyLimit < 1 || historyLimit > 100000 {
		return nil, v1.Invalid("endpoint history limit")
	}
	catalog := &Catalog{
		path: path, limit: historyLimit, changed: make(chan struct{}),
		doc: catalogDocument{APIVersion: v1.Version, Items: make(map[string]v1.ReplicaObservation), UIDKeys: make(map[string]string)},
	}
	var recovered catalogDocument
	if found, err := statefile.Read(path, &recovered); err != nil {
		return nil, err
	} else if found {
		catalog.doc = recovered
	}
	if err := validateCatalog(catalog.doc); err != nil {
		return nil, err
	}
	return catalog, nil
}

func validateCatalog(doc catalogDocument) error {
	if err := v1.CheckVersion(doc.APIVersion); err != nil {
		return err
	}
	if doc.Items == nil || doc.UIDKeys == nil || uint64(len(doc.UIDKeys)) > doc.Version ||
		uint64(len(doc.History)) > doc.Version || (doc.Version > 0 && len(doc.History) == 0) {
		return v1.Invalid("endpoint catalog inventory or history")
	}
	latest := make(map[string]v1.EndpointEvent)
	for i, event := range doc.History {
		if event.ResourceVersion != doc.Version-uint64(len(doc.History))+uint64(i)+1 {
			return v1.Invalid("endpoint history sequence")
		}
		if err := validateEvent(event); err != nil {
			return err
		}
		if doc.UIDKeys[event.Object.UID] != event.Object.Key() {
			return v1.Invalid("endpoint event UID missing from the identity ledger")
		}
		latest[event.Object.Key()] = event
	}
	for key, endpoint := range doc.Items {
		if key != endpoint.Key() || doc.UIDKeys[endpoint.UID] != key ||
			endpoint.ResourceVersion == 0 || endpoint.ResourceVersion > doc.Version {
			return v1.Invalid("endpoint catalog key or resource version")
		}
		if err := endpoint.Validate(); err != nil {
			return err
		}
	}
	for key, event := range latest {
		current, exists := doc.Items[key]
		if event.Type == v1.Deleted {
			if exists {
				return v1.Invalid("deleted endpoint present in catalog")
			}
			continue
		}
		if !exists || current != event.Object {
			return v1.Invalid("endpoint history and current inventory disagree")
		}
	}
	return nil
}

func validateEvent(event v1.EndpointEvent) error {
	if err := v1.CheckVersion(event.APIVersion); err != nil {
		return err
	}
	if event.Type != v1.Added && event.Type != v1.Modified && event.Type != v1.Deleted {
		return v1.Invalid("endpoint event type")
	}
	if event.ResourceVersion == 0 || event.Object.ResourceVersion != event.ResourceVersion {
		return v1.Invalid("endpoint event version")
	}
	return event.Object.Validate()
}

func (c *Catalog) List(ctx context.Context) (v1.EndpointList, error) {
	if err := ctx.Err(); err != nil {
		return v1.EndpointList{}, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.uncertain != nil {
		return v1.EndpointList{}, c.uncertain
	}
	items := make([]v1.ReplicaObservation, 0, len(c.doc.Items))
	for _, endpoint := range c.doc.Items {
		items = append(items, endpoint)
	}
	slices.SortFunc(items, compareEndpoint)
	return v1.EndpointList{APIVersion: v1.Version, ResourceVersion: c.doc.Version, Items: items}, nil
}

func compareEndpoint(a, b v1.ReplicaObservation) int {
	switch {
	case a.Key() < b.Key():
		return -1
	case a.Key() > b.Key():
		return 1
	default:
		return 0
	}
}

func (c *Catalog) Put(endpoint v1.ReplicaObservation) (v1.ReplicaObservation, error) {
	if err := endpoint.Validate(); err != nil {
		return v1.ReplicaObservation{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.uncertain != nil {
		return v1.ReplicaObservation{}, c.uncertain
	}
	current, exists := c.doc.Items[endpoint.Key()]
	if current.ResourceVersion != endpoint.ResourceVersion {
		return v1.ReplicaObservation{}, v1.ErrConflict
	}
	if exists && current.UID == endpoint.UID && (current.Revision != endpoint.Revision || current.Address != endpoint.Address) {
		return v1.ReplicaObservation{}, v1.Invalid("address and revision changes require a new endpoint UID")
	}
	if _, seen := c.doc.UIDKeys[endpoint.UID]; seen && current.UID != endpoint.UID {
		return v1.ReplicaObservation{}, fmt.Errorf("%w: endpoint UID already used", v1.ErrConflict)
	}
	if exists && current == endpoint {
		return current, nil
	}
	kind := v1.Added
	if exists {
		kind = v1.Modified
	}
	return c.commit(kind, endpoint)
}

func (c *Catalog) Delete(service v1.Service, node, name, uid string, version uint64) (v1.EndpointEvent, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.uncertain != nil {
		return v1.EndpointEvent{}, c.uncertain
	}
	current, exists := c.doc.Items[service.String()+"/"+node+"/"+name]
	if !exists {
		return v1.EndpointEvent{}, v1.ErrNotFound
	}
	if current.UID != uid || current.ResourceVersion != version {
		return v1.EndpointEvent{}, v1.ErrConflict
	}
	deleted, err := c.commit(v1.Deleted, current)
	if err != nil {
		return v1.EndpointEvent{}, err
	}
	return v1.EndpointEvent{APIVersion: v1.Version, Type: v1.Deleted, ResourceVersion: deleted.ResourceVersion, Object: deleted}, nil
}

func (c *Catalog) commit(kind string, endpoint v1.ReplicaObservation) (v1.ReplicaObservation, error) {
	if c.doc.Version == ^uint64(0) {
		return v1.ReplicaObservation{}, v1.Invalid("endpoint version exhausted")
	}
	next := catalogDocument{
		APIVersion: v1.Version, Version: c.doc.Version + 1,
		Items:   make(map[string]v1.ReplicaObservation, len(c.doc.Items)+1),
		UIDKeys: maps.Clone(c.doc.UIDKeys),
	}
	endpoint.ResourceVersion = next.Version
	for key, item := range c.doc.Items {
		next.Items[key] = item
	}
	if kind == v1.Deleted {
		delete(next.Items, endpoint.Key())
	} else {
		next.Items[endpoint.Key()] = endpoint
		next.UIDKeys[endpoint.UID] = endpoint.Key()
	}
	history := c.doc.History
	if len(history) >= c.limit {
		history = history[len(history)-c.limit+1:]
	}
	next.History = append(slices.Clone(history), v1.EndpointEvent{
		APIVersion: v1.Version, Type: kind, ResourceVersion: next.Version, Object: endpoint,
	})
	renamed, err := statefile.Write(c.path, next)
	if err != nil {
		if renamed {
			c.uncertain = fmt.Errorf("%w: endpoint catalog durability: %v", v1.ErrUnavailable, err)
			close(c.changed)
			c.changed = make(chan struct{})
		}
		return v1.ReplicaObservation{}, err
	}
	c.doc = next
	close(c.changed)
	c.changed = make(chan struct{})
	return endpoint, nil
}

// WithResolvedTargets admits a request against a pinned endpoint inventory.
// A recreated endpoint needs a matching installed rule before use.
func (c *Catalog) WithResolvedTargets(service v1.Service, revision string, targets []v1.Target, admit func([]v1.ReplicaObservation) error) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.uncertain != nil {
		return c.uncertain
	}
	var live []v1.ReplicaObservation
	for _, target := range targets {
		endpoint, exists := c.doc.Items[service.String()+"/"+target.Key()]
		if exists && endpoint.Ready && endpoint.UID == target.UID && endpoint.Revision == revision {
			live = append(live, endpoint)
		}
	}
	return admit(live)
}

func (c *Catalog) Watch(ctx context.Context, from uint64) (Stream, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.checkCursor(from); err != nil {
		return nil, err
	}
	return &catalogStream{catalog: c, from: from}, nil
}

func (c *Catalog) checkCursor(from uint64) error {
	if c.uncertain != nil {
		return c.uncertain
	}
	if from > c.doc.Version {
		return fmt.Errorf("%w: watch cursor ahead of catalog", v1.ErrConflict)
	}
	if from < c.doc.Version-uint64(len(c.doc.History)) {
		return v1.ErrExpired
	}
	return nil
}

type catalogStream struct {
	catalog *Catalog
	from    uint64
}

func (s *catalogStream) Close() error { return nil }

func (s *catalogStream) Next(ctx context.Context) (v1.EndpointEvent, error) {
	c := s.catalog
	for {
		if err := ctx.Err(); err != nil {
			return v1.EndpointEvent{}, err
		}
		c.mu.RLock()
		if err := c.checkCursor(s.from); err != nil {
			c.mu.RUnlock()
			return v1.EndpointEvent{}, err
		}
		position := sort.Search(len(c.doc.History), func(i int) bool {
			return c.doc.History[i].ResourceVersion > s.from
		})
		if position < len(c.doc.History) {
			event := c.doc.History[position]
			c.mu.RUnlock()
			s.from = event.ResourceVersion
			return event, nil
		}
		changed := c.changed
		c.mu.RUnlock()
		select {
		case <-ctx.Done():
			return v1.EndpointEvent{}, ctx.Err()
		case <-changed:
		}
	}
}
