package discovery

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"wavegate/v1"
)

type Source interface {
	List(context.Context) (v1.EndpointList, error)
	Watch(context.Context, uint64) (Stream, error)
}

type Stream interface {
	Next(context.Context) (v1.EndpointEvent, error)
	Close() error
}

type Snapshot struct {
	Version    uint64
	Generation uint64
	Synced     bool
	Ready      []v1.ReplicaObservation
}

type Index struct {
	mu         sync.RWMutex
	version    uint64
	generation uint64
	synced     bool
	items      map[v1.Service]map[string]v1.ReplicaObservation
	notify     chan struct{}
}

func NewIndex() *Index {
	return &Index{
		items:  make(map[v1.Service]map[string]v1.ReplicaObservation),
		notify: make(chan struct{}, 1),
	}
}

func (i *Index) Notify() <-chan struct{} { return i.notify }

func (i *Index) signal() {
	select {
	case i.notify <- struct{}{}:
	default:
	}
}

func (i *Index) Replace(list v1.EndpointList) error {
	if err := v1.CheckVersion(list.APIVersion); err != nil {
		return err
	}
	next := make(map[v1.Service]map[string]v1.ReplicaObservation)
	for _, endpoint := range list.Items {
		if err := endpoint.Validate(); err != nil {
			return err
		}
		if endpoint.ResourceVersion > list.ResourceVersion || endpoint.ResourceVersion == 0 {
			return v1.Invalid("list item resource version")
		}
		group := next[endpoint.Service]
		if group == nil {
			group = make(map[string]v1.ReplicaObservation)
			next[endpoint.Service] = group
		}
		if _, exists := group[endpoint.Key()]; exists {
			return v1.Invalid("duplicate endpoint in list")
		}
		group[endpoint.Key()] = endpoint
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if list.ResourceVersion < i.version {
		return fmt.Errorf("%w: relist moved backwards", v1.ErrConflict)
	}
	i.items, i.version, i.synced = next, list.ResourceVersion, true
	i.signal()
	return nil
}

func (i *Index) Apply(event v1.EndpointEvent) error {
	if err := validateEvent(event); err != nil {
		return err
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if !i.synced {
		return v1.ErrUnavailable
	}
	if event.ResourceVersion <= i.version {
		return nil
	}
	if event.ResourceVersion != i.version+1 {
		return fmt.Errorf("%w: endpoint watch sequence gap", v1.ErrExpired)
	}
	endpoint := event.Object
	group := i.items[endpoint.Service]
	old, exists := group[endpoint.Key()]
	switch event.Type {
	case v1.Deleted:
		if !exists || old.UID != endpoint.UID {
			return fmt.Errorf("%w: endpoint delete does not match live UID", v1.ErrExpired)
		}
		delete(group, endpoint.Key())
		if len(group) == 0 {
			delete(i.items, endpoint.Service)
		}
	case v1.Added, v1.Modified:
		if exists != (event.Type == v1.Modified) {
			return fmt.Errorf("%w: endpoint event does not match inventory", v1.ErrExpired)
		}
		if group == nil {
			group = make(map[string]v1.ReplicaObservation)
			i.items[endpoint.Service] = group
		}
		group[endpoint.Key()] = endpoint
	}
	i.version = event.ResourceVersion
	i.signal()
	return nil
}

func (i *Index) Disconnect() {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.synced {
		i.synced = false
		i.generation++
		i.signal()
	}
}

func (i *Index) Snapshot(service v1.Service) Snapshot {
	i.mu.RLock()
	defer i.mu.RUnlock()
	snapshot := Snapshot{Version: i.version, Generation: i.generation, Synced: i.synced}
	for _, endpoint := range i.items[service] {
		if endpoint.Ready {
			snapshot.Ready = append(snapshot.Ready, endpoint)
		}
	}
	slices.SortFunc(snapshot.Ready, compareEndpoint)
	return snapshot
}

func Follow(ctx context.Context, source Source, index *Index, delay time.Duration, report func(error)) {
	defer index.Disconnect()
	for ctx.Err() == nil {
		err := followOnce(ctx, source, index)
		index.Disconnect()
		if ctx.Err() != nil {
			return
		}
		if report != nil {
			report(err)
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return
		}
	}
}

func followOnce(ctx context.Context, source Source, index *Index) error {
	list, err := source.List(ctx)
	if err != nil {
		return err
	}
	stream, err := source.Watch(ctx, list.ResourceVersion)
	if err != nil {
		return err
	}
	defer stream.Close()
	if err := index.Replace(list); err != nil {
		return err
	}
	for {
		event, err := stream.Next(ctx)
		if err != nil {
			return err
		}
		if err := index.Apply(event); err != nil {
			return err
		}
	}
}
