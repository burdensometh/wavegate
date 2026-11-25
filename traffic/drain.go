package traffic

import (
	"cmp"
	"slices"
	"sync"

	"wavegate/v1"
)

type RequestActivity struct {
	Target         v1.Target `json:"target"`
	BindingVersion uint64    `json:"bindingVersion"`
	InFlight       int       `json:"inFlight"`
}

type requestGroup struct {
	service v1.Service
	target  v1.Target
	version uint64
}

type DrainTracker struct {
	mu     sync.RWMutex
	active map[requestGroup]int
}

func NewDrainTracker() *DrainTracker {
	return &DrainTracker{active: make(map[requestGroup]int)}
}

// Begin runs under the endpoint and binding admission fences. End is called
// after the proxy returns, so the drain covers streaming response bodies.
func (d *DrainTracker) Begin(service v1.Service, target v1.Target, version uint64) func() {
	group := requestGroup{service: service, target: target, version: version}
	d.mu.Lock()
	d.active[group]++
	d.mu.Unlock()
	return sync.OnceFunc(func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		if d.active[group] == 1 {
			delete(d.active, group)
		} else {
			d.active[group]--
		}
	})
}

func (d *DrainTracker) InUse(service v1.Service, target v1.Target) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	for group := range d.active {
		if service == group.service && target == group.target {
			return true
		}
	}
	return false
}

func (d *DrainTracker) Activity(service v1.Service) []RequestActivity {
	d.mu.RLock()
	defer d.mu.RUnlock()
	items := []RequestActivity{}
	for group, count := range d.active {
		if group.service == service {
			items = append(items, RequestActivity{Target: group.target, BindingVersion: group.version, InFlight: count})
		}
	}
	slices.SortFunc(items, func(a, b RequestActivity) int {
		if byTarget := compareTarget(a.Target, b.Target); byTarget != 0 {
			return byTarget
		}
		return cmp.Compare(a.BindingVersion, b.BindingVersion)
	})
	return items
}
