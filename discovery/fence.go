package discovery

import (
	"fmt"

	"wavegate/v1"
)

// WithReadyRule pins the authoritative inventory during a traffic mutation.
// Stable-only emergency rollback can publish even when its targets are stale.
func (c *Catalog) WithReadyRule(rule v1.Rule, mutate func() error) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.uncertain != nil {
		return c.uncertain
	}
	if c.doc.Version < rule.DiscoveryVersion {
		return fmt.Errorf("%w: endpoint fence ahead of the catalog", v1.ErrExpired)
	}
	for _, group := range []struct {
		revision string
		targets  []v1.Target
	}{{rule.Stable, rule.StableTargets}, {rule.Candidate, rule.CandidateTargets}} {
		for _, target := range group.targets {
			item, found := c.doc.Items[rule.Service.String()+"/"+target.Key()]
			if !found || item.UID != target.UID || item.Revision != group.revision || !item.Ready {
				return fmt.Errorf("%w: target %s no longer matches the rule", v1.ErrExpired, target.Key())
			}
		}
	}
	return mutate()
}
