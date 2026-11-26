package config

import (
	"fmt"
	"net"
	"os"
	"time"

	"wavegate/quota"
	"wavegate/v1"
)

type Config struct {
	APIVersion        string                 `json:"apiVersion"`
	Listen            string                 `json:"listen"`
	StateDirectory    string                 `json:"stateDirectory"`
	DefaultQuota      quota.Limit            `json:"defaultQuota"`
	Quotas            map[string]quota.Limit `json:"quotas"`
	SharedQuota       quota.Shared           `json:"sharedQuota"`
	EndpointHistory   int                    `json:"endpointHistory"`
	JournalRetention  int                    `json:"journalRetention"`
	ReconcileMS       int                    `json:"reconcileMs"`
	WatchRetryMS      int                    `json:"watchRetryMs"`
	UpstreamTimeoutMS int                    `json:"upstreamTimeoutMs"`
	DrainMS           int                    `json:"drainMs"`
}

func Default() Config {
	return Config{
		APIVersion: v1.Version, Listen: "127.0.0.1:8080", StateDirectory: "var/wavegate",
		DefaultQuota: quota.Limit{Releases: 2, CandidateUnits: 4}, Quotas: map[string]quota.Limit{},
		EndpointHistory: 256, JournalRetention: 128, ReconcileMS: 200, WatchRetryMS: 100,
		UpstreamTimeoutMS: 3000, DrainMS: 2000,
	}
}

func Load(path string) (Config, error) {
	if path == "" {
		return Default(), nil
	}
	file, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}
	defer file.Close()
	var cfg Config
	if err := v1.Decode(file, &cfg); err != nil {
		return cfg, err
	}
	if err := cfg.Validate(); err != nil {
		return cfg, fmt.Errorf("%s: %w", path, err)
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if err := v1.CheckVersion(c.APIVersion); err != nil {
		return err
	}
	host, port, err := net.SplitHostPort(c.Listen)
	if err != nil || port == "" || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return v1.Invalid("fake API requires a loopback listener")
	}
	if c.StateDirectory == "" || c.EndpointHistory < 1 || c.EndpointHistory > 100000 ||
		c.JournalRetention < 2 || c.JournalRetention > 10000 {
		return v1.Invalid("state directory or history retention")
	}
	if c.ReconcileMS < 1 || c.ReconcileMS > 60000 || c.WatchRetryMS < 1 || c.WatchRetryMS > 60000 ||
		c.UpstreamTimeoutMS < 1 || c.UpstreamTimeoutMS > 60000 || c.DrainMS < 0 || c.DrainMS > 60000 {
		return v1.Invalid("poll, retry, upstream or drain interval")
	}
	if err := c.DefaultQuota.Validate(); err != nil {
		return err
	}
	for namespace, limit := range c.Quotas {
		if !v1.ValidLabel(namespace) {
			return v1.Invalid("quota namespace")
		}
		if err := limit.Validate(); err != nil {
			return err
		}
	}
	return c.SharedQuota.Validate()
}

func (c Config) ReconcileInterval() time.Duration {
	return time.Duration(c.ReconcileMS) * time.Millisecond
}
func (c Config) WatchRetry() time.Duration { return time.Duration(c.WatchRetryMS) * time.Millisecond }
func (c Config) UpstreamTimeout() time.Duration {
	return time.Duration(c.UpstreamTimeoutMS) * time.Millisecond
}
func (c Config) Drain() time.Duration { return time.Duration(c.DrainMS) * time.Millisecond }
