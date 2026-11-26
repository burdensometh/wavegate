package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"wavegate/clock"
	"wavegate/config"
	"wavegate/discovery"
	"wavegate/internal/statefile"
	"wavegate/journal"
	"wavegate/metrics"
	"wavegate/rollout"
	"wavegate/traffic"
)

type App struct {
	cfg        config.Config
	Clock      clock.Clock
	Catalog    *discovery.Catalog
	Index      *discovery.Index
	Specs      *rollout.Specs
	Plane      *traffic.Store
	Metrics    *metrics.Store
	Journal    *journal.Store
	Engine     *rollout.Engine
	Drain      *traffic.DrainTracker
	Recovery   RecoveryReport
	router     *traffic.Router
	transport  *http.Transport
	lease      *os.File
	wrapSource func(discovery.Source) discovery.Source
}

type Options struct {
	Clock      clock.Clock
	WrapPlane  func(traffic.Plane) traffic.Plane
	WrapSource func(discovery.Source) discovery.Source
	Report     func(error)
}

func Open(cfg config.Config, options Options) (_ *App, resultErr error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	lease, err := statefile.Lock(cfg.StateDirectory)
	if err != nil {
		return nil, err
	}
	defer func() {
		if resultErr != nil {
			_ = lease.Close()
		}
	}()
	at := options.Clock
	if at == nil {
		at = clock.Real{}
	}
	app := &App{cfg: cfg, Clock: at, Index: discovery.NewIndex(), lease: lease,
		Drain: traffic.NewDrainTracker(), wrapSource: options.WrapSource}
	path := func(name string) string { return filepath.Join(cfg.StateDirectory, name+".v1.json") }
	app.Catalog, err = discovery.OpenCatalog(path("endpoints"), cfg.EndpointHistory)
	if err != nil {
		return nil, fmt.Errorf("open endpoint catalog: %w", err)
	}
	app.Specs, err = rollout.OpenSpecs(path("releases"))
	if err != nil {
		return nil, fmt.Errorf("open release store: %w", err)
	}
	app.Plane, err = traffic.Open(path("traffic"))
	if err != nil {
		return nil, fmt.Errorf("open traffic store: %w", err)
	}
	app.Metrics, err = metrics.Open(path("samples"))
	if err != nil {
		return nil, fmt.Errorf("open sample store: %w", err)
	}
	app.Journal, err = journal.Open(path("decisions"), cfg.JournalRetention)
	if err != nil {
		return nil, fmt.Errorf("open decision store: %w", err)
	}
	app.Recovery, err = app.inspectRecovery()
	if err != nil {
		return nil, fmt.Errorf("reconcile v1 state at startup: %w", err)
	}
	plane := traffic.Plane(app.Plane)
	if options.WrapPlane != nil {
		plane = options.WrapPlane(plane)
	}
	app.Engine, err = rollout.New(rollout.Dependencies{
		Specs: app.Specs, Index: app.Index, Fence: app.Catalog, Plane: plane, Windows: app.Metrics, Journal: app.Journal,
		Clock: at, DefaultQuota: cfg.DefaultQuota, Quotas: cfg.Quotas, SharedQuota: cfg.SharedQuota, Drain: cfg.Drain(),
		DrainInUse: app.Drain.InUse,
	})
	if err != nil {
		return nil, err
	}
	var instance [12]byte
	if _, err := rand.Read(instance[:]); err != nil {
		return nil, err
	}
	app.transport = &http.Transport{
		DialContext:           (&net.Dialer{Timeout: cfg.UpstreamTimeout()}).DialContext,
		ResponseHeaderTimeout: cfg.UpstreamTimeout(), IdleConnTimeout: 30 * time.Second,
		MaxIdleConns: 64, MaxIdleConnsPerHost: 8,
	}
	app.router = traffic.NewRouter(app.Plane, app.Catalog, app.Drain, app.Metrics, at, app.transport, hex.EncodeToString(instance[:]), options.Report)
	return app, nil
}

func (a *App) Close() error {
	a.transport.CloseIdleConnections()
	return a.lease.Close()
}

func (a *App) Serve(ctx context.Context, listener net.Listener, report func(error)) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	httpServer := &http.Server{
		Handler: a.Handler(), ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout: 30 * time.Second, MaxHeaderBytes: 32 << 10,
		BaseContext: func(net.Listener) context.Context { return runCtx },
	}
	serving := make(chan error, 1)
	go func() { serving <- httpServer.Serve(listener) }()
	source := discovery.Source(discovery.HTTPSource{Origin: "http://" + listener.Addr().String(), Client: &http.Client{Transport: a.transport}})
	if a.wrapSource != nil {
		source = a.wrapSource(source)
	}
	followed := make(chan struct{})
	go func() {
		defer close(followed)
		discovery.Follow(runCtx, source, a.Index, a.cfg.WatchRetry(), report)
	}()
	reconciled := make(chan struct{})
	go func() {
		defer close(reconciled)
		a.Engine.Run(runCtx, a.cfg.ReconcileInterval(), a.Index.Notify(), report)
	}()
	var result error
	select {
	case <-runCtx.Done():
	case err := <-serving:
		if !errors.Is(err, http.ErrServerClosed) {
			result = err
		}
	}
	cancel()
	shutdownCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		result = errors.Join(result, err, httpServer.Close())
	}
	<-followed
	<-reconciled
	return result
}
