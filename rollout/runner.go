package rollout

import (
	"context"
	"errors"
	"fmt"
	"time"

	"wavegate/v1"
)

func (e *Engine) Run(ctx context.Context, interval time.Duration, discoveryNotify <-chan struct{}, report func(error)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		e.scan(ctx, report)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-e.specs.Notify():
		case <-discoveryNotify:
		}
	}
}

func (e *Engine) scan(ctx context.Context, report func(error)) {
	if ctx.Err() != nil {
		return
	}
	list, err := e.specs.List()
	if err != nil {
		if report != nil {
			report(err)
		}
		return
	}
	for _, view := range list.Items {
		if err := e.Reconcile(ctx, view.Spec.Service.Namespace, view.Spec.Name); err != nil &&
			report != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, v1.ErrNotFound) {
			report(fmt.Errorf("reconcile %s: %w", view.Spec.Key(), err))
		}
	}
}
