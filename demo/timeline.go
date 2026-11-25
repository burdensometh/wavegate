package demo

import (
	"context"
	"net/http"

	"wavegate/discovery"
	"wavegate/journal"
	"wavegate/v1"
)

type endpointTimeline struct {
	APIVersion string             `json:"apiVersion"`
	List       v1.EndpointList    `json:"list"`
	Events     []v1.EndpointEvent `json:"events"`
}

func (s *scenario) exportTimeline(ctx context.Context) error {
	source := discovery.HTTPSource{Origin: s.api.URL, Client: s.api.Client()}
	list, err := source.List(ctx)
	if err != nil {
		return err
	}
	stream, err := source.Watch(ctx, 0)
	if err != nil {
		return err
	}
	defer stream.Close()
	timeline := endpointTimeline{APIVersion: v1.Version, List: list, Events: []v1.EndpointEvent{}}
	for i := uint64(0); i < list.ResourceVersion; i++ {
		event, err := stream.Next(ctx)
		if err != nil {
			return err
		}
		timeline.Events = append(timeline.Events, event)
	}
	if err := s.write("endpoint-events.v1.json", timeline); err != nil {
		return err
	}
	events, err := call[journal.Page](ctx, s.api, http.MethodGet, "/v1/decisions?after=0&limit=100", nil)
	if err != nil {
		return err
	}
	return s.write("decision-events.v1.json", events)
}
