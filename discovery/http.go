package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"wavegate/v1"
)

type HTTPSource struct {
	Origin string
	Client *http.Client
}

func (s HTTPSource) get(ctx context.Context, suffix string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.Origin+"/v1/endpoints"+suffix, nil)
	if err != nil {
		return nil, err
	}
	response, err := s.Client.Do(req)
	if err != nil {
		return nil, err
	}
	switch response.StatusCode {
	case http.StatusOK:
		return response, nil
	case http.StatusGone:
		err = v1.ErrExpired
	case http.StatusConflict:
		err = v1.ErrConflict
	default:
		err = fmt.Errorf("%w: endpoint API returned %d", v1.ErrUnavailable, response.StatusCode)
	}
	response.Body.Close()
	return nil, err
}

func (s HTTPSource) List(ctx context.Context) (v1.EndpointList, error) {
	var list v1.EndpointList
	response, err := s.get(ctx, "")
	if err != nil {
		return list, err
	}
	defer response.Body.Close()
	err = v1.Decode(io.LimitReader(response.Body, 64*v1.MaxBody), &list)
	return list, err
}

func (s HTTPSource) Watch(ctx context.Context, from uint64) (Stream, error) {
	response, err := s.get(ctx, "?watch=1&resourceVersion="+strconv.FormatUint(from, 10))
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(response.Body)
	decoder.DisallowUnknownFields()
	return &httpStream{body: response.Body, decoder: decoder}, nil
}

type httpStream struct {
	body    io.ReadCloser
	decoder *json.Decoder
}

func (s *httpStream) Close() error { return s.body.Close() }

func (s *httpStream) Next(ctx context.Context) (v1.EndpointEvent, error) {
	var event v1.EndpointEvent
	if err := ctx.Err(); err != nil {
		return event, err
	}
	err := s.decoder.Decode(&event)
	return event, err
}
