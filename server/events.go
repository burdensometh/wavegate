package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"wavegate/v1"
)

type decisionFrame struct {
	APIVersion string             `json:"apiVersion"`
	Type       string             `json:"type"`
	Record     *v1.DecisionRecord `json:"record,omitempty"`
	Checkpoint uint64             `json:"checkpointSequence,omitempty"`
}

func (a *App) watchDecisions(w http.ResponseWriter, r *http.Request) {
	after, err := number(r.URL.Query().Get("after"), 0)
	if err != nil {
		respondError(w, err)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		respondError(w, v1.ErrUnavailable)
		return
	}
	cursor, err := a.Journal.Watch(after)
	if errors.Is(err, v1.ErrExpired) {
		reply(w, http.StatusGone, problem{
			APIVersion: v1.Version, Code: http.StatusGone, Message: "decision cursor compacted",
			Checkpoint: a.Journal.CheckpointSequence(),
		})
		return
	}
	if err != nil {
		respondError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	encoder := json.NewEncoder(w)
	for {
		record, err := cursor.Next(r.Context())
		if errors.Is(err, v1.ErrExpired) {
			_ = encoder.Encode(decisionFrame{APIVersion: v1.Version, Type: "RESET", Checkpoint: a.Journal.CheckpointSequence()})
			flusher.Flush()
		}
		if err != nil || encoder.Encode(decisionFrame{APIVersion: v1.Version, Type: "DECISION", Record: &record}) != nil {
			return
		}
		flusher.Flush()
	}
}
