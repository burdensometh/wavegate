package journal

import (
	"context"

	"wavegate/v1"
)

type Cursor struct {
	store *Store
	after uint64
}

func (s *Store) Watch(after uint64) (*Cursor, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.checkCursor(after); err != nil {
		return nil, err
	}
	return &Cursor{store: s, after: after}, nil
}

func (s *Store) checkCursor(after uint64) error {
	if s.uncertain != nil {
		return s.uncertain
	}
	if after < s.doc.Checkpoint.Sequence {
		return v1.ErrExpired
	}
	if after > s.sequence {
		return v1.ErrConflict
	}
	return nil
}

func (s *Store) CheckpointSequence() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.doc.Checkpoint.Sequence
}

func (s *Cursor) Next(ctx context.Context) (v1.DecisionRecord, error) {
	store := s.store
	for {
		if err := ctx.Err(); err != nil {
			return v1.DecisionRecord{}, err
		}
		store.mu.RLock()
		if err := store.checkCursor(s.after); err != nil {
			store.mu.RUnlock()
			return v1.DecisionRecord{}, err
		}
		if s.after < store.sequence {
			index := s.after - store.doc.Checkpoint.Sequence
			record := store.doc.Records[index]
			store.mu.RUnlock()
			copied, err := v1.Copy(record)
			if err == nil {
				s.after = record.Sequence
			}
			return copied, err
		}
		changed := store.changed
		store.mu.RUnlock()
		select {
		case <-ctx.Done():
			return v1.DecisionRecord{}, ctx.Err()
		case <-changed:
		}
	}
}
