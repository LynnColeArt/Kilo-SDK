package kilo

import (
	"context"
	"fmt"
	"time"
)

type TailQuery struct {
	MinSeq uint64 `json:"min_seq,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}

type TailEntry struct {
	Seq      uint64    `json:"seq"`
	Mutation Mutation  `json:"mutation"`
	At       time.Time `json:"at,omitempty"`
}

type TailResult struct {
	Entries []TailEntry `json:"entries"`
}

func (s *Store) Tail(ctx context.Context, query TailQuery) (TailResult, error) {
	if err := ctx.Err(); err != nil {
		return TailResult{}, err
	}
	if query.Limit < 0 {
		return TailResult{}, fmt.Errorf("tail limit must be non-negative")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := make([]TailEntry, 0, len(s.projectionEvents))
	for _, event := range s.projectionEvents {
		if query.MinSeq > 0 && event.Seq < query.MinSeq {
			continue
		}
		entries = append(entries, TailEntry{
			Seq:      event.Seq,
			Mutation: cloneMutationForProjection(event.Mutation),
			At:       event.At,
		})
	}
	if query.Limit > 0 && len(entries) > query.Limit {
		entries = entries[len(entries)-query.Limit:]
	}
	return TailResult{Entries: entries}, nil
}
