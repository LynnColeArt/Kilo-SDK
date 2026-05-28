package kilo

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

type sourceSpanIndexes struct {
	namespace map[string]map[string]struct{}
	projectID map[string]map[string]struct{}
	sessionID map[string]map[string]struct{}
	missionID map[string]map[string]struct{}
	recordID  map[string]map[string]struct{}
}

func (s *Store) CreateSourceSpan(ctx context.Context, span SourceSpan) (ApplyResult, error) {
	return s.Apply(ctx, Mutation{
		Type:       MutationCreateSourceSpan,
		SourceSpan: span,
	})
}

func (s *Store) ReadSourceSpan(ctx context.Context, id string) (SourceSpanRead, bool, error) {
	if err := ctx.Err(); err != nil {
		return SourceSpanRead{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	span, ok := s.sourceSpans[strings.TrimSpace(id)]
	if !ok {
		return SourceSpanRead{}, false, nil
	}
	read, err := s.sourceSpanReadLocked(span)
	if err != nil {
		return SourceSpanRead{}, false, err
	}
	return read, true, nil
}

func (s *Store) QuerySourceSpans(ctx context.Context, query SourceSpanQuery) (SourceSpanQueryResult, error) {
	if err := ctx.Err(); err != nil {
		return SourceSpanQueryResult{}, err
	}
	normalized := normalizeSourceSpanQuery(query)

	s.mu.RLock()
	defer s.mu.RUnlock()

	candidateIDs := s.sourceSpanCandidateIDsLocked(normalized)
	spans := make([]SourceSpan, 0, len(candidateIDs))
	for _, id := range candidateIDs {
		span, ok := s.sourceSpans[id]
		if !ok || !sourceSpanMatchesQuery(span, normalized) {
			continue
		}
		spans = append(spans, cloneSourceSpan(span))
	}
	sort.Slice(spans, func(i, j int) bool {
		if spans[i].UpdatedSeq == spans[j].UpdatedSeq {
			return spans[i].ID < spans[j].ID
		}
		return spans[i].UpdatedSeq > spans[j].UpdatedSeq
	})
	if normalized.Limit > 0 && len(spans) > normalized.Limit {
		spans = spans[:normalized.Limit]
	}

	result := SourceSpanQueryResult{Spans: spans}
	if normalized.IncludeCandidates {
		result.CandidateIDs = make([]string, 0, len(spans))
		for _, span := range spans {
			result.CandidateIDs = append(result.CandidateIDs, span.ID)
		}
		sort.Strings(result.CandidateIDs)
	}
	return result, nil
}

func (s *Store) applyCreateSourceSpan(seq uint64, mutation Mutation) (SourceSpan, error) {
	span := cloneSourceSpan(mutation.SourceSpan)
	if _, exists := s.sourceSpans[span.ID]; exists {
		return SourceSpan{}, fmt.Errorf("%w: source span %q already exists", ErrConflict, span.ID)
	}
	if err := s.validateSourceSpanReferencesLocked(span); err != nil {
		return SourceSpan{}, err
	}
	span.ChunkRefs = s.sortSourceSpanRefsLocked(span.ChunkRefs)
	now := mutation.At.UTC()
	if now.IsZero() {
		now = s.clock.Now().UTC()
	}
	span.Revision = 1
	span.CreatedAt = now
	span.UpdatedAt = now
	span.CreatedSeq = seq
	span.UpdatedSeq = seq
	s.sourceSpans[span.ID] = cloneSourceSpan(span)
	s.indexSourceSpanLocked(span)
	return span, nil
}

func (s *Store) sourceSpanReadLocked(span SourceSpan) (SourceSpanRead, error) {
	span = cloneSourceSpan(span)
	span.ChunkRefs = s.sortSourceSpanRefsLocked(span.ChunkRefs)
	chunks := make([]Chunk, 0, len(span.ChunkRefs))
	for _, ref := range span.ChunkRefs {
		chunk, ok := s.chunks[ref.ChunkID]
		if !ok {
			return SourceSpanRead{}, fmt.Errorf("%w: chunk %q", ErrNotFound, ref.ChunkID)
		}
		chunks = append(chunks, cloneChunk(chunk))
	}
	return SourceSpanRead{Span: span, Chunks: chunks}, nil
}

func (s *Store) validateSourceSpanReferencesLocked(span SourceSpan) error {
	record, ok := s.records[span.RecordID]
	if !ok || record.Deleted {
		return fmt.Errorf("%w: record %q", ErrNotFound, span.RecordID)
	}
	for _, ref := range span.ChunkRefs {
		chunk, ok := s.chunks[ref.ChunkID]
		if !ok {
			return fmt.Errorf("%w: chunk %q", ErrNotFound, ref.ChunkID)
		}
		if ref.StartOffset > chunk.ContentLength {
			return fmt.Errorf("%w: chunk %q start offset exceeds content length", ErrConflict, ref.ChunkID)
		}
		if ref.EndOffset > 0 && ref.EndOffset > chunk.ContentLength {
			return fmt.Errorf("%w: chunk %q end offset exceeds content length", ErrConflict, ref.ChunkID)
		}
	}
	return nil
}

func (s *Store) sortSourceSpanRefsLocked(refs []SourceSpanChunkRef) []SourceSpanChunkRef {
	out := cloneSourceSpanChunkRefs(refs)
	sort.Slice(out, func(i, j int) bool {
		left := s.chunks[out[i].ChunkID]
		right := s.chunks[out[j].ChunkID]
		if left.Sequence == right.Sequence {
			return out[i].ChunkID < out[j].ChunkID
		}
		return left.Sequence < right.Sequence
	})
	return out
}

func normalizeSourceSpan(span SourceSpan) SourceSpan {
	span.ID = strings.TrimSpace(span.ID)
	span.Namespace = strings.TrimSpace(span.Namespace)
	span.ProjectID = strings.TrimSpace(span.ProjectID)
	span.SessionID = strings.TrimSpace(span.SessionID)
	span.MissionID = strings.TrimSpace(span.MissionID)
	span.RecordID = strings.TrimSpace(span.RecordID)
	span.ChunkRefs = normalizeSourceSpanChunkRefs(span.ChunkRefs)
	span.Attributes = normalizeStringMap(span.Attributes)
	return span
}

func normalizeSourceSpanChunkRefs(refs []SourceSpanChunkRef) []SourceSpanChunkRef {
	out := make([]SourceSpanChunkRef, 0, len(refs))
	for _, ref := range refs {
		ref.ChunkID = strings.TrimSpace(ref.ChunkID)
		if ref.ChunkID == "" {
			continue
		}
		out = append(out, ref)
	}
	return out
}

func validateSourceSpanForWrite(span SourceSpan) error {
	if span.ID == "" {
		return fmt.Errorf("source span id is required")
	}
	if span.Namespace == "" {
		return fmt.Errorf("source span namespace is required")
	}
	if span.RecordID == "" {
		return fmt.Errorf("source span record id is required")
	}
	if len(span.ChunkRefs) == 0 {
		return fmt.Errorf("source span chunk refs are required")
	}
	for _, ref := range span.ChunkRefs {
		if ref.ChunkID == "" {
			return fmt.Errorf("source span chunk id is required")
		}
		if ref.StartOffset < 0 {
			return fmt.Errorf("source span start offset must be non-negative")
		}
		if ref.EndOffset < 0 {
			return fmt.Errorf("source span end offset must be non-negative")
		}
		if ref.EndOffset > 0 && ref.StartOffset > ref.EndOffset {
			return fmt.Errorf("source span start offset exceeds end offset")
		}
	}
	return nil
}

func normalizeSourceSpanQuery(query SourceSpanQuery) SourceSpanQuery {
	query.Namespace = strings.TrimSpace(query.Namespace)
	query.ProjectID = strings.TrimSpace(query.ProjectID)
	query.SessionID = strings.TrimSpace(query.SessionID)
	query.MissionID = strings.TrimSpace(query.MissionID)
	query.RecordID = strings.TrimSpace(query.RecordID)
	return query
}

func sourceSpanMatchesQuery(span SourceSpan, query SourceSpanQuery) bool {
	if query.Namespace != "" && span.Namespace != query.Namespace {
		return false
	}
	if query.ProjectID != "" && span.ProjectID != query.ProjectID {
		return false
	}
	if query.SessionID != "" && span.SessionID != query.SessionID {
		return false
	}
	if query.MissionID != "" && span.MissionID != query.MissionID {
		return false
	}
	if query.RecordID != "" && span.RecordID != query.RecordID {
		return false
	}
	return true
}

func cloneSourceSpan(span SourceSpan) SourceSpan {
	span.ChunkRefs = cloneSourceSpanChunkRefs(span.ChunkRefs)
	span.Attributes = cloneStringMap(span.Attributes)
	return span
}

func cloneSourceSpanChunkRefs(refs []SourceSpanChunkRef) []SourceSpanChunkRef {
	if refs == nil {
		return nil
	}
	out := make([]SourceSpanChunkRef, len(refs))
	copy(out, refs)
	return out
}

func newSourceSpanIndexes() sourceSpanIndexes {
	return sourceSpanIndexes{
		namespace: map[string]map[string]struct{}{},
		projectID: map[string]map[string]struct{}{},
		sessionID: map[string]map[string]struct{}{},
		missionID: map[string]map[string]struct{}{},
		recordID:  map[string]map[string]struct{}{},
	}
}

func (s *Store) indexSourceSpanLocked(span SourceSpan) {
	addIndexValue(s.spanIndexes.namespace, span.Namespace, span.ID)
	addIndexValue(s.spanIndexes.projectID, span.ProjectID, span.ID)
	addIndexValue(s.spanIndexes.sessionID, span.SessionID, span.ID)
	addIndexValue(s.spanIndexes.missionID, span.MissionID, span.ID)
	addIndexValue(s.spanIndexes.recordID, span.RecordID, span.ID)
}

func (s *Store) sourceSpanCandidateIDsLocked(query SourceSpanQuery) []string {
	var candidateSet map[string]struct{}
	intersect := func(index map[string]map[string]struct{}, value string) {
		if value == "" {
			return
		}
		bucket := index[value]
		if candidateSet == nil {
			candidateSet = cloneIDSet(bucket)
			return
		}
		for id := range candidateSet {
			if _, ok := bucket[id]; !ok {
				delete(candidateSet, id)
			}
		}
	}

	intersect(s.spanIndexes.namespace, query.Namespace)
	intersect(s.spanIndexes.projectID, query.ProjectID)
	intersect(s.spanIndexes.sessionID, query.SessionID)
	intersect(s.spanIndexes.missionID, query.MissionID)
	intersect(s.spanIndexes.recordID, query.RecordID)
	if candidateSet == nil {
		candidateSet = make(map[string]struct{}, len(s.sourceSpans))
		for id := range s.sourceSpans {
			candidateSet[id] = struct{}{}
		}
	}

	ids := make([]string, 0, len(candidateSet))
	for id := range candidateSet {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
