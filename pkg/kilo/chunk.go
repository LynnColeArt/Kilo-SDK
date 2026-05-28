package kilo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type chunkIndexes struct {
	namespace map[string]map[string]struct{}
	projectID map[string]map[string]struct{}
	sessionID map[string]map[string]struct{}
	missionID map[string]map[string]struct{}
	kind      map[string]map[string]struct{}
}

type chunkBodyStore struct {
	root       string
	syncWrites bool
}

func (s *Store) CreateChunk(ctx context.Context, chunk Chunk, content []byte) (ApplyResult, error) {
	return s.Apply(ctx, Mutation{
		Type:         MutationCreateChunk,
		Chunk:        chunk,
		ChunkContent: content,
	})
}

func (s *Store) GetChunk(ctx context.Context, id string) (Chunk, bool, error) {
	if err := ctx.Err(); err != nil {
		return Chunk{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	chunk, ok := s.chunks[strings.TrimSpace(id)]
	if !ok {
		return Chunk{}, false, nil
	}
	return cloneChunk(chunk), true, nil
}

func (s *Store) ReadChunkContent(ctx context.Context, id string) ([]byte, bool, error) {
	chunk, found, err := s.GetChunk(ctx, id)
	if err != nil || !found {
		return nil, found, err
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	body, err := s.chunkBodies.Read(chunk.ID)
	if err != nil {
		return nil, false, fmt.Errorf("read chunk %q content: %w", chunk.ID, err)
	}
	if int64(len(body)) != chunk.ContentLength {
		return nil, false, fmt.Errorf("%w: chunk %q content length mismatch: expected %d got %d", ErrCorrupt, chunk.ID, chunk.ContentLength, len(body))
	}
	if got := hashChunkContent(body); got != chunk.ContentHash {
		return nil, false, fmt.Errorf("%w: chunk %q content hash mismatch: expected %s got %s", ErrCorrupt, chunk.ID, chunk.ContentHash, got)
	}
	return body, true, nil
}

func (s *Store) QueryChunks(ctx context.Context, query ChunkQuery) (ChunkQueryResult, error) {
	if err := ctx.Err(); err != nil {
		return ChunkQueryResult{}, err
	}
	normalized := normalizeChunkQuery(query)

	s.mu.RLock()
	defer s.mu.RUnlock()

	candidateIDs := s.chunkCandidateIDsLocked(normalized)
	chunks := make([]Chunk, 0, len(candidateIDs))
	for _, id := range candidateIDs {
		chunk, ok := s.chunks[id]
		if !ok || !chunkMatchesQuery(chunk, normalized) {
			continue
		}
		chunks = append(chunks, cloneChunk(chunk))
	}
	sort.Slice(chunks, func(i, j int) bool {
		if chunks[i].Sequence == chunks[j].Sequence {
			return chunks[i].ID < chunks[j].ID
		}
		return chunks[i].Sequence < chunks[j].Sequence
	})
	if normalized.Limit > 0 && len(chunks) > normalized.Limit {
		chunks = chunks[:normalized.Limit]
	}

	result := ChunkQueryResult{Chunks: chunks}
	if normalized.IncludeCandidates {
		result.CandidateIDs = make([]string, 0, len(chunks))
		for _, chunk := range chunks {
			result.CandidateIDs = append(result.CandidateIDs, chunk.ID)
		}
	}
	return result, nil
}

func (s *Store) applyCreateChunk(seq uint64, mutation Mutation) (Chunk, error) {
	chunk := cloneChunk(mutation.Chunk)
	if _, exists := s.chunks[chunk.ID]; exists {
		return Chunk{}, fmt.Errorf("%w: chunk %q already exists", ErrConflict, chunk.ID)
	}
	now := mutation.At.UTC()
	if now.IsZero() {
		now = s.clock.Now().UTC()
	}
	chunk.Revision = 1
	chunk.CreatedAt = now
	chunk.UpdatedAt = now
	chunk.CreatedSeq = seq
	chunk.UpdatedSeq = seq
	s.chunks[chunk.ID] = cloneChunk(chunk)
	s.indexChunkLocked(chunk)
	return chunk, nil
}

func newChunkBodyStore(root string, syncWrites bool) (*chunkBodyStore, error) {
	store := &chunkBodyStore{
		root:       filepath.Join(root, "chunks"),
		syncWrites: syncWrites,
	}
	if err := os.MkdirAll(store.root, 0o755); err != nil {
		return nil, fmt.Errorf("create chunk directory: %w", err)
	}
	return store, nil
}

func (b *chunkBodyStore) Write(id string, content []byte) error {
	path := b.path(id)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create chunk body directory: %w", err)
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".chunk-*")
	if err != nil {
		return fmt.Errorf("create chunk body temp file: %w", err)
	}
	tmpPath := file.Name()
	removeTemp := true
	defer func() {
		if removeTemp {
			_ = os.Remove(tmpPath)
		}
	}()
	if err := writeAll(file, content); err != nil {
		_ = file.Close()
		return fmt.Errorf("write chunk body: %w", err)
	}
	if b.syncWrites {
		if err := file.Sync(); err != nil {
			_ = file.Close()
			return fmt.Errorf("sync chunk body: %w", err)
		}
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close chunk body: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("install chunk body: %w", err)
	}
	removeTemp = false
	if b.syncWrites {
		if err := syncDirectory(filepath.Dir(path)); err != nil {
			return err
		}
	}
	return nil
}

func (b *chunkBodyStore) Read(id string) ([]byte, error) {
	return os.ReadFile(b.path(id))
}

func (b *chunkBodyStore) path(id string) string {
	sum := sha256.Sum256([]byte(id))
	encoded := hex.EncodeToString(sum[:])
	return filepath.Join(b.root, encoded[:2], encoded[2:4], encoded+".chunk")
}

func prepareChunkForCreate(chunk Chunk, content []byte) (Chunk, error) {
	chunk = normalizeChunk(chunk)
	if err := validateChunkForWrite(chunk); err != nil {
		return Chunk{}, err
	}
	if len(content) == 0 {
		return Chunk{}, fmt.Errorf("chunk content is required")
	}
	chunk.ContentHash = hashChunkContent(content)
	chunk.ContentLength = int64(len(content))
	return chunk, nil
}

func normalizeChunk(chunk Chunk) Chunk {
	chunk.ID = strings.TrimSpace(chunk.ID)
	chunk.Namespace = strings.TrimSpace(chunk.Namespace)
	chunk.ProjectID = strings.TrimSpace(chunk.ProjectID)
	chunk.SessionID = strings.TrimSpace(chunk.SessionID)
	chunk.MissionID = strings.TrimSpace(chunk.MissionID)
	chunk.Kind = strings.TrimSpace(chunk.Kind)
	chunk.ContentHash = strings.TrimSpace(chunk.ContentHash)
	chunk.Attributes = normalizeStringMap(chunk.Attributes)
	return chunk
}

func validateChunkForWrite(chunk Chunk) error {
	if chunk.ID == "" {
		return fmt.Errorf("chunk id is required")
	}
	if chunk.Namespace == "" {
		return fmt.Errorf("chunk namespace is required")
	}
	if chunk.Kind == "" {
		return fmt.Errorf("chunk kind is required")
	}
	if chunk.Sequence == 0 {
		return fmt.Errorf("chunk sequence is required")
	}
	return nil
}

func normalizeChunkQuery(query ChunkQuery) ChunkQuery {
	query.Namespace = strings.TrimSpace(query.Namespace)
	query.ProjectID = strings.TrimSpace(query.ProjectID)
	query.SessionID = strings.TrimSpace(query.SessionID)
	query.MissionID = strings.TrimSpace(query.MissionID)
	query.Kind = strings.TrimSpace(query.Kind)
	return query
}

func chunkMatchesQuery(chunk Chunk, query ChunkQuery) bool {
	if query.Namespace != "" && chunk.Namespace != query.Namespace {
		return false
	}
	if query.ProjectID != "" && chunk.ProjectID != query.ProjectID {
		return false
	}
	if query.SessionID != "" && chunk.SessionID != query.SessionID {
		return false
	}
	if query.MissionID != "" && chunk.MissionID != query.MissionID {
		return false
	}
	if query.Kind != "" && chunk.Kind != query.Kind {
		return false
	}
	if query.MinSequence > 0 && chunk.Sequence < query.MinSequence {
		return false
	}
	if query.MaxSequence > 0 && chunk.Sequence > query.MaxSequence {
		return false
	}
	return true
}

func cloneChunk(chunk Chunk) Chunk {
	chunk.Attributes = cloneStringMap(chunk.Attributes)
	return chunk
}

func newChunkIndexes() chunkIndexes {
	return chunkIndexes{
		namespace: map[string]map[string]struct{}{},
		projectID: map[string]map[string]struct{}{},
		sessionID: map[string]map[string]struct{}{},
		missionID: map[string]map[string]struct{}{},
		kind:      map[string]map[string]struct{}{},
	}
}

func (s *Store) indexChunkLocked(chunk Chunk) {
	addIndexValue(s.chunkIndexes.namespace, chunk.Namespace, chunk.ID)
	addIndexValue(s.chunkIndexes.projectID, chunk.ProjectID, chunk.ID)
	addIndexValue(s.chunkIndexes.sessionID, chunk.SessionID, chunk.ID)
	addIndexValue(s.chunkIndexes.missionID, chunk.MissionID, chunk.ID)
	addIndexValue(s.chunkIndexes.kind, chunk.Kind, chunk.ID)
}

func (s *Store) chunkCandidateIDsLocked(query ChunkQuery) []string {
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

	intersect(s.chunkIndexes.namespace, query.Namespace)
	intersect(s.chunkIndexes.projectID, query.ProjectID)
	intersect(s.chunkIndexes.sessionID, query.SessionID)
	intersect(s.chunkIndexes.missionID, query.MissionID)
	intersect(s.chunkIndexes.kind, query.Kind)
	if candidateSet == nil {
		candidateSet = make(map[string]struct{}, len(s.chunks))
		for id := range s.chunks {
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

func hashChunkContent(content []byte) string {
	sum := sha256.Sum256(content)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func cloneBytes(values []byte) []byte {
	if values == nil {
		return nil
	}
	cloned := make([]byte, len(values))
	copy(cloned, values)
	return cloned
}

func writeAll(file *os.File, content []byte) error {
	for len(content) > 0 {
		n, err := file.Write(content)
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("short write")
		}
		content = content[n:]
	}
	return nil
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open chunk body directory for sync: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync chunk body directory: %w", err)
	}
	return nil
}
