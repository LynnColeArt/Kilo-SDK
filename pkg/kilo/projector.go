package kilo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type projectorWorker struct {
	store     *Store
	name      string
	projector Projector
	running   bool
	lastError string
}

type projectorCheckpointStore struct {
	root       string
	syncWrites bool
}

func (s *Store) RegisterProjector(ctx context.Context, projector Projector) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if projector == nil {
		return fmt.Errorf("projector is required")
	}
	name := normalizeProjectorName(projector.Name())
	if name == "" {
		return fmt.Errorf("projector name is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("store is closed")
	}
	if _, exists := s.projectors[name]; exists {
		return fmt.Errorf("%w: projector %q already registered", ErrConflict, name)
	}
	checkpoint, hasCheckpoint := s.projectorCheckpoints[name]
	if s.compactionSeq > 0 && (!hasCheckpoint || checkpoint.IndexedSeq < s.compactionSeq) {
		return fmt.Errorf("projector %q requires compacted projection events through seq %d", name, s.compactionSeq)
	}
	if !hasCheckpoint {
		s.projectorCheckpoints[name] = ProjectionCheckpoint{Name: name}
	}
	worker := &projectorWorker{
		store:     s,
		name:      name,
		projector: projector,
		running:   true,
	}
	s.projectors[name] = worker
	s.projectorWG.Add(1)
	go worker.run()
	if s.projectorCond != nil {
		s.projectorCond.Broadcast()
	}
	return nil
}

func (s *Store) WaitForProjection(ctx context.Context, wait ProjectionWait) (ProjectionWaitResult, error) {
	if err := ctx.Err(); err != nil {
		return ProjectionWaitResult{}, err
	}
	normalized, err := normalizeProjectionWait(wait)
	if err != nil {
		return ProjectionWaitResult{}, err
	}

	var timer *time.Timer
	var timeoutC <-chan time.Time
	var deadline time.Time
	if normalized.Timeout > 0 {
		timer = time.NewTimer(normalized.Timeout)
		timeoutC = timer.C
		deadline = time.Now().Add(normalized.Timeout)
		defer timer.Stop()
	}
	done := make(chan struct{})
	defer close(done)
	go s.broadcastProjectionWait(ctx, timeoutC, done)

	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		status, ok := s.projectorStatusLocked(normalized.Projector)
		if !ok {
			return ProjectionWaitResult{}, fmt.Errorf("%w: projector %q", ErrNotFound, normalized.Projector)
		}
		result := projectionWaitResult(normalized, status, false)
		if projectionWaitSatisfied(normalized, status) {
			result.Satisfied = true
			return result, nil
		}
		if ctx.Err() != nil {
			return projectionWaitResult(normalized, status, false), ctx.Err()
		}
		if normalized.Timeout == 0 || (!deadline.IsZero() && !time.Now().Before(deadline)) {
			return projectionWaitResult(normalized, status, normalized.Timeout > 0), nil
		}
		s.projectorCond.Wait()
	}
}

func (s *Store) broadcastProjectionWait(ctx context.Context, timeoutC <-chan time.Time, done <-chan struct{}) {
	select {
	case <-ctx.Done():
	case <-timeoutC:
	case <-done:
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.projectorCond != nil {
		s.projectorCond.Broadcast()
	}
}

func (w *projectorWorker) run() {
	defer w.store.projectorWG.Done()
	defer w.markStopped()

	for {
		event, ok := w.nextEvent()
		if !ok {
			return
		}
		if err := w.projector.Project(w.store.projectorCtx, event); err != nil {
			if w.store.projectorCtx.Err() != nil {
				return
			}
			w.setError(err)
			if !sleepProjectorRetry(w.store.projectorCtx) {
				return
			}
			continue
		}
		if err := w.store.advanceProjectorCheckpoint(w.name, event.Seq); err != nil {
			if w.store.projectorCtx.Err() != nil {
				return
			}
			w.setError(err)
			if !sleepProjectorRetry(w.store.projectorCtx) {
				return
			}
			continue
		}
		w.setError(nil)
	}
}

func (w *projectorWorker) nextEvent() (ProjectionEvent, bool) {
	s := w.store
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		if s.closed || s.projectorCtx.Err() != nil {
			return ProjectionEvent{}, false
		}
		checkpoint := s.projectorCheckpoints[w.name]
		for _, event := range s.projectionEvents {
			if event.Seq > checkpoint.IndexedSeq {
				return cloneProjectionEvent(event), true
			}
		}
		s.projectorCond.Wait()
	}
}

func (w *projectorWorker) setError(err error) {
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	if err == nil {
		w.lastError = ""
		return
	}
	w.lastError = err.Error()
}

func (w *projectorWorker) markStopped() {
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	w.running = false
	if current := w.store.projectors[w.name]; current == w {
		delete(w.store.projectors, w.name)
	}
	if w.store.projectorCond != nil {
		w.store.projectorCond.Broadcast()
	}
}

func (s *Store) advanceProjectorCheckpoint(name string, indexedSeq uint64) error {
	s.mu.RLock()
	current := s.projectorCheckpoints[name]
	if current.IndexedSeq >= indexedSeq {
		s.mu.RUnlock()
		return nil
	}
	s.mu.RUnlock()

	now := s.clock.Now().UTC()
	checkpoint := ProjectionCheckpoint{
		Name:       name,
		IndexedSeq: indexedSeq,
		UpdatedAt:  now,
	}
	if err := s.checkpoints.Write(checkpoint); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	current = s.projectorCheckpoints[name]
	if current.IndexedSeq >= indexedSeq {
		return nil
	}
	s.projectorCheckpoints[name] = checkpoint
	if s.projectorCond != nil {
		s.projectorCond.Broadcast()
	}
	return nil
}

func (s *Store) appendProjectionEventLocked(seq uint64, mutation Mutation) {
	event := ProjectionEvent{
		Seq:      seq,
		Mutation: cloneMutationForProjection(mutation),
		At:       mutation.At,
	}
	s.projectionEvents = append(s.projectionEvents, event)
}

func (s *Store) projectorStatusesLocked() map[string]ProjectorStatus {
	if len(s.projectorCheckpoints) == 0 && len(s.projectors) == 0 {
		return nil
	}
	names := map[string]struct{}{}
	for name := range s.projectorCheckpoints {
		names[name] = struct{}{}
	}
	for name := range s.projectors {
		names[name] = struct{}{}
	}

	out := make(map[string]ProjectorStatus, len(names))
	for name := range names {
		out[name], _ = s.projectorStatusLocked(name)
	}
	return out
}

func (s *Store) projectorStatusLocked(name string) (ProjectorStatus, bool) {
	checkpoint, hasCheckpoint := s.projectorCheckpoints[name]
	worker, hasWorker := s.projectors[name]
	if !hasCheckpoint && !hasWorker {
		return ProjectorStatus{}, false
	}
	status := ProjectorStatus{
		Name:       name,
		DurableSeq: s.durableSeq,
		IndexedSeq: checkpoint.IndexedSeq,
		UpdatedAt:  checkpoint.UpdatedAt,
	}
	if s.durableSeq > status.IndexedSeq {
		status.Lag = s.durableSeq - status.IndexedSeq
	}
	if hasWorker {
		status.Running = worker.running
		status.LastError = worker.lastError
	}
	return status, true
}

func newProjectorCheckpointStore(root string, syncWrites bool) (*projectorCheckpointStore, map[string]ProjectionCheckpoint, error) {
	store := &projectorCheckpointStore{
		root:       filepath.Join(root, "projectors"),
		syncWrites: syncWrites,
	}
	if err := os.MkdirAll(store.root, 0o755); err != nil {
		return nil, nil, fmt.Errorf("create projector directory: %w", err)
	}
	checkpoints, err := store.Load()
	if err != nil {
		return nil, nil, err
	}
	return store, checkpoints, nil
}

func (s *projectorCheckpointStore) Load() (map[string]ProjectionCheckpoint, error) {
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return nil, fmt.Errorf("read projector checkpoints: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})
	checkpoints := map[string]ProjectionCheckpoint{}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(s.root, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read projector checkpoint %q: %w", entry.Name(), err)
		}
		var checkpoint ProjectionCheckpoint
		if err := json.Unmarshal(raw, &checkpoint); err != nil {
			return nil, fmt.Errorf("decode projector checkpoint %q: %w", entry.Name(), err)
		}
		checkpoint.Name = normalizeProjectorName(checkpoint.Name)
		if checkpoint.Name == "" {
			return nil, fmt.Errorf("projector checkpoint %q has empty name", entry.Name())
		}
		checkpoints[checkpoint.Name] = checkpoint
	}
	return checkpoints, nil
}

func (s *projectorCheckpointStore) Write(checkpoint ProjectionCheckpoint) error {
	checkpoint.Name = normalizeProjectorName(checkpoint.Name)
	if checkpoint.Name == "" {
		return fmt.Errorf("projector checkpoint name is required")
	}
	if checkpoint.UpdatedAt.IsZero() {
		checkpoint.UpdatedAt = time.Now().UTC()
	} else {
		checkpoint.UpdatedAt = checkpoint.UpdatedAt.UTC()
	}
	encoded, err := json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		return fmt.Errorf("encode projector checkpoint: %w", err)
	}
	encoded = append(encoded, '\n')

	path := s.path(checkpoint.Name)
	file, err := os.CreateTemp(s.root, ".checkpoint-*")
	if err != nil {
		return fmt.Errorf("create projector checkpoint temp file: %w", err)
	}
	tmpPath := file.Name()
	removeTemp := true
	defer func() {
		if removeTemp {
			_ = os.Remove(tmpPath)
		}
	}()
	if err := writeAll(file, encoded); err != nil {
		_ = file.Close()
		return fmt.Errorf("write projector checkpoint: %w", err)
	}
	if s.syncWrites {
		if err := file.Sync(); err != nil {
			_ = file.Close()
			return fmt.Errorf("sync projector checkpoint: %w", err)
		}
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close projector checkpoint: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("install projector checkpoint: %w", err)
	}
	removeTemp = false
	if s.syncWrites {
		if err := syncDirectory(s.root); err != nil {
			return err
		}
	}
	return nil
}

func (s *projectorCheckpointStore) path(name string) string {
	sum := sha256.Sum256([]byte(name))
	return filepath.Join(s.root, hex.EncodeToString(sum[:])+".json")
}

func cloneProjectionEvent(event ProjectionEvent) ProjectionEvent {
	event.Mutation = cloneMutationForProjection(event.Mutation)
	return event
}

func cloneMutationForProjection(mutation Mutation) Mutation {
	mutation.Record = cloneRecord(mutation.Record)
	mutation.Chunk = cloneChunk(mutation.Chunk)
	mutation.ChunkContent = nil
	mutation.SourceSpan = cloneSourceSpan(mutation.SourceSpan)
	mutation.Node = cloneNode(mutation.Node)
	mutation.Edge = cloneEdge(mutation.Edge)
	mutation.PurgeRequest = clonePurgeRequest(mutation.PurgeRequest)
	mutation.Metadata = cloneStringMap(mutation.Metadata)
	return mutation
}

func normalizeProjectorName(name string) string {
	return strings.TrimSpace(name)
}

func normalizeProjectionWait(wait ProjectionWait) (ProjectionWait, error) {
	wait.Projector = normalizeProjectorName(wait.Projector)
	if wait.Projector == "" {
		return ProjectionWait{}, fmt.Errorf("projector name is required")
	}
	if wait.Timeout < 0 {
		return ProjectionWait{}, fmt.Errorf("projection wait timeout must be non-negative")
	}
	return wait, nil
}

func projectionWaitSatisfied(wait ProjectionWait, status ProjectorStatus) bool {
	if status.IndexedSeq < wait.MinIndexedSeq {
		return false
	}
	if wait.EnforceMaxLag && status.Lag > wait.MaxLag {
		return false
	}
	return true
}

func projectionWaitResult(wait ProjectionWait, status ProjectorStatus, timedOut bool) ProjectionWaitResult {
	result := ProjectionWaitResult{
		Projector:     status,
		MinIndexedSeq: wait.MinIndexedSeq,
		MaxLag:        wait.MaxLag,
		EnforceMaxLag: wait.EnforceMaxLag,
		TimedOut:      timedOut,
	}
	result.Satisfied = projectionWaitSatisfied(wait, status)
	result.Stale = !result.Satisfied
	return result
}

func sleepProjectorRetry(ctx context.Context) bool {
	timer := time.NewTimer(10 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
