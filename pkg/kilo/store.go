package kilo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/LynnColeArt/Kilo-SDK/internal/segmentlog"
)

type Store struct {
	mu                   sync.RWMutex
	path                 string
	log                  *segmentlog.Log
	ownerLock            *storeOwnerLock
	chunkBodies          *chunkBodyStore
	checkpoints          *projectorCheckpointStore
	clock                Clock
	syncWrites           bool
	durableSeq           uint64
	snapshotSeq          uint64
	compactionSeq        uint64
	projectionEvents     []ProjectionEvent
	projectorCheckpoints map[string]ProjectionCheckpoint
	projectors           map[string]*projectorWorker
	projectorCond        *sync.Cond
	projectorWG          sync.WaitGroup
	projectorCtx         context.Context
	projectorCancel      context.CancelFunc
	records              map[string]Record
	chunks               map[string]Chunk
	sourceSpans          map[string]SourceSpan
	nodes                map[string]Node
	edges                map[string]Edge
	purgeRequests        map[string]PurgeRequest
	indexes              recordIndexes
	chunkIndexes         chunkIndexes
	spanIndexes          sourceSpanIndexes
	graph                graphIndexes
	idempotency          map[string]ApplyResult
	closed               bool
}

type recordIndexes struct {
	namespace map[string]map[string]struct{}
	projectID map[string]map[string]struct{}
	kind      map[string]map[string]struct{}
	tag       map[string]map[string]struct{}
	humanID   map[string]map[string]struct{}
	sessionID map[string]map[string]struct{}
	missionID map[string]map[string]struct{}
	persona   map[string]map[string]struct{}
}

type graphIndexes struct {
	outgoing map[string]map[string]struct{}
	incoming map[string]map[string]struct{}
}

type systemClock struct{}

func (systemClock) Now() time.Time {
	return time.Now().UTC()
}

func Open(ctx context.Context, opts Options) (*Store, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path := strings.TrimSpace(opts.Path)
	if path == "" {
		return nil, fmt.Errorf("store path is required")
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, fmt.Errorf("create store path: %w", err)
	}
	if resolved, err := filepath.Abs(path); err == nil {
		path = resolved
	}
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		path = resolved
	}
	clock := opts.Clock
	if clock == nil {
		clock = systemClock{}
	}
	ownerLock, err := acquireStoreOwnerLock(path, clock.Now().UTC(), opts.SyncWrites)
	if err != nil {
		return nil, err
	}
	var log *segmentlog.Log
	var projectorCancel context.CancelFunc
	cleanup := func() {
		if projectorCancel != nil {
			projectorCancel()
		}
		if log != nil {
			_ = log.Close()
		}
		_ = ownerLock.Close()
	}
	log, entries, err := segmentlog.Open(path, opts.SyncWrites)
	if err != nil {
		_ = ownerLock.Close()
		return nil, err
	}
	chunkBodies, err := newChunkBodyStore(path, opts.SyncWrites)
	if err != nil {
		cleanup()
		return nil, err
	}
	checkpointStore, checkpoints, err := newProjectorCheckpointStore(path, opts.SyncWrites)
	if err != nil {
		cleanup()
		return nil, err
	}
	projectorCtx, cancel := context.WithCancel(context.Background())
	projectorCancel = cancel
	store := &Store{
		log:                  log,
		path:                 path,
		ownerLock:            ownerLock,
		chunkBodies:          chunkBodies,
		checkpoints:          checkpointStore,
		clock:                clock,
		syncWrites:           opts.SyncWrites,
		projectorCheckpoints: checkpoints,
		projectors:           map[string]*projectorWorker{},
		projectorCtx:         projectorCtx,
		projectorCancel:      projectorCancel,
		records:              map[string]Record{},
		chunks:               map[string]Chunk{},
		sourceSpans:          map[string]SourceSpan{},
		nodes:                map[string]Node{},
		edges:                map[string]Edge{},
		purgeRequests:        map[string]PurgeRequest{},
		indexes:              newRecordIndexes(),
		chunkIndexes:         newChunkIndexes(),
		spanIndexes:          newSourceSpanIndexes(),
		graph:                newGraphIndexes(),
		idempotency:          map[string]ApplyResult{},
	}
	store.projectorCond = sync.NewCond(&store.mu)
	if snapshot, ok, err := loadCatalogSnapshot(path); err != nil {
		cleanup()
		return nil, fmt.Errorf("load catalog snapshot: %w", err)
	} else if ok {
		store.installCatalogSnapshot(snapshot.Data)
	}
	if compaction, ok, err := loadLatestCompactionManifest(path); err != nil {
		cleanup()
		return nil, fmt.Errorf("load compaction manifest: %w", err)
	} else if ok {
		store.compactionSeq = compaction.CompactedSeq
		if store.snapshotSeq < store.compactionSeq {
			cleanup()
			return nil, fmt.Errorf("compaction manifest covers seq %d but snapshot covers seq %d", store.compactionSeq, store.snapshotSeq)
		}
	}
	for _, entry := range entries {
		var mutation Mutation
		if err := json.Unmarshal(entry.Payload, &mutation); err != nil {
			cleanup()
			return nil, fmt.Errorf("decode mutation at seq %d: %w", entry.Seq, err)
		}
		if entry.Seq <= store.durableSeq {
			store.appendProjectionEventLocked(entry.Seq, mutation)
			continue
		}
		if _, err := store.applyLocked(entry.Seq, mutation, true); err != nil {
			cleanup()
			return nil, fmt.Errorf("replay mutation at seq %d: %w", entry.Seq, err)
		}
	}
	return store, nil
}

func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	if s.projectorCancel != nil {
		s.projectorCancel()
	}
	if s.projectorCond != nil {
		s.projectorCond.Broadcast()
	}
	log := s.log
	ownerLock := s.ownerLock
	s.mu.Unlock()

	s.projectorWG.Wait()
	var err error
	if log == nil {
		if ownerLock == nil {
			return nil
		}
		return ownerLock.Close()
	}
	err = log.Close()
	if ownerLock != nil {
		err = errors.Join(err, ownerLock.Close())
	}
	return err
}

func (s *Store) Apply(ctx context.Context, mutation Mutation) (ApplyResult, error) {
	if err := ctx.Err(); err != nil {
		return ApplyResult{}, err
	}
	normalized, err := normalizeMutation(mutation)
	if err != nil {
		return ApplyResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ApplyResult{}, fmt.Errorf("store is closed")
	}
	if normalized.IdempotencyKey != "" {
		if existing, ok := s.idempotency[normalized.IdempotencyKey]; ok {
			existing.Idempotent = true
			return cloneApplyResult(existing), nil
		}
	}
	if normalized.At.IsZero() {
		normalized.At = s.clock.Now().UTC()
	} else {
		normalized.At = normalized.At.UTC()
	}
	if err := s.preflightLocked(normalized); err != nil {
		return ApplyResult{}, err
	}
	if normalized.Type == MutationCreateChunk {
		if err := s.chunkBodies.Write(normalized.Chunk.ID, normalized.ChunkContent); err != nil {
			return ApplyResult{}, err
		}
	}
	seq := s.durableSeq + 1
	payload, err := json.Marshal(normalized)
	if err != nil {
		return ApplyResult{}, fmt.Errorf("encode mutation: %w", err)
	}
	if err := s.log.Append(seq, payload); err != nil {
		return ApplyResult{}, err
	}
	return s.applyLocked(seq, normalized, false)
}

func (s *Store) GetRecord(ctx context.Context, id string) (Record, bool, error) {
	record, found, err := s.getRecord(ctx, id)
	if err != nil || !found || record.Deleted {
		return Record{}, false, err
	}
	return record, true, nil
}

func (s *Store) GetRecordIncludingDeleted(ctx context.Context, id string) (Record, bool, error) {
	return s.getRecord(ctx, id)
}

func (s *Store) Status() Status {
	s.mu.RLock()
	defer s.mu.RUnlock()
	status := Status{
		DurableSeq:    s.durableSeq,
		SnapshotSeq:   s.snapshotSeq,
		CompactionSeq: s.compactionSeq,
	}
	if s.ownerLock != nil {
		status.Owner = s.ownerLock.Owner()
	}
	for _, record := range s.records {
		if record.Deleted {
			status.Tombstones++
		} else {
			status.Records++
		}
	}
	status.Chunks = len(s.chunks)
	status.SourceSpans = len(s.sourceSpans)
	for _, node := range s.nodes {
		if !node.Deleted {
			status.Nodes++
		}
	}
	for _, edge := range s.edges {
		if !edge.Deleted {
			status.Edges++
		}
	}
	status.PurgeRequests = len(s.purgeRequests)
	status.Projectors = s.projectorStatusesLocked()
	return status
}

func (s *Store) QueryRecords(ctx context.Context, query RecordQuery) (RecordQueryResult, error) {
	if err := ctx.Err(); err != nil {
		return RecordQueryResult{}, err
	}
	normalized := normalizeRecordQuery(query)

	s.mu.RLock()
	defer s.mu.RUnlock()

	candidateIDs := s.candidateIDsLocked(normalized)
	records := make([]Record, 0, len(candidateIDs))
	for _, id := range candidateIDs {
		record, ok := s.records[id]
		if !ok {
			continue
		}
		if !normalized.IncludeDeleted && record.Deleted {
			continue
		}
		if !recordMatchesQuery(record, normalized) {
			continue
		}
		records = append(records, cloneRecord(record))
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].UpdatedSeq == records[j].UpdatedSeq {
			return records[i].ID < records[j].ID
		}
		return records[i].UpdatedSeq > records[j].UpdatedSeq
	})
	if normalized.Limit > 0 && len(records) > normalized.Limit {
		records = records[:normalized.Limit]
	}

	result := RecordQueryResult{Records: records}
	if normalized.IncludeAuditIDs {
		result.CandidateIDs = append([]string{}, candidateIDs...)
		sort.Strings(result.CandidateIDs)
	}
	return result, nil
}

func (s *Store) getRecord(ctx context.Context, id string) (Record, bool, error) {
	if err := ctx.Err(); err != nil {
		return Record{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	record, ok := s.records[strings.TrimSpace(id)]
	if !ok {
		return Record{}, false, nil
	}
	return cloneRecord(record), true, nil
}

func (s *Store) preflightLocked(mutation Mutation) error {
	switch mutation.Type {
	case MutationCreateRecord:
		if _, exists := s.records[mutation.Record.ID]; exists {
			return fmt.Errorf("%w: record %q already exists", ErrConflict, mutation.Record.ID)
		}
	case MutationUpdateRecord:
		existing, ok := s.records[mutation.Record.ID]
		if !ok || existing.Deleted {
			return fmt.Errorf("%w: record %q", ErrNotFound, mutation.Record.ID)
		}
		if mutation.ExpectedRevision > 0 && mutation.ExpectedRevision != existing.Revision {
			return fmt.Errorf("%w: expected revision %d got %d", ErrConflict, mutation.ExpectedRevision, existing.Revision)
		}
	case MutationDeleteRecord:
		existing, ok := s.records[mutation.Record.ID]
		if !ok || existing.Deleted {
			return fmt.Errorf("%w: record %q", ErrNotFound, mutation.Record.ID)
		}
		if mutation.ExpectedRevision > 0 && mutation.ExpectedRevision != existing.Revision {
			return fmt.Errorf("%w: expected revision %d got %d", ErrConflict, mutation.ExpectedRevision, existing.Revision)
		}
	case MutationCreateChunk:
		if _, exists := s.chunks[mutation.Chunk.ID]; exists {
			return fmt.Errorf("%w: chunk %q already exists", ErrConflict, mutation.Chunk.ID)
		}
	case MutationCreateSourceSpan:
		if _, exists := s.sourceSpans[mutation.SourceSpan.ID]; exists {
			return fmt.Errorf("%w: source span %q already exists", ErrConflict, mutation.SourceSpan.ID)
		}
		if err := s.validateSourceSpanReferencesLocked(mutation.SourceSpan); err != nil {
			return err
		}
	case MutationCreateNode:
		if _, exists := s.nodes[mutation.Node.ID]; exists {
			return fmt.Errorf("%w: node %q already exists", ErrConflict, mutation.Node.ID)
		}
	case MutationUpdateNode:
		existing, ok := s.nodes[mutation.Node.ID]
		if !ok || existing.Deleted {
			return fmt.Errorf("%w: node %q", ErrNotFound, mutation.Node.ID)
		}
		if mutation.ExpectedRevision > 0 && mutation.ExpectedRevision != existing.Revision {
			return fmt.Errorf("%w: expected revision %d got %d", ErrConflict, mutation.ExpectedRevision, existing.Revision)
		}
	case MutationDeleteNode:
		existing, ok := s.nodes[mutation.Node.ID]
		if !ok || existing.Deleted {
			return fmt.Errorf("%w: node %q", ErrNotFound, mutation.Node.ID)
		}
		if mutation.ExpectedRevision > 0 && mutation.ExpectedRevision != existing.Revision {
			return fmt.Errorf("%w: expected revision %d got %d", ErrConflict, mutation.ExpectedRevision, existing.Revision)
		}
	case MutationCreateEdge:
		if _, exists := s.edges[mutation.Edge.ID]; exists {
			return fmt.Errorf("%w: edge %q already exists", ErrConflict, mutation.Edge.ID)
		}
		if err := s.validateEdgeEndpointsLocked(mutation.Edge); err != nil {
			return err
		}
	case MutationDeleteEdge:
		existing, ok := s.edges[mutation.Edge.ID]
		if !ok || existing.Deleted {
			return fmt.Errorf("%w: edge %q", ErrNotFound, mutation.Edge.ID)
		}
		if mutation.ExpectedRevision > 0 && mutation.ExpectedRevision != existing.Revision {
			return fmt.Errorf("%w: expected revision %d got %d", ErrConflict, mutation.ExpectedRevision, existing.Revision)
		}
	case MutationCreatePurgeRequest:
		if _, exists := s.purgeRequests[mutation.PurgeRequest.ID]; exists {
			return fmt.Errorf("%w: purge request %q already exists", ErrConflict, mutation.PurgeRequest.ID)
		}
	}
	return nil
}

func (s *Store) applyLocked(seq uint64, mutation Mutation, replay bool) (ApplyResult, error) {
	if mutation.IdempotencyKey != "" {
		if existing, ok := s.idempotency[mutation.IdempotencyKey]; ok {
			existing.Idempotent = true
			return cloneApplyResult(existing), nil
		}
	}
	result := ApplyResult{Seq: seq}
	var err error
	switch mutation.Type {
	case MutationCreateRecord:
		result.Record, err = s.applyCreate(seq, mutation)
	case MutationUpdateRecord:
		result.Record, err = s.applyUpdate(seq, mutation)
	case MutationDeleteRecord:
		result.Record, err = s.applyDelete(seq, mutation)
	case MutationCreateChunk:
		result.Chunk, err = s.applyCreateChunk(seq, mutation)
	case MutationCreateSourceSpan:
		result.SourceSpan, err = s.applyCreateSourceSpan(seq, mutation)
	case MutationCreateNode:
		result.Node, err = s.applyCreateNode(seq, mutation)
	case MutationUpdateNode:
		result.Node, err = s.applyUpdateNode(seq, mutation)
	case MutationDeleteNode:
		result.Node, err = s.applyDeleteNode(seq, mutation)
	case MutationCreateEdge:
		result.Edge, err = s.applyCreateEdge(seq, mutation)
	case MutationDeleteEdge:
		result.Edge, err = s.applyDeleteEdge(seq, mutation)
	case MutationCreatePurgeRequest:
		result.PurgeRequest, err = s.applyCreatePurgeRequest(seq, mutation)
	default:
		err = fmt.Errorf("unsupported mutation type %q", mutation.Type)
	}
	if err != nil {
		return ApplyResult{}, err
	}
	s.durableSeq = seq
	result.Record = cloneRecord(result.Record)
	result.Chunk = cloneChunk(result.Chunk)
	result.SourceSpan = cloneSourceSpan(result.SourceSpan)
	result.Node = cloneNode(result.Node)
	result.Edge = cloneEdge(result.Edge)
	result.PurgeRequest = clonePurgeRequest(result.PurgeRequest)
	if mutation.IdempotencyKey != "" {
		s.idempotency[mutation.IdempotencyKey] = cloneApplyResult(result)
	}
	s.appendProjectionEventLocked(seq, mutation)
	if replay {
		result.Idempotent = false
	} else if s.projectorCond != nil {
		s.projectorCond.Broadcast()
	}
	return result, nil
}

func (s *Store) applyCreate(seq uint64, mutation Mutation) (Record, error) {
	record := cloneRecord(mutation.Record)
	if _, exists := s.records[record.ID]; exists {
		return Record{}, fmt.Errorf("%w: record %q already exists", ErrConflict, record.ID)
	}
	now := mutation.At.UTC()
	if now.IsZero() {
		now = s.clock.Now().UTC()
	}
	record.Revision = 1
	record.Deleted = false
	record.CreatedAt = now
	record.UpdatedAt = now
	record.CreatedSeq = seq
	record.UpdatedSeq = seq
	s.records[record.ID] = cloneRecord(record)
	s.indexRecordLocked(record)
	return record, nil
}

func (s *Store) applyUpdate(seq uint64, mutation Mutation) (Record, error) {
	existing, ok := s.records[mutation.Record.ID]
	if !ok || existing.Deleted {
		return Record{}, fmt.Errorf("%w: record %q", ErrNotFound, mutation.Record.ID)
	}
	if mutation.ExpectedRevision > 0 && mutation.ExpectedRevision != existing.Revision {
		return Record{}, fmt.Errorf("%w: expected revision %d got %d", ErrConflict, mutation.ExpectedRevision, existing.Revision)
	}
	updated := cloneRecord(mutation.Record)
	updated.Revision = existing.Revision + 1
	updated.Deleted = false
	updated.CreatedAt = existing.CreatedAt
	updated.CreatedSeq = existing.CreatedSeq
	updated.UpdatedAt = mutation.At.UTC()
	if updated.UpdatedAt.IsZero() {
		updated.UpdatedAt = s.clock.Now().UTC()
	}
	updated.UpdatedSeq = seq
	s.removeRecordIndexLocked(existing)
	s.records[updated.ID] = cloneRecord(updated)
	s.indexRecordLocked(updated)
	return updated, nil
}

func (s *Store) applyDelete(seq uint64, mutation Mutation) (Record, error) {
	id := strings.TrimSpace(mutation.Record.ID)
	existing, ok := s.records[id]
	if !ok || existing.Deleted {
		return Record{}, fmt.Errorf("%w: record %q", ErrNotFound, id)
	}
	if mutation.ExpectedRevision > 0 && mutation.ExpectedRevision != existing.Revision {
		return Record{}, fmt.Errorf("%w: expected revision %d got %d", ErrConflict, mutation.ExpectedRevision, existing.Revision)
	}
	deleted := cloneRecord(existing)
	deleted.Revision = existing.Revision + 1
	deleted.Deleted = true
	deleted.UpdatedAt = mutation.At.UTC()
	if deleted.UpdatedAt.IsZero() {
		deleted.UpdatedAt = s.clock.Now().UTC()
	}
	deleted.UpdatedSeq = seq
	s.removeRecordIndexLocked(existing)
	s.records[deleted.ID] = cloneRecord(deleted)
	s.indexRecordLocked(deleted)
	return deleted, nil
}

func normalizeMutation(mutation Mutation) (Mutation, error) {
	normalized := mutation
	normalized.Type = MutationType(strings.TrimSpace(string(normalized.Type)))
	normalized.IdempotencyKey = strings.TrimSpace(normalized.IdempotencyKey)
	normalized.ChunkContent = cloneBytes(normalized.ChunkContent)
	normalized.Record = normalizeRecord(normalized.Record)
	normalized.Chunk = normalizeChunk(normalized.Chunk)
	normalized.SourceSpan = normalizeSourceSpan(normalized.SourceSpan)
	normalized.Node = normalizeNode(normalized.Node)
	normalized.Edge = normalizeEdge(normalized.Edge)
	normalized.PurgeRequest = normalizePurgeRequest(normalized.PurgeRequest)
	switch normalized.Type {
	case MutationCreateRecord, MutationUpdateRecord:
		if err := validateRecordForWrite(normalized.Record); err != nil {
			return Mutation{}, err
		}
	case MutationDeleteRecord:
		if normalized.Record.ID == "" {
			return Mutation{}, fmt.Errorf("record id is required")
		}
	case MutationCreateChunk:
		prepared, err := prepareChunkForCreate(normalized.Chunk, normalized.ChunkContent)
		if err != nil {
			return Mutation{}, err
		}
		normalized.Chunk = prepared
	case MutationCreateSourceSpan:
		if err := validateSourceSpanForWrite(normalized.SourceSpan); err != nil {
			return Mutation{}, err
		}
	case MutationCreateNode, MutationUpdateNode:
		if err := validateNodeForWrite(normalized.Node); err != nil {
			return Mutation{}, err
		}
	case MutationDeleteNode:
		if normalized.Node.ID == "" {
			return Mutation{}, fmt.Errorf("node id is required")
		}
	case MutationCreateEdge:
		if err := validateEdgeForWrite(normalized.Edge); err != nil {
			return Mutation{}, err
		}
	case MutationDeleteEdge:
		if normalized.Edge.ID == "" {
			return Mutation{}, fmt.Errorf("edge id is required")
		}
	case MutationCreatePurgeRequest:
		if err := validatePurgeRequestForWrite(normalized.PurgeRequest); err != nil {
			return Mutation{}, err
		}
	default:
		return Mutation{}, fmt.Errorf("unsupported mutation type %q", normalized.Type)
	}
	if normalized.Metadata == nil {
		normalized.Metadata = map[string]string{}
	}
	return normalized, nil
}

func normalizeRecord(record Record) Record {
	record.ID = strings.TrimSpace(record.ID)
	record.Namespace = strings.TrimSpace(record.Namespace)
	record.ProjectID = strings.TrimSpace(record.ProjectID)
	record.Kind = strings.TrimSpace(record.Kind)
	record.Summary = strings.TrimSpace(record.Summary)
	record.HumanID = strings.TrimSpace(record.HumanID)
	record.SessionID = strings.TrimSpace(record.SessionID)
	record.MissionID = strings.TrimSpace(record.MissionID)
	record.Persona = strings.TrimSpace(record.Persona)
	record.Tags = normalizeTags(record.Tags)
	record.Attributes = normalizeStringMap(record.Attributes)
	return record
}

func validateRecordForWrite(record Record) error {
	if record.ID == "" {
		return fmt.Errorf("record id is required")
	}
	if record.Namespace == "" {
		return fmt.Errorf("record namespace is required")
	}
	if record.Kind == "" {
		return fmt.Errorf("record kind is required")
	}
	if record.Summary == "" {
		return fmt.Errorf("record summary is required")
	}
	return nil
}

func normalizeTags(tags []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(tags))
	for _, tag := range tags {
		clean := strings.ToLower(strings.TrimSpace(tag))
		if clean == "" {
			continue
		}
		if _, ok := seen[clean]; ok {
			continue
		}
		seen[clean] = struct{}{}
		out = append(out, clean)
	}
	sort.Strings(out)
	return out
}

func cloneRecord(record Record) Record {
	record.Tags = append([]string{}, record.Tags...)
	record.Attributes = cloneStringMap(record.Attributes)
	return record
}

func cloneApplyResult(result ApplyResult) ApplyResult {
	result.Record = cloneRecord(result.Record)
	result.Chunk = cloneChunk(result.Chunk)
	result.SourceSpan = cloneSourceSpan(result.SourceSpan)
	result.Node = cloneNode(result.Node)
	result.Edge = cloneEdge(result.Edge)
	result.PurgeRequest = clonePurgeRequest(result.PurgeRequest)
	return result
}

func newRecordIndexes() recordIndexes {
	return recordIndexes{
		namespace: map[string]map[string]struct{}{},
		projectID: map[string]map[string]struct{}{},
		kind:      map[string]map[string]struct{}{},
		tag:       map[string]map[string]struct{}{},
		humanID:   map[string]map[string]struct{}{},
		sessionID: map[string]map[string]struct{}{},
		missionID: map[string]map[string]struct{}{},
		persona:   map[string]map[string]struct{}{},
	}
}

func (s *Store) indexRecordLocked(record Record) {
	addIndexValue(s.indexes.namespace, record.Namespace, record.ID)
	addIndexValue(s.indexes.projectID, record.ProjectID, record.ID)
	addIndexValue(s.indexes.kind, record.Kind, record.ID)
	addIndexValue(s.indexes.humanID, record.HumanID, record.ID)
	addIndexValue(s.indexes.sessionID, record.SessionID, record.ID)
	addIndexValue(s.indexes.missionID, record.MissionID, record.ID)
	addIndexValue(s.indexes.persona, record.Persona, record.ID)
	for _, tag := range record.Tags {
		addIndexValue(s.indexes.tag, tag, record.ID)
	}
}

func (s *Store) removeRecordIndexLocked(record Record) {
	removeIndexValue(s.indexes.namespace, record.Namespace, record.ID)
	removeIndexValue(s.indexes.projectID, record.ProjectID, record.ID)
	removeIndexValue(s.indexes.kind, record.Kind, record.ID)
	removeIndexValue(s.indexes.humanID, record.HumanID, record.ID)
	removeIndexValue(s.indexes.sessionID, record.SessionID, record.ID)
	removeIndexValue(s.indexes.missionID, record.MissionID, record.ID)
	removeIndexValue(s.indexes.persona, record.Persona, record.ID)
	for _, tag := range record.Tags {
		removeIndexValue(s.indexes.tag, tag, record.ID)
	}
}

func addIndexValue(index map[string]map[string]struct{}, value, id string) {
	if value == "" || id == "" {
		return
	}
	bucket, ok := index[value]
	if !ok {
		bucket = map[string]struct{}{}
		index[value] = bucket
	}
	bucket[id] = struct{}{}
}

func removeIndexValue(index map[string]map[string]struct{}, value, id string) {
	if value == "" || id == "" {
		return
	}
	bucket, ok := index[value]
	if !ok {
		return
	}
	delete(bucket, id)
	if len(bucket) == 0 {
		delete(index, value)
	}
}

func (s *Store) candidateIDsLocked(query RecordQuery) []string {
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

	intersect(s.indexes.namespace, query.Namespace)
	intersect(s.indexes.projectID, query.ProjectID)
	intersect(s.indexes.kind, query.Kind)
	intersect(s.indexes.humanID, query.HumanID)
	intersect(s.indexes.sessionID, query.SessionID)
	intersect(s.indexes.missionID, query.MissionID)
	intersect(s.indexes.persona, query.Persona)
	for _, tag := range query.Tags {
		intersect(s.indexes.tag, tag)
	}
	if candidateSet == nil {
		candidateSet = make(map[string]struct{}, len(s.records))
		for id := range s.records {
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

func cloneIDSet(source map[string]struct{}) map[string]struct{} {
	out := map[string]struct{}{}
	for id := range source {
		out[id] = struct{}{}
	}
	return out
}

func normalizeRecordQuery(query RecordQuery) RecordQuery {
	query.Namespace = strings.TrimSpace(query.Namespace)
	query.ProjectID = strings.TrimSpace(query.ProjectID)
	query.Kind = strings.TrimSpace(query.Kind)
	query.Tags = normalizeTags(query.Tags)
	query.HumanID = strings.TrimSpace(query.HumanID)
	query.SessionID = strings.TrimSpace(query.SessionID)
	query.MissionID = strings.TrimSpace(query.MissionID)
	query.Persona = strings.TrimSpace(query.Persona)
	return query
}

func recordMatchesQuery(record Record, query RecordQuery) bool {
	if query.Namespace != "" && record.Namespace != query.Namespace {
		return false
	}
	if query.ProjectID != "" && record.ProjectID != query.ProjectID {
		return false
	}
	if query.Kind != "" && record.Kind != query.Kind {
		return false
	}
	if query.HumanID != "" && record.HumanID != query.HumanID {
		return false
	}
	if query.SessionID != "" && record.SessionID != query.SessionID {
		return false
	}
	if query.MissionID != "" && record.MissionID != query.MissionID {
		return false
	}
	if query.Persona != "" && record.Persona != query.Persona {
		return false
	}
	if len(query.Tags) == 0 {
		return true
	}
	recordTags := make(map[string]struct{}, len(record.Tags))
	for _, tag := range record.Tags {
		recordTags[tag] = struct{}{}
	}
	for _, tag := range query.Tags {
		if _, ok := recordTags[tag]; !ok {
			return false
		}
	}
	return true
}
