package kilo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const (
	catalogSnapshotType    = "kilo.catalog_snapshot"
	catalogSnapshotVersion = 1
	catalogSnapshotName    = "catalog.json"
)

type catalogSnapshotEnvelope struct {
	Type     string              `json:"type"`
	Version  int                 `json:"version"`
	Data     catalogSnapshotData `json:"data"`
	Checksum string              `json:"checksum"`
}

type catalogSnapshotChecksumPayload struct {
	Type    string              `json:"type"`
	Version int                 `json:"version"`
	Data    catalogSnapshotData `json:"data"`
}

type catalogSnapshotData struct {
	DurableSeq    uint64                  `json:"durable_seq"`
	CreatedAt     time.Time               `json:"created_at"`
	Records       map[string]Record       `json:"records,omitempty"`
	Chunks        map[string]Chunk        `json:"chunks,omitempty"`
	SourceSpans   map[string]SourceSpan   `json:"source_spans,omitempty"`
	Nodes         map[string]Node         `json:"nodes,omitempty"`
	Edges         map[string]Edge         `json:"edges,omitempty"`
	PurgeRequests map[string]PurgeRequest `json:"purge_requests,omitempty"`
	Idempotency   map[string]ApplyResult  `json:"idempotency,omitempty"`
}

func (s *Store) WriteSnapshot(ctx context.Context) (SnapshotMetadata, error) {
	if err := ctx.Err(); err != nil {
		return SnapshotMetadata{}, err
	}
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return SnapshotMetadata{}, fmt.Errorf("store is closed")
	}
	data := s.catalogSnapshotDataLocked(s.clock.Now().UTC())
	root := s.path
	syncWrites := s.syncWrites
	s.mu.RUnlock()

	metadata, err := writeCatalogSnapshotData(root, data, syncWrites)
	if err != nil {
		return SnapshotMetadata{}, err
	}

	s.mu.Lock()
	if data.DurableSeq > s.snapshotSeq {
		s.snapshotSeq = data.DurableSeq
	}
	s.mu.Unlock()

	return metadata, nil
}

func writeCatalogSnapshotData(root string, data catalogSnapshotData, syncWrites bool) (SnapshotMetadata, error) {
	envelope := catalogSnapshotEnvelope{
		Type:    catalogSnapshotType,
		Version: catalogSnapshotVersion,
		Data:    data,
	}
	checksum, err := catalogSnapshotChecksum(envelope)
	if err != nil {
		return SnapshotMetadata{}, err
	}
	envelope.Checksum = checksum
	path, err := writeCatalogSnapshot(root, envelope, syncWrites)
	if err != nil {
		return SnapshotMetadata{}, err
	}

	return SnapshotMetadata{
		Version:    envelope.Version,
		DurableSeq: data.DurableSeq,
		Checksum:   checksum,
		Path:       path,
		CreatedAt:  data.CreatedAt,
	}, nil
}

func (s *Store) catalogSnapshotDataLocked(createdAt time.Time) catalogSnapshotData {
	return catalogSnapshotData{
		DurableSeq:    s.durableSeq,
		CreatedAt:     createdAt,
		Records:       cloneRecordMap(s.records),
		Chunks:        cloneChunkMap(s.chunks),
		SourceSpans:   cloneSourceSpanMap(s.sourceSpans),
		Nodes:         cloneNodeMap(s.nodes),
		Edges:         cloneEdgeMap(s.edges),
		PurgeRequests: clonePurgeRequestMap(s.purgeRequests),
		Idempotency:   cloneApplyResultMap(s.idempotency),
	}
}

func writeCatalogSnapshot(root string, envelope catalogSnapshotEnvelope, syncWrites bool) (string, error) {
	dir := catalogSnapshotDir(root)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("create catalog snapshot directory: %w", err)
	}
	encoded, err := json.MarshalIndent(envelope, "", "  ")
	if err != nil {
		return "", fmt.Errorf("encode catalog snapshot: %w", err)
	}
	encoded = append(encoded, '\n')

	file, err := os.CreateTemp(dir, ".catalog-*.json")
	if err != nil {
		return "", fmt.Errorf("create catalog snapshot temp file: %w", err)
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
		return "", fmt.Errorf("write catalog snapshot: %w", err)
	}
	if syncWrites {
		if err := file.Sync(); err != nil {
			_ = file.Close()
			return "", fmt.Errorf("sync catalog snapshot: %w", err)
		}
	}
	if err := file.Close(); err != nil {
		return "", fmt.Errorf("close catalog snapshot: %w", err)
	}
	path := catalogSnapshotPath(root)
	if err := os.Rename(tmpPath, path); err != nil {
		return "", fmt.Errorf("install catalog snapshot: %w", err)
	}
	removeTemp = false
	if syncWrites {
		if err := syncDirectory(dir); err != nil {
			return "", err
		}
	}
	return path, nil
}

func loadCatalogSnapshot(root string) (catalogSnapshotEnvelope, bool, error) {
	path := catalogSnapshotPath(root)
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return catalogSnapshotEnvelope{}, false, nil
	}
	if err != nil {
		return catalogSnapshotEnvelope{}, false, fmt.Errorf("read catalog snapshot: %w", err)
	}
	var envelope catalogSnapshotEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return catalogSnapshotEnvelope{}, false, fmt.Errorf("decode catalog snapshot: %w", err)
	}
	if envelope.Type != catalogSnapshotType {
		return catalogSnapshotEnvelope{}, false, fmt.Errorf("unsupported catalog snapshot type %q", envelope.Type)
	}
	if envelope.Version != catalogSnapshotVersion {
		return catalogSnapshotEnvelope{}, false, fmt.Errorf("unsupported catalog snapshot version %d", envelope.Version)
	}
	checksum, err := catalogSnapshotChecksum(envelope)
	if err != nil {
		return catalogSnapshotEnvelope{}, false, err
	}
	if envelope.Checksum != checksum {
		return catalogSnapshotEnvelope{}, false, fmt.Errorf("%w: catalog snapshot checksum mismatch: expected %s got %s", ErrCorrupt, envelope.Checksum, checksum)
	}
	return envelope, true, nil
}

func catalogSnapshotChecksum(envelope catalogSnapshotEnvelope) (string, error) {
	payload := catalogSnapshotChecksumPayload{
		Type:    envelope.Type,
		Version: envelope.Version,
		Data:    envelope.Data,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("checksum catalog snapshot: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func catalogSnapshotDir(root string) string {
	return filepath.Join(root, "snapshots")
}

func catalogSnapshotPath(root string) string {
	return filepath.Join(catalogSnapshotDir(root), catalogSnapshotName)
}

func (s *Store) installCatalogSnapshot(data catalogSnapshotData) {
	s.durableSeq = data.DurableSeq
	s.snapshotSeq = data.DurableSeq
	s.records = cloneRecordMap(data.Records)
	s.chunks = cloneChunkMap(data.Chunks)
	s.sourceSpans = cloneSourceSpanMap(data.SourceSpans)
	s.nodes = cloneNodeMap(data.Nodes)
	s.edges = cloneEdgeMap(data.Edges)
	s.purgeRequests = clonePurgeRequestMap(data.PurgeRequests)
	s.idempotency = cloneApplyResultMap(data.Idempotency)
	s.indexes = newRecordIndexes()
	s.chunkIndexes = newChunkIndexes()
	s.spanIndexes = newSourceSpanIndexes()
	s.graph = newGraphIndexes()
	for _, record := range s.records {
		s.indexRecordLocked(record)
	}
	for _, chunk := range s.chunks {
		s.indexChunkLocked(chunk)
	}
	for _, span := range s.sourceSpans {
		s.indexSourceSpanLocked(span)
	}
	for _, edge := range s.edges {
		s.indexEdgeLocked(edge)
	}
}

func cloneRecordMap(values map[string]Record) map[string]Record {
	out := make(map[string]Record, len(values))
	for key, value := range values {
		out[key] = cloneRecord(value)
	}
	return out
}

func cloneChunkMap(values map[string]Chunk) map[string]Chunk {
	out := make(map[string]Chunk, len(values))
	for key, value := range values {
		out[key] = cloneChunk(value)
	}
	return out
}

func cloneSourceSpanMap(values map[string]SourceSpan) map[string]SourceSpan {
	out := make(map[string]SourceSpan, len(values))
	for key, value := range values {
		out[key] = cloneSourceSpan(value)
	}
	return out
}

func cloneNodeMap(values map[string]Node) map[string]Node {
	out := make(map[string]Node, len(values))
	for key, value := range values {
		out[key] = cloneNode(value)
	}
	return out
}

func cloneEdgeMap(values map[string]Edge) map[string]Edge {
	out := make(map[string]Edge, len(values))
	for key, value := range values {
		out[key] = cloneEdge(value)
	}
	return out
}

func clonePurgeRequestMap(values map[string]PurgeRequest) map[string]PurgeRequest {
	out := make(map[string]PurgeRequest, len(values))
	for key, value := range values {
		out[key] = clonePurgeRequest(value)
	}
	return out
}

func cloneApplyResultMap(values map[string]ApplyResult) map[string]ApplyResult {
	out := make(map[string]ApplyResult, len(values))
	for key, value := range values {
		out[key] = cloneApplyResult(value)
	}
	return out
}
