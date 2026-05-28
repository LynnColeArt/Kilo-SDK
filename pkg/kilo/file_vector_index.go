package kilo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

var fileVectorMagic = []byte("KILOVEC1\n")

type FileVectorIndexOptions struct {
	Path       string
	SyncWrites bool
}

type FileVectorIndex struct {
	mu         sync.RWMutex
	path       string
	syncWrites bool
	entries    map[string]exactVectorEntry
	indexedSeq uint64
}

type fileVectorMetadata struct {
	NodeID         string            `json:"node_id"`
	NodeType       string            `json:"node_type"`
	HierarchyScope map[string]string `json:"hierarchy_scope,omitempty"`
	EmbeddingModel string            `json:"embedding_model"`
	Dimensions     int               `json:"dimensions"`
	SourceSeq      uint64            `json:"source_seq"`
	Attributes     map[string]string `json:"attributes,omitempty"`
}

func NewFileVectorIndex(opts FileVectorIndexOptions) (*FileVectorIndex, error) {
	path := strings.TrimSpace(opts.Path)
	if path == "" {
		return nil, fmt.Errorf("file vector index path is required")
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, fmt.Errorf("create vector index path: %w", err)
	}
	index := &FileVectorIndex{
		path:       path,
		syncWrites: opts.SyncWrites,
		entries:    map[string]exactVectorEntry{},
	}
	if err := index.load(); err != nil {
		return nil, err
	}
	return index, nil
}

func (i *FileVectorIndex) ApplyVectorOperation(ctx context.Context, op VectorOperation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	normalized, err := normalizeVectorOperation(op, op.SourceSeq)
	if err != nil {
		return err
	}

	i.mu.Lock()
	defer i.mu.Unlock()
	switch normalized.Type {
	case VectorOperationUpsert:
		entry := exactVectorEntry{
			nodeID:         normalized.NodeID,
			nodeType:       normalized.NodeType,
			hierarchyScope: cloneStringMap(normalized.HierarchyScope),
			embeddingModel: normalized.EmbeddingModel,
			dimensions:     normalized.Dimensions,
			vector:         cloneFloat32Slice(normalized.Vector),
			magnitude:      vectorMagnitude(normalized.Vector),
			sourceSeq:      normalized.SourceSeq,
			attributes:     cloneStringMap(normalized.Attributes),
		}
		if err := i.writeEntry(entry); err != nil {
			return err
		}
		i.entries[entry.nodeID] = entry
	case VectorOperationDelete:
		if err := i.deleteEntry(normalized.NodeID); err != nil {
			return err
		}
		delete(i.entries, normalized.NodeID)
	default:
		return fmt.Errorf("unsupported vector operation type %q", normalized.Type)
	}
	if normalized.SourceSeq > i.indexedSeq {
		i.indexedSeq = normalized.SourceSeq
	}
	return nil
}

func (i *FileVectorIndex) SearchVectors(ctx context.Context, query VectorSearchQuery) (VectorSearchResult, error) {
	if err := ctx.Err(); err != nil {
		return VectorSearchResult{}, err
	}
	normalized, err := normalizeVectorSearchQuery(query)
	if err != nil {
		return VectorSearchResult{}, err
	}
	queryMagnitude := vectorMagnitude(normalized.Vector)
	if queryMagnitude == 0 {
		return VectorSearchResult{}, fmt.Errorf("vector search query vector must be non-zero")
	}

	i.mu.RLock()
	indexedSeq := i.indexedSeq
	candidates := make([]exactVectorEntry, 0, len(i.entries))
	for _, entry := range i.entries {
		if exactVectorEntryMatchesQuery(entry, normalized) {
			candidates = append(candidates, cloneExactVectorEntry(entry))
		}
	}
	i.mu.RUnlock()

	result := VectorSearchResult{
		IndexedSeq:    indexedSeq,
		MinIndexedSeq: normalized.MinIndexedSeq,
		MaxLag:        normalized.MaxLag,
		EnforceMaxLag: normalized.EnforceMaxLag,
		Stale:         normalized.MinIndexedSeq > indexedSeq,
	}
	if normalized.IncludeCandidates {
		result.CandidateIDs = exactVectorCandidateIDs(candidates)
	}
	result.Hits = make([]VectorSearchHit, 0, len(candidates))
	for _, entry := range candidates {
		result.Hits = append(result.Hits, VectorSearchHit{
			NodeID:         entry.nodeID,
			NodeType:       entry.nodeType,
			Score:          cosineSimilarity(normalized.Vector, queryMagnitude, entry.vector, entry.magnitude),
			HierarchyScope: cloneStringMap(entry.hierarchyScope),
			EmbeddingModel: entry.embeddingModel,
			Dimensions:     entry.dimensions,
			SourceSeq:      entry.sourceSeq,
			Attributes:     cloneStringMap(entry.attributes),
		})
	}
	sortVectorSearchHits(result.Hits)
	if normalized.Limit > 0 && len(result.Hits) > normalized.Limit {
		result.Hits = result.Hits[:normalized.Limit]
	}
	return result, nil
}

func (i *FileVectorIndex) load() error {
	paths, err := filepath.Glob(filepath.Join(i.path, "*.kvec"))
	if err != nil {
		return fmt.Errorf("list vector files: %w", err)
	}
	sort.Strings(paths)
	for _, path := range paths {
		entry, err := readFileVectorEntry(path)
		if err != nil {
			return err
		}
		i.entries[entry.nodeID] = entry
		if entry.sourceSeq > i.indexedSeq {
			i.indexedSeq = entry.sourceSeq
		}
	}
	return nil
}

func (i *FileVectorIndex) writeEntry(entry exactVectorEntry) error {
	payload, err := encodeFileVectorEntry(entry)
	if err != nil {
		return err
	}
	path := i.vectorPath(entry.nodeID)
	tmp, err := os.CreateTemp(i.path, ".vector-*")
	if err != nil {
		return fmt.Errorf("create vector temp file: %w", err)
	}
	tmpPath := tmp.Name()
	removeTemp := true
	defer func() {
		if removeTemp {
			_ = os.Remove(tmpPath)
		}
	}()
	if err := writeAll(tmp, payload); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write vector file: %w", err)
	}
	if i.syncWrites {
		if err := tmp.Sync(); err != nil {
			_ = tmp.Close()
			return fmt.Errorf("sync vector file: %w", err)
		}
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close vector file: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("install vector file: %w", err)
	}
	removeTemp = false
	if i.syncWrites {
		if err := syncDirectory(i.path); err != nil {
			return err
		}
	}
	return nil
}

func (i *FileVectorIndex) deleteEntry(nodeID string) error {
	if err := os.Remove(i.vectorPath(nodeID)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete vector file: %w", err)
	}
	if i.syncWrites {
		if err := syncDirectory(i.path); err != nil {
			return err
		}
	}
	return nil
}

func (i *FileVectorIndex) vectorPath(nodeID string) string {
	sum := sha256.Sum256([]byte(nodeID))
	return filepath.Join(i.path, hex.EncodeToString(sum[:])+".kvec")
}

func encodeFileVectorEntry(entry exactVectorEntry) ([]byte, error) {
	metadata := fileVectorMetadata{
		NodeID:         entry.nodeID,
		NodeType:       entry.nodeType,
		HierarchyScope: cloneStringMap(entry.hierarchyScope),
		EmbeddingModel: entry.embeddingModel,
		Dimensions:     entry.dimensions,
		SourceSeq:      entry.sourceSeq,
		Attributes:     cloneStringMap(entry.attributes),
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return nil, fmt.Errorf("encode vector metadata: %w", err)
	}
	var out bytes.Buffer
	out.Write(fileVectorMagic)
	if uint64(len(encoded)) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("vector metadata is too large")
	}
	if err := binary.Write(&out, binary.LittleEndian, uint32(len(encoded))); err != nil {
		return nil, fmt.Errorf("write vector metadata length: %w", err)
	}
	out.Write(encoded)
	for _, value := range entry.vector {
		if err := binary.Write(&out, binary.LittleEndian, value); err != nil {
			return nil, fmt.Errorf("write vector value: %w", err)
		}
	}
	return out.Bytes(), nil
}

func readFileVectorEntry(path string) (exactVectorEntry, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return exactVectorEntry{}, fmt.Errorf("read vector file %q: %w", path, err)
	}
	if !bytes.HasPrefix(raw, fileVectorMagic) {
		return exactVectorEntry{}, fmt.Errorf("%w: vector file %q has invalid magic", ErrCorrupt, path)
	}
	reader := bytes.NewReader(raw[len(fileVectorMagic):])
	var metadataLen uint32
	if err := binary.Read(reader, binary.LittleEndian, &metadataLen); err != nil {
		return exactVectorEntry{}, fmt.Errorf("%w: read vector metadata length: %w", ErrCorrupt, err)
	}
	if metadataLen == 0 || int(metadataLen) > reader.Len() {
		return exactVectorEntry{}, fmt.Errorf("%w: vector metadata length %d is invalid", ErrCorrupt, metadataLen)
	}
	metadataBytes := make([]byte, metadataLen)
	if _, err := io.ReadFull(reader, metadataBytes); err != nil {
		return exactVectorEntry{}, fmt.Errorf("%w: read vector metadata: %w", ErrCorrupt, err)
	}
	var metadata fileVectorMetadata
	if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
		return exactVectorEntry{}, fmt.Errorf("%w: decode vector metadata: %w", ErrCorrupt, err)
	}
	if metadata.Dimensions <= 0 {
		return exactVectorEntry{}, fmt.Errorf("%w: vector dimensions must be positive", ErrCorrupt)
	}
	if metadata.Dimensions > reader.Len()/4 {
		return exactVectorEntry{}, fmt.Errorf("%w: vector dimensions exceed payload length", ErrCorrupt)
	}
	expectedBytes := metadata.Dimensions * 4
	if reader.Len() != expectedBytes {
		return exactVectorEntry{}, fmt.Errorf("%w: vector file has %d vector bytes, expected %d", ErrCorrupt, reader.Len(), expectedBytes)
	}
	vector := make([]float32, metadata.Dimensions)
	for index := range vector {
		if err := binary.Read(reader, binary.LittleEndian, &vector[index]); err != nil {
			return exactVectorEntry{}, fmt.Errorf("%w: read vector value: %w", ErrCorrupt, err)
		}
	}
	op, err := normalizeVectorOperation(VectorOperation{
		Type:           VectorOperationUpsert,
		NodeID:         metadata.NodeID,
		NodeType:       metadata.NodeType,
		HierarchyScope: metadata.HierarchyScope,
		EmbeddingModel: metadata.EmbeddingModel,
		Dimensions:     metadata.Dimensions,
		Vector:         vector,
		SourceSeq:      metadata.SourceSeq,
		Attributes:     metadata.Attributes,
	}, metadata.SourceSeq)
	if err != nil {
		return exactVectorEntry{}, fmt.Errorf("%w: normalize vector file %q: %w", ErrCorrupt, path, err)
	}
	return exactVectorEntry{
		nodeID:         op.NodeID,
		nodeType:       op.NodeType,
		hierarchyScope: cloneStringMap(op.HierarchyScope),
		embeddingModel: op.EmbeddingModel,
		dimensions:     op.Dimensions,
		vector:         cloneFloat32Slice(op.Vector),
		magnitude:      vectorMagnitude(op.Vector),
		sourceSeq:      op.SourceSeq,
		attributes:     cloneStringMap(op.Attributes),
	}, nil
}
