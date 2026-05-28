package kilo

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileVectorIndexStoresBinaryVectorsAndReopens(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	index, err := NewFileVectorIndex(FileVectorIndexOptions{Path: path, SyncWrites: true})
	if err != nil {
		t.Fatalf("open file vector index: %v", err)
	}
	if err := index.ApplyVectorOperation(ctx, VectorOperation{
		Type:           VectorOperationUpsert,
		NodeID:         "node:memory:pref",
		NodeType:       "memory_record",
		HierarchyScope: map[string]string{"human_id": "human:lynn", "session_id": "session:alpha"},
		EmbeddingModel: "embedding-test-v1",
		Dimensions:     3,
		Vector:         []float32{0.25, 0.5, 0.75},
		SourceSeq:      7,
		Attributes:     map[string]string{"record_id": "record:pref"},
	}); err != nil {
		t.Fatalf("apply vector operation: %v", err)
	}

	files, err := filepath.Glob(filepath.Join(path, "*.kvec"))
	if err != nil {
		t.Fatalf("glob vector files: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected one vector file, got %v", files)
	}
	raw, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatalf("read vector file: %v", err)
	}
	if !bytes.HasPrefix(raw, fileVectorMagic) {
		t.Fatalf("vector file missing magic")
	}
	if bytes.Contains(raw, []byte("[0.25")) || bytes.Contains(raw, []byte("0.75]")) {
		t.Fatalf("vector payload appears to be stored as JSON: %q", string(raw))
	}

	reopened, err := NewFileVectorIndex(FileVectorIndexOptions{Path: path, SyncWrites: true})
	if err != nil {
		t.Fatalf("reopen file vector index: %v", err)
	}
	result, err := reopened.SearchVectors(ctx, VectorSearchQuery{
		Vector:            []float32{0.25, 0.5, 0.75},
		NodeTypes:         []string{"memory_record"},
		HierarchyScope:    map[string]string{"human_id": "human:lynn"},
		EmbeddingModel:    "embedding-test-v1",
		IncludeCandidates: true,
		MinIndexedSeq:     7,
	})
	if err != nil {
		t.Fatalf("search reopened vector index: %v", err)
	}
	if ids := vectorHitIDs(result.Hits); strings.Join(ids, ",") != "node:memory:pref" {
		t.Fatalf("unexpected vector hits: %v", ids)
	}
	if result.IndexedSeq != 7 || result.Stale {
		t.Fatalf("unexpected vector watermark: %+v", result)
	}
}

func TestFileVectorIndexDeletesPersistedVectors(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	index, err := NewFileVectorIndex(FileVectorIndexOptions{Path: path})
	if err != nil {
		t.Fatalf("open file vector index: %v", err)
	}
	if err := index.ApplyVectorOperation(ctx, VectorOperation{
		Type:           VectorOperationUpsert,
		NodeID:         "node:memory:delete",
		NodeType:       "memory_record",
		EmbeddingModel: "embedding-test-v1",
		Vector:         []float32{1, 0},
		SourceSeq:      1,
	}); err != nil {
		t.Fatalf("upsert vector: %v", err)
	}
	if err := index.ApplyVectorOperation(ctx, VectorOperation{
		Type:      VectorOperationDelete,
		NodeID:    "node:memory:delete",
		SourceSeq: 2,
	}); err != nil {
		t.Fatalf("delete vector: %v", err)
	}
	reopened, err := NewFileVectorIndex(FileVectorIndexOptions{Path: path})
	if err != nil {
		t.Fatalf("reopen vector index: %v", err)
	}
	result, err := reopened.SearchVectors(ctx, VectorSearchQuery{
		Vector:         []float32{1, 0},
		EmbeddingModel: "embedding-test-v1",
	})
	if err != nil {
		t.Fatalf("search deleted vector: %v", err)
	}
	if len(result.Hits) != 0 {
		t.Fatalf("expected deleted vector to stay deleted, got %+v", result.Hits)
	}
}
