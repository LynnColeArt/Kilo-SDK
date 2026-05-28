package kilo_test

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/LynnColeArt/Kilo-SDK/pkg/kilo"
)

func ExampleStore_Write_memoryMetadata() {
	ctx := context.Background()
	store, cleanup := openExampleStore(ctx)
	defer cleanup()

	_, err := store.Write(ctx, kilo.Write{
		IdempotencyKey: "example-memory-metadata",
		CreateRecord: &kilo.Record{
			ID:        "record:preference:go-sdk",
			Namespace: "project:kilo",
			Kind:      "preference",
			Summary:   "Kilo should remain embeddable as a Go SDK.",
			Tags:      []string{"SDK", "Embedding"},
			HumanID:   "human:lynn",
		},
	})
	if err != nil {
		panic(err)
	}

	read, err := store.Read(ctx, kilo.Read{RecordID: "record:preference:go-sdk"})
	if err != nil {
		panic(err)
	}

	fmt.Println(read.Record.Kind)
	fmt.Println(strings.Join(read.Record.Tags, ","))
	fmt.Println(store.Status().Records)

	// Output:
	// preference
	// embedding,sdk
	// 1
}

func ExampleStore_Write_transcriptChunk() {
	ctx := context.Background()
	store, cleanup := openExampleStore(ctx)
	defer cleanup()

	_, err := store.Write(ctx, kilo.Write{
		CreateChunk: &kilo.ChunkWrite{
			Chunk: kilo.Chunk{
				ID:        "chunk:session-alpha:000001",
				Namespace: "project:kilo",
				SessionID: "session:alpha",
				Kind:      "transcript",
				Sequence:  1,
			},
			Content: []byte("Planner: keep the storage engine embedded."),
		},
	})
	if err != nil {
		panic(err)
	}

	read, err := store.Read(ctx, kilo.Read{ChunkContentID: "chunk:session-alpha:000001"})
	if err != nil {
		panic(err)
	}

	fmt.Println(string(read.ChunkContent))
	fmt.Println(read.Chunk.ContentLength)

	// Output:
	// Planner: keep the storage engine embedded.
	// 42
}

func ExampleStore_Search_hierarchicalMemory() {
	ctx := context.Background()
	store, cleanup := openExampleStore(ctx)
	defer cleanup()

	mustWrite(ctx, store, kilo.Write{CreateNode: &kilo.Node{
		ID:   "node:human:lynn",
		Type: "human",
		Name: "Lynn",
	}})
	mustWrite(ctx, store, kilo.Write{CreateNode: &kilo.Node{
		ID:   "node:session:alpha",
		Type: "session",
		Name: "Engineering preferences with Lynn",
	}})
	mustWrite(ctx, store, kilo.Write{CreateNode: &kilo.Node{
		ID:      "node:memory:engineering-preference",
		Type:    "memory_record",
		Summary: "Prefer Go for Kilo's embedded SDK boundary.",
	}})
	mustWrite(ctx, store, kilo.Write{CreateEdge: &kilo.Edge{
		ID:         "edge:lynn:session-alpha",
		Type:       "participant",
		FromNodeID: "node:human:lynn",
		ToNodeID:   "node:session:alpha",
	}})
	mustWrite(ctx, store, kilo.Write{CreateEdge: &kilo.Edge{
		ID:         "edge:session-alpha:memory-preference",
		Type:       "contains",
		FromNodeID: "node:session:alpha",
		ToNodeID:   "node:memory:engineering-preference",
	}})

	index := kilo.NewExactVectorIndex()
	err := index.ApplyVectorOperation(ctx, kilo.VectorOperation{
		Type:           kilo.VectorOperationUpsert,
		NodeID:         "node:memory:engineering-preference",
		NodeType:       "memory_record",
		HierarchyScope: map[string]string{"human_id": "human:lynn", "topic": "engineering_preferences"},
		EmbeddingModel: "example-embedding-v1",
		Vector:         []float32{1, 0},
		SourceSeq:      store.Status().DurableSeq,
	})
	if err != nil {
		panic(err)
	}

	result, err := store.Search(ctx, kilo.Search{
		VectorSearcher: index,
		Vectors: &kilo.VectorSearchQuery{
			Vector:            []float32{1, 0},
			NodeTypes:         []string{"memory_record"},
			HierarchyScope:    map[string]string{"human_id": "human:lynn", "topic": "engineering_preferences"},
			MinIndexedSeq:     store.Status().DurableSeq,
			ExpandParents:     true,
			ParentEdgeType:    "contains",
			EmbeddingModel:    "example-embedding-v1",
			IncludeCandidates: true,
		},
		RequireFresh: true,
	})
	if err != nil {
		panic(err)
	}

	fmt.Println(result.Vectors.Hits[0].NodeID)
	fmt.Println(result.Vectors.Expanded.Nodes[0].Name)
	fmt.Println(strings.Join(result.Vectors.CandidateIDs, ","))

	// Output:
	// node:memory:engineering-preference
	// Engineering preferences with Lynn
	// node:memory:engineering-preference
}

func openExampleStore(ctx context.Context) (*kilo.Store, func()) {
	dir, err := os.MkdirTemp("", "kilo-example-*")
	if err != nil {
		panic(err)
	}
	store, err := kilo.Open(ctx, kilo.Options{Path: dir})
	if err != nil {
		_ = os.RemoveAll(dir)
		panic(err)
	}
	return store, func() {
		_ = store.Close()
		_ = os.RemoveAll(dir)
	}
}

func mustWrite(ctx context.Context, store *kilo.Store, write kilo.Write) {
	if _, err := store.Write(ctx, write); err != nil {
		panic(err)
	}
}
