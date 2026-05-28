package kilo

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestSDKFacadeWritesReadsQueriesAndReportsStatus(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, t.TempDir(), fixedClock(time.Now()))
	defer store.Close()

	written, err := store.Write(ctx, Write{
		IdempotencyKey: "sdk-create-record",
		CreateRecord: &Record{
			ID:        "record:sdk-pref",
			Namespace: "project:kilo",
			Kind:      "preference",
			Summary:   "Kilo should be embedded as a Go SDK.",
			Tags:      []string{"sdk", "embedding"},
			HumanID:   "human:lynn",
		},
	})
	if err != nil {
		t.Fatalf("sdk write record: %v", err)
	}
	if written.Seq != 1 || written.Record.Revision != 1 {
		t.Fatalf("unexpected sdk write result: %+v", written)
	}

	read, err := store.Read(ctx, Read{RecordID: "record:sdk-pref"})
	if err != nil {
		t.Fatalf("sdk read record: %v", err)
	}
	if !read.Found || read.Record.Summary != "Kilo should be embedded as a Go SDK." {
		t.Fatalf("unexpected sdk read: %+v", read)
	}

	query, err := store.Query(ctx, Query{Records: &RecordQuery{
		Namespace: "project:kilo",
		Kind:      "preference",
		HumanID:   "human:lynn",
		Tags:      []string{"sdk"},
	}})
	if err != nil {
		t.Fatalf("sdk query records: %v", err)
	}
	if ids := recordIDs(query.Records.Records); strings.Join(ids, ",") != "record:sdk-pref" {
		t.Fatalf("unexpected sdk query ids: %v", ids)
	}
	if status := store.Status(); status.DurableSeq != 1 || status.Records != 1 {
		t.Fatalf("unexpected sdk status: %+v", status)
	}
}

func TestSDKFacadeRejectsAmbiguousOperationsAndPropagatesContext(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, t.TempDir(), fixedClock(time.Now()))
	defer store.Close()

	_, err := store.Write(ctx, Write{
		CreateRecord: &Record{ID: "record:a", Namespace: "project:kilo", Kind: "note", Summary: "A."},
		CreateNode:   &Node{ID: "node:a", Type: "note"},
	})
	if err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("expected ambiguous write error, got %v", err)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = store.Read(cancelled, Read{RecordID: "record:a"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}

func TestSDKSearchCanRequireFreshProjection(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, t.TempDir(), fixedClock(time.Now()))
	defer store.Close()

	index := NewExactVectorIndex()
	if err := index.ApplyVectorOperation(ctx, VectorOperation{
		Type:           VectorOperationUpsert,
		NodeID:         "node:sdk-pref",
		NodeType:       "memory_record",
		EmbeddingModel: "embedding-test-v1",
		Vector:         []float32{1, 0},
		SourceSeq:      1,
	}); err != nil {
		t.Fatalf("apply vector operation: %v", err)
	}

	_, err := store.Search(ctx, Search{
		VectorSearcher: index,
		Vectors: &VectorSearchQuery{
			Vector:        []float32{1, 0},
			MinIndexedSeq: 2,
		},
		RequireFresh: true,
	})
	if !errors.Is(err, ErrStaleProjection) {
		t.Fatalf("expected stale projection error, got %v", err)
	}

	result, err := store.Search(ctx, Search{
		VectorSearcher: index,
		Vectors: &VectorSearchQuery{
			Vector:        []float32{1, 0},
			MinIndexedSeq: 1,
		},
		RequireFresh: true,
	})
	if err != nil {
		t.Fatalf("fresh sdk search: %v", err)
	}
	if ids := vectorHitIDs(result.Vectors.Hits); strings.Join(ids, ",") != "node:sdk-pref" {
		t.Fatalf("unexpected sdk search ids: %v", ids)
	}
}

func TestSDKFacadeDispatchesChunkSpanGraphAndAuditOperations(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, t.TempDir(), fixedClock(time.Now()))
	defer store.Close()

	recordWrite, err := store.Write(ctx, Write{
		CreateRecord: &Record{
			ID:        "record:dispatch",
			Namespace: "project:kilo",
			Kind:      "decision",
			Summary:   "Dispatch through the public SDK facade.",
		},
	})
	if err != nil {
		t.Fatalf("sdk create record: %v", err)
	}
	if _, err := store.Write(ctx, Write{
		CreateChunk: &ChunkWrite{
			Chunk: Chunk{
				ID:        "chunk:dispatch:000001",
				Namespace: "project:kilo",
				Kind:      "transcript",
				Sequence:  1,
			},
			Content: []byte("Kilo exposes chunk bodies explicitly."),
		},
	}); err != nil {
		t.Fatalf("sdk create chunk: %v", err)
	}
	if _, err := store.Write(ctx, Write{
		CreateSourceSpan: &SourceSpan{
			ID:        "span:dispatch",
			Namespace: "project:kilo",
			RecordID:  "record:dispatch",
			ChunkRefs: []SourceSpanChunkRef{{ChunkID: "chunk:dispatch:000001"}},
		},
	}); err != nil {
		t.Fatalf("sdk create source span: %v", err)
	}

	chunkRead, err := store.Read(ctx, Read{ChunkContentID: "chunk:dispatch:000001"})
	if err != nil {
		t.Fatalf("sdk read chunk content: %v", err)
	}
	if got := string(chunkRead.ChunkContent); got != "Kilo exposes chunk bodies explicitly." {
		t.Fatalf("unexpected chunk body: %q", got)
	}
	spanRead, err := store.Read(ctx, Read{SourceSpanID: "span:dispatch"})
	if err != nil {
		t.Fatalf("sdk read source span: %v", err)
	}
	if !spanRead.Found || spanRead.SourceSpan.Span.RecordID != "record:dispatch" {
		t.Fatalf("unexpected source span read: %+v", spanRead)
	}
	spans, err := store.Query(ctx, Query{SourceSpans: &SourceSpanQuery{RecordID: "record:dispatch"}})
	if err != nil {
		t.Fatalf("sdk query source spans: %v", err)
	}
	if len(spans.SourceSpans.Spans) != 1 || spans.SourceSpans.Spans[0].ID != "span:dispatch" {
		t.Fatalf("unexpected source span query: %+v", spans.SourceSpans)
	}

	updated, err := store.Write(ctx, Write{
		UpdateRecord: &Record{
			ID:        "record:dispatch",
			Namespace: "project:kilo",
			Kind:      "decision",
			Summary:   "Dispatch through the public SDK facade after update.",
		},
		ExpectedRevision: recordWrite.Record.Revision,
	})
	if err != nil {
		t.Fatalf("sdk update record: %v", err)
	}
	if _, err := store.Write(ctx, Write{
		DeleteRecord:     "record:dispatch",
		ExpectedRevision: updated.Record.Revision,
	}); err != nil {
		t.Fatalf("sdk delete record: %v", err)
	}
	hiddenRecord, err := store.Read(ctx, Read{RecordID: "record:dispatch"})
	if err != nil {
		t.Fatalf("sdk read tombstoned record: %v", err)
	}
	if hiddenRecord.Found {
		t.Fatalf("tombstoned record should be hidden by default: %+v", hiddenRecord)
	}
	auditRecord, err := store.Read(ctx, Read{RecordID: "record:dispatch", IncludeDeleted: true})
	if err != nil {
		t.Fatalf("sdk audit read tombstoned record: %v", err)
	}
	if !auditRecord.Found || !auditRecord.Record.Deleted {
		t.Fatalf("expected tombstoned audit record: %+v", auditRecord)
	}

	human, err := store.Write(ctx, Write{CreateNode: &Node{ID: "node:human:lynn", Type: "human", Name: "Lynn"}})
	if err != nil {
		t.Fatalf("sdk create human node: %v", err)
	}
	if _, err := store.Write(ctx, Write{
		UpdateNode: &Node{
			ID:      "node:human:lynn",
			Type:    "human",
			Name:    "Lynn",
			Summary: "Primary project collaborator.",
		},
		ExpectedRevision: human.Node.Revision,
	}); err != nil {
		t.Fatalf("sdk update human node: %v", err)
	}
	memory, err := store.Write(ctx, Write{CreateNode: &Node{
		ID:      "node:memory:dispatch",
		Type:    "memory_record",
		Summary: "SDK facade dispatch should remain covered.",
	}})
	if err != nil {
		t.Fatalf("sdk create memory node: %v", err)
	}
	edge, err := store.Write(ctx, Write{CreateEdge: &Edge{
		ID:         "edge:lynn:dispatch-memory",
		Type:       "mentions",
		FromNodeID: "node:human:lynn",
		ToNodeID:   "node:memory:dispatch",
	}})
	if err != nil {
		t.Fatalf("sdk create edge: %v", err)
	}

	children, err := store.Query(ctx, Query{Children: &GraphQuery{
		NodeID: "node:human:lynn",
		Edge:   EdgeQuery{Type: "mentions"},
	}})
	if err != nil {
		t.Fatalf("sdk query children: %v", err)
	}
	if ids := nodeIDs(children.Graph.Nodes); strings.Join(ids, ",") != "node:memory:dispatch" {
		t.Fatalf("unexpected child nodes: %v", ids)
	}
	parents, err := store.Query(ctx, Query{Parents: &GraphQuery{
		NodeID: "node:memory:dispatch",
		Edge:   EdgeQuery{Type: "mentions"},
	}})
	if err != nil {
		t.Fatalf("sdk query parents: %v", err)
	}
	if ids := nodeIDs(parents.Graph.Nodes); strings.Join(ids, ",") != "node:human:lynn" {
		t.Fatalf("unexpected parent nodes: %v", ids)
	}
	if _, err := store.Write(ctx, Write{
		DeleteEdge:       "edge:lynn:dispatch-memory",
		ExpectedRevision: edge.Edge.Revision,
	}); err != nil {
		t.Fatalf("sdk delete edge: %v", err)
	}
	if _, err := store.Write(ctx, Write{
		DeleteNode:       "node:memory:dispatch",
		ExpectedRevision: memory.Node.Revision,
	}); err != nil {
		t.Fatalf("sdk delete node: %v", err)
	}
	auditNode, err := store.Read(ctx, Read{NodeID: "node:memory:dispatch", IncludeDeleted: true})
	if err != nil {
		t.Fatalf("sdk audit read tombstoned node: %v", err)
	}
	if !auditNode.Found || !auditNode.Node.Deleted {
		t.Fatalf("expected tombstoned audit node: %+v", auditNode)
	}

	if _, err := store.Write(ctx, Write{CreatePurgeRequest: &PurgeRequest{
		ID:         "purge:dispatch",
		Action:     PurgeActionRedact,
		TargetType: "record",
		TargetID:   "record:dispatch",
		Reason:     "example redaction request",
	}}); err != nil {
		t.Fatalf("sdk create purge request: %v", err)
	}
	purge, err := store.Read(ctx, Read{PurgeRequestID: "purge:dispatch"})
	if err != nil {
		t.Fatalf("sdk read purge request: %v", err)
	}
	if !purge.Found || purge.PurgeRequest.Action != PurgeActionRedact {
		t.Fatalf("unexpected purge request read: %+v", purge)
	}
}
