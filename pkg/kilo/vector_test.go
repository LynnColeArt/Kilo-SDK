package kilo

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestVectorProjectorEmitsInsertUpdateAndDeleteOperations(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, t.TempDir(), fixedClock(time.Now()))
	defer store.Close()

	index := newRecordingVectorIndex(4)
	projector, err := NewVectorProjector(VectorProjectorOptions{
		Name:  "vector-test",
		Index: index,
		Mapper: VectorOperationMapperFunc(func(ctx context.Context, event ProjectionEvent) (VectorOperation, bool, error) {
			switch event.Mutation.Type {
			case MutationCreateNode, MutationUpdateNode:
				node := event.Mutation.Node
				return VectorOperation{
					Type:           VectorOperationUpsert,
					NodeID:         node.ID,
					NodeType:       node.Type,
					HierarchyScope: vectorHierarchyScope(node),
					EmbeddingModel: "embedding-test-v1",
					Vector:         []float32{0.1, 0.2, 0.3},
					Attributes: map[string]string{
						"summary": node.Summary,
					},
				}, true, nil
			case MutationDeleteNode:
				return VectorOperation{
					Type:   VectorOperationDelete,
					NodeID: event.Mutation.Node.ID,
				}, true, nil
			default:
				return VectorOperation{}, false, nil
			}
		}),
	})
	if err != nil {
		t.Fatalf("new vector projector: %v", err)
	}
	if err := store.RegisterProjector(ctx, projector); err != nil {
		t.Fatalf("register vector projector: %v", err)
	}

	created, err := store.Apply(ctx, Mutation{
		Type: MutationCreateNode,
		Node: Node{
			ID:      "node:memory:pref",
			Type:    "memory_record",
			Summary: "SDK-first packaging preference.",
			Attributes: map[string]string{
				"project_id": "project:kilo",
				"session_id": "session:alpha",
			},
		},
	})
	if err != nil {
		t.Fatalf("create vector node: %v", err)
	}
	insert := receiveVectorOperation(t, index.ops)
	assertVectorUpsert(t, insert, 1, "SDK-first packaging preference.")
	if insert.HierarchyScope["project_id"] != "project:kilo" || insert.HierarchyScope["session_id"] != "session:alpha" {
		t.Fatalf("unexpected hierarchy scope: %+v", insert.HierarchyScope)
	}

	if _, err := store.Apply(ctx, Mutation{
		Type:             MutationUpdateNode,
		ExpectedRevision: created.Node.Revision,
		Node: Node{
			ID:      "node:memory:pref",
			Type:    "memory_record",
			Summary: "Updated SDK-first packaging preference.",
			Attributes: map[string]string{
				"project_id": "project:kilo",
				"session_id": "session:alpha",
			},
		},
	}); err != nil {
		t.Fatalf("update vector node: %v", err)
	}
	update := receiveVectorOperation(t, index.ops)
	assertVectorUpsert(t, update, 2, "Updated SDK-first packaging preference.")

	if _, err := store.Apply(ctx, Mutation{
		Type:             MutationDeleteNode,
		ExpectedRevision: 2,
		Node:             Node{ID: "node:memory:pref"},
	}); err != nil {
		t.Fatalf("delete vector node: %v", err)
	}
	deleted := receiveVectorOperation(t, index.ops)
	if deleted.Type != VectorOperationDelete || deleted.NodeID != "node:memory:pref" || deleted.SourceSeq != 3 {
		t.Fatalf("unexpected delete operation: %+v", deleted)
	}
	waitForProjectorSeq(t, store, "vector-test", 3)
}

func TestVectorProjectorValidatesOperationShape(t *testing.T) {
	ctx := context.Background()
	index := newRecordingVectorIndex(1)
	projector, err := NewVectorProjector(VectorProjectorOptions{
		Name:  "vector-test",
		Index: index,
		Mapper: VectorOperationMapperFunc(func(ctx context.Context, event ProjectionEvent) (VectorOperation, bool, error) {
			return VectorOperation{
				Type:           VectorOperationUpsert,
				NodeID:         "node:bad",
				NodeType:       "memory_record",
				EmbeddingModel: "embedding-test-v1",
				Dimensions:     3,
				Vector:         []float32{0.1, 0.2},
			}, true, nil
		}),
	})
	if err != nil {
		t.Fatalf("new vector projector: %v", err)
	}
	err = projector.Project(ctx, ProjectionEvent{Seq: 7})
	if err == nil || !strings.Contains(err.Error(), "dimensions") {
		t.Fatalf("expected dimensions validation error, got %v", err)
	}
	assertNoVectorOperation(t, index.ops)
}

func TestExactVectorSearchFiltersHierarchyBeforeScoring(t *testing.T) {
	ctx := context.Background()
	index := NewExactVectorIndex()
	for _, op := range []VectorOperation{
		{
			Type:           VectorOperationUpsert,
			NodeID:         "node:lynn-pref",
			NodeType:       "memory_record",
			HierarchyScope: map[string]string{"human_id": "human:lynn"},
			EmbeddingModel: "embedding-test-v1",
			Vector:         []float32{1, 0},
			SourceSeq:      1,
		},
		{
			Type:           VectorOperationUpsert,
			NodeID:         "node:casey-pref",
			NodeType:       "memory_record",
			HierarchyScope: map[string]string{"human_id": "human:casey"},
			EmbeddingModel: "embedding-test-v1",
			Vector:         []float32{0, 1},
			SourceSeq:      2,
		},
		{
			Type:           VectorOperationUpsert,
			NodeID:         "node:lynn-release",
			NodeType:       "release_note",
			HierarchyScope: map[string]string{"human_id": "human:lynn"},
			EmbeddingModel: "embedding-test-v1",
			Vector:         []float32{0, 1},
			SourceSeq:      3,
		},
	} {
		if err := index.ApplyVectorOperation(ctx, op); err != nil {
			t.Fatalf("apply vector operation %s: %v", op.NodeID, err)
		}
	}

	unfiltered, err := index.SearchVectors(ctx, VectorSearchQuery{
		Vector: []float32{0, 1},
		NodeTypes: []string{
			"memory_record",
		},
		IncludeCandidates: true,
	})
	if err != nil {
		t.Fatalf("unfiltered vector search: %v", err)
	}
	if ids := vectorHitIDs(unfiltered.Hits); strings.Join(ids, ",") != "node:casey-pref,node:lynn-pref" {
		t.Fatalf("unexpected unfiltered ranking: %v", ids)
	}

	filtered, err := index.SearchVectors(ctx, VectorSearchQuery{
		Vector: []float32{0, 1},
		NodeTypes: []string{
			"memory_record",
		},
		HierarchyScope: map[string]string{
			"human_id": "human:lynn",
		},
		MinIndexedSeq:     3,
		IncludeCandidates: true,
	})
	if err != nil {
		t.Fatalf("filtered vector search: %v", err)
	}
	if filtered.Stale || filtered.IndexedSeq != 3 || filtered.MinIndexedSeq != 3 {
		t.Fatalf("unexpected fresh watermarks: %+v", filtered)
	}
	if ids := filtered.CandidateIDs; strings.Join(ids, ",") != "node:lynn-pref" {
		t.Fatalf("hierarchy filter should narrow candidates before ranking, got %v", ids)
	}
	if ids := vectorHitIDs(filtered.Hits); strings.Join(ids, ",") != "node:lynn-pref" {
		t.Fatalf("unexpected filtered hits: %v", ids)
	}

	stale, err := index.SearchVectors(ctx, VectorSearchQuery{
		Vector:        []float32{1, 0},
		MinIndexedSeq: 99,
	})
	if err != nil {
		t.Fatalf("stale vector search: %v", err)
	}
	if !stale.Stale || stale.IndexedSeq != 3 || stale.MinIndexedSeq != 99 {
		t.Fatalf("expected stale watermark, got %+v", stale)
	}
}

func TestStoreVectorSearchExpandsHierarchyAndReportsProjectionWatermarks(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, t.TempDir(), fixedClock(time.Now()))
	defer store.Close()

	index := NewExactVectorIndex()
	projector, err := NewVectorProjector(VectorProjectorOptions{
		Name:  "vector-test",
		Index: index,
		Mapper: VectorOperationMapperFunc(func(ctx context.Context, event ProjectionEvent) (VectorOperation, bool, error) {
			switch event.Mutation.Type {
			case MutationCreateNode, MutationUpdateNode:
				node := event.Mutation.Node
				if node.Type != "memory_record" {
					return VectorOperation{}, false, nil
				}
				return VectorOperation{
					Type:           VectorOperationUpsert,
					NodeID:         node.ID,
					NodeType:       node.Type,
					HierarchyScope: vectorHierarchyScope(node),
					EmbeddingModel: "embedding-test-v1",
					Vector:         []float32{1, 0},
				}, true, nil
			case MutationDeleteNode:
				return VectorOperation{
					Type:   VectorOperationDelete,
					NodeID: event.Mutation.Node.ID,
				}, true, nil
			default:
				return VectorOperation{}, false, nil
			}
		}),
	})
	if err != nil {
		t.Fatalf("new vector projector: %v", err)
	}
	if err := store.RegisterProjector(ctx, projector); err != nil {
		t.Fatalf("register vector projector: %v", err)
	}

	var lastSeq uint64
	for _, mutation := range []Mutation{
		{Type: MutationCreateNode, Node: Node{ID: "node:human:lynn", Type: "human", Name: "Lynn"}},
		{Type: MutationCreateNode, Node: Node{ID: "node:episode:preferences", Type: "episode", Name: "Engineering preferences"}},
		{Type: MutationCreateNode, Node: Node{
			ID:      "node:memory:pref",
			Type:    "memory_record",
			Summary: "Engineering preference memory.",
			Attributes: map[string]string{
				"human_id":   "human:lynn",
				"project_id": "project:kilo",
			},
		}},
		{Type: MutationCreateNode, Node: Node{ID: "node:detail:pref", Type: "detail", Summary: "SDK packaging detail."}},
		{Type: MutationCreateNode, Node: Node{ID: "node:evidence:pref", Type: "source_evidence", Summary: "Transcript evidence."}},
		{Type: MutationCreateEdge, Edge: Edge{ID: "edge:episode-memory", Type: "contains", FromNodeID: "node:episode:preferences", ToNodeID: "node:memory:pref"}},
		{Type: MutationCreateEdge, Edge: Edge{ID: "edge:memory-detail", Type: "contains", FromNodeID: "node:memory:pref", ToNodeID: "node:detail:pref"}},
		{Type: MutationCreateEdge, Edge: Edge{ID: "edge:evidence-memory", Type: "evidence_for", FromNodeID: "node:evidence:pref", ToNodeID: "node:memory:pref"}},
	} {
		result, err := store.Apply(ctx, mutation)
		if err != nil {
			t.Fatalf("apply %s: %v", mutation.Type, err)
		}
		lastSeq = result.Seq
	}
	waitForProjectorSeq(t, store, "vector-test", lastSeq)

	result, err := store.SearchVectors(ctx, index, VectorSearchQuery{
		Projector: "vector-test",
		Vector:    []float32{1, 0},
		NodeTypes: []string{
			"memory_record",
		},
		HierarchyScope: map[string]string{
			"human_id": "human:lynn",
		},
		MinIndexedSeq:     lastSeq,
		IncludeCandidates: true,
		ExpandParents:     true,
		ParentEdgeType:    "contains",
		ExpandChildren:    true,
		ChildEdgeType:     "contains",
		ExpandEvidence:    true,
		EvidenceEdgeType:  "evidence_for",
	})
	if err != nil {
		t.Fatalf("store vector search: %v", err)
	}
	if result.Stale || result.IndexedSeq != lastSeq || result.DurableSeq != lastSeq || result.Lag != 0 {
		t.Fatalf("unexpected projection watermarks: %+v", result)
	}
	if ids := result.CandidateIDs; strings.Join(ids, ",") != "node:memory:pref" {
		t.Fatalf("unexpected vector candidate set: %v", ids)
	}
	if ids := vectorHitIDs(result.Hits); strings.Join(ids, ",") != "node:memory:pref" {
		t.Fatalf("unexpected vector hits: %v", ids)
	}
	if ids := nodeIDs(result.Expanded.Nodes); strings.Join(ids, ",") != "node:detail:pref,node:episode:preferences,node:evidence:pref" {
		t.Fatalf("unexpected expanded nodes: %v", ids)
	}
	if ids := edgeIDs(result.Expanded.Edges); strings.Join(ids, ",") != "edge:episode-memory,edge:evidence-memory,edge:memory-detail" {
		t.Fatalf("unexpected expanded edges: %v", ids)
	}
}

type recordingVectorIndex struct {
	ops chan VectorOperation
}

func newRecordingVectorIndex(buffer int) *recordingVectorIndex {
	return &recordingVectorIndex{ops: make(chan VectorOperation, buffer)}
}

func (i *recordingVectorIndex) ApplyVectorOperation(ctx context.Context, op VectorOperation) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case i.ops <- op:
		return nil
	}
}

func receiveVectorOperation(t *testing.T, ops <-chan VectorOperation) VectorOperation {
	t.Helper()
	select {
	case op := <-ops:
		return op
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for vector operation")
	}
	return VectorOperation{}
}

func assertNoVectorOperation(t *testing.T, ops <-chan VectorOperation) {
	t.Helper()
	select {
	case op := <-ops:
		t.Fatalf("unexpected vector operation: %+v", op)
	case <-time.After(50 * time.Millisecond):
	}
}

func assertVectorUpsert(t *testing.T, op VectorOperation, seq uint64, summary string) {
	t.Helper()
	if op.Type != VectorOperationUpsert {
		t.Fatalf("expected vector upsert, got %+v", op)
	}
	if op.NodeID != "node:memory:pref" || op.NodeType != "memory_record" {
		t.Fatalf("unexpected vector node identity: %+v", op)
	}
	if op.EmbeddingModel != "embedding-test-v1" || op.Dimensions != 3 || len(op.Vector) != 3 {
		t.Fatalf("unexpected embedding metadata: %+v", op)
	}
	if op.SourceSeq != seq {
		t.Fatalf("unexpected source seq: %+v", op)
	}
	if op.Attributes["summary"] != summary {
		t.Fatalf("unexpected vector attributes: %+v", op.Attributes)
	}
}

func vectorHierarchyScope(node Node) map[string]string {
	return map[string]string{
		"project_id": node.Attributes["project_id"],
		"session_id": node.Attributes["session_id"],
		"human_id":   node.Attributes["human_id"],
	}
}

func vectorHitIDs(hits []VectorSearchHit) []string {
	ids := make([]string, 0, len(hits))
	for _, hit := range hits {
		ids = append(ids, hit.NodeID)
	}
	return ids
}
