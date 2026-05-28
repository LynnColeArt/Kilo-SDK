package pilotvalidate

import (
	"context"
	"strings"
	"testing"

	"github.com/LynnColeArt/Kilo-SDK/internal/pilotimport"
	"github.com/LynnColeArt/Kilo-SDK/pkg/kilo"
)

func TestValidateReportsHierarchyRecallImprovementAndPerformance(t *testing.T) {
	ctx := context.Background()
	store := openValidationTestStore(t)
	defer store.Close()
	vectorIndex, err := kilo.NewFileVectorIndex(kilo.FileVectorIndexOptions{Path: t.TempDir(), SyncWrites: true})
	if err != nil {
		t.Fatalf("open vector index: %v", err)
	}

	importedLynn, err := pilotimport.ImportMemoryRecords(ctx, store, []pilotimport.MemoryRecord{{
		RecordID:       "record:lynn-pref",
		Kind:           "preference",
		Summary:        "Lynn prefers engineering plans with explicit tradeoffs.",
		SessionID:      "session:eng",
		MissionID:      "mission:preferences",
		SourcePersona:  "lynn",
		EmbeddingModel: "validation-embedder",
		Embedding:      []float64{1, 0},
	}}, pilotimport.MemoryImportOptions{
		Namespace:   "project:kilo",
		ProjectID:   "project:kilo",
		HumanID:     "human:lynn",
		VectorIndex: vectorIndex,
	})
	if err != nil {
		t.Fatalf("import lynn memory: %v", err)
	}
	if _, err := pilotimport.ImportMemoryRecords(ctx, store, []pilotimport.MemoryRecord{{
		RecordID:       "record:avery-pref",
		Kind:           "preference",
		Summary:        "Avery prefers concise design answers.",
		SessionID:      "session:eng",
		MissionID:      "mission:preferences",
		SourcePersona:  "avery",
		EmbeddingModel: "validation-embedder",
		Embedding:      []float64{1, 0},
	}}, pilotimport.MemoryImportOptions{
		Namespace:   "project:kilo",
		ProjectID:   "project:kilo",
		HumanID:     "human:avery",
		VectorIndex: vectorIndex,
	}); err != nil {
		t.Fatalf("import avery memory: %v", err)
	}

	report, err := Validate(ctx, store, vectorIndex, Plan{
		Cases: []Case{{
			Name:              "engineering preferences with Lynn",
			Query:             "all conversations about engineering preferences with Lynn",
			QueryVector:       []float64{1, 0},
			EmbeddingModel:    "validation-embedder",
			HierarchyScope:    map[string]string{"human_id": "human:lynn"},
			ExpectedRecordIDs: []string{"record:lynn-pref"},
			Limit:             1,
		}},
		ParallelSessions: 2,
		Repeats:          2,
	}, importedLynn)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if report.Summary.Cases != 1 || report.Summary.Passed != 1 || report.Summary.Failed != 0 || report.Summary.Improved != 1 {
		t.Fatalf("unexpected summary: %+v", report.Summary)
	}
	if report.Imported.Vectors != 1 {
		t.Fatalf("expected imported vector count in report, got %+v", report.Imported)
	}
	if got := strings.Join(report.Cases[0].BaselineHitIDs, ","); got != "node:memory:record:avery-pref" {
		t.Fatalf("expected flat baseline to hit newest ambiguous memory, got %s", got)
	}
	if got := strings.Join(report.Cases[0].KiloHitIDs, ","); got != "node:memory:record:lynn-pref" {
		t.Fatalf("expected hierarchy-aware Kilo hit, got %s", got)
	}
	if len(report.Cases[0].MissingNodeIDs) != 0 {
		t.Fatalf("expected no missing nodes, got %v", report.Cases[0].MissingNodeIDs)
	}
	if report.Cases[0].DurableSeq == 0 {
		t.Fatalf("expected durable sequence to be reported: %+v", report.Cases[0])
	}
	if report.Performance.Queries != 4 || report.Performance.ParallelSessions != 2 || report.Performance.Repeats != 2 {
		t.Fatalf("unexpected performance report: %+v", report.Performance)
	}
}

func TestLoadPlanNormalizesExpectedRecordIDs(t *testing.T) {
	plan, err := LoadPlan(strings.NewReader(`{
		"schema":"kilo.memory_validation/v1",
		"cases":[{
			"name":"record id case",
			"query_vector":[0.5,0.5],
			"expected_record_ids":["record:one"]
		}]
	}`))
	if err != nil {
		t.Fatalf("load plan: %v", err)
	}
	if got := strings.Join(plan.Cases[0].ExpectedNodeIDs, ","); got != "node:memory:record:one" {
		t.Fatalf("unexpected expected node ids: %s", got)
	}
	if got := strings.Join(plan.Cases[0].NodeTypes, ","); got != "memory_record" {
		t.Fatalf("unexpected default node types: %s", got)
	}
}

func TestValidateRequireFreshReportsStaleWithoutAbortingPerformance(t *testing.T) {
	ctx := context.Background()
	store := openValidationTestStore(t)
	defer store.Close()
	vectorIndex := kilo.NewExactVectorIndex()
	if err := vectorIndex.ApplyVectorOperation(ctx, kilo.VectorOperation{
		Type:           kilo.VectorOperationUpsert,
		NodeID:         "node:memory:stale",
		NodeType:       "memory_record",
		EmbeddingModel: "validation-embedder",
		Vector:         []float32{1, 0},
		SourceSeq:      1,
	}); err != nil {
		t.Fatalf("apply vector operation: %v", err)
	}
	if _, err := store.Write(ctx, kilo.Write{CreateRecord: &kilo.Record{
		ID:        "record:stale",
		Namespace: "project:kilo",
		Kind:      "preference",
		Summary:   "A durable record beyond the vector watermark.",
	}}); err != nil {
		t.Fatalf("write record: %v", err)
	}
	if _, err := store.Write(ctx, kilo.Write{CreateNode: &kilo.Node{
		ID:      "node:memory:stale",
		Type:    "memory_record",
		Summary: "A stale vector-backed memory.",
	}}); err != nil {
		t.Fatalf("write node: %v", err)
	}

	report, err := Validate(ctx, store, vectorIndex, Plan{
		RequireFresh: true,
		Cases: []Case{{
			Name:            "stale freshness report",
			QueryVector:     []float64{1, 0},
			EmbeddingModel:  "validation-embedder",
			ExpectedNodeIDs: []string{"node:memory:stale"},
		}},
		ParallelSessions: 1,
		Repeats:          1,
	}, pilotimport.MemoryImportResult{})
	if err != nil {
		t.Fatalf("validate should report stale cases instead of aborting: %v", err)
	}
	if report.Summary.Passed != 0 || report.Summary.Failed != 1 {
		t.Fatalf("unexpected stale summary: %+v", report.Summary)
	}
	if !report.Cases[0].Stale || report.Cases[0].Lag == 0 {
		t.Fatalf("expected stale case with lag: %+v", report.Cases[0])
	}
	if report.Performance.Queries != 1 {
		t.Fatalf("expected performance measurement to complete: %+v", report.Performance)
	}
}

func TestLoadPlanRejectsExcessivePerformanceWork(t *testing.T) {
	_, err := LoadPlan(strings.NewReader(`{
		"schema":"kilo.memory_validation/v1",
		"parallel_sessions":1024,
		"repeats":100000,
		"cases":[{
			"name":"too much",
			"query_vector":[1,0],
			"expected_node_ids":["node:memory:one"]
		}]
	}`))
	if err == nil || !strings.Contains(err.Error(), "exceed") {
		t.Fatalf("expected excessive query count error, got %v", err)
	}
}

func openValidationTestStore(t *testing.T) *kilo.Store {
	t.Helper()
	store, err := kilo.Open(context.Background(), kilo.Options{Path: t.TempDir(), SyncWrites: true})
	if err != nil {
		t.Fatalf("open validation test store: %v", err)
	}
	return store
}
