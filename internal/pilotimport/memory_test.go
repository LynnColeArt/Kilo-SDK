package pilotimport

import (
	"context"
	"strings"
	"testing"

	"github.com/LynnColeArt/Kilo-SDK/pkg/kilo"
)

func TestImportMemoryRecordsPreservesSugarFangMetadataAndHierarchy(t *testing.T) {
	ctx := context.Background()
	store := openImportTestStore(t)
	defer store.Close()
	vectorIndex, err := kilo.NewFileVectorIndex(kilo.FileVectorIndexOptions{Path: t.TempDir(), SyncWrites: true})
	if err != nil {
		t.Fatalf("open vector index: %v", err)
	}

	startLine := 12
	endLine := 18
	qaApproved := true
	records := []MemoryRecord{
		{
			Schema:        MemoryImportSchema,
			RecordID:      "record:old",
			Kind:          "decision",
			Summary:       "Old implementation choice.",
			Tags:          []string{"Architecture"},
			SessionID:     "session:alpha",
			MissionID:     "mission:build",
			MissionPhase:  "worker",
			SourcePersona: "nora",
			Disposition:   "accepted",
			CreatedAt:     "2026-05-09T12:00:00Z",
		},
		{
			Schema:             MemoryImportSchema,
			RecordID:           "record:new",
			Kind:               "context_summary",
			Summary:            "New implementation choice.",
			Detail:             "Keep Kilo SDK-first and map SugarFang memory as data.",
			Rationale:          "The pilot should not import workflow policy.",
			Tags:               []string{"Architecture", "Pilot"},
			Source:             "agent",
			SessionID:          "session:alpha",
			MissionID:          "mission:build",
			MissionPhase:       "qa",
			SourcePersona:      "lynn",
			FilePath:           "docs/diary/qa/entry.md",
			StartLine:          &startLine,
			EndLine:            &endLine,
			CreatedAt:          "2026-05-09T12:05:00Z",
			Disposition:        "superseded",
			ReviewedBy:         "qa",
			SupersedesRecordID: "record:old",
			Metadata:           map[string]any{"anchor_kind": "mission_diary", "status": "accepted"},
			EmbeddingModel:     "sugarfang-test-embedder",
			Embedding:          []float64{0.1, 0.2, 0.3},
			QAApproved:         &qaApproved,
			RetrievalText:      `{"schema":"sugarfang.memory.embedding_document/v1"}`,
			SourceText:         "  Diary source excerpt.\n",
		},
	}

	result, err := ImportMemoryRecords(ctx, store, records, MemoryImportOptions{
		Namespace:   "project:kilo",
		ProjectID:   "project:kilo",
		HumanID:     "human:lynn",
		VectorIndex: vectorIndex,
	})
	if err != nil {
		t.Fatalf("import memory records: %v", err)
	}
	if result.Records != 2 || result.Nodes != 6 || result.Edges != 6 || result.Chunks != 1 || result.SourceSpans != 1 || result.Vectors != 1 {
		t.Fatalf("unexpected import result: %+v", result)
	}

	read, err := store.Read(ctx, kilo.Read{RecordID: "record:new"})
	if err != nil {
		t.Fatalf("read imported record: %v", err)
	}
	attrs := read.Record.Attributes
	if read.Record.Kind != "context_summary" ||
		read.Record.SessionID != "session:alpha" ||
		read.Record.MissionID != "mission:build" ||
		read.Record.Persona != "lynn" ||
		attrs["disposition"] != "superseded" ||
		attrs["supersedes_record_id"] != "record:old" ||
		attrs["file_path"] != "docs/diary/qa/entry.md" ||
		attrs["start_line"] != "12" ||
		attrs["end_line"] != "18" ||
		attrs["mission_phase"] != "qa" ||
		attrs["qa_approved"] != "true" ||
		attrs["embedding_model"] != "sugarfang-test-embedder" ||
		attrs["embedding_dimensions"] != "3" {
		t.Fatalf("import did not preserve record metadata: %+v", read.Record)
	}
	if _, ok := attrs["embedding_json"]; ok {
		t.Fatalf("embedding_json should not be stored as record metadata: %+v", attrs)
	}
	if !strings.Contains(attrs["metadata_json"], "mission_diary") {
		t.Fatalf("metadata json was not preserved: %+v", attrs)
	}

	vectorResult, err := vectorIndex.SearchVectors(ctx, kilo.VectorSearchQuery{
		Vector:         []float32{0.1, 0.2, 0.3},
		NodeTypes:      []string{"memory_record"},
		HierarchyScope: map[string]string{"mission_id": "mission:build", "source_persona": "lynn", "human_id": "human:lynn"},
		EmbeddingModel: "sugarfang-test-embedder",
		MinIndexedSeq:  2,
	})
	if err != nil {
		t.Fatalf("search imported vector: %v", err)
	}
	if ids := vectorHitIDs(vectorResult.Hits); strings.Join(ids, ",") != "node:memory:record:new" {
		t.Fatalf("unexpected vector hits: %v", ids)
	}

	nodeQuery, err := store.Query(ctx, kilo.Query{Nodes: &kilo.NodeQuery{
		Type:             "memory_record",
		RelatedNodeID:    "node:mission:mission:build",
		RelatedEdgeType:  "contains",
		RelatedDirection: kilo.EdgeDirectionOutgoing,
	}})
	if err != nil {
		t.Fatalf("query memory nodes by mission: %v", err)
	}
	if ids := nodeIDs(nodeQuery.Nodes.Nodes); strings.Join(ids, ",") != "node:memory:record:new,node:memory:record:old" {
		t.Fatalf("unexpected mission memory nodes: %v", ids)
	}

	superseded, err := store.Query(ctx, kilo.Query{Children: &kilo.GraphQuery{
		NodeID: "node:memory:record:new",
		Edge:   kilo.EdgeQuery{Type: "supersedes"},
	}})
	if err != nil {
		t.Fatalf("query superseded memory: %v", err)
	}
	if ids := nodeIDs(superseded.Graph.Nodes); strings.Join(ids, ",") != "node:memory:record:old" {
		t.Fatalf("unexpected superseded memory nodes: %v", ids)
	}

	spans, err := store.Query(ctx, kilo.Query{SourceSpans: &kilo.SourceSpanQuery{RecordID: "record:new"}})
	if err != nil {
		t.Fatalf("query imported source span: %v", err)
	}
	if len(spans.SourceSpans.Spans) != 1 {
		t.Fatalf("expected source span, got %+v", spans.SourceSpans)
	}
	content, err := store.Read(ctx, kilo.Read{ChunkContentID: spans.SourceSpans.Spans[0].ChunkRefs[0].ChunkID})
	if err != nil {
		t.Fatalf("read source chunk: %v", err)
	}
	if string(content.ChunkContent) != "  Diary source excerpt.\n" {
		t.Fatalf("unexpected source chunk content: %q", string(content.ChunkContent))
	}
}

func TestImportMemoryRecordsRequiresVectorIndexForEmbeddings(t *testing.T) {
	ctx := context.Background()
	store := openImportTestStore(t)
	defer store.Close()

	_, err := ImportMemoryRecords(ctx, store, []MemoryRecord{{
		RecordID:       "record:needs-vector-index",
		Kind:           "decision",
		Summary:        "Embeddings need typed vector storage.",
		EmbeddingModel: "sugarfang-test-embedder",
		Embedding:      []float64{0.1, 0.2},
	}}, MemoryImportOptions{})
	if err == nil || !strings.Contains(err.Error(), "vector index is required") {
		t.Fatalf("expected vector index requirement, got %v", err)
	}
	if status := store.Status(); status.DurableSeq != 0 || status.Records != 0 {
		t.Fatalf("import should fail before writing without vector index: %+v", status)
	}
}

func TestLoadMemoryRecordsSupportsJSONL(t *testing.T) {
	records, err := LoadMemoryRecords(strings.NewReader(`{"record_id":"record:1","kind":"summary","summary":"One."}` + "\n" + `{"record_id":"record:2","kind":"decision","summary":"Two."}`))
	if err != nil {
		t.Fatalf("load jsonl: %v", err)
	}
	if len(records) != 2 || records[0].RecordID != "record:1" || records[1].RecordID != "record:2" {
		t.Fatalf("unexpected records: %+v", records)
	}
}

func openImportTestStore(t *testing.T) *kilo.Store {
	t.Helper()
	store, err := kilo.Open(context.Background(), kilo.Options{Path: t.TempDir(), SyncWrites: true})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return store
}

func nodeIDs(nodes []kilo.Node) []string {
	ids := make([]string, 0, len(nodes))
	for _, node := range nodes {
		ids = append(ids, node.ID)
	}
	return ids
}

func vectorHitIDs(hits []kilo.VectorSearchHit) []string {
	ids := make([]string, 0, len(hits))
	for _, hit := range hits {
		ids = append(ids, hit.NodeID)
	}
	return ids
}
