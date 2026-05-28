package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/LynnColeArt/Kilo-SDK/pkg/kilo"
)

func TestHandlerServesStatusInspectionAndTail(t *testing.T) {
	ctx := context.Background()
	store := openHTTPTestStore(t)
	defer store.Close()

	if _, err := store.Write(ctx, kilo.Write{CreateRecord: &kilo.Record{
		ID:        "record:http",
		Namespace: "project:kilo",
		Kind:      "note",
		Summary:   "HTTP adapter should stay thin.",
	}}); err != nil {
		t.Fatalf("write record: %v", err)
	}
	if _, err := store.Write(ctx, kilo.Write{CreateNode: &kilo.Node{
		ID:      "node:http",
		Type:    "memory_record",
		Summary: "HTTP inspection node.",
	}}); err != nil {
		t.Fatalf("write node: %v", err)
	}

	handler := New(store)

	statusResponse := httptest.NewRecorder()
	statusRequest := httptest.NewRequest(http.MethodGet, "/v1/status", nil)
	handler.ServeHTTP(statusResponse, statusRequest)
	if statusResponse.Code != http.StatusOK {
		t.Fatalf("status code = %d body = %s", statusResponse.Code, statusResponse.Body.String())
	}
	var status kilo.Status
	if err := json.Unmarshal(statusResponse.Body.Bytes(), &status); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if status.DurableSeq != 2 || status.Records != 1 || status.Nodes != 1 {
		t.Fatalf("unexpected status: %+v", status)
	}

	recordResponse := httptest.NewRecorder()
	recordRequest := httptest.NewRequest(http.MethodGet, "/v1/records/record:http", nil)
	handler.ServeHTTP(recordResponse, recordRequest)
	if recordResponse.Code != http.StatusOK {
		t.Fatalf("record code = %d body = %s", recordResponse.Code, recordResponse.Body.String())
	}
	var recordRead kilo.ReadResult
	if err := json.Unmarshal(recordResponse.Body.Bytes(), &recordRead); err != nil {
		t.Fatalf("decode record read: %v", err)
	}
	if !recordRead.Found || recordRead.Record.ID != "record:http" {
		t.Fatalf("unexpected record read: %+v", recordRead)
	}

	tailResponse := httptest.NewRecorder()
	tailRequest := httptest.NewRequest(http.MethodGet, "/v1/tail?limit=1", nil)
	handler.ServeHTTP(tailResponse, tailRequest)
	if tailResponse.Code != http.StatusOK {
		t.Fatalf("tail code = %d body = %s", tailResponse.Code, tailResponse.Body.String())
	}
	var tail kilo.TailResult
	if err := json.Unmarshal(tailResponse.Body.Bytes(), &tail); err != nil {
		t.Fatalf("decode tail: %v", err)
	}
	if len(tail.Entries) != 1 || tail.Entries[0].Mutation.Node.ID != "node:http" {
		t.Fatalf("unexpected tail: %+v", tail)
	}

	queryResponse := httptest.NewRecorder()
	queryRequest := httptest.NewRequest(http.MethodGet, "/v1/query/records?namespace=project:kilo&kind=note&limit=10", nil)
	handler.ServeHTTP(queryResponse, queryRequest)
	if queryResponse.Code != http.StatusOK {
		t.Fatalf("query records code = %d body = %s", queryResponse.Code, queryResponse.Body.String())
	}
	var query kilo.RecordQueryResult
	if err := json.Unmarshal(queryResponse.Body.Bytes(), &query); err != nil {
		t.Fatalf("decode record query: %v", err)
	}
	if len(query.Records) != 1 || query.Records[0].ID != "record:http" {
		t.Fatalf("unexpected record query: %+v", query)
	}
}

func TestHandlerReturnsMachineReadableErrors(t *testing.T) {
	store := openHTTPTestStore(t)
	defer store.Close()

	handler := New(store)
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/records/missing", nil)
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("missing record code = %d body = %s", response.Code, response.Body.String())
	}
	var body ErrorResponse
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if body.Error == "" {
		t.Fatalf("expected machine-readable error body")
	}
}

func TestHandlerImportsMemoryRecordsIdempotently(t *testing.T) {
	store := openHTTPTestStore(t)
	defer store.Close()
	vectorIndex, err := kilo.NewFileVectorIndex(kilo.FileVectorIndexOptions{Path: t.TempDir(), SyncWrites: true})
	if err != nil {
		t.Fatalf("open vector index: %v", err)
	}
	handler := NewWithOptions(store, ServerOptions{
		MemoryImportVectorIndex: vectorIndex,
		DefaultNamespace:        "sugarfang",
		DefaultProjectID:        "project:http",
		DefaultHumanID:          "human:test",
	})
	body := []byte(`{
		"records": [{
			"schema": "kilo.memory_import/v1",
			"record_id": "record:dual-write",
			"kind": "decision",
			"summary": "Dual write goes through the owner.",
			"session_id": "session:http",
			"mission_id": "mission:http",
			"source_persona": "carlo",
			"embedding_model": "test-embedding",
			"embedding_dimensions": 3,
			"embedding": [0.1, 0.2, 0.3]
		}]
	}`)

	first := httptest.NewRecorder()
	handler.ServeHTTP(first, httptest.NewRequest(http.MethodPost, "/v1/import-memory", bytes.NewReader(body)))
	if first.Code != http.StatusOK {
		t.Fatalf("first import code = %d body = %s", first.Code, first.Body.String())
	}
	var firstResponse MemoryImportResponse
	if err := json.Unmarshal(first.Body.Bytes(), &firstResponse); err != nil {
		t.Fatalf("decode first response: %v", err)
	}
	if firstResponse.Result.Records != 1 || firstResponse.Result.Nodes == 0 || firstResponse.Result.Vectors != 1 {
		t.Fatalf("unexpected first import response: %+v", firstResponse)
	}
	if firstResponse.DurableSeq == 0 {
		t.Fatalf("expected durable seq in response: %+v", firstResponse)
	}

	second := httptest.NewRecorder()
	handler.ServeHTTP(second, httptest.NewRequest(http.MethodPost, "/v1/import-memory", bytes.NewReader(body)))
	if second.Code != http.StatusOK {
		t.Fatalf("second import code = %d body = %s", second.Code, second.Body.String())
	}
	var secondResponse MemoryImportResponse
	if err := json.Unmarshal(second.Body.Bytes(), &secondResponse); err != nil {
		t.Fatalf("decode second response: %v", err)
	}
	if secondResponse.Result.Records != 0 || secondResponse.Result.SkippedRecords != 1 {
		t.Fatalf("expected idempotent second import, got %+v", secondResponse)
	}
	if secondResponse.DurableSeq != firstResponse.DurableSeq {
		t.Fatalf("idempotent import should not append, first seq=%d second seq=%d", firstResponse.DurableSeq, secondResponse.DurableSeq)
	}

	read, err := store.Read(context.Background(), kilo.Read{RecordID: "record:dual-write"})
	if err != nil {
		t.Fatalf("read imported record: %v", err)
	}
	if !read.Found || read.Record.ProjectID != "project:http" || read.Record.HumanID != "human:test" {
		t.Fatalf("unexpected imported record: %+v", read.Record)
	}
}

func openHTTPTestStore(t *testing.T) *kilo.Store {
	t.Helper()
	store, err := kilo.Open(context.Background(), kilo.Options{Path: t.TempDir(), SyncWrites: true})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return store
}
