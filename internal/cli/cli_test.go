package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LynnColeArt/Kilo-SDK/internal/pilotvalidate"
	"github.com/LynnColeArt/Kilo-SDK/pkg/kilo"
)

func TestRunStatusInspectAndTail(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	store, err := kilo.Open(ctx, kilo.Options{Path: path, SyncWrites: true})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, err := store.Write(ctx, kilo.Write{CreateRecord: &kilo.Record{
		ID:        "record:cli",
		Namespace: "project:kilo",
		Kind:      "note",
		Summary:   "CLI should inspect SDK-created stores.",
	}}); err != nil {
		t.Fatalf("write record: %v", err)
	}
	if _, err := store.Write(ctx, kilo.Write{CreateNode: &kilo.Node{
		ID:      "node:cli",
		Type:    "memory_record",
		Summary: "CLI inspection node.",
	}}); err != nil {
		t.Fatalf("write node: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	statusOut, statusErr := &bytes.Buffer{}, &bytes.Buffer{}
	if code := Run([]string{"status", "--path", path}, statusOut, statusErr); code != 0 {
		t.Fatalf("status code = %d stderr = %s", code, statusErr.String())
	}
	var status kilo.Status
	if err := json.Unmarshal(statusOut.Bytes(), &status); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if status.DurableSeq != 2 || status.Records != 1 || status.Nodes != 1 {
		t.Fatalf("unexpected status: %+v", status)
	}

	inspectOut, inspectErr := &bytes.Buffer{}, &bytes.Buffer{}
	if code := Run([]string{"inspect", "record", "record:cli", "--path", path}, inspectOut, inspectErr); code != 0 {
		t.Fatalf("inspect code = %d stderr = %s", code, inspectErr.String())
	}
	var read kilo.ReadResult
	if err := json.Unmarshal(inspectOut.Bytes(), &read); err != nil {
		t.Fatalf("decode inspect: %v", err)
	}
	if !read.Found || read.Record.ID != "record:cli" {
		t.Fatalf("unexpected inspect result: %+v", read)
	}

	tailOut, tailErr := &bytes.Buffer{}, &bytes.Buffer{}
	if code := Run([]string{"tail", "--path", path, "--limit", "1"}, tailOut, tailErr); code != 0 {
		t.Fatalf("tail code = %d stderr = %s", code, tailErr.String())
	}
	var tail kilo.TailResult
	if err := json.Unmarshal(tailOut.Bytes(), &tail); err != nil {
		t.Fatalf("decode tail: %v", err)
	}
	if len(tail.Entries) != 1 || tail.Entries[0].Mutation.Node.ID != "node:cli" {
		t.Fatalf("unexpected tail: %+v", tail)
	}
}

func TestRunImportMemoryInitializesExplicitStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kilo-store")
	input := filepath.Join(t.TempDir(), "memory.jsonl")
	if err := os.WriteFile(input, []byte(`{"record_id":"record:cli-import","kind":"decision","summary":"Import memory through the CLI.","session_id":"session:cli","mission_id":"mission:cli","source_persona":"nora","tags":["Pilot"],"source_text":"source excerpt","embedding_model":"cli-test-embedder","embedding":[1,0]}`+"\n"), 0o644); err != nil {
		t.Fatalf("write input: %v", err)
	}

	importOut, importErr := &bytes.Buffer{}, &bytes.Buffer{}
	code := Run([]string{
		"import-memory",
		"--path", path,
		"--input", input,
		"--init",
		"--namespace", "project:kilo",
		"--project-id", "project:kilo",
		"--human-id", "human:lynn",
	}, importOut, importErr)
	if code != 0 {
		t.Fatalf("import code = %d stderr = %s", code, importErr.String())
	}
	var result struct {
		Records     int `json:"records"`
		Nodes       int `json:"nodes"`
		Edges       int `json:"edges"`
		Chunks      int `json:"chunks"`
		SourceSpans int `json:"source_spans"`
		Vectors     int `json:"vectors"`
	}
	if err := json.Unmarshal(importOut.Bytes(), &result); err != nil {
		t.Fatalf("decode import result: %v", err)
	}
	if result.Records != 1 || result.Nodes != 4 || result.Edges != 3 || result.Chunks != 1 || result.SourceSpans != 1 || result.Vectors != 1 {
		t.Fatalf("unexpected import result: %+v", result)
	}

	inspectOut, inspectErr := &bytes.Buffer{}, &bytes.Buffer{}
	if code := Run([]string{"inspect", "record", "record:cli-import", "--path", path}, inspectOut, inspectErr); code != 0 {
		t.Fatalf("inspect imported record code = %d stderr = %s", code, inspectErr.String())
	}
	var read kilo.ReadResult
	if err := json.Unmarshal(inspectOut.Bytes(), &read); err != nil {
		t.Fatalf("decode imported record: %v", err)
	}
	if !read.Found || read.Record.Attributes["source_persona"] != "nora" {
		t.Fatalf("unexpected imported record: %+v", read)
	}
	if _, ok := read.Record.Attributes["embedding_json"]; ok {
		t.Fatalf("embedding_json should not be stored as record metadata: %+v", read.Record.Attributes)
	}

	vectorIndex, err := kilo.NewFileVectorIndex(kilo.FileVectorIndexOptions{Path: filepath.Join(path, "vectors")})
	if err != nil {
		t.Fatalf("open CLI vector index: %v", err)
	}
	vectorResult, err := vectorIndex.SearchVectors(context.Background(), kilo.VectorSearchQuery{
		Vector:         []float32{1, 0},
		NodeTypes:      []string{"memory_record"},
		HierarchyScope: map[string]string{"mission_id": "mission:cli", "source_persona": "nora"},
		EmbeddingModel: "cli-test-embedder",
	})
	if err != nil {
		t.Fatalf("search CLI vector index: %v", err)
	}
	if len(vectorResult.Hits) != 1 || vectorResult.Hits[0].NodeID != "node:memory:record:cli-import" {
		t.Fatalf("unexpected CLI vector hits: %+v", vectorResult.Hits)
	}
}

func TestOpenStoreForServeCanInitializeExplicitStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kilo-store")
	stderr := &bytes.Buffer{}
	_, closeStore, ok := openStoreForServe(options{path: path, initStore: true}, stderr)
	if !ok {
		t.Fatalf("open store for serve failed: %s", stderr.String())
	}
	closeStore()

	statusOut, statusErr := &bytes.Buffer{}, &bytes.Buffer{}
	if code := Run([]string{"status", "--path", path}, statusOut, statusErr); code != 0 {
		t.Fatalf("status code = %d stderr = %s", code, statusErr.String())
	}
	var status kilo.Status
	if err := json.Unmarshal(statusOut.Bytes(), &status); err != nil {
		t.Fatalf("decode status: %v", err)
	}
}

func TestRunValidateMemoryImportsAndReportsRecall(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kilo-store")
	input := filepath.Join(t.TempDir(), "memory.jsonl")
	cases := filepath.Join(t.TempDir(), "cases.json")
	if err := os.WriteFile(input, []byte(`{"record_id":"record:validate-import","kind":"preference","summary":"Validate recall through the CLI.","session_id":"session:validate","mission_id":"mission:validate","source_persona":"lynn","embedding_model":"cli-validation-embedder","embedding":[1,0]}`+"\n"), 0o644); err != nil {
		t.Fatalf("write input: %v", err)
	}
	if err := os.WriteFile(cases, []byte(`{
		"schema":"kilo.memory_validation/v1",
		"cases":[{
			"name":"validate lynn preference",
			"query_vector":[1,0],
			"embedding_model":"cli-validation-embedder",
			"hierarchy_scope":{"human_id":"human:lynn"},
			"expected_record_ids":["record:validate-import"],
			"limit":1
		}]
	}`), 0o644); err != nil {
		t.Fatalf("write cases: %v", err)
	}

	validateOut, validateErr := &bytes.Buffer{}, &bytes.Buffer{}
	code := Run([]string{
		"validate-memory",
		"--path", path,
		"--input", input,
		"--cases", cases,
		"--init",
		"--namespace", "project:kilo",
		"--project-id", "project:kilo",
		"--human-id", "human:lynn",
		"--parallel-sessions", "2",
		"--repeats", "1",
	}, validateOut, validateErr)
	if code != 0 {
		t.Fatalf("validate code = %d stderr = %s", code, validateErr.String())
	}
	var report pilotvalidate.Report
	if err := json.Unmarshal(validateOut.Bytes(), &report); err != nil {
		t.Fatalf("decode validation report: %v", err)
	}
	if report.Summary.Passed != 1 || report.Summary.Failed != 0 || report.Imported.Vectors != 1 {
		t.Fatalf("unexpected validation report: %+v", report)
	}
	if report.Performance.Queries != 2 {
		t.Fatalf("expected parallel validation timing, got %+v", report.Performance)
	}
	if len(report.Cases) != 1 || strings.Join(report.Cases[0].KiloHitIDs, ",") != "node:memory:record:validate-import" {
		t.Fatalf("unexpected validation hits: %+v", report.Cases)
	}
}

func TestRunReportsUsageErrors(t *testing.T) {
	stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}
	if code := Run([]string{"status"}, stdout, stderr); code == 0 {
		t.Fatalf("expected usage failure")
	}
	if stdout.Len() != 0 {
		t.Fatalf("usage error should not write stdout: %s", stdout.String())
	}
	if !strings.Contains(stderr.String(), "--path") {
		t.Fatalf("expected path guidance, got %q", stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	if code := Run([]string{"validate-memory", "--path", "/tmp/kilo", "--cases", "/tmp/cases.json", "--parallel-sessions", "-1"}, stdout, stderr); code == 0 {
		t.Fatalf("expected negative parallel-sessions failure")
	}
	if stdout.Len() != 0 {
		t.Fatalf("usage parse error should not write stdout: %s", stdout.String())
	}
	if !strings.Contains(stderr.String(), "non-negative") {
		t.Fatalf("expected non-negative guidance, got %q", stderr.String())
	}
}

func TestRunDoesNotCreateMissingStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing")
	stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}

	if code := Run([]string{"status", "--path", path}, stdout, stderr); code == 0 {
		t.Fatalf("expected missing store failure")
	}
	if stdout.Len() != 0 {
		t.Fatalf("missing store should not write stdout: %s", stdout.String())
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("missing store path should not be created, stat err = %v", err)
	}
}

func TestRunDoesNotInitializeEmptyDirectory(t *testing.T) {
	path := t.TempDir()
	stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}

	if code := Run([]string{"status", "--path", path}, stdout, stderr); code == 0 {
		t.Fatalf("expected uninitialized store failure")
	}
	if stdout.Len() != 0 {
		t.Fatalf("uninitialized store should not write stdout: %s", stdout.String())
	}
	if _, err := os.Stat(filepath.Join(path, "segments")); !os.IsNotExist(err) {
		t.Fatalf("segments directory should not be created, stat err = %v", err)
	}
}
