package pilotimport

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/LynnColeArt/Kilo-SDK/pkg/kilo"
)

const MemoryImportSchema = "kilo.memory_import/v1"

type MemoryImportOptions struct {
	Namespace   string
	ProjectID   string
	HumanID     string
	VectorIndex kilo.VectorIndex
}

type MemoryRecord struct {
	Schema              string         `json:"schema,omitempty"`
	RecordID            string         `json:"record_id"`
	Kind                string         `json:"kind"`
	Summary             string         `json:"summary"`
	Detail              string         `json:"detail,omitempty"`
	Rationale           string         `json:"rationale,omitempty"`
	Tags                []string       `json:"tags,omitempty"`
	Source              string         `json:"source,omitempty"`
	SessionID           string         `json:"session_id,omitempty"`
	FilePath            string         `json:"file_path,omitempty"`
	StartLine           *int           `json:"start_line,omitempty"`
	EndLine             *int           `json:"end_line,omitempty"`
	CreatedAt           string         `json:"created_at,omitempty"`
	Disposition         string         `json:"disposition,omitempty"`
	ReviewedBy          string         `json:"reviewed_by,omitempty"`
	SupersedesRecordID  string         `json:"supersedes_record_id,omitempty"`
	Metadata            map[string]any `json:"metadata,omitempty"`
	EmbeddingModel      string         `json:"embedding_model,omitempty"`
	EmbeddingDimensions int            `json:"embedding_dimensions,omitempty"`
	Embedding           []float64      `json:"embedding,omitempty"`
	MissionID           string         `json:"mission_id,omitempty"`
	MissionPhase        string         `json:"mission_phase,omitempty"`
	SourcePersona       string         `json:"source_persona,omitempty"`
	QAApproved          *bool          `json:"qa_approved,omitempty"`
	RetrievalText       string         `json:"retrieval_text,omitempty"`
	SourceText          string         `json:"source_text,omitempty"`
}

type MemoryImportResult struct {
	Records                  int `json:"records"`
	SkippedRecords           int `json:"skipped_records"`
	Nodes                    int `json:"nodes"`
	Edges                    int `json:"edges"`
	Chunks                   int `json:"chunks"`
	SourceSpans              int `json:"source_spans"`
	Vectors                  int `json:"vectors"`
	SkippedSupersessionEdges int `json:"skipped_supersession_edges"`
}

func LoadMemoryRecords(r io.Reader) ([]MemoryRecord, error) {
	raw, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read memory import: %w", err)
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("memory import is empty")
	}
	if trimmed[0] == '[' {
		var records []MemoryRecord
		if err := json.Unmarshal(trimmed, &records); err != nil {
			return nil, fmt.Errorf("decode memory import array: %w", err)
		}
		return records, nil
	}

	scanner := bufio.NewScanner(bytes.NewReader(trimmed))
	scanner.Buffer(make([]byte, 1024), 64*1024*1024)
	records := []MemoryRecord{}
	line := 0
	for scanner.Scan() {
		line++
		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			continue
		}
		var record MemoryRecord
		if err := json.Unmarshal([]byte(text), &record); err != nil {
			return nil, fmt.Errorf("decode memory import line %d: %w", line, err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan memory import: %w", err)
	}
	return records, nil
}

func ImportMemoryRecordsFromFile(ctx context.Context, store *kilo.Store, path string, opts MemoryImportOptions) (MemoryImportResult, error) {
	file, err := os.Open(path)
	if err != nil {
		return MemoryImportResult{}, fmt.Errorf("open memory import: %w", err)
	}
	defer file.Close()
	records, err := LoadMemoryRecords(file)
	if err != nil {
		return MemoryImportResult{}, err
	}
	return ImportMemoryRecords(ctx, store, records, opts)
}

func ImportMemoryRecords(ctx context.Context, store *kilo.Store, records []MemoryRecord, opts MemoryImportOptions) (MemoryImportResult, error) {
	if err := ctx.Err(); err != nil {
		return MemoryImportResult{}, err
	}
	if store == nil {
		return MemoryImportResult{}, fmt.Errorf("store is required")
	}
	opts = normalizeMemoryImportOptions(opts)
	normalized := make([]MemoryRecord, 0, len(records))
	for index, record := range records {
		item, err := normalizeMemoryRecord(record)
		if err != nil {
			return MemoryImportResult{}, fmt.Errorf("memory import record %d: %w", index+1, err)
		}
		if len(item.Embedding) > 0 && opts.VectorIndex == nil {
			return MemoryImportResult{}, fmt.Errorf("memory import record %d: vector index is required for imported embeddings", index+1)
		}
		normalized = append(normalized, item)
	}

	var result MemoryImportResult
	for _, record := range normalized {
		if created, err := importRecord(ctx, store, record, opts); err != nil {
			return MemoryImportResult{}, err
		} else if created {
			result.Records++
		} else {
			result.SkippedRecords++
		}
		if created, err := ensureMemoryNode(ctx, store, record, opts); err != nil {
			return MemoryImportResult{}, err
		} else if created {
			result.Nodes++
		}
		if len(record.Embedding) > 0 {
			if err := importEmbeddingVector(ctx, store, record, opts); err != nil {
				return MemoryImportResult{}, err
			}
			result.Vectors++
		}
		if record.SessionID != "" {
			if created, err := ensureNode(ctx, store, sessionNode(record.SessionID, record, opts), recordTime(record)); err != nil {
				return MemoryImportResult{}, err
			} else if created {
				result.Nodes++
			}
		}
		if record.MissionID != "" {
			if created, err := ensureNode(ctx, store, missionNode(record.MissionID, record, opts), recordTime(record)); err != nil {
				return MemoryImportResult{}, err
			} else if created {
				result.Nodes++
			}
		}
		if record.SourcePersona != "" {
			if created, err := ensureNode(ctx, store, personaNode(record.SourcePersona, record, opts), recordTime(record)); err != nil {
				return MemoryImportResult{}, err
			} else if created {
				result.Nodes++
			}
		}
		if strings.TrimSpace(record.SourceText) != "" {
			chunkCreated, spanCreated, err := ensureSourceSpan(ctx, store, record, opts)
			if err != nil {
				return MemoryImportResult{}, err
			}
			if chunkCreated {
				result.Chunks++
			}
			if spanCreated {
				result.SourceSpans++
			}
		}
	}

	for _, record := range normalized {
		created, skippedSupersession, err := importEdges(ctx, store, record, opts)
		if err != nil {
			return MemoryImportResult{}, err
		}
		result.Edges += created
		if skippedSupersession {
			result.SkippedSupersessionEdges++
		}
	}
	return result, nil
}

func importRecord(ctx context.Context, store *kilo.Store, record MemoryRecord, opts MemoryImportOptions) (bool, error) {
	read, err := store.Read(ctx, kilo.Read{RecordID: record.RecordID, IncludeDeleted: true})
	if err != nil {
		return false, err
	}
	if read.Found {
		return false, nil
	}
	_, err = store.Write(ctx, kilo.Write{
		IdempotencyKey: "pilot-import:record:" + record.RecordID,
		CreateRecord: &kilo.Record{
			ID:         record.RecordID,
			Namespace:  opts.Namespace,
			ProjectID:  opts.ProjectID,
			Kind:       record.Kind,
			Summary:    record.Summary,
			Tags:       append([]string{}, record.Tags...),
			HumanID:    opts.HumanID,
			SessionID:  record.SessionID,
			MissionID:  record.MissionID,
			Persona:    record.SourcePersona,
			Attributes: memoryAttributes(record),
		},
		At: recordTime(record),
	})
	if err != nil {
		return false, err
	}
	return true, nil
}

func importEmbeddingVector(ctx context.Context, store *kilo.Store, record MemoryRecord, opts MemoryImportOptions) error {
	vector, err := memoryEmbeddingVector(record)
	if err != nil {
		return err
	}
	return opts.VectorIndex.ApplyVectorOperation(ctx, kilo.VectorOperation{
		Type:           kilo.VectorOperationUpsert,
		NodeID:         memoryNodeID(record.RecordID),
		NodeType:       "memory_record",
		HierarchyScope: memoryVectorHierarchy(record, opts),
		EmbeddingModel: record.EmbeddingModel,
		Dimensions:     record.EmbeddingDimensions,
		Vector:         vector,
		SourceSeq:      store.Status().DurableSeq,
		Attributes:     memoryVectorAttributes(record, opts),
	})
}

func ensureMemoryNode(ctx context.Context, store *kilo.Store, record MemoryRecord, opts MemoryImportOptions) (bool, error) {
	node := kilo.Node{
		ID:      memoryNodeID(record.RecordID),
		Type:    "memory_record",
		Summary: record.Summary,
		Attributes: mergeAttributes(memoryAttributes(record), map[string]string{
			"record_id":  record.RecordID,
			"project_id": opts.ProjectID,
			"human_id":   opts.HumanID,
		}),
	}
	return ensureNode(ctx, store, node, recordTime(record))
}

func ensureNode(ctx context.Context, store *kilo.Store, node kilo.Node, at time.Time) (bool, error) {
	read, err := store.Read(ctx, kilo.Read{NodeID: node.ID, IncludeDeleted: true})
	if err != nil {
		return false, err
	}
	if read.Found {
		return false, nil
	}
	_, err = store.Write(ctx, kilo.Write{
		IdempotencyKey: "pilot-import:node:" + node.ID,
		CreateNode:     &node,
		At:             at,
	})
	if err != nil {
		return false, err
	}
	return true, nil
}

func ensureSourceSpan(ctx context.Context, store *kilo.Store, record MemoryRecord, opts MemoryImportOptions) (bool, bool, error) {
	chunkID := stableID("chunk:source", record.RecordID)
	spanID := stableID("span:source", record.RecordID)
	chunkCreated := false
	if read, err := store.Read(ctx, kilo.Read{ChunkID: chunkID}); err != nil {
		return false, false, err
	} else if !read.Found {
		_, err := store.Write(ctx, kilo.Write{
			IdempotencyKey: "pilot-import:chunk:" + chunkID,
			CreateChunk: &kilo.ChunkWrite{
				Chunk: kilo.Chunk{
					ID:        chunkID,
					Namespace: opts.Namespace,
					ProjectID: opts.ProjectID,
					SessionID: record.SessionID,
					MissionID: record.MissionID,
					Kind:      "source_excerpt",
					Sequence:  1,
					Attributes: map[string]string{
						"record_id":  record.RecordID,
						"file_path":  record.FilePath,
						"start_line": intString(record.StartLine),
						"end_line":   intString(record.EndLine),
					},
				},
				Content: []byte(record.SourceText),
			},
			At: recordTime(record),
		})
		if err != nil {
			return false, false, err
		}
		chunkCreated = true
	}
	if read, err := store.Read(ctx, kilo.Read{SourceSpanID: spanID}); err != nil {
		return false, false, err
	} else if read.Found {
		return chunkCreated, false, nil
	}
	_, err := store.Write(ctx, kilo.Write{
		IdempotencyKey: "pilot-import:source-span:" + spanID,
		CreateSourceSpan: &kilo.SourceSpan{
			ID:        spanID,
			Namespace: opts.Namespace,
			ProjectID: opts.ProjectID,
			SessionID: record.SessionID,
			MissionID: record.MissionID,
			RecordID:  record.RecordID,
			ChunkRefs: []kilo.SourceSpanChunkRef{{ChunkID: chunkID}},
			Attributes: map[string]string{
				"record_id":  record.RecordID,
				"file_path":  record.FilePath,
				"start_line": intString(record.StartLine),
				"end_line":   intString(record.EndLine),
			},
		},
		At: recordTime(record),
	})
	if err != nil {
		return false, false, err
	}
	return chunkCreated, true, nil
}

func importEdges(ctx context.Context, store *kilo.Store, record MemoryRecord, opts MemoryImportOptions) (int, bool, error) {
	created := 0
	createdOne := func(edge kilo.Edge) error {
		ok, err := ensureEdge(ctx, store, edge, recordTime(record))
		if err != nil {
			return err
		}
		if ok {
			created++
		}
		return nil
	}
	memoryID := memoryNodeID(record.RecordID)
	if record.SessionID != "" && record.MissionID != "" {
		if err := createdOne(edge("contains", sessionNodeID(record.SessionID), missionNodeID(record.MissionID), record.RecordID)); err != nil {
			return 0, false, err
		}
	}
	if record.SessionID != "" && record.MissionID == "" {
		if err := createdOne(edge("contains", sessionNodeID(record.SessionID), memoryID, record.RecordID)); err != nil {
			return 0, false, err
		}
	}
	if record.MissionID != "" {
		if err := createdOne(edge("contains", missionNodeID(record.MissionID), memoryID, record.RecordID)); err != nil {
			return 0, false, err
		}
	}
	if record.SourcePersona != "" {
		if err := createdOne(edge("authored_by", personaNodeID(record.SourcePersona), memoryID, record.RecordID)); err != nil {
			return 0, false, err
		}
	}
	if record.SupersedesRecordID == "" {
		return created, false, nil
	}
	targetID := memoryNodeID(record.SupersedesRecordID)
	target, err := store.Read(ctx, kilo.Read{NodeID: targetID, IncludeDeleted: true})
	if err != nil {
		return 0, false, err
	}
	if !target.Found {
		return created, true, nil
	}
	if err := createdOne(edge("supersedes", memoryID, targetID, record.RecordID)); err != nil {
		return 0, false, err
	}
	return created, false, nil
}

func ensureEdge(ctx context.Context, store *kilo.Store, edge kilo.Edge, at time.Time) (bool, error) {
	read, err := store.Read(ctx, kilo.Read{EdgeID: edge.ID, IncludeDeleted: true})
	if err != nil {
		return false, err
	}
	if read.Found {
		return false, nil
	}
	_, err = store.Write(ctx, kilo.Write{
		IdempotencyKey: "pilot-import:edge:" + edge.ID,
		CreateEdge:     &edge,
		At:             at,
	})
	if err != nil {
		return false, err
	}
	return true, nil
}

func normalizeMemoryImportOptions(opts MemoryImportOptions) MemoryImportOptions {
	opts.Namespace = strings.TrimSpace(opts.Namespace)
	if opts.Namespace == "" {
		opts.Namespace = "sugarfang"
	}
	opts.ProjectID = strings.TrimSpace(opts.ProjectID)
	opts.HumanID = strings.TrimSpace(opts.HumanID)
	return opts
}

func normalizeMemoryRecord(record MemoryRecord) (MemoryRecord, error) {
	record.Schema = strings.TrimSpace(record.Schema)
	if record.Schema != "" && record.Schema != MemoryImportSchema {
		return MemoryRecord{}, fmt.Errorf("unsupported schema %q", record.Schema)
	}
	record.RecordID = strings.TrimSpace(record.RecordID)
	if record.RecordID == "" {
		return MemoryRecord{}, fmt.Errorf("record_id is required")
	}
	record.Kind = strings.TrimSpace(strings.ToLower(record.Kind))
	if record.Kind == "" {
		return MemoryRecord{}, fmt.Errorf("kind is required")
	}
	record.Summary = strings.TrimSpace(record.Summary)
	if record.Summary == "" {
		return MemoryRecord{}, fmt.Errorf("summary is required")
	}
	record.Detail = strings.TrimSpace(record.Detail)
	record.Rationale = strings.TrimSpace(record.Rationale)
	record.Source = strings.TrimSpace(record.Source)
	record.SessionID = strings.TrimSpace(record.SessionID)
	record.FilePath = strings.TrimSpace(record.FilePath)
	record.CreatedAt = strings.TrimSpace(record.CreatedAt)
	record.Disposition = strings.TrimSpace(strings.ToLower(record.Disposition))
	if record.Disposition == "" {
		record.Disposition = "accepted"
	}
	record.ReviewedBy = strings.TrimSpace(record.ReviewedBy)
	record.SupersedesRecordID = strings.TrimSpace(record.SupersedesRecordID)
	record.EmbeddingModel = strings.TrimSpace(record.EmbeddingModel)
	record.MissionID = strings.TrimSpace(record.MissionID)
	record.MissionPhase = strings.TrimSpace(record.MissionPhase)
	record.SourcePersona = strings.TrimSpace(record.SourcePersona)
	record.RetrievalText = strings.TrimSpace(record.RetrievalText)
	record.Tags = normalizeTags(record.Tags)
	if record.Metadata == nil {
		record.Metadata = map[string]any{}
	}
	if record.StartLine != nil && *record.StartLine <= 0 {
		return MemoryRecord{}, fmt.Errorf("start_line must be positive")
	}
	if record.EndLine != nil && *record.EndLine <= 0 {
		return MemoryRecord{}, fmt.Errorf("end_line must be positive")
	}
	if record.StartLine != nil && record.EndLine != nil && *record.EndLine < *record.StartLine {
		return MemoryRecord{}, fmt.Errorf("end_line must be greater than or equal to start_line")
	}
	if record.CreatedAt != "" {
		if _, err := parseTime(record.CreatedAt); err != nil {
			return MemoryRecord{}, err
		}
	}
	if record.EmbeddingDimensions < 0 {
		return MemoryRecord{}, fmt.Errorf("embedding_dimensions must be non-negative")
	}
	if record.EmbeddingDimensions == 0 && len(record.Embedding) > 0 {
		record.EmbeddingDimensions = len(record.Embedding)
	}
	if len(record.Embedding) > 0 {
		if record.EmbeddingModel == "" {
			return MemoryRecord{}, fmt.Errorf("embedding_model is required when embedding is present")
		}
		if record.EmbeddingDimensions != len(record.Embedding) {
			return MemoryRecord{}, fmt.Errorf("embedding_dimensions mismatch: expected %d got %d", record.EmbeddingDimensions, len(record.Embedding))
		}
		if _, err := memoryEmbeddingVector(record); err != nil {
			return MemoryRecord{}, err
		}
	}
	return record, nil
}

func memoryAttributes(record MemoryRecord) map[string]string {
	attributes := map[string]string{
		"schema":               MemoryImportSchema,
		"source":               record.Source,
		"detail":               record.Detail,
		"rationale":            record.Rationale,
		"disposition":          record.Disposition,
		"reviewed_by":          record.ReviewedBy,
		"file_path":            record.FilePath,
		"mission_phase":        record.MissionPhase,
		"source_persona":       record.SourcePersona,
		"supersedes_record_id": record.SupersedesRecordID,
		"embedding_model":      record.EmbeddingModel,
		"retrieval_text":       record.RetrievalText,
	}
	setInt(attributes, "start_line", record.StartLine)
	setInt(attributes, "end_line", record.EndLine)
	setBool(attributes, "qa_approved", record.QAApproved)
	if record.EmbeddingDimensions > 0 {
		attributes["embedding_dimensions"] = strconv.Itoa(record.EmbeddingDimensions)
	}
	if len(record.Tags) > 0 {
		attributes["original_tags_json"] = jsonString(record.Tags)
	}
	if len(record.Metadata) > 0 {
		attributes["metadata_json"] = jsonString(record.Metadata)
	}
	for key, value := range attributes {
		if strings.TrimSpace(value) == "" {
			delete(attributes, key)
		}
	}
	return attributes
}

func memoryEmbeddingVector(record MemoryRecord) ([]float32, error) {
	vector := make([]float32, len(record.Embedding))
	for index, value := range record.Embedding {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, fmt.Errorf("embedding value %d must be finite", index)
		}
		if value > math.MaxFloat32 || value < -math.MaxFloat32 {
			return nil, fmt.Errorf("embedding value %d exceeds float32 range", index)
		}
		vector[index] = float32(value)
	}
	return vector, nil
}

func memoryVectorHierarchy(record MemoryRecord, opts MemoryImportOptions) map[string]string {
	return compactAttributes(map[string]string{
		"namespace":      opts.Namespace,
		"project_id":     opts.ProjectID,
		"human_id":       opts.HumanID,
		"session_id":     record.SessionID,
		"mission_id":     record.MissionID,
		"mission_phase":  record.MissionPhase,
		"source_persona": record.SourcePersona,
		"kind":           record.Kind,
		"disposition":    record.Disposition,
		"record_id":      record.RecordID,
	})
}

func memoryVectorAttributes(record MemoryRecord, opts MemoryImportOptions) map[string]string {
	return compactAttributes(map[string]string{
		"record_id":            record.RecordID,
		"kind":                 record.Kind,
		"source":               record.Source,
		"session_id":           record.SessionID,
		"mission_id":           record.MissionID,
		"mission_phase":        record.MissionPhase,
		"source_persona":       record.SourcePersona,
		"project_id":           opts.ProjectID,
		"human_id":             opts.HumanID,
		"disposition":          record.Disposition,
		"file_path":            record.FilePath,
		"embedding_model":      record.EmbeddingModel,
		"embedding_dimensions": strconv.Itoa(record.EmbeddingDimensions),
	})
}

func sessionNode(id string, record MemoryRecord, opts MemoryImportOptions) kilo.Node {
	return kilo.Node{
		ID:   sessionNodeID(id),
		Type: "session",
		Name: id,
		Attributes: compactAttributes(map[string]string{
			"session_id": id,
			"project_id": opts.ProjectID,
			"human_id":   opts.HumanID,
		}),
	}
}

func missionNode(id string, record MemoryRecord, opts MemoryImportOptions) kilo.Node {
	return kilo.Node{
		ID:   missionNodeID(id),
		Type: "mission",
		Name: id,
		Attributes: compactAttributes(map[string]string{
			"mission_id":    id,
			"mission_phase": record.MissionPhase,
			"session_id":    record.SessionID,
			"project_id":    opts.ProjectID,
			"human_id":      opts.HumanID,
		}),
	}
}

func personaNode(id string, record MemoryRecord, opts MemoryImportOptions) kilo.Node {
	return kilo.Node{
		ID:   personaNodeID(id),
		Type: "persona",
		Name: id,
		Attributes: compactAttributes(map[string]string{
			"source_persona": id,
			"project_id":     opts.ProjectID,
			"human_id":       opts.HumanID,
		}),
	}
}

func edge(edgeType, from, to, seed string) kilo.Edge {
	return kilo.Edge{
		ID:         stableID("edge:"+edgeType, from, to),
		Type:       edgeType,
		FromNodeID: from,
		ToNodeID:   to,
		Attributes: map[string]string{
			"import_seed": seed,
		},
	}
}

func memoryNodeID(id string) string {
	return "node:memory:" + id
}

func sessionNodeID(id string) string {
	return "node:session:" + id
}

func missionNodeID(id string) string {
	return "node:mission:" + id
}

func personaNodeID(id string) string {
	return "node:persona:" + id
}

func stableID(prefix string, parts ...string) string {
	hash := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return prefix + ":" + hex.EncodeToString(hash[:8])
}

func recordTime(record MemoryRecord) time.Time {
	if record.CreatedAt == "" {
		return time.Time{}
	}
	parsed, err := parseTime(record.CreatedAt)
	if err != nil {
		return time.Time{}
	}
	return parsed
}

func parseTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("created_at must be RFC3339: %w", err)
	}
	return parsed.UTC(), nil
}

func normalizeTags(tags []string) []string {
	out := make([]string, 0, len(tags))
	seen := map[string]struct{}{}
	for _, tag := range tags {
		clean := strings.TrimSpace(tag)
		if clean == "" {
			continue
		}
		if _, ok := seen[clean]; ok {
			continue
		}
		seen[clean] = struct{}{}
		out = append(out, clean)
	}
	return out
}

func mergeAttributes(left, right map[string]string) map[string]string {
	merged := map[string]string{}
	for key, value := range left {
		merged[key] = value
	}
	for key, value := range right {
		merged[key] = value
	}
	return compactAttributes(merged)
}

func compactAttributes(attributes map[string]string) map[string]string {
	for key, value := range attributes {
		if strings.TrimSpace(value) == "" {
			delete(attributes, key)
		}
	}
	return attributes
}

func setInt(attributes map[string]string, key string, value *int) {
	if value != nil {
		attributes[key] = strconv.Itoa(*value)
	}
}

func setBool(attributes map[string]string, key string, value *bool) {
	if value != nil {
		attributes[key] = strconv.FormatBool(*value)
	}
}

func intString(value *int) string {
	if value == nil {
		return ""
	}
	return strconv.Itoa(*value)
}

func jsonString(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%v", value)
	}
	return string(encoded)
}
