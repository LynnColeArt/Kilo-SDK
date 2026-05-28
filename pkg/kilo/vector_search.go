package kilo

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
)

type VectorSearcher interface {
	SearchVectors(context.Context, VectorSearchQuery) (VectorSearchResult, error)
}

type VectorSearchQuery struct {
	Projector                 string            `json:"projector,omitempty"`
	Vector                    []float32         `json:"vector"`
	NodeTypes                 []string          `json:"node_types,omitempty"`
	HierarchyScope            map[string]string `json:"hierarchy_scope,omitempty"`
	EmbeddingModel            string            `json:"embedding_model,omitempty"`
	Dimensions                int               `json:"dimensions,omitempty"`
	MinIndexedSeq             uint64            `json:"min_index_seq,omitempty"`
	MaxLag                    uint64            `json:"max_lag,omitempty"`
	EnforceMaxLag             bool              `json:"enforce_max_lag,omitempty"`
	IncludeCandidates         bool              `json:"include_candidates,omitempty"`
	Limit                     int               `json:"limit,omitempty"`
	ExpandParents             bool              `json:"expand_parents,omitempty"`
	ParentEdgeType            string            `json:"parent_edge_type,omitempty"`
	ExpandChildren            bool              `json:"expand_children,omitempty"`
	ChildEdgeType             string            `json:"child_edge_type,omitempty"`
	ExpandEvidence            bool              `json:"expand_evidence,omitempty"`
	EvidenceEdgeType          string            `json:"evidence_edge_type,omitempty"`
	ExpandSourceSpans         bool              `json:"expand_source_spans,omitempty"`
	SourceSpanRecordAttribute string            `json:"source_span_record_attribute,omitempty"`
}

type VectorSearchHit struct {
	NodeID         string            `json:"node_id"`
	NodeType       string            `json:"node_type"`
	Score          float64           `json:"score"`
	HierarchyScope map[string]string `json:"hierarchy_scope,omitempty"`
	EmbeddingModel string            `json:"embedding_model,omitempty"`
	Dimensions     int               `json:"dimensions,omitempty"`
	SourceSeq      uint64            `json:"source_seq"`
	Attributes     map[string]string `json:"attributes,omitempty"`
}

type VectorSearchResult struct {
	Hits          []VectorSearchHit `json:"hits"`
	Expanded      GraphTraversal    `json:"expanded,omitempty"`
	SourceSpans   []SourceSpanRead  `json:"source_spans,omitempty"`
	CandidateIDs  []string          `json:"candidate_ids,omitempty"`
	Projector     ProjectorStatus   `json:"projector,omitempty"`
	IndexedSeq    uint64            `json:"indexed_seq"`
	DurableSeq    uint64            `json:"durable_seq,omitempty"`
	Lag           uint64            `json:"lag,omitempty"`
	MinIndexedSeq uint64            `json:"min_index_seq,omitempty"`
	MaxLag        uint64            `json:"max_lag,omitempty"`
	EnforceMaxLag bool              `json:"enforce_max_lag,omitempty"`
	Stale         bool              `json:"stale"`
}

type ExactVectorIndex struct {
	mu         sync.RWMutex
	entries    map[string]exactVectorEntry
	indexedSeq uint64
}

type exactVectorEntry struct {
	nodeID         string
	nodeType       string
	hierarchyScope map[string]string
	embeddingModel string
	dimensions     int
	vector         []float32
	magnitude      float64
	sourceSeq      uint64
	attributes     map[string]string
}

func NewExactVectorIndex() *ExactVectorIndex {
	return &ExactVectorIndex{
		entries: map[string]exactVectorEntry{},
	}
}

func (i *ExactVectorIndex) ApplyVectorOperation(ctx context.Context, op VectorOperation) error {
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
		i.entries[normalized.NodeID] = exactVectorEntry{
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
	case VectorOperationDelete:
		delete(i.entries, normalized.NodeID)
	default:
		return fmt.Errorf("unsupported vector operation type %q", normalized.Type)
	}
	if normalized.SourceSeq > i.indexedSeq {
		i.indexedSeq = normalized.SourceSeq
	}
	return nil
}

func (i *ExactVectorIndex) SearchVectors(ctx context.Context, query VectorSearchQuery) (VectorSearchResult, error) {
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

func (s *Store) SearchVectors(ctx context.Context, searcher VectorSearcher, query VectorSearchQuery) (VectorSearchResult, error) {
	if err := ctx.Err(); err != nil {
		return VectorSearchResult{}, err
	}
	if searcher == nil {
		return VectorSearchResult{}, fmt.Errorf("vector searcher is required")
	}
	normalized, err := normalizeVectorSearchQuery(query)
	if err != nil {
		return VectorSearchResult{}, err
	}
	result, err := searcher.SearchVectors(ctx, normalized)
	if err != nil {
		return VectorSearchResult{}, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if normalized.Projector != "" {
		status, ok := s.projectorStatusLocked(normalized.Projector)
		if !ok {
			return VectorSearchResult{}, fmt.Errorf("%w: projector %q", ErrNotFound, normalized.Projector)
		}
		result.Projector = status
		result.IndexedSeq = status.IndexedSeq
		result.DurableSeq = status.DurableSeq
		result.Lag = status.Lag
		result.Stale = !projectionWaitSatisfied(ProjectionWait{
			Projector:     normalized.Projector,
			MinIndexedSeq: normalized.MinIndexedSeq,
			MaxLag:        normalized.MaxLag,
			EnforceMaxLag: normalized.EnforceMaxLag,
		}, status)
	}

	expansionQuery := vectorSearchExpansionQuery(normalized)
	matched := s.vectorSearchMatchedNodesLocked(result.Hits)
	result.Expanded = s.expandQueryResultLocked(matched, expansionQuery)
	if normalized.ExpandSourceSpans {
		result.SourceSpans, err = s.expandSourceSpansLocked(matched, result.Expanded.Nodes, expansionQuery)
		if err != nil {
			return VectorSearchResult{}, err
		}
	}
	return result, nil
}

func normalizeVectorSearchQuery(query VectorSearchQuery) (VectorSearchQuery, error) {
	query.Projector = normalizeProjectorName(query.Projector)
	query.Vector = cloneFloat32Slice(query.Vector)
	query.NodeTypes = normalizeStringSlice(query.NodeTypes)
	query.HierarchyScope = normalizeStringMap(query.HierarchyScope)
	query.EmbeddingModel = strings.TrimSpace(query.EmbeddingModel)
	query.ParentEdgeType = strings.TrimSpace(query.ParentEdgeType)
	query.ChildEdgeType = strings.TrimSpace(query.ChildEdgeType)
	query.EvidenceEdgeType = strings.TrimSpace(query.EvidenceEdgeType)
	query.SourceSpanRecordAttribute = strings.TrimSpace(query.SourceSpanRecordAttribute)
	if query.SourceSpanRecordAttribute == "" {
		query.SourceSpanRecordAttribute = DefaultSourceSpanRecordAttribute
	}
	if len(query.Vector) == 0 {
		return VectorSearchQuery{}, fmt.Errorf("vector search query vector is required")
	}
	if query.Dimensions < 0 {
		return VectorSearchQuery{}, fmt.Errorf("vector search dimensions must be non-negative")
	}
	if query.Dimensions == 0 {
		query.Dimensions = len(query.Vector)
	}
	if query.Dimensions != len(query.Vector) {
		return VectorSearchQuery{}, fmt.Errorf("vector search dimensions mismatch: expected %d got %d", query.Dimensions, len(query.Vector))
	}
	if query.Limit < 0 {
		return VectorSearchQuery{}, fmt.Errorf("vector search limit must be non-negative")
	}
	return query, nil
}

func normalizeStringSlice(values []string) []string {
	seen := map[string]struct{}{}
	normalized := make([]string, 0, len(values))
	for _, value := range values {
		clean := strings.TrimSpace(value)
		if clean == "" {
			continue
		}
		if _, ok := seen[clean]; ok {
			continue
		}
		seen[clean] = struct{}{}
		normalized = append(normalized, clean)
	}
	return normalized
}

func exactVectorEntryMatchesQuery(entry exactVectorEntry, query VectorSearchQuery) bool {
	if query.EmbeddingModel != "" && entry.embeddingModel != query.EmbeddingModel {
		return false
	}
	if query.Dimensions != 0 && entry.dimensions != query.Dimensions {
		return false
	}
	if len(query.NodeTypes) > 0 && !stringInSet(entry.nodeType, query.NodeTypes) {
		return false
	}
	for key, value := range query.HierarchyScope {
		if entry.hierarchyScope[key] != value {
			return false
		}
	}
	return true
}

func stringInSet(value string, values []string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func exactVectorCandidateIDs(candidates []exactVectorEntry) []string {
	ids := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		ids = append(ids, candidate.nodeID)
	}
	sort.Strings(ids)
	return ids
}

func sortVectorSearchHits(hits []VectorSearchHit) {
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Score == hits[j].Score {
			if hits[i].SourceSeq == hits[j].SourceSeq {
				return hits[i].NodeID < hits[j].NodeID
			}
			return hits[i].SourceSeq > hits[j].SourceSeq
		}
		return hits[i].Score > hits[j].Score
	})
}

func cloneExactVectorEntry(entry exactVectorEntry) exactVectorEntry {
	entry.hierarchyScope = cloneStringMap(entry.hierarchyScope)
	entry.vector = cloneFloat32Slice(entry.vector)
	entry.attributes = cloneStringMap(entry.attributes)
	return entry
}

func vectorMagnitude(vector []float32) float64 {
	var sum float64
	for _, value := range vector {
		sum += float64(value) * float64(value)
	}
	return math.Sqrt(sum)
}

func cosineSimilarity(left []float32, leftMagnitude float64, right []float32, rightMagnitude float64) float64 {
	if leftMagnitude == 0 || rightMagnitude == 0 || len(left) != len(right) {
		return 0
	}
	var dot float64
	for i := range left {
		dot += float64(left[i]) * float64(right[i])
	}
	return dot / (leftMagnitude * rightMagnitude)
}

func vectorSearchExpansionQuery(query VectorSearchQuery) NodeQuery {
	return NodeQuery{
		ExpandParents:             query.ExpandParents,
		ParentEdgeType:            query.ParentEdgeType,
		ExpandChildren:            query.ExpandChildren,
		ChildEdgeType:             query.ChildEdgeType,
		ExpandEvidence:            query.ExpandEvidence,
		EvidenceEdgeType:          query.EvidenceEdgeType,
		ExpandSourceSpans:         query.ExpandSourceSpans,
		SourceSpanRecordAttribute: query.SourceSpanRecordAttribute,
	}
}

func (s *Store) vectorSearchMatchedNodesLocked(hits []VectorSearchHit) []Node {
	nodes := make([]Node, 0, len(hits))
	for _, hit := range hits {
		node, ok := s.nodes[hit.NodeID]
		if !ok || node.Deleted {
			continue
		}
		nodes = append(nodes, cloneNode(node))
	}
	return nodes
}
