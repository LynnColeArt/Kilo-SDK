package pilotvalidate

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/LynnColeArt/Kilo-SDK/internal/pilotimport"
	"github.com/LynnColeArt/Kilo-SDK/pkg/kilo"
)

const PlanSchema = "kilo.memory_validation/v1"

const (
	maxValidationCases              = 10000
	maxValidationParallelSessions   = 1024
	maxValidationRepeats            = 100000
	maxValidationPerformanceQueries = 1000000
)

type Plan struct {
	Schema           string            `json:"schema,omitempty"`
	Cases            []Case            `json:"cases"`
	ParallelSessions int               `json:"parallel_sessions,omitempty"`
	Repeats          int               `json:"repeats,omitempty"`
	Limit            int               `json:"limit,omitempty"`
	RequireFresh     bool              `json:"require_fresh,omitempty"`
	Metadata         map[string]string `json:"metadata,omitempty"`
}

type Case struct {
	Name              string            `json:"name"`
	Query             string            `json:"query,omitempty"`
	QueryVector       []float64         `json:"query_vector"`
	NodeTypes         []string          `json:"node_types,omitempty"`
	HierarchyScope    map[string]string `json:"hierarchy_scope,omitempty"`
	EmbeddingModel    string            `json:"embedding_model,omitempty"`
	ExpectedNodeIDs   []string          `json:"expected_node_ids,omitempty"`
	ExpectedRecordIDs []string          `json:"expected_record_ids,omitempty"`
	Limit             int               `json:"limit,omitempty"`
	MinIndexedSeq     uint64            `json:"min_index_seq,omitempty"`
	RequireFresh      bool              `json:"require_fresh,omitempty"`
	ExpandParents     bool              `json:"expand_parents,omitempty"`
	ParentEdgeType    string            `json:"parent_edge_type,omitempty"`
	ExpandChildren    bool              `json:"expand_children,omitempty"`
	ChildEdgeType     string            `json:"child_edge_type,omitempty"`
	ExpandEvidence    bool              `json:"expand_evidence,omitempty"`
	EvidenceEdgeType  string            `json:"evidence_edge_type,omitempty"`
	ExpandSourceSpans bool              `json:"expand_source_spans,omitempty"`
}

type Report struct {
	Schema      string                         `json:"schema"`
	Imported    pilotimport.MemoryImportResult `json:"imported,omitempty"`
	Status      kilo.Status                    `json:"status"`
	Summary     Summary                        `json:"summary"`
	Cases       []CaseReport                   `json:"cases"`
	Performance PerformanceReport              `json:"performance,omitempty"`
	Metadata    map[string]string              `json:"metadata,omitempty"`
}

type Summary struct {
	Cases                int `json:"cases"`
	Passed               int `json:"passed"`
	Failed               int `json:"failed"`
	Improved             int `json:"improved"`
	ExpectedKiloHits     int `json:"expected_kilo_hits"`
	ExpectedBaselineHits int `json:"expected_baseline_hits"`
}

type CaseReport struct {
	Name                   string                `json:"name"`
	Query                  string                `json:"query,omitempty"`
	Passed                 bool                  `json:"passed"`
	Improved               bool                  `json:"improved,omitempty"`
	ExpectedNodeIDs        []string              `json:"expected_node_ids"`
	BaselineHitIDs         []string              `json:"baseline_hit_ids"`
	KiloHitIDs             []string              `json:"kilo_hit_ids"`
	MissingNodeIDs         []string              `json:"missing_node_ids,omitempty"`
	ExpectedBaselineHits   int                   `json:"expected_baseline_hits"`
	ExpectedKiloHits       int                   `json:"expected_kilo_hits"`
	BaselineDurationMillis float64               `json:"baseline_duration_ms"`
	KiloDurationMillis     float64               `json:"kilo_duration_ms"`
	IndexedSeq             uint64                `json:"indexed_seq"`
	DurableSeq             uint64                `json:"durable_seq"`
	Lag                    uint64                `json:"lag,omitempty"`
	MinIndexedSeq          uint64                `json:"min_index_seq,omitempty"`
	Stale                  bool                  `json:"stale"`
	RequireFresh           bool                  `json:"require_fresh,omitempty"`
	Expanded               kilo.GraphTraversal   `json:"expanded,omitempty"`
	SourceSpans            []kilo.SourceSpanRead `json:"source_spans,omitempty"`
	Error                  string                `json:"error,omitempty"`
}

type PerformanceReport struct {
	ParallelSessions int     `json:"parallel_sessions,omitempty"`
	Repeats          int     `json:"repeats,omitempty"`
	Queries          int     `json:"queries,omitempty"`
	DurationMillis   float64 `json:"duration_ms,omitempty"`
	QueriesPerSecond float64 `json:"queries_per_second,omitempty"`
	P50Millis        float64 `json:"p50_ms,omitempty"`
	P95Millis        float64 `json:"p95_ms,omitempty"`
	MaxMillis        float64 `json:"max_ms,omitempty"`
}

func LoadPlan(r io.Reader) (Plan, error) {
	var plan Plan
	if err := json.NewDecoder(r).Decode(&plan); err != nil {
		return Plan{}, fmt.Errorf("decode validation plan: %w", err)
	}
	return normalizePlan(plan)
}

func Validate(ctx context.Context, store *kilo.Store, searcher kilo.VectorSearcher, plan Plan, imported pilotimport.MemoryImportResult) (Report, error) {
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	if store == nil {
		return Report{}, fmt.Errorf("store is required")
	}
	if searcher == nil {
		return Report{}, fmt.Errorf("vector searcher is required")
	}
	normalized, err := normalizePlan(plan)
	if err != nil {
		return Report{}, err
	}

	report := Report{
		Schema:   PlanSchema,
		Imported: imported,
		Status:   store.Status(),
		Cases:    make([]CaseReport, 0, len(normalized.Cases)),
		Metadata: cloneStringMap(normalized.Metadata),
	}
	for _, item := range normalized.Cases {
		caseReport := runCase(ctx, store, searcher, normalized, item)
		report.Cases = append(report.Cases, caseReport)
		report.Summary.Cases++
		report.Summary.ExpectedKiloHits += caseReport.ExpectedKiloHits
		report.Summary.ExpectedBaselineHits += caseReport.ExpectedBaselineHits
		if caseReport.Improved {
			report.Summary.Improved++
		}
		if caseReport.Passed {
			report.Summary.Passed++
		} else {
			report.Summary.Failed++
		}
	}
	performance, err := measurePerformance(ctx, store, searcher, normalized)
	if err != nil {
		return Report{}, err
	}
	report.Performance = performance
	return report, nil
}

func normalizePlan(plan Plan) (Plan, error) {
	plan.Schema = strings.TrimSpace(plan.Schema)
	if plan.Schema != "" && plan.Schema != PlanSchema {
		return Plan{}, fmt.Errorf("unsupported validation plan schema %q", plan.Schema)
	}
	if len(plan.Cases) == 0 {
		return Plan{}, fmt.Errorf("validation plan requires at least one case")
	}
	if len(plan.Cases) > maxValidationCases {
		return Plan{}, fmt.Errorf("validation plan has %d cases; maximum is %d", len(plan.Cases), maxValidationCases)
	}
	if plan.Limit < 0 {
		return Plan{}, fmt.Errorf("validation plan limit must be non-negative")
	}
	if plan.ParallelSessions < 0 {
		return Plan{}, fmt.Errorf("parallel_sessions must be non-negative")
	}
	if plan.ParallelSessions > maxValidationParallelSessions {
		return Plan{}, fmt.Errorf("parallel_sessions must be <= %d", maxValidationParallelSessions)
	}
	if plan.Repeats < 0 {
		return Plan{}, fmt.Errorf("repeats must be non-negative")
	}
	if plan.Repeats > maxValidationRepeats {
		return Plan{}, fmt.Errorf("repeats must be <= %d", maxValidationRepeats)
	}
	if _, err := performanceQueryCount(plan); err != nil {
		return Plan{}, err
	}
	plan.Metadata = normalizeStringMap(plan.Metadata)
	for index, item := range plan.Cases {
		normalized, err := normalizeCase(item, plan)
		if err != nil {
			return Plan{}, fmt.Errorf("validation case %d: %w", index+1, err)
		}
		plan.Cases[index] = normalized
	}
	return plan, nil
}

func normalizeCase(item Case, plan Plan) (Case, error) {
	item.Name = strings.TrimSpace(item.Name)
	if item.Name == "" {
		return Case{}, fmt.Errorf("name is required")
	}
	item.Query = strings.TrimSpace(item.Query)
	item.NodeTypes = normalizeStringSlice(item.NodeTypes)
	if len(item.NodeTypes) == 0 {
		item.NodeTypes = []string{"memory_record"}
	}
	item.HierarchyScope = normalizeStringMap(item.HierarchyScope)
	item.EmbeddingModel = strings.TrimSpace(item.EmbeddingModel)
	item.ExpectedNodeIDs = normalizeStringSlice(item.ExpectedNodeIDs)
	item.ExpectedRecordIDs = normalizeStringSlice(item.ExpectedRecordIDs)
	for _, id := range item.ExpectedRecordIDs {
		item.ExpectedNodeIDs = append(item.ExpectedNodeIDs, "node:memory:"+id)
	}
	item.ExpectedNodeIDs = normalizeStringSlice(item.ExpectedNodeIDs)
	if len(item.ExpectedNodeIDs) == 0 {
		return Case{}, fmt.Errorf("expected_node_ids or expected_record_ids is required")
	}
	if len(item.QueryVector) == 0 {
		return Case{}, fmt.Errorf("query_vector is required")
	}
	if item.Limit < 0 {
		return Case{}, fmt.Errorf("limit must be non-negative")
	}
	item.ParentEdgeType = strings.TrimSpace(item.ParentEdgeType)
	item.ChildEdgeType = strings.TrimSpace(item.ChildEdgeType)
	item.EvidenceEdgeType = strings.TrimSpace(item.EvidenceEdgeType)
	if _, err := float32Vector(item.QueryVector); err != nil {
		return Case{}, err
	}
	return item, nil
}

func runCase(ctx context.Context, store *kilo.Store, searcher kilo.VectorSearcher, plan Plan, item Case) CaseReport {
	report := CaseReport{
		Name:            item.Name,
		Query:           item.Query,
		ExpectedNodeIDs: append([]string{}, item.ExpectedNodeIDs...),
		RequireFresh:    plan.RequireFresh || item.RequireFresh,
	}
	vector, err := float32Vector(item.QueryVector)
	if err != nil {
		report.Error = err.Error()
		return report
	}

	baselineQuery := vectorQuery(item, vector, false, plan.Limit)
	kiloQuery := vectorQuery(item, vector, true, plan.Limit)
	if report.RequireFresh && kiloQuery.MinIndexedSeq == 0 {
		kiloQuery.MinIndexedSeq = store.Status().DurableSeq
	}

	started := time.Now()
	baseline, err := store.Search(ctx, kilo.Search{
		VectorSearcher: searcher,
		Vectors:        &baselineQuery,
	})
	report.BaselineDurationMillis = durationMillis(time.Since(started))
	if err != nil {
		report.Error = err.Error()
		return report
	}

	started = time.Now()
	kiloResult, err := store.Search(ctx, kilo.Search{
		VectorSearcher: searcher,
		Vectors:        &kiloQuery,
	})
	report.KiloDurationMillis = durationMillis(time.Since(started))
	if err != nil {
		report.Error = err.Error()
		return report
	}

	report.BaselineHitIDs = vectorHitIDs(baseline.Vectors.Hits)
	report.KiloHitIDs = vectorHitIDs(kiloResult.Vectors.Hits)
	report.ExpectedBaselineHits = countExpectedHits(report.BaselineHitIDs, item.ExpectedNodeIDs)
	report.ExpectedKiloHits = countExpectedHits(report.KiloHitIDs, item.ExpectedNodeIDs)
	report.MissingNodeIDs = missingIDs(report.KiloHitIDs, item.ExpectedNodeIDs)
	report.Improved = report.ExpectedKiloHits > report.ExpectedBaselineHits
	report.IndexedSeq = kiloResult.Vectors.IndexedSeq
	report.DurableSeq = kiloResult.Vectors.DurableSeq
	if report.DurableSeq == 0 {
		report.DurableSeq = store.Status().DurableSeq
	}
	report.Lag = lag(report.DurableSeq, report.IndexedSeq)
	report.MinIndexedSeq = kiloResult.Vectors.MinIndexedSeq
	report.Stale = kiloResult.Vectors.Stale
	report.Expanded = kiloResult.Vectors.Expanded
	report.SourceSpans = kiloResult.Vectors.SourceSpans
	report.Passed = len(report.MissingNodeIDs) == 0 && (!report.RequireFresh || !report.Stale)
	return report
}

func vectorQuery(item Case, vector []float32, includeHierarchy bool, defaultLimit int) kilo.VectorSearchQuery {
	limit := item.Limit
	if limit == 0 {
		limit = defaultLimit
	}
	query := kilo.VectorSearchQuery{
		Vector:            vector,
		NodeTypes:         item.NodeTypes,
		EmbeddingModel:    item.EmbeddingModel,
		Limit:             limit,
		MinIndexedSeq:     item.MinIndexedSeq,
		ExpandParents:     item.ExpandParents,
		ParentEdgeType:    item.ParentEdgeType,
		ExpandChildren:    item.ExpandChildren,
		ChildEdgeType:     item.ChildEdgeType,
		ExpandEvidence:    item.ExpandEvidence,
		EvidenceEdgeType:  item.EvidenceEdgeType,
		ExpandSourceSpans: item.ExpandSourceSpans,
	}
	if includeHierarchy {
		query.HierarchyScope = item.HierarchyScope
	}
	return query
}

func measurePerformance(ctx context.Context, store *kilo.Store, searcher kilo.VectorSearcher, plan Plan) (PerformanceReport, error) {
	if plan.ParallelSessions == 0 || plan.Repeats == 0 {
		return PerformanceReport{}, nil
	}
	totalQueries, err := performanceQueryCount(plan)
	if err != nil {
		return PerformanceReport{}, err
	}
	durations := make(chan time.Duration, totalQueries)
	errors := make(chan error, totalQueries)
	var wg sync.WaitGroup
	started := time.Now()
	for session := 0; session < plan.ParallelSessions; session++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for repeat := 0; repeat < plan.Repeats; repeat++ {
				for _, item := range plan.Cases {
					vector, err := float32Vector(item.QueryVector)
					if err != nil {
						errors <- err
						return
					}
					query := vectorQuery(item, vector, true, plan.Limit)
					query.IncludeCandidates = false
					requireFresh := plan.RequireFresh || item.RequireFresh
					if requireFresh {
						query.MinIndexedSeq = store.Status().DurableSeq
					}
					before := time.Now()
					_, err = store.Search(ctx, kilo.Search{
						VectorSearcher: searcher,
						Vectors:        &query,
					})
					durations <- time.Since(before)
					if err != nil {
						errors <- err
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	close(durations)
	close(errors)
	for err := range errors {
		if err != nil {
			return PerformanceReport{}, err
		}
	}
	values := make([]time.Duration, 0, totalQueries)
	for duration := range durations {
		values = append(values, duration)
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	elapsed := time.Since(started)
	report := PerformanceReport{
		ParallelSessions: plan.ParallelSessions,
		Repeats:          plan.Repeats,
		Queries:          len(values),
		DurationMillis:   durationMillis(elapsed),
	}
	if elapsed > 0 {
		report.QueriesPerSecond = float64(len(values)) / elapsed.Seconds()
	}
	if len(values) > 0 {
		report.P50Millis = durationMillis(percentile(values, 0.50))
		report.P95Millis = durationMillis(percentile(values, 0.95))
		report.MaxMillis = durationMillis(values[len(values)-1])
	}
	return report, nil
}

func performanceQueryCount(plan Plan) (int, error) {
	if plan.ParallelSessions == 0 || plan.Repeats == 0 {
		return 0, nil
	}
	if len(plan.Cases) > maxValidationPerformanceQueries/plan.ParallelSessions/plan.Repeats {
		return 0, fmt.Errorf("validation performance run would exceed %d queries", maxValidationPerformanceQueries)
	}
	return plan.ParallelSessions * plan.Repeats * len(plan.Cases), nil
}

func float32Vector(values []float64) ([]float32, error) {
	out := make([]float32, len(values))
	for index, value := range values {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, fmt.Errorf("query_vector value %d must be finite", index)
		}
		if value > math.MaxFloat32 || value < -math.MaxFloat32 {
			return nil, fmt.Errorf("query_vector value %d exceeds float32 range", index)
		}
		out[index] = float32(value)
	}
	return out, nil
}

func vectorHitIDs(hits []kilo.VectorSearchHit) []string {
	ids := make([]string, 0, len(hits))
	for _, hit := range hits {
		ids = append(ids, hit.NodeID)
	}
	return ids
}

func countExpectedHits(hits, expected []string) int {
	hitSet := map[string]struct{}{}
	for _, id := range hits {
		hitSet[id] = struct{}{}
	}
	count := 0
	for _, id := range expected {
		if _, ok := hitSet[id]; ok {
			count++
		}
	}
	return count
}

func missingIDs(hits, expected []string) []string {
	hitSet := map[string]struct{}{}
	for _, id := range hits {
		hitSet[id] = struct{}{}
	}
	missing := []string{}
	for _, id := range expected {
		if _, ok := hitSet[id]; !ok {
			missing = append(missing, id)
		}
	}
	return missing
}

func percentile(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	index := int(math.Ceil(float64(len(values))*p)) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index]
}

func durationMillis(duration time.Duration) float64 {
	return float64(duration) / float64(time.Millisecond)
}

func lag(durableSeq, indexedSeq uint64) uint64 {
	if durableSeq <= indexedSeq {
		return 0
	}
	return durableSeq - indexedSeq
}

func normalizeStringSlice(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		clean := strings.TrimSpace(value)
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

func normalizeStringMap(values map[string]string) map[string]string {
	out := map[string]string{}
	for key, value := range values {
		cleanKey := strings.TrimSpace(key)
		cleanValue := strings.TrimSpace(value)
		if cleanKey == "" || cleanValue == "" {
			continue
		}
		out[cleanKey] = cleanValue
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}
