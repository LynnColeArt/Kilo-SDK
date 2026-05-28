package kilo

import (
	"context"
	"fmt"
	"strings"
)

type VectorOperationType string

const (
	VectorOperationUpsert VectorOperationType = "vector.upsert"
	VectorOperationDelete VectorOperationType = "vector.delete"
)

type VectorOperation struct {
	Type           VectorOperationType `json:"type"`
	NodeID         string              `json:"node_id"`
	NodeType       string              `json:"node_type,omitempty"`
	HierarchyScope map[string]string   `json:"hierarchy_scope,omitempty"`
	EmbeddingModel string              `json:"embedding_model,omitempty"`
	Dimensions     int                 `json:"dimensions,omitempty"`
	Vector         []float32           `json:"vector,omitempty"`
	SourceSeq      uint64              `json:"source_seq"`
	Attributes     map[string]string   `json:"attributes,omitempty"`
}

type VectorIndex interface {
	ApplyVectorOperation(context.Context, VectorOperation) error
}

type VectorOperationMapper interface {
	MapVectorOperation(context.Context, ProjectionEvent) (VectorOperation, bool, error)
}

type VectorOperationMapperFunc func(context.Context, ProjectionEvent) (VectorOperation, bool, error)

func (f VectorOperationMapperFunc) MapVectorOperation(ctx context.Context, event ProjectionEvent) (VectorOperation, bool, error) {
	return f(ctx, event)
}

type VectorProjectorOptions struct {
	Name   string
	Index  VectorIndex
	Mapper VectorOperationMapper
}

type vectorProjector struct {
	name   string
	index  VectorIndex
	mapper VectorOperationMapper
}

func NewVectorProjector(opts VectorProjectorOptions) (Projector, error) {
	name := normalizeProjectorName(opts.Name)
	if name == "" {
		return nil, fmt.Errorf("vector projector name is required")
	}
	if opts.Index == nil {
		return nil, fmt.Errorf("vector index is required")
	}
	if opts.Mapper == nil {
		return nil, fmt.Errorf("vector operation mapper is required")
	}
	return &vectorProjector{
		name:   name,
		index:  opts.Index,
		mapper: opts.Mapper,
	}, nil
}

func (p *vectorProjector) Name() string {
	return p.name
}

func (p *vectorProjector) Project(ctx context.Context, event ProjectionEvent) error {
	op, ok, err := p.mapper.MapVectorOperation(ctx, event)
	if err != nil || !ok {
		return err
	}
	normalized, err := normalizeVectorOperation(op, event.Seq)
	if err != nil {
		return err
	}
	return p.index.ApplyVectorOperation(ctx, normalized)
}

func normalizeVectorOperation(op VectorOperation, fallbackSeq uint64) (VectorOperation, error) {
	op.Type = VectorOperationType(strings.TrimSpace(string(op.Type)))
	op.NodeID = strings.TrimSpace(op.NodeID)
	op.NodeType = strings.TrimSpace(op.NodeType)
	op.EmbeddingModel = strings.TrimSpace(op.EmbeddingModel)
	op.HierarchyScope = normalizeStringMap(op.HierarchyScope)
	op.Attributes = normalizeStringMap(op.Attributes)
	op.Vector = cloneFloat32Slice(op.Vector)
	if op.SourceSeq == 0 {
		op.SourceSeq = fallbackSeq
	}

	switch op.Type {
	case VectorOperationUpsert:
		if op.NodeID == "" {
			return VectorOperation{}, fmt.Errorf("vector upsert node id is required")
		}
		if op.NodeType == "" {
			return VectorOperation{}, fmt.Errorf("vector upsert node type is required")
		}
		if op.EmbeddingModel == "" {
			return VectorOperation{}, fmt.Errorf("vector upsert embedding model is required")
		}
		if len(op.Vector) == 0 {
			return VectorOperation{}, fmt.Errorf("vector upsert embedding vector is required")
		}
		if op.Dimensions == 0 {
			op.Dimensions = len(op.Vector)
		}
		if op.Dimensions != len(op.Vector) {
			return VectorOperation{}, fmt.Errorf("vector upsert dimensions mismatch: expected %d got %d", op.Dimensions, len(op.Vector))
		}
	case VectorOperationDelete:
		if op.NodeID == "" {
			return VectorOperation{}, fmt.Errorf("vector delete node id is required")
		}
		op.Vector = nil
		if op.Dimensions < 0 {
			return VectorOperation{}, fmt.Errorf("vector delete dimensions must be non-negative")
		}
	default:
		return VectorOperation{}, fmt.Errorf("unsupported vector operation type %q", op.Type)
	}
	if op.SourceSeq == 0 {
		return VectorOperation{}, fmt.Errorf("vector operation source sequence is required")
	}
	return op, nil
}

func cloneFloat32Slice(values []float32) []float32 {
	if values == nil {
		return nil
	}
	out := make([]float32, len(values))
	copy(out, values)
	return out
}
