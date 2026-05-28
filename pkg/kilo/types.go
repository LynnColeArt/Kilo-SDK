package kilo

import (
	"context"
	"errors"
	"time"
)

var (
	ErrConflict        = errors.New("kilo conflict")
	ErrNotFound        = errors.New("kilo not found")
	ErrCorrupt         = errors.New("kilo corrupt data")
	ErrStaleProjection = errors.New("kilo stale projection")
	ErrStoreLocked     = errors.New("kilo store locked")
)

const DefaultSourceSpanRecordAttribute = "record_id"

type Clock interface {
	Now() time.Time
}

type Options struct {
	Path       string
	SyncWrites bool
	Clock      Clock
}

type MutationType string

const (
	MutationCreateRecord       MutationType = "record.create"
	MutationUpdateRecord       MutationType = "record.update"
	MutationDeleteRecord       MutationType = "record.delete"
	MutationCreateChunk        MutationType = "chunk.create"
	MutationCreateSourceSpan   MutationType = "source_span.create"
	MutationCreateNode         MutationType = "node.create"
	MutationUpdateNode         MutationType = "node.update"
	MutationDeleteNode         MutationType = "node.delete"
	MutationCreateEdge         MutationType = "edge.create"
	MutationDeleteEdge         MutationType = "edge.delete"
	MutationCreatePurgeRequest MutationType = "purge_request.create"
)

type Actor struct {
	ActorID   string `json:"actor_id,omitempty"`
	HumanID   string `json:"human_id,omitempty"`
	SessionID string `json:"session_id,omitempty"`
	MissionID string `json:"mission_id,omitempty"`
	Persona   string `json:"persona,omitempty"`
}

type Mutation struct {
	Type             MutationType      `json:"type"`
	IdempotencyKey   string            `json:"idempotency_key,omitempty"`
	ExpectedRevision uint64            `json:"expected_revision,omitempty"`
	Actor            Actor             `json:"actor,omitempty"`
	At               time.Time         `json:"at,omitempty"`
	Record           Record            `json:"record"`
	Chunk            Chunk             `json:"chunk,omitempty"`
	ChunkContent     []byte            `json:"-"`
	SourceSpan       SourceSpan        `json:"source_span,omitempty"`
	Node             Node              `json:"node,omitempty"`
	Edge             Edge              `json:"edge,omitempty"`
	PurgeRequest     PurgeRequest      `json:"purge_request,omitempty"`
	Metadata         map[string]string `json:"metadata,omitempty"`
}

type Projector interface {
	Name() string
	Project(context.Context, ProjectionEvent) error
}

type ProjectionEvent struct {
	Seq      uint64    `json:"seq"`
	Mutation Mutation  `json:"mutation"`
	At       time.Time `json:"at"`
}

type ProjectionCheckpoint struct {
	Name       string    `json:"name"`
	IndexedSeq uint64    `json:"indexed_seq"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type ProjectorStatus struct {
	Name       string    `json:"name"`
	DurableSeq uint64    `json:"durable_seq"`
	IndexedSeq uint64    `json:"indexed_seq"`
	Lag        uint64    `json:"lag"`
	Running    bool      `json:"running"`
	LastError  string    `json:"last_error,omitempty"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type ProjectionWait struct {
	Projector     string        `json:"projector"`
	MinIndexedSeq uint64        `json:"min_index_seq"`
	MaxLag        uint64        `json:"max_lag,omitempty"`
	EnforceMaxLag bool          `json:"enforce_max_lag,omitempty"`
	Timeout       time.Duration `json:"timeout,omitempty"`
}

type ProjectionWaitResult struct {
	Projector     ProjectorStatus `json:"projector"`
	MinIndexedSeq uint64          `json:"min_index_seq"`
	MaxLag        uint64          `json:"max_lag,omitempty"`
	EnforceMaxLag bool            `json:"enforce_max_lag,omitempty"`
	Satisfied     bool            `json:"satisfied"`
	Stale         bool            `json:"stale"`
	TimedOut      bool            `json:"timed_out,omitempty"`
}

type Record struct {
	ID         string            `json:"id"`
	Namespace  string            `json:"namespace"`
	ProjectID  string            `json:"project_id,omitempty"`
	Kind       string            `json:"kind"`
	Summary    string            `json:"summary"`
	Tags       []string          `json:"tags,omitempty"`
	HumanID    string            `json:"human_id,omitempty"`
	SessionID  string            `json:"session_id,omitempty"`
	MissionID  string            `json:"mission_id,omitempty"`
	Persona    string            `json:"persona,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
	Revision   uint64            `json:"revision"`
	Deleted    bool              `json:"deleted,omitempty"`
	CreatedAt  time.Time         `json:"created_at"`
	UpdatedAt  time.Time         `json:"updated_at"`
	CreatedSeq uint64            `json:"created_seq"`
	UpdatedSeq uint64            `json:"updated_seq"`
}

type ApplyResult struct {
	Seq          uint64       `json:"seq"`
	Record       Record       `json:"record"`
	Chunk        Chunk        `json:"chunk,omitempty"`
	SourceSpan   SourceSpan   `json:"source_span,omitempty"`
	Node         Node         `json:"node,omitempty"`
	Edge         Edge         `json:"edge,omitempty"`
	PurgeRequest PurgeRequest `json:"purge_request,omitempty"`
	Idempotent   bool         `json:"idempotent,omitempty"`
}

type Status struct {
	DurableSeq    uint64                     `json:"durable_seq"`
	SnapshotSeq   uint64                     `json:"snapshot_seq,omitempty"`
	CompactionSeq uint64                     `json:"compaction_seq,omitempty"`
	Owner         StoreOwner                 `json:"owner"`
	Records       int                        `json:"records"`
	Tombstones    int                        `json:"tombstones"`
	Chunks        int                        `json:"chunks"`
	SourceSpans   int                        `json:"source_spans"`
	Nodes         int                        `json:"nodes"`
	Edges         int                        `json:"edges"`
	PurgeRequests int                        `json:"purge_requests"`
	Projectors    map[string]ProjectorStatus `json:"projectors,omitempty"`
}

type StoreOwner struct {
	PID      int       `json:"pid"`
	Hostname string    `json:"hostname,omitempty"`
	Path     string    `json:"path"`
	LockPath string    `json:"lock_path"`
	OpenedAt time.Time `json:"opened_at"`
}

type SnapshotMetadata struct {
	Version    int       `json:"version"`
	DurableSeq uint64    `json:"durable_seq"`
	Checksum   string    `json:"checksum"`
	Path       string    `json:"path,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

type CatalogCounts struct {
	Records       int `json:"records"`
	Tombstones    int `json:"tombstones"`
	Chunks        int `json:"chunks"`
	SourceSpans   int `json:"source_spans"`
	Nodes         int `json:"nodes"`
	Edges         int `json:"edges"`
	PurgeRequests int `json:"purge_requests"`
}

type CompactionMetadata struct {
	Version         int              `json:"version"`
	CompactedSeq    uint64           `json:"compacted_seq"`
	Snapshot        SnapshotMetadata `json:"snapshot"`
	Counts          CatalogCounts    `json:"counts"`
	RemovedFrames   int              `json:"removed_frames"`
	KeptFrames      int              `json:"kept_frames"`
	ActiveSegment   string           `json:"active_segment"`
	ArchivedSegment string           `json:"archived_segment"`
	Path            string           `json:"path,omitempty"`
	Checksum        string           `json:"checksum"`
	CreatedAt       time.Time        `json:"created_at"`
}

type RecordQuery struct {
	Namespace       string
	ProjectID       string
	Kind            string
	Tags            []string
	HumanID         string
	SessionID       string
	MissionID       string
	Persona         string
	IncludeDeleted  bool
	IncludeAuditIDs bool
	Limit           int
}

type RecordQueryResult struct {
	Records      []Record `json:"records"`
	CandidateIDs []string `json:"candidate_ids,omitempty"`
}

type Chunk struct {
	ID            string            `json:"id"`
	Namespace     string            `json:"namespace"`
	ProjectID     string            `json:"project_id,omitempty"`
	SessionID     string            `json:"session_id,omitempty"`
	MissionID     string            `json:"mission_id,omitempty"`
	Kind          string            `json:"kind"`
	Sequence      uint64            `json:"sequence"`
	ContentHash   string            `json:"content_hash"`
	ContentLength int64             `json:"content_length"`
	Attributes    map[string]string `json:"attributes,omitempty"`
	Revision      uint64            `json:"revision"`
	CreatedAt     time.Time         `json:"created_at"`
	UpdatedAt     time.Time         `json:"updated_at"`
	CreatedSeq    uint64            `json:"created_seq"`
	UpdatedSeq    uint64            `json:"updated_seq"`
}

type ChunkQuery struct {
	Namespace         string
	ProjectID         string
	SessionID         string
	MissionID         string
	Kind              string
	MinSequence       uint64
	MaxSequence       uint64
	IncludeCandidates bool
	Limit             int
}

type ChunkQueryResult struct {
	Chunks       []Chunk  `json:"chunks"`
	CandidateIDs []string `json:"candidate_ids,omitempty"`
}

type SourceSpan struct {
	ID         string               `json:"id"`
	Namespace  string               `json:"namespace"`
	ProjectID  string               `json:"project_id,omitempty"`
	SessionID  string               `json:"session_id,omitempty"`
	MissionID  string               `json:"mission_id,omitempty"`
	RecordID   string               `json:"record_id"`
	ChunkRefs  []SourceSpanChunkRef `json:"chunk_refs"`
	Attributes map[string]string    `json:"attributes,omitempty"`
	Revision   uint64               `json:"revision"`
	CreatedAt  time.Time            `json:"created_at"`
	UpdatedAt  time.Time            `json:"updated_at"`
	CreatedSeq uint64               `json:"created_seq"`
	UpdatedSeq uint64               `json:"updated_seq"`
}

type SourceSpanChunkRef struct {
	ChunkID     string `json:"chunk_id"`
	StartOffset int64  `json:"start_offset,omitempty"`
	EndOffset   int64  `json:"end_offset,omitempty"`
}

type SourceSpanRead struct {
	Span   SourceSpan `json:"span"`
	Chunks []Chunk    `json:"chunks"`
}

type SourceSpanQuery struct {
	Namespace         string
	ProjectID         string
	SessionID         string
	MissionID         string
	RecordID          string
	IncludeCandidates bool
	Limit             int
}

type SourceSpanQueryResult struct {
	Spans        []SourceSpan `json:"spans"`
	CandidateIDs []string     `json:"candidate_ids,omitempty"`
}

type Node struct {
	ID         string            `json:"id"`
	Type       string            `json:"type"`
	Name       string            `json:"name,omitempty"`
	Summary    string            `json:"summary,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
	Revision   uint64            `json:"revision"`
	Deleted    bool              `json:"deleted,omitempty"`
	CreatedAt  time.Time         `json:"created_at"`
	UpdatedAt  time.Time         `json:"updated_at"`
	CreatedSeq uint64            `json:"created_seq"`
	UpdatedSeq uint64            `json:"updated_seq"`
}

type Edge struct {
	ID         string            `json:"id"`
	Type       string            `json:"type"`
	FromNodeID string            `json:"from_node_id"`
	ToNodeID   string            `json:"to_node_id"`
	Attributes map[string]string `json:"attributes,omitempty"`
	Revision   uint64            `json:"revision"`
	Deleted    bool              `json:"deleted,omitempty"`
	CreatedAt  time.Time         `json:"created_at"`
	UpdatedAt  time.Time         `json:"updated_at"`
	CreatedSeq uint64            `json:"created_seq"`
	UpdatedSeq uint64            `json:"updated_seq"`
}

type PurgeAction string

const (
	PurgeActionPurge  PurgeAction = "purge"
	PurgeActionRedact PurgeAction = "redact"
)

type PurgeRequest struct {
	ID         string            `json:"id"`
	Action     PurgeAction       `json:"action"`
	TargetType string            `json:"target_type"`
	TargetID   string            `json:"target_id"`
	Reason     string            `json:"reason"`
	Attributes map[string]string `json:"attributes,omitempty"`
	Revision   uint64            `json:"revision"`
	CreatedAt  time.Time         `json:"created_at"`
	UpdatedAt  time.Time         `json:"updated_at"`
	CreatedSeq uint64            `json:"created_seq"`
	UpdatedSeq uint64            `json:"updated_seq"`
}

type EdgeQuery struct {
	Type           string
	IncludeDeleted bool
}

type GraphTraversal struct {
	Nodes []Node `json:"nodes"`
	Edges []Edge `json:"edges"`
}

type EdgeDirection string

const (
	EdgeDirectionAny      EdgeDirection = ""
	EdgeDirectionOutgoing EdgeDirection = "outgoing"
	EdgeDirectionIncoming EdgeDirection = "incoming"
)

type NodeQuery struct {
	Type                      string
	Attributes                map[string]string
	UpdatedAfter              time.Time
	UpdatedBefore             time.Time
	RelatedNodeID             string
	RelatedEdgeType           string
	RelatedDirection          EdgeDirection
	ExpandParents             bool
	ParentEdgeType            string
	ExpandChildren            bool
	ChildEdgeType             string
	ExpandEvidence            bool
	EvidenceEdgeType          string
	ExpandSourceSpans         bool
	SourceSpanRecordAttribute string
	IncludeDeleted            bool
	IncludeCandidates         bool
	Limit                     int
}

type NodeQueryResult struct {
	Nodes        []Node           `json:"nodes"`
	MatchEdges   []Edge           `json:"match_edges,omitempty"`
	Expanded     GraphTraversal   `json:"expanded,omitempty"`
	SourceSpans  []SourceSpanRead `json:"source_spans,omitempty"`
	CandidateIDs []string         `json:"candidate_ids,omitempty"`
}
