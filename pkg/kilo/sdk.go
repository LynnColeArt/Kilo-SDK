package kilo

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Write is the public SDK write envelope. Exactly one operation field must be set.
type Write struct {
	CreateRecord *Record
	UpdateRecord *Record
	DeleteRecord string

	CreateChunk *ChunkWrite

	CreateSourceSpan *SourceSpan

	CreateNode *Node
	UpdateNode *Node
	DeleteNode string

	CreateEdge *Edge
	DeleteEdge string

	CreatePurgeRequest *PurgeRequest

	IdempotencyKey   string
	ExpectedRevision uint64
	Actor            Actor
	At               time.Time
	Metadata         map[string]string
}

type ChunkWrite struct {
	Chunk   Chunk
	Content []byte
}

// Read is the public SDK point-read envelope. Exactly one ID field must be set.
type Read struct {
	RecordID       string
	ChunkID        string
	ChunkContentID string
	SourceSpanID   string
	NodeID         string
	EdgeID         string
	PurgeRequestID string
	IncludeDeleted bool
}

type ReadResult struct {
	Found        bool
	Record       Record
	Chunk        Chunk
	ChunkContent []byte
	SourceSpan   SourceSpanRead
	Node         Node
	Edge         Edge
	PurgeRequest PurgeRequest
}

// Query is the public SDK query envelope. Exactly one query field must be set.
type Query struct {
	Records     *RecordQuery
	Chunks      *ChunkQuery
	SourceSpans *SourceSpanQuery
	Nodes       *NodeQuery
	Children    *GraphQuery
	Parents     *GraphQuery
}

type GraphQuery struct {
	NodeID string
	Edge   EdgeQuery
}

type QueryResult struct {
	Records     RecordQueryResult
	Chunks      ChunkQueryResult
	SourceSpans SourceSpanQueryResult
	Nodes       NodeQueryResult
	Graph       GraphTraversal
}

// Search is the public SDK search envelope. Vector search is intentionally
// injected so embedding applications can choose their own async projection.
type Search struct {
	VectorSearcher VectorSearcher
	Vectors        *VectorSearchQuery
	RequireFresh   bool
}

type SearchResult struct {
	Vectors VectorSearchResult
}

func (s *Store) Write(ctx context.Context, write Write) (ApplyResult, error) {
	if err := ctx.Err(); err != nil {
		return ApplyResult{}, err
	}
	mutation, err := write.mutation()
	if err != nil {
		return ApplyResult{}, err
	}
	return s.Apply(ctx, mutation)
}

func (s *Store) Read(ctx context.Context, read Read) (ReadResult, error) {
	if err := ctx.Err(); err != nil {
		return ReadResult{}, err
	}
	if err := validateExactlyOne("read operation", readOperationCount(read)); err != nil {
		return ReadResult{}, err
	}

	if id := strings.TrimSpace(read.RecordID); id != "" {
		var (
			record Record
			found  bool
			err    error
		)
		if read.IncludeDeleted {
			record, found, err = s.GetRecordIncludingDeleted(ctx, id)
		} else {
			record, found, err = s.GetRecord(ctx, id)
		}
		return ReadResult{Found: found, Record: record}, err
	}
	if id := strings.TrimSpace(read.ChunkID); id != "" {
		chunk, found, err := s.GetChunk(ctx, id)
		return ReadResult{Found: found, Chunk: chunk}, err
	}
	if id := strings.TrimSpace(read.ChunkContentID); id != "" {
		chunk, found, err := s.GetChunk(ctx, id)
		if err != nil || !found {
			return ReadResult{Found: found}, err
		}
		content, found, err := s.ReadChunkContent(ctx, id)
		return ReadResult{Found: found, Chunk: chunk, ChunkContent: content}, err
	}
	if id := strings.TrimSpace(read.SourceSpanID); id != "" {
		span, found, err := s.ReadSourceSpan(ctx, id)
		return ReadResult{Found: found, SourceSpan: span}, err
	}
	if id := strings.TrimSpace(read.NodeID); id != "" {
		var (
			node  Node
			found bool
			err   error
		)
		if read.IncludeDeleted {
			node, found, err = s.GetNodeIncludingDeleted(ctx, id)
		} else {
			node, found, err = s.GetNode(ctx, id)
		}
		return ReadResult{Found: found, Node: node}, err
	}
	if id := strings.TrimSpace(read.EdgeID); id != "" {
		var (
			edge  Edge
			found bool
			err   error
		)
		if read.IncludeDeleted {
			edge, found, err = s.GetEdgeIncludingDeleted(ctx, id)
		} else {
			edge, found, err = s.GetEdge(ctx, id)
		}
		return ReadResult{Found: found, Edge: edge}, err
	}

	id := strings.TrimSpace(read.PurgeRequestID)
	request, found, err := s.GetPurgeRequest(ctx, id)
	return ReadResult{Found: found, PurgeRequest: request}, err
}

func (s *Store) Query(ctx context.Context, query Query) (QueryResult, error) {
	if err := ctx.Err(); err != nil {
		return QueryResult{}, err
	}
	if err := validateExactlyOne("query operation", queryOperationCount(query)); err != nil {
		return QueryResult{}, err
	}

	if query.Records != nil {
		result, err := s.QueryRecords(ctx, *query.Records)
		return QueryResult{Records: result}, err
	}
	if query.Chunks != nil {
		result, err := s.QueryChunks(ctx, *query.Chunks)
		return QueryResult{Chunks: result}, err
	}
	if query.SourceSpans != nil {
		result, err := s.QuerySourceSpans(ctx, *query.SourceSpans)
		return QueryResult{SourceSpans: result}, err
	}
	if query.Nodes != nil {
		result, err := s.QueryNodes(ctx, *query.Nodes)
		return QueryResult{Nodes: result}, err
	}
	if query.Children != nil {
		result, err := s.Children(ctx, query.Children.NodeID, query.Children.Edge)
		return QueryResult{Graph: result}, err
	}

	result, err := s.Parents(ctx, query.Parents.NodeID, query.Parents.Edge)
	return QueryResult{Graph: result}, err
}

func (s *Store) Search(ctx context.Context, search Search) (SearchResult, error) {
	if err := ctx.Err(); err != nil {
		return SearchResult{}, err
	}
	if err := validateExactlyOne("search operation", boolCount(search.Vectors != nil)); err != nil {
		return SearchResult{}, err
	}

	vectors, err := s.SearchVectors(ctx, search.VectorSearcher, *search.Vectors)
	result := SearchResult{Vectors: vectors}
	if err != nil {
		return result, err
	}
	if search.RequireFresh && vectors.Stale {
		return result, ErrStaleProjection
	}
	return result, nil
}

func (write Write) mutation() (Mutation, error) {
	if err := validateExactlyOne("write operation", writeOperationCount(write)); err != nil {
		return Mutation{}, err
	}
	mutation := Mutation{
		IdempotencyKey:   write.IdempotencyKey,
		ExpectedRevision: write.ExpectedRevision,
		Actor:            write.Actor,
		At:               write.At,
		Metadata:         cloneStringMap(write.Metadata),
	}
	switch {
	case write.CreateRecord != nil:
		mutation.Type = MutationCreateRecord
		mutation.Record = cloneRecord(*write.CreateRecord)
	case write.UpdateRecord != nil:
		mutation.Type = MutationUpdateRecord
		mutation.Record = cloneRecord(*write.UpdateRecord)
	case strings.TrimSpace(write.DeleteRecord) != "":
		mutation.Type = MutationDeleteRecord
		mutation.Record = Record{ID: write.DeleteRecord}
	case write.CreateChunk != nil:
		mutation.Type = MutationCreateChunk
		mutation.Chunk = cloneChunk(write.CreateChunk.Chunk)
		mutation.ChunkContent = cloneBytes(write.CreateChunk.Content)
	case write.CreateSourceSpan != nil:
		mutation.Type = MutationCreateSourceSpan
		mutation.SourceSpan = cloneSourceSpan(*write.CreateSourceSpan)
	case write.CreateNode != nil:
		mutation.Type = MutationCreateNode
		mutation.Node = cloneNode(*write.CreateNode)
	case write.UpdateNode != nil:
		mutation.Type = MutationUpdateNode
		mutation.Node = cloneNode(*write.UpdateNode)
	case strings.TrimSpace(write.DeleteNode) != "":
		mutation.Type = MutationDeleteNode
		mutation.Node = Node{ID: write.DeleteNode}
	case write.CreateEdge != nil:
		mutation.Type = MutationCreateEdge
		mutation.Edge = cloneEdge(*write.CreateEdge)
	case strings.TrimSpace(write.DeleteEdge) != "":
		mutation.Type = MutationDeleteEdge
		mutation.Edge = Edge{ID: write.DeleteEdge}
	case write.CreatePurgeRequest != nil:
		mutation.Type = MutationCreatePurgeRequest
		mutation.PurgeRequest = clonePurgeRequest(*write.CreatePurgeRequest)
	}
	return mutation, nil
}

func writeOperationCount(write Write) int {
	return boolCount(
		write.CreateRecord != nil,
		write.UpdateRecord != nil,
		strings.TrimSpace(write.DeleteRecord) != "",
		write.CreateChunk != nil,
		write.CreateSourceSpan != nil,
		write.CreateNode != nil,
		write.UpdateNode != nil,
		strings.TrimSpace(write.DeleteNode) != "",
		write.CreateEdge != nil,
		strings.TrimSpace(write.DeleteEdge) != "",
		write.CreatePurgeRequest != nil,
	)
}

func readOperationCount(read Read) int {
	return boolCount(
		strings.TrimSpace(read.RecordID) != "",
		strings.TrimSpace(read.ChunkID) != "",
		strings.TrimSpace(read.ChunkContentID) != "",
		strings.TrimSpace(read.SourceSpanID) != "",
		strings.TrimSpace(read.NodeID) != "",
		strings.TrimSpace(read.EdgeID) != "",
		strings.TrimSpace(read.PurgeRequestID) != "",
	)
}

func queryOperationCount(query Query) int {
	return boolCount(
		query.Records != nil,
		query.Chunks != nil,
		query.SourceSpans != nil,
		query.Nodes != nil,
		query.Children != nil,
		query.Parents != nil,
	)
}

func boolCount(values ...bool) int {
	count := 0
	for _, value := range values {
		if value {
			count++
		}
	}
	return count
}

func validateExactlyOne(name string, count int) error {
	if count != 1 {
		return fmt.Errorf("%s requires exactly one operation, got %d", name, count)
	}
	return nil
}
