package kilo

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

func (s *Store) GetNode(ctx context.Context, id string) (Node, bool, error) {
	node, found, err := s.getNode(ctx, id)
	if err != nil || !found || node.Deleted {
		return Node{}, false, err
	}
	return node, true, nil
}

func (s *Store) GetNodeIncludingDeleted(ctx context.Context, id string) (Node, bool, error) {
	return s.getNode(ctx, id)
}

func (s *Store) GetEdge(ctx context.Context, id string) (Edge, bool, error) {
	edge, found, err := s.getEdge(ctx, id)
	if err != nil || !found || edge.Deleted {
		return Edge{}, false, err
	}
	return edge, true, nil
}

func (s *Store) GetEdgeIncludingDeleted(ctx context.Context, id string) (Edge, bool, error) {
	return s.getEdge(ctx, id)
}

func (s *Store) Children(ctx context.Context, nodeID string, query EdgeQuery) (GraphTraversal, error) {
	return s.traverse(ctx, strings.TrimSpace(nodeID), normalizeEdgeQuery(query), true)
}

func (s *Store) Parents(ctx context.Context, nodeID string, query EdgeQuery) (GraphTraversal, error) {
	return s.traverse(ctx, strings.TrimSpace(nodeID), normalizeEdgeQuery(query), false)
}

func (s *Store) QueryNodes(ctx context.Context, query NodeQuery) (NodeQueryResult, error) {
	if err := ctx.Err(); err != nil {
		return NodeQueryResult{}, err
	}
	normalized := normalizeNodeQuery(query)

	s.mu.RLock()
	defer s.mu.RUnlock()

	nodeIDs := make([]string, 0, len(s.nodes))
	for id := range s.nodes {
		nodeIDs = append(nodeIDs, id)
	}
	sort.Strings(nodeIDs)

	result := NodeQueryResult{}
	for _, id := range nodeIDs {
		node := s.nodes[id]
		if !nodeMatchesQuery(node, normalized) {
			continue
		}
		matchEdges := s.relatedEdgesLocked(node.ID, normalized)
		if normalized.RelatedNodeID != "" && len(matchEdges) == 0 {
			continue
		}
		result.Nodes = append(result.Nodes, cloneNode(node))
		for _, edge := range matchEdges {
			result.MatchEdges = append(result.MatchEdges, cloneEdge(edge))
		}
	}
	sort.Slice(result.Nodes, func(i, j int) bool {
		if result.Nodes[i].UpdatedSeq == result.Nodes[j].UpdatedSeq {
			return result.Nodes[i].ID < result.Nodes[j].ID
		}
		return result.Nodes[i].UpdatedSeq > result.Nodes[j].UpdatedSeq
	})
	if normalized.Limit > 0 && len(result.Nodes) > normalized.Limit {
		result.Nodes = result.Nodes[:normalized.Limit]
	}
	result.MatchEdges = filterEdgesForNodes(result.MatchEdges, result.Nodes)
	sortEdges(result.MatchEdges)

	if normalized.IncludeCandidates {
		result.CandidateIDs = make([]string, 0, len(result.Nodes))
		for _, node := range result.Nodes {
			result.CandidateIDs = append(result.CandidateIDs, node.ID)
		}
		sort.Strings(result.CandidateIDs)
	}
	result.Expanded = s.expandQueryResultLocked(result.Nodes, normalized)
	if normalized.ExpandSourceSpans {
		var err error
		result.SourceSpans, err = s.expandSourceSpansLocked(result.Nodes, result.Expanded.Nodes, normalized)
		if err != nil {
			return NodeQueryResult{}, err
		}
	}
	return result, nil
}

func (s *Store) getNode(ctx context.Context, id string) (Node, bool, error) {
	if err := ctx.Err(); err != nil {
		return Node{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	node, ok := s.nodes[strings.TrimSpace(id)]
	if !ok {
		return Node{}, false, nil
	}
	return cloneNode(node), true, nil
}

func (s *Store) getEdge(ctx context.Context, id string) (Edge, bool, error) {
	if err := ctx.Err(); err != nil {
		return Edge{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	edge, ok := s.edges[strings.TrimSpace(id)]
	if !ok {
		return Edge{}, false, nil
	}
	return cloneEdge(edge), true, nil
}

func (s *Store) traverse(ctx context.Context, nodeID string, query EdgeQuery, forward bool) (GraphTraversal, error) {
	if err := ctx.Err(); err != nil {
		return GraphTraversal{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	start, ok := s.nodes[nodeID]
	if !ok || (!query.IncludeDeleted && start.Deleted) {
		return GraphTraversal{}, fmt.Errorf("%w: node %q", ErrNotFound, nodeID)
	}

	var edgeSet map[string]struct{}
	if forward {
		edgeSet = s.graph.outgoing[nodeID]
	} else {
		edgeSet = s.graph.incoming[nodeID]
	}
	edgeIDs := make([]string, 0, len(edgeSet))
	for edgeID := range edgeSet {
		edgeIDs = append(edgeIDs, edgeID)
	}
	sort.Strings(edgeIDs)

	return s.traverseEdgesLocked(edgeIDs, query, forward), nil
}

func (s *Store) applyCreateNode(seq uint64, mutation Mutation) (Node, error) {
	node := cloneNode(mutation.Node)
	if _, exists := s.nodes[node.ID]; exists {
		return Node{}, fmt.Errorf("%w: node %q already exists", ErrConflict, node.ID)
	}
	now := mutation.At.UTC()
	if now.IsZero() {
		now = s.clock.Now().UTC()
	}
	node.Revision = 1
	node.Deleted = false
	node.CreatedAt = now
	node.UpdatedAt = now
	node.CreatedSeq = seq
	node.UpdatedSeq = seq
	s.nodes[node.ID] = cloneNode(node)
	return node, nil
}

func (s *Store) applyUpdateNode(seq uint64, mutation Mutation) (Node, error) {
	existing, ok := s.nodes[mutation.Node.ID]
	if !ok || existing.Deleted {
		return Node{}, fmt.Errorf("%w: node %q", ErrNotFound, mutation.Node.ID)
	}
	if mutation.ExpectedRevision > 0 && mutation.ExpectedRevision != existing.Revision {
		return Node{}, fmt.Errorf("%w: expected revision %d got %d", ErrConflict, mutation.ExpectedRevision, existing.Revision)
	}
	updated := cloneNode(mutation.Node)
	updated.Revision = existing.Revision + 1
	updated.Deleted = false
	updated.CreatedAt = existing.CreatedAt
	updated.CreatedSeq = existing.CreatedSeq
	updated.UpdatedAt = mutation.At.UTC()
	if updated.UpdatedAt.IsZero() {
		updated.UpdatedAt = s.clock.Now().UTC()
	}
	updated.UpdatedSeq = seq
	s.nodes[updated.ID] = cloneNode(updated)
	return updated, nil
}

func (s *Store) applyDeleteNode(seq uint64, mutation Mutation) (Node, error) {
	id := strings.TrimSpace(mutation.Node.ID)
	existing, ok := s.nodes[id]
	if !ok || existing.Deleted {
		return Node{}, fmt.Errorf("%w: node %q", ErrNotFound, id)
	}
	if mutation.ExpectedRevision > 0 && mutation.ExpectedRevision != existing.Revision {
		return Node{}, fmt.Errorf("%w: expected revision %d got %d", ErrConflict, mutation.ExpectedRevision, existing.Revision)
	}
	deleted := cloneNode(existing)
	deleted.Revision = existing.Revision + 1
	deleted.Deleted = true
	deleted.UpdatedAt = mutation.At.UTC()
	if deleted.UpdatedAt.IsZero() {
		deleted.UpdatedAt = s.clock.Now().UTC()
	}
	deleted.UpdatedSeq = seq
	s.nodes[deleted.ID] = cloneNode(deleted)
	return deleted, nil
}

func (s *Store) applyCreateEdge(seq uint64, mutation Mutation) (Edge, error) {
	edge := cloneEdge(mutation.Edge)
	if _, exists := s.edges[edge.ID]; exists {
		return Edge{}, fmt.Errorf("%w: edge %q already exists", ErrConflict, edge.ID)
	}
	if err := s.validateEdgeEndpointsLocked(edge); err != nil {
		return Edge{}, err
	}
	now := mutation.At.UTC()
	if now.IsZero() {
		now = s.clock.Now().UTC()
	}
	edge.Revision = 1
	edge.Deleted = false
	edge.CreatedAt = now
	edge.UpdatedAt = now
	edge.CreatedSeq = seq
	edge.UpdatedSeq = seq
	s.edges[edge.ID] = cloneEdge(edge)
	s.indexEdgeLocked(edge)
	return edge, nil
}

func (s *Store) applyDeleteEdge(seq uint64, mutation Mutation) (Edge, error) {
	id := strings.TrimSpace(mutation.Edge.ID)
	existing, ok := s.edges[id]
	if !ok || existing.Deleted {
		return Edge{}, fmt.Errorf("%w: edge %q", ErrNotFound, id)
	}
	if mutation.ExpectedRevision > 0 && mutation.ExpectedRevision != existing.Revision {
		return Edge{}, fmt.Errorf("%w: expected revision %d got %d", ErrConflict, mutation.ExpectedRevision, existing.Revision)
	}
	deleted := cloneEdge(existing)
	deleted.Revision = existing.Revision + 1
	deleted.Deleted = true
	deleted.UpdatedAt = mutation.At.UTC()
	if deleted.UpdatedAt.IsZero() {
		deleted.UpdatedAt = s.clock.Now().UTC()
	}
	deleted.UpdatedSeq = seq
	s.edges[deleted.ID] = cloneEdge(deleted)
	return deleted, nil
}

func (s *Store) validateEdgeEndpointsLocked(edge Edge) error {
	from, ok := s.nodes[edge.FromNodeID]
	if !ok || from.Deleted {
		return fmt.Errorf("%w: from node %q", ErrNotFound, edge.FromNodeID)
	}
	to, ok := s.nodes[edge.ToNodeID]
	if !ok || to.Deleted {
		return fmt.Errorf("%w: to node %q", ErrNotFound, edge.ToNodeID)
	}
	return nil
}

func newGraphIndexes() graphIndexes {
	return graphIndexes{
		outgoing: map[string]map[string]struct{}{},
		incoming: map[string]map[string]struct{}{},
	}
}

func (s *Store) indexEdgeLocked(edge Edge) {
	addIndexValue(s.graph.outgoing, edge.FromNodeID, edge.ID)
	addIndexValue(s.graph.incoming, edge.ToNodeID, edge.ID)
}

func normalizeNode(node Node) Node {
	node.ID = strings.TrimSpace(node.ID)
	node.Type = strings.TrimSpace(node.Type)
	node.Name = strings.TrimSpace(node.Name)
	node.Summary = strings.TrimSpace(node.Summary)
	node.Attributes = normalizeStringMap(node.Attributes)
	return node
}

func validateNodeForWrite(node Node) error {
	if node.ID == "" {
		return fmt.Errorf("node id is required")
	}
	if node.Type == "" {
		return fmt.Errorf("node type is required")
	}
	return nil
}

func normalizeEdge(edge Edge) Edge {
	edge.ID = strings.TrimSpace(edge.ID)
	edge.Type = strings.TrimSpace(edge.Type)
	edge.FromNodeID = strings.TrimSpace(edge.FromNodeID)
	edge.ToNodeID = strings.TrimSpace(edge.ToNodeID)
	edge.Attributes = normalizeStringMap(edge.Attributes)
	return edge
}

func validateEdgeForWrite(edge Edge) error {
	if edge.ID == "" {
		return fmt.Errorf("edge id is required")
	}
	if edge.Type == "" {
		return fmt.Errorf("edge type is required")
	}
	if edge.FromNodeID == "" {
		return fmt.Errorf("edge from_node_id is required")
	}
	if edge.ToNodeID == "" {
		return fmt.Errorf("edge to_node_id is required")
	}
	return nil
}

func normalizeEdgeQuery(query EdgeQuery) EdgeQuery {
	query.Type = strings.TrimSpace(query.Type)
	return query
}

func normalizeNodeQuery(query NodeQuery) NodeQuery {
	query.Type = strings.TrimSpace(query.Type)
	query.Attributes = normalizeStringMap(query.Attributes)
	query.RelatedNodeID = strings.TrimSpace(query.RelatedNodeID)
	query.RelatedEdgeType = strings.TrimSpace(query.RelatedEdgeType)
	query.RelatedDirection = EdgeDirection(strings.TrimSpace(string(query.RelatedDirection)))
	query.ParentEdgeType = strings.TrimSpace(query.ParentEdgeType)
	query.ChildEdgeType = strings.TrimSpace(query.ChildEdgeType)
	query.EvidenceEdgeType = strings.TrimSpace(query.EvidenceEdgeType)
	query.SourceSpanRecordAttribute = strings.TrimSpace(query.SourceSpanRecordAttribute)
	if query.SourceSpanRecordAttribute == "" {
		query.SourceSpanRecordAttribute = DefaultSourceSpanRecordAttribute
	}
	return query
}

func nodeMatchesQuery(node Node, query NodeQuery) bool {
	if !query.IncludeDeleted && node.Deleted {
		return false
	}
	if query.Type != "" && node.Type != query.Type {
		return false
	}
	if !query.UpdatedAfter.IsZero() && !node.UpdatedAt.After(query.UpdatedAfter) {
		return false
	}
	if !query.UpdatedBefore.IsZero() && !node.UpdatedAt.Before(query.UpdatedBefore) {
		return false
	}
	for key, value := range query.Attributes {
		if node.Attributes[key] != value {
			return false
		}
	}
	return true
}

func (s *Store) relatedEdgesLocked(nodeID string, query NodeQuery) []Edge {
	if query.RelatedNodeID == "" {
		return nil
	}
	edges := []Edge{}
	if query.RelatedDirection == EdgeDirectionAny || query.RelatedDirection == EdgeDirectionOutgoing {
		for edgeID := range s.graph.outgoing[query.RelatedNodeID] {
			edge := s.edges[edgeID]
			if edge.ToNodeID == nodeID && edgeMatchesRelation(edge, query) {
				edges = append(edges, cloneEdge(edge))
			}
		}
	}
	if query.RelatedDirection == EdgeDirectionAny || query.RelatedDirection == EdgeDirectionIncoming {
		for edgeID := range s.graph.incoming[query.RelatedNodeID] {
			edge := s.edges[edgeID]
			if edge.FromNodeID == nodeID && edgeMatchesRelation(edge, query) {
				edges = append(edges, cloneEdge(edge))
			}
		}
	}
	sortEdges(edges)
	return edges
}

func edgeMatchesRelation(edge Edge, query NodeQuery) bool {
	if !query.IncludeDeleted && edge.Deleted {
		return false
	}
	if query.RelatedEdgeType != "" && edge.Type != query.RelatedEdgeType {
		return false
	}
	return true
}

func (s *Store) expandQueryResultLocked(nodes []Node, query NodeQuery) GraphTraversal {
	nodeByID := map[string]Node{}
	edgeByID := map[string]Edge{}
	addTraversal := func(traversal GraphTraversal) {
		for _, node := range traversal.Nodes {
			nodeByID[node.ID] = node
		}
		for _, edge := range traversal.Edges {
			edgeByID[edge.ID] = edge
		}
	}

	for _, node := range nodes {
		if query.ExpandParents {
			addTraversal(s.traverseIDsLocked(s.graph.incoming[node.ID], EdgeQuery{
				Type:           query.ParentEdgeType,
				IncludeDeleted: query.IncludeDeleted,
			}, false))
		}
		if query.ExpandChildren {
			addTraversal(s.traverseIDsLocked(s.graph.outgoing[node.ID], EdgeQuery{
				Type:           query.ChildEdgeType,
				IncludeDeleted: query.IncludeDeleted,
			}, true))
		}
		if query.ExpandEvidence {
			addTraversal(s.traverseIDsLocked(s.graph.incoming[node.ID], EdgeQuery{
				Type:           firstNonEmpty(query.EvidenceEdgeType, "evidence_for"),
				IncludeDeleted: query.IncludeDeleted,
			}, false))
		}
	}

	return GraphTraversal{
		Nodes: sortedNodes(nodeByID),
		Edges: sortedEdges(edgeByID),
	}
}

func (s *Store) expandSourceSpansLocked(matched []Node, expanded []Node, query NodeQuery) ([]SourceSpanRead, error) {
	recordIDs := map[string]struct{}{}
	addNode := func(node Node) {
		recordID := strings.TrimSpace(node.Attributes[query.SourceSpanRecordAttribute])
		if recordID != "" {
			recordIDs[recordID] = struct{}{}
		}
	}
	for _, node := range matched {
		addNode(node)
	}
	for _, node := range expanded {
		addNode(node)
	}
	if len(recordIDs) == 0 {
		return nil, nil
	}

	spanIDs := map[string]struct{}{}
	for recordID := range recordIDs {
		for spanID := range s.spanIndexes.recordID[recordID] {
			spanIDs[spanID] = struct{}{}
		}
	}
	ids := make([]string, 0, len(spanIDs))
	for id := range spanIDs {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	reads := make([]SourceSpanRead, 0, len(ids))
	for _, id := range ids {
		span, ok := s.sourceSpans[id]
		if !ok {
			continue
		}
		read, err := s.sourceSpanReadLocked(span)
		if err != nil {
			return nil, err
		}
		reads = append(reads, read)
	}
	sort.Slice(reads, func(i, j int) bool {
		if reads[i].Span.UpdatedSeq == reads[j].Span.UpdatedSeq {
			return reads[i].Span.ID < reads[j].Span.ID
		}
		return reads[i].Span.UpdatedSeq > reads[j].Span.UpdatedSeq
	})
	return reads, nil
}

func (s *Store) traverseIDsLocked(edgeSet map[string]struct{}, query EdgeQuery, forward bool) GraphTraversal {
	edgeIDs := make([]string, 0, len(edgeSet))
	for edgeID := range edgeSet {
		edgeIDs = append(edgeIDs, edgeID)
	}
	sort.Strings(edgeIDs)
	return s.traverseEdgesLocked(edgeIDs, query, forward)
}

func (s *Store) traverseEdgesLocked(edgeIDs []string, query EdgeQuery, forward bool) GraphTraversal {
	result := GraphTraversal{}
	for _, edgeID := range edgeIDs {
		edge := s.edges[edgeID]
		if query.Type != "" && edge.Type != query.Type {
			continue
		}
		if !query.IncludeDeleted && edge.Deleted {
			continue
		}
		nextID := edge.ToNodeID
		if !forward {
			nextID = edge.FromNodeID
		}
		next, ok := s.nodes[nextID]
		if !ok || (!query.IncludeDeleted && next.Deleted) {
			continue
		}
		result.Edges = append(result.Edges, cloneEdge(edge))
		result.Nodes = append(result.Nodes, cloneNode(next))
	}
	return result
}

func sortedNodes(nodes map[string]Node) []Node {
	ids := make([]string, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]Node, 0, len(ids))
	for _, id := range ids {
		out = append(out, cloneNode(nodes[id]))
	}
	return out
}

func sortedEdges(edges map[string]Edge) []Edge {
	ids := make([]string, 0, len(edges))
	for id := range edges {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]Edge, 0, len(ids))
	for _, id := range ids {
		out = append(out, cloneEdge(edges[id]))
	}
	return out
}

func filterEdgesForNodes(edges []Edge, nodes []Node) []Edge {
	if len(edges) == 0 || len(nodes) == 0 {
		return nil
	}
	selected := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		selected[node.ID] = struct{}{}
	}
	out := make([]Edge, 0, len(edges))
	for _, edge := range edges {
		if _, ok := selected[edge.FromNodeID]; ok {
			out = append(out, edge)
			continue
		}
		if _, ok := selected[edge.ToNodeID]; ok {
			out = append(out, edge)
		}
	}
	return out
}

func sortEdges(edges []Edge) {
	sort.Slice(edges, func(i, j int) bool {
		return edges[i].ID < edges[j].ID
	})
}

func cloneNode(node Node) Node {
	node.Attributes = cloneStringMap(node.Attributes)
	return node
}

func cloneEdge(edge Edge) Edge {
	edge.Attributes = cloneStringMap(edge.Attributes)
	return edge
}
