package kilo

import (
	"context"
	"fmt"
	"strings"
)

func (s *Store) GetPurgeRequest(ctx context.Context, id string) (PurgeRequest, bool, error) {
	if err := ctx.Err(); err != nil {
		return PurgeRequest{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	request, ok := s.purgeRequests[strings.TrimSpace(id)]
	if !ok {
		return PurgeRequest{}, false, nil
	}
	return clonePurgeRequest(request), true, nil
}

func (s *Store) applyCreatePurgeRequest(seq uint64, mutation Mutation) (PurgeRequest, error) {
	request := clonePurgeRequest(mutation.PurgeRequest)
	if _, exists := s.purgeRequests[request.ID]; exists {
		return PurgeRequest{}, fmt.Errorf("%w: purge request %q already exists", ErrConflict, request.ID)
	}
	now := mutation.At.UTC()
	if now.IsZero() {
		now = s.clock.Now().UTC()
	}
	request.Revision = 1
	request.CreatedAt = now
	request.UpdatedAt = now
	request.CreatedSeq = seq
	request.UpdatedSeq = seq
	s.purgeRequests[request.ID] = clonePurgeRequest(request)
	return request, nil
}

func normalizePurgeRequest(request PurgeRequest) PurgeRequest {
	request.ID = strings.TrimSpace(request.ID)
	request.Action = PurgeAction(strings.TrimSpace(string(request.Action)))
	request.TargetType = strings.TrimSpace(request.TargetType)
	request.TargetID = strings.TrimSpace(request.TargetID)
	request.Reason = strings.TrimSpace(request.Reason)
	request.Attributes = normalizeStringMap(request.Attributes)
	return request
}

func validatePurgeRequestForWrite(request PurgeRequest) error {
	if request.ID == "" {
		return fmt.Errorf("purge request id is required")
	}
	switch request.Action {
	case PurgeActionPurge, PurgeActionRedact:
	default:
		return fmt.Errorf("unsupported purge request action %q", request.Action)
	}
	if request.TargetType == "" {
		return fmt.Errorf("purge request target type is required")
	}
	if request.TargetID == "" {
		return fmt.Errorf("purge request target id is required")
	}
	if request.Reason == "" {
		return fmt.Errorf("purge request reason is required")
	}
	return nil
}

func clonePurgeRequest(request PurgeRequest) PurgeRequest {
	request.Attributes = cloneStringMap(request.Attributes)
	return request
}
