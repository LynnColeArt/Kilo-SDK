package httpapi

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/LynnColeArt/Kilo-SDK/internal/pilotimport"
	"github.com/LynnColeArt/Kilo-SDK/pkg/kilo"
)

type ErrorResponse struct {
	Error string `json:"error"`
}

type Server struct {
	store   *kilo.Store
	options ServerOptions
}

type ServerOptions struct {
	MemoryImportVectorIndex kilo.VectorIndex
	DefaultNamespace        string
	DefaultProjectID        string
	DefaultHumanID          string
}

type MemoryImportRequest struct {
	Records   []pilotimport.MemoryRecord `json:"records"`
	Namespace string                     `json:"namespace,omitempty"`
	ProjectID string                     `json:"project_id,omitempty"`
	HumanID   string                     `json:"human_id,omitempty"`
}

type MemoryImportResponse struct {
	Result     pilotimport.MemoryImportResult `json:"result"`
	DurableSeq uint64                         `json:"durable_seq"`
	Status     kilo.Status                    `json:"status"`
}

func New(store *kilo.Store) http.Handler {
	return NewWithOptions(store, ServerOptions{})
}

func NewWithOptions(store *kilo.Store, opts ServerOptions) http.Handler {
	server := &Server{store: store, options: opts}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", server.health)
	mux.HandleFunc("/v1/status", server.status)
	mux.HandleFunc("/v1/tail", server.tail)
	mux.HandleFunc("/v1/import-memory", server.importMemory)
	mux.HandleFunc("/v1/query/records", server.queryRecords)
	mux.HandleFunc("/v1/records/", server.record)
	mux.HandleFunc("/v1/nodes/", server.node)
	mux.HandleFunc("/v1/edges/", server.edge)
	mux.HandleFunc("/v1/chunks/", server.chunk)
	mux.HandleFunc("/v1/source-spans/", server.sourceSpan)
	mux.HandleFunc("/v1/purge-requests/", server.purgeRequest)
	return mux
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	writeJSON(w, http.StatusOK, s.store.Status())
}

func (s *Server) tail(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	query, err := tailQueryFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	result, err := s.store.Tail(r.Context(), query)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) queryRecords(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	query, err := recordQueryFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	result, err := s.store.Query(r.Context(), kilo.Query{Records: &query})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, result.Records)
}

func (s *Server) importMemory(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodPost) {
		return
	}
	request, err := decodeMemoryImportRequest(w, r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if len(request.Records) == 0 {
		writeError(w, http.StatusBadRequest, fmt.Errorf("records is required"))
		return
	}

	result, err := pilotimport.ImportMemoryRecords(r.Context(), s.store, request.Records, pilotimport.MemoryImportOptions{
		Namespace:   firstNonEmpty(request.Namespace, s.options.DefaultNamespace),
		ProjectID:   firstNonEmpty(request.ProjectID, s.options.DefaultProjectID),
		HumanID:     firstNonEmpty(request.HumanID, s.options.DefaultHumanID),
		VectorIndex: s.options.MemoryImportVectorIndex,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	status := s.store.Status()
	writeJSON(w, http.StatusOK, MemoryImportResponse{
		Result:     result,
		DurableSeq: status.DurableSeq,
		Status:     status,
	})
}

func (s *Server) record(w http.ResponseWriter, r *http.Request) {
	s.read(w, r, "/v1/records/", func(id string, includeDeleted bool) kilo.Read {
		return kilo.Read{RecordID: id, IncludeDeleted: includeDeleted}
	})
}

func (s *Server) node(w http.ResponseWriter, r *http.Request) {
	s.read(w, r, "/v1/nodes/", func(id string, includeDeleted bool) kilo.Read {
		return kilo.Read{NodeID: id, IncludeDeleted: includeDeleted}
	})
}

func (s *Server) edge(w http.ResponseWriter, r *http.Request) {
	s.read(w, r, "/v1/edges/", func(id string, includeDeleted bool) kilo.Read {
		return kilo.Read{EdgeID: id, IncludeDeleted: includeDeleted}
	})
}

func (s *Server) chunk(w http.ResponseWriter, r *http.Request) {
	s.read(w, r, "/v1/chunks/", func(id string, includeDeleted bool) kilo.Read {
		return kilo.Read{ChunkID: id}
	})
}

func (s *Server) sourceSpan(w http.ResponseWriter, r *http.Request) {
	s.read(w, r, "/v1/source-spans/", func(id string, includeDeleted bool) kilo.Read {
		return kilo.Read{SourceSpanID: id}
	})
}

func (s *Server) purgeRequest(w http.ResponseWriter, r *http.Request) {
	s.read(w, r, "/v1/purge-requests/", func(id string, includeDeleted bool) kilo.Read {
		return kilo.Read{PurgeRequestID: id}
	})
}

func (s *Server) read(w http.ResponseWriter, r *http.Request, prefix string, build func(string, bool) kilo.Read) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	id, err := pathID(r.URL.Path, prefix)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	result, err := s.store.Read(r.Context(), build(id, includeDeleted(r)))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if !result.Found {
		writeError(w, http.StatusNotFound, fmt.Errorf("not found"))
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func decodeMemoryImportRequest(w http.ResponseWriter, r *http.Request) (MemoryImportRequest, error) {
	defer r.Body.Close()
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4*1024*1024))
	decoder.DisallowUnknownFields()
	var request MemoryImportRequest
	if err := decoder.Decode(&request); err != nil {
		return MemoryImportRequest{}, fmt.Errorf("decode memory import request: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return MemoryImportRequest{}, fmt.Errorf("decode memory import request: trailing JSON")
	}
	return request, nil
}

func recordQueryFromRequest(r *http.Request) (kilo.RecordQuery, error) {
	values := r.URL.Query()
	var query kilo.RecordQuery
	query.Namespace = strings.TrimSpace(values.Get("namespace"))
	query.ProjectID = strings.TrimSpace(values.Get("project_id"))
	query.Kind = strings.TrimSpace(values.Get("kind"))
	query.HumanID = strings.TrimSpace(values.Get("human_id"))
	query.SessionID = strings.TrimSpace(values.Get("session_id"))
	query.MissionID = strings.TrimSpace(values.Get("mission_id"))
	query.Persona = strings.TrimSpace(values.Get("persona"))
	query.Tags = compactRepeated(values["tag"])
	query.IncludeDeleted = boolQuery(values.Get("include_deleted"))
	query.IncludeAuditIDs = boolQuery(values.Get("include_audit_ids"))
	if raw := values.Get("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil {
			return kilo.RecordQuery{}, fmt.Errorf("invalid limit %q", raw)
		}
		if limit < 0 {
			return kilo.RecordQuery{}, fmt.Errorf("limit must be non-negative")
		}
		query.Limit = limit
	}
	return query, nil
}

func tailQueryFromRequest(r *http.Request) (kilo.TailQuery, error) {
	values := r.URL.Query()
	var query kilo.TailQuery
	if raw := values.Get("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil {
			return kilo.TailQuery{}, fmt.Errorf("invalid limit %q", raw)
		}
		if limit < 0 {
			return kilo.TailQuery{}, fmt.Errorf("limit must be non-negative")
		}
		query.Limit = limit
	}
	if raw := values.Get("min_seq"); raw != "" {
		minSeq, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return kilo.TailQuery{}, fmt.Errorf("invalid min_seq %q", raw)
		}
		query.MinSeq = minSeq
	}
	return query, nil
}

func includeDeleted(r *http.Request) bool {
	return boolQuery(r.URL.Query().Get("include_deleted"))
}

func boolQuery(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	return value == "1" || value == "true" || value == "yes"
}

func compactRepeated(values []string) []string {
	out := []string{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func pathID(path, prefix string) (string, error) {
	raw := strings.TrimPrefix(path, prefix)
	if raw == "" || raw == path {
		return "", fmt.Errorf("id is required")
	}
	if strings.Contains(raw, "/") {
		return "", fmt.Errorf("id must not contain slashes")
	}
	id, err := url.PathUnescape(raw)
	if err != nil {
		return "", fmt.Errorf("decode id: %w", err)
	}
	if strings.TrimSpace(id) == "" {
		return "", fmt.Errorf("id is required")
	}
	return id, nil
}

func allowMethod(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method == method {
		return true
	}
	w.Header().Set("Allow", method)
	writeError(w, http.StatusMethodNotAllowed, fmt.Errorf("method %s is not allowed", r.Method))
	return false
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, ErrorResponse{Error: err.Error()})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
