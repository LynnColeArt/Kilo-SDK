# Kilo Task List

## Milestone 0: Contracts And File Format Sketch

### KILO-0001: Define Public Concepts

Tasks:

- Define store, namespace, mutation, record, chunk, node, edge, projection, and
  watermark terminology.
- Document how CRUD maps to audited mutations.
- Document which concepts are canonical and which are projections.

Acceptance criteria:

- `docs/PROJECT_PLAN.md` links to or includes the canonical terminology.
- The plan clearly separates transcript chunks, memory metadata, hierarchy, and
  projections.
- A future implementer can tell what must be replayable.

### KILO-0002: Sketch Initial File Format

Tasks:

- Describe segment files, manifest files, snapshots, and projector checkpoints.
- Choose initial versioning fields for mutation records and segment headers.
- Decide how checksums are represented.

Acceptance criteria:

- File-format sketch has explicit version fields.
- Corruption detection is considered before implementation begins.
- Snapshot and tail replay are described as first-class startup behavior.

### KILO-0003: Define Hierarchy Query Semantics

Tasks:

- Define required node types for human, project, session, mission, episode,
  chunk, artifact, preference, and memory record.
- Define edge types such as contains, authored_by, mentions, derived_from,
  supersedes, source_span, and evidence_for.
- Write example queries, including multi-human preference search.

Acceptance criteria:

- Query examples cover "engineering preferences with Lynn".
- The hierarchy model does not depend on embedding text alone.
- The model allows graph edges where a strict tree would be too rigid.

## Milestone 1: Go Module And Test Harness

### KILO-0101: Initialize Go Module

Tasks:

- Create `go.mod`.
- Establish initial package layout.
- Add a minimal smoke test.

Acceptance criteria:

- `go test ./...` passes.
- Public SDK package path is reserved under `pkg/kilo`.
- No server-first API is introduced.

### KILO-0102: Add Test Utilities

Tasks:

- Add temp-store helpers.
- Add crash/reopen helper patterns.
- Add deterministic sequence and clock test hooks where needed.

Acceptance criteria:

- Storage tests can open, write, close, reopen, and assert replay behavior.
- Tests avoid sleeps where deterministic synchronization is possible.

## Milestone 2: Append Log And Mutation Model

### KILO-0201: Implement Segment Append

Tasks:

- Write append-only segment files.
- Add monotonic sequence numbers.
- Add segment header and record checksums.
- Add clean close and reopen behavior.

Acceptance criteria:

- A committed mutation survives reopen.
- A partial trailing record is detected and handled according to documented
  recovery rules.
- Corrupted records fail loudly in tests.

### KILO-0202: Implement Audited CRUD Mutations

Tasks:

- Add create, update, delete/tombstone, restore, and purge-request mutation
  types.
- Add actor, session, mission, timestamp, idempotency key, and expected revision
  fields.
- Add optimistic concurrency checks.

Acceptance criteria:

- Updates with stale expected revisions are rejected.
- Retried writes with the same idempotency key do not duplicate effects.
- Deletes are tombstones by default.

## Milestone 3: Materialized Catalog

### KILO-0301: Build Current-State Record Catalog

Tasks:

- Materialize records from the mutation log.
- Track current revision, disposition, tombstone state, and timestamps.
- Add lookup by ID and list by namespace/kind.

Acceptance criteria:

- Reads are served from materialized state, not log scans.
- Replaying the same log produces the same catalog.
- Tombstoned records are hidden by default and visible when requested.

### KILO-0302: Add Secondary Metadata Indexes

Tasks:

- Index namespace, kind, tags, human, project, session, mission, persona, and
  timestamps.
- Add simple query API over these indexes.

Acceptance criteria:

- Queries combine at least namespace, kind, tag, and session filters.
- Queryable provenance includes human, project, session, mission, and persona.
- Indexes rebuild correctly after reopen.

## Milestone 4: Chunk Store

### KILO-0401: Store Transcript Chunks

Tasks:

- Add chunk create/read APIs.
- Support stable chunk IDs and sequence ordering.
- Store content hash and content length metadata.

Acceptance criteria:

- Million-scale chunk IDs can be addressed without loading chunk bodies into
  memory.
- Chunk metadata can be queried separately from chunk content.
- Chunk content survives reopen.

Current implementation notes:

- `CreateChunk`, `GetChunk`, `QueryChunks`, and `ReadChunkContent` cover the
  first create/read SDK surface.
- Chunk bodies are stored under `chunks/` by chunk-ID hash while replay only
  materializes metadata.
- Source-span citation is intentionally handled by KILO-0402.

### KILO-0402: Add Source Spans

Tasks:

- Represent source spans across chunk ranges.
- Allow memory records to point to source spans.
- Add validation for missing chunk references.

Acceptance criteria:

- A memory record can cite a transcript span without storing the transcript in
  the memory record.
- Span reads return ordered chunk references.

Current implementation notes:

- `CreateSourceSpan`, `ReadSourceSpan`, and `QuerySourceSpans` cover the first
  source-span SDK surface.
- Span creation validates the cited memory record and every referenced chunk.
- Reads return ordered chunk metadata references; body reads remain explicit
  chunk operations.

## Milestone 5: Hierarchy Graph

### KILO-0501: Implement Nodes And Edges

Tasks:

- Add node create/update/tombstone mutations.
- Add typed edge create/tombstone mutations.
- Maintain adjacency indexes.

Acceptance criteria:

- Parent-to-child and child-to-parent traversal work after replay.
- Edge tombstones remove current traversal results without erasing audit
  history.
- Node update and tombstone operations use optimistic revisions.

### KILO-0502: Implement Structural Search

Tasks:

- Add query filters for participant, project, session, mission, persona,
  node type, edge type, and time range.
- Add graph expansion options for parents, children, evidence, and source spans.

Acceptance criteria:

- A query can find all conversation episodes involving one human.
- A query can expand from matched nodes to parents, children, and evidence.
- A query can expand explicit memory-record bridges to source spans and ordered
  chunk references.

Current implementation notes:

- `ExpandSourceSpans` follows graph-node attributes named `record_id` by
  default.
- Source-span expansion returns metadata and ordered chunk references; transcript
  bodies still require explicit chunk reads.

## Milestone 6: Projection Framework

### KILO-0601: Add Projector Runtime

Tasks:

- Add projector registration.
- Add async worker lifecycle.
- Add durable projector checkpoint records.
- Add projection watermarks.

Acceptance criteria:

- Projectors resume from their checkpoint after restart.
- Store status exposes durable sequence and per-projector indexed sequence.
- Lag is visible and testable.

Current implementation notes:

- `RegisterProjector` starts an async worker that receives committed mutation
  events after the projector's durable checkpoint.
- Checkpoints are persisted under `projectors/` as one JSON watermark per
  projector.
- `Status` reports durable sequence, indexed sequence, lag, running state, and
  last error for each known projector.

### KILO-0602: Add Backpressure And Bounded Waits

Tasks:

- Add bounded wait API for projection catch-up.
- Add lag thresholds and status reporting.
- Ensure writes remain durable when projectors are behind.

Acceptance criteria:

- Search can request `min_index_seq` with a timeout.
- Timed-out searches report stale coverage explicitly.
- No projector lag test drops committed writes.

Current implementation notes:

- `WaitForProjection` waits for a projector to reach `min_index_seq` within a
  caller-specified timeout.
- Wait results return the current projector status plus `satisfied`, `stale`,
  and `timed_out` flags instead of hiding stale coverage behind success.
- Callers can enforce a max-lag threshold in addition to `min_index_seq`.
- Tests cover stale timeouts while committed writes remain readable.

## Milestone 7: Vector Search MVP

### KILO-0701: Define Vector Projection Interface

Tasks:

- Define vector insert/update/delete projection operations.
- Include node ID, node type, hierarchy scope, embedding model, dimensions, and
  source sequence.
- Keep ANN implementation swappable.

Acceptance criteria:

- Vector search code does not depend on canonical log internals.
- The interface can support exact search first and ANN later.

Current implementation notes:

- `VectorOperation` defines `vector.upsert` and `vector.delete` with node ID,
  node type, hierarchy scope, embedding model, dimensions, and source sequence.
- `VectorIndex` is the swappable apply boundary for exact or ANN-backed indexes.
- `NewVectorProjector` adapts public `ProjectionEvent` values into vector
  operations through a caller-supplied mapper, keeping embedding production out
  of the canonical store.

### KILO-0702: Implement Hierarchy-Aware Search

Tasks:

- Add semantic query over selected node types.
- Apply structural filters before vector scoring where possible.
- Add parent/child/evidence expansion.

Acceptance criteria:

- Search can combine "with Lynn" as structure and "engineering preferences" as
  semantic intent.
- Results include watermarks and stale status.
- Tests prove hierarchy filters change the candidate set before ranking.

Current implementation notes:

- `ExactVectorIndex` provides the first MVP search backend with cosine scoring,
  node-type filters, hierarchy-scope filters, candidate reporting, and projection
  watermarks.
- `FileVectorIndex` persists exact-search vector entries under `vectors/` with
  binary float32 payloads and JSON metadata, giving the embedded SDK a durable
  non-JSON storage path for imported embeddings.
- `Store.SearchVectors` enriches vector hits with parent, child, evidence, and
  source-span expansion from the materialized hierarchy without coupling the
  vector index to canonical log internals.
- Projector status can override raw index watermarks so skipped projection events
  still report accurate freshness for async vector searches.

## Milestone 8: Snapshots, Compaction, And Replay

### KILO-0801: Add Catalog Snapshots

Tasks:

- Write snapshots of materialized state.
- Load snapshot plus replay tail on startup.
- Validate snapshot version and checksum.

Acceptance criteria:

- Startup can skip old segments covered by a valid snapshot.
- Invalid snapshots fall back safely or fail with a clear error.

Current implementation notes:

- `Store.WriteSnapshot` writes `snapshots/catalog.json` with materialized
  records, chunks, source spans, graph nodes, graph edges, idempotency state,
  durable sequence, version, and checksum.
- `Open` validates snapshot type, version, and checksum, hydrates catalog state,
  and applies only segment entries after the snapshot sequence to current state.
- Segment entries covered by the snapshot are still decoded into the projector
  event queue in the single-segment development format so projector replay
  behavior remains compatible until compaction manifests exist.

### KILO-0802: Add Audited Compaction

Tasks:

- Compact old segments with tombstones and superseded records.
- Preserve auditability through compaction manifests.
- Add purge/redaction as explicit audited operations.

Acceptance criteria:

- Compaction does not change current query results.
- Audit trail can explain what was compacted.
- Purge behavior is explicit and tested separately from ordinary delete.

Current implementation notes:

- `Store.Compact` writes a fresh catalog snapshot, archives the current active
  segment under `compactions/<seq>/segments/`, rewrites the active segment with
  `base_seq = compacted_seq + 1`, and writes a checksum-protected compaction
  manifest.
- Compaction preserves current query behavior, record tombstones, graph state,
  source spans, chunks, idempotency state, and explicit purge/redaction request
  records through the snapshot.
- Known projectors must be caught up before compaction. New mutation-replay
  projectors are rejected after compaction unless they have a durable checkpoint
  at or beyond the compacted sequence.
- `purge_request.create` records explicit `purge` or `redact` requests without
  executing destructive deletion; ordinary delete remains a tombstone.

## Milestone 9: Embedded SDK API

### KILO-0901: Stabilize SDK Surface

Tasks:

- Add `Open`, `Close`, `Write`, `Read`, `Query`, `Search`, and `Status` APIs.
- Define context cancellation behavior.
- Define error types for conflicts, corruption, stale projections, and missing
  records.

Acceptance criteria:

- A small example can embed Kilo without starting a server.
- API docs describe durability and projection freshness semantics.

Current implementation notes:

- `Store.Write`, `Store.Read`, `Store.Query`, and `Store.Search` provide a
  minimal SDK facade over the lower-level audited mutation and materialized
  query primitives.
- `Open` acquires a single-owner store lock at `<store>/owner.lock`; concurrent
  opens return `ErrStoreLocked`, and `Status` includes owner metadata.
- Public calls validate exactly one operation per envelope and return caller
  context errors before doing work.
- SDK sentinel errors now include `ErrConflict`, `ErrNotFound`, `ErrCorrupt`,
  `ErrStaleProjection`, and `ErrStoreLocked`; callers should use `errors.Is`
  because operation context is preserved in wrapped errors.
- `docs/API.md` documents the embedding model, durability behavior, projection
  freshness, and stable error contract.

### KILO-0902: Add SDK Examples

Tasks:

- Add examples for transcript chunk storage.
- Add examples for memory metadata records.
- Add examples for hierarchical search.

Acceptance criteria:

- Examples run as tests.
- Examples avoid SugarFang-specific workflow policy.

Current implementation notes:

- `pkg/kilo/example_test.go` includes runnable examples for compact memory
  metadata, transcript chunk body reads, and hierarchy-aware vector search.
- The examples use the public SDK facade and avoid server setup or
  SugarFang-specific workflow policy.

## Milestone 10: Optional Server And CLI

### KILO-1001: Add Thin Server Adapter

Tasks:

- Expose SDK operations through a minimal local HTTP or Unix socket adapter.
- Keep server request handlers thin.
- Add status and inspection endpoints.

Acceptance criteria:

- Server tests exercise the same SDK behavior as direct SDK tests.
- The server can be disabled without losing core functionality.

Current implementation notes:

- `internal/httpapi` exposes read-only health, status, tail, and point
  inspection endpoints over an injected `*kilo.Store`.
- Handlers call `Store.Status`, `Store.Read`, and `Store.Tail` directly and
  return JSON responses plus machine-readable JSON errors.
- `cmd/kilo serve` starts the optional adapter; embedded SDK consumers do not
  depend on it.

### KILO-1002: Add CLI Inspection Tools

Tasks:

- Add commands for status, tail log, inspect node, inspect record, and search.
- Add machine-readable output.

Acceptance criteria:

- CLI can inspect a store created by SDK tests.
- CLI does not mutate state except through explicit commands.

Current implementation notes:

- `cmd/kilo` supports `status`, `inspect`, `tail`, and `serve` commands with
  JSON stdout for successful inspection commands.
- `inspect` covers records, nodes, edges, chunks, chunk content, source spans,
  and purge requests through the SDK `Read` facade.
- `pkg/kilo.Tail` exposes recent audited mutation entries for CLI/server
  inspection without reading segment files directly from adapters.
- CLI commands and `serve` use the SDK open path, so they respect the
  single-owner lock instead of bypassing file ownership.
- `docs/ADAPTERS.md` documents CLI commands and HTTP endpoints.

## Milestone 11: SugarFang Pilot Migration

### KILO-1101: Map Existing SugarFang Records

Tasks:

- Map current memory record fields to Kilo records.
- Map retrieval documents to hierarchy nodes and metadata.
- Map context summaries and diary anchors to source spans.

Acceptance criteria:

- Mapping preserves session, mission, persona, disposition, tags, file spans, and
  supersession.
- No SugarFang workflow policy moves into Kilo.

Current implementation notes:

- `internal/pilotimport` imports neutral `kilo.memory_import/v1` JSONL records
  into Kilo records, hierarchy nodes, source chunks, source spans, and structural
  edges.
- The mapper preserves SugarFang session, mission, mission phase, source
  persona, disposition, review, QA approval, tags, file spans, embedding
  metadata, retrieval text, and supersession metadata.
- Imported embedding arrays are applied to a caller-supplied vector index as
  typed `vector.upsert` operations; the CLI uses `FileVectorIndex` under
  `<store>/vectors` and record attributes no longer receive `embedding_json`.
- `cmd/kilo import-memory` provides the explicit mutating import path while
  ordinary inspection commands still refuse missing or uninitialized stores.
- `docs/SUGARFANG_PILOT_IMPORT.md` records the verified SugarFang memory schema
  and the Kilo-side export/import contract.

### KILO-1102: Validate Recall Parity And Improvements

Tasks:

- Import a small SugarFang project state.
- Compare current semantic recall against Kilo hierarchy-aware recall.
- Add query cases for multi-human and preference search.

Acceptance criteria:

- Kilo returns equivalent or better evidence for current recall cases.
- Query freshness and projection lag are visible.
- Performance is measured under multi-session load.

Current implementation notes:

- `internal/pilotvalidate` loads `kilo.memory_validation/v1` case plans and
  compares flat vector baseline hits against Kilo hierarchy-aware hits.
- Validation reports include expected-hit counts, missing nodes, indexed
  sequence, durable sequence, lag, stale status, and optional source/graph
  expansion results.
- `cmd/kilo validate-memory` can import neutral memory JSONL, run validation
  cases, and report parallel-session timing without reading or modifying
  SugarFang state directly.
- `docs/PILOT_VALIDATION.md` documents the plan format, CLI flow, report
  semantics, and remaining pilot proof boundary.
