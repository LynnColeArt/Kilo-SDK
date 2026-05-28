# Kilo Project Plan

## Product Model

Kilo is an embeddable Go SDK for durable, hierarchical memory state in busy
multi-agent systems. The primary consumer is SugarFang, but Kilo should be
usable as a small local state engine by other agent runtimes.

The right mental model is:

```text
embedded SDK first
optional server facade second
storage engine discipline throughout
```

Kilo should not start as a standalone database server that SugarFang talks to
over HTTP. It should start as a library SugarFang can embed directly, with an
optional local server or Unix socket adapter for tools and inspection.

## Problem Statement

SugarFang currently stores context and memory metadata in DuckDB. Hierarchical
context is partly represented as structured fields and partly as JSON embedded
into flat vector documents. This works for early semantic recall, but it does
not make hierarchy, multi-human identity, replay, or async vector projection
first-class storage concerns.

Kilo should replace that substrate with a purpose-built local state engine that
supports:

- high concurrency across busy multi-agent sessions
- full CRUD semantics through audited immutable mutations
- replayable canonical state
- hierarchical memory and transcript relationships
- long transcript storage at millions-of-chunks scale
- async vector search with explicit freshness watermarks
- an SDK surface designed for embedding in SugarFang

## Design Principles

### 1. Mutations Are Canonical

CRUD operations are public API semantics. Internally, each write becomes an
immutable mutation record in an append-only log.

Current state is materialized from the log. Projections can be rebuilt from the
log and snapshots.

### 2. Hierarchy Is Structural

Human, workspace, project, session, mission, role lane, episode, chunk, artifact,
and memory metadata relationships must be indexable structure. They should not
exist only as JSON text inside an embedding payload.

Example hierarchy:

```text
human
  workspace
    project
      session
        mission or role lane
          episode
            transcript chunk or artifact pointer
              memory metadata record
```

The hierarchy should be a graph where needed, not a strict tree. A memory record
may point to source spans, superseded records, decisions, tickets, artifacts, and
human preference nodes.

### 3. Metadata And Long-Form Content Are Different

Memory metadata is compact: kind, summary, tags, provenance, hierarchy, source
pointers, timestamps, disposition, and embedding references.

Long-form content is stored as transcript or artifact chunks. The memory layer
points to those chunks instead of becoming a document store by accident.

### 4. Projections Are Rebuildable

Vector indexes, FTS indexes, salience views, summaries, and hierarchy-aware
search paths are derived from canonical state. They expose watermarks and lag.

Writes must not be silently dropped when projectors fall behind.

### 5. Replay Must Be Fast Enough

Replay does not mean replaying all history from zero on every startup. Kilo
needs snapshots, segment manifests, and tail replay so large stores reopen
quickly.

## Milestones

### Milestone 0: Contracts And File Format Sketch

Define the SDK-first architecture, canonical mutation record shape, hierarchy
node model, projection watermark model, and initial on-disk format strategy.

Outcome: implementation can begin without re-litigating the storage boundaries.

### Milestone 1: Go Module And Test Harness

Create the Go module, package layout, core test utilities, crash/reopen test
helpers, and basic CI commands.

Outcome: every later storage behavior can be developed test-first.

### Milestone 2: Append Log And Mutation Model

Implement segmented append-only mutation logging with checksums, versions,
monotonic sequence numbers, idempotency keys, and optimistic revision checks.

Outcome: Kilo can durably accept audited CRUD mutations.

### Milestone 3: Materialized Catalog

Build the current-state catalog for records, chunks, hierarchy nodes, edges,
tombstones, and revisions.

Outcome: reads do not scan the log during normal operation.

### Milestone 4: Chunk Store

Add long transcript and artifact chunk storage with stable chunk IDs, optional
content hashes, range lookup, and source-span references.

Outcome: Kilo supports long conversations without stuffing bodies into memory
metadata.

### Milestone 5: Hierarchy Graph

Implement first-class hierarchy nodes and edges for humans, projects, sessions,
missions, episodes, chunks, artifacts, preferences, and memory records.

Outcome: queries like "all conversations about engineering preferences with
Lynn" can combine structural predicates with semantic search.

### Milestone 6: Projection Framework

Implement async projector workers, durable projector checkpoints, backpressure,
watermarks, and stale-result reporting.

Outcome: vector and search indexes can lag without lying.

### Milestone 7: Vector Search MVP

Implement hierarchy-aware vector search over selected node types. Start with an
exact or simple partitioned index if that keeps tests clear, but preserve the
interface needed for ANN later.

Outcome: semantic search can descend through hierarchy instead of scanning one
flat vector soup.

### Milestone 8: Snapshots, Compaction, And Replay

Implement snapshots for materialized state, segment compaction, tombstone
handling, and audited purge/redaction operations.

Outcome: large stores reopen quickly and remain maintainable over time.

### Milestone 9: Embedded SDK API

Stabilize the public Go API for opening a store, writing mutations, reading
current state, querying hierarchy, searching projections, and inspecting
watermarks.

Outcome: SugarFang can embed Kilo directly without depending on a server process.

Current reference: `docs/API.md`.

### Milestone 10: Optional Server And CLI

Add a thin server/CLI adapter for inspection, debugging, smoke tests, and tools
that cannot embed the SDK.

Outcome: Kilo can be operated externally without compromising the SDK-first
model.

Current reference: `docs/ADAPTERS.md`.

### Milestone 11: SugarFang Pilot Migration

Map SugarFang's current memory/context records into Kilo, dual-write or import a
small project, compare recall behavior, and validate performance under
multi-session load.

Outcome: Kilo proves it can replace DuckDB for this slice without losing
SugarFang semantics.

Current reference: `docs/SUGARFANG_PILOT_IMPORT.md`.

Validation reference: `docs/PILOT_VALIDATION.md`.

## Open Questions

- What is the first supported namespace boundary: project, workspace, or store?
- Which hierarchy node types are required for the first SugarFang pilot?
- Which CRUD operations need strict read-your-write projection waits?
- Should transcript chunks be content-addressed, sequence-addressed, or both?
- What is the first acceptable vector index: exact partitioned search, HNSW, or
  pluggable ANN behind a stable interface?
- What is the expected local durability policy: fsync every write, group commit,
  or caller-selectable durability classes?
