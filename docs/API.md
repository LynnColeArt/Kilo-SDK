# Kilo Embedded SDK API

Kilo is SDK-first. Applications embed `github.com/LynnColeArt/Kilo-SDK/pkg/kilo`
and treat any future server as an adapter over the same API.

## Open And Close

```go
store, err := kilo.Open(ctx, kilo.Options{
	Path:       "/path/to/state",
	SyncWrites: true,
})
defer store.Close()
```

`Open` creates the store path, acquires the store-owner lock, validates existing
snapshots and compaction manifests, replays the mutation tail, and rebuilds
materialized state. If another process or another handle in the same process
owns the store, it returns `ErrStoreLocked`. `Close` stops async projectors,
closes the segment log, and releases the owner lock.

The lock is advisory operational state written to `<store>/owner.lock`; it is
designed to prevent split-brain file ownership, not to serialize independent
clients. Embedded applications that need multiple observers should expose those
reads through the owning runtime.

## Write

`Store.Write` accepts a `kilo.Write` envelope with exactly one operation field
set. The method converts the operation into an audited append-only mutation and
then materializes current state.

Supported operations:

- records: create, update, tombstone delete
- chunks: create chunk metadata plus body content
- source spans: create transcript or artifact citations
- nodes: create, update, tombstone delete
- edges: create, tombstone delete
- purge requests: create explicit purge or redact intent records

`IdempotencyKey`, `ExpectedRevision`, `Actor`, `At`, and `Metadata` are shared
write metadata. Stale `ExpectedRevision` values return `ErrConflict`.

## Read

`Store.Read` accepts a `kilo.Read` envelope with exactly one ID field set. It
returns `ReadResult.Found == false` for missing point reads. Records, nodes, and
edges hide tombstones by default; set `IncludeDeleted` for audit-aware reads.

Chunk body reads are explicit through `ChunkContentID`. Kilo validates stored
length and hash when reading chunk bodies and returns `ErrCorrupt` for a mismatch.

## Query

`Store.Query` accepts a `kilo.Query` envelope with exactly one query field set.
It exposes the current materialized indexes for records, chunks, source spans,
hierarchy nodes, children, and parents.

Hierarchy node queries support structural filters and expansion:

- `RelatedNodeID`, `RelatedEdgeType`, and `RelatedDirection`
- parent, child, and evidence expansion
- source-span expansion through memory-record bridge attributes

## Search

`Store.Search` currently exposes vector search through an injected
`VectorSearcher`. This keeps embedding generation and ANN choices outside the
canonical state engine.

`NewFileVectorIndex` is the embedded exact-search implementation for durable
local use. It stores vector values as binary float32 payloads under `vectors/`
with JSON metadata for node ID, hierarchy scope, model, dimensions, and source
sequence.

Search results include indexed sequence, durable sequence where available, lag,
and stale status. Set `RequireFresh` to turn stale coverage into
`ErrStaleProjection`.

```go
result, err := store.Search(ctx, kilo.Search{
	VectorSearcher: index,
	Vectors: &kilo.VectorSearchQuery{
		Vector:        []float32{1, 0},
		NodeTypes:     []string{"memory_record"},
		MinIndexedSeq: store.Status().DurableSeq,
	},
	RequireFresh: true,
})
```

## Status

`Store.Status` returns durable sequence, snapshot sequence, compaction sequence,
current owner metadata, materialized object counts, and per-projector status.
Projection status includes durable sequence, indexed sequence, lag, running
state, and the last projector error.

## Tail

`Store.Tail` returns recent audited mutation entries from the active replay
tail. `TailQuery.MinSeq` filters by sequence and `TailQuery.Limit` keeps the
most recent entries after filtering.

Tail entries are intended for inspection, debugging, and adapters. Chunk body
bytes are not included in tailed mutations.

## Context Cancellation

Public SDK calls check `ctx.Err()` before doing work and return the original
context error, such as `context.Canceled` or `context.DeadlineExceeded`.
Projection waits also respect context and caller-specified timeouts.

## Durability

Writes append a normalized mutation to the segment log before current state is
materialized. With `Options.SyncWrites`, chunk bodies, segment frames,
snapshots, compaction manifests, and projector checkpoints use fsync-oriented
paths where implemented.

Snapshots and compaction improve startup time without changing the append-only
audit model. Projections remain rebuildable and expose their watermarks instead
of pretending stale indexes are current.

## Errors

Stable SDK sentinel errors:

- `ErrConflict`: optimistic revision conflicts or create collisions
- `ErrNotFound`: missing required references or missing traversal roots
- `ErrCorrupt`: checksum, length, or hash validation failures
- `ErrStaleProjection`: caller requested fresh search but projection coverage is stale
- `ErrStoreLocked`: another handle currently owns the store path

Callers should use `errors.Is` because SDK methods wrap these sentinels with
operation-specific context.

## Runnable Examples

SDK examples live in `pkg/kilo/example_test.go` and run with:

```sh
go test ./pkg/kilo
```

They cover compact memory metadata, transcript chunk body reads, and
hierarchy-aware vector search without adding SugarFang workflow policy to Kilo.
