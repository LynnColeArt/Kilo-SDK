# Kilo File Format

This document describes the initial development format. It is versioned from
the start, but it is not yet a compatibility promise.

## Store Layout

```text
<store>/
  owner.lock
  segments/
    00000000000000000001.kseg
  snapshots/
    catalog.json
  compactions/
    <compacted-seq>/
      manifest.json
      segments/
        00000000000000000001.kseg
  chunks/
    <id-hash-prefix>/
      <id-hash-prefix>/
        <sha256(chunk-id)>.chunk
  projectors/
    <sha256(projector-name)>.json
  vectors/
    <sha256(node-id)>.kvec
```

The first implementation writes one append-only segment. Later milestones can
add manifests and multiple immutable segments without changing the public SDK
model.

## Owner Lock

`owner.lock` is operational metadata for embedded ownership. `Open` creates the
file, obtains an exclusive non-blocking advisory file lock where the platform
implementation supports it, and writes a small JSON object:

```json
{
  "pid": 12345,
  "hostname": "workstation",
  "path": "/path/to/store",
  "lock_path": "/path/to/store/owner.lock",
  "opened_at": "2026-05-09T00:00:00Z"
}
```

The lock prevents two independent runtimes from appending to the same segment
or maintaining divergent in-memory catalogs. It is not part of the replayable
audit stream. On clean close, Kilo clears the metadata and releases the lock;
after a crash, the operating system releases the lock and the next owner
overwrites stale metadata.

## Catalog Snapshot

`snapshots/catalog.json` stores a versioned materialized catalog snapshot:

```json
{
  "type": "kilo.catalog_snapshot",
  "version": 1,
  "data": {
    "durable_seq": 1,
    "created_at": "2026-05-09T00:00:00Z"
  },
  "checksum": "sha256:..."
}
```

The checksum is SHA-256 over the snapshot type, version, and `data` payload
with the checksum field omitted. Open validates the snapshot type, version, and
checksum before hydrating catalog state. A valid snapshot covers catalog replay
through `durable_seq`; startup then applies only segment entries after that
watermark to bring current state up to the latest durable mutation. Segment
entries at or below the snapshot watermark are still decoded into the projector
event queue while the single-segment development format remains in use, so
registered projectors preserve their existing replay behavior.

## Compaction Manifest

`compactions/<compacted-seq>/manifest.json` records each audited segment
compaction:

```json
{
  "type": "kilo.compaction_manifest",
  "version": 1,
  "data": {
    "compacted_seq": 42,
    "snapshot": {},
    "counts": {},
    "removed_frames": 42,
    "kept_frames": 0,
    "active_segment": "/path/to/store/segments/00000000000000000001.kseg",
    "archived_segment": "/path/to/store/compactions/00000000000000000042/segments/00000000000000000001.kseg",
    "created_at": "2026-05-09T00:00:00Z"
  },
  "checksum": "sha256:..."
}
```

The checksum is SHA-256 over the manifest type, version, and `data` payload
with the checksum field omitted. Compaction first writes a catalog snapshot,
archives the active segment under the compaction directory, then rewrites the
active segment to keep only frames after `compacted_seq`. The archived segment
and manifest preserve an audit explanation of what was compacted while the
snapshot preserves current catalog state including tombstones and purge or
redaction requests.

Known projectors must be caught up through the compaction sequence before
compaction can proceed. A projector with no durable checkpoint cannot be
registered after compaction because mutation events at or below
`compacted_seq` are no longer in the active replay segment; future snapshot-aware
projector rebuild support can relax that rule explicitly.

## Segment Header

The first line is JSON:

```json
{"type":"kilo.segment","version":1,"base_seq":1}
```

Unknown header versions are rejected. `base_seq` is initially `1`; after
compaction it is the first sequence still present in the active segment.

## Segment Frames

Each following line is one JSON frame:

```json
{
  "data": {
    "version": 1,
    "seq": 1,
    "payload": {}
  },
  "crc32": "00000000"
}
```

`payload` is the canonical mutation JSON. The checksum is CRC-32 over the JSON
encoding of `data`. Sequence numbers must be contiguous. Each frame must end in
a newline; a missing trailing newline is treated as a partial frame and the
store fails to open.

## Recovery Rule

The current recovery rule is strict: corrupt, partial, unsupported, or
out-of-order frames fail loudly on open. Automatic truncation/repair is deferred
until compaction and recovery tooling exists.

## Current Mutation Payload

The SDK currently writes record, chunk, node, and edge mutations:

- `record.create`
- `record.update`
- `record.delete`
- `chunk.create`
- `source_span.create`
- `node.create`
- `node.update`
- `node.delete`
- `edge.create`
- `edge.delete`
- `purge_request.create`

Record payloads include compact queryable metadata:

- namespace and project ID
- kind and summary
- tags
- human, session, mission, and persona provenance
- string attributes
- revision, tombstone, and sequence metadata materialized during replay

The materialized catalog indexes namespace, project ID, kind, tags, human,
session, mission, and persona in memory. Those indexes are rebuilt from the
segment log on open.

Chunk payloads include transcript or artifact body metadata: namespace, project
ID, session ID, mission ID, kind, stable sequence, content hash, content length,
string attributes, and replay metadata. Chunk bodies are written outside the
segment log under `chunks/`, sharded by the SHA-256 hash of the chunk ID. The
segment log remains the canonical metadata/audit stream; the chunk file is the
durable long-form body addressed by that metadata. Reads verify body length and
content hash.

Source span payloads attach memory records to ordered transcript or artifact
chunk references. A span stores record ID, namespace/provenance fields, chunk
IDs, and optional byte offsets. Span creation validates that the cited memory
record and every referenced chunk already exist. Span reads return ordered chunk
metadata references without loading transcript bodies.

Graph payloads include typed nodes and typed directed edges. The materialized
graph keeps outgoing and incoming adjacency indexes in memory. Edge tombstones
are retained for audit reads but skipped by default traversal.

Purge request payloads record explicit retention operations without executing
destructive data removal. `purge` and `redact` actions include a target type,
target ID, reason, optional string attributes, and sequence metadata. Ordinary
delete remains a tombstone; compaction preserves tombstones and purge/redaction
requests until a future audited purge executor exists.

Projector checkpoint files persist per-projector watermarks outside the
mutation segment. Each checkpoint records projector name, indexed sequence, and
update time. Projectors resume from this durable watermark on restart, while the
segment log remains the source for mutation replay.

## Current Vector Index Format

`FileVectorIndex` stores one vector entry per node under `vectors/`. Each file
is named with the SHA-256 hash of the node ID and uses this layout:

```text
KILOVEC1\n
uint32 little-endian metadata length
JSON metadata
float32 little-endian vector values
```

The JSON metadata stores node ID, node type, hierarchy scope, embedding model,
dimensions, source sequence, and optional string attributes. The vector payload
is not JSON. On open, Kilo validates the magic header, metadata length,
dimension count, and vector byte count before rebuilding the exact-search
entries in memory.

## Current Structural Query Surface

`QueryNodes` filters materialized graph nodes by type, attributes, updated-time
range, and relationship to another node through a typed edge. It can expand from
matched nodes to parents, children, evidence nodes, and source-span citations.
Source-span expansion follows explicit graph-node attributes named `record_id`
by default and returns ordered chunk metadata references; transcript body reads
remain explicit chunk operations. This keeps structural predicates such as "with
Lynn" outside embedding text and leaves semantic search free to operate on a
narrower candidate set later.
