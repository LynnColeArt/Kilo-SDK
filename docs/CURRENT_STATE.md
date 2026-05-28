# Kilo Current State

Date: 2026-05-09

This document records the repository state after the Kilo-side MVP, QA
hardening, and single-owner store-lock work. It is a handoff document, not a
replacement for the milestone plan in `docs/TASK_LIST.md`.

## Repository

- Local path: `/home/lynn/projects/kilo-db`
- Remote: `https://github.com/LynnColeArt/Kilo-SDK.git`
- Branch: `main`
- Baseline checkpoint before the owner-lock update: `f7c04ff`
- Workflow: direct-to-main commits and pushes

No SugarFang files were changed during the Kilo implementation and QA work.

## Implemented Surface

Kilo currently provides:

- an embeddable Go SDK in `pkg/kilo`
- audited append-only mutation storage
- materialized reads and query indexes for records, chunks, source spans, graph
  nodes, graph edges, and purge requests
- optimistic revision checks and idempotency keys
- transcript and artifact chunk storage outside compact memory metadata
- source spans that connect memory records to ordered chunk references
- hierarchy graph traversal and expansion
- async projector runtime with durable watermarks and bounded waits
- exact vector search with hierarchy-scope filtering and freshness metadata
- file-backed vector storage under `vectors/` with binary float32 payloads
- catalog snapshots and audited compaction manifests
- a single-owner store lock under `owner.lock`, surfaced through
  `Store.Status().Owner` and `ErrStoreLocked`
- optional CLI and read-only HTTP inspection adapters
- Kilo-side SugarFang pilot import tooling for neutral JSONL exports
- Kilo-side pilot validation tooling for recall, freshness, and parallel query
  timing against exported data

## Important Boundaries

Kilo stores durable state. It does not contain SugarFang mission law, planner
policy, ticket semantics, or workflow authorization.

Embeddings are not optional for the intended migration path. Imported embeddings
are written into a typed vector index and are not stored as JSON attributes on
Kilo records.

The optional server is an adapter over the SDK. It is intended for local
inspection and does not provide authentication, authorization, or TLS
termination.

Only one process should own a Kilo store path at a time. CLI, HTTP, TUI, MCP,
or evaluator inspection should either run inside the owning runtime or wait for
the owner to close the store. This avoids the hidden file-handle contention
that motivated the DuckDB replacement discussion.

## Verification Baseline

The following checks passed during the QA pass for this owner-lock checkpoint:

```sh
go test -count=1 ./...
go vet ./...
go test -race -count=1 ./...
go test -cover ./...
go run ./cmd/kilo help
git diff --check
```

Coverage from `go test -cover ./...` at that checkpoint:

- `internal/cli`: 47.7%
- `internal/httpapi`: 59.8%
- `internal/pilotimport`: 76.2%
- `internal/pilotvalidate`: 80.7%
- `pkg/kilo`: 79.7%

`govulncheck` was run with:

```sh
go run golang.org/x/vuln/cmd/govulncheck@latest ./...
```

It reported 11 reachable vulnerabilities in the local `go1.25.4` standard
library, with fixes listed across later Go 1.25 patch releases. It also found
additional imported-package or required-module advisories that the code does
not appear to call. The module currently has no third-party runtime
dependencies. Kilo should be built and operated with a patched Go 1.25
toolchain.

## Known Limits

- The current vector index is exact search. The public interface is designed so
  an ANN-backed implementation can replace it later.
- The file format is versioned but should still be treated as a development
  format rather than a long-term compatibility promise.
- The Kilo-side pilot harness can validate representative exports, but this
  repository does not yet include a live SugarFang export or SugarFang-side
  exporter.
- HTTP endpoints are read-only inspection tools, not a production access
  control boundary.
- Recovery remains strict: corrupt or partial segment state fails loudly rather
  than attempting automatic repair.

## Next Evidence Step

The next non-documentation proof is to supply a representative SugarFang memory
export and a validation-case file, then run:

```sh
go run ./cmd/kilo validate-memory \
  --path /path/to/kilo-store \
  --input /path/to/sugarfang-memory.jsonl \
  --cases /path/to/validation-cases.json \
  --init \
  --parallel-sessions 8 \
  --repeats 50 \
  --pretty
```

That run should decide whether the exact file-backed vector index is sufficient
for the pilot or whether an ANN implementation is required behind the existing
vector interfaces.
