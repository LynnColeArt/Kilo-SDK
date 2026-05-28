# Kilo Pilot Validation

This document defines the Kilo-side validation harness for the SugarFang pilot.
It does not read or modify SugarFang repositories or DuckDB files. It consumes
neutral exported memory JSONL plus a validation-case file.

## Validation Plan

Validation plans are JSON files with this schema:

```json
{
  "schema": "kilo.memory_validation/v1",
  "parallel_sessions": 4,
  "repeats": 25,
  "cases": [
    {
      "name": "engineering preferences with Lynn",
      "query": "all conversations about engineering preferences with Lynn",
      "query_vector": [1.0, 0.0, 0.0],
      "embedding_model": "example-embedder",
      "hierarchy_scope": {
        "human_id": "human:lynn",
        "kind": "preference"
      },
      "expected_record_ids": ["record:lynn-engineering-pref"],
      "limit": 5
    }
  ]
}
```

`expected_record_ids` are converted to memory node IDs by prefixing
`node:memory:`. `expected_node_ids` may be used directly when a case targets
non-record nodes.

## CLI

Import an exported memory file and validate it in one explicit operation:

```sh
go run ./cmd/kilo validate-memory \
  --path /path/to/kilo-store \
  --input /path/to/sugarfang-memory.jsonl \
  --cases /path/to/validation-cases.json \
  --init \
  --namespace sugarfang \
  --project-id project:example \
  --human-id human:lynn \
  --parallel-sessions 8 \
  --repeats 50 \
  --pretty
```

Validate an already-imported Kilo store by omitting `--input`:

```sh
go run ./cmd/kilo validate-memory \
  --path /path/to/kilo-store \
  --cases /path/to/validation-cases.json \
  --pretty
```

The command uses `<store>/vectors` for the file-backed vector index. It exits
non-zero when any validation case fails.

## Report Semantics

Each case runs two searches:

- baseline: vector search over the selected node types without hierarchy scope
- Kilo: vector search with hierarchy scope and optional graph/source expansion

The report includes baseline hits, hierarchy-aware Kilo hits, missing expected
nodes, expected-hit counts, indexed sequence, durable sequence, lag, and stale
status. When `require_fresh` or `--require-fresh` is set, stale coverage makes
the case fail.

When `parallel_sessions` and `repeats` are non-zero, Kilo runs the case set in
parallel and reports total query count, queries per second, p50, p95, and max
latency. This is a harness measurement, not a substitute for a real
production-sized soak. The harness rejects validation plans that would run more
than 1,000,000 measured queries in one invocation.

## Completion Boundary

Kilo can now prove the migration contract against exported fixtures and pilot
datasets without changing SugarFang. The remaining product proof is to supply a
representative SugarFang export, run this harness, and decide whether exact
search is enough for the pilot or whether the stable vector interface should be
backed by an ANN index.
