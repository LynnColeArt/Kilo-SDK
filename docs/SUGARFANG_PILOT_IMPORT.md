# SugarFang Pilot Import

This document defines the first Kilo-side import contract for a SugarFang pilot.
Kilo does not read SugarFang's DuckDB files directly. SugarFang should export
memory rows into the neutral JSONL shape below, and Kilo imports that file
through the SDK.

## Verified SugarFang Shape

The current SugarFang checkout stores memory in DuckDB tables initialized by
`internal/memory/store.go`:

- `memory_records`
- `memory_retrieval_index`

The fields that matter for the first pilot are:

- `record_id`
- `kind`
- `summary`, `detail`, `rationale`
- `tags_json`
- `source`
- `session_id`
- `file_path`, `start_line`, `end_line`
- `created_at`
- `disposition`, `reviewed_by`, `supersedes_record_id`
- `metadata_json`
- `embedding_model`, `embedding_dimensions`, `embedding_json`
- `mission_id`, `mission_phase`, `source_persona`
- `qa_approved`

SugarFang retrieval documents are rebuildable projection text, currently shaped
as `sugarfang.memory.embedding_document/v1`. Kilo preserves exported retrieval
text as import metadata, but imported embedding arrays are converted into typed
vector operations instead of being stored as JSON record attributes.

## Import Contract

Each line is one JSON object:

```json
{
  "schema": "kilo.memory_import/v1",
  "record_id": "record:example",
  "kind": "context_summary",
  "summary": "Kilo should remain SDK-first.",
  "detail": "Optional compact detail.",
  "rationale": "Optional rationale.",
  "tags": ["architecture", "pilot"],
  "source": "agent",
  "session_id": "session:alpha",
  "mission_id": "mission:build",
  "mission_phase": "qa",
  "source_persona": "nora",
  "file_path": "docs/diary/qa/entry.md",
  "start_line": 12,
  "end_line": 18,
  "created_at": "2026-05-09T12:05:00Z",
  "disposition": "accepted",
  "reviewed_by": "qa",
  "supersedes_record_id": "record:older",
  "metadata": {"anchor_kind": "mission_diary", "status": "accepted"},
  "embedding_model": "sugarfang-test-embedder",
  "embedding_dimensions": 3,
  "embedding": [0.1, 0.2, 0.3],
  "retrieval_text": "{\"schema\":\"sugarfang.memory.embedding_document/v1\"}",
  "source_text": "Optional excerpt used to create a Kilo source span.",
  "qa_approved": true
}
```

`source_text` is optional. When present, Kilo creates a `source_excerpt` chunk
and a source span tied to the imported record. When absent, file span fields are
still preserved as metadata.

## Mapping

For each imported memory row, Kilo creates or reuses:

- a Kilo `Record` with the original `record_id`
- a `memory_record` hierarchy node linked back through `record_id`
- a `session` node when `session_id` exists
- a `mission` node when `mission_id` exists
- a `persona` node when `source_persona` exists
- `contains` edges from session to mission or memory
- `contains` edges from mission to memory
- `authored_by` edges from persona to memory
- `supersedes` edges when the superseded memory node exists
- a source chunk and source span when `source_text` exists
- a binary vector index entry when `embedding` exists

The importer preserves session, mission, persona, disposition, tags, file span,
review, QA approval, embedding metadata, retrieval text, and supersession data.
Embedding payloads are written to Kilo's vector index as float32 values with
hierarchy scope metadata; `embedding_json` is not written to Kilo record or node
attributes. The importer does not import SugarFang ticket law, mission launch
rules, planner permissions, or workflow policy.

## CLI

Create or update a Kilo store explicitly:

```sh
go run ./cmd/kilo import-memory \
  --path /path/to/kilo-store \
  --input /path/to/sugarfang-memory.jsonl \
  --init \
  --namespace sugarfang \
  --project-id project:example \
  --human-id human:lynn
```

`--init` is required when the target store has not already been initialized.
The CLI creates or reopens a file-backed vector index under `<store>/vectors`
for imported embeddings. Inspection commands still refuse missing or
uninitialized store paths.

## Current Limit

This slice defines the import side, a neutral export shape, and the Kilo-side
validation harness. It does not ship a SugarFang-side exporter or import a live
project by itself. Recall parity measurements can be run from Kilo with
`docs/PILOT_VALIDATION.md` once a representative export is available.
