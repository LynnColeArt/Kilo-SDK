# Kilo Optional Adapters

Kilo remains SDK-first. The CLI and HTTP server are thin inspection adapters
over `pkg/kilo`; they are optional and can be left out by embedded consumers.

## CLI

Build or run the CLI with:

```sh
go run ./cmd/kilo help
```

Commands produce JSON on stdout.

```sh
kilo status --path /path/to/store
kilo inspect record record:example --path /path/to/store
kilo inspect node node:example --path /path/to/store --include-deleted
kilo tail --path /path/to/store --limit 20
kilo import-memory --path /path/to/store --input sugarfang-memory.jsonl --init
kilo validate-memory --path /path/to/store --input sugarfang-memory.jsonl --cases validation-cases.json --init
kilo serve --path /path/to/store --addr 127.0.0.1:8765
```

Inspection kinds:

- `record`
- `node`
- `edge`
- `chunk`
- `chunk-content`
- `source-span`
- `purge-request`

Inspection commands do not mutate state, but they still open the SDK store and
therefore acquire the store-owner lock. They require an initialized store path
with an active segment, return machine-readable inspection data, and close the
SDK store. If another runtime already owns the store, inspection commands fail
with the SDK lock error instead of reading files behind that owner. `serve`
owns the lock for its process lifetime.

`import-memory` is an explicit mutating command for the SugarFang pilot import
contract documented in `docs/SUGARFANG_PILOT_IMPORT.md`.
`validate-memory` can import the same neutral file and run Kilo-side recall,
freshness, and parallel query checks documented in `docs/PILOT_VALIDATION.md`.

## HTTP

The HTTP adapter is exposed through `internal/httpapi` and is used by
`kilo serve`. It is intended for local inspection and tooling. The server sets
a read-header timeout to avoid unbounded slow-client header reads, but it does
not provide authentication, authorization, or TLS termination.

Read endpoints:

- `GET /healthz`
- `GET /v1/status`
- `GET /v1/tail?limit=20&min_seq=10`
- `GET /v1/query/records?namespace=sugarfang&project_id=project:id&kind=decision&limit=20`
- `GET /v1/records/{id}?include_deleted=true`
- `GET /v1/nodes/{id}?include_deleted=true`
- `GET /v1/edges/{id}?include_deleted=true`
- `GET /v1/chunks/{id}`
- `GET /v1/source-spans/{id}`
- `GET /v1/purge-requests/{id}`

Write endpoints:

- `POST /v1/import-memory`

`POST /v1/import-memory` accepts the SugarFang memory import record shape under
`records`, plus optional `namespace`, `project_id`, and `human_id` fields. The
served process applies the records through `pilotimport.ImportMemoryRecords`,
including vector upserts through the server-owned `FileVectorIndex`, then
returns the import counts and current durable sequence. Replaying the same
record id is idempotent and reports `skipped_records`.

Errors are JSON objects:

```json
{"error":"not found"}
```

The adapter intentionally does not define independent storage behavior. Request
handlers call `Store.Status`, `Store.Read`, `Store.Tail`, and the pilot memory
importer directly. Consequently, a served store has one file owner: the process
that called `kilo.Open` before constructing the HTTP handler.

## Security Notes

Build and run Kilo with a patched Go toolchain. QA on 2026-05-09 found standard
library vulnerabilities in the local `go1.25.4` toolchain via `govulncheck`;
they are fixed in later Go 1.25 patch releases. Kilo currently has no third
party module dependencies.
