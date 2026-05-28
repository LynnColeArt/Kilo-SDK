# Kilo Agent Guide

Kilo is a Go-first embeddable storage SDK for hierarchical, auditable memory and
transcript state. Treat any server binary as an adapter over the SDK, not the
center of the project.

## Working Style

- Practice TDD: add or update a failing test before implementing meaningful
  behavior.
- Keep changes small, reviewed, and easy to replay mentally.
- Prefer simple designs with clear ownership boundaries over clever shared
  abstractions.
- Apply SOLID and DRY where they reduce real coupling or duplication. Do not
  add abstractions only to look architectural.
- Keep implementation code in Go unless a future benchmark proves a narrow
  low-level component needs another language.

## Core Boundaries

- Canonical truth is append-only and auditable.
- Current state is materialized from canonical truth.
- Vector, FTS, salience, and summary indexes are rebuildable projections.
- Long transcripts and large artifacts are chunked content, not memory metadata.
- Memory records are compact metadata with provenance, hierarchy, and pointers.
- Hierarchy is first-class. Do not flatten human/project/session/mission/episode
  relationships into JSON-only vector text.
- Kilo stores state. SugarFang workflow policy and mission law stay outside the
  storage core.

## Packaging Model

- Public SDK API lives under `pkg/kilo`.
- Internal storage packages stay under `internal`.
- Optional process/server adapters live under `cmd`.
- Tests should exercise the SDK directly before testing adapters.
- File formats and log records need explicit versions from the start.

## Safety Rules

- Do not silently drop writes when projectors lag.
- Expose watermarks for durable state and each projection.
- Prefer tombstones and audited compaction over untracked deletion.
- Do not make vector indexing synchronous on hot write paths unless a caller
  explicitly requests and bounds the wait.
