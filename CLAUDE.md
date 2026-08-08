# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

An AI Research & Learning Copilot over the OpenAlex scholarly graph, built as
the capstone for the Databricks "Rise of the AI Data Engineer" bootcamp (see
`../CLAUDE.md` for the shared workspace conventions this project follows —
git identity, deploy sequence, the `search_path`/`options` Lakebase
constraint, the three-script verification pattern). This file covers only
what's specific to this project.

**Status: early build.** Read this alongside the plan file (ask the user for
its path if not obvious from context) before assuming anything about pieces
not yet listed below as done — `app/` and `mcp_server/` are empty folders
right now, not partially-built apps.

## The corpus-density finding — the reason this project is shaped the way it is

**Do not "simplify" the harvester back to pulling an OpenAlex Parquet
snapshot partition.** That was the original plan, and it was measured and
rejected before any pipeline code was written against it:

- A single `updated_date` partition's induced citation subgraph measures at
  **0.09–0.18% edge density** under every filtering strategy tried (whole
  partition, CS-only, by publication year, CS+year). Only 1–3% of papers have
  even one citation edge to another paper in the same pulled slice.
- The cause is structural, not fixable by filtering harder: `updated_date` is
  uncorrelated with citation structure, so a partition is close to a
  *uniform random* sample of the works graph. The odds a cited work is also
  in that same random sample are close to chance (slice_size / corpus_size).
- The fix, measured on a spike, not assumed: seed on a topic query, snowball
  outward via `referenced_works` (inline, free) and `filter=cites:{id}`
  (batches with `|`-OR the same as `openalex_id:` — confirmed via the API's
  own `x_query` echo), batch-resolve new ids 100-at-a-time. That measures at
  **19.14% in-corpus density** — roughly 100–150× denser — because both
  endpoints of real citation edges are deliberately included by construction.

This is why `harvester/snowball.py` exists as its own auditable stage instead
of the pipeline reading the snapshot's works entity directly. The snapshot is
still used — for the *other* 20 entity types (`authors`, `institutions`,
`topics`, `sources`), which are dimension tables, not for works.

## Commands (what exists so far)

```bash
# offline verification -- no network, no Databricks, no Lakebase
python scripts/check_api.py    # 36 checks: abstract reconstruction, reading-path
                                # ordering, harvester's pure helpers
python scripts/check_sql.py    # 3 checks: every sql/*.sql statement via pglast

# the harvester (needs network; hits the live OpenAlex API)
python harvester/snowball.py --out-dir ./harvest --per-topic 200
```

`scripts/check_connection.py` (live, against Lakebase) does not exist yet —
add it when `sql/01_schema.sql` is first applied to a real instance, mirroring
the sibling projects' version of that script.

## Architecture (as built so far)

- **`abstract_reconstruction.py`** (root) — `reconstruct_abstract()`. OpenAlex
  never exposes a plain `abstract` field, only `abstract_inverted_index`
  (`{word: [positions]}`), and it arrives as a dict from the REST API but as
  a **JSON string** from Parquet. Handles both; returns `None` (never `""`)
  on anything unusable. This is the single most common way to silently get an
  empty embedding corpus, which is why it has its own dedicated, tested
  module rather than being an inline lambda in the pipeline.
- **`reading_path.py`** (root) — `build_reading_path()`, the signature
  feature. A topological sort over citation edges restricted to a retrieved
  set, with a deterministic `foundational_score` tie-break and graceful
  handling of cycles and disconnected papers (both real, not edge cases to
  design away). Ordering is computed from data; an agent explains it, never
  invents it. Raises `KeyError` on an edge referencing a paper outside the
  supplied set, deliberately — the induced-subgraph restriction is the
  caller's job (upstream, in SQL), not this function's.
- **`harvester/snowball.py`** — the corpus-density fix, see above. Seed →
  cited → citing → resolve, per topic, for 8 seed topics chosen by measuring
  `meta.count` before picking (thousands-to-low-100k; coherent technical
  subfields, not generic buzzwords that dilute density). Writes one JSON
  Lines file per topic. Pure helpers (`short_id`, `batched`, `build_or_filter`)
  are separated from the network-calling functions specifically so the former
  can be checked offline in `scripts/check_api.py` without hitting the API.
- **`sql/01_schema.sql`** — Lakebase DDL. `research` schema, created on boot
  via the `pg_namespace` guard (same pattern as every sibling project — see
  `../CLAUDE.md` for why the guard exists). `papers.seed_topic` records which
  of the 8 harvest topics pulled each work in.

## Conventions specific to this project

- **`abstract_reconstruction.py` and `reading_path.py` live at the repo root**,
  not nested under `pipelines/`, because both are shared across deploy
  boundaries: the pipeline needs `reconstruct_abstract` as a silver-layer
  transform, and the MCP server / app both need `build_reading_path` at
  request time. Per the established sibling convention, each Databricks App
  folder gets its own copy at deploy time rather than importing across the
  app boundary — do this when `app/` and `mcp_server/` are actually built,
  don't import from the root at runtime from inside either app folder.
- **8 seed topics, not 1.** Multi-topic on purpose: it's more data (LinkedIn
  "rich in data" goal), and it means the demo app can support several
  distinct, realistic learning goals instead of one. See `SEED_TOPICS` in
  `harvester/snowball.py` for the list and why each was picked.
- **`harvester/` has no `requirements.txt`.** It only uses the stdlib
  (`urllib`, `json`, `argparse`) — deliberately, so it has nothing to install
  before it can run as either a plain script or the Databricks job in
  `resources/openalex_harvest_job.yml`.
