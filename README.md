# Research Copilot

An AI Research & Learning Copilot over the [OpenAlex](https://openalex.org)
scholarly graph — 250M+ works, citations, and topics. Set a learning goal,
get back a **reading path sequenced by real citation dependency** (not an
LLM's guess at ordering), track progress, and ask an agent that can both
retrieve papers and take real write actions on your behalf.

Built for the Databricks "Rise of the AI Data Engineer" bootcamp capstone —
see the [capstone brief](https://github.com/EcZachly/databricks-ai-bootcamp-capstone)
(idea #3) — as a **Lakeflow Declarative Pipeline** (medallion architecture,
schema contracts, `dp.expect*` quality gates), a **Lakebase** (Postgres +
pgvector) serving layer, an **MCP server** with read and write tools, and an
**Agent Bricks** agent, fronted by a Databricks App.

**Status: early build.** This README will grow into the full project writeup
as each piece lands; see the build order below for what's done vs. pending.

## Why citation-graph ordering, and why it isn't trivial

A naive corpus — pull a slice of OpenAlex's works and hope citations land
inside it — measures at under 0.2% induced-subgraph density: almost nothing
in a random slice cites anything else in that same slice, because citation
edges span the *entire* 250M+-work graph, not a time-bounded neighborhood of
it. This project instead builds each topic's corpus as a **citation
snowball** (seed papers → what they cite → what cites them → resolve),
measured at **~19% in-corpus density** — roughly 100–150× denser — because
the corpus is constructed so that real citation edges land on both ends by
design, not by chance. That measurement, and why it matters, is the core
data-engineering decision behind this project.

## Structure

```
harvester/       topic-seeded citation-snowball harvest (OpenAlex API)
pipelines/       Lakeflow Declarative Pipeline: bronze -> silver -> gold
sql/             Lakebase (Postgres) schema
app/             Databricks App -- Flask UI
mcp_server/      Databricks App -- FastMCP server, read + write tools
resources/       Databricks Asset Bundle job/pipeline definitions
scripts/         offline + live verification (check_api / check_sql / check_connection)
docs/            setup notes, Agent Bricks configuration
```

`abstract_reconstruction.py` and `reading_path.py` sit at the repo root as
shared logic (imported by the pipeline and, per the established convention in
this bootcamp's sibling projects, duplicated into each Databricks App folder
rather than imported across the app deployment boundary).

## Build order

1. ✅ Corpus strategy — measured, corrected from partition-slicing to
   topic-seeded snowball harvesting (see above).
2. ✅ Reading-path algorithm + Lakebase schema, offline-verified.
3. ✅ Declarative pipeline: bronze → silver → gold, with `dp.expect*` gates
   (`pipelines/openalex_pipeline.py`). Syntax-verified and hand-checked
   against current Databricks docs; **not yet run against a real Databricks
   pipeline** — `spark`/`dp` are runtime-injected and can't be exercised
   locally. First live pipeline run is the next real proof point.
4. 🔲 Lakebase batch writer + embeddings.
5. 🔲 MCP server + Agent Bricks registration.
6. 🔲 App UI, including the dependency-graph view.
7. 🔲 Verification suites green end to end; this README rewritten in full.

## Verification

```bash
python scripts/check_api.py    # offline: data-shape handling + reading-path correctness
python scripts/check_sql.py    # offline: SQL grammar, via pglast
python scripts/check_connection.py   # live: Lakebase (once built)
```
