# Research Copilot

An AI Research & Learning Copilot over the [OpenAlex](https://openalex.org)
scholarly graph. Set a learning goal, search papers semantically, and get
back a **reading path sequenced by real citation dependency** — not an LLM's
guess at ordering, but a topological sort over actual citation edges, with an
agent that can both retrieve papers and take real write actions (creating
collections, saving notes, recording progress) on your behalf.

Built for the Databricks "Rise of the AI Data Engineer" bootcamp capstone —
see the [capstone brief](https://github.com/EcZachly/databricks-ai-bootcamp-capstone)
(idea #3) and the [OpenAlex API docs](https://developers.openalex.org/).

## Why citation-graph ordering, and why it isn't trivial

The obvious way to build this corpus — pull a slice of OpenAlex's works
snapshot and hope citations land inside it — was tried, measured, and
rejected. A single Parquet partition's induced citation subgraph measures at
**0.09–0.18% density** regardless of how it's filtered (by field, by year, by
both): only 1–3% of papers have even one citation edge to another paper in
the same pulled slice. The cause is structural, not fixable by filtering
harder — `updated_date` (what a partition is keyed on) is uncorrelated with
citation structure, so a partition is close to a *uniform random* sample of
a 250M+-work graph, and the odds a cited work is *also* in that same random
sample are close to chance.

The fix, also measured: build each topic's corpus as a **citation snowball**
instead — seed on a topic query, pull the union of what the seeds cite and
what cites them, batch-resolve the new ids, repeat. That measures at
**19.14% in-corpus density** on a spike — roughly **100–150× denser** —
because both endpoints of a real citation edge are deliberately included by
construction, not left to chance. `harvester/snowball.py` is that fix,
operationalized. See `CLAUDE.md` for the full numbers.

## Architecture

```
OpenAlex REST API                        OpenAlex Parquet snapshot (S3)
  topic-seeded citation snowball           the `topics` taxonomy only
  (harvester/snowball.py, 8 topics)        (harvester/land_topics.py)
            |                                     |
            v                                     v
   +----------------------------------------------------+
   |  Lakeflow Declarative Pipeline (pipelines/)          |
   |  bronze -> silver -> gold, dp.expect* quality gates   |
   +----------------------------------------------------+
            |                         |
   gold Delta (citation graph,  curated subset ---> Lakebase (Postgres + pgvector)
   foundational_score, topic    via psycopg2          operational serving
   rollups, author metrics)     execute_values               |
   pipelines/sync_to_lakebase.py                +-------------+-------------+
   pipelines/embed_papers.py                    |                           |
                                        Databricks App (app/)      MCP server (mcp_server/)
                                        Flask + vanilla JS         FastMCP, 7 read + 7 write tools
                                        reading path + graph                |
                                                                   Agent Bricks agent
```

**The split that matters:** Spark/Delta owns heavy analytics — the citation
graph, foundational scoring, topic rollups. Lakebase owns operational serving
and vector search. The batch writer pushes the curated subset into Lakebase
via **psycopg2 `execute_values`, never `spark.write.jdbc(...)`** — Spark JDBC
writes against Lakebase have proven unreliable in this bootcamp's other
projects (see the workspace root `CLAUDE.md`).

## Structure

```
harvester/         topic-seeded citation-snowball harvest + topics-taxonomy landing (OpenAlex)
pipelines/         Lakeflow Declarative Pipeline (bronze/silver/gold) + Lakebase sync + embedding
sql/               Lakebase (Postgres) schema
app/               Databricks App -- Flask UI (own copy of lakebase.py/embedder.py/reading_path.py)
mcp_server/        Databricks App -- FastMCP server, 7 read + 7 write tools (own copies too)
resources/         Databricks Asset Bundle job/pipeline definitions
scripts/           verification: check_api.py, check_sql.py, check_connection.py
docs/              Agent Bricks registration steps
abstract_reconstruction.py, reading_path.py, lakebase.py, embedder.py
                   canonical, check_api.py-tested reference copies at the repo
                   root; duplicated, not imported, everywhere they're actually
                   used -- inlined in pipelines/ (a UDF/cloudpickle
                   constraint) and copied into app/ and mcp_server/ (a
                   deployment-boundary convention) -- see CLAUDE.md for both
```

## Setup

```bash
cp env.example .env      # fill in LAKEBASE_URL or rely on the secret scope
pip install -r requirements.txt
python scripts/check_api.py   # offline, no dependencies beyond the above
python scripts/check_sql.py   # offline, needs pglast: pip install pglast
```

To actually run the pipeline and populate Lakebase, in a Databricks workspace:

0. **Prerequisites, once**: a Unity Catalog Volume for the harvester to land
   into (nothing creates this automatically):
   ```sql
   CREATE SCHEMA IF NOT EXISTS main.research_copilot;
   CREATE VOLUME IF NOT EXISTS main.research_copilot.raw;
   ```
   Also confirm the shared Lakebase secret already exists (`database` /
   `lakebase-url`) — it does if you've deployed any sibling bootcamp project
   in this workspace already; this project has no `setup_secrets.py` of its
   own and deliberately reuses that one (see the workspace root `CLAUDE.md`).
1. **Deploy the bundle**: `databricks bundle deploy` from the repo root —
   creates the pipeline (`resources/openalex_pipeline.yml`) and both jobs
   (`resources/openalex_harvest_job.yml`, `resources/openalex_sync_job.yml`).
   Fill in your workspace host in `databricks.yml` first.
2. **Harvest**: run the `openalex-snowball-harvest` job (Jobs UI or
   `databricks bundle run openalex_snowball_harvest`) — its two tasks land
   the citation-snowball corpus and the topics taxonomy into the Volume.
3. **Pipeline**: run the `openalex-research-copilot` pipeline — produces the
   bronze/silver/gold Delta tables. Check the pipeline's event log / DQ
   metrics for expectation drop counts after the first run.
4. **Sync + embed**: run the `openalex-sync-to-lakebase` job — writes the
   curated subset into Lakebase, then embeds it.
5. **Deploy the two Apps**: `app/` and `mcp_server/`, each via the workspace's
   Git-folder → Pull → Deploy sequence (see the workspace root `CLAUDE.md`) —
   independently; deploying one does not deploy the other.
6. **Register the agent**: see `docs/agent_bricks_setup.md` — then set
   `AGENT_SERVING_ENDPOINT` in `app/app.yaml` and redeploy `app/` (step 5
   again) to wire the chat panel to it.

## MCP tools (`mcp_server/tools.py`)

**Read:** `search_papers`, `get_reading_path`, `compare_papers`,
`get_collection`, `get_progress`, `explain_citation_link`, `find_prerequisites`.

**Write:** `create_learning_goal`, `create_collection`, `add_to_collection`,
`remove_from_collection`, `record_reading_progress`, `save_note`,
`save_reading_plan`.

Every write tool sources the acting user from the `x-forwarded-email` header
Databricks Apps injects (`mcp_server/identity.py`) — never from a
caller-supplied argument, and every tool that mutates a collection verifies
the current user owns it first. `get_reading_path` computes but doesn't
persist; `save_reading_plan` computes the same thing and writes
`sequence_rank` — the read/write split the capstone brief asks for.

## Verification

```bash
python scripts/check_api.py                 # offline: 50 checks
python scripts/check_sql.py                  # offline: 3 checks, via pglast
python scripts/check_connection.py           # live: pgvector, schema, dimension via format_type()
python scripts/check_connection.py --write   # live: + a self-cleaning cosine round trip
```

`check_api.py` covers data-shape handling (abstract reconstruction from
OpenAlex's inverted index, in both its dict and JSON-string shapes),
reading-path correctness (clean DAGs, cycles, disconnected sets, deterministic
tie-breaks), the harvester's pure helpers, chunking/vector-literal formatting,
two frontend conventions checked by scanning `app/`'s source text rather than
executing it (`app.js` never uses `innerHTML`, and every `getElementById`
call has a matching element in `index.html`), and one Lakebase write-safety
convention checked the same way: parsing `mcp_server/tools.py` and
`app/app.py` with `ast` to confirm no SQL containing `INSERT`/`UPDATE`/
`DELETE` is ever passed to `run_query()`, which never commits (see
`lakebase.py`).

## What's verified vs. what isn't

Built and offline-tested in this sandbox: the abstract-reconstruction and
reading-path algorithms (hand-verified expected outputs), the SQL schema
(pglast grammar-checked), the harvester's pure logic, and both the MCP server
and the Flask app's import-level wiring (constructing the real `FastMCP`/
`Flask` apps, registering every tool/route, with no live network).

**Not run against live Databricks, Spark, or Lakebase** — this sandbox has
neither a Spark runtime nor a provisioned Lakebase instance. The declarative
pipeline, the harvester's live API calls beyond the corpus-density spike, the
sync/embed jobs, and `check_connection.py` are all correct-by-construction and
verified against current API documentation, not executed end to end. That's
the next real proof point, in an actual workspace.
