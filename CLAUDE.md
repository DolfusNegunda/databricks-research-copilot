# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

An AI Research & Learning Copilot over the OpenAlex scholarly graph, built as
the capstone for the Databricks "Rise of the AI Data Engineer" bootcamp (see
`../CLAUDE.md` for the shared workspace conventions this project follows —
git identity, deploy sequence, the `search_path`/`options` Lakebase
constraint, the three-script verification pattern). This file covers only
what's specific to this project.

**Status: feature-complete, not live-verified.** Every piece in the build
order is written: harvester, declarative pipeline, Lakebase schema + writer +
embedding, the MCP server (`mcp_server/`), and the Flask app (`app/`). What's
missing is a live run against a real Databricks workspace/Lakebase instance —
this was built in a sandbox with no Spark runtime and no provisioned
Lakebase, so verification stops at: offline checks (green), import-level
construction of both `FastMCP`/`Flask` apps (real, not just syntax-checked),
and hand-review against current API docs. See README.md's "what's verified
vs. what isn't" section before assuming anything beyond that has been proven.

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
# offline verification -- no network, no Databricks, no live Lakebase
python scripts/check_api.py    # 58 checks: abstract reconstruction, reading-path
                                # ordering, harvester's pure helpers, chunking/vector
                                # formatting, frontend conventions (no innerHTML, id
                                # cross-check), run_query() write-safety (ast-parsed),
                                # _render_schema_sql() placeholder substitution (x4 copies)
python scripts/check_sql.py    # 12 checks: sql/, app/sql/, mcp_server/sql/ via pglast

# the harvester (needs network; hits the live OpenAlex API)
python harvester/snowball.py --out-dir ./harvest --per-topic 200
python harvester/land_topics.py --out ./harvest/topics/topics.jsonl

# live, needs Lakebase configured (env.example)
python scripts/check_connection.py           # read-only: pgvector, schema, dimension
python scripts/check_connection.py --write   # + a self-cleaning cosine round trip

# run either app locally (needs Lakebase configured; each has its own copy
# of lakebase.py/embedder.py/reading_path.py, see "Conventions" below)
cd app && python app.py            # http://localhost:8000
cd mcp_server && python main.py    # http://localhost:8000/mcp (or DATABRICKS_APP_PORT)
```

`lakebase.py`/`embedder.py` import psycopg2/SQLAlchemy/sentence-transformers
(the last one lazily) -- installed via the root `requirements.txt` for local
dev, and per-task via `resources/openalex_sync_job.yml`'s `libraries:` blocks
for the Databricks Job path. `scripts/check_api.py` importing `embedder` does
**not** load the model (lazy singleton, see `embedder.get_model()`); importing
`lakebase` does **not** open a connection (pool is lazy too) -- both stay
genuinely offline-safe to import despite needing those packages installed.

## Architecture (as built so far)

- **`abstract_reconstruction.py`** (root) — `reconstruct_abstract()`. OpenAlex
  never exposes a plain `abstract` field, only `abstract_inverted_index`
  (`{word: [positions]}`), and it arrives as a dict from the REST API but as
  a **JSON string** from Parquet. Handles both; returns `None` (never `""`)
  on anything unusable. This is the single most common way to silently get an
  empty embedding corpus, which is why it has its own dedicated, tested
  module rather than being an inline lambda in the pipeline. The canonical,
  `check_api.py`-tested copy — **not** the one the pipeline actually runs, see
  the `pipelines/openalex_pipeline.py` bullet below for why.
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
- **`pipelines/openalex_pipeline.py`** — bronze (`bronze_works_raw` via Auto
  Loader over the harvester's JSON, `bronze_dim_topics` via Auto Loader over
  the landed snapshot entity) → silver (`silver_works` with the schema
  contract + `dp.expect_all_or_drop`/`dp.expect` gates plus
  `dropDuplicates(["work_id"])` -- NOT `dp.expect_or_fail`, see below --
  `silver_authors`/`silver_paper_authors`/`silver_citation_edges` exploded
  from it, `silver_topics_dim`) → gold (`gold_citation_graph` restricted to
  the induced subgraph, `gold_paper_metrics` with `foundational_score`,
  `gold_topic_rollups`, `gold_author_metrics`, `gold_papers_for_serving`).
  Cross-table reads use `spark.read.table(...)`/`spark.readStream.table(...)`
  — confirmed against current docs; `dlt.read()`/`dlt.read_stream()` do not
  exist in the `pyspark.pipelines` module despite being the classic spelling.
  `abstract_inverted_index` is pinned to `STRING` via `cloudFiles.schemaHints`
  in bronze specifically because its keys are the abstract's own words (an
  unbounded vocabulary) — without the hint, Auto Loader's schema inference
  tries to build one struct field per distinct word ever seen. Its own
  `_reconstruct_abstract` is a deliberate copy of the root
  `abstract_reconstruction.py`, not an import of it: `F.udf()` ships the
  wrapped function to executors via cloudpickle, which for an importably-named
  function pickles *by reference* (executors would need to
  `import abstract_reconstruction` themselves — nothing distributes that root
  file there); defining it inline makes cloudpickle serialize it *by value*
  instead, sidestepping the question entirely. `check_api.py` tests the root
  copy, not this one — keep them in sync by hand.
  **`silver_works` originally hard-failed on a duplicate `work_id` via
  `dp.expect_or_fail("no_duplicate_work_ids", "count(*) OVER (PARTITION BY
  work_id) = 1")` — wrong on two counts, caught on a live run, not in
  review.** Window functions can't be evaluated inside a WHERE clause in
  standard SQL, and `dp.expect*`'s row-level expectations apply their
  predicate exactly that way, so this never could have passed. Even fixed
  syntactically, hard-failing would have been the wrong response anyway:
  the same paper legitimately gets pulled into more than one seed topic's
  snowball, so a repeated `work_id` across topic files is expected data
  (`harvester/snowball.py`'s own harvest summary reports a pre-dedup total
  for exactly this reason), not a row to reject the whole pipeline over.
  `dropDuplicates(["work_id"])` replaced it — same fix already used for
  `silver_authors`.
- **`resources/openalex_pipeline.yml`, `resources/openalex_harvest_job.yml`,
  `resources/openalex_sync_job.yml`** — the Asset Bundle definitions. The
  pipeline's `schema:` is a Unity Catalog schema for Delta tables; Lakebase's
  `research` schema is an unrelated Postgres schema in a different system —
  same word, don't conflate them.
- **`lakebase.py`** — connection helper, mirroring the sibling projects'
  proven shape exactly (pooled `ThreadedConnectionPool`, three-path auth,
  `SET search_path` per checkout never libpq `options`, `pg_namespace` guard
  before `CREATE SCHEMA`). `ensure_research_schema(embedding_dim=384)`
  substitutes `__SCHEMA__`/`__SCHEMA_NAME__`/`{{EMBEDDING_DIM}}` into
  `sql/01_schema.sql` before executing it. **This was broken in all four
  copies until a live run caught it**: `_render_schema_sql()` originally
  only substituted `{{EMBEDDING_DIM}}` — `__SCHEMA__`/`__SCHEMA_NAME__` were
  never wired up outside `scripts/check_sql.py`'s own, separate, hardcoded
  substitution used purely for offline grammar-checking. Every live
  `CREATE TABLE IF NOT EXISTS __SCHEMA__.papers` therefore actually ran with
  `__SCHEMA__` completely unsubstituted -- and since Postgres folds
  unquoted identifiers to lowercase, `__SCHEMA__` is a syntactically valid
  schema name on its own, so it created a real, working table named
  literally `"__schema__".papers`, no error, nothing to notice, until
  `sync_to_lakebase.py`'s bare `INSERT INTO papers` (relying on
  `search_path`, which never included that schema) failed with
  `relation "papers" does not exist`. `scripts/check_api.py` now imports
  all four copies and asserts the rendered SQL text is placeholder-free and
  actually references `LAKEBASE_SCHEMA` — this offline check would have
  caught it without ever touching a live database.
- **pgvector's `vector` type is not guaranteed to be on `search_path` just
  because the extension "exists".** Extensions are database-wide, not
  schema-scoped: on this shared Lakebase instance, some other bootcamp
  project's own bootstrap may have already installed pgvector into ITS OWN
  primary schema, so `CREATE EXTENSION IF NOT EXISTS vector` in
  `sql/01_schema.sql` silently no-ops (it already exists, just not
  somewhere this project's `search_path` reaches) — confirmed live, twice:
  once inside `ensure_research_schema()`'s own bootstrap, and again inside
  `embed_papers.py`, which never calls `ensure_research_schema()` at all
  and went straight to a `::vector` insert. `get_connection()` in every
  copy of `lakebase.py` now resolves this once per process
  (`_resolve_vector_extension_schema`, cached, never re-queried) and adds
  the extension's real schema to `search_path` on every checkout if it
  isn't already `LAKEBASE_SCHEMA`/`public` — fixed at the one function
  every caller already goes through, not by remembering to bootstrap
  first at each new call site.
- **`embedder.py`** — chunking + embedding, same shape as the weather-rag
  sibling's. Only module that imports `sentence_transformers`, only lazily
  inside `get_model()`.
- **`pipelines/sync_to_lakebase.py`** — gold/silver Delta → Lakebase via
  psycopg2 `execute_values`, in FK order (papers → authors → paper_authors /
  citation_edges). Clears `papers.embedded_at` only when
  `narrative_abstract` actually changed, compared inline against the
  pre-update row inside the same `ON CONFLICT DO UPDATE` — no separate
  content-hash column needed for that comparison. Its own `CATALOG`/`SCHEMA`
  constants fully-qualify every `spark.read.table(...)` call
  (`rise_of_ai_de.research_copilot.gold_papers_for_serving`, not the bare
  name `openalex_pipeline.py`'s `@dp.table` defs use) — this runs as a plain
  `spark_python_task` on its own job cluster, a different Spark session than
  the pipeline's, with no default catalog/schema to inherit unqualified
  names against. Keep these constants in sync by hand with
  `resources/openalex_pipeline.yml`'s `catalog:`/`schema:`.
- **`pipelines/embed_papers.py`** — deliberately separate from sync (same
  reason as the weather-rag sibling: sync is cheap, embedding loads a real
  model). Batches the model's `encode()` call across every paper fetched in
  one round, not one call per paper. Deletes a paper's existing chunks before
  reinserting, so a shrinking chunk count can't leave stale rows behind.
- **`mcp_server/`** — FastMCP app. Structure verified against Databricks' own
  official template (`databricks/app-templates/mcp-server-hello-world`), not
  assumed from the bootcamp lab: `FastMCP(...).http_app(stateless_http=True)`
  combined with a plain FastAPI app into one `combined_app`, identity captured
  by a `@combined_app.middleware("http")` function into a `ContextVar`
  (`identity.py`) — not a `BaseHTTPMiddleware` subclass, and not
  `mcp.run(transport="http", ...)`, both of which are the older pattern the
  lab uses. 14 tools in `tools.py` (7 read, 7 write); every write tool calls
  `identity.current_user_email()` rather than accepting a user id as an
  argument — an agent could otherwise write data as any user it chose to name.
- **`app/`** — Flask UI. `app.py` reads `x-forwarded-email` directly per
  request (no ContextVar needed — Flask's request context is already
  request-scoped). `static/css/app.css` is the sibling design system's base
  copied verbatim through the scrollbar section, with this project's own
  additions below the "research copilot page" comment — the dependency-graph
  SVG view (`static/js/app.js`'s `buildGraph()`) lays out nodes in a wrapping
  grid ordered by reading rank rather than a full DAG layered layout; every
  position and edge drawn is still real data, not decorative.

**Known limitation, not a bug: `search_papers`'s HNSW index isn't actually
used.** Both `app/app.py`'s `/api/search` and `mcp_server/tools.py`'s
`_SEARCH_PAPERS` dedupe multiple chunks per paper via
`DISTINCT ON (p.work_id) ... ORDER BY p.work_id, similarity DESC` before the
outer `ORDER BY similarity DESC LIMIT`. That inner ordering is by `work_id`
first, not by the `<=>` distance the `idx_paper_embeddings_hnsw` index (see
`sql/01_schema.sql`) is built to accelerate, so Postgres falls back to a full
scan + sort rather than an ANN index walk. Correct results, real index,
neither one exercising the other -- fine at this corpus's scale, but don't
cite it as evidence the HNSW index does anything here. Fixing it means
ranking within a `LATERAL` per-paper subquery instead of a flat
`DISTINCT ON`; not done, since it's a performance property, not a
correctness one.

## Conventions specific to this project

- **`abstract_reconstruction.py` and `reading_path.py` live at the repo root**
  as the canonical, `check_api.py`-tested reference implementations — but
  neither is actually *imported* by anything outside `scripts/check_api.py`.
  `pipelines/openalex_pipeline.py` carries its own inlined
  `_reconstruct_abstract` (a `pyspark.sql.functions.udf` cloudpickle
  constraint, see its bullet above). `lakebase.py`/`embedder.py` specifically
  are duplicated in **four** places now (root, `app/`, `mcp_server/`,
  `pipelines/`), for **two different reasons**: `app/`/`mcp_server/` can't
  import across their own deployment boundary, same as every sibling
  project's Databricks Apps; `pipelines/` needs its own copies for an
  unrelated reason found live — `sync_to_lakebase.py`/`embed_papers.py` run
  as a `spark_python_task` via `exec(compile(source, filename, 'exec'))`
  inside an IPython kernel, which never injects `__file__` into that exec'd
  namespace, so the root copies' `sys.path.insert(Path(__file__)...)` import
  trick `NameError`s before either import runs. A same-directory
  `import lakebase` needs no path computation at all, which is why the fix
  is a copy, not a patched path. `pipelines/` does not need its own copy of
  `reading_path.py` — nothing in `pipelines/` imports it. Keep all of these
  in sync by hand with the root originals when fixing a bug in one; nothing
  enforces this automatically, same tradeoff the sibling projects accepted
  for their own copied-then-diverged CSS.
- **`sql/01_schema.sql` is duplicated into `app/sql/` and `mcp_server/sql/`
  too, for the same deployment-boundary reason as `lakebase.py`/`embedder.py`
  above — found live, the hard way.** `app/lakebase.py`'s and
  `mcp_server/lakebase.py`'s `_SQL_DIR` originally pointed at the repo-root
  `sql/` (two `.parent` calls up from each file), which works for a local
  checkout but not for either deployed App: Databricks Apps deployment only
  uploads files from *within* the app's own folder, never sibling
  directories, so the repo-root `sql/` simply isn't there. Every deploy of
  both apps hit `FileNotFoundError` on `ensure_research_schema()`'s own
  module-load bootstrap call, silently caught by its own error handling
  (by design, so it can't crash the app) — meaning neither app has ever
  actually applied its own schema; both have only ever worked because the
  Job-based path (which *does* get the full repo tree) already had. Fixed
  by copying the file in and changing `_SQL_DIR` to a single `.parent`.
  `scripts/check_sql.py` now validates all three copies (12 checks, not 4)
  so a schema edit applied only to the root file fails offline instead of
  silently drifting.
- **8 seed topics, not 1.** Multi-topic on purpose: richness of data was an
  explicit goal, and it means the demo app supports several distinct,
  realistic learning goals instead of one. See `SEED_TOPICS` in
  `harvester/snowball.py` for the list and why each was picked.
- **`harvester/` has no `requirements.txt`.** `snowball.py` is pure stdlib
  (`urllib`, `json`, `argparse`) and needs nothing extra. `land_topics.py`
  also imports `pyarrow` to read the Parquet snapshot's footer/row-groups
  directly over HTTPS -- not stdlib, and (unlike on a classic Databricks
  Runtime cluster, where it ships preinstalled) not assumed present on
  serverless either, so `land_topics_env`'s `spec.dependencies` in
  `resources/openalex_harvest_job.yml` declares it explicitly. Running
  `land_topics.py` by hand outside Databricks needs `pip install pyarrow`
  first (also in the root `requirements.txt`, for that same reason).
- **This workspace only permits serverless compute for jobs** -- a
  `new_cluster:` block on any task is a hard `400 INVALID_PARAMETER_VALUE`
  at deploy time, not just a style preference. Every `spark_python_task` in
  `resources/openalex_harvest_job.yml` and `resources/openalex_sync_job.yml`
  instead declares `environment_key:` pointing at an entry in that job's own
  `environments:` list (`spec.environment_version` + `spec.dependencies:` --
  the serverless equivalent of a cluster's `libraries:` block, which
  serverless doesn't support at all). `environment_version: "2"` is current
  as of this writing; if a future deploy rejects it, the error names the
  versions that are actually supported -- bump the string, this isn't a
  design decision worth debating. `resources/openalex_pipeline.yml` is
  unaffected: Lakeflow Declarative Pipelines have their own `serverless:`
  boolean, a completely different mechanism from a Job task's compute.
