# SQL setup files for Lakebase

Run these manually against your Lakebase Postgres database, or let the app
apply them on boot (each app's `ensure_schema()`-equivalent applies every file
here in order, idempotently, the same pattern as the sibling projects).

## Setup order

### 1. Run `01_schema.sql`

Creates the `research` schema and every table: `users`, `learning_goals`,
`papers`, `authors`, `paper_authors`, `collections`, `collection_papers`,
`reading_progress`, `notes`, plus `paper_embeddings` (pgvector, `VECTOR(384)`,
HNSW) and `citation_edges` (the reading-path graph, materialized from the
gold Delta layer so the UI never waits on a Spark round trip).

**IMPORTANT:** replace `{{EMBEDDING_DIM}}` with your model's dimension --
`lakebase.ensure_research_schema()` does this automatically from `EMBEDDING_DIM`
(default 384, matching `sentence-transformers/all-MiniLM-L6-v2`). If you change
`EMBEDDING_MODEL_NAME` in `env.example`, update `EMBEDDING_DIM` to match, or
every embedding insert fails with a dimension mismatch.

Validated offline by `scripts/check_sql.py` (pglast / libpg_query). That
catches grammar errors, not semantics -- `scripts/check_connection.py --write`
is what proves the pgvector cast and cosine arithmetic actually work against
a live instance.
