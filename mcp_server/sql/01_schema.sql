-- Lakebase schema for the AI Research & Learning Copilot capstone.
-- Own copy for mcp_server/ -- see mcp_server/lakebase.py's _SQL_DIR:
-- Databricks Apps deployment only uploads files from within this app's own
-- folder, never sibling directories like the repo-root sql/ this was copied
-- from (the same bug confirmed live for app/'s identical copy:
-- FileNotFoundError on '.../sql/01_schema.sql' on every deploy, silently
-- caught by ensure_research_schema()'s own error handling, so nothing
-- crashed -- it just never actually bootstrapped the schema from this app,
-- relying entirely on the Job-based path having already done it). Keep in
-- sync by hand with the root sql/01_schema.sql.
--
-- Tables named exactly per the capstone brief (users, learning_goals, papers,
-- authors, paper_authors, collections, collection_papers, reading_progress,
-- notes), plus two serving-layer additions the brief implies but doesn't
-- name: paper_embeddings (pgvector) and citation_edges (the reading-path
-- graph, materialized from the gold Delta layer so the UI never waits on a
-- Spark round trip for an interactive feature).
--
-- __SCHEMA__ / __SCHEMA_NAME__ substitution and the CREATE SCHEMA guard
-- follow the pattern already proven across the Day 1-3 projects.

DO $ensure_schema$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = '__SCHEMA_NAME__') THEN
        EXECUTE 'CREATE SCHEMA __SCHEMA__';
    END IF;
END;
$ensure_schema$;

-- Extensions are database-wide, not schema-scoped -- CREATE EXTENSION IF NOT
-- EXISTS alone silently no-ops the moment ANY project sharing this Lakebase
-- instance has already installed pgvector into ITS OWN primary schema, which
-- leaves the `vector` type unresolvable from this schema's search_path even
-- though the extension technically "exists" (confirmed live: this failed
-- with "type vector does not exist" on CREATE TABLE, well past the
-- IF NOT EXISTS that apparently found nothing to do). Check where it
-- actually lives and adapt instead of assuming it's already reachable.
DO $ensure_vector_extension$
DECLARE
    _vector_schema TEXT;
BEGIN
    SELECT extnamespace::regnamespace::text INTO _vector_schema
    FROM pg_extension WHERE extname = 'vector';

    IF _vector_schema IS NULL THEN
        CREATE EXTENSION vector SCHEMA public;
    ELSIF _vector_schema NOT IN ('__SCHEMA_NAME__', 'public') THEN
        -- Installed by some other project's own bootstrap, into its own
        -- schema. Don't move or duplicate it -- just widen this session's
        -- search_path so the type resolves for the rest of this script too.
        EXECUTE format('SET search_path TO %I, public, %I', '__SCHEMA_NAME__', _vector_schema);
    END IF;
END;
$ensure_vector_extension$;

-- ---------------------------------------------------------------- users ----
CREATE TABLE IF NOT EXISTS __SCHEMA__.users (
    user_id      TEXT PRIMARY KEY,        -- the signed-in Databricks identity (email)
    display_name TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- --------------------------------------------------------- learning_goals ----
CREATE TABLE IF NOT EXISTS __SCHEMA__.learning_goals (
    goal_id      BIGSERIAL PRIMARY KEY,
    user_id      TEXT NOT NULL REFERENCES __SCHEMA__.users (user_id) ON DELETE CASCADE,
    title        TEXT NOT NULL,
    description  TEXT,
    status       TEXT NOT NULL DEFAULT 'active',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT learning_goals_title_not_blank
        CHECK (length(btrim(title)) > 0),
    CONSTRAINT learning_goals_status_valid
        CHECK (status IN ('active', 'paused', 'completed', 'archived'))
);

-- ---------------------------------------------------------------- papers ----
-- work_id is the bare OpenAlex id ("W4389984066"), never the full URL --
-- normalized once at ingest so every join/lookup elsewhere can assume the
-- short form.
CREATE TABLE IF NOT EXISTS __SCHEMA__.papers (
    work_id            TEXT PRIMARY KEY,
    title              TEXT NOT NULL,
    doi                TEXT,
    publication_year   INTEGER,
    primary_topic_domain TEXT,
    primary_topic_field  TEXT,
    cited_by_count     INTEGER NOT NULL DEFAULT 0,
    fwci               DOUBLE PRECISION,
    foundational_score DOUBLE PRECISION NOT NULL DEFAULT 0,
    is_oa              BOOLEAN NOT NULL DEFAULT false,
    oa_url             TEXT,
    language           TEXT,
    seed_topic         TEXT,               -- which of the 8 harvest seed queries pulled this work in
    narrative_abstract TEXT,               -- reconstructed from abstract_inverted_index
    payload            JSONB NOT NULL,     -- raw provenance
    synced_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    embedded_at        TIMESTAMPTZ,        -- NULL => needs (re-)embedding

    CONSTRAINT papers_title_not_blank
        CHECK (length(btrim(title)) > 0),
    CONSTRAINT papers_year_plausible
        CHECK (publication_year IS NULL OR publication_year BETWEEN 1800 AND 2100)
);

CREATE INDEX IF NOT EXISTS idx_papers_unembedded
    ON __SCHEMA__.papers (synced_at) WHERE embedded_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_papers_topic_field
    ON __SCHEMA__.papers (primary_topic_field);
CREATE INDEX IF NOT EXISTS idx_papers_year
    ON __SCHEMA__.papers (publication_year);
CREATE INDEX IF NOT EXISTS idx_papers_seed_topic
    ON __SCHEMA__.papers (seed_topic);

-- --------------------------------------------------------------- authors ----
CREATE TABLE IF NOT EXISTS __SCHEMA__.authors (
    author_id    TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    orcid        TEXT,
    payload      JSONB
);

CREATE TABLE IF NOT EXISTS __SCHEMA__.paper_authors (
    work_id         TEXT NOT NULL REFERENCES __SCHEMA__.papers (work_id) ON DELETE CASCADE,
    author_id       TEXT NOT NULL REFERENCES __SCHEMA__.authors (author_id) ON DELETE CASCADE,
    author_position INTEGER,
    is_corresponding BOOLEAN NOT NULL DEFAULT false,

    PRIMARY KEY (work_id, author_id)
);

-- ----------------------------------------------------------- collections ----
CREATE TABLE IF NOT EXISTS __SCHEMA__.collections (
    collection_id BIGSERIAL PRIMARY KEY,
    user_id       TEXT NOT NULL REFERENCES __SCHEMA__.users (user_id) ON DELETE CASCADE,
    goal_id       BIGINT REFERENCES __SCHEMA__.learning_goals (goal_id) ON DELETE SET NULL,
    name          TEXT NOT NULL,
    description   TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT collections_name_not_blank
        CHECK (length(btrim(name)) > 0)
);

CREATE TABLE IF NOT EXISTS __SCHEMA__.collection_papers (
    collection_id  BIGINT NOT NULL REFERENCES __SCHEMA__.collections (collection_id) ON DELETE CASCADE,
    work_id        TEXT NOT NULL REFERENCES __SCHEMA__.papers (work_id) ON DELETE CASCADE,
    added_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    sequence_rank  INTEGER,   -- filled in when a reading path is generated; NULL until then

    PRIMARY KEY (collection_id, work_id)
);

-- ------------------------------------------------------- reading_progress ----
CREATE TABLE IF NOT EXISTS __SCHEMA__.reading_progress (
    user_id      TEXT NOT NULL REFERENCES __SCHEMA__.users (user_id) ON DELETE CASCADE,
    work_id      TEXT NOT NULL REFERENCES __SCHEMA__.papers (work_id) ON DELETE CASCADE,
    status       TEXT NOT NULL DEFAULT 'not_started',
    started_at   TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (user_id, work_id),
    CONSTRAINT reading_progress_status_valid
        CHECK (status IN ('not_started', 'in_progress', 'completed', 'abandoned'))
);

-- ------------------------------------------------------------------ notes ----
CREATE TABLE IF NOT EXISTS __SCHEMA__.notes (
    note_id       BIGSERIAL PRIMARY KEY,
    user_id       TEXT NOT NULL REFERENCES __SCHEMA__.users (user_id) ON DELETE CASCADE,
    work_id       TEXT NOT NULL REFERENCES __SCHEMA__.papers (work_id) ON DELETE CASCADE,
    collection_id BIGINT REFERENCES __SCHEMA__.collections (collection_id) ON DELETE SET NULL,
    note_text     TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT notes_text_not_blank
        CHECK (length(btrim(note_text)) > 0)
);

CREATE INDEX IF NOT EXISTS idx_notes_user_work
    ON __SCHEMA__.notes (user_id, work_id);

-- ------------------------------------------------------------ embeddings ----
CREATE TABLE IF NOT EXISTS __SCHEMA__.paper_embeddings (
    embedding_id TEXT PRIMARY KEY,      -- "{work_id}:{chunk_index}"
    work_id      TEXT NOT NULL REFERENCES __SCHEMA__.papers (work_id) ON DELETE CASCADE,
    chunk_index  INTEGER NOT NULL,
    chunk_text   TEXT NOT NULL,
    embedding    VECTOR({{EMBEDDING_DIM}}) NOT NULL,
    model_name   TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT paper_embeddings_work_chunk_unique
        UNIQUE (work_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_paper_embeddings_hnsw
    ON __SCHEMA__.paper_embeddings USING hnsw (embedding vector_cosine_ops);

-- -------------------------------------------------------- citation_edges ----
-- Materialized from gold_citation_graph (Delta) for interactive serving --
-- the reading-path feature must not wait on a Spark round trip per request.
CREATE TABLE IF NOT EXISTS __SCHEMA__.citation_edges (
    citing_work_id TEXT NOT NULL REFERENCES __SCHEMA__.papers (work_id) ON DELETE CASCADE,
    cited_work_id  TEXT NOT NULL REFERENCES __SCHEMA__.papers (work_id) ON DELETE CASCADE,

    PRIMARY KEY (citing_work_id, cited_work_id)
);

CREATE INDEX IF NOT EXISTS idx_citation_edges_cited
    ON __SCHEMA__.citation_edges (cited_work_id);

-- ------------------------------------------------------------- triggers ----
CREATE OR REPLACE FUNCTION __SCHEMA__.set_updated_at()
RETURNS TRIGGER AS $set_updated_at$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$set_updated_at$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS learning_goals_set_updated_at ON __SCHEMA__.learning_goals;
CREATE TRIGGER learning_goals_set_updated_at
    BEFORE UPDATE ON __SCHEMA__.learning_goals
    FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.set_updated_at();

DROP TRIGGER IF EXISTS reading_progress_set_updated_at ON __SCHEMA__.reading_progress;
CREATE TRIGGER reading_progress_set_updated_at
    BEFORE UPDATE ON __SCHEMA__.reading_progress
    FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.set_updated_at();

DROP TRIGGER IF EXISTS notes_set_updated_at ON __SCHEMA__.notes;
CREATE TRIGGER notes_set_updated_at
    BEFORE UPDATE ON __SCHEMA__.notes
    FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.set_updated_at();
