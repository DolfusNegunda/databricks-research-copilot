"""Sync gold/silver Delta tables into Lakebase -- via psycopg2 execute_values,
never spark.write.jdbc(...): Spark JDBC writes against Lakebase have proven
unreliable in this workspace (see the workspace root CLAUDE.md's Day 2
constraint, carried into every project since).

Runs on a Spark cluster (reads Delta tables via spark.read.table) but writes
through lakebase.py's psycopg2 pool. The curated subset is capstone-scale --
thousands of rows, not big-data-scale -- so collecting gold rows to the
driver before the batch write is the right call here, not a shortcut taken
under time pressure.

Write order respects foreign keys: papers, then authors, then the two tables
that reference both (paper_authors, citation_edges).

Embedding is a deliberately separate stage (pipelines/embed_papers.py) --
nothing here touches sentence-transformers or paper_embeddings. A newly
synced or changed paper sits with embedded_at IS NULL until that job runs;
see _UPSERT_PAPERS below for exactly when embedded_at gets cleared.

`spark` is runtime-injected in a Databricks notebook/job context; this file
is not run with a plain `python` interpreter.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from psycopg2.extras import execute_values

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import lakebase  # noqa: E402

_UPSERT_PAPERS = """
    INSERT INTO papers (
        work_id, title, doi, publication_year, primary_topic_domain, primary_topic_field,
        cited_by_count, fwci, foundational_score, is_oa, oa_url, language, seed_topic,
        narrative_abstract, payload
    ) VALUES %s
    ON CONFLICT (work_id) DO UPDATE SET
        title = EXCLUDED.title,
        doi = EXCLUDED.doi,
        publication_year = EXCLUDED.publication_year,
        primary_topic_domain = EXCLUDED.primary_topic_domain,
        primary_topic_field = EXCLUDED.primary_topic_field,
        cited_by_count = EXCLUDED.cited_by_count,
        fwci = EXCLUDED.fwci,
        foundational_score = EXCLUDED.foundational_score,
        is_oa = EXCLUDED.is_oa,
        oa_url = EXCLUDED.oa_url,
        language = EXCLUDED.language,
        seed_topic = EXCLUDED.seed_topic,
        narrative_abstract = EXCLUDED.narrative_abstract,
        payload = EXCLUDED.payload,
        synced_at = now(),
        -- Only clear embedded_at (forcing re-embedding) when the text that
        -- gets embedded actually changed. papers.narrative_abstract here
        -- refers to the PRE-update row (standard ON CONFLICT DO UPDATE
        -- semantics), EXCLUDED to the incoming one -- comparing them inline
        -- like this needs no separate content-hash column.
        embedded_at = CASE
            WHEN papers.narrative_abstract IS DISTINCT FROM EXCLUDED.narrative_abstract THEN NULL
            ELSE papers.embedded_at
        END
"""

_UPSERT_AUTHORS = """
    INSERT INTO authors (author_id, display_name, orcid) VALUES %s
    ON CONFLICT (author_id) DO UPDATE SET
        display_name = EXCLUDED.display_name,
        orcid = EXCLUDED.orcid
"""

_UPSERT_PAPER_AUTHORS = """
    INSERT INTO paper_authors (work_id, author_id, author_position, is_corresponding) VALUES %s
    ON CONFLICT (work_id, author_id) DO UPDATE SET
        author_position = EXCLUDED.author_position,
        is_corresponding = EXCLUDED.is_corresponding
"""

_UPSERT_CITATION_EDGES = """
    INSERT INTO citation_edges (citing_work_id, cited_work_id) VALUES %s
    ON CONFLICT (citing_work_id, cited_work_id) DO NOTHING
"""


def _batched_upsert(cur, sql: str, rows: list[tuple], template: str | None = None, batch_size: int = 1000) -> int:
    total = 0
    for i in range(0, len(rows), batch_size):
        chunk = rows[i : i + batch_size]
        execute_values(cur, sql, chunk, template=template)
        total += len(chunk)
    return total


def sync() -> dict:
    bootstrap = lakebase.ensure_research_schema()
    if not bootstrap["ok"]:
        raise lakebase.LakebaseUnavailable(f"Schema bootstrap failed: {bootstrap['error']}")

    papers = [
        (
            r["work_id"],
            r["title"],
            r["doi"],
            r["publication_year"],
            r["primary_topic_domain"],
            r["primary_topic_field"],
            r["cited_by_count"],
            r["fwci"],
            r["foundational_score"],
            r["is_oa"],
            r["oa_url"],
            r["language"],
            r["seed_topic"],
            r["narrative_abstract"],
            json.dumps(json.loads(r["payload"])) if isinstance(r["payload"], str) else json.dumps(r["payload"]),
        )
        for r in spark.read.table("gold_papers_for_serving").collect()  # noqa: F821
    ]
    authors = [
        (r["author_id"], r["display_name"], r["orcid"]) for r in spark.read.table("silver_authors").collect()  # noqa: F821
    ]
    paper_authors = [
        (r["work_id"], r["author_id"], r["author_position"], r["is_corresponding"])
        for r in spark.read.table("silver_paper_authors").collect()  # noqa: F821
    ]
    citation_edges = [
        (r["citing_work_id"], r["cited_work_id"]) for r in spark.read.table("gold_citation_graph").collect()  # noqa: F821
    ]

    counts = {"papers": 0, "authors": 0, "paper_authors": 0, "citation_edges": 0}
    with lakebase.get_connection() as conn:
        with conn.cursor() as cur:
            counts["papers"] = _batched_upsert(cur, _UPSERT_PAPERS, papers)
            counts["authors"] = _batched_upsert(cur, _UPSERT_AUTHORS, authors)
            counts["paper_authors"] = _batched_upsert(cur, _UPSERT_PAPER_AUTHORS, paper_authors)
            counts["citation_edges"] = _batched_upsert(cur, _UPSERT_CITATION_EDGES, citation_edges)
        conn.commit()

    return counts


if __name__ == "__main__":
    result = sync()
    print(f"synced: {result}")
