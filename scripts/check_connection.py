"""Live checks against a real Lakebase instance. Needs LAKEBASE_URL (or the
database/lakebase-url secret, or PGHOST) configured -- see env.example.

    python scripts/check_connection.py            # read-only
    python scripts/check_connection.py --write     # + a self-cleaning write/read round trip

What the offline suites (check_api.py, check_sql.py) structurally cannot see
is semantics: pglast accepts SQL that computes the wrong number, and nothing
offline can prove the pgvector cast and cosine arithmetic actually work
against a live instance. That's what --write is for.
"""

from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import lakebase  # noqa: E402

passed = 0
failed: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    global passed
    if condition:
        passed += 1
        print(f"PASS {name}")
    else:
        failed.append(name)
        print(f"FAIL {name} :: {detail}")


def python_cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    return dot / (norm_a * norm_b)


def main() -> None:
    write_mode = "--write" in sys.argv

    summary = lakebase.target_summary()
    print(f"target: {summary}")
    if summary["auth"] == "unconfigured":
        check("lakebase is configured", False, "set LAKEBASE_URL, a secret, or PGHOST -- see env.example")
        print(f"\n{passed} passed, {len(failed)} failed")
        sys.exit(1)

    bootstrap = lakebase.ensure_research_schema()
    check("schema bootstrap succeeds", bootstrap["ok"], str(bootstrap.get("error")))
    if not bootstrap["ok"]:
        print(f"\n{passed} passed, {len(failed)} failed")
        sys.exit(1)

    rows = lakebase.run_query("SELECT 1 FROM pg_extension WHERE extname = 'vector'")
    check("pgvector extension is installed", len(rows) == 1)

    rows = lakebase.run_query(
        "SELECT format_type(atttypid, atttypmod) AS t FROM pg_attribute "
        "WHERE attrelid = 'paper_embeddings'::regclass AND attname = 'embedding'"
    )
    dim_type = rows[0]["t"] if rows else None
    check(
        "paper_embeddings.embedding is vector(384)",
        dim_type == "vector(384)",
        f"got {dim_type!r} -- format_type(), not information_schema.udt_name, so a stale "
        "wrong-dimension column can't pass silently",
    )

    expected_tables = {
        "users", "learning_goals", "papers", "authors", "paper_authors",
        "collections", "collection_papers", "reading_progress", "notes",
        "paper_embeddings", "citation_edges",
    }
    rows = lakebase.run_query(
        "SELECT tablename FROM pg_tables WHERE schemaname = current_schema()"
    )
    found_tables = {r["tablename"] for r in rows}
    missing = expected_tables - found_tables
    check("every schema table exists", not missing, f"missing: {missing}")

    if write_mode:
        _write_checks()

    print(f"\n{passed} passed, {len(failed)} failed")
    if failed:
        for name in failed:
            print(f"  FAILED: {name}")
        sys.exit(1)
    print("ALL CHECKS PASSED")


def _write_checks() -> None:
    test_work_id = "TEST_CHECK_CONNECTION_W1"
    vector_a = [0.1] * 384
    vector_b = [0.1] * 191 + [0.9] * 193  # deliberately different direction from a

    lakebase.run_write("DELETE FROM papers WHERE work_id = %s", (test_work_id,))
    try:
        lakebase.run_write(
            "INSERT INTO papers (work_id, title, payload) VALUES (%s, %s, %s::jsonb)",
            (test_work_id, "check_connection test row", "{}"),
        )

        from psycopg2.extras import execute_values

        with lakebase.get_connection() as conn:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    "INSERT INTO paper_embeddings (embedding_id, work_id, chunk_index, chunk_text, embedding, model_name) VALUES %s",
                    [
                        (f"{test_work_id}:0", test_work_id, 0, "chunk a", str(vector_a).replace(" ", ""), "test"),
                        (f"{test_work_id}:1", test_work_id, 1, "chunk b", str(vector_b).replace(" ", ""), "test"),
                    ],
                    template="(%s, %s, %s, %s, %s::vector, %s)",
                )
            conn.commit()

        rows = lakebase.run_query(
            "SELECT 1 - (embedding <=> embedding) AS self_sim FROM paper_embeddings "
            "WHERE embedding_id = %s",
            (f"{test_work_id}:0",),
        )
        self_sim = rows[0]["self_sim"] if rows else None
        check("self-similarity is exactly 1.0", self_sim is not None and abs(self_sim - 1.0) < 1e-9, str(self_sim))

        rows = lakebase.run_query(
            "SELECT 1 - (a.embedding <=> b.embedding) AS sim FROM paper_embeddings a, paper_embeddings b "
            "WHERE a.embedding_id = %s AND b.embedding_id = %s",
            (f"{test_work_id}:0", f"{test_work_id}:1"),
        )
        pg_sim = rows[0]["sim"] if rows else None
        py_sim = python_cosine(vector_a, vector_b)
        check(
            "Postgres cosine similarity matches an independently computed Python cosine",
            pg_sim is not None and abs(pg_sim - py_sim) < 1e-6,
            f"postgres={pg_sim} python={py_sim}",
        )
    finally:
        lakebase.run_write("DELETE FROM papers WHERE work_id = %s", (test_work_id,))
        rows = lakebase.run_query("SELECT 1 FROM paper_embeddings WHERE work_id = %s", (test_work_id,))
        check("cleanup: cascade removed the test embeddings too", len(rows) == 0)


if __name__ == "__main__":
    main()
