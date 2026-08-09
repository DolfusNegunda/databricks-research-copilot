"""MCP tools: 7 read, 7 write. Every write tool sources the acting user from
identity.current_user_email() -- never from a caller-supplied argument, and
every tool that operates on a collection/note/goal verifies the current user
owns it before mutating, so one user's agent session can't act on another
user's data by guessing an id.

Ordering (build_reading_path, reading_path.py) is always computed from real
citation_edges rows, never asked of the calling agent to invent -- these
tools are the only place that ordering comes from.
"""

from __future__ import annotations

import embedder
import lakebase
from reading_path import Paper, build_reading_path

_ENSURE_USER = "INSERT INTO users (user_id) VALUES (%s) ON CONFLICT (user_id) DO NOTHING"

_SEARCH_PAPERS = """
    SELECT * FROM (
        SELECT DISTINCT ON (p.work_id)
            p.work_id, p.title, p.publication_year, p.primary_topic_field, p.primary_topic_domain,
            p.cited_by_count, p.foundational_score, p.is_oa, p.oa_url, p.seed_topic,
            1 - (e.embedding <=> %(vector)s::vector) AS similarity
        FROM paper_embeddings e
        JOIN papers p ON p.work_id = e.work_id
        WHERE (%(topic_field)s::text IS NULL OR p.primary_topic_field = %(topic_field)s)
          AND (%(year_from)s::int IS NULL OR p.publication_year >= %(year_from)s)
          AND (%(open_access_only)s IS FALSE OR p.is_oa)
        ORDER BY p.work_id, similarity DESC
    ) deduped
    ORDER BY similarity DESC
    LIMIT %(top_k)s
"""

_COLLECTION_PAPERS = """
    SELECT p.work_id, p.title, p.foundational_score
    FROM collection_papers cp JOIN papers p ON p.work_id = cp.work_id
    WHERE cp.collection_id = %s
"""

_INDUCED_EDGES = """
    SELECT citing_work_id, cited_work_id FROM citation_edges
    WHERE citing_work_id = ANY(%(ids)s) AND cited_work_id = ANY(%(ids)s)
"""

_PAPER_BY_ID = "SELECT * FROM papers WHERE work_id = %s"

_EDGE_BETWEEN = """
    SELECT citing_work_id, cited_work_id FROM citation_edges
    WHERE (citing_work_id = %s AND cited_work_id = %s) OR (citing_work_id = %s AND cited_work_id = %s)
"""

_COLLECTION_BY_ID = "SELECT * FROM collections WHERE collection_id = %s"

_COLLECTION_PAPERS_ORDERED = """
    SELECT p.work_id, p.title, p.foundational_score, cp.sequence_rank, cp.added_at
    FROM collection_papers cp JOIN papers p ON p.work_id = cp.work_id
    WHERE cp.collection_id = %s
    ORDER BY cp.sequence_rank NULLS LAST, cp.added_at
"""

_PROGRESS_FOR_USER = """
    SELECT rp.work_id, p.title, rp.status, rp.started_at, rp.completed_at
    FROM reading_progress rp JOIN papers p ON p.work_id = rp.work_id
    WHERE rp.user_id = %(user_id)s
      AND (%(collection_id)s::bigint IS NULL OR rp.work_id IN (
          SELECT work_id FROM collection_papers WHERE collection_id = %(collection_id)s
      ))
"""

_PREREQUISITES = """
    SELECT p.work_id, p.title, p.foundational_score
    FROM citation_edges ce JOIN papers p ON p.work_id = ce.cited_work_id
    WHERE ce.citing_work_id = %s
    ORDER BY p.foundational_score DESC
"""

_INSERT_LEARNING_GOAL = """
    INSERT INTO learning_goals (user_id, title, description) VALUES (%s, %s, %s)
    RETURNING goal_id
"""

_INSERT_COLLECTION = """
    INSERT INTO collections (user_id, goal_id, name, description) VALUES (%s, %s, %s, %s)
    RETURNING collection_id
"""

_COLLECTION_OWNER = "SELECT user_id FROM collections WHERE collection_id = %s"

_INSERT_COLLECTION_PAPER = """
    INSERT INTO collection_papers (collection_id, work_id) VALUES (%s, %s)
    ON CONFLICT (collection_id, work_id) DO NOTHING
"""

_DELETE_COLLECTION_PAPER = "DELETE FROM collection_papers WHERE collection_id = %s AND work_id = %s"

_UPSERT_PROGRESS = """
    INSERT INTO reading_progress (user_id, work_id, status, started_at, completed_at)
    VALUES (%(user_id)s, %(work_id)s, %(status)s,
            CASE WHEN %(status)s = 'in_progress' THEN now() ELSE NULL END,
            CASE WHEN %(status)s = 'completed' THEN now() ELSE NULL END)
    ON CONFLICT (user_id, work_id) DO UPDATE SET
        status = EXCLUDED.status,
        started_at = COALESCE(reading_progress.started_at, EXCLUDED.started_at),
        completed_at = EXCLUDED.completed_at,
        updated_at = now()
"""

_INSERT_NOTE = """
    INSERT INTO notes (user_id, work_id, collection_id, note_text) VALUES (%s, %s, %s, %s)
    RETURNING note_id
"""

_UPDATE_SEQUENCE_RANK = """
    UPDATE collection_papers SET sequence_rank = %s WHERE collection_id = %s AND work_id = %s
"""


def _ensure_user(email: str) -> None:
    lakebase.run_write(_ENSURE_USER, (email,))


def _require_collection_owner(collection_id: int, email: str) -> dict | None:
    """Returns an error dict if the collection doesn't exist or belongs to
    someone else; None if the current user is clear to mutate it."""
    rows = lakebase.run_query(_COLLECTION_OWNER, (collection_id,))
    if not rows:
        return {"error": f"collection {collection_id} does not exist"}
    if rows[0]["user_id"] != email:
        return {"error": "not authorized: this collection belongs to a different user"}
    return None


def _reading_path_for_collection(collection_id: int) -> list[dict]:
    papers = lakebase.run_query(_COLLECTION_PAPERS, (collection_id,))
    if not papers:
        return []
    work_ids = [p["work_id"] for p in papers]
    edges = lakebase.run_query(_INDUCED_EDGES, {"ids": work_ids})
    paper_objs = [Paper(work_id=p["work_id"], foundational_score=p["foundational_score"] or 0.0, title=p["title"]) for p in papers]
    edge_pairs = [(e["citing_work_id"], e["cited_work_id"]) for e in edges]
    return build_reading_path(paper_objs, edge_pairs)


def load_tools(mcp_server) -> None:  # noqa: ANN001
    # ----------------------------------------------------------------- read --

    @mcp_server.tool
    def search_papers(
        query: str,
        top_k: int = 10,
        topic_field: str | None = None,
        year_from: int | None = None,
        open_access_only: bool = False,
    ) -> dict:
        """Semantic search over paper abstracts (pgvector cosine similarity).

        Args:
            query: a natural-language description of what to find.
            top_k: max results, default 10.
            topic_field: restrict to an OpenAlex field name (e.g. "Computer Science"), optional.
            year_from: only papers published in or after this year, optional.
            open_access_only: only return open-access papers.

        Returns: {"results": [{work_id, title, similarity, ...}, ...]}
        """
        try:
            vector = embedder.to_vector_literal(embedder.embed_query(query))
            rows = lakebase.run_query(
                _SEARCH_PAPERS,
                {
                    "vector": vector,
                    "topic_field": topic_field,
                    "year_from": year_from,
                    "open_access_only": open_access_only,
                    "top_k": top_k,
                },
            )
            return {"results": rows}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @mcp_server.tool
    def get_reading_path(collection_id: int) -> dict:
        """Compute a citation-ordered reading path for a collection's papers.

        Sequences papers so that anything cited *by another paper in this
        collection* comes first -- real citation edges, not a model's guess.
        Read-only: does not persist the order. Call save_reading_plan to persist it.

        Returns: {"path": [{work_id, title, prerequisites, unlocks, in_citation_cycle}, ...]}
        """
        try:
            return {"path": _reading_path_for_collection(collection_id)}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @mcp_server.tool
    def compare_papers(work_id_a: str, work_id_b: str) -> dict:
        """Fetch two papers and report whether either cites the other.

        Returns: {"paper_a": {...}, "paper_b": {...}, "a_cites_b": bool, "b_cites_a": bool}
        """
        try:
            a = lakebase.run_query(_PAPER_BY_ID, (work_id_a,))
            b = lakebase.run_query(_PAPER_BY_ID, (work_id_b,))
            if not a or not b:
                return {"error": "one or both work_ids were not found"}
            edges = lakebase.run_query(_EDGE_BETWEEN, (work_id_a, work_id_b, work_id_b, work_id_a))
            a_cites_b = any(e["citing_work_id"] == work_id_a for e in edges)
            b_cites_a = any(e["citing_work_id"] == work_id_b for e in edges)
            return {"paper_a": a[0], "paper_b": b[0], "a_cites_b": a_cites_b, "b_cites_a": b_cites_a}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @mcp_server.tool
    def get_collection(collection_id: int) -> dict:
        """Fetch a collection's metadata and its papers, ordered by reading
        sequence if save_reading_plan has been run, else by when they were added.

        Returns: {"collection": {...}, "papers": [...]}
        """
        try:
            collections = lakebase.run_query(_COLLECTION_BY_ID, (collection_id,))
            if not collections:
                return {"error": f"collection {collection_id} does not exist"}
            papers = lakebase.run_query(_COLLECTION_PAPERS_ORDERED, (collection_id,))
            return {"collection": collections[0], "papers": papers}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @mcp_server.tool
    def get_progress(collection_id: int | None = None) -> dict:
        """Reading progress for the current user, optionally restricted to one collection.

        Returns: {"progress": [{work_id, title, status, ...}, ...]}
        """
        try:
            from identity import current_user_email

            email = current_user_email()
            rows = lakebase.run_query(_PROGRESS_FOR_USER, {"user_id": email, "collection_id": collection_id})
            return {"progress": rows}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @mcp_server.tool
    def explain_citation_link(citing_work_id: str, cited_work_id: str) -> dict:
        """Explain whether/why one paper should be read before another.

        Returns: {"cites": bool, "citing": {...}, "cited": {...}, "explanation": str}
        """
        try:
            edges = lakebase.run_query(
                _EDGE_BETWEEN, (citing_work_id, cited_work_id, cited_work_id, citing_work_id)
            )
            direct = any(
                e["citing_work_id"] == citing_work_id and e["cited_work_id"] == cited_work_id for e in edges
            )
            citing = lakebase.run_query(_PAPER_BY_ID, (citing_work_id,))
            cited = lakebase.run_query(_PAPER_BY_ID, (cited_work_id,))
            if not citing or not cited:
                return {"error": "one or both work_ids were not found"}
            explanation = (
                f"{citing[0]['title']!r} cites {cited[0]['title']!r} -- read the latter first."
                if direct
                else f"No direct citation edge found between these two papers in this corpus."
            )
            return {"cites": direct, "citing": citing[0], "cited": cited[0], "explanation": explanation}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @mcp_server.tool
    def find_prerequisites(work_id: str) -> dict:
        """Papers that `work_id` cites -- read these first, ranked by foundational_score.

        Returns: {"prerequisites": [{work_id, title, foundational_score}, ...]}
        """
        try:
            rows = lakebase.run_query(_PREREQUISITES, (work_id,))
            return {"prerequisites": rows}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    # ---------------------------------------------------------------- write --

    @mcp_server.tool
    def create_learning_goal(title: str, description: str | None = None) -> dict:
        """Create a new learning goal for the current user.

        Returns: {"goal_id": int}
        """
        try:
            from identity import current_user_email

            email = current_user_email()
            _ensure_user(email)
            rows = lakebase.run_write_returning(_INSERT_LEARNING_GOAL, (email, title, description))
            return {"goal_id": rows[0]["goal_id"]}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @mcp_server.tool
    def create_collection(name: str, goal_id: int | None = None, description: str | None = None) -> dict:
        """Create a new paper collection for the current user, optionally linked to a learning goal.

        Returns: {"collection_id": int}
        """
        try:
            from identity import current_user_email

            email = current_user_email()
            _ensure_user(email)
            rows = lakebase.run_write_returning(_INSERT_COLLECTION, (email, goal_id, name, description))
            return {"collection_id": rows[0]["collection_id"]}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @mcp_server.tool
    def add_to_collection(collection_id: int, work_id: str) -> dict:
        """Add a paper to one of the current user's collections.

        Returns: {"ok": true} or {"error": str}
        """
        try:
            from identity import current_user_email

            email = current_user_email()
            denied = _require_collection_owner(collection_id, email)
            if denied:
                return denied
            lakebase.run_write(_INSERT_COLLECTION_PAPER, (collection_id, work_id))
            return {"ok": True}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @mcp_server.tool
    def remove_from_collection(collection_id: int, work_id: str) -> dict:
        """Remove a paper from one of the current user's collections.

        Returns: {"ok": true} or {"error": str}
        """
        try:
            from identity import current_user_email

            email = current_user_email()
            denied = _require_collection_owner(collection_id, email)
            if denied:
                return denied
            lakebase.run_write(_DELETE_COLLECTION_PAPER, (collection_id, work_id))
            return {"ok": True}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @mcp_server.tool
    def record_reading_progress(work_id: str, status: str) -> dict:
        """Record the current user's reading progress on a paper.

        Args:
            status: one of "not_started", "in_progress", "completed", "abandoned".

        Returns: {"ok": true} or {"error": str}
        """
        try:
            from identity import current_user_email

            email = current_user_email()
            _ensure_user(email)
            lakebase.run_write(_UPSERT_PROGRESS, {"user_id": email, "work_id": work_id, "status": status})
            return {"ok": True}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @mcp_server.tool
    def save_note(work_id: str, note_text: str, collection_id: int | None = None) -> dict:
        """Save a note on a paper for the current user, optionally attached to a collection.

        Returns: {"note_id": int}
        """
        try:
            from identity import current_user_email

            email = current_user_email()
            _ensure_user(email)
            rows = lakebase.run_write_returning(_INSERT_NOTE, (email, work_id, collection_id, note_text))
            return {"note_id": rows[0]["note_id"]}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @mcp_server.tool
    def save_reading_plan(collection_id: int) -> dict:
        """Compute the citation-ordered reading path for a collection and persist
        it as sequence_rank on each paper -- unlike get_reading_path, this writes.

        Returns: {"path": [...]} with the same shape as get_reading_path.
        """
        try:
            from identity import current_user_email

            email = current_user_email()
            denied = _require_collection_owner(collection_id, email)
            if denied:
                return denied
            path = _reading_path_for_collection(collection_id)
            with lakebase.get_connection() as conn:
                with conn.cursor() as cur:
                    for rank, entry in enumerate(path):
                        cur.execute(_UPDATE_SEQUENCE_RANK, (rank, collection_id, entry["work_id"]))
                conn.commit()
            return {"path": path}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}
