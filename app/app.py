"""Flask UI for the AI Research & Learning Copilot.

Routes, hand-written SQL, and the Agent Bricks chat proxy -- same role as the
weather-rag sibling's app.py. Identity comes from the x-forwarded-email
header Databricks Apps injects (same mechanism mcp_server/identity.py uses,
just read directly per-request here since Flask's request context is already
request-scoped -- no ContextVar needed for a synchronous WSGI app).

Write routes never accept a user id from the client; see _current_user_email.
"""

from __future__ import annotations

import os

from flask import Flask, jsonify, render_template, request

import embedder
import lakebase
from reading_path import Paper, build_reading_path

app = Flask(__name__)

_DEV_FALLBACK_EMAIL = os.environ.get("FLASK_DEV_USER_EMAIL")
AGENT_SERVING_ENDPOINT = os.environ.get("AGENT_SERVING_ENDPOINT")

_bootstrap = lakebase.ensure_research_schema()


def _current_user_email() -> str | None:
    email = request.headers.get("x-forwarded-email") or request.headers.get("x-forwarded-user")
    return email or _DEV_FALLBACK_EMAIL


def _require_user():
    email = _current_user_email()
    if not email:
        return None, (jsonify(error="no end-user identity on this request"), 401)
    lakebase.run_write("INSERT INTO users (user_id) VALUES (%s) ON CONFLICT (user_id) DO NOTHING", (email,))
    return email, None


def _reading_path_for_collection(collection_id: int) -> list[dict]:
    papers = lakebase.run_query(
        "SELECT p.work_id, p.title, p.foundational_score FROM collection_papers cp "
        "JOIN papers p ON p.work_id = cp.work_id WHERE cp.collection_id = %s",
        (collection_id,),
    )
    if not papers:
        return []
    work_ids = [p["work_id"] for p in papers]
    edges = lakebase.run_query(
        "SELECT citing_work_id, cited_work_id FROM citation_edges "
        "WHERE citing_work_id = ANY(%(ids)s) AND cited_work_id = ANY(%(ids)s)",
        {"ids": work_ids},
    )
    paper_objs = [
        Paper(work_id=p["work_id"], foundational_score=p["foundational_score"] or 0.0, title=p["title"])
        for p in papers
    ]
    edge_pairs = [(e["citing_work_id"], e["cited_work_id"]) for e in edges]
    return build_reading_path(paper_objs, edge_pairs)


# ------------------------------------------------------------------- pages --


@app.get("/")
def index():
    return render_template("index.html")


@app.get("/healthz")
def healthz():
    return jsonify(status="healthy" if _bootstrap["ok"] else "degraded", lakebase=_bootstrap)


@app.get("/api/me")
def get_me():
    email = _current_user_email()
    return jsonify(email=email, authenticated=email is not None)


# ----------------------------------------------------------------- search --


@app.get("/api/search")
def search_papers():
    query = (request.args.get("q") or "").strip()
    if not query:
        return jsonify(error="q is required"), 400
    top_k = min(max(int(request.args.get("top_k", 10)), 1), 50)
    topic_field = request.args.get("topic_field") or None
    year_from = request.args.get("year_from", type=int)
    open_access_only = request.args.get("open_access_only", "false").lower() == "true"

    vector = embedder.to_vector_literal(embedder.embed_query(query))
    rows = lakebase.run_query(
        """
        SELECT * FROM (
            SELECT DISTINCT ON (p.work_id)
                p.work_id, p.title, p.publication_year, p.primary_topic_field, p.primary_topic_domain,
                p.cited_by_count, p.foundational_score, p.is_oa, p.oa_url, p.seed_topic,
                1 - (e.embedding <=> %(vector)s::vector) AS similarity
            FROM paper_embeddings e JOIN papers p ON p.work_id = e.work_id
            WHERE (%(topic_field)s::text IS NULL OR p.primary_topic_field = %(topic_field)s)
              AND (%(year_from)s::int IS NULL OR p.publication_year >= %(year_from)s)
              AND (%(open_access_only)s IS FALSE OR p.is_oa)
            ORDER BY p.work_id, similarity DESC
        ) deduped
        ORDER BY similarity DESC
        LIMIT %(top_k)s
        """,
        {
            "vector": vector,
            "topic_field": topic_field,
            "year_from": year_from,
            "open_access_only": open_access_only,
            "top_k": top_k,
        },
    )
    return jsonify(results=rows)


@app.get("/api/topics")
def list_topics():
    rows = lakebase.run_query(
        "SELECT seed_topic, count(*) AS paper_count, sum(cited_by_count) AS total_citations "
        "FROM papers GROUP BY seed_topic ORDER BY paper_count DESC"
    )
    return jsonify(topics=rows)


@app.get("/api/papers/<work_id>")
def get_paper(work_id: str):
    rows = lakebase.run_query("SELECT * FROM papers WHERE work_id = %s", (work_id,))
    if not rows:
        return jsonify(error="not found"), 404
    prerequisites = lakebase.run_query(
        "SELECT p.work_id, p.title, p.foundational_score FROM citation_edges ce "
        "JOIN papers p ON p.work_id = ce.cited_work_id WHERE ce.citing_work_id = %s "
        "ORDER BY p.foundational_score DESC",
        (work_id,),
    )
    unlocks = lakebase.run_query(
        "SELECT p.work_id, p.title, p.foundational_score FROM citation_edges ce "
        "JOIN papers p ON p.work_id = ce.citing_work_id WHERE ce.cited_work_id = %s "
        "ORDER BY p.foundational_score DESC",
        (work_id,),
    )
    return jsonify(paper=rows[0], prerequisites=prerequisites, unlocks=unlocks)


# -------------------------------------------------------------------- goals --


@app.get("/api/goals")
def list_goals():
    email, err = _require_user()
    if err:
        return err
    rows = lakebase.run_query(
        "SELECT * FROM learning_goals WHERE user_id = %s ORDER BY created_at DESC", (email,)
    )
    return jsonify(goals=rows)


@app.post("/api/goals")
def create_goal():
    email, err = _require_user()
    if err:
        return err
    body = request.get_json(force=True, silent=True) or {}
    title = (body.get("title") or "").strip()
    if not title:
        return jsonify(error="title is required"), 400
    rows = lakebase.run_write_returning(
        "INSERT INTO learning_goals (user_id, title, description) VALUES (%s, %s, %s) RETURNING goal_id",
        (email, title, body.get("description")),
    )
    return jsonify(goal_id=rows[0]["goal_id"]), 201


# ------------------------------------------------------------- collections --


@app.get("/api/collections")
def list_collections():
    email, err = _require_user()
    if err:
        return err
    rows = lakebase.run_query(
        "SELECT c.*, count(cp.work_id) AS paper_count FROM collections c "
        "LEFT JOIN collection_papers cp ON cp.collection_id = c.collection_id "
        "WHERE c.user_id = %s GROUP BY c.collection_id ORDER BY c.created_at DESC",
        (email,),
    )
    return jsonify(collections=rows)


@app.post("/api/collections")
def create_collection():
    email, err = _require_user()
    if err:
        return err
    body = request.get_json(force=True, silent=True) or {}
    name = (body.get("name") or "").strip()
    if not name:
        return jsonify(error="name is required"), 400
    rows = lakebase.run_write_returning(
        "INSERT INTO collections (user_id, goal_id, name, description) VALUES (%s, %s, %s, %s) "
        "RETURNING collection_id",
        (email, body.get("goal_id"), name, body.get("description")),
    )
    return jsonify(collection_id=rows[0]["collection_id"]), 201


def _owned_collection_or_404(collection_id: int, email: str):
    rows = lakebase.run_query("SELECT * FROM collections WHERE collection_id = %s", (collection_id,))
    if not rows:
        return None, (jsonify(error="collection not found"), 404)
    if rows[0]["user_id"] != email:
        return None, (jsonify(error="not authorized"), 403)
    return rows[0], None


@app.get("/api/collections/<int:collection_id>")
def get_collection(collection_id: int):
    email, err = _require_user()
    if err:
        return err
    collection, err = _owned_collection_or_404(collection_id, email)
    if err:
        return err
    papers = lakebase.run_query(
        "SELECT p.*, cp.sequence_rank, cp.added_at FROM collection_papers cp "
        "JOIN papers p ON p.work_id = cp.work_id WHERE cp.collection_id = %s "
        "ORDER BY cp.sequence_rank NULLS LAST, cp.added_at",
        (collection_id,),
    )
    return jsonify(collection=collection, papers=papers)


@app.post("/api/collections/<int:collection_id>/papers")
def add_paper_to_collection(collection_id: int):
    email, err = _require_user()
    if err:
        return err
    _, err = _owned_collection_or_404(collection_id, email)
    if err:
        return err
    body = request.get_json(force=True, silent=True) or {}
    work_id = body.get("work_id")
    if not work_id:
        return jsonify(error="work_id is required"), 400
    lakebase.run_write(
        "INSERT INTO collection_papers (collection_id, work_id) VALUES (%s, %s) "
        "ON CONFLICT (collection_id, work_id) DO NOTHING",
        (collection_id, work_id),
    )
    return jsonify(ok=True), 201


@app.delete("/api/collections/<int:collection_id>/papers/<work_id>")
def remove_paper_from_collection(collection_id: int, work_id: str):
    email, err = _require_user()
    if err:
        return err
    _, err = _owned_collection_or_404(collection_id, email)
    if err:
        return err
    lakebase.run_write(
        "DELETE FROM collection_papers WHERE collection_id = %s AND work_id = %s", (collection_id, work_id)
    )
    return jsonify(ok=True)


@app.get("/api/collections/<int:collection_id>/reading-path")
def get_reading_path(collection_id: int):
    email, err = _require_user()
    if err:
        return err
    _, err = _owned_collection_or_404(collection_id, email)
    if err:
        return err
    return jsonify(path=_reading_path_for_collection(collection_id))


@app.post("/api/collections/<int:collection_id>/reading-path")
def save_reading_path(collection_id: int):
    """Same computation as GET, but persists sequence_rank -- the write
    counterpart, mirroring mcp_server's get_reading_path/save_reading_plan split.
    """
    email, err = _require_user()
    if err:
        return err
    _, err = _owned_collection_or_404(collection_id, email)
    if err:
        return err
    path = _reading_path_for_collection(collection_id)
    with lakebase.get_connection() as conn:
        with conn.cursor() as cur:
            for rank, entry in enumerate(path):
                cur.execute(
                    "UPDATE collection_papers SET sequence_rank = %s WHERE collection_id = %s AND work_id = %s",
                    (rank, collection_id, entry["work_id"]),
                )
        conn.commit()
    return jsonify(path=path)


# ------------------------------------------------------------------ progress --


@app.get("/api/progress")
def list_progress():
    email, err = _require_user()
    if err:
        return err
    collection_id = request.args.get("collection_id", type=int)
    rows = lakebase.run_query(
        "SELECT rp.work_id, p.title, rp.status, rp.started_at, rp.completed_at "
        "FROM reading_progress rp JOIN papers p ON p.work_id = rp.work_id "
        "WHERE rp.user_id = %(user_id)s AND (%(collection_id)s::bigint IS NULL OR rp.work_id IN ("
        "  SELECT work_id FROM collection_papers WHERE collection_id = %(collection_id)s"
        "))",
        {"user_id": email, "collection_id": collection_id},
    )
    return jsonify(progress=rows)


@app.post("/api/progress")
def record_progress():
    email, err = _require_user()
    if err:
        return err
    body = request.get_json(force=True, silent=True) or {}
    work_id, status = body.get("work_id"), body.get("status")
    if not work_id or status not in ("not_started", "in_progress", "completed", "abandoned"):
        return jsonify(error="work_id and a valid status are required"), 400
    lakebase.run_write(
        """
        INSERT INTO reading_progress (user_id, work_id, status, started_at, completed_at)
        VALUES (%(user_id)s, %(work_id)s, %(status)s,
                CASE WHEN %(status)s = 'in_progress' THEN now() ELSE NULL END,
                CASE WHEN %(status)s = 'completed' THEN now() ELSE NULL END)
        ON CONFLICT (user_id, work_id) DO UPDATE SET
            status = EXCLUDED.status,
            started_at = COALESCE(reading_progress.started_at, EXCLUDED.started_at),
            completed_at = EXCLUDED.completed_at,
            updated_at = now()
        """,
        {"user_id": email, "work_id": work_id, "status": status},
    )
    return jsonify(ok=True)


# --------------------------------------------------------------------- notes --


@app.get("/api/notes")
def list_notes():
    email, err = _require_user()
    if err:
        return err
    work_id = request.args.get("work_id")
    if not work_id:
        return jsonify(error="work_id is required"), 400
    rows = lakebase.run_query(
        "SELECT * FROM notes WHERE user_id = %s AND work_id = %s ORDER BY created_at DESC", (email, work_id)
    )
    return jsonify(notes=rows)


@app.post("/api/notes")
def save_note():
    email, err = _require_user()
    if err:
        return err
    body = request.get_json(force=True, silent=True) or {}
    work_id, note_text = body.get("work_id"), (body.get("note_text") or "").strip()
    if not work_id or not note_text:
        return jsonify(error="work_id and note_text are required"), 400
    rows = lakebase.run_write_returning(
        "INSERT INTO notes (user_id, work_id, collection_id, note_text) VALUES (%s, %s, %s, %s) RETURNING note_id",
        (email, work_id, body.get("collection_id"), note_text),
    )
    return jsonify(note_id=rows[0]["note_id"]), 201


# --------------------------------------------------------------- agent chat --


@app.post("/api/agent/chat")
def agent_chat():
    """Proxies to the Agent Bricks serving endpoint once configured
    (AGENT_SERVING_ENDPOINT). Degrades clearly rather than 500ing when it
    isn't -- registering the agent is a manual last step in the Databricks
    workspace UI, done after this app and the MCP server are both deployed.
    """
    if not AGENT_SERVING_ENDPOINT:
        return jsonify(
            error="agent_not_configured",
            message="Set AGENT_SERVING_ENDPOINT once the Agent Bricks agent is created and the MCP server is registered.",
        ), 503

    body = request.get_json(force=True, silent=True) or {}
    message = (body.get("message") or "").strip()
    if not message:
        return jsonify(error="message is required"), 400

    try:
        import requests
        from databricks.sdk import WorkspaceClient

        # This agent's endpoint is Responses-API-shaped ({"input": [...]}),
        # which serving_endpoints.query()'s typed parameters (inputs=,
        # messages=, instances=, ...) don't map onto -- confirmed live, it
        # 400s asking for "input" specifically. Reuse the SDK purely for its
        # already-authenticated config and call the endpoint directly with
        # the exact shape it asked for.
        cfg = WorkspaceClient().config
        resp = requests.post(
            f"{cfg.host}/serving-endpoints/{AGENT_SERVING_ENDPOINT}/invocations",
            headers=cfg.authenticate(),
            json={"input": [{"role": "user", "content": message}], "stream": False},
            timeout=60,
        )
        resp.raise_for_status()
        try:
            return jsonify(response=resp.json())
        except ValueError:
            # Surface the raw body on the next failure instead of a bare
            # JSONDecodeError -- this endpoint's exact response shape hasn't
            # been confirmed live yet, so a diagnostic here beats a second
            # blind guess if stream: False isn't the whole story.
            raise RuntimeError(f"non-JSON response (status {resp.status_code}): {resp.text[:500]!r}") from None
    except Exception as exc:  # noqa: BLE001
        return jsonify(error="agent_call_failed", message=str(exc)), 502


if __name__ == "__main__":
    app.run(host=os.environ.get("FLASK_RUN_HOST", "0.0.0.0"), port=int(os.environ.get("FLASK_RUN_PORT", "8000")))
