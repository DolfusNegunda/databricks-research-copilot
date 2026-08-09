"""Embed papers synced by sync_to_lakebase.py -- a deliberately separate
stage, same as the weather-rag sibling: sync is cheap (a batch SQL write),
embedding loads a ~290MB model and runs inference. Nothing embeds
automatically on sync; a paper sits with embedded_at IS NULL until this runs.

Pure Python + psycopg2 + sentence-transformers -- no Spark, unlike
sync_to_lakebase.py. Safe to run as a plain Databricks Job task, a notebook,
or by hand: `python pipelines/embed_papers.py`.

Chunks are deleted and reinserted per paper rather than upserted by
chunk_index alone, so a paper whose new chunk count is *smaller* than its
last embedding (only possible if EMBEDDING_CHUNK_SIZE/OVERLAP changed between
runs) can't leave stale higher-numbered chunks behind.
"""

from __future__ import annotations

import sys
from pathlib import Path

from psycopg2.extras import execute_values

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import embedder  # noqa: E402
import lakebase  # noqa: E402

_FETCH_BATCH = 200

_SELECT_UNEMBEDDED = """
    SELECT work_id, narrative_abstract FROM papers
    WHERE embedded_at IS NULL AND narrative_abstract IS NOT NULL
    ORDER BY synced_at
    LIMIT %s
"""

_DELETE_STALE_CHUNKS = "DELETE FROM paper_embeddings WHERE work_id = ANY(%s)"

_INSERT_EMBEDDINGS = """
    INSERT INTO paper_embeddings (embedding_id, work_id, chunk_index, chunk_text, embedding, model_name)
    VALUES %s
"""
_INSERT_EMBEDDINGS_TEMPLATE = "(%s, %s, %s, %s, %s::vector, %s)"

_MARK_EMBEDDED = "UPDATE papers SET embedded_at = now() WHERE work_id = ANY(%s)"


def embed_batch(batch_size: int = _FETCH_BATCH) -> dict:
    with lakebase.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(_SELECT_UNEMBEDDED, (batch_size,))
            papers = cur.fetchall()

        if not papers:
            return {"papers_seen": 0, "chunks_embedded": 0}

        # Flatten to (work_id, chunk_index, chunk_text) across every paper in
        # this batch, so the model runs one batched encode() call rather than
        # one tiny call per paper.
        flat: list[tuple[str, int, str]] = []
        for paper in papers:
            chunks = embedder.chunk_text(paper["narrative_abstract"])
            for idx, chunk in enumerate(chunks):
                flat.append((paper["work_id"], idx, chunk))

        work_ids = [p["work_id"] for p in papers]
        with conn.cursor() as cur:
            cur.execute(_DELETE_STALE_CHUNKS, (work_ids,))

        if flat:
            vectors = embedder.embed_texts([chunk for _, _, chunk in flat])
            rows = [
                (
                    f"{work_id}:{idx}",
                    work_id,
                    idx,
                    chunk,
                    embedder.to_vector_literal(vector),
                    embedder.MODEL_NAME,
                )
                for (work_id, idx, chunk), vector in zip(flat, vectors)
            ]
            with conn.cursor() as cur:
                execute_values(cur, _INSERT_EMBEDDINGS, rows, template=_INSERT_EMBEDDINGS_TEMPLATE)

        # Mark every paper in this batch embedded, including any whose
        # abstract chunked to nothing (rare -- a narrative_abstract that
        # passed silver's has_abstract gate is at least 100 chars) -- leaving
        # it NULL would make this job reprocess the same paper forever.
        with conn.cursor() as cur:
            cur.execute(_MARK_EMBEDDED, (work_ids,))
        conn.commit()

    return {"papers_seen": len(papers), "chunks_embedded": len(flat)}


def main() -> None:
    total_papers = 0
    total_chunks = 0
    while True:
        result = embed_batch()
        if result["papers_seen"] == 0:
            break
        total_papers += result["papers_seen"]
        total_chunks += result["chunks_embedded"]
        print(f"  embedded {result['papers_seen']} papers ({result['chunks_embedded']} chunks)")
    print(f"done: {total_papers} papers, {total_chunks} chunks")


if __name__ == "__main__":
    main()
