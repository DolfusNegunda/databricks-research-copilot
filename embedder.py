"""
Chunking and embedding for paper abstracts.

The only module that imports sentence_transformers, and only lazily, inside
get_model(). Everything else -- the embedding sync job, the app's search
route, the MCP server -- calls chunk_text() / embed_texts() / embed_query() /
to_vector_literal() and never touches the model library directly.

That indirection matters for deployment size and startup time: torch's
CPU-only wheel is ~200MB and the all-MiniLM-L6-v2 weights are another ~90MB,
real weight to carry in a Databricks App container. The model loads lazily on
first use rather than at import time, so importing this module during app
startup never risks a health check timing out on a model load.
"""

from __future__ import annotations

import os
import threading

MODEL_NAME = os.environ.get("EMBEDDING_MODEL_NAME", "sentence-transformers/all-MiniLM-L6-v2")
EMBEDDING_DIM = int(os.environ.get("EMBEDDING_DIM", "384"))
CHUNK_SIZE = int(os.environ.get("EMBEDDING_CHUNK_SIZE", "800"))
CHUNK_OVERLAP = int(os.environ.get("EMBEDDING_CHUNK_OVERLAP", "100"))

_BATCH_SIZE = 32

_model = None
_model_lock = threading.Lock()


def chunk_text(text: str, chunk_size: int = CHUNK_SIZE, chunk_overlap: int = CHUNK_OVERLAP) -> list[str]:
    """Slide a character window over text, stripping and dropping empties.

    Chunk i covers [i * (chunk_size - chunk_overlap), that + chunk_size).
    Pure and model-free so it works without sentence-transformers installed.
    Most abstracts are well under chunk_size and come back as a single chunk;
    the sliding window exists for the minority that don't.
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    if chunk_overlap >= chunk_size:
        raise ValueError("chunk_overlap must be smaller than chunk_size")

    if not text or not text.strip():
        return []

    stride = chunk_size - chunk_overlap
    chunks = []
    start = 0
    text_len = len(text)
    while start < text_len:
        piece = text[start : start + chunk_size].strip()
        if piece:
            chunks.append(piece)
        if start + chunk_size >= text_len:
            break
        start += stride
    return chunks


def get_model():
    """Lazily construct and cache the SentenceTransformer singleton.

    Imports sentence_transformers here, not at module top level, so the rest
    of the app stays importable when torch/sentence-transformers are absent
    or fail to load.
    """
    global _model
    if _model is not None:
        return _model

    with _model_lock:
        if _model is not None:
            return _model
        try:
            from sentence_transformers import SentenceTransformer

            cache_folder = os.environ.get("SENTENCE_TRANSFORMERS_HOME")
            kwargs = {"cache_folder": cache_folder} if cache_folder else {}
            _model = SentenceTransformer(MODEL_NAME, **kwargs)
        except Exception as exc:  # noqa: BLE001 -- rewrapped for the caller
            raise RuntimeError(
                f"Embeddings are unavailable: could not load model '{MODEL_NAME}' ({type(exc).__name__}: {exc})"
            ) from exc

    return _model


def embed_texts(texts: list[str]) -> list[list[float]]:
    """Embed a batch of texts, returning one list[float] per input, in order."""
    if not texts:
        return []
    model = get_model()
    vectors = model.encode(texts, batch_size=_BATCH_SIZE, show_progress_bar=False)
    return [vector.tolist() for vector in vectors]


def embed_query(text: str) -> list[float]:
    """Embed a single string, returning a flat list[float]."""
    return embed_texts([text])[0]


def to_vector_literal(vector: list[float]) -> str:
    """Format a vector as a pgvector literal, for binding alongside %s::vector."""
    return "[" + ",".join(f"{v:.8f}" for v in vector) + "]"
