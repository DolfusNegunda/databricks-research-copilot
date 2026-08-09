"""Offline checks: OpenAlex data-shape handling and reading-path correctness.

No network access, no Databricks, no Lakebase -- pure logic, run anywhere:
    python scripts/check_api.py

Mirrors the check_api.py convention from the sibling Day 1-3 projects: one
PASS/FAIL line per check, exit non-zero on any failure. This is the only
place these two modules' correctness is asserted, so it must stay in sync
with abstract_reconstruction.py and reading_path.py -- if you change either
module's behaviour, update the expected values here, don't just delete the
check that caught it.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from abstract_reconstruction import reconstruct_abstract  # noqa: E402
from embedder import chunk_text, embed_texts, to_vector_literal  # noqa: E402
from harvester.snowball import batched, build_or_filter, short_id  # noqa: E402
from reading_path import Paper, build_reading_path  # noqa: E402

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


# ---------------------------------------------------------------------------
# reconstruct_abstract
# ---------------------------------------------------------------------------

known = {
    "Despite": [0],
    "growing": [1],
    "interest": [2],
    "in": [3, 7],
    "RAG": [4],
    "systems,": [5],
    "gaps": [6],
    "evaluation": [8],
    "remain.": [9],
}
result = reconstruct_abstract(known)
check(
    "abstract: hand-built dict reconstructs exactly",
    result == "Despite growing interest in RAG systems, gaps in evaluation remain.",
    repr(result),
)

result_from_json = reconstruct_abstract(json.dumps(known))
check(
    "abstract: JSON-string form (Parquet shape) matches the dict form",
    result_from_json == result,
    f"dict={result!r} json_str={result_from_json!r}",
)

check("abstract: empty dict -> None", reconstruct_abstract({}) is None)
check("abstract: None -> None", reconstruct_abstract(None) is None)
check("abstract: empty JSON object string -> None", reconstruct_abstract("{}") is None)
check("abstract: blank string -> None", reconstruct_abstract("   ") is None)
check("abstract: malformed JSON string -> None, not raise", reconstruct_abstract("{not json") is None)
check("abstract: JSON array (wrong shape) -> None", reconstruct_abstract("[1,2,3]") is None)
check("abstract: non-dict, non-string input -> None", reconstruct_abstract(12345) is None)

repeated = {"the": [0, 3], "cat": [1], "chased": [2], "dog": [4]}
check(
    "abstract: repeated word appears at every listed position",
    reconstruct_abstract(repeated) == "the cat chased the dog",
    repr(reconstruct_abstract(repeated)),
)

shuffled = {"world": [1], "Hello": [0]}
check(
    "abstract: output order follows positions, not input key order",
    reconstruct_abstract(shuffled) == "Hello world",
    repr(reconstruct_abstract(shuffled)),
)

gapped = {"first": [0], "third": [5]}
check(
    "abstract: gapped positions still reconstruct in order, no crash",
    reconstruct_abstract(gapped) == "first third",
    repr(reconstruct_abstract(gapped)),
)

# ---------------------------------------------------------------------------
# build_reading_path
# ---------------------------------------------------------------------------

papers = [
    Paper("A", foundational_score=10.0, title="Foundational work"),
    Paper("B", foundational_score=5.0, title="Builds on A, higher score"),
    Paper("C", foundational_score=1.0, title="Builds on A, lower score"),
    Paper("D", foundational_score=0.5, title="Builds on B and C"),
]
edges = [("B", "A"), ("C", "A"), ("D", "B"), ("D", "C")]  # (citing, cited)
path = build_reading_path(papers, edges)
order = [p["work_id"] for p in path]
check(
    "reading_path: clean DAG, exact expected order (A, B, C, D)",
    order == ["A", "B", "C", "D"],
    f"got {order}",
)
a_entry = next(p for p in path if p["work_id"] == "A")
check("reading_path: A has no prerequisites", a_entry["prerequisites"] == [])
check("reading_path: A unlocks B and C", a_entry["unlocks"] == ["B", "C"])
d_entry = next(p for p in path if p["work_id"] == "D")
check("reading_path: D's prerequisites are exactly B and C", d_entry["prerequisites"] == ["B", "C"])
check("reading_path: nothing marked in_citation_cycle in a clean DAG", not any(p["in_citation_cycle"] for p in path))

cyclic_papers = [Paper("X", foundational_score=2.0), Paper("Y", foundational_score=9.0)]
cyclic_edges = [("X", "Y"), ("Y", "X")]
cyclic_path = build_reading_path(cyclic_papers, cyclic_edges)
check("reading_path: 2-cycle returns both papers (does not hang or drop one)", len(cyclic_path) == 2)
check(
    "reading_path: 2-cycle both flagged in_citation_cycle",
    all(p["in_citation_cycle"] for p in cyclic_path),
    str(cyclic_path),
)
check(
    "reading_path: 2-cycle tie-break by foundational_score (Y=9.0 before X=2.0)",
    [p["work_id"] for p in cyclic_path] == ["Y", "X"],
    str([p["work_id"] for p in cyclic_path]),
)

mixed_papers = [
    Paper("P", foundational_score=10.0),  # foundational, outside the cycle
    Paper("Q", foundational_score=5.0),  # cites P, outside the cycle
    Paper("X", foundational_score=2.0),  # in a cycle with Y
    Paper("Y", foundational_score=1.0),  # in a cycle with X
]
mixed_edges = [("Q", "P"), ("X", "Y"), ("Y", "X")]
mixed_path = build_reading_path(mixed_papers, mixed_edges)
mixed_order = [p["work_id"] for p in mixed_path]
check(
    "reading_path: mixed graph, P and Q ordered correctly despite an unrelated cycle",
    mixed_order.index("P") < mixed_order.index("Q"),
    str(mixed_order),
)
flagged = {p["work_id"] for p in mixed_path if p["in_citation_cycle"]}
check("reading_path: mixed graph, only X and Y flagged as cyclic", flagged == {"X", "Y"}, str(flagged))
check(
    "reading_path: mixed graph, real prereqs resolve before the cycle remainder",
    mixed_order.index("Q") < mixed_order.index("X") and mixed_order.index("Q") < mixed_order.index("Y"),
    str(mixed_order),
)

lone_papers = [Paper("M", foundational_score=3.0), Paper("N", foundational_score=7.0), Paper("O", foundational_score=1.0)]
lone_path = build_reading_path(lone_papers, [])
check(
    "reading_path: disconnected set, pure score ordering, all three present",
    [p["work_id"] for p in lone_path] == ["N", "M", "O"],
    str([p["work_id"] for p in lone_path]),
)
check(
    "reading_path: disconnected set, every entry has empty prerequisites and unlocks",
    all(p["prerequisites"] == [] and p["unlocks"] == [] for p in lone_path),
)

tied_papers_a = [Paper("Z", foundational_score=1.0), Paper("A", foundational_score=1.0), Paper("M", foundational_score=1.0)]
tied_papers_b = [Paper("M", foundational_score=1.0), Paper("Z", foundational_score=1.0), Paper("A", foundational_score=1.0)]
order_a = [p["work_id"] for p in build_reading_path(tied_papers_a, [])]
order_b = [p["work_id"] for p in build_reading_path(tied_papers_b, [])]
check(
    "reading_path: exact score ties break by work_id, independent of input order",
    order_a == order_b == ["A", "M", "Z"],
    f"a={order_a} b={order_b}",
)

try:
    build_reading_path([Paper("P1")], [("P1", "OUTSIDE_THE_SET")])
    check("reading_path: edge outside the set raises KeyError", False, "no exception was raised")
except KeyError:
    check("reading_path: edge outside the set raises KeyError", True)

chain_papers = [Paper(str(i), foundational_score=float(-i)) for i in range(5)]
chain_edges = [(str(i), str(i - 1)) for i in range(1, 5)]  # i cites i-1
chain_order = [p["work_id"] for p in build_reading_path(chain_papers, chain_edges)]
check(
    "reading_path: 5-node chain resolves in exact dependency order",
    chain_order == ["0", "1", "2", "3", "4"],
    str(chain_order),
)

# ---------------------------------------------------------------------------
# harvester.snowball pure helpers (no network)
# ---------------------------------------------------------------------------

check("short_id: strips the full URL form", short_id("https://openalex.org/W123") == "W123")
check("short_id: idempotent on an already-short id", short_id("W123") == "W123")

check(
    "batched: splits evenly with no remainder",
    list(batched([1, 2, 3, 4], 2)) == [[1, 2], [3, 4]],
)
check(
    "batched: last chunk holds the remainder, not dropped or padded",
    list(batched([1, 2, 3, 4, 5], 2)) == [[1, 2], [3, 4], [5]],
)
check("batched: empty input yields no chunks", list(batched([], 100)) == [])
check(
    "batched: chunk size >= input length yields exactly one chunk",
    list(batched([1, 2], 100)) == [[1, 2]],
)

check(
    "build_or_filter: joins normalized ids with |",
    build_or_filter("openalex_id", ["https://openalex.org/W1", "W2"]) == "openalex_id:W1|W2",
)
try:
    build_or_filter("openalex_id", [])
    check("build_or_filter: empty id list raises ValueError", False, "no exception was raised")
except ValueError:
    check("build_or_filter: empty id list raises ValueError", True)

# ---------------------------------------------------------------------------
# embedder pure helpers (no model load, no network)
# ---------------------------------------------------------------------------

check("chunk_text: empty string yields no chunks", chunk_text("") == [])
check("chunk_text: whitespace-only yields no chunks", chunk_text("   ") == [])
check(
    "chunk_text: text shorter than chunk_size yields exactly one chunk",
    chunk_text("a short abstract", chunk_size=800, chunk_overlap=100) == ["a short abstract"],
)
long_text = "word " * 500  # 2500 chars, well over an 800-char chunk_size
long_chunks = chunk_text(long_text, chunk_size=800, chunk_overlap=100)
check("chunk_text: long text splits into multiple chunks", len(long_chunks) > 1, str(len(long_chunks)))
check(
    "chunk_text: every chunk is non-empty after stripping",
    all(c == c.strip() and c for c in long_chunks),
)
try:
    chunk_text("x", chunk_size=0)
    check("chunk_text: chunk_size<=0 raises ValueError", False, "no exception was raised")
except ValueError:
    check("chunk_text: chunk_size<=0 raises ValueError", True)
try:
    chunk_text("x", chunk_size=100, chunk_overlap=100)
    check("chunk_text: overlap>=chunk_size raises ValueError", False, "no exception was raised")
except ValueError:
    check("chunk_text: overlap>=chunk_size raises ValueError", True)

check("embed_texts: empty list returns empty list without loading a model", embed_texts([]) == [])

check(
    "to_vector_literal: formats as a bracketed, comma-separated pgvector literal",
    to_vector_literal([1.0, -0.5, 0.0]) == "[1.00000000,-0.50000000,0.00000000]",
)
check("to_vector_literal: empty vector formats as an empty literal", to_vector_literal([]) == "[]")

# ---------------------------------------------------------------------------
# frontend conventions (source text inspection, no JS execution)
# ---------------------------------------------------------------------------

_APP_DIR = Path(__file__).resolve().parent.parent / "app"
_APP_JS = (_APP_DIR / "static" / "js" / "app.js").read_text(encoding="utf-8")
_APP_HTML = (_APP_DIR / "templates" / "index.html").read_text(encoding="utf-8")

check(
    "app.js: never uses innerHTML (DOM built with createElement/textContent only)",
    "innerHTML" not in _APP_JS,
)

_html_ids = set(re.findall(r'id="([^"]+)"', _APP_HTML))
_js_ids = set(re.findall(r'getElementById\(["\']([^"\']+)["\']\)', _APP_JS))
_missing_ids = _js_ids - _html_ids
check(
    "app.js: every getElementById has a matching element in index.html",
    not _missing_ids,
    f"referenced but not found in the template: {_missing_ids}",
)

print(f"\n{passed} passed, {len(failed)} failed")
if failed:
    for name in failed:
        print(f"  FAILED: {name}")
    sys.exit(1)
print("ALL CHECKS PASSED")
