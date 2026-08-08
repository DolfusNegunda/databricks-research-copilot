"""Topic-seeded citation-snowball harvester -- replaces pulling an arbitrary
OpenAlex Parquet partition slice, which measured at 0.09-0.18% induced-subgraph
citation density (see docs/corpus_strategy.md for the full measurement). This
harvester builds a corpus where citation edges are dense *by construction*:

  1. Seed  -- top-N works for a topic query, by citation count.
  2. Cited -- the union of what the seeds cite (`referenced_works` is already
     inline on every work -- no extra call per seed).
  3. Citing -- the union of works that cite any seed (`filter=cites:{id}`,
     batched the same `|`-OR way as everything else here).
  4. Resolve -- batch-fetch full records for every new id collected in steps
     2-3 (`filter=openalex_id:W1|W2|...`, 100 ids/call), so the corpus has
     complete data for the whole induced subgraph, not just the seeds.

Measured on a 50-seed spike for "retrieval augmented generation": 3,027-work
corpus, 19.14% in-corpus edge density -- roughly 100-150x the partition-slice
approach. See the capstone plan for the full numbers.

Output: one JSON Lines file per seed topic, one record per work, each record
the raw OpenAlex fields needed downstream (see SELECT_FIELDS) plus `_seed_topic`
and `_hop` provenance. Landed wherever OUT_DIR points -- a local directory when
run by hand, a UC Volume path when run as a Databricks job (parametrize via
--out-dir or the OPENALEX_HARVEST_DIR env var; nothing else changes).

Run standalone: python harvester/snowball.py --out-dir ./harvest --per-topic 300
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

API_ROOT = "https://api.openalex.org/works"
MAILTO = os.environ.get("OPENALEX_MAILTO", "dnegunda@bscglobal.com")
USER_AGENT = f"databricks-research-copilot/1.0 (mailto:{MAILTO})"

# Coherent technical subfields, sized by measured meta.count before picking
# (see the plan): thousands-to-low-100k, not generic buzzwords that show up
# in nearly every paper's introduction regardless of relevance.
SEED_TOPICS = [
    "retrieval augmented generation",
    "vector database",
    "vector embedding search",
    "data pipeline orchestration",
    "feature store machine learning",
    "graph neural network",
    "prompt engineering",
    "semantic search embeddings",
]

SELECT_FIELDS = ",".join(
    [
        "id",
        "doi",
        "display_name",
        "publication_year",
        "cited_by_count",
        "fwci",
        "language",
        "primary_topic",
        "open_access",
        "authorships",
        "abstract_inverted_index",
        "referenced_works",
    ]
)

BATCH_SIZE = 100  # OpenAlex's own OR-filter cap, confirmed empirically


# ---------------------------------------------------------------------------
# Pure helpers -- no network, checked offline in scripts/check_api.py
# ---------------------------------------------------------------------------


def short_id(openalex_id: str) -> str:
    """"https://openalex.org/W123" or "W123" -> "W123". Idempotent."""
    return openalex_id.rsplit("/", 1)[-1]


def batched(items, size: int):
    """Chunk `items` into lists of at most `size` -- for the 100-wide OR
    filter cap. Yields nothing for an empty input; never yields an empty
    chunk otherwise.
    """
    items = list(items)
    for i in range(0, len(items), size):
        yield items[i : i + size]


def build_or_filter(field: str, ids) -> str:
    """field="openalex_id", ids=["W1","W2"] -> "openalex_id:W1|W2".
    Raises ValueError on an empty id list -- an empty OR-filter is a caller
    bug (would either error against the API or silently match everything
    depending on how it degrades), not something to send.
    """
    ids = [short_id(i) for i in ids]
    if not ids:
        raise ValueError("build_or_filter requires at least one id")
    return f"{field}:" + "|".join(ids)


# ---------------------------------------------------------------------------
# Network calls -- exercised live, not by the offline checks
# ---------------------------------------------------------------------------


def _get(url: str, retries: int = 3, backoff: float = 1.5):
    last_exc = None
    for attempt in range(retries):
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.load(resp)
        except (urllib.error.HTTPError, urllib.error.URLError) as exc:
            last_exc = exc
            if attempt < retries - 1:
                time.sleep(backoff * (attempt + 1))
    raise RuntimeError(f"GET failed after {retries} attempts: {url}") from last_exc


def fetch_seeds(topic: str, n: int) -> list[dict]:
    """Top-n works for a topic query, ranked by citation count."""
    filt = "title_and_abstract.search:" + urllib.parse.quote(topic)
    url = (
        f"{API_ROOT}?filter={filt}&per-page={min(n, 200)}&sort=cited_by_count:desc"
        f"&select={SELECT_FIELDS}&mailto={MAILTO}"
    )
    return _get(url).get("results", [])


def resolve_ids(ids) -> list[dict]:
    """Batch-fetch full records for arbitrary work ids, 100 at a time."""
    out: list[dict] = []
    for chunk in batched(ids, BATCH_SIZE):
        filt = build_or_filter("openalex_id", chunk)
        url = f"{API_ROOT}?filter={filt}&per-page={len(chunk)}&select={SELECT_FIELDS}&mailto={MAILTO}"
        out.extend(_get(url).get("results", []))
    return out


def fetch_citing(seed_ids) -> list[dict]:
    """Works that cite any of the given seed ids -- `cites:` accepts the same
    `|`-OR batching as `openalex_id:` (confirmed via the API's own x_query
    echo: it's implemented as an OR over referenced_works), so this is one
    call per 100 seeds, not one call per seed.
    """
    out: list[dict] = []
    seen: set[str] = set()
    for chunk in batched(seed_ids, BATCH_SIZE):
        filt = build_or_filter("cites", chunk)
        url = f"{API_ROOT}?filter={filt}&per-page=200&select={SELECT_FIELDS}&mailto={MAILTO}"
        for work in _get(url).get("results", []):
            wid = short_id(work["id"])
            if wid not in seen:
                seen.add(wid)
                out.append(work)
    return out


def harvest_topic(topic: str, per_topic: int) -> list[dict]:
    """Run the full 4-step snowball for one topic. Returns tagged records
    (each work dict plus _seed_topic and _hop), deduplicated by work id --
    a work found via multiple hops keeps its *first* hop tag (seed beats
    cited beats citing), since that is the most informative provenance.
    """
    tagged: dict[str, dict] = {}

    def tag(works: list[dict], hop: str) -> None:
        for w in works:
            wid = short_id(w["id"])
            if wid not in tagged:
                tagged[wid] = {**w, "_seed_topic": topic, "_hop": hop}

    seeds = fetch_seeds(topic, per_topic)
    tag(seeds, "seed")
    seed_ids = [short_id(w["id"]) for w in seeds]
    print(f"  [{topic}] seeds: {len(seeds)}", file=sys.stderr)

    cited_ids = sorted({short_id(r) for w in seeds for r in (w.get("referenced_works") or [])} - set(seed_ids))
    if cited_ids:
        tag(resolve_ids(cited_ids), "cited")
    print(f"  [{topic}] + cited: {len(tagged)} total", file=sys.stderr)

    citing = fetch_citing(seed_ids)
    tag(citing, "citing")
    print(f"  [{topic}] + citing: {len(tagged)} total", file=sys.stderr)

    return list(tagged.values())


def write_jsonl(records: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out-dir",
        default=os.environ.get("OPENALEX_HARVEST_DIR", "./harvest"),
        help="Output directory (local path now; a UC Volume path when run as a Databricks job).",
    )
    parser.add_argument("--per-topic", type=int, default=200, help="Seed works per topic (<=200).")
    parser.add_argument("--topics", nargs="*", default=SEED_TOPICS, help="Override the default topic list.")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    summary = []
    for topic in args.topics:
        print(f"harvesting: {topic!r}", file=sys.stderr)
        records = harvest_topic(topic, args.per_topic)
        slug = topic.replace(" ", "_")
        write_jsonl(records, out_dir / f"{slug}.jsonl")
        summary.append((topic, len(records)))

    print("\nharvest summary:")
    for topic, count in summary:
        print(f"  {count:>6,}  {topic}")
    print(f"  {sum(c for _, c in summary):>6,}  TOTAL (before cross-topic dedup)")


if __name__ == "__main__":
    main()
