"""Order a set of retrieved papers into a reading path from their citation
graph -- the capstone's signature feature. Computed from data (which papers
in the set cite which others), never asked of an LLM to guess.

The core idea: within the retrieved subgraph, a paper that is cited *by other
papers in the set* is foundational relative to them and should be read first.
This is a topological sort over "cites" edges, read in reverse (a paper's
prerequisites are the things IT cites, so a work with no unread prerequisite
in the set is safe to schedule next), with two real-world complications a
naive topo-sort ignores:

  1. The graph is very unlikely to be a clean DAG in practice -- citation
     rings, and more commonly retrieval returning two papers that (indirectly
     or directly) cite each other, are real. A cycle must not hang the whole
     ordering; it must degrade to a deterministic tie-break, for exactly the
     papers involved in it, and nothing else.
  2. Disconnected papers (nothing in the retrieved set cites them, and they
     cite nothing else in the set) carry no ordering signal at all. They must
     still appear in the output, positioned by their own merit
     (foundational_score), not dropped or arbitrarily first/last.

Callers must restrict `edges` to the induced subgraph over `papers` before
calling this (see get_reading_path in the MCP server / app serving layer,
which pulls candidates via pgvector search then filters citation_edges down
to just those work_ids). Checked offline in scripts/check_api.py.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Paper:
    work_id: str
    foundational_score: float = 0.0  # tie-break signal: e.g. citation percentile blended with age
    title: str = ""


def build_reading_path(
    papers: list[Paper],
    edges: list[tuple[str, str]],
) -> list[dict]:
    """papers: the retrieved set. edges: (citing_id, cited_id) pairs, restricted
    to the induced subgraph (both ends must be in `papers`) by the caller --
    this function does not filter edges itself, so a caller passing an edge
    with an endpoint outside `papers` gets a KeyError, deliberately: that is a
    caller bug (the induced-subgraph restriction happening upstream, in SQL),
    not something to silently drop here.

    Returns papers in reading order, each annotated with why it landed there:
    `prerequisites` (ids in this set that it cites) and `unlocks` (ids in this
    set that cite it) -- both empty for a fully disconnected paper.
    """
    by_id = {p.work_id: p for p in papers}
    for citing, cited in edges:
        if citing not in by_id or cited not in by_id:
            raise KeyError(
                f"edge ({citing!r} -> {cited!r}) references a paper outside "
                "the supplied set -- restrict edges to the induced subgraph "
                "before calling build_reading_path()"
            )

    cites: dict[str, set[str]] = {p.work_id: set() for p in papers}  # citing -> {cited}
    cited_by: dict[str, set[str]] = {p.work_id: set() for p in papers}  # cited -> {citing}
    for citing, cited in edges:
        cites[citing].add(cited)
        cited_by[cited].add(citing)

    # in_degree here means "how many prerequisites (things it cites, that are
    # IN this set) remain unscheduled" -- this is a prerequisite count, the
    # reverse of citation in-degree. A paper becomes schedulable once every
    # paper it cites has already been placed in the path.
    remaining_prereqs = {work_id: len(cites[work_id]) for work_id in by_id}

    # Deterministic tie-break: higher foundational_score first, then work_id
    # for full determinism when scores tie exactly. Never insertion order --
    # this must be reproducible independent of how the caller happened to
    # order `papers`.
    def sort_key(work_id: str) -> tuple[float, str]:
        return (-by_id[work_id].foundational_score, work_id)

    ready = sorted(
        (wid for wid, count in remaining_prereqs.items() if count == 0),
        key=sort_key,
    )

    ordered: list[str] = []
    while ready:
        ready.sort(key=sort_key)
        current = ready.pop(0)
        ordered.append(current)
        for dependent in cited_by[current]:
            if dependent in ordered or dependent in ready:
                continue
            remaining_prereqs[dependent] -= 1
            if remaining_prereqs[dependent] == 0:
                ready.append(dependent)

    # Anything left has remaining_prereqs > 0 -- it is part of a cycle (its
    # prerequisites never all clear). Append in the same deterministic
    # tie-break order rather than hanging or raising: a citation ring is real
    # data, not a bug to crash on.
    cyclic_remainder = sorted(
        (wid for wid in by_id if wid not in ordered),
        key=sort_key,
    )
    ordered.extend(cyclic_remainder)

    return [
        {
            "work_id": wid,
            "title": by_id[wid].title,
            "foundational_score": by_id[wid].foundational_score,
            "prerequisites": sorted(cites[wid]),
            "unlocks": sorted(cited_by[wid]),
            "in_citation_cycle": wid in cyclic_remainder,
        }
        for wid in ordered
    ]
