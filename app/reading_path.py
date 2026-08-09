"""Order a set of retrieved papers into a reading path from their citation
graph. Computed from data, never asked of an LLM to guess. This is app/'s own
copy -- see mcp_server/lakebase.py's docstring for why shared modules are
duplicated per app folder.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Paper:
    work_id: str
    foundational_score: float = 0.0
    title: str = ""


def build_reading_path(
    papers: list[Paper],
    edges: list[tuple[str, str]],
) -> list[dict]:
    by_id = {p.work_id: p for p in papers}
    for citing, cited in edges:
        if citing not in by_id or cited not in by_id:
            raise KeyError(
                f"edge ({citing!r} -> {cited!r}) references a paper outside "
                "the supplied set -- restrict edges to the induced subgraph "
                "before calling build_reading_path()"
            )

    cites: dict[str, set[str]] = {p.work_id: set() for p in papers}
    cited_by: dict[str, set[str]] = {p.work_id: set() for p in papers}
    for citing, cited in edges:
        cites[citing].add(cited)
        cited_by[cited].add(citing)

    remaining_prereqs = {work_id: len(cites[work_id]) for work_id in by_id}

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
