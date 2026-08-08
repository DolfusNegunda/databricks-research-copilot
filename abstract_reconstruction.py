"""Reconstruct plain-text abstracts from OpenAlex's inverted index.

OpenAlex never gives you a plain `abstract` field -- confirmed during planning
by fetching real records from both the REST API and the Parquet snapshot. What
exists instead is `abstract_inverted_index`: {word: [positions]}, and it
arrives in two different shapes depending on the source:

  - REST API:  a JSON *object*      {"Despite": [0], "growing": [1], ...}
  - Parquet:   a JSON *string*      '{"Despite": [0], "growing": [1], ...}'

Silently getting this wrong produces an empty embedding corpus with no error
anywhere pointing at why -- exactly the failure mode this function exists to
prevent. It accepts either shape.

Also real, confirmed during planning: some works have no abstract at all --
an empty dict, an absent key, or (rarely) a malformed/non-dict payload. All
of those must return None, not raise and not silently swallow into "".

Used from the silver layer of the declarative pipeline (pipelines/) as the
abstract_inverted_index -> narrative_abstract transform, and from the
harvester/enrichment path wherever a raw OpenAlex work needs a plain-text
abstract before embedding. Checked offline in scripts/check_api.py.
"""

from __future__ import annotations

import json


def reconstruct_abstract(inverted_index) -> str | None:  # noqa: ANN001
    """word->positions map (dict, or a JSON string of one) -> plain text.

    Returns None if there is nothing usable to reconstruct -- never "", so a
    caller filtering on `IS NOT NULL` behaves correctly and a caller checking
    truthiness doesn't need to special-case an empty string separately.
    """
    if inverted_index is None:
        return None

    if isinstance(inverted_index, str):
        stripped = inverted_index.strip()
        if not stripped:
            return None
        try:
            inverted_index = json.loads(stripped)
        except (json.JSONDecodeError, ValueError):
            return None

    if not isinstance(inverted_index, dict) or not inverted_index:
        return None

    position_to_word: dict[int, str] = {}
    for word, positions in inverted_index.items():
        if not isinstance(positions, (list, tuple)):
            continue
        for position in positions:
            if isinstance(position, int) and position >= 0:
                # Last write wins on a genuine collision (should not happen in
                # real OpenAlex data; deterministic rather than undefined if
                # it ever does).
                position_to_word[position] = word

    if not position_to_word:
        return None

    ordered = [position_to_word[p] for p in sorted(position_to_word)]
    text = " ".join(ordered).strip()
    return text or None
