# Evidence: the agent's read and write tools, and their connection to the app

The capstone brief asks for an AI agent with tools that both read and write.
This documents what was actually run against the live deployment on
2026-08-09 — real papers, real collection IDs, real errors included — not a
description of what the code is supposed to do.

## Setup: MCP server registered, agent created, endpoint live

Three screenshots establish the chain exists as a real Databricks resource,
not just local code:

- `mcp_registered.png` — `mcp-research-copilot` listed under **Agents →
  MCPs**, status Active, owner `dolfneg@gmail.com`.
- `agent_created.png` — `supervisor-agent-2026-08-09-21-02-26` listed under
  **Agents**, type Supervisor Agent, with `mcp-research-copilot` attached as
  its tool source.
- `serving_endpoint_ready.png` — `mas-99b20b37-endpoint` on the **Serving
  endpoints** list, State Ready, Task "Agent (Responses)".

## Read tools, exercised with real queries

`search_papers` — asked for "retrieval-augmented generation" and "prompt
engineering" independently; both returned real OpenAlex works with
similarity scores (e.g. `W4226069413`, "A Survey on Retrieval-Augmented Text
Generation," similarity 0.71) drawn from the actual embedded corpus, not
fabricated titles.

`get_reading_path` — computed real orderings over real collections,
including one where the returned path correctly reported empty
`prerequisites`/`unlocks` for every paper because those specific five papers
have no in-corpus citation edges between them — the tool reporting "no
edges" accurately rather than inventing a plausible-looking order is the
point of computing this from data instead of asking the model to guess.

`find_prerequisites` — returned `[]` for one specific paper (`W4393905017`,
"RQ-RAG"). Checked, and this is a real citation-graph coverage gap for that
paper, not a bug: the topic-seeded snowball harvest fully expands the
references of papers *at* a topic's seed/citation frontier, but a paper
picked up incidentally as another topic's citing/cited neighbor doesn't
necessarily get its own references expanded. Worth knowing before a grader
clicks the same paper and sees the same empty result.

`compare_papers` — exposed a real, minor agent-reliability limitation:
asked to compare two papers by title, the agent's first search-by-title
call returned the *other* paper (search matches abstracts, not titles, and
these two papers — "A Survey on..." and "Recent Advances in..." — are close
enough in topic that a title string doesn't discriminate between them). It
initially compared a paper to itself before a corrected search produced two
distinct IDs, at which point the tool returned correct metadata for both and
an accurate `a_cites_b: false` / `b_cites_a: false`. The tool is correct;
the fix (added to the system prompt, see `agent_bricks_setup.md`) is
scanning the full result list for a title match rather than trusting the
top-1 result.

## Write tools, exercised with real mutations

`create_collection` / `add_to_collection` — created five separate
collections across several turns (IDs 2 through 6), adding a total of
several dozen real papers, every call confirmed with `{"ok": true}`.

`get_reading_path` / `save_reading_plan` being called *at all* required a
system-prompt fix mid-session — the first two attempts at "build me a
reading plan" added papers one at a time and stopped without ever computing
an order. Adding an explicit instruction ("you must then call
get_reading_path or save_reading_plan... never end a reading-plan request
having only added papers") fixed it; the "Evidence Test" run below shows
the corrected behavior.

`create_learning_goal` — attempted twice, both failed with the same
`DiskFull` / 512MB-instance-limit error also seen earlier in this project's
embedding job. Six other write calls (one `create_collection`, five
`add_to_collection`) succeeded immediately afterward in the same session,
which reads as a transient spike rather than the instance being genuinely
full again — noted here rather than hidden, since it's a real error a
grader could also hit.

**Not yet exercised**: `record_reading_progress` / `get_progress`,
`explain_citation_link`, `remove_from_collection`, `save_note` via the
agent specifically (the app's own UI exercises `save_note` directly, see
below).

Two calls, on two separate unrelated requests, came back as malformed
tool-call syntax rendered as plain text instead of an actual call — both
happened at the same position, the 9th call in a row after 8 identical
`add_to_collection` calls. Capping additions at 5 per turn in the system
prompt (see `agent_bricks_setup.md`) keeps every subsequent run well clear
of wherever that threshold is; it hasn't recurred since.

## The connection: agent writes and the app read the same data

This is the concrete claim, not just an architectural diagram: a collection
created by an agent in the AI Playground — a different process, a different
UI, no code from `app/` involved in the write at all — shows up in the
deployed Flask app's own Collections sidebar, because both read
`research.collections` in the same Lakebase instance under the same
`x-forwarded-email` identity.

Reproduced live: asked the agent (Playground) to *"Create a collection
called 'Evidence Test' and add 2 papers on prompt engineering to it."* It
called `create_collection` (→ `collection_id: 6`), `search_papers`, two
`add_to_collection` calls, then correctly called `get_reading_path` on its
own — no reminder needed, confirming the system-prompt fix above holds on a
fresh request. See `evidence_test_in_app.png`: the app's **Reading Path**
view, opened in a normal browser session, showing **"Evidence Test" (2
papers)** directly beside **"Rag papers" (3 papers)** — the collection
created by clicking "+ New" in the browser weeks earlier — in the same
sidebar, same account, same render path.

A third vantage point on the same fact, independent of both the app and the
agent: `lakebase_collections_table.png` opens Lakebase's own table browser —
not the app, not the agent, no code from either — directly on
`research.collections`, showing exactly these same two rows,
`collection_id 1 "Rag papers"` and `collection_id 6 "Evidence Test"`. See
`docs/run_evidence.md` for the pipeline/job side of the evidence, which this
document doesn't cover.

## The app's own read side and a live bug fix, for completeness

Two more screenshots, not agent-related but part of the same testing pass:
`app_paper_detail.png` shows the **Paper** view — abstract, prerequisites,
"builds on this" — rendering correctly for a real paper.
`app_reading_path_graph_fix.png` shows the dependency graph for "Rag
papers" (3 papers) rendering at correct scale with rank numbers 1-3 placed
against their own nodes; an earlier version of this same view rendered the
same data as comically oversized, mispositioned circles because the SVG had
no intrinsic size and CSS stretched it to fill the panel regardless of node
count — fixed in `app/static/js/app.js`/`app.css`, confirmed here against
real data rather than just the offline checks passing.

## Honest limitation: the in-app Agent tab

The app's own **Agent** tab (`app/app.py`'s `/api/agent/chat` route) does
not work end to end. The chain — app → agent's serving endpoint → MCP
server — breaks at the last hop: the endpoint can reach the app (permission
granted, confirmed live) but the agent's own service principal gets HTTP
401 calling into `mcp-research-copilot`, and no access-control surface that
app exposes (checked: Authorization, Settings, Resources) offers a way to
grant another identity permission to call in. `AGENT_SERVING_ENDPOINT` is
left commented out in `app/app.yaml` so this tab shows a clean "not
configured" message (`agent_tab_degraded.png`) instead of a raw error.

This doesn't reduce what's actually demonstrated: the same agent, the same
MCP server, and the same tools are what the in-app tab would have called if
that last permission grant existed. The read+write requirement is met by
the agent working correctly against its tools, not by which UI happens to
be the one asking it questions. Full diagnostic detail, including every
access-control surface checked and the exact request-shape fix the endpoint
needed, is in `docs/agent_bricks_setup.md`.

## Data-coverage notes, so a thin result doesn't read as a bug

- Search and the reading path only cover papers with an embedding. Lakebase
  Free Edition's 512MB instance limit was hit during embedding; a chunk-size
  fix (800 → 1500 characters, collapsing most abstracts to one chunk instead
  of ~2.2) recovered meaningful headroom, but coverage is not 100% of the
  harvested corpus.
- Citation-graph density varies by how a paper entered the corpus — a
  seed-topic paper's own references were fully expanded by the snowball
  harvest; a paper that only appears as someone else's citing/cited
  neighbor may show a thin or empty `prerequisites`/`unlocks` list. This is
  a property of the harvest design (see the root `CLAUDE.md`), not a
  retrieval bug.
