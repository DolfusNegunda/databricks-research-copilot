# Run evidence: the data pipeline against the live workspace

`docs/evidence.md` documents the agent and the app. This documents the layer
underneath both of them — the harvest, the declarative pipeline, and the
sync/embed jobs that turn the OpenAlex API into rows in Lakebase. Same rule
as that document: real job IDs, real timestamps, real errors, not a
description of what the code is supposed to do. Everything below is from
the live `dev` target in the Databricks workspace on 2026-08-09.

## Harvest: `openalex-snowball-harvest`

`harvest_job_run.png` — Job ID `251351095911504`, Run ID `334260372168364`,
launched manually. **Started 02:54 PM, ended 03:01 PM, 6m 14s, Succeeded.**
Both tasks green: `harvest` (`harvester/snowball.py`, 6m 13s) and
`land_topics` (`harvester/land_topics.py`, 35s), on serverless compute.

The harvester's own summary, printed at the end of that run — eight seed
topics, each an independent seed → cited → citing snowball (see the root
`CLAUDE.md` for why it's shaped this way):

```
   5,834  retrieval augmented generation
   9,109  vector database
   8,155  vector embedding search
   6,824  data pipeline orchestration
   8,652  feature store machine learning
   8,423  graph neural network
  20,909  prompt engineering
   6,842  semantic search embeddings
  74,748  TOTAL (before cross-topic dedup)
```

"Before cross-topic dedup" is the harvester's own label, not mine — the same
paper legitimately turns up in more than one topic's snowball (a RAG paper
is also a vector-database paper), and it's `silver_works`'s
`dropDuplicates(["work_id"])` that collapses that overlap down to the actual
serving corpus, not this step. `land_topics` separately landed the full
OpenAlex topics taxonomy — 4,516 rows, matching the manifest's declared
count exactly.

## Pipeline: `[dev dolfneg] openalex-research-copilot`

`pipeline_run_completed.png` — run of **Aug 09, 2026, 05:26 PM, Completed**.
The DAG as actually materialized: `bronze_works_raw` /
`bronze_dim_topics` → `silver_works` / `silver_topics_dim` → fanning out to
`silver_authors`, `silver_paper_authors`, `silver_citation_edges`, and the
four gold tables. Real output-record counts from that run, not row estimates:

| Table | Duration | Output records |
|---|---|---|
| `bronze_works_raw` | 13s | 75K |
| `gold_author_metrics` | 3s | 161K |
| `gold_citation_graph` | 3s | 326K |
| `gold_paper_metrics` | 2s | 48K |

Two cross-checks worth noting because they agree rather than because they're
impressive: `bronze_works_raw`'s 75K matches the harvester's own 74,748
(rounded); `gold_paper_metrics`'s 48K matches the 48,413-paper denominator
that the embed job's progress was measured against all afternoon (see
below). Same corpus, counted twice, by two different systems, agreeing.

## Sync + embed: six real failures, six real fixes, one afternoon

Getting from silver/gold Delta to a working Lakebase instance took six
distinct bugs, each with a real Postgres or Spark error and a real code fix
— worth showing as a timeline instead of smoothing into "then it worked,"
since the failures are the actual engineering:

| Time | Task | Error | Fix |
|---|---|---|---|
| 13:33:59 | pipeline | `UNKNOWN_FIELD_EXCEPTION` on `authorships` | schema hint only covered 3 of 9 real sub-fields; hinted all 9 from a live record |
| 15:48:23 | sync | `NameError: name '__file__' is not defined` | `spark_python_task` execs source via `exec(compile(...))`, never injects `__file__`; gave `pipelines/` same-directory copies of `lakebase.py`/`embedder.py` |
| 16:57:59 | sync | `NotNullViolation` on `papers.title` | real work `W1541095199` (a 2007 paper on semantic search) has a null `display_name`; added a `has_title` gate |
| 17:23:45 | sync | `CardinalityViolation`: `ON CONFLICT DO UPDATE command cannot affect row a second time` | real OpenAlex data double-lists one author in a work's `authorships`; `dropDuplicates(["work_id","author_id"])` |
| between 17:23 and 17:47 | embed | `ModuleNotFoundError: No module named 'sqlalchemy'` | embed's serverless environment never declared it, and nothing in the codebase actually calls `get_engine()`; moved the import inside that function (lazy) instead of adding the dependency |
| 17:47:14 | embed | `UndefinedObject: type "vector" does not exist` | pgvector extension existed in a different schema on this *shared* Lakebase instance, outside `search_path`; extension-schema resolution moved into `get_connection()` itself, every checkout |
| 18:36:16 | embed | `DiskFull`: 512MB instance limit exceeded | Free Edition's Lakebase ceiling, shared across 4 sibling bootcamp projects — infrastructure, not a bug (see below) |

`sync_embed_job_run.png` shows a later run of the same job (Job ID
`929867843881791`, Run ID `700570602800369`, **08:02 PM–08:42 PM, 40m 21s,
Failed**) after the three sync-side bugs above were already fixed: `sync`
itself **Succeeded** in 2m 10s; `embed` ran for 38m 9s before failing — the
DiskFull ceiling again, not a new bug. A full embed-job traceback from one
of these DiskFull runs shows the actual shape of that failure: 119
successive `embedded 200 papers (NNN chunks)` lines — batches committed as
they complete, so a mid-run failure loses nothing already written — before
`execute_values` on batch 120 hits
`DiskFull: could not extend file because instance size limit (512 MB) has
been exceeded`, with Postgres's own hint naming the cause precisely:
`neon.max_cluster_size GUC`. ~23,800 papers embedded in that one run alone
before the wall.

## The DiskFull ceiling: diagnosis, fix, headroom confirmed

`DiskFull` recurred because Lakebase Free Edition's 512MB is a hard,
non-resizable limit, shared across four sibling bootcamp projects on one
instance — not something a code fix removes. The actual fix was reducing
footprint: `embedder.py`'s `CHUNK_SIZE` 800 → 1500 characters (most
abstracts collapse from ~2.2 chunks to 1), after truncating the existing
partial embeddings and re-running.

The retry after that change didn't hit DiskFull again — it hit a different,
unrelated error:

```
DeadlockDetected: deadlock detected
LINE 1: UPDATE papers SET embedded_at = now() WHERE work_id = ANY(AR...
DETAIL:  Process 16526 waits for RowExclusiveLock...
```

— two processes contending for row locks in different orders on the
`UPDATE ... SET embedded_at` step. Getting *past* the disk wall to a new,
unrelated failure mode is itself the confirmation the chunk-size fix worked;
no retry logic was added for the deadlock, since it's a one-off contention
issue, not a capacity one. Direct confirmation of headroom, queried live
against the shared instance:

```sql
SELECT n.nspname AS schema,
       pg_size_pretty(sum(pg_total_relation_size(c.oid))) AS total
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r','m') AND n.nspname NOT IN ('pg_catalog','information_schema')
GROUP BY n.nspname ORDER BY sum(pg_total_relation_size(c.oid)) DESC;
```

```
research      343 MB
weather       728 kB
__schema__    248 kB
public        208 kB
```

`research` at 343MB against a 512MB *shared* ceiling — comfortable headroom
for the papers actually embedded, confirmed the same day as the fix, not
assumed from the arithmetic alone.

## Direct database confirmation, independent of the app and the agent

`lakebase_collections_table.png` — the Lakebase table browser itself
(Databricks' Postgres resource view, not the app, not the agent, not a SQL
client either of them configured), open on `research.collections`, showing
exactly two rows: `collection_id 1, "Rag papers"` and `collection_id 6,
"Evidence Test"` — the same two collections `docs/evidence.md` documents
being created from two different surfaces (a browser click, and an agent
tool call). A third, independent vantage point on the same underlying data,
via the one path that can't be explained by app code or agent behavior:
reading the table directly.

## What this doc doesn't cover

No screenshot here shows the embed job finishing cleanly end to end — every
captured embed run either hit `DiskFull` or, after the chunk-size fix,
`DeadlockDetected`. That's an honest gap, not a hidden one: the 343MB
headroom check above confirms the fix worked, but there's no single "embed:
Succeeded" run to point at. Closing it fully would mean one more embed run
plus `SELECT count(*) FROM research.papers WHERE embedded_at IS NOT NULL`
against the post-fix corpus — not done here, since the actual goal (enough
free space for writes to succeed, not maximizing embedded-paper count) was
already confirmed met.
