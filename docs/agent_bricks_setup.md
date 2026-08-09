# Registering the agent

Manual, workspace-UI steps done after `app/` and `mcp_server/` are both
deployed — none of this can be scripted from the repo. Rewritten after
actually doing it live against a Free Edition workspace (2026-08-09); the
menu paths below are what was actually there, not a guess at Databricks'
general docs. If a label has moved, search for the same concept rather than
assuming the step no longer applies.

## 1. Confirm the MCP server is reachable

Deploy `mcp_server/` as its own Databricks App (Git folder → Pull → Deploy,
same as every app in this workspace). Confirm it's up:

```
curl https://<mcp-server-app-url>/healthz
```

Should return `{"status": "healthy", ...}`. If `"degraded"`, Lakebase isn't
configured yet on that app — fix that before registering it with anything.

## 2. Register the MCP server

**Agents** (left nav) → **MCPs** tab → **+ MCP**. Point it at the deployed
`mcp_server/` app. This is what makes the tools in `mcp_server/tools.py`
discoverable to an agent — confirmed by name (`mcp-research-copilot`) showing
up, Active, on that tab.

(Not "AI Gateway → MCP servers" — that's a different, older surface on this
workspace and doesn't list Databricks-App-hosted MCP servers the same way.)

## 3. Create the Agent Bricks agent

**Agents → Create Agent** opens a type picker — there's no generic "make an
agent" option. The presets are Supervisor Agent, Information Extraction,
Text Classification, Genie Agent, and "Code your own agent" (the code-first
Agent Framework path, heavier: write a Python agent, register it to Unity
Catalog, deploy via `agents.deploy()`). **Supervisor Agent** is the one that
takes MCP servers as a tool source plus a plain-text instructions field —
pick that.

In its builder: **Tools and sub-agents** → search box (clear any
`type:built-in` filter first) → find `mcp-research-copilot` → add it.

**Instructions** (the system prompt) — the load-bearing rule is still: the
agent must call `get_reading_path` or `save_reading_plan` for an ordering,
never invent one — that tool call is the entire point of the citation-graph
reading path. Two more lines earned from live testing, not guessed:

> You are the Research Copilot assistant, helping users explore academic
> papers on OpenAlex and build citation-ordered reading plans.
>
> Always call your tools rather than answering from your own knowledge --
> cite real work_ids and titles from tool results, never invent one.
>
> When a user asks for a reading plan, a reading order, or how to sequence
> papers on a topic:
> 1. Search for relevant papers and create or reuse a collection.
> 2. Add at most 5 of the most relevant papers to it, then stop adding and
>    move on -- never add more than 5 in a single turn.
> 3. You must then call get_reading_path or save_reading_plan on that
>    collection before replying. Never end a reading-plan request having
>    only added papers -- the ordering step is required, not optional.
>
> When a user names a specific paper, search_papers matches against
> abstracts, not titles -- a title-only query can return a different paper
> on a very similar topic. Check the full result list for a title match
> before picking a work_id, rather than trusting the top result blindly.
>
> When comparing papers or explaining a citation relationship, use
> compare_papers or explain_citation_link rather than reasoning about it
> yourself.
>
> Keep replies concise and reference specific papers by title.

Why the "at most 5" line: without it, a long run of near-identical
`add_to_collection` calls (8+ in a row) reliably broke on this workspace —
the model emitted malformed tool-call syntax as plain text instead of a real
call, twice, at the same position, on two separate unrelated requests.
Capping the run keeps it well clear of wherever that threshold is.

The test panel on this same page runs against *your* identity while you're
signed in — a `create_collection`/`add_to_collection` call working there
does not by itself prove the *deployed* endpoint works standalone (see §6).

## 4. Get the serving endpoint

Saving/testing the agent auto-provisions a serving endpoint — there's no
separate "deploy" click. Its name and status show as a green-dot
**Endpoint** link (with an external-link icon) in the header once the agent
has been saved. That name is what `AGENT_SERVING_ENDPOINT` needs.

It also shows up on the classic **Serving → Serving endpoints** list once
created (Task column reads "Agent (Responses)" -- a second, independent
confirmation of the request shape in §6) -- it just won't be there *before*
the agent exists, which is easy to mistake for "this workspace doesn't
support agent endpoints" if checked too early.

## 5. Grant the app permission to query the endpoint

Querying the endpoint from `app/` requires an explicit grant — Databricks
Apps run under their own service principal, separate from your own identity,
and it starts with zero access to anything.

`app/` (the Flask app, in the workspace) → **Edit** → **App resources** →
**+ Add resource** → **Model serving endpoint** → select the agent's
endpoint → permission **Can query** → save → redeploy `app/`.

## 6. Wire the endpoint into the app -- and its actual request shape

```yaml
# app/app.yaml
env:
  - name: AGENT_SERVING_ENDPOINT
    value: "<the agent's serving endpoint name, from step 4>"
```

This endpoint is **Responses-API shaped**, not the classic ChatCompletions
shape the SDK's typed helper expects. `WorkspaceClient().serving_endpoints
.query(inputs=...)` (and `messages=`, `instances=`) all send the wrong body
and the endpoint 400s asking for `input` specifically. What actually works,
confirmed live (`app/app.py`'s `/api/agent/chat` route): use the SDK only for
its already-authenticated config, then POST the exact shape by hand --

```python
cfg = WorkspaceClient().config
resp = requests.post(
    f"{cfg.host}/serving-endpoints/{AGENT_SERVING_ENDPOINT}/invocations",
    headers=cfg.authenticate(),
    json={"input": [{"role": "user", "content": message}], "stream": False},
    timeout=60,
)
```

`stream: False` matters -- omit it and a 200 response comes back as SSE
chunks (`event: ...\ndata: ...`), which fails `resp.json()` with a bare
`Expecting value: line 1 column 1` rather than anything that names the
actual problem.

## Where this currently dead-ends on Free Edition

Even with steps 1-6 done and the app able to reach the endpoint, the
*endpoint itself* fails calling back into the MCP server:

```
event: error
data: {"error_code": "INVALID_PARAMETER_VALUE", "message": "Failed to
register tools from Databricks App MCP server 'mcp-research-copilot':
HTTP 401"}
```

The agent's own service principal has never been granted permission to call
`mcp-research-copilot` (which is itself a private-by-default Databricks
App). Every access-control surface that app exposes was checked live and
none of them offers a way to grant another identity permission to call in:
**Authorization** (only shows this app's own outbound service-principal
identity and on-behalf-of-user OAuth scopes -- unrelated direction),
**Settings** (General / Resources / User authorization / Compute / App
telemetry -- no ACL anywhere), and **Edit → Resources** (also outbound-only
-- adding the *agent's* endpoint as a resource *here* grants the MCP server
permission to call the agent, the reverse of what's needed).

Given that, `AGENT_SERVING_ENDPOINT` in `app/app.yaml` is left commented
out: the in-app **Agent** tab shows a clean "not configured" message instead
of a live error. If a future Databricks release exposes an access-control
page for App-hosted MCP servers, steps 1-6 above are otherwise complete and
this is the one remaining grant.

## 7. Test end to end (what actually works today)

Not the app's chat panel -- test in the **AI Playground**, or the
Supervisor Agent's own test panel while signed in, either of which runs
under your own identity end to end:

*"Find me papers on retrieval-augmented generation and build me a reading
plan."*

Should call `search_papers`, then `create_collection` + up to 5
`add_to_collection` calls, then `get_reading_path` or `save_reading_plan` --
and the reply should cite real `work_id`s, not paraphrase them from memory.
Writes land in the same Lakebase tables the app reads, under your email as
`x-forwarded-email` -- collections created this way should appear in the
app's own **Collections** sidebar (same account), which is the actual
integration evidence: not that the chat panel calls the agent, but that the
agent's writes and the app's reads are the same data.
