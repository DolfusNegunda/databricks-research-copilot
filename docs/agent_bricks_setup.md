# Registering the agent

This is a manual, workspace-UI step done after `app/` and `mcp_server/` are
both deployed — it can't be scripted from this repo. Menu labels below match
Databricks' documented flow as of this project's build; if a label has
moved, search the workspace UI or current docs for the same concept rather
than assuming the step no longer applies.

## 1. Confirm the MCP server is reachable

Deploy `mcp_server/` as its own Databricks App (Git folder → Pull → Deploy,
same as every app in this workspace). Confirm it's up:

```
curl https://<mcp-server-app-url>/healthz
```

Should return `{"status": "healthy", ...}`. If `"degraded"`, Lakebase isn't
configured yet on that app — fix that before registering it with anything.

## 2. Register the MCP server with AI Gateway

In the workspace: **AI Gateway → MCP servers** (or wherever the workspace
currently surfaces external MCP registration) → add a new server, pointing
at the deployed `mcp_server/` app's URL. This is what makes the 14 tools in
`mcp_server/tools.py` discoverable to an agent.

## 3. Create the Agent Bricks agent

**Agents (Agent Bricks)** → create a new agent. Attach the MCP server
registered in step 2 as a tool source.

**System prompt** — the load-bearing instruction is: the agent must call
`get_reading_path` (or `save_reading_plan`) to get an ordering, never invent
one itself. The whole point of `reading_path.py`'s topological sort is that
ordering is computed from real citation edges, not asked of the model to
guess — a system prompt that lets the agent skip the tool call defeats that.
Suggested prompt, adapt as needed:

> You are a research assistant over a corpus of academic papers. When asked
> for a reading plan or "what should I read first," always call
> `get_reading_path` or `save_reading_plan` for the relevant collection —
> never invent an ordering yourself. Cite `work_id`s from tool results when
> referencing specific papers. Before writing (creating a collection, adding
> a paper, saving a note, recording progress), confirm with the user what
> you're about to do if it isn't exactly what they asked for.

## 4. Wire the app's chat panel to it

Once the agent has a serving endpoint name, set it on `app/`'s deployment:

```yaml
# app/app.yaml
env:
  - name: AGENT_SERVING_ENDPOINT
    value: "<the agent's serving endpoint name>"
```

Redeploy `app/`. The chat panel (`app/static/js/app.js`, the "Agent" tab)
posts to `/api/agent/chat`, which proxies to that endpoint via
`WorkspaceClient().serving_endpoints.query(...)` — see `app/app.py`. Until
this is set, that route returns a clear `agent_not_configured` response
rather than a 500, so the rest of the app works fine without it.

## 5. Test end to end

Ask the agent something like *"Find me papers on retrieval-augmented
generation and build me a reading plan."* It should call `search_papers`,
then `create_collection` + `add_to_collection` for each result, then
`save_reading_plan` — and the reply should cite real `work_id`s, not
paraphrase them from memory.
