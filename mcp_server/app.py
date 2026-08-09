"""FastMCP server for the AI Research & Learning Copilot.

Structure verified against Databricks' own official MCP server template
(databricks/app-templates, mcp-server-hello-world) rather than assumed from
older examples: `FastMCP.http_app(stateless_http=True)` produces a
FastAPI-compatible ASGI app, which is combined with a second FastAPI app
(for a plain health route) into one `combined_app`, sharing the MCP app's
lifespan. stateless_http=True matters because some MCP clients (Databricks
Assistant included) don't send an `mcp-session-id` header, which a stateful
server would reject with a 400.

Identity middleware (identity.py) is registered the same verified way: a
plain `@combined_app.middleware("http")` function, not a BaseHTTPMiddleware
subclass -- simpler, and what the official template actually does.
"""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastmcp import FastMCP

import lakebase
from identity import set_request_headers
from tools import load_tools

mcp_server = FastMCP(name="research-copilot-mcp")
load_tools(mcp_server)

mcp_app = mcp_server.http_app(stateless_http=True)

app = FastAPI(
    title="Research Copilot MCP",
    description="Read + write tools over the OpenAlex research corpus in Lakebase.",
    version="0.1.0",
    lifespan=mcp_app.lifespan,
)


@app.get("/healthz", include_in_schema=False)
async def healthz():
    bootstrap = lakebase.ensure_research_schema()
    return {"status": "healthy" if bootstrap["ok"] else "degraded", "lakebase": bootstrap}


combined_app = FastAPI(
    title="Research Copilot MCP (combined)",
    routes=[*mcp_app.routes, *app.routes],
    lifespan=mcp_app.lifespan,
)


@combined_app.middleware("http")
async def capture_identity_headers(request: Request, call_next):
    """Captures x-forwarded-email/x-forwarded-user into identity.py's
    ContextVar before any tool call runs, so tools.py's write tools can
    attribute the write without trusting a caller-supplied argument.
    """
    set_request_headers(dict(request.headers))
    return await call_next(request)
