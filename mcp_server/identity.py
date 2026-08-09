"""End-user identity capture for the MCP server.

Pattern verified against Databricks' own official MCP server template
(databricks/app-templates, mcp-server-hello-world) rather than assumed:
a ContextVar populated by a `@app.middleware("http")` function, read back out
inside tool implementations. That template mints a user-scoped WorkspaceClient
from `x-forwarded-access-token` (for calling other Databricks APIs on the
user's behalf); this project only needs a stable identifier to attribute
writes to, so it reads the simpler `x-forwarded-email` header Databricks Apps
also injects, and skips the token exchange.

Every write tool in tools.py calls current_user_email() -- never accepts a
user identifier as a caller-supplied argument. An agent could otherwise pass
any email it wanted and write data as a different user; sourcing identity
only from the verified proxy header is what prevents that.
"""

from __future__ import annotations

import contextvars
import os

_header_store: contextvars.ContextVar[dict] = contextvars.ContextVar("header_store", default={})

# Local dev fallback only -- set explicitly, never guessed, so a missing
# header in a real deployment fails loudly instead of silently attributing
# writes to a placeholder user.
_DEV_FALLBACK_EMAIL = os.environ.get("MCP_DEV_USER_EMAIL")


def set_request_headers(headers: dict) -> None:
    _header_store.set(headers)


def current_user_email() -> str:
    headers = _header_store.get()
    email = headers.get("x-forwarded-email") or headers.get("x-forwarded-user")
    if email:
        return email
    if _DEV_FALLBACK_EMAIL:
        return _DEV_FALLBACK_EMAIL
    raise PermissionError(
        "No end-user identity on this request (x-forwarded-email/x-forwarded-user "
        "missing). Set MCP_DEV_USER_EMAIL for local development."
    )
