"""Entrypoint: `python main.py`. Databricks Apps sets DATABRICKS_APP_PORT;
falls back to 8000 (Databricks Apps' own default) for local runs.
"""

from __future__ import annotations

import os

import uvicorn

if __name__ == "__main__":
    port = int(os.environ.get("DATABRICKS_APP_PORT", os.environ.get("PORT", "8000")))
    uvicorn.run("app:combined_app", host="0.0.0.0", port=port)
