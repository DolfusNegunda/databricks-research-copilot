"""
Lakebase (Databricks-managed Postgres) connection helper.

Same shape as the sibling bootcamp projects' lakebase.py. This is app/'s own
copy, not an import from the repo root -- see mcp_server/lakebase.py's
docstring for why shared modules are duplicated per app folder rather than
imported across the deployment boundary. Keep in sync by hand.
"""

from __future__ import annotations

import base64
import logging
import os
import re
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse

import psycopg2
import psycopg2.pool
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine

log = logging.getLogger("research_copilot.lakebase")

_SQL_DIR = Path(__file__).resolve().parent.parent / "sql"

_SCOPE = os.environ.get("LAKEBASE_SECRET_SCOPE", "database")
_KEY = os.environ.get("LAKEBASE_SECRET_KEY", "lakebase-url")

LAKEBASE_SCHEMA = (os.environ.get("LAKEBASE_SCHEMA") or "research").strip()
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", LAKEBASE_SCHEMA):
    raise ValueError(
        f"LAKEBASE_SCHEMA={LAKEBASE_SCHEMA!r} is not a plain SQL identifier. "
        "Use letters, digits and underscores only."
    )

_POOL_MIN = int(os.environ.get("LAKEBASE_POOL_MIN", "1"))
_POOL_MAX = int(os.environ.get("LAKEBASE_POOL_MAX", "5"))
_CONNECT_TIMEOUT = int(os.environ.get("LAKEBASE_CONNECT_TIMEOUT", "30"))
_POOL_MAX_AGE_SECONDS = int(os.environ.get("LAKEBASE_POOL_MAX_AGE_SECONDS", "1500"))

PGDATABASE = os.environ.get("PGDATABASE", "databricks_postgres")
PGSSLMODE = os.environ.get("PGSSLMODE", "require")


class LakebaseUnavailable(RuntimeError):
    """Lakebase could not be reached or is not configured. Always caught and
    reported, never allowed to crash the process at import time."""


class _SecretResolver:
    def __init__(self, scope: str, key: str):
        self.scope = scope
        self.key = key
        self._value: str | None = None
        self._error: str | None = None
        self._lock = threading.Lock()

    def resolve(self) -> str | None:
        if self._value is not None:
            return self._value
        with self._lock:
            if self._value is not None:
                return self._value
            if self._error is not None:
                return None
            try:
                from databricks.sdk import WorkspaceClient

                secret = WorkspaceClient().secrets.get_secret(scope=self.scope, key=self.key)
                self._value = base64.b64decode(secret.value or "").decode("utf-8").strip()
            except Exception as exc:  # noqa: BLE001
                self._error = f"{type(exc).__name__}: {exc}"
                log.info("No DSN from secret %s/%s (%s)", self.scope, self.key, self._error)
                return None
            return self._value

    @property
    def error(self) -> str | None:
        return self._error


_secret_resolver = _SecretResolver(_SCOPE, _KEY)


def _parse_dsn(dsn: str) -> dict:
    parsed = urlparse(dsn)
    if parsed.scheme not in ("postgres", "postgresql", "postgresql+psycopg2"):
        raise LakebaseUnavailable("The Lakebase connection string must start with postgresql://")
    if not parsed.hostname:
        raise LakebaseUnavailable("The Lakebase connection string has no host.")
    database = (parsed.path or "/").lstrip("/") or PGDATABASE
    return {
        "host": parsed.hostname,
        "port": parsed.port or 5432,
        "user": unquote(parsed.username) if parsed.username else "",
        "password": unquote(parsed.password) if parsed.password else "",
        "database": database,
    }


def _dsn() -> str | None:
    url = (os.environ.get("LAKEBASE_URL") or "").strip()
    if url:
        return url
    return _secret_resolver.resolve()


class _TokenProvider:
    _SKEW_SECONDS = 120

    def __init__(self, instance_name: str):
        self.instance_name = instance_name
        self._token: str | None = None
        self._expires_at: float = 0.0
        self._lock = threading.Lock()

    def token(self) -> str:
        now = time.time()
        if self._token and now < self._expires_at - self._SKEW_SECONDS:
            return self._token
        with self._lock:
            now = time.time()
            if self._token and now < self._expires_at - self._SKEW_SECONDS:
                return self._token
            self._token, self._expires_at = self._mint()
            return self._token

    def _mint(self) -> tuple[str, float]:
        try:
            from databricks.sdk import WorkspaceClient

            credential = WorkspaceClient().database.generate_database_credential(
                request_id=str(uuid.uuid4()), instance_names=[self.instance_name]
            )
        except Exception as exc:  # noqa: BLE001
            raise LakebaseUnavailable(
                f"Could not mint a Lakebase credential for '{self.instance_name}' ({type(exc).__name__})."
            ) from exc

        token = getattr(credential, "token", None)
        if not token:
            raise LakebaseUnavailable("Databricks returned an empty Lakebase credential.")

        expiry = getattr(credential, "expiration_time", None)
        expires_at = time.time() + 3000
        if isinstance(expiry, datetime):
            when = expiry if expiry.tzinfo else expiry.replace(tzinfo=timezone.utc)
            expires_at = when.timestamp()
        return token, expires_at


def _instance_name(host: str) -> str:
    configured = (os.environ.get("LAKEBASE_INSTANCE_NAME") or "").strip()
    if configured:
        return configured
    return host.split(".", 1)[0]


_pool: psycopg2.pool.ThreadedConnectionPool | None = None
_pool_lock = threading.Lock()
_pool_created_at: float = 0.0
_token_provider: _TokenProvider | None = None


def _connect_kwargs() -> dict:
    dsn = _dsn()
    if dsn:
        parts = _parse_dsn(dsn)
        host, port, user, database = parts["host"], parts["port"], parts["user"], parts["database"]
        password = parts["password"] or None
    else:
        host = (os.environ.get("PGHOST") or "").strip()
        if not host:
            raise LakebaseUnavailable(
                "Lakebase is not configured. Set LAKEBASE_URL, or store the "
                f"connection string in the secret {_SCOPE}/{_KEY}, or set PGHOST."
            )
        port = int(os.environ.get("PGPORT", "5432"))
        user = (os.environ.get("PGUSER") or "").strip()
        database = PGDATABASE
        password = None

    if not user:
        raise LakebaseUnavailable("The Lakebase connection has no username.")

    if password is None:
        global _token_provider
        if _token_provider is None or _token_provider.instance_name != _instance_name(host):
            _token_provider = _TokenProvider(_instance_name(host))
        password = _token_provider.token()

    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "dbname": database,
        "sslmode": PGSSLMODE,
        "connect_timeout": _CONNECT_TIMEOUT,
        "cursor_factory": RealDictCursor,
        "application_name": "databricks-research-copilot-app",
    }


def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    global _pool, _pool_created_at
    with _pool_lock:
        age = time.time() - _pool_created_at
        if _pool is not None and age < _POOL_MAX_AGE_SECONDS:
            return _pool
        if _pool is not None:
            try:
                _pool.closeall()
            except Exception:  # noqa: BLE001
                pass
        kwargs = _connect_kwargs()
        _pool = psycopg2.pool.ThreadedConnectionPool(_POOL_MIN, _POOL_MAX, **kwargs)
        _pool_created_at = time.time()
        return _pool


def dispose_pool() -> None:
    global _pool
    with _pool_lock:
        if _pool is not None:
            try:
                _pool.closeall()
            except Exception:  # noqa: BLE001
                pass
        _pool = None


@contextmanager
def get_connection():
    """Yield a pooled psycopg2 connection with a RealDictCursor factory,
    already pointed at LAKEBASE_SCHEMA via SET search_path (never libpq
    `options`, which Lakebase's proxy silently drops)."""
    pool = _get_pool()
    conn = pool.getconn()
    broken = False
    try:
        with conn.cursor() as cur:
            cur.execute(f'SET search_path TO "{LAKEBASE_SCHEMA}", public')
        yield conn
    except Exception:
        broken = True
        raise
    finally:
        try:
            if broken:
                conn.close()
            pool.putconn(conn, close=broken)
        except Exception:  # noqa: BLE001
            pass


def _ensure_schema_exists(conn) -> None:  # noqa: ANN001
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s", (LAKEBASE_SCHEMA,))
        if cur.fetchone() is None:
            cur.execute(f'CREATE SCHEMA "{LAKEBASE_SCHEMA}"')


def get_engine():
    dsn = _dsn()
    if not dsn:
        raise LakebaseUnavailable("Lakebase is not configured.")
    return create_engine(dsn)


def run_query(sql: str, params: tuple | dict | None = None) -> list[dict]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]


def run_write(sql: str, params: tuple | dict | None = None) -> int:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            conn.commit()
            return cur.rowcount


def run_write_returning(sql: str, params: tuple | dict | None = None) -> list[dict]:
    """INSERT/UPDATE ... RETURNING, committed before the connection returns to
    the pool. run_query() never commits, so a RETURNING write run through it
    gets rolled back by the pool's putconn() -- use this instead."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = [dict(row) for row in cur.fetchall()]
            conn.commit()
            return rows


def _render_schema_sql(embedding_dim: int) -> str:
    text = (_SQL_DIR / "01_schema.sql").read_text(encoding="utf-8")
    return text.replace("{{EMBEDDING_DIM}}", str(int(embedding_dim)))


def ensure_research_schema(embedding_dim: int = 384) -> dict:
    result = {"ok": False, "error": None}
    try:
        with get_connection() as conn:
            _ensure_schema_exists(conn)
            with conn.cursor() as cur:
                cur.execute(_render_schema_sql(embedding_dim))
            conn.commit()
        result["ok"] = True
    except Exception as exc:  # noqa: BLE001
        log.exception("Research schema bootstrap failed")
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


def target_summary() -> dict:
    dsn = _dsn()
    summary = {"host": None, "database": None, "schema": LAKEBASE_SCHEMA, "user": None, "auth": "unconfigured"}
    if dsn:
        try:
            parts = _parse_dsn(dsn)
        except LakebaseUnavailable as exc:
            summary["auth"] = f"invalid connection string: {exc}"
            return summary
        summary["host"] = parts["host"]
        summary["database"] = parts["database"]
        summary["user"] = parts["user"] or None
        summary["auth"] = "connection string password" if parts["password"] else "oauth token"
    elif os.environ.get("PGHOST"):
        summary["host"] = os.environ.get("PGHOST")
        summary["database"] = PGDATABASE
        summary["user"] = os.environ.get("PGUSER") or None
        summary["auth"] = "oauth token"
    return summary
