"""Offline grammar check of every SQL file in sql/, via pglast (libpg_query --
Postgres's own parser). A pass means real Postgres accepts the grammar; it
cannot check semantics, which is what scripts/check_connection.py is for.

    python scripts/check_sql.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

from pglast import parse_sql
from pglast.parser import parse_plpgsql_json

SCHEMA = "research"
_REPO_ROOT = Path(__file__).resolve().parent.parent

# app/sql/ and mcp_server/sql/ are copies of the root sql/, not imports of it
# -- Databricks Apps deployment only uploads files from within an app's own
# folder, never sibling directories, so each app's lakebase.py needs its own
# copy to find (confirmed live: the root-relative path 404'd on every app
# deploy until this was fixed). Validate all three, not just the root one,
# so a schema change that only gets applied to the root copy still fails
# offline instead of silently drifting.
SQL_DIRS = [_REPO_ROOT / "sql", _REPO_ROOT / "app" / "sql", _REPO_ROOT / "mcp_server" / "sql"]

passed = 0
failed: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    global passed
    if condition:
        passed += 1
        print(f"PASS {name}")
    else:
        failed.append(name)
        print(f"FAIL {name} :: {detail}")


sql_files = sorted(
    (path, path.relative_to(_REPO_ROOT)) for sql_dir in SQL_DIRS for path in sql_dir.glob("*.sql")
)
if not sql_files:
    print(f"FAIL no .sql files found under {SQL_DIRS}")
    sys.exit(1)

for sql_path, sql_label in sql_files:
    raw = sql_path.read_text(encoding="utf-8")
    sql = (
        raw.replace("__SCHEMA_NAME__", SCHEMA)
        .replace("__SCHEMA__", f'"{SCHEMA}"')
        .replace("{{EMBEDDING_DIM}}", "384")
    )

    try:
        statements = parse_sql(sql)
        check(f"{sql_label}: whole file parses ({len(statements)} statements)", True)
    except Exception as exc:
        check(f"{sql_label}: whole file parses", False, str(exc))
        continue

    # pglast 8.x caveat hit in every prior project: parse_plpgsql() itself is
    # broken for RETURNS TRIGGER via a JSON-decode bug; parse_plpgsql_json
    # (the layer before the broken decode) is what actually validates it.
    for trigger in re.finditer(
        r"CREATE (?:OR REPLACE )?FUNCTION.*?\$(\w*)\$ LANGUAGE plpgsql;", sql, re.DOTALL
    ):
        try:
            parse_plpgsql_json(trigger.group(0))
            check(f"{sql_label}: plpgsql function body parses ($${trigger.group(1)}$$)", True)
        except Exception as exc:
            check(f"{sql_label}: plpgsql function body parses ($${trigger.group(1)}$$)", False, str(exc))

    for do_block in re.finditer(r"DO \$(\w+)\$(.*?)\$\1\$;", sql, re.DOTALL):
        label, body = do_block.group(1), do_block.group(2)
        wrapped = f"CREATE FUNCTION __check() RETURNS void LANGUAGE plpgsql AS $$ {body} $$;"
        try:
            parse_plpgsql_json(wrapped)
            check(f"{sql_label}: DO ${label}$ block parses", True)
        except Exception as exc:
            check(f"{sql_label}: DO ${label}$ block parses", False, str(exc))

print(f"\n{passed} passed, {len(failed)} failed")
if failed:
    for name in failed:
        print(f"  FAILED: {name}")
    sys.exit(1)
print("ALL CHECKS PASSED")
