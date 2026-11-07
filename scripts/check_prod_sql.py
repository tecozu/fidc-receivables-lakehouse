"""Check that every model and test compiled for production is valid Databricks SQL.

CI compiles the dbt project with the `prod` target, without connecting to a workspace,
and then parses each compiled file with sqlglot's Databricks dialect. This catches
DuckDB-only syntax leaking into the shared models before it reaches Databricks. It
proves the syntax only; running on a real warehouse is what proves the behaviour.
"""

from __future__ import annotations

import sys
from pathlib import Path

import sqlglot
from sqlglot.errors import ParseError

COMPILED = Path(__file__).resolve().parents[1] / "dbt" / "target" / "compiled"


def main() -> int:
    files = sorted(COMPILED.rglob("*.sql"))
    if not files:
        print(f"No compiled SQL under {COMPILED}; run `dbt compile` with the prod target first.")
        return 1
    failures = 0
    for path in files:
        try:
            sqlglot.parse_one(path.read_text(encoding="utf-8"), read="databricks")
        except ParseError as exc:
            failures += 1
            print(f"FAIL {path.relative_to(COMPILED)}: {str(exc).splitlines()[0]}")
    print(f"{len(files)} compiled files checked against the Databricks dialect, {failures} failed.")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
