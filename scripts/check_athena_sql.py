"""Check that every model and test compiled for the Athena target is valid Trino SQL.

CI compiles the dbt project with `--target prod` (against a local moto server, so no
AWS account is needed) and then parses each compiled file with sqlglot's Trino
dialect. This catches DuckDB-only syntax leaking into the shared models before it
ever reaches Athena.
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
        print(f"No compiled SQL under {COMPILED}; run `dbt compile --target prod` first.")
        return 1
    failures = 0
    for path in files:
        try:
            sqlglot.parse_one(path.read_text(encoding="utf-8"), read="trino")
        except ParseError as exc:
            failures += 1
            print(f"FAIL {path.relative_to(COMPILED)}: {str(exc).splitlines()[0]}")
    print(f"{len(files)} compiled files checked against the Trino dialect, {failures} failed.")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
