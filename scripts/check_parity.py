"""Check that DuckDB and Databricks produce the same daily fund summary.

Run the same files through both engines first, then compare:

    FIDC_LANDING_DIR=<files> FIDC_DUCKDB_PATH=<file.duckdb> fidc backfill --start ... --end ...
    # ... and the fidc_backfill job on Databricks for the same range ...
    python scripts/check_parity.py --duckdb <file.duckdb>

It reads `rpt_fund_daily_summary` on both sides, compares row by row with the rules in
`fidc_lakehouse.parity`, prints every difference, and exits with 1 if there is any.
Databricks is reached through the Databricks CLI profile, so no token is stored here.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import duckdb

from fidc_lakehouse.parity import COLUMNS, KEY, compare

SELECT = f"select {', '.join(KEY + COLUMNS)} from {{table}} order by 1, 2"


def local_rows(db_path: Path, schema: str) -> dict[tuple, dict]:
    with duckdb.connect(str(db_path), read_only=True) as con:
        cursor = con.execute(SELECT.format(table=f"{schema}.rpt_fund_daily_summary"))
        names = [d[0] for d in cursor.description]
        return _by_key(names, cursor.fetchall())


def databricks_rows(profile: str, warehouse_id: str, table: str) -> dict[tuple, dict]:
    payload = {
        "warehouse_id": warehouse_id,
        "wait_timeout": "50s",
        "statement": SELECT.format(table=table),
    }
    run = subprocess.run(
        [
            "databricks",
            "api",
            "post",
            "/api/2.0/sql/statements",
            "-p",
            profile,
            "--json",
            json.dumps(payload),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    # The CLI may print a notice before the JSON, so start at the first brace.
    answer = json.loads(run.stdout[run.stdout.index("{") :])
    if answer["status"]["state"] != "SUCCEEDED":
        raise SystemExit(f"Databricks query did not succeed: {answer['status']}")
    names = [c["name"] for c in answer["manifest"]["schema"]["columns"]]
    return _by_key(names, answer["result"]["data_array"])


def _by_key(names: list[str], rows) -> dict[tuple, dict]:
    out = {}
    for row in rows:
        record = dict(zip(names, row, strict=True))
        # Databricks returns every value as text, so keys are compared as text too.
        out[(str(record["as_of_date"]), record["fund_code"])] = record
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--duckdb", type=Path, default=Path("data/warehouse.duckdb"))
    parser.add_argument("--local-schema", default="marts")
    parser.add_argument("--profile", default="fidc", help="Databricks CLI profile")
    parser.add_argument("--warehouse-id", default="7bca809443b4daf4")
    parser.add_argument("--table", default="workspace.fidc_marts.rpt_fund_daily_summary")
    args = parser.parse_args()

    local = local_rows(args.duckdb, args.local_schema)
    remote = databricks_rows(args.profile, args.warehouse_id, args.table)
    differences = compare(local, remote)

    days = len({key[0] for key in local | remote})
    if differences:
        print(f"PARITY FAILED: {len(differences)} difference(s)")
        for difference in differences:
            print(f"  {difference}")
        return 1
    print(f"Parity OK: {len(local)} rows ({days} days) match on DuckDB and Databricks")
    return 0


if __name__ == "__main__":
    sys.exit(main())
