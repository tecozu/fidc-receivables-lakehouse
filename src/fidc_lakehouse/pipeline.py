"""Orchestration: ingestion + dbt, for one date or a range of dates (backfill).

The daily valuation (`fct_daily_portfolio`) is an incremental model keyed by
`as_of_date` with a delete+insert strategy: running a date again *replaces* that
date's snapshot, so reprocessing after a corrected file is just `fidc run --as-of`.
"""

from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path

from fidc_lakehouse import config
from fidc_lakehouse.business_calendar import BusinessCalendar
from fidc_lakehouse.ingest import FileResult, ingest


class DbtError(RuntimeError):
    pass


def run_dbt(args: list[str], as_of: date, db_path: Path) -> None:
    from dbt.cli.main import dbtRunner  # imported lazily: heavy import

    os.environ["FIDC_DUCKDB_PATH"] = str(db_path.resolve())
    cli_args = [
        *args,
        "--project-dir", str(config.DBT_DIR),
        "--profiles-dir", str(config.DBT_DIR),
        "--vars", json.dumps({"as_of_date": as_of.isoformat()}),
    ]  # fmt: skip
    result = dbtRunner().invoke(cli_args)
    if not result.success:
        raise DbtError(f"dbt {' '.join(args)} failed for {as_of}")


def summarize(results: list[FileResult]) -> str:
    loaded = [r for r in results if r.status == "loaded"]
    skipped = len(results) - len(loaded)
    rows = sum(r.rows for r in loaded)
    rejected = sum(r.rejected for r in loaded)
    return f"{len(loaded)} files loaded ({rows} rows, {rejected} rejected), {skipped} skipped"


def run_day(as_of: date, full_build: bool = True) -> None:
    db = config.warehouse_path()
    results = ingest(db, config.landing_dir(), config.load_funds(), up_to=as_of)
    print(f"[{as_of}] ingestion: {summarize(results)}")
    if full_build:
        run_dbt(["build"], as_of, db)
    else:
        # Rebuild the whole lineage of the snapshot (ledger, dimensions) so it sees the
        # files ingested today; tests run once at the end of the backfill.
        run_dbt(["run", "--select", "+fct_daily_portfolio"], as_of, db)


def backfill(start: date, end: date) -> None:
    """Rebuild every business day in [start, end], then run the full test suite."""
    days = BusinessCalendar.from_csv().business_days(start, end)
    if not days:
        raise ValueError("No business days in the given range")
    run_dbt(["seed"], days[0], config.warehouse_path())
    for i, d in enumerate(days):
        run_day(d, full_build=(i == 0))
    run_dbt(["build"], days[-1], config.warehouse_path())
