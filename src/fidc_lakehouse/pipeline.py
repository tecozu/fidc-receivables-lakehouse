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
from fidc_lakehouse.ingest import IngestSummary, ingest


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


def summarize(result: IngestSummary) -> str:
    return (
        f"{result.files} files loaded ({result.rows} rows, {result.rejected} rejected), "
        f"{result.identical_skipped} identical re-sends skipped"
    )


def run_day(as_of: date, full_build: bool = True, ingest_first: bool = True) -> None:
    db = config.warehouse_path()
    if ingest_first:
        result = ingest(db, config.landing_dir(), config.load_funds(), up_to=as_of)
        print(f"[{as_of}] ingestion: {summarize(result)}")
    if full_build:
        run_dbt(["build"], as_of, db)
    else:
        # Rebuild the whole lineage of the snapshot (ledger, dimensions) so it sees the
        # files ingested today; tests run once at the end of the backfill.
        run_dbt(["run", "--select", "+fct_daily_portfolio"], as_of, db)


def backfill(start: date, end: date) -> None:
    """Rebuild every business day in [start, end], then run the full test suite.

    All files up to `end` are ingested once, up front. Every date then sees the latest
    version of each file, which is what a reprocessing after a correction should do.
    """
    days = BusinessCalendar.from_csv().business_days(start, end)
    if not days:
        raise ValueError("No business days in the given range")
    db = config.warehouse_path()
    result = ingest(db, config.landing_dir(), config.load_funds(), up_to=days[-1])
    print(f"[{days[0]}..{days[-1]}] ingestion: {summarize(result)}")
    run_dbt(["seed"], days[0], db)
    for i, d in enumerate(days):
        run_day(d, full_build=(i == 0), ingest_first=False)
    run_dbt(["build"], days[-1], db)
