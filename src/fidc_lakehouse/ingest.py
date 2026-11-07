"""Idempotent ingestion of landing files into the DuckDB raw layer.

Guarantees:
* **Idempotent** - every file is fingerprinted (SHA-256) in `meta.file_ledger`;
  re-running ingestion, or receiving the same file twice, loads nothing new.
* **Atomic per file** - ledger entry, accepted rows and rejected rows are written
  in one transaction, so a crash never leaves a half-loaded file.
* **Corrections supported** - a *different* file with the same name is a new
  version; the dbt staging layer keeps only the latest version of each file.
* **Quality gate** - rows that break a rule go to `meta.rejected_rows` with the
  reason and the original payload instead of failing the whole file.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import duckdb

from fidc_lakehouse.config import Fund
from fidc_lakehouse.validation import EVENT_TYPES, FILE_PATTERN, process_file

DDL = """
create schema if not exists raw;
create schema if not exists meta;
create sequence if not exists meta.file_id_seq start 1;

create table if not exists meta.file_ledger (
    file_id        bigint primary key,
    fund_code      varchar not null,
    event_type     varchar not null,
    file_name      varchar not null,
    file_date      date not null,
    sha256         varchar not null unique,
    row_count      integer not null,
    accepted_count integer not null,
    rejected_count integer not null,
    ingested_at    timestamp not null default current_timestamp
);

create table if not exists meta.rejected_rows (
    file_id    bigint not null,
    row_number integer not null,
    reason     varchar not null,
    payload    json
);

create table if not exists raw.acquisitions (
    fund_code        varchar not null,
    receivable_id    varchar not null,
    contract_id      varchar,
    receivable_type  varchar,
    seller_name      varchar,
    seller_doc       varchar,
    originator_name  varchar,
    originator_doc   varchar,
    debtor_name      varchar,
    debtor_doc       varchar not null,
    issue_date       date,
    due_date         date not null,
    acquisition_date date not null,
    face_value       decimal(18, 2) not null,
    purchase_price   decimal(18, 2) not null,
    file_id          bigint not null,
    row_number       integer not null
);

create table if not exists raw.settlements (
    fund_code       varchar not null,
    receivable_id   varchar not null,
    settlement_date date not null,
    settlement_type varchar not null,
    amount_paid     decimal(18, 2) not null,
    file_id         bigint not null,
    row_number      integer not null
);
"""


@dataclass
class FileResult:
    path: Path
    status: str  # "loaded" | "skipped_duplicate"
    rows: int = 0
    accepted: int = 0
    rejected: int = 0


def init_warehouse(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(DDL)


def discover_files(landing: Path, funds: list[Fund], up_to: date | None = None):
    """Yield (fund, event_type, file_date, path) in chronological order."""
    found = []
    for fund in funds:
        for event in EVENT_TYPES:
            for path in (landing / fund.code / event).glob("*.csv"):
                m = FILE_PATTERN.match(path.name)
                if not m or m["event"] != event:
                    continue
                file_date = date.fromisoformat(m["date"])
                if up_to is None or file_date <= up_to:
                    found.append((file_date, fund.code, event, fund, path))
    for file_date, _, event, fund, path in sorted(found, key=lambda x: x[:3]):
        yield fund, event, file_date, path


def ingest_file(
    con: duckdb.DuckDBPyConnection, fund: Fund, event: str, file_date: date, path: Path
) -> FileResult:
    content = path.read_bytes()
    digest = hashlib.sha256(content).hexdigest()
    if con.execute("select 1 from meta.file_ledger where sha256 = ?", [digest]).fetchone():
        return FileResult(path, "skipped_duplicate")

    processed = process_file(content, fund, event)
    accepted, rejects = processed.accepted, processed.rejects

    con.begin()
    try:
        file_id = con.execute("select nextval('meta.file_id_seq')").fetchone()[0]
        con.execute(
            """insert into meta.file_ledger (file_id, fund_code, event_type, file_name, file_date,
                   sha256, row_count, accepted_count, rejected_count)
               values (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [file_id, fund.code, event, path.name, file_date, digest,
             processed.rows, len(accepted), len(rejects)],
        )  # fmt: skip
        if len(accepted):
            con.register("accepted_df", accepted.assign(file_id=file_id))
            con.execute(f"insert into raw.{event} by name select * from accepted_df")
            con.unregister("accepted_df")
        if len(rejects):
            con.register("rejects_df", rejects.assign(file_id=file_id))
            con.execute("insert into meta.rejected_rows by name select * from rejects_df")
            con.unregister("rejects_df")
        con.commit()
    except Exception:
        con.rollback()
        raise
    return FileResult(path, "loaded", processed.rows, len(accepted), len(rejects))


def ingest(
    db_path: Path, landing: Path, funds: list[Fund], up_to: date | None = None
) -> list[FileResult]:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as con:
        init_warehouse(con)
        return [ingest_file(con, *item) for item in discover_files(landing, funds, up_to)]
