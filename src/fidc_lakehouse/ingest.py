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
import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd

from fidc_lakehouse.config import Fund
from fidc_lakehouse.documents import is_valid_document
from fidc_lakehouse.parsers import get_parser

EVENT_TYPES = ("acquisitions", "settlements")
FILE_PATTERN = re.compile(r"^(?P<event>[a-z]+)_(?P<date>\d{4}-\d{2}-\d{2})\.csv$")

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


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


# --------------------------------------------------------------------------- validation
def _decimal(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.str.strip().str.replace(",", ".", regex=False), errors="coerce")


def _date(s: pd.Series) -> pd.Series:
    # Kept as datetime64 (NaT-safe comparisons); DuckDB casts to DATE on insert.
    return pd.to_datetime(s.str.strip(), format="%d/%m/%Y", errors="coerce")


def _split(
    raw: pd.DataFrame, typed: pd.DataFrame, rules: dict[str, pd.Series]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply `rules` (reason -> boolean mask of *invalid* rows). First failing rule wins."""
    reason = pd.Series(pd.NA, index=raw.index, dtype="object")
    for name, invalid in rules.items():
        reason = reason.where(reason.notna() | ~invalid.fillna(True), name)
    bad = reason.notna().to_numpy()
    rejects = pd.DataFrame(
        {
            "row_number": raw.index[bad] + 1,
            "reason": reason[bad].to_numpy(),
            "payload": [json.dumps(r, ensure_ascii=False) for r in raw[bad].to_dict("records")],
        }
    )
    return typed[~bad].copy(), rejects


def validate_acquisitions(raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    t = raw.copy()
    for col in ("issue_date", "due_date", "acquisition_date"):
        t[col] = _date(raw[col])
    for col in ("face_value", "purchase_price"):
        t[col] = _decimal(raw[col])
    rules = {
        "missing_receivable_id": raw["receivable_id"].str.strip() == "",
        "invalid_debtor_doc": ~raw["debtor_doc"].map(is_valid_document),
        "invalid_seller_doc": ~raw["seller_doc"].map(is_valid_document),
        "invalid_date": t[["due_date", "acquisition_date"]].isna().any(axis=1),
        "non_positive_face_value": ~(t["face_value"] > 0),
        "purchase_price_out_of_range": ~((t["purchase_price"] > 0)
                                         & (t["purchase_price"] <= t["face_value"])),
        "due_before_acquisition": ~(t["due_date"] > t["acquisition_date"]),
        "duplicate_in_file": raw["receivable_id"].duplicated(keep="first"),
    }  # fmt: skip
    return _split(raw, t, rules)


def validate_settlements(raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    t = raw.copy()
    t["settlement_date"] = _date(raw["settlement_date"])
    t["amount_paid"] = _decimal(raw["amount_paid"])
    rules = {
        "missing_receivable_id": raw["receivable_id"].str.strip() == "",
        "invalid_date": t["settlement_date"].isna(),
        "unknown_settlement_type": ~raw["settlement_type"].isin(
            ["FULL", "PARTIAL", "CANCELLATION"]
        ),
        "negative_amount": ~(t["amount_paid"] >= 0),
    }
    return _split(raw, t, rules)


VALIDATORS = {"acquisitions": validate_acquisitions, "settlements": validate_settlements}


# --------------------------------------------------------------------------- loading
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
    digest = sha256_of(path)
    if con.execute("select 1 from meta.file_ledger where sha256 = ?", [digest]).fetchone():
        return FileResult(path, "skipped_duplicate")

    raw = get_parser(fund.layouts[event])(path)
    accepted, rejects = VALIDATORS[event](raw)

    con.begin()
    try:
        file_id = con.execute("select nextval('meta.file_id_seq')").fetchone()[0]
        con.execute(
            """insert into meta.file_ledger (file_id, fund_code, event_type, file_name, file_date,
                   sha256, row_count, accepted_count, rejected_count)
               values (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [file_id, fund.code, event, path.name, file_date, digest,
             len(raw), len(accepted), len(rejects)],
        )  # fmt: skip
        if len(accepted):
            accepted = accepted.assign(
                fund_code=fund.code, file_id=file_id, row_number=accepted.index + 1
            )
            con.register("accepted_df", accepted)
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
    return FileResult(path, "loaded", len(raw), len(accepted), len(rejects))


def ingest(
    db_path: Path, landing: Path, funds: list[Fund], up_to: date | None = None
) -> list[FileResult]:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as con:
        init_warehouse(con)
        return [ingest_file(con, *item) for item in discover_files(landing, funds, up_to)]
