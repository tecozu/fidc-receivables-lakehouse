"""The ingestion quality gate: turn one landing file into accepted and rejected rows.

This is the single set of rules for both engines. The local ingestion (DuckDB) and
the Databricks pipeline both call `process_file`, so this module must stay free of
any warehouse or Spark dependency.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

import pandas as pd

from fidc_lakehouse.config import Fund
from fidc_lakehouse.documents import is_valid_document
from fidc_lakehouse.parsers import get_parser

EVENT_TYPES = ("acquisitions", "settlements")
FILE_PATTERN = re.compile(r"^(?P<event>[a-z]+)_(?P<date>\d{4}-\d{2}-\d{2})\.csv$")


@dataclass
class ProcessedFile:
    rows: int
    accepted: pd.DataFrame  # typed columns plus fund_code and row_number
    rejects: pd.DataFrame  # row_number, reason, payload


def _decimal(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.str.strip().str.replace(",", ".", regex=False), errors="coerce")


def _date(s: pd.Series) -> pd.Series:
    # Kept as datetime64 (NaT-safe comparisons); the warehouse casts to DATE on load.
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


def process_file(content: bytes, fund: Fund, event: str) -> ProcessedFile:
    """Parse one file with the layout its fund declares and split its rows."""
    raw = get_parser(fund.layouts[event])(content)
    accepted, rejects = VALIDATORS[event](raw)
    accepted = accepted.assign(fund_code=fund.code, row_number=accepted.index + 1)
    return ProcessedFile(len(raw), accepted, rejects)
