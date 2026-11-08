"""The ingestion quality gate: turn one landing file into accepted and rejected rows.

This is the single set of rules for both engines. The local ingestion (DuckDB) and
the Databricks pipeline both call `process_file`, so this module must stay free of
any warehouse or Spark dependency. The rules and their order are documented in
docs/data-contract.md.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass

import pandas as pd

from fidc_lakehouse.config import Fund
from fidc_lakehouse.documents import is_valid_document
from fidc_lakehouse.parsers import LAYOUTS_WITH_TRANSACTION_ID, get_parser

EVENT_TYPES = ("acquisitions", "settlements")
FILE_PATTERN = re.compile(r"^(?P<event>[a-z]+)_(?P<date>\d{4}-\d{2}-\d{2})\.csv$")
TRANSACTION_ID_SEPARATOR = "|"


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


def _conflicting_ids(rows: pd.DataFrame, key: str) -> pd.Series:
    """Rows whose `key` appears in the file with different contents.

    No row can be trusted over the other, so all of them are rejected. This rule
    doesn't depend on row order, so engines without line numbers apply it identically.
    """
    distinct = rows.drop_duplicates()
    ids = distinct[key][distinct[key].duplicated(keep=False)]
    return rows[key].isin(ids)


def derive_transaction_id(raw: pd.DataFrame, fund_code: str) -> pd.Series:
    """Build a settlement ID for layouts whose files don't carry one.

    SHA-256 of the fields that define a payment, joined with a separator so that
    ("1", "23,00") and ("12", "3,00") can't produce the same text. The same payment gets
    the same ID in any file and position, so a corrected file keeps the IDs of unchanged
    rows. Field order and the two-decimal amount are part of the contract: the Spark
    version must produce identical IDs.
    """
    amount = _decimal(raw["amount_paid"]).map(lambda x: "" if pd.isna(x) else f"{x:.2f}")
    joined = pd.Series(fund_code, index=raw.index, dtype="object").str.cat(
        [
            raw["installment_id"].str.strip(),
            raw["settlement_date"].str.strip(),
            raw["settlement_type"].str.strip(),
            amount,
        ],
        sep=TRANSACTION_ID_SEPARATOR,
    )
    return joined.map(lambda text: hashlib.sha256(text.encode("utf-8")).hexdigest())


def validate_acquisitions(raw: pd.DataFrame, fund: Fund) -> tuple[pd.DataFrame, pd.DataFrame]:
    t = raw.copy()
    for col in ("issue_date", "due_date", "acquisition_date"):
        t[col] = _date(raw[col])
    for col in ("face_value", "purchase_price"):
        t[col] = _decimal(raw[col])
    number = pd.to_numeric(raw["installment_number"].str.strip(), errors="coerce")
    t["installment_number"] = number
    rules = {
        "missing_installment_id": raw["installment_id"].str.strip() == "",
        "invalid_installment_number": ~((number >= 1) & (number % 1 == 0)),
        "invalid_debtor_doc": ~raw["debtor_doc"].map(is_valid_document),
        "invalid_seller_doc": ~raw["seller_doc"].map(is_valid_document),
        "invalid_date": t[["due_date", "acquisition_date"]].isna().any(axis=1),
        "non_positive_face_value": ~(t["face_value"] > 0),
        "purchase_price_out_of_range": ~((t["purchase_price"] > 0)
                                         & (t["purchase_price"] <= t["face_value"])),
        "due_before_acquisition": ~(t["due_date"] > t["acquisition_date"]),
        "conflicting_duplicate_in_file": _conflicting_ids(raw, "installment_id"),
        # Exact copies are interchangeable: keep one, reject the rest.
        "duplicate_in_file": raw.duplicated(keep="first"),
    }  # fmt: skip
    accepted, rejects = _split(raw, t, rules)
    # Every accepted row passed `invalid_installment_number`, so the cast is safe.
    accepted["installment_number"] = accepted["installment_number"].astype("int64")
    return accepted, rejects


def validate_settlements(raw: pd.DataFrame, fund: Fund) -> tuple[pd.DataFrame, pd.DataFrame]:
    provider_id = fund.layouts["settlements"] in LAYOUTS_WITH_TRANSACTION_ID
    keyed = raw.copy()
    if provider_id:
        if "transaction_id" not in raw.columns:
            raise ValueError(
                f"Layout '{fund.layouts['settlements']}' declares a transaction_id column, "
                f"but the file has: {list(raw.columns)}"
            )
    else:
        keyed["transaction_id"] = derive_transaction_id(raw, fund.code)

    t = keyed.copy()
    t["settlement_date"] = _date(raw["settlement_date"])
    t["amount_paid"] = _decimal(raw["amount_paid"])
    rules = {
        "missing_installment_id": raw["installment_id"].str.strip() == "",
        # A derived ID is never empty, so this rule only exists for provider layouts.
        **(
            {"missing_transaction_id": keyed["transaction_id"].str.strip() == ""}
            if provider_id
            else {}
        ),
        "invalid_date": t["settlement_date"].isna(),
        "unknown_settlement_type": ~raw["settlement_type"].isin(
            ["FULL", "PARTIAL", "CANCELLATION"]
        ),
        "negative_amount": ~(t["amount_paid"] >= 0),
        "conflicting_duplicate_in_file": _conflicting_ids(keyed, "transaction_id"),
        "duplicate_in_file": raw.duplicated(keep="first"),
    }
    return _split(raw, t, rules)


VALIDATORS = {"acquisitions": validate_acquisitions, "settlements": validate_settlements}


def process_file(content: bytes, fund: Fund, event: str) -> ProcessedFile:
    """Parse one file with the layout its fund declares and split its rows."""
    raw = get_parser(fund.layouts[event])(content)
    accepted, rejects = VALIDATORS[event](raw, fund)
    accepted = accepted.assign(fund_code=fund.code, row_number=accepted.index + 1)
    return ProcessedFile(len(raw), accepted, rejects)
