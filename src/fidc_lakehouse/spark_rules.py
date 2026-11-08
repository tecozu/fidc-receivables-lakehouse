"""The ingestion quality gate as native Spark expressions.

`validation.py` is the reference: it applies the rules with pandas, one file at a time.
This module applies the same rules inside Spark, so the Databricks pipeline can validate
many files in parallel without moving rows through Python. A parity test keeps the two in
sync (tests/test_spark_rules.py). The rules and their order are in docs/data-contract.md.

Everything here builds `Column` expressions. Nothing runs until Spark evaluates them, and
no Python UDF is involved, so Spark can optimize the whole plan.

Three Spark behaviours shape the code:

* **NULL means "unknown", not "false".** `NULL > 0` is NULL, and `NOT NULL` is still NULL.
  A rule written as "invalid when NOT (x > 0)" would let a missing `x` through. Each rule
  is wrapped so that unknown counts as invalid, like pandas' `fillna(True)`.
* **Parsing never raises.** Spark 4 runs in ANSI mode, where a plain `cast` of bad text
  fails the whole job. The `try_*` functions return NULL instead, which the rules then
  reject row by row.
* **`concat_ws` skips NULL arguments.** That would silently drop a separator and change a
  hash, so every part is coalesced to an empty string first.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

# Wider than the warehouse's decimal(18, 2) on purpose: rounding happens when a value is
# stored, not when a rule compares it, so `price <= face` sees what pandas sees.
PARSE_DECIMAL = "decimal(38, 10)"
STORED_DECIMAL = "decimal(18, 2)"
TRANSACTION_ID_SEPARATOR = "|"
ROW_SEPARATOR = "\x1f"  # ASCII "unit separator": can't appear in a CSV field by accident


def _strip(column: Column) -> Column:
    """Remove surrounding whitespace the way Python's `str.strip` does.

    `F.trim` only removes spaces, so a tab, a line break or a non-breaking space would
    survive and the two engines would read the same text differently.
    """
    return F.regexp_replace(column, r"(?U)^\s+|\s+$", "")


def _text(name: str) -> Column:
    """A column as stripped text, with NULL turned into an empty string."""
    return F.coalesce(_strip(F.col(name)), F.lit(""))


# --- parsers ----------------------------------------------------------------------------


def br_date(name: str) -> Column:
    """`dd/mm/yyyy` text to a date, NULL when it can't be read.

    `d/M/yyyy` accepts unpadded days and months ("5/1/2026"), like pandas does.
    """
    return F.try_to_date(_strip(F.col(name)), "d/M/yyyy")


def br_decimal(name: str) -> Column:
    """Brazilian decimal text ("1000,50") to a number, NULL when it can't be read."""
    return F.regexp_replace(_strip(F.col(name)), ",", ".").try_cast(PARSE_DECIMAL)


def _digit(digits: Column, position: int) -> Column:
    """The digit at 1-based `position`, or NULL if the text is shorter."""
    return F.substring(digits, position, 1).try_cast("int")


def _check_digit(digits: Column, weights: list[int]) -> Column:
    """Mod-11 check digit over the first `len(weights)` digits."""
    total = sum((_digit(digits, i + 1) * w for i, w in enumerate(weights)), F.lit(0))
    remainder = total % 11
    return F.when(remainder < 2, F.lit(0)).otherwise(11 - remainder)


_CPF_WEIGHTS_1 = list(range(10, 1, -1))
_CPF_WEIGHTS_2 = list(range(11, 1, -1))
_CNPJ_WEIGHTS_1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
_CNPJ_WEIGHTS_2 = [6, *_CNPJ_WEIGHTS_1]


def document_is_valid(name: str) -> Column:
    """True for a CPF (11 digits) or CNPJ (14 digits) with correct check digits.

    The Spark twin of `documents.is_valid_document`. A missing or malformed value is
    False, never NULL.
    """
    digits = F.regexp_replace(F.coalesce(F.col(name), F.lit("")), r"[^0-9]", "")
    all_same_digit = digits.rlike(r"^(\d)\1+$")
    cpf = (_digit(digits, 10) == _check_digit(digits, _CPF_WEIGHTS_1)) & (
        _digit(digits, 11) == _check_digit(digits, _CPF_WEIGHTS_2)
    )
    cnpj = (_digit(digits, 13) == _check_digit(digits, _CNPJ_WEIGHTS_1)) & (
        _digit(digits, 14) == _check_digit(digits, _CNPJ_WEIGHTS_2)
    )
    verdict = (
        F.when(all_same_digit, F.lit(False))
        .when(F.length(digits) == 11, cpf)
        .when(F.length(digits) == 14, cnpj)
        .otherwise(F.lit(False))
    )
    # A NULL here would mean "unknown"; for a validity check unknown must mean "not valid".
    return F.coalesce(verdict, F.lit(False))


# --- building blocks for the rules ------------------------------------------------------


def first_failure(*rules: tuple[str, Column]) -> Column:
    """The reason of the first rule that fires, or NULL when the row passes every rule.

    `when(cond, reason)` is NULL unless `cond` is true, and `coalesce` returns its first
    non-NULL argument, so the rules are tried in the order given.
    """
    return F.coalesce(*[F.when(invalid, F.lit(name)) for name, invalid in rules])


def _unless(valid: Column) -> Column:
    """Turn a "this is valid" condition into a rule: invalid unless provably valid."""
    return F.coalesce(~valid, F.lit(True))


def _payload(columns: list[str]) -> Column:
    """The original row as JSON, with empty fields written as "" (a CSV reader reads them
    as NULL, and `to_json` would leave them out)."""
    return F.to_json(F.struct(*[F.coalesce(F.col(c), F.lit("")).alias(c) for c in columns]))


def _data_columns(df: DataFrame, file_key: list[str]) -> list[str]:
    """The columns that came from the file, without the columns identifying the file."""
    return [c for c in df.columns if c not in file_key]


def _row_hash(columns: list[str]) -> Column:
    """A fingerprint of a row's contents, to tell exact copies from conflicting rows."""
    return F.sha2(
        F.concat_ws(ROW_SEPARATOR, *[F.coalesce(F.col(c), F.lit("")) for c in columns]), 256
    )


def _conflicts_on(key: Column, row_hash: Column, file_key: list[str]) -> Column:
    """True when `key` appears in the same file with more than one distinct content."""
    same_key = Window.partitionBy(*file_key, key)
    return F.size(F.collect_set(row_hash).over(same_key)) > 1


def _is_repeated_copy(row_hash: Column, file_key: list[str]) -> Column:
    """True for every exact copy of a row after the first one in the same file."""
    same_row = Window.partitionBy(*file_key, row_hash).orderBy(F.monotonically_increasing_id())
    return F.row_number().over(same_row) > 1


# --- the rules --------------------------------------------------------------------------


def rate_acquisitions(df: DataFrame, file_key: list[str]) -> DataFrame:
    """Apply the acquisition rules to rows read as text.

    `df` holds the file's columns as strings, plus the columns in `file_key` that identify
    the file a row came from (rules that compare rows only compare rows of one file).
    Returns the typed columns, `payload` (the original row as JSON) and `reason`, which is
    NULL for an accepted row.
    """
    raw = _data_columns(df, file_key)
    row_hash = _row_hash(raw)
    face, price = br_decimal("face_value"), br_decimal("purchase_price")
    due, acquired = br_date("due_date"), br_date("acquisition_date")
    number = _strip(F.col("installment_number")).try_cast(PARSE_DECIMAL)
    installment_key = F.coalesce(F.col("installment_id"), F.lit(""))

    reason = first_failure(
        ("missing_installment_id", _text("installment_id") == ""),
        ("invalid_installment_number", _unless((number >= 1) & (number == F.floor(number)))),
        ("invalid_debtor_doc", ~document_is_valid("debtor_doc")),
        ("invalid_seller_doc", ~document_is_valid("seller_doc")),
        ("invalid_date", due.isNull() | acquired.isNull()),
        ("non_positive_face_value", _unless(face > 0)),
        ("purchase_price_out_of_range", _unless((price > 0) & (price <= face))),
        ("due_before_acquisition", _unless(due > acquired)),
        ("conflicting_duplicate_in_file", _conflicts_on(installment_key, row_hash, file_key)),
        # Exact copies are interchangeable: keep one, reject the rest.
        ("duplicate_in_file", _is_repeated_copy(row_hash, file_key)),
    )
    return (
        df.withColumn("payload", _payload(raw))
        .withColumn("reason", reason)
        .withColumns(
            {
                "issue_date": br_date("issue_date"),
                "due_date": due,
                "acquisition_date": acquired,
                "face_value": face.try_cast(STORED_DECIMAL),
                "purchase_price": price.try_cast(STORED_DECIMAL),
                "installment_number": number.try_cast("int"),
            }
        )
    )


def derived_transaction_id(fund_code: str) -> Column:
    """The settlement ID for layouts whose files don't carry one.

    The Spark twin of `validation.derive_transaction_id`: same fields, same order, same
    separator, and the amount written in cents rounded half up.
    """
    cents = br_decimal("amount_paid").try_cast(STORED_DECIMAL).cast("string")
    parts = [
        F.lit(fund_code),
        _text("installment_id"),
        _text("settlement_date"),
        _text("settlement_type"),
        F.coalesce(cents, F.lit("")),
    ]
    return F.sha2(F.concat_ws(TRANSACTION_ID_SEPARATOR, *parts), 256)


def rate_settlements(
    df: DataFrame, file_key: list[str], fund_code: str, provider_transaction_id: bool
) -> DataFrame:
    """Apply the settlement rules to rows read as text.

    With `provider_transaction_id` the file's own `transaction_id` column is used (and
    required). Otherwise the ID is derived from the payment's fields.
    """
    raw = _data_columns(df, file_key)
    row_hash = _row_hash(raw)
    paid_on, amount = br_date("settlement_date"), br_decimal("amount_paid")
    transaction_id = (
        F.col("transaction_id") if provider_transaction_id else derived_transaction_id(fund_code)
    )
    transaction_key = F.coalesce(transaction_id, F.lit(""))

    # A derived ID is never empty, so this rule only exists for provider layouts.
    provider_only = []
    if provider_transaction_id:
        provider_only.append(("missing_transaction_id", _text("transaction_id") == ""))

    known_types = F.col("settlement_type").isin("FULL", "PARTIAL", "CANCELLATION")
    reason = first_failure(
        ("missing_installment_id", _text("installment_id") == ""),
        *provider_only,
        ("invalid_date", paid_on.isNull()),
        ("unknown_settlement_type", _unless(known_types)),
        ("negative_amount", _unless(amount >= 0)),
        ("conflicting_duplicate_in_file", _conflicts_on(transaction_key, row_hash, file_key)),
        ("duplicate_in_file", _is_repeated_copy(row_hash, file_key)),
    )
    return (
        df.withColumn("payload", _payload(raw))
        .withColumn("reason", reason)
        .withColumns(
            {
                "transaction_id": transaction_id,
                "settlement_date": paid_on,
                "amount_paid": amount.try_cast(STORED_DECIMAL),
            }
        )
    )
