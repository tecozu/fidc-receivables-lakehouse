"""Parity tests: the Spark rules must give the same answers as the pandas reference.

The pandas code in `validation.py` and `documents.py` is the reference. Each test feeds
the same inputs to both engines and compares the results. Spark DataFrames are built from
plain Python lists on purpose: PySpark does not fully support pandas 3 yet, and the
Databricks workspace runs pandas 2, so the tests must not depend on the conversion.
"""

from __future__ import annotations

import csv
import io
import json
import random
from collections import Counter
from datetime import date

import pandas as pd
import pytest

from fidc_lakehouse.config import Fund
from fidc_lakehouse.documents import _CNPJ_W1, _CNPJ_W2, _check_digit, is_valid_document
from fidc_lakehouse.parsers import LAYOUTS_WITH_TRANSACTION_ID
from fidc_lakehouse.spark_rules import (
    br_date,
    br_decimal,
    document_is_valid,
    rate_acquisitions,
    rate_settlements,
)
from fidc_lakehouse.validation import _date, _decimal, process_file


def _column(spark, values):
    return spark.createDataFrame([(v,) for v in values], "value string")


def _valid_cpf(rng: random.Random) -> str:
    digits = [rng.randint(0, 9) for _ in range(9)]
    digits.append(_check_digit(digits, list(range(10, 1, -1))))
    digits.append(_check_digit(digits, list(range(11, 1, -1))))
    return "".join(map(str, digits))


def _valid_cnpj(rng: random.Random) -> str:
    digits = [rng.randint(0, 9) for _ in range(12)]
    digits.append(_check_digit(digits, _CNPJ_W1))
    digits.append(_check_digit(digits, _CNPJ_W2))
    return "".join(map(str, digits))


def _corrupt(number: str) -> str:
    """The same number with its last digit changed, so the checksum must fail."""
    return number[:-1] + str((int(number[-1]) + 1) % 10)


def _mask_cpf(n: str) -> str:
    return f"{n[:3]}.{n[3:6]}.{n[6:9]}-{n[9:]}"


def _mask_cnpj(n: str) -> str:
    return f"{n[:2]}.{n[2:5]}.{n[5:8]}/{n[8:12]}-{n[12:]}"


# --- CPF / CNPJ -------------------------------------------------------------------------


def test_document_check_matches_pandas_on_hand_picked_cases(spark):
    cases = [
        "529.982.247-25",  # valid CPF
        "11.222.333/0001-81",  # valid CNPJ
        "111.111.111-11",  # right length, but all digits equal
        "00000000000000",  # CNPJ-sized, all digits equal
        "529.982.247-24",  # wrong check digit
        "12.ABC.345/0001-XX",  # letters
        "123",  # too short
        "5299822472500",  # 13 digits
        "",
        "   ",
        None,
    ]

    spark_result = [
        row.ok for row in _column(spark, cases).select(document_is_valid("value").alias("ok"))
        .collect()
    ]  # fmt: skip

    assert spark_result == [is_valid_document(c) for c in cases]


def test_document_check_matches_pandas_on_generated_numbers(spark):
    rng = random.Random(2026)
    cpfs = [_valid_cpf(rng) for _ in range(150)]
    cnpjs = [_valid_cnpj(rng) for _ in range(150)]
    cases = (
        cpfs
        + [_mask_cpf(n) for n in cpfs]
        + [_corrupt(n) for n in cpfs]
        + cnpjs
        + [_mask_cnpj(n) for n in cnpjs]
        + [_corrupt(n) for n in cnpjs]
    )

    spark_result = [
        row.ok for row in _column(spark, cases).select(document_is_valid("value").alias("ok"))
        .collect()
    ]  # fmt: skip
    expected = [is_valid_document(c) for c in cases]

    assert spark_result == expected
    assert 0.3 < sum(expected) / len(expected) < 0.7  # the cases really mix valid and invalid


def test_document_check_never_returns_null(spark):
    df = _column(spark, [None, "", "abc", "529.982.247-25"])

    values = [row.ok for row in df.select(document_is_valid("value").alias("ok")).collect()]

    assert None not in values


# --- dates ------------------------------------------------------------------------------

DATE_CASES = [
    "05/01/2026",
    "5/1/2026",  # unpadded day and month
    " 05/01/2026 ",  # surrounding spaces
    "\t05/01/2026\t",  # tabs
    "\u00a005/01/2026\u00a0",  # non-breaking spaces
    "31/12/2026",
    "29/02/2028",  # leap day
    "29/02/2026",  # not a real date
    "31/02/2026",
    "32/01/2026",
    "2026-01-05",  # wrong format
    "05-01-2026",
    "abc",
    "",
    None,
]


def test_date_parser_matches_pandas(spark):
    spark_result = [
        row.d for row in _column(spark, DATE_CASES).select(br_date("value").alias("d")).collect()
    ]

    pandas_result = _date(pd.Series(DATE_CASES, dtype="object"))
    expected = [None if pd.isna(d) else d.date() for d in pandas_result]

    assert spark_result == expected
    assert date(2026, 1, 5) in spark_result  # and the parse is not simply returning NULL


# --- decimals ---------------------------------------------------------------------------

DECIMAL_CASES = [
    "1000,50",
    "10,5",
    " 10,50 ",
    "\t10,50\n",  # tab and line break
    "-3,2",
    "0",
    "0,00",
    "1.000,50",  # thousands separator: not accepted by either engine
    "10,555",  # more than two decimals
    "abc",
    "10,5,5",
    "",
    None,
]


def test_decimal_parser_matches_pandas(spark):
    rows = _column(spark, DECIMAL_CASES).select(br_decimal("value").alias("n")).collect()
    spark_result = [None if r.n is None else float(r.n) for r in rows]

    pandas_result = _decimal(pd.Series(DECIMAL_CASES, dtype="object"))
    expected = [None if pd.isna(n) else float(n) for n in pandas_result]

    assert [r is None for r in spark_result] == [e is None for e in expected]
    for got, want in zip(spark_result, expected, strict=True):
        if want is not None:
            assert got == pytest.approx(want)


def test_a_value_with_three_decimals_is_not_rounded_by_the_parser(spark):
    """Rules compare what the file said. Rounding to cents happens when a value is stored."""
    row = _column(spark, ["10,555"]).select(br_decimal("value").alias("n")).first()

    assert float(row.n) == pytest.approx(10.555)


# --- the rules: the same files through both engines -------------------------------------

VALID_DEBTOR = "529.982.247-25"
VALID_SELLER = "11.222.333/0001-81"
FILE_KEY = ["_file"]

ACQUISITION_COLUMNS = [
    "contract_id", "installment_id", "installment_number", "receivable_type",
    "seller_name", "seller_doc", "originator_name", "originator_doc",
    "debtor_name", "debtor_doc", "issue_date", "due_date", "acquisition_date",
    "face_value", "purchase_price",
]  # fmt: skip
DERIVED_SETTLEMENT_COLUMNS = ["installment_id", "settlement_date", "settlement_type", "amount_paid"]
PROVIDER_SETTLEMENT_COLUMNS = [
    "installment_id", "transaction_id", "settlement_date", "settlement_type", "amount_paid",
]  # fmt: skip


def _acq(**overrides) -> dict:
    row = {
        "contract_id": "C1", "installment_id": "X-1", "installment_number": "1",
        "receivable_type": "TRADE_BILL", "seller_name": "Seller", "seller_doc": VALID_SELLER,
        "originator_name": "Orig", "originator_doc": VALID_SELLER, "debtor_name": "João",
        "debtor_doc": VALID_DEBTOR, "issue_date": "02/01/2026", "due_date": "02/03/2026",
        "acquisition_date": "05/01/2026", "face_value": "1000,50", "purchase_price": "950,25",
    }  # fmt: skip
    row.update(overrides)
    return row


def _sett(**overrides) -> dict:
    row = {
        "installment_id": "X-1", "transaction_id": "TX-1", "settlement_date": "05/01/2026",
        "settlement_type": "FULL", "amount_paid": "1000,00",
    }  # fmt: skip
    row.update(overrides)
    return row


def _csv(columns: list[str], rows: list[dict]) -> bytes:
    lines = [";".join(columns)] + [";".join(r[c] for c in columns) for r in rows]
    return "\n".join(lines).encode("utf-8")


def _spark_rows(spark, files: dict[str, bytes]):
    """All files as one DataFrame of strings, tagged with the file each row came from."""
    rows = []
    for name, content in files.items():
        for row in csv.DictReader(io.StringIO(content.decode("utf-8")), delimiter=";"):
            rows.append({**row, "_file": name})
    columns = list(rows[0])
    return spark.createDataFrame(
        [tuple(r[c] for c in columns) for r in rows], ", ".join(f"`{c}` string" for c in columns)
    )


def _day(value) -> date | None:
    return (
        None
        if value is None or pd.isna(value)
        else value.date()
        if hasattr(value, "date")
        else value
    )


def _payload(text: str) -> tuple:
    return tuple(sorted(json.loads(text).items()))


def _acquisition_key(r: dict) -> tuple:
    return (
        r["installment_id"], r["contract_id"], int(r["installment_number"]),
        round(float(r["face_value"]), 2), round(float(r["purchase_price"]), 2),
        _day(r["issue_date"]), _day(r["due_date"]), _day(r["acquisition_date"]),
    )  # fmt: skip


def _settlement_key(r: dict) -> tuple:
    # The amount is not compared here: pandas holds it as a float and Spark as an exact
    # decimal, so they print 10.555 differently. The derived ID covers the amount, and
    # test_the_derived_id_agrees_on_amounts_with_fractional_cents covers the rounding.
    return (
        r["installment_id"], r["transaction_id"], r["settlement_type"], _day(r["settlement_date"]),
    )  # fmt: skip


def _compare(spark, files: dict[str, bytes], fund: Fund, event: str):
    """Run every file through pandas and through Spark, and compare file by file."""
    if event == "acquisitions":
        rated = rate_acquisitions(_spark_rows(spark, files), FILE_KEY)
        key = _acquisition_key
    else:
        provider = fund.layouts["settlements"] in LAYOUTS_WITH_TRANSACTION_ID
        rated = rate_settlements(_spark_rows(spark, files), FILE_KEY, fund.code, provider)
        key = _settlement_key
    spark_rows = [r.asDict() for r in rated.collect()]

    for name, content in files.items():
        expected = process_file(content, fund, event)
        mine = [r for r in spark_rows if r["_file"] == name]

        assert len(mine) == expected.rows, name
        assert Counter(key(r) for r in mine if r["reason"] is None) == Counter(
            key(r) for r in expected.accepted.to_dict("records")
        ), f"accepted rows differ in {name}"
        assert Counter(
            (r["reason"], _payload(r["payload"])) for r in mine if r["reason"]
        ) == Counter(
            (r["reason"], _payload(r["payload"])) for r in expected.rejects.to_dict("records")
        ), f"rejected rows differ in {name}"
    return spark_rows


def _fund(settlement_layout: str = "csv_br_v1") -> Fund:
    return Fund("ALPHA", "Alpha", {"acquisitions": "csv_br_v1", "settlements": settlement_layout})


def test_acquisition_rules_match_pandas_on_hand_picked_files(spark):
    files = {
        "clean": _csv(
            ACQUISITION_COLUMNS, [_acq(), _acq(installment_id="X-2", installment_number="2")]
        ),
        "documents": _csv(
            ACQUISITION_COLUMNS,
            [
                _acq(installment_id="D-1", debtor_doc="12.ABC.345/0001-XX"),
                _acq(installment_id="D-2", seller_doc="12.345"),
                _acq(installment_id="D-3", debtor_doc=""),
                _acq(installment_id="D-4", debtor_doc="111.111.111-11"),
            ],
        ),
        "dates": _csv(
            ACQUISITION_COLUMNS,
            [
                _acq(installment_id="T-1", due_date=""),  # the NULL case
                _acq(installment_id="T-2", acquisition_date=""),
                _acq(installment_id="T-3", due_date="31/02/2026"),
                _acq(installment_id="T-4", due_date="01/01/2026"),  # before the acquisition
                _acq(installment_id="T-5", due_date="05/01/2026"),  # same day as the acquisition
                _acq(installment_id="T-6", due_date="2/3/2026", acquisition_date="5/1/2026"),
                _acq(installment_id="T-7", issue_date=""),  # issue_date has no rule
            ],
        ),
        "values": _csv(
            ACQUISITION_COLUMNS,
            [
                _acq(installment_id="V-1", face_value="-1,00"),
                _acq(installment_id="V-2", face_value="0,00"),
                _acq(installment_id="V-3", face_value=""),
                _acq(installment_id="V-4", face_value="abc"),
                _acq(installment_id="V-5", purchase_price="2000,00"),
                _acq(installment_id="V-6", purchase_price="0"),
                _acq(installment_id="V-7", purchase_price=""),
                _acq(installment_id="V-8", purchase_price="1000,50"),  # equal to the face value
                _acq(installment_id="V-9", face_value="", purchase_price=""),
            ],
        ),
        "numbers": _csv(
            ACQUISITION_COLUMNS,
            [
                _acq(installment_id=f"N-{i}", installment_number=n)
                for i, n in enumerate(["0", "-1", "1.5", "", "abc", "1.0", " 2 ", "3", "10"])
            ],
        ),
        "ids": _csv(
            ACQUISITION_COLUMNS,
            [
                _acq(installment_id="", debtor_doc="bad"),  # the missing ID wins
                _acq(installment_id="   "),
                _acq(installment_id="ok"),
            ],
        ),
        "duplicates": _csv(
            ACQUISITION_COLUMNS,
            [
                _acq(installment_id="U-1"),
                _acq(installment_id="U-1"),
                _acq(installment_id="U-1"),
                _acq(installment_id="U-2", face_value="1000,50"),
                _acq(installment_id="U-2", face_value="2000,00"),  # same ID, other contents
                _acq(installment_id="U-2", face_value="1000,50"),  # a copy of a conflicting row
                _acq(installment_id="U-3"),
                _acq(installment_id="U-3", face_value="-1,00"),  # conflicting AND invalid
            ],
        ),
        "whitespace": _csv(
            ACQUISITION_COLUMNS,
            [
                _acq(installment_id="W-1", due_date="\t02/03/2026 ", face_value=" 1000,50\t"),
                _acq(installment_id="W-2", installment_number="\u00a01\u00a0"),
            ],
        ),
    }

    _compare(spark, files, _fund(), "acquisitions")


def test_derived_settlement_rules_match_pandas_on_hand_picked_files(spark):
    columns = DERIVED_SETTLEMENT_COLUMNS
    files = {
        "clean": _csv(
            columns,
            [
                _sett(),
                _sett(installment_id="X-2", settlement_type="PARTIAL", amount_paid="400,00"),
                _sett(installment_id="X-2", settlement_date="20/01/2026", amount_paid="600,00"),
            ],
        ),
        "rules": _csv(
            columns,
            [
                _sett(installment_id=""),
                _sett(installment_id="X-3", settlement_date=""),
                _sett(installment_id="X-4", settlement_date="31/02/2026"),
                _sett(installment_id="X-5", settlement_type="REFUND"),
                _sett(installment_id="X-6", settlement_type="full"),
                _sett(installment_id="X-7", amount_paid="-1,00"),
                _sett(installment_id="X-8", amount_paid=""),
                _sett(installment_id="X-9", amount_paid="abc"),
                _sett(installment_id="X-10", settlement_type="CANCELLATION", amount_paid="0,00"),
            ],
        ),
        "cents": _csv(
            columns,
            [
                _sett(installment_id="C-1", amount_paid="10,555"),
                _sett(installment_id="C-2", amount_paid="1,005"),
                _sett(installment_id="C-3", amount_paid="0,125"),
                _sett(installment_id="C-4", amount_paid="2,675"),
                _sett(installment_id="C-5", amount_paid="10,5"),
                _sett(installment_id="C-6", amount_paid=" 10,50 "),
            ],
        ),
        "duplicates": _csv(
            columns,
            [
                _sett(installment_id="P-1", amount_paid="400,00"),
                _sett(installment_id="P-1", amount_paid="400,00"),  # identical: one is kept
                _sett(installment_id="P-1", amount_paid="600,00"),  # another payment
            ],
        ),
        "whitespace": _csv(
            columns,
            [
                _sett(installment_id=" W-1 ", settlement_date="\t05/01/2026", amount_paid="1,00\t"),
                _sett(installment_id="W-1", settlement_date="05/01/2026", amount_paid="1,00"),
            ],
        ),
    }

    _compare(spark, files, _fund("csv_br_v1"), "settlements")


def test_provider_settlement_rules_match_pandas_on_hand_picked_files(spark):
    columns = PROVIDER_SETTLEMENT_COLUMNS
    files = {
        "clean": _csv(
            columns,
            [
                _sett(transaction_id="TX-1", settlement_type="PARTIAL", amount_paid="400,00"),
                _sett(transaction_id="TX-2", amount_paid="600,00"),
            ],
        ),
        "missing": _csv(
            columns,
            [
                _sett(transaction_id=""),
                _sett(transaction_id="   "),
                _sett(installment_id="", transaction_id=""),
            ],
        ),
        "conflicts": _csv(
            columns,
            [
                _sett(transaction_id="TX-1", amount_paid="1000,00"),
                _sett(transaction_id="TX-1", amount_paid="50,00"),
                _sett(transaction_id="TX-3"),
                _sett(transaction_id="TX-3"),  # an exact copy
                _sett(transaction_id="TX-4", amount_paid="-5,00"),
                _sett(transaction_id="TX-4", amount_paid="5,00"),  # conflicting, one also invalid
            ],
        ),
    }

    _compare(spark, files, _fund("csv_br_v2"), "settlements")


def test_the_derived_id_agrees_on_amounts_with_fractional_cents(spark):
    """Pandas used a float and Spark an exact decimal: 10,555 rounded to different cents."""
    amounts = ["10,555", "1,005", "0,125", "2,675", "10,5", "10,50", "0", "0,001", "1e3"]
    rows = [_sett(installment_id=f"A-{i}", amount_paid=a) for i, a in enumerate(amounts)]
    content = _csv(DERIVED_SETTLEMENT_COLUMNS, rows)

    rated = rate_settlements(_spark_rows(spark, {"f": content}), FILE_KEY, "ALPHA", False)
    from_spark = {r.installment_id: r.transaction_id for r in rated.collect()}
    accepted = process_file(content, _fund(), "settlements").accepted
    from_pandas = dict(zip(accepted["installment_id"], accepted["transaction_id"], strict=True))

    assert len(from_pandas) == len(amounts)  # every amount is a valid payment
    assert from_spark == from_pandas


def test_every_generated_file_gets_the_same_verdict_from_both_engines(spark, landing, funds):
    """The synthetic landing zone has injected errors; both engines must treat them alike."""
    for fund in funds:
        for event in ("acquisitions", "settlements"):
            files = {
                p.name: p.read_bytes() for p in sorted((landing / fund.code / event).glob("*.csv"))
            }
            if files:
                _compare(spark, files, fund, event)
