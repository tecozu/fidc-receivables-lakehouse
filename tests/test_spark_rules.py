"""The Spark quality-gate rules: what each input must produce.

Every expectation here was first confirmed against an independent pandas implementation
of the same rules (since removed): both engines agreed on every case below, so these are
not snapshots of whatever the code happens to do.

Spark DataFrames are built from plain Python lists: the tests need no pandas at all.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
from datetime import date
from decimal import ROUND_HALF_UP, Decimal

from faker import Faker

from fidc_lakehouse.layouts import (
    ACQUISITION_COLUMNS,
    SETTLEMENT_COLUMNS,
    SETTLEMENT_COLUMNS_WITH_TRANSACTION_ID,
)
from fidc_lakehouse.spark_rules import (
    br_date,
    br_decimal,
    document_is_valid,
    rate_acquisitions,
    rate_settlements,
)

VALID_DEBTOR = "529.982.247-25"
VALID_SELLER = "11.222.333/0001-81"
FILE_KEY = ["_file"]
DERIVED_SETTLEMENT_COLUMNS = SETTLEMENT_COLUMNS
PROVIDER_SETTLEMENT_COLUMNS = SETTLEMENT_COLUMNS_WITH_TRANSACTION_ID


def _column(spark, values):
    return spark.createDataFrame([(v,) for v in values], "value string")


# --- CPF / CNPJ -------------------------------------------------------------------------

DOCUMENT_CASES = [
    ("529.982.247-25", True),  # valid CPF
    ("52998224725", True),  # the same, without punctuation
    ("11.222.333/0001-81", True),  # valid CNPJ
    ("111.111.111-11", False),  # right length, but all digits equal
    ("00000000000000", False),  # CNPJ-sized, all digits equal
    ("529.982.247-24", False),  # wrong check digit
    ("12.ABC.345/0001-XX", False),  # letters
    ("123", False),  # too short
    ("5299822472500", False),  # 13 digits
    ("", False),
    ("   ", False),
    (None, False),
]


def _corrupt(number: str) -> str:
    """The same number with its last digit changed, so the checksum must fail."""
    return number[:-1] + str((int(number[-1]) + 1) % 10)


def test_document_check_on_hand_picked_cases(spark):
    values = [v for v, _ in DOCUMENT_CASES]

    rows = _column(spark, values).select(document_is_valid("value").alias("ok")).collect()

    assert [r.ok for r in rows] == [expected for _, expected in DOCUMENT_CASES]


def test_document_check_on_generated_numbers(spark):
    fake = Faker("pt_BR")
    fake.seed_instance(2026)
    cpfs = [fake.cpf() for _ in range(150)]  # Faker builds numbers with correct check digits
    cnpjs = [fake.cnpj() for _ in range(150)]
    valid = cpfs + cnpjs + [c.replace(".", "").replace("-", "").replace("/", "") for c in cpfs]
    broken = [_corrupt(c) for c in cpfs + cnpjs]

    rows = _column(spark, valid + broken).select(document_is_valid("value").alias("ok")).collect()
    verdicts = [r.ok for r in rows]

    assert verdicts == [True] * len(valid) + [False] * len(broken)


def test_document_check_never_returns_null(spark):
    df = _column(spark, [None, "", "abc", "529.982.247-25"])

    values = [row.ok for row in df.select(document_is_valid("value").alias("ok")).collect()]

    assert None not in values


# --- dates and decimals -----------------------------------------------------------------

JAN_5 = date(2026, 1, 5)
DATE_CASES = [
    ("05/01/2026", JAN_5),
    ("5/1/2026", JAN_5),  # unpadded day and month
    (" 05/01/2026 ", JAN_5),  # surrounding spaces
    ("\t05/01/2026\t", JAN_5),  # tabs
    (" 05/01/2026 ", JAN_5),  # non-breaking spaces
    ("31/12/2026", date(2026, 12, 31)),
    ("29/02/2028", date(2028, 2, 29)),  # leap day
    ("29/02/2026", None),  # not a real date
    ("31/02/2026", None),
    ("32/01/2026", None),
    ("2026-01-05", None),  # wrong format
    ("05-01-2026", None),
    ("abc", None),
    ("", None),
    (None, None),
]
DECIMAL_CASES = [
    ("1000,50", Decimal("1000.5")),
    ("10,5", Decimal("10.5")),
    (" 10,50 ", Decimal("10.5")),
    ("\t10,50\n", Decimal("10.5")),  # tab and line break
    ("-3,2", Decimal("-3.2")),
    ("0", Decimal(0)),
    ("0,00", Decimal(0)),
    ("10,555", Decimal("10.555")),  # more than two decimals, not rounded by the parser
    ("1.000,50", None),  # a thousands separator is not accepted
    ("abc", None),
    ("10,5,5", None),
    ("", None),
    (None, None),
]


def test_date_parser(spark):
    rows = _column(spark, [v for v, _ in DATE_CASES]).select(br_date("value").alias("d")).collect()

    assert [r.d for r in rows] == [expected for _, expected in DATE_CASES]


def test_decimal_parser(spark):
    values = [v for v, _ in DECIMAL_CASES]
    rows = _column(spark, values).select(br_decimal("value").alias("n")).collect()

    assert [r.n for r in rows] == [expected for _, expected in DECIMAL_CASES]


# --- helpers for whole files ------------------------------------------------------------


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


def _csv(columns, rows: list[dict]) -> bytes:
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


def _verdicts(spark, files: dict[str, bytes], event: str, **settlement_options):
    """For each file: a sorted list of (row identifier, reason), reason None if accepted."""
    rows = _spark_rows(spark, files)
    if event == "acquisitions":
        rated = rate_acquisitions(rows, FILE_KEY)
    else:
        rated = rate_settlements(rows, FILE_KEY, "ALPHA", **settlement_options)

    def identify(payload: str) -> str:
        p = json.loads(payload)
        if event == "acquisitions":
            return p["installment_id"]
        return "|".join([p["installment_id"], p.get("transaction_id", ""), p["amount_paid"]])

    out: dict[str, list] = {name: [] for name in files}
    for r in rated.collect():
        out[r["_file"]].append((identify(r["payload"]), r["reason"]))
    return {name: sorted(v, key=lambda x: (x[0], x[1] or "")) for name, v in out.items()}


# --- the scenarios and what each row must produce ---------------------------------------

ACQUISITION_FILES = {
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


DERIVED_SETTLEMENT_FILES = {
    "clean": _csv(
        DERIVED_SETTLEMENT_COLUMNS,
        [
            _sett(),
            _sett(installment_id="X-2", settlement_type="PARTIAL", amount_paid="400,00"),
            _sett(installment_id="X-2", settlement_date="20/01/2026", amount_paid="600,00"),
        ],
    ),
    "rules": _csv(
        DERIVED_SETTLEMENT_COLUMNS,
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
        DERIVED_SETTLEMENT_COLUMNS,
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
        DERIVED_SETTLEMENT_COLUMNS,
        [
            _sett(installment_id="P-1", amount_paid="400,00"),
            _sett(installment_id="P-1", amount_paid="400,00"),  # identical: one is kept
            _sett(installment_id="P-1", amount_paid="600,00"),  # another payment
        ],
    ),
    "whitespace": _csv(
        DERIVED_SETTLEMENT_COLUMNS,
        [
            _sett(installment_id=" W-1 ", settlement_date="\t05/01/2026", amount_paid="1,00\t"),
            _sett(installment_id="W-1", settlement_date="05/01/2026", amount_paid="1,00"),
        ],
    ),
}


PROVIDER_SETTLEMENT_FILES = {
    "clean": _csv(
        PROVIDER_SETTLEMENT_COLUMNS,
        [
            _sett(transaction_id="TX-1", settlement_type="PARTIAL", amount_paid="400,00"),
            _sett(transaction_id="TX-2", amount_paid="600,00"),
        ],
    ),
    "missing": _csv(
        PROVIDER_SETTLEMENT_COLUMNS,
        [
            _sett(transaction_id=""),
            _sett(transaction_id="   "),
            _sett(installment_id="", transaction_id=""),
        ],
    ),
    "conflicts": _csv(
        PROVIDER_SETTLEMENT_COLUMNS,
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


EXPECTED_ACQUISITIONS = {
    "clean": [("X-1", None), ("X-2", None)],
    "documents": [
        ("D-1", "invalid_debtor_doc"),
        ("D-2", "invalid_seller_doc"),
        ("D-3", "invalid_debtor_doc"),
        ("D-4", "invalid_debtor_doc"),
    ],
    "dates": [
        ("T-1", "invalid_date"),
        ("T-2", "invalid_date"),
        ("T-3", "invalid_date"),
        ("T-4", "due_before_acquisition"),
        ("T-5", "due_before_acquisition"),
        ("T-6", None),
        ("T-7", None),
    ],
    "values": [
        ("V-1", "non_positive_face_value"),
        ("V-2", "non_positive_face_value"),
        ("V-3", "non_positive_face_value"),
        ("V-4", "non_positive_face_value"),
        ("V-5", "purchase_price_out_of_range"),
        ("V-6", "purchase_price_out_of_range"),
        ("V-7", "purchase_price_out_of_range"),
        ("V-8", None),
        ("V-9", "non_positive_face_value"),
    ],
    "numbers": [
        ("N-0", "invalid_installment_number"),
        ("N-1", "invalid_installment_number"),
        ("N-2", "invalid_installment_number"),
        ("N-3", "invalid_installment_number"),
        ("N-4", "invalid_installment_number"),
        ("N-5", None),
        ("N-6", None),
        ("N-7", None),
        ("N-8", None),
    ],
    "ids": [("", "missing_installment_id"), ("   ", "missing_installment_id"), ("ok", None)],
    "duplicates": [
        ("U-1", None),
        ("U-1", "duplicate_in_file"),
        ("U-1", "duplicate_in_file"),
        ("U-2", "conflicting_duplicate_in_file"),
        ("U-2", "conflicting_duplicate_in_file"),
        ("U-2", "conflicting_duplicate_in_file"),
        ("U-3", "conflicting_duplicate_in_file"),
        ("U-3", "non_positive_face_value"),
    ],
    "whitespace": [("W-1", None), ("W-2", None)],
}

EXPECTED_DERIVED_SETTLEMENTS = {
    "clean": [("X-1||1000,00", None), ("X-2||400,00", None), ("X-2||600,00", None)],
    "rules": [
        ("X-10||0,00", None),
        ("X-3||1000,00", "invalid_date"),
        ("X-4||1000,00", "invalid_date"),
        ("X-5||1000,00", "unknown_settlement_type"),
        ("X-6||1000,00", "unknown_settlement_type"),
        ("X-7||-1,00", "negative_amount"),
        ("X-8||", "negative_amount"),
        ("X-9||abc", "negative_amount"),
        ("||1000,00", "missing_installment_id"),
    ],
    "cents": [
        ("C-1||10,555", None),
        ("C-2||1,005", None),
        ("C-3||0,125", None),
        ("C-4||2,675", None),
        ("C-5||10,5", None),
        ("C-6|| 10,50 ", None),
    ],
    "duplicates": [
        ("P-1||400,00", None),
        ("P-1||400,00", "duplicate_in_file"),
        ("P-1||600,00", None),
    ],
    "whitespace": [
        (" W-1 ||1,00\t", "conflicting_duplicate_in_file"),
        ("W-1||1,00", "conflicting_duplicate_in_file"),
    ],
}

EXPECTED_PROVIDER_SETTLEMENTS = {
    "clean": [("X-1|TX-1|400,00", None), ("X-1|TX-2|600,00", None)],
    "missing": [
        ("X-1|   |1000,00", "missing_transaction_id"),
        ("X-1||1000,00", "missing_transaction_id"),
        ("||1000,00", "missing_installment_id"),
    ],
    "conflicts": [
        ("X-1|TX-1|1000,00", "conflicting_duplicate_in_file"),
        ("X-1|TX-1|50,00", "conflicting_duplicate_in_file"),
        ("X-1|TX-3|1000,00", None),
        ("X-1|TX-3|1000,00", "duplicate_in_file"),
        ("X-1|TX-4|-5,00", "negative_amount"),
        ("X-1|TX-4|5,00", "conflicting_duplicate_in_file"),
    ],
}


def test_acquisition_rules(spark):
    assert _verdicts(spark, ACQUISITION_FILES, "acquisitions") == EXPECTED_ACQUISITIONS


def test_settlement_rules_with_a_derived_transaction_id(spark):
    verdicts = _verdicts(
        spark, DERIVED_SETTLEMENT_FILES, "settlements", provider_transaction_id=False
    )

    # Note "whitespace": the two rows are the same payment once trimmed, but they are not
    # byte-identical, so they count as conflicting contents, not as exact copies.
    assert verdicts == EXPECTED_DERIVED_SETTLEMENTS


def test_settlement_rules_with_a_provider_transaction_id(spark):
    verdicts = _verdicts(
        spark, PROVIDER_SETTLEMENT_FILES, "settlements", provider_transaction_id=True
    )

    assert verdicts == EXPECTED_PROVIDER_SETTLEMENTS


# --- the derived transaction ID ---------------------------------------------------------


def _expected_transaction_id(fund: str, installment: str, day: str, kind: str, amount: str):
    """The contract written out in plain Python, as an independent oracle."""
    cents = Decimal(amount.strip().replace(",", ".")).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )
    text = "|".join([fund, installment.strip(), day.strip(), kind.strip(), f"{cents:.2f}"])
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _derived_ids(spark, rows: list[dict]) -> dict[str, str]:
    content = _csv(DERIVED_SETTLEMENT_COLUMNS, rows)
    rated = rate_settlements(
        _spark_rows(spark, {"f": content}), FILE_KEY, "ALPHA", provider_transaction_id=False
    )
    return {r["installment_id"]: r["transaction_id"] for r in rated.collect()}


def test_the_derived_id_matches_a_known_vector(spark):
    ids = _derived_ids(spark, [_sett()])

    # sha256("ALPHA|X-1|05/01/2026|FULL|1000.00"): changing this value changes every ID.
    assert ids["X-1"] == "cf3e447d7154dccfa0da7857ce76257e3fc93325dfd8766cb00036a544369a13"


def test_the_derived_id_matches_the_contract_for_fractional_cents(spark):
    """The amount is an exact decimal, rounded half up: 10,555 is 10.56, never 10.55."""
    amounts = ["10,555", "1,005", "0,125", "2,675", "10,5", "10,50", "0", "0,001", "1e3"]
    rows = [_sett(installment_id=f"A-{i}", amount_paid=a) for i, a in enumerate(amounts)]

    ids = _derived_ids(spark, rows)

    expected = {
        f"A-{i}": _expected_transaction_id("ALPHA", f"A-{i}", "05/01/2026", "FULL", a)
        for i, a in enumerate(amounts)
    }
    assert ids == expected


def test_the_derived_id_ignores_spaces_around_fields(spark):
    rows = [
        _sett(installment_id="S-1", amount_paid="10,5"),
        _sett(installment_id=" S-1 ", settlement_date=" 05/01/2026\t", amount_paid=" 10,50 "),
    ]
    content = _csv(DERIVED_SETTLEMENT_COLUMNS, rows)
    rated = rate_settlements(
        _spark_rows(spark, {"f": content}), FILE_KEY, "ALPHA", provider_transaction_id=False
    )

    ids = [r["transaction_id"] for r in rated.collect()]

    assert len(ids) == 2 and ids[0] == ids[1]


def test_the_derived_id_depends_on_the_fund(spark):
    content = _csv(DERIVED_SETTLEMENT_COLUMNS, [_sett()])
    rows = _spark_rows(spark, {"f": content})

    alpha = rate_settlements(rows, FILE_KEY, "ALPHA", provider_transaction_id=False).first()
    beta = rate_settlements(rows, FILE_KEY, "BETA", provider_transaction_id=False).first()

    assert alpha["transaction_id"] != beta["transaction_id"]


def test_the_field_separator_keeps_boundaries():
    """Without a separator these two different payments would give the same text."""
    first = ("ALPHA", "1", "23/01/2026", "FULL", "10,00")
    second = ("ALPHA", "12", "3/01/2026", "FULL", "10,00")

    assert "".join(first[1:3]) == "".join(second[1:3])  # the collision a separator prevents
    assert _expected_transaction_id(*first) != _expected_transaction_id(*second)


def test_partial_payments_of_one_installment_get_different_ids(spark):
    rows = [
        _sett(installment_id="Q", settlement_type="PARTIAL", amount_paid="400,00"),
        _sett(installment_id="Q", settlement_date="20/01/2026", amount_paid="600,00"),
    ]
    content = _csv(DERIVED_SETTLEMENT_COLUMNS, rows)

    rated = rate_settlements(
        _spark_rows(spark, {"f": content}), FILE_KEY, "ALPHA", provider_transaction_id=False
    )

    ids = {r["transaction_id"] for r in rated.collect()}
    assert len(ids) == 2
    assert all(r["reason"] is None for r in rated.collect())


# --- types of the accepted rows ---------------------------------------------------------


def test_accepted_rows_are_typed(spark):
    content = _csv(ACQUISITION_COLUMNS, [_acq(installment_number=" 3 ", face_value="1000,50")])

    row = rate_acquisitions(_spark_rows(spark, {"f": content}), FILE_KEY).first()

    assert row["reason"] is None
    assert row["installment_number"] == 3
    assert row["face_value"] == Decimal("1000.50")
    assert row["due_date"] == date(2026, 3, 2)


def test_a_header_only_file_is_empty_not_an_error(spark):
    schema = (
        "installment_id string, settlement_date string, settlement_type string, "
        "amount_paid string, _file string"
    )
    rows = spark.createDataFrame([], schema)

    rated = rate_settlements(rows, FILE_KEY, "ALPHA", provider_transaction_id=False)

    assert rated.count() == 0
