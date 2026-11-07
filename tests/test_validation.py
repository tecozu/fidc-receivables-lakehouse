import json
import subprocess
import sys
from datetime import date

import pandas as pd
import pytest

from fidc_lakehouse.config import Fund
from fidc_lakehouse.validation import process_file

VALID_DEBTOR = "529.982.247-25"
VALID_SELLER = "11.222.333/0001-81"
ACQUISITION_HEADER = (
    "receivable_id;contract_id;receivable_type;seller_name;seller_doc;originator_name;"
    "originator_doc;debtor_name;debtor_doc;issue_date;due_date;acquisition_date;"
    "face_value;purchase_price"
)


def _fund(layout="csv_br_v1"):
    return Fund("ALPHA", "Alpha", {"acquisitions": layout, "settlements": layout})


def _acquisition(rid="X-1", debtor_doc=VALID_DEBTOR, face="1000,50"):
    return (
        f"{rid};C1;TRADE_BILL;Seller;{VALID_SELLER};Orig;{VALID_SELLER};João;{debtor_doc};"
        f"02/01/2026;02/03/2026;05/01/2026;{face};950,25"
    )


def _file(header, *rows):
    return "\n".join([header, *rows]).encode("utf-8")


def test_accepted_rows_are_typed_and_keep_their_lineage():
    content = _file(ACQUISITION_HEADER, _acquisition())

    result = process_file(content, _fund(), "acquisitions")

    assert (result.rows, len(result.accepted), len(result.rejects)) == (1, 1, 0)
    row = result.accepted.iloc[0]
    assert row["fund_code"] == "ALPHA"
    assert row["row_number"] == 1
    assert row["face_value"] == pytest.approx(1000.50)
    assert row["due_date"].date() == date(2026, 3, 2)


def test_rejected_rows_keep_the_reason_the_payload_and_their_position():
    content = _file(
        ACQUISITION_HEADER,
        _acquisition("X-1"),
        _acquisition("X-2", debtor_doc="12.ABC.345/0001-XX"),
        _acquisition("X-3"),
    )

    result = process_file(content, _fund(), "acquisitions")

    assert result.rows == 3
    assert result.accepted["row_number"].tolist() == [1, 3]
    reject = result.rejects.iloc[0]
    assert (reject["row_number"], reject["reason"]) == (2, "invalid_debtor_doc")
    payload = json.loads(reject["payload"])
    assert payload["receivable_id"] == "X-2"
    assert payload["debtor_name"] == "João"


def test_exact_copies_keep_one_row():
    content = _file(ACQUISITION_HEADER, _acquisition("X-1"), _acquisition("X-1"))

    result = process_file(content, _fund(), "acquisitions")

    assert result.accepted["row_number"].tolist() == [1]
    assert result.rejects["reason"].tolist() == ["duplicate_in_file"]


def test_conflicting_rows_with_the_same_id_are_all_rejected():
    content = _file(
        ACQUISITION_HEADER,
        _acquisition("X-1", face="1000,50"),
        _acquisition("X-2"),
        _acquisition("X-1", face="2000,00"),
        _acquisition("X-1", face="1000,50"),  # an exact copy of a conflicting row
    )

    result = process_file(content, _fund(), "acquisitions")

    assert result.accepted["receivable_id"].tolist() == ["X-2"]
    assert result.rejects["row_number"].tolist() == [1, 3, 4]
    assert set(result.rejects["reason"]) == {"conflicting_duplicate_in_file"}


def test_a_conflicting_row_that_breaks_another_rule_reports_that_rule():
    content = _file(ACQUISITION_HEADER, _acquisition("X-1"), _acquisition("X-1", face="-1,00"))

    result = process_file(content, _fund(), "acquisitions")

    assert result.rejects["reason"].tolist() == [
        "conflicting_duplicate_in_file",
        "non_positive_face_value",
    ]


def test_settlements_use_their_own_rules():
    content = _file(
        "receivable_id;settlement_date;settlement_type;amount_paid",
        "X-1;05/01/2026;FULL;1000,00",
        "X-2;05/01/2026;REFUND;10,00",
    )

    result = process_file(content, _fund(), "settlements")

    assert result.accepted["receivable_id"].tolist() == ["X-1"]
    assert result.accepted["amount_paid"].tolist() == [1000.0]
    assert result.rejects["reason"].tolist() == ["unknown_settlement_type"]


def test_unknown_layout_fails_loudly():
    with pytest.raises(ValueError, match="Unknown layout"):
        process_file(b"", _fund(layout="nope"), "acquisitions")


def test_a_file_where_every_row_is_rejected_returns_empty_accepted():
    content = _file(ACQUISITION_HEADER, _acquisition(face="-1,00"))

    result = process_file(content, _fund(), "acquisitions")

    assert isinstance(result.accepted, pd.DataFrame)
    assert (len(result.accepted), len(result.rejects)) == (0, 1)


def test_validation_does_not_need_local_only_dependencies():
    """The pipeline wheel ships without these, so importing them here would break it."""
    code = (
        "import sys\n"
        "for name in ('duckdb', 'faker', 'dbt'):\n"
        "    sys.modules[name] = None\n"
        "import fidc_lakehouse.validation\n"
    )
    subprocess.run([sys.executable, "-c", code], check=True)
