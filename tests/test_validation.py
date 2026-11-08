import json
import subprocess
import sys
from datetime import date

import pandas as pd
import pytest

from fidc_lakehouse.config import Fund
from fidc_lakehouse.validation import derive_transaction_id, process_file

VALID_DEBTOR = "529.982.247-25"
VALID_SELLER = "11.222.333/0001-81"
ACQUISITION_HEADER = (
    "contract_id;installment_id;installment_number;receivable_type;seller_name;seller_doc;"
    "originator_name;originator_doc;debtor_name;debtor_doc;issue_date;due_date;"
    "acquisition_date;face_value;purchase_price"
)
DERIVED_SETTLEMENT_HEADER = "installment_id;settlement_date;settlement_type;amount_paid"
PROVIDER_SETTLEMENT_HEADER = (
    "installment_id;transaction_id;settlement_date;settlement_type;amount_paid"
)


def _fund(layout="csv_br_v1"):
    return Fund("ALPHA", "Alpha", {"acquisitions": layout, "settlements": layout})


def _provider_fund():
    """A fund whose settlement files carry the administrator's own payment ID."""
    return Fund("ALPHA", "Alpha", {"acquisitions": "csv_br_v1", "settlements": "csv_br_v2"})


def _acquisition(iid="X-1", debtor_doc=VALID_DEBTOR, face="1000,50", number="1"):
    return (
        f"C1;{iid};{number};TRADE_BILL;Seller;{VALID_SELLER};Orig;{VALID_SELLER};João;"
        f"{debtor_doc};02/01/2026;02/03/2026;05/01/2026;{face};950,25"
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
    assert row["installment_number"] == 1
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
    assert payload["installment_id"] == "X-2"
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

    assert result.accepted["installment_id"].tolist() == ["X-2"]
    assert result.rejects["row_number"].tolist() == [1, 3, 4]
    assert set(result.rejects["reason"]) == {"conflicting_duplicate_in_file"}


def test_a_conflicting_row_that_breaks_another_rule_reports_that_rule():
    content = _file(ACQUISITION_HEADER, _acquisition("X-1"), _acquisition("X-1", face="-1,00"))

    result = process_file(content, _fund(), "acquisitions")

    assert result.rejects["reason"].tolist() == [
        "conflicting_duplicate_in_file",
        "non_positive_face_value",
    ]


def test_a_missing_installment_id_is_rejected_before_anything_else():
    content = _file(ACQUISITION_HEADER, _acquisition("", debtor_doc="bad"))

    result = process_file(content, _fund(), "acquisitions")

    assert result.rejects["reason"].tolist() == ["missing_installment_id"]


@pytest.mark.parametrize("number", ["0", "-1", "1.5", "", "abc"])
def test_installment_number_must_be_an_integer_of_one_or_more(number):
    content = _file(ACQUISITION_HEADER, _acquisition(number=number))

    result = process_file(content, _fund(), "acquisitions")

    assert result.rejects["reason"].tolist() == ["invalid_installment_number"]


def test_installments_of_one_contract_are_accepted_together():
    content = _file(
        ACQUISITION_HEADER,
        _acquisition("X-1", number="1"),
        _acquisition("X-2", number="2"),
    )

    result = process_file(content, _fund(), "acquisitions")

    assert result.accepted["installment_number"].tolist() == [1, 2]
    assert result.accepted["contract_id"].nunique() == 1


# --- settlements: derived transaction ID (the layout has no payment ID column) -------------


def test_settlements_use_their_own_rules():
    content = _file(
        DERIVED_SETTLEMENT_HEADER,
        "X-1;05/01/2026;FULL;1000,00",
        "X-2;05/01/2026;REFUND;10,00",
    )

    result = process_file(content, _fund(), "settlements")

    assert result.accepted["installment_id"].tolist() == ["X-1"]
    assert result.accepted["amount_paid"].tolist() == [1000.0]
    assert result.rejects["reason"].tolist() == ["unknown_settlement_type"]


def test_a_missing_installment_id_in_a_settlement_is_rejected():
    content = _file(DERIVED_SETTLEMENT_HEADER, ";05/01/2026;FULL;1000,00")

    result = process_file(content, _fund(), "settlements")

    assert result.rejects["reason"].tolist() == ["missing_installment_id"]


def test_the_derived_transaction_id_is_stable_across_files_and_positions():
    first = _file(
        DERIVED_SETTLEMENT_HEADER,
        "X-1;05/01/2026;PARTIAL;400,00",
        "X-2;05/01/2026;FULL;100,00",
    )
    corrected = _file(
        DERIVED_SETTLEMENT_HEADER,
        "X-2;05/01/2026;FULL;100,00",  # moved to another position
        "X-1;05/01/2026;PARTIAL;450,00",  # amount corrected
    )

    ids_first = process_file(first, _fund(), "settlements").accepted.set_index("installment_id")
    ids_corrected = process_file(corrected, _fund(), "settlements").accepted.set_index(
        "installment_id"
    )

    assert ids_first.loc["X-2", "transaction_id"] == ids_corrected.loc["X-2", "transaction_id"]
    assert ids_first.loc["X-1", "transaction_id"] != ids_corrected.loc["X-1", "transaction_id"]


def test_partial_payments_of_one_installment_get_different_transaction_ids():
    content = _file(
        DERIVED_SETTLEMENT_HEADER,
        "X-1;05/01/2026;PARTIAL;400,00",
        "X-1;20/01/2026;FULL;600,00",
    )

    result = process_file(content, _fund(), "settlements")

    assert len(result.accepted) == 2
    assert result.accepted["transaction_id"].nunique() == 2


def test_identical_derived_payments_are_treated_as_exact_copies():
    """The documented limit of derived IDs: two identical payments can't be told apart."""
    row = "X-1;05/01/2026;PARTIAL;400,00"
    content = _file(DERIVED_SETTLEMENT_HEADER, row, row)

    result = process_file(content, _fund(), "settlements")

    assert result.accepted["row_number"].tolist() == [1]
    assert result.rejects["reason"].tolist() == ["duplicate_in_file"]


def test_the_transaction_id_hash_keeps_field_boundaries():
    """Without a separator these two different payments would give the same text."""
    columns = ["installment_id", "settlement_date", "settlement_type", "amount_paid"]
    raw = pd.DataFrame(
        [["1", "23/01/2026", "FULL", "10,00"], ["12", "3/01/2026", "FULL", "10,00"]],
        columns=columns,
    )

    glued = raw["installment_id"] + raw["settlement_date"]
    assert glued[0] == glued[1]  # the collision a separator prevents

    ids = derive_transaction_id(raw, "ALPHA")
    assert ids[0] != ids[1]


def test_the_transaction_id_ignores_spaces_and_amount_formatting():
    columns = ["installment_id", "settlement_date", "settlement_type", "amount_paid"]
    raw = pd.DataFrame(
        [["1", "05/01/2026", "FULL", "10,5"], [" 1 ", "05/01/2026 ", "FULL", "10,50"]],
        columns=columns,
    )

    ids = derive_transaction_id(raw, "ALPHA")

    assert ids[0] == ids[1]


def test_the_transaction_id_depends_on_the_fund():
    columns = ["installment_id", "settlement_date", "settlement_type", "amount_paid"]
    raw = pd.DataFrame([["1", "05/01/2026", "FULL", "10,00"]], columns=columns)

    assert derive_transaction_id(raw, "ALPHA")[0] != derive_transaction_id(raw, "BETA")[0]


# --- settlements: transaction ID supplied by the administrator -----------------------------


def test_a_provider_transaction_id_is_kept_as_is():
    content = _file(
        PROVIDER_SETTLEMENT_HEADER,
        "X-1;TX-1;05/01/2026;PARTIAL;400,00",
        "X-1;TX-2;20/01/2026;FULL;600,00",
    )

    result = process_file(content, _provider_fund(), "settlements")

    assert result.accepted["transaction_id"].tolist() == ["TX-1", "TX-2"]


def test_a_missing_provider_transaction_id_is_rejected():
    content = _file(PROVIDER_SETTLEMENT_HEADER, "X-1;;05/01/2026;FULL;1000,00")

    result = process_file(content, _provider_fund(), "settlements")

    assert result.rejects["reason"].tolist() == ["missing_transaction_id"]


def test_provider_transaction_ids_with_different_contents_conflict():
    content = _file(
        PROVIDER_SETTLEMENT_HEADER,
        "X-1;TX-1;05/01/2026;FULL;1000,00",
        "X-2;TX-1;05/01/2026;FULL;50,00",
        "X-3;TX-3;05/01/2026;FULL;70,00",
    )

    result = process_file(content, _provider_fund(), "settlements")

    assert result.accepted["transaction_id"].tolist() == ["TX-3"]
    assert set(result.rejects["reason"]) == {"conflicting_duplicate_in_file"}


def test_a_provider_payment_sent_twice_keeps_one_row():
    row = "X-1;TX-1;05/01/2026;FULL;1000,00"
    content = _file(PROVIDER_SETTLEMENT_HEADER, row, row)

    result = process_file(content, _provider_fund(), "settlements")

    assert result.accepted["row_number"].tolist() == [1]
    assert result.rejects["reason"].tolist() == ["duplicate_in_file"]


def test_a_provider_layout_without_its_transaction_id_column_fails_loudly():
    content = _file(DERIVED_SETTLEMENT_HEADER, "X-1;05/01/2026;FULL;1000,00")

    with pytest.raises(ValueError, match="transaction_id"):
        process_file(content, _provider_fund(), "settlements")


def test_unknown_layout_fails_loudly():
    with pytest.raises(ValueError, match="Unknown layout"):
        process_file(b"", _fund(layout="nope"), "acquisitions")


def test_a_file_where_every_row_is_rejected_returns_empty_accepted():
    content = _file(ACQUISITION_HEADER, _acquisition(face="-1,00"))

    result = process_file(content, _fund(), "acquisitions")

    assert isinstance(result.accepted, pd.DataFrame)
    assert (len(result.accepted), len(result.rejects)) == (0, 1)


def test_a_settlement_file_with_only_a_header_is_empty_not_an_error():
    result = process_file(DERIVED_SETTLEMENT_HEADER.encode(), _fund(), "settlements")

    assert (result.rows, len(result.accepted), len(result.rejects)) == (0, 0, 0)


def test_validation_does_not_need_local_only_dependencies():
    """The pipeline wheel ships without these, so importing them here would break it."""
    code = (
        "import sys\n"
        "for name in ('duckdb', 'faker', 'dbt'):\n"
        "    sys.modules[name] = None\n"
        "import fidc_lakehouse.validation\n"
    )
    subprocess.run([sys.executable, "-c", code], check=True)
