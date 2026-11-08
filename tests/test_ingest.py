from datetime import date

import duckdb
import pandas as pd

from fidc_lakehouse.config import Fund
from fidc_lakehouse.ingest import ingest
from fidc_lakehouse.validation import validate_acquisitions


def _count(db, table):
    with duckdb.connect(str(db)) as con:
        return con.execute(f"select count(*) from {table}").fetchone()[0]


def test_ingestion_is_idempotent(tmp_path, landing, funds):
    db = tmp_path / "wh.duckdb"
    first = ingest(db, landing, funds)
    rows_after_first = _count(db, "raw.acquisitions")

    second = ingest(db, landing, funds)

    assert all(r.status == "loaded" for r in first)
    assert all(r.status == "skipped_duplicate" for r in second)
    assert _count(db, "raw.acquisitions") == rows_after_first


def test_up_to_only_loads_files_until_that_date(tmp_path, landing, funds):
    db = tmp_path / "wh.duckdb"
    ingest(db, landing, funds, up_to=date(2026, 1, 7))
    with duckdb.connect(str(db)) as con:
        max_date = con.execute("select max(file_date) from meta.file_ledger").fetchone()[0]
    assert max_date == date(2026, 1, 7)


def test_every_row_is_either_accepted_or_rejected(tmp_path, landing, funds):
    db = tmp_path / "wh.duckdb"
    results = ingest(db, landing, funds)
    assert all(r.rows == r.accepted + r.rejected for r in results)
    assert _count(db, "meta.rejected_rows") == sum(r.rejected for r in results)


def test_corrected_file_is_loaded_as_new_version(tmp_path, landing, funds):
    db = tmp_path / "wh.duckdb"
    ingest(db, landing, funds)
    f = next((landing / funds[0].code / "acquisitions").glob("*.csv"))
    f.write_text(f.read_text(encoding="utf-8") + "\n", encoding="utf-8")  # content changed

    results = ingest(db, landing, funds)

    assert [r.path.name for r in results if r.status == "loaded"] == [f.name]


def _row(**overrides):
    row = {
        "contract_id": "C1", "installment_id": "X-1", "installment_number": "1",
        "receivable_type": "TRADE_BILL",
        "seller_name": "S", "seller_doc": "11.222.333/0001-81",
        "originator_name": "O", "originator_doc": "11.222.333/0001-81",
        "debtor_name": "D", "debtor_doc": "529.982.247-25",
        "issue_date": "02/01/2026", "due_date": "02/03/2026", "acquisition_date": "05/01/2026",
        "face_value": "1000,00", "purchase_price": "950,00",
    }  # fmt: skip
    row.update(overrides)
    return row


def test_validation_rules():
    raw = pd.DataFrame(
        [
            _row(),
            _row(installment_id="X-2", debtor_doc="12.ABC.345/0001-XX"),
            _row(installment_id="X-3", face_value="-1000,00"),
            _row(installment_id="X-4", purchase_price="1100,00"),
            _row(installment_id="X-5", due_date="01/01/2026"),
            _row(),  # duplicate of X-1
        ]
    )
    accepted, rejects = validate_acquisitions(raw, Fund("ALPHA", "Alpha", {}))
    assert accepted["installment_id"].tolist() == ["X-1"]
    assert rejects["reason"].tolist() == [
        "invalid_debtor_doc",
        "non_positive_face_value",
        "purchase_price_out_of_range",
        "due_before_acquisition",
        "duplicate_in_file",
    ]
