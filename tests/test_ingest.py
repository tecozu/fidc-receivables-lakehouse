"""The local runner: landing files in, DuckDB tables out (Spark runs inside)."""

from __future__ import annotations

import os
import shutil
from datetime import date

import duckdb
import pytest

from fidc_lakehouse.config import load_funds
from fidc_lakehouse.generate import generate
from fidc_lakehouse.ingest import ingest

REASONS = {
    "missing_installment_id",
    "invalid_installment_number",
    "invalid_debtor_doc",
    "invalid_seller_doc",
    "invalid_date",
    "non_positive_face_value",
    "purchase_price_out_of_range",
    "due_before_acquisition",
    "missing_transaction_id",
    "unknown_settlement_type",
    "negative_amount",
    "conflicting_duplicate_in_file",
    "duplicate_in_file",
}


def _query(db, sql):
    with duckdb.connect(str(db), read_only=True) as con:
        return con.execute(sql).fetchall()


def _count(db, table):
    return _query(db, f"select count(*) from {table}")[0][0]


def _csv_files(landing):
    return sorted(landing.rglob("*.csv"))


@pytest.fixture(scope="module")
def loaded_landing(tmp_path_factory):
    """Two weeks of generated files, ingested once and shared by the read-only tests."""
    landing = tmp_path_factory.mktemp("landing")
    generate(load_funds(), date(2026, 1, 5), date(2026, 1, 16), landing, seed=7)
    return landing


@pytest.fixture(scope="module")
def loaded(spark, loaded_landing, tmp_path_factory):
    db = tmp_path_factory.mktemp("warehouse") / "wh.duckdb"
    return db, ingest(db, loaded_landing, load_funds())


@pytest.fixture()
def small_landing(tmp_path):
    landing = tmp_path / "landing"
    generate(load_funds(), date(2026, 1, 5), date(2026, 1, 7), landing, seed=3)
    return landing


def test_every_row_is_either_accepted_or_rejected(loaded):
    db, summary = loaded

    accepted = _count(db, "silver.acquisitions") + _count(db, "silver.settlements")
    assert summary.rows == summary.accepted + summary.rejected
    assert accepted == summary.accepted
    assert _count(db, "meta.rejected_rows") == summary.rejected
    assert summary.rejected > 0  # the generator injects errors on purpose


def test_there_is_one_ledger_row_per_file_version(loaded, loaded_landing):
    db, summary = loaded

    assert summary.files + summary.identical_skipped == len(_csv_files(loaded_landing))
    assert _count(db, "meta.file_ledger") == summary.files
    assert _query(db, "select count(distinct file_id) from meta.file_ledger")[0][0] == summary.files


def test_rejected_rows_carry_a_known_reason_and_their_original_row(loaded):
    db, _ = loaded

    reasons = {r[0] for r in _query(db, "select distinct reason from meta.rejected_rows")}
    payloads = _query(db, "select payload from meta.rejected_rows limit 5")

    assert reasons <= REASONS
    assert all(p[0].startswith("{") for p in payloads)


def test_each_fund_gets_the_transaction_id_its_layout_declares(loaded):
    db, _ = loaded

    rows = dict(
        _query(
            db,
            "select fund_code, min(transaction_id) from silver.settlements group by fund_code",
        )
    )

    assert rows["ALPHA"].startswith("TX-ALPHA-")  # supplied by the administrator
    assert len(rows["BETA"]) == 64  # derived: a SHA-256 in hex


def test_the_file_id_comes_from_the_content_so_it_is_the_same_in_every_run(
    spark, loaded, loaded_landing, tmp_path
):
    first_db, _ = loaded
    second_db = tmp_path / "again.duckdb"

    ingest(second_db, loaded_landing, load_funds())

    query = "select file_name, file_id from meta.file_ledger order by 1, 2"
    assert _query(first_db, query) == _query(second_db, query)


def test_ingesting_again_changes_nothing(spark, small_landing, tmp_path):
    db = tmp_path / "wh.duckdb"
    first = ingest(db, small_landing, load_funds())
    before = {t: _count(db, t) for t in ("silver.acquisitions", "silver.settlements")}

    second = ingest(db, small_landing, load_funds())

    assert second == first
    assert {t: _count(db, t) for t in before} == before


def test_the_same_bytes_under_another_name_are_ignored(spark, small_landing, tmp_path):
    db = tmp_path / "wh.duckdb"
    original = next((small_landing / "ALPHA" / "acquisitions").glob("*.csv"))
    copy = original.with_name("acquisitions_2026-01-30.csv")
    shutil.copy(original, copy)
    os.utime(copy, (original.stat().st_mtime + 60, original.stat().st_mtime + 60))

    summary = ingest(db, small_landing, load_funds())

    assert summary.identical_skipped == 1
    names = {r[0] for r in _query(db, "select file_name from meta.file_ledger")}
    assert original.name in names and copy.name not in names


def test_a_corrected_file_is_kept_as_a_new_version(spark, small_landing, tmp_path):
    db = tmp_path / "wh.duckdb"
    ingest(db, small_landing, load_funds())
    target = next((small_landing / "ALPHA" / "acquisitions").glob("*.csv"))
    target.write_text(target.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    later = target.stat().st_mtime + 60
    os.utime(target, (later, later))

    ingest(db, small_landing, load_funds())

    versions = _query(
        db,
        f"select file_id, ingested_at from meta.file_ledger where file_name = '{target.name}'"
        " order by ingested_at",
    )
    assert len(versions) == 2
    assert versions[0][0] != versions[1][0]
    assert versions[0][1] < versions[1][1]


def test_up_to_only_loads_files_until_that_date(spark, small_landing, tmp_path):
    db = tmp_path / "wh.duckdb"

    ingest(db, small_landing, load_funds(), up_to=date(2026, 1, 6))

    assert _query(db, "select max(file_date) from meta.file_ledger")[0][0] == date(2026, 1, 6)


def test_an_empty_landing_zone_still_creates_the_tables(spark, tmp_path):
    db = tmp_path / "wh.duckdb"

    summary = ingest(db, tmp_path / "nothing-here", load_funds())

    assert (summary.files, summary.rows) == (0, 0)
    for table in ("silver.acquisitions", "silver.settlements", "meta.rejected_rows"):
        assert _count(db, table) == 0
    assert _count(db, "meta.file_ledger") == 0


def test_a_file_without_a_column_its_layout_requires_fails_loudly(spark, tmp_path):
    landing = tmp_path / "landing"
    folder = landing / "ALPHA" / "settlements"  # ALPHA's layout includes transaction_id
    folder.mkdir(parents=True)
    (folder / "settlements_2026-01-05.csv").write_text(
        "installment_id;settlement_date;settlement_type;amount_paid\nX;05/01/2026;FULL;1,00\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="transaction_id"):
        ingest(tmp_path / "wh.duckdb", landing, load_funds())


def test_a_file_with_only_a_header_is_recorded_with_zero_rows(spark, tmp_path):
    landing = tmp_path / "landing"
    folder = landing / "BETA" / "settlements"
    folder.mkdir(parents=True)
    name = "settlements_2026-01-05.csv"
    (folder / name).write_text(
        "installment_id;settlement_date;settlement_type;amount_paid\n", encoding="utf-8"
    )
    db = tmp_path / "wh.duckdb"

    ingest(db, landing, load_funds())

    row = _query(db, "select row_count, accepted_count, rejected_count from meta.file_ledger")
    assert row == [(0, 0, 0)]
