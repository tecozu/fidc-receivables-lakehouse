import csv
import hashlib
from datetime import date

from fidc_lakehouse.generate import generate


def _digest(folder):
    h = hashlib.sha256()
    for p in sorted(folder.rglob("*.csv")):
        h.update(p.name.encode())
        h.update(p.read_bytes())
    return h.hexdigest()


def test_generation_is_deterministic(tmp_path, funds):
    a, b = tmp_path / "a", tmp_path / "b"
    generate(funds, date(2026, 1, 5), date(2026, 1, 9), a, seed=1)
    generate(funds, date(2026, 1, 5), date(2026, 1, 9), b, seed=1)
    assert _digest(a) == _digest(b)


def test_one_acquisition_file_per_fund_per_business_day(landing, funds):
    for fund in funds:
        files = list((landing / fund.code / "acquisitions").glob("*.csv"))
        assert len(files) == 10  # 2026-01-05..16 = 10 business days


def test_files_use_brazilian_formats(landing, funds):
    f = next((landing / funds[0].code / "acquisitions").glob("*.csv"))
    with open(f, encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle, delimiter=";"))
    assert rows
    assert all("," in r["face_value"] for r in rows)
    assert all(len(r["acquisition_date"]) == 10 and r["acquisition_date"][2] == "/" for r in rows)
