"""The funds are listed in three places; they must not drift apart.

Onboarding a fund means adding it to funds.yaml (what the ingestion processes) and to the
dbt seed funds.csv (its provision policy). dbt_project.yml carries a default list only so
the project compiles offline.
"""

import csv

import yaml

from fidc_lakehouse.config import DBT_DIR, load_funds


def test_the_dbt_default_funds_match_funds_yaml():
    with open(DBT_DIR / "dbt_project.yml", encoding="utf-8") as f:
        default = yaml.safe_load(f)["vars"]["funds"]

    assert default == [fund.code for fund in load_funds()]


def test_every_fund_has_a_row_in_the_dbt_seed():
    with open(DBT_DIR / "seeds" / "funds.csv", encoding="utf-8", newline="") as f:
        seeded = {row["fund_code"] for row in csv.DictReader(f)}

    assert seeded == {fund.code for fund in load_funds()}
