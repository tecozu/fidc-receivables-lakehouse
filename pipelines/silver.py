"""Silver layer: the quality gate applied to each fund's bronze rows.

For every fund this defines four materialized views:

* `<fund>_acquisitions`, `<fund>_settlements`: the accepted rows, typed;
* `<fund>_rejected_rows`: the rejected rows, with the reason and the original row;
* `<fund>_file_ledger`: one row per file version.

They call the same functions as the local runner (`fidc_lakehouse.spark_pipeline`), so
the rules exist once. A materialized view (not a streaming table) is used because the
duplicate rules compare a row with the other rows of its file.

There is deliberately no table that unions the funds here: it would make every fund wait
for the slowest one. Consumers (dbt) union the per-fund tables themselves.

This file runs inside a Databricks pipeline, where `spark` already exists.
"""

from pyspark import pipelines as dp

from fidc_lakehouse import spark_pipeline
from fidc_lakehouse.config import load_funds
from fidc_lakehouse.layouts import get_layout

CATALOG = spark.conf.get("fidc.catalog")  # noqa: F821
SILVER = f"{CATALOG}.{spark.conf.get('fidc.silver_schema')}"  # noqa: F821
META = f"{CATALOG}.{spark.conf.get('fidc.meta_schema')}"  # noqa: F821
SELECTED = {code.strip() for code in spark.conf.get("fidc.funds").split(",")}  # noqa: F821


def rated_rows(fund, event):
    """A fund's bronze rows for one event, with the rules applied."""
    layout = get_layout(fund.layouts[event])
    bronze = spark.read.table(f"{fund.code.lower()}_{event}_rows")  # noqa: F821
    rows = bronze.select(*layout.columns[event], *spark_pipeline.FILE_KEY)
    header = layout.expected_header(event)
    if event == "acquisitions":
        return spark_pipeline.rate_acquisitions(rows, fund.code, header)
    has_id = layout.has_transaction_id(event)
    return spark_pipeline.rate_settlements(rows, fund.code, has_id, header)


def layers_of(fund):
    files = spark.read.table(f"{fund.code.lower()}_file_fingerprints")  # noqa: F821
    return spark_pipeline.build_layers(
        spark,  # noqa: F821
        [rated_rows(fund, "acquisitions")],
        [rated_rows(fund, "settlements")],
        files,
    )


def define_silver_views(fund):
    code = fund.code.lower()

    @dp.materialized_view(
        name=f"{SILVER}.{code}_acquisitions",
        comment=f"{fund.code}: acquisitions that passed the quality gate, typed.",
    )
    def acquisitions():
        return layers_of(fund).acquisitions

    @dp.materialized_view(
        name=f"{SILVER}.{code}_settlements",
        comment=f"{fund.code}: settlements that passed the quality gate, typed.",
    )
    def settlements():
        return layers_of(fund).settlements

    @dp.materialized_view(
        name=f"{META}.{code}_rejected_rows",
        comment=f"{fund.code}: rows that broke a rule, with the reason and the original row.",
    )
    def rejected_rows():
        return layers_of(fund).rejected_rows

    @dp.materialized_view(
        name=f"{META}.{code}_file_ledger",
        comment=f"{fund.code}: one row per file version, with counts and SHA-256.",
    )
    def file_ledger():
        return layers_of(fund).file_ledger


for fund in load_funds():
    if fund.code in SELECTED:
        define_silver_views(fund)
