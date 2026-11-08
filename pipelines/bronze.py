"""Bronze layer: every row of every file, exactly as it arrived.

Nothing is validated or rejected here. Rows stay as text, next to the file version they
came from, so a rule can change later and the silver tables can be rebuilt from bronze
without asking an administrator to resend anything.

The layout gives the schema, so a file's columns are never inferred. That alone does not
protect against a changed header (with `enforceSchema=false`, a swapped header was seen to
shift values silently), so each file's header line is stored with its fingerprint and
silver rejects every file whose header is not exactly the layout's.

The tables are created in a loop over the funds in `funds.yaml`: onboarding a fund is a
line in that file, not new code. Each fund gets its own tables, so a fund's files, size
and failures never touch another fund's.

This file runs inside a Databricks pipeline, where `spark` already exists.
"""

from pyspark import pipelines as dp

from fidc_lakehouse import spark_pipeline
from fidc_lakehouse.config import load_funds
from fidc_lakehouse.layouts import get_layout

LANDING = spark.conf.get("fidc.landing")  # noqa: F821
SELECTED = {code.strip() for code in spark.conf.get("fidc.funds").split(",")}  # noqa: F821


def define_rows_table(fund, event):
    layout = get_layout(fund.layouts[event])

    @dp.table(
        name=f"{fund.code.lower()}_{event}_rows",
        comment=f"{fund.code} {event} files, one row per CSV row, all columns as text.",
    )
    def rows():
        columns = ", ".join(f"`{c}` STRING" for c in layout.columns[event])
        stream = (
            spark.readStream.format("cloudFiles")  # noqa: F821
            .option("cloudFiles.format", "csv")
            # A corrected file re-sent under the same name must be read again.
            .option("cloudFiles.allowOverwrites", "true")
            # The layout gives the schema (all text). Auto Loader cannot infer one from an
            # empty folder, and that failure stops the whole pipeline update, every fund,
            # so a fund registered before its first file would block the others.
            .schema(columns)
            .option("rescuedDataColumn", "_rescued_data")
            .options(**layout.reader_options())
            .load(f"{LANDING}/{fund.code}/{event}/")
        )
        return spark_pipeline.with_file_metadata(stream)

    return rows


def define_fingerprint_table(fund):
    @dp.table(
        name=f"{fund.code.lower()}_file_fingerprints",
        comment=f"{fund.code}: path, size, modification time and SHA-256 of every file.",
    )
    def fingerprints():
        stream = (
            spark.readStream.format("cloudFiles")  # noqa: F821
            .option("cloudFiles.format", "binaryFile")
            .option("cloudFiles.allowOverwrites", "true")
            .option("pathGlobFilter", "*.csv")
            .load(f"{LANDING}/{fund.code}/")
        )
        return spark_pipeline.fingerprints(stream)

    return fingerprints


for fund in load_funds():
    if fund.code not in SELECTED:
        continue
    for event in fund.layouts:
        define_rows_table(fund, event)
    define_fingerprint_table(fund)
