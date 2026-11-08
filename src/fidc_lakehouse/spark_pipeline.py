"""From bronze rows to the silver tables: the logic both runners share.

Bronze holds every row of every file exactly as it arrived (text, plus where it came
from). This module turns that into:

* the accepted acquisitions and settlements, typed (what dbt reads);
* the rejected rows, with the reason and the original row as JSON;
* the file ledger, one row per file version.

The Databricks pipeline (Lakeflow) and the local runner (`ingest.py`) both call these
functions. They differ only in how bronze is read and where the results are written, so
the business rules exist exactly once, in `spark_rules.py`.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import reduce

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from fidc_lakehouse import spark_rules

# A file version is identified by its path and its modification time: a corrected file
# re-sent under the same name has the same path and a later modification time.
FILE_KEY = ["file_path", "file_modification_time"]

ACQUISITION_DDL = (
    "fund_code string, contract_id string, installment_id string, installment_number int, "
    "receivable_type string, seller_name string, seller_doc string, originator_name string, "
    "originator_doc string, debtor_name string, debtor_doc string, issue_date date, "
    "due_date date, acquisition_date date, face_value decimal(18, 2), "
    "purchase_price decimal(18, 2), file_id bigint, row_number int"
)
SETTLEMENT_DDL = (
    "fund_code string, installment_id string, transaction_id string, settlement_date date, "
    "settlement_type string, amount_paid decimal(18, 2), file_id bigint, row_number int"
)
REJECTED_DDL = "file_id bigint, row_number int, reason string, payload string"
LEDGER_DDL = (
    "file_id bigint, fund_code string, event_type string, file_name string, file_date date, "
    "sha256 string, row_count int, accepted_count int, rejected_count int, "
    "ingested_at timestamp"
)

# <anything>/<FUND>/<event>/<file name>
_PATH_PARTS = r"/([^/]+)/(acquisitions|settlements)/([^/]+)$"


@dataclass
class Layers:
    acquisitions: DataFrame
    settlements: DataFrame
    rejected_rows: DataFrame
    file_ledger: DataFrame


def normalize_path(column: Column) -> Column:
    """Paths from different readers must compare equal.

    `binaryFile` reports `dbfs:/Volumes/...` and Auto Loader's `_metadata.file_path`
    reports `/Volumes/...`, so a join on the raw values would match nothing.
    """
    return F.regexp_replace(column, r"^dbfs:", "")


def with_file_metadata(df: DataFrame) -> DataFrame:
    """Add the columns that say which file version each row came from."""
    return df.select(
        "*",
        normalize_path(F.col("_metadata.file_path")).alias("file_path"),
        F.col("_metadata.file_modification_time").alias("file_modification_time"),
    )


def fingerprints(binary_files: DataFrame) -> DataFrame:
    """One row per file with its size and SHA-256, without keeping the content."""
    return binary_files.select(
        normalize_path(F.col("path")).alias("file_path"),
        F.col("modificationTime").alias("file_modification_time"),
        F.col("length").alias("size_bytes"),
        F.sha2(F.col("content"), 256).alias("sha256"),
    )


def keep_first_of_each_content(files: DataFrame) -> DataFrame:
    """A file whose SHA-256 was already seen is a re-send of the same bytes: drop it.

    This is what makes ingestion idempotent and keeps a payment from being counted twice
    when the administrator sends the same file under another name.
    """
    first = Window.partitionBy("sha256").orderBy("file_modification_time", "file_path")
    return files.withColumn("_n", F.row_number().over(first)).where("_n = 1").drop("_n")


def file_id(sha256: Column) -> Column:
    """A number derived from the file's content: the same bytes give the same ID anywhere.

    15 hex digits are 60 bits, which fits a signed 64-bit integer.
    """
    return F.conv(F.substring(sha256, 1, 15), 16, 10).try_cast("bigint")


def rate_acquisitions(rows: DataFrame, fund_code: str) -> DataFrame:
    """`rows` holds exactly the layout's columns plus FILE_KEY."""
    rated = spark_rules.rate_acquisitions(rows, FILE_KEY)
    return rated.withColumn("fund_code", F.lit(fund_code))


def rate_settlements(rows: DataFrame, fund_code: str, provider_transaction_id: bool) -> DataFrame:
    rated = spark_rules.rate_settlements(rows, FILE_KEY, fund_code, provider_transaction_id)
    return rated.withColumn("fund_code", F.lit(fund_code))


def _conform(df: DataFrame, ddl: str) -> DataFrame:
    schema = StructType.fromDDL(ddl)
    return df.select(*[F.col(f.name).cast(f.dataType) for f in schema.fields])


def _empty(spark: SparkSession, ddl: str) -> DataFrame:
    return spark.createDataFrame([], StructType.fromDDL(ddl))


def _union(frames: list[DataFrame]) -> DataFrame | None:
    return reduce(lambda a, b: a.unionByName(b), frames) if frames else None


def _number_rows(df: DataFrame, ids: DataFrame) -> DataFrame:
    """Attach `file_id` and a stable position inside the file.

    The position orders rows by their original content, not by their line in the file:
    a parallel reader has no line numbers. It is used for lineage and as a tie-breaker,
    never as part of a key.
    """
    in_file = Window.partitionBy(*FILE_KEY).orderBy(
        F.sha2(F.col("payload"), 256), F.monotonically_increasing_id()
    )
    return df.join(ids, FILE_KEY, "inner").withColumn("row_number", F.row_number().over(in_file))


def build_layers(
    spark: SparkSession,
    acquisitions: list[DataFrame],
    settlements: list[DataFrame],
    files: DataFrame,
) -> Layers:
    """Combine the rated rows of every fund with the file fingerprints.

    `acquisitions` and `settlements` are the outputs of `rate_*`, one per fund.
    `files` is the output of `fingerprints`.
    """
    kept = keep_first_of_each_content(files).withColumn("file_id", file_id(F.col("sha256")))
    ids = kept.select(*FILE_KEY, "file_id")

    acq = _union(acquisitions)
    sett = _union(settlements)
    acq = _number_rows(acq, ids) if acq is not None else None
    sett = _number_rows(sett, ids) if sett is not None else None

    def accepted(df: DataFrame | None, ddl: str) -> DataFrame:
        if df is None:
            return _empty(spark, ddl)
        return _conform(df.where(F.col("reason").isNull()), ddl)

    def rejected(df: DataFrame | None) -> DataFrame:
        if df is None:
            return _empty(spark, REJECTED_DDL)
        return _conform(df.where(F.col("reason").isNotNull()), REJECTED_DDL)

    rated = [df.select(*FILE_KEY, "reason") for df in (acq, sett) if df is not None]
    if rated:
        counts = (
            _union(rated)
            .groupBy(*FILE_KEY)
            .agg(
                F.count(F.lit(1)).alias("row_count"),
                F.count(F.when(F.col("reason").isNull(), 1)).alias("accepted_count"),
            )
        )
    else:
        counts = _empty(spark, "file_path string, file_modification_time timestamp, "
                               "row_count int, accepted_count int")  # fmt: skip

    ledger = (
        kept.join(counts, FILE_KEY, "left")
        .withColumn("row_count", F.coalesce(F.col("row_count"), F.lit(0)))
        .withColumn("accepted_count", F.coalesce(F.col("accepted_count"), F.lit(0)))
        .withColumn("rejected_count", F.col("row_count") - F.col("accepted_count"))
        .withColumn("fund_code", F.regexp_extract("file_path", _PATH_PARTS, 1))
        .withColumn("event_type", F.regexp_extract("file_path", _PATH_PARTS, 2))
        .withColumn("file_name", F.regexp_extract("file_path", _PATH_PARTS, 3))
        .withColumn(
            "file_date",
            F.to_date(F.regexp_extract("file_name", r"_(\d{4}-\d{2}-\d{2})\.csv$", 1)),
        )
        .withColumn("ingested_at", F.col("file_modification_time"))
    )
    rejected_rows = [rejected(df) for df in (acq, sett)]
    return Layers(
        acquisitions=accepted(acq, ACQUISITION_DDL),
        settlements=accepted(sett, SETTLEMENT_DDL),
        rejected_rows=rejected_rows[0].unionByName(rejected_rows[1]),
        file_ledger=_conform(ledger, LEDGER_DDL),
    )
