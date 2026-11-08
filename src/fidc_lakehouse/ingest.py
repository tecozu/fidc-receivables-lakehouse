"""The local runner: ingest the landing files with Spark running on this machine.

It does what the Databricks pipeline does, with the same functions:

    landing CSV files -> bronze rows (text) -> Spark rules -> silver tables, rejected
    rows and file ledger

The only differences are where bronze is read (a local folder instead of Auto Loader)
and where the results go: Parquet files that are loaded into DuckDB, so dbt can run on
them without a Databricks workspace.

Guarantees:
* **Idempotent** - every run rebuilds the tables from all the files, and a file whose
  SHA-256 was already seen is ignored, so running twice gives the same result.
* **Corrections supported** - a *different* file with the same name is a new version;
  the dbt staging layer keeps only the latest one.
* **Quality gate** - rows that break a rule go to `meta.rejected_rows` with the reason
  and the original row, instead of failing the file.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import duckdb
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from fidc_lakehouse import spark_pipeline as pipeline
from fidc_lakehouse.config import Fund
from fidc_lakehouse.landing import LandingFile, discover_files
from fidc_lakehouse.layouts import get_layout
from fidc_lakehouse.spark_session import get_spark

TABLES = {
    "acquisitions": "silver.acquisitions",
    "settlements": "silver.settlements",
    "rejected_rows": "meta.rejected_rows",
    "file_ledger": "meta.file_ledger",
}


@dataclass(frozen=True)
class IngestSummary:
    files: int  # file versions loaded
    identical_skipped: int  # files ignored because the same bytes were already loaded
    rows: int
    accepted: int
    rejected: int


def _read_rows(spark: SparkSession, files: list[LandingFile]) -> DataFrame:
    """The rows of files that share a fund and a layout, as text plus file metadata."""
    first = files[0]
    layout = get_layout(first.fund.layouts[first.event])
    paths = [str(f.path) for f in files]
    df = pipeline.with_file_metadata(spark.read.options(**layout.reader_options()).csv(paths))
    expected = layout.columns[first.event]
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(
            f"Layout '{layout.name}' requires {missing} in {first.event} files of "
            f"{first.fund.code}, but they have: {df.columns}"
        )
    return df.select(*expected, *pipeline.FILE_KEY)


def _group(files: list[LandingFile]) -> dict[tuple[str, str], list[LandingFile]]:
    groups: dict[tuple[str, str], list[LandingFile]] = {}
    for f in files:
        groups.setdefault((f.fund.code, f.event), []).append(f)
    return groups


def build_layers(spark: SparkSession, files: list[LandingFile]) -> pipeline.Layers:
    rated: dict[str, list[DataFrame]] = {"acquisitions": [], "settlements": []}
    for group in _group(files).values():
        fund, event = group[0].fund, group[0].event
        rows = _read_rows(spark, group)
        if event == "acquisitions":
            rated[event].append(pipeline.rate_acquisitions(rows, fund.code))
        else:
            layout = get_layout(fund.layouts[event])
            has_id = layout.has_transaction_id(event)
            rated[event].append(pipeline.rate_settlements(rows, fund.code, has_id))

    if files:
        binary = spark.read.format("binaryFile").load(
            [str(f.path) for f in files]
        )  # one row per file: path, size, modification time and bytes
        fingerprints = pipeline.fingerprints(binary)
    else:
        fingerprints = spark.createDataFrame(
            [], "file_path string, file_modification_time timestamp, size_bytes long, sha256 string"
        )
    return pipeline.build_layers(spark, rated["acquisitions"], rated["settlements"], fingerprints)


def _load_duckdb(db_path: Path, lake: Path, layers: pipeline.Layers) -> None:
    """Write each table as Parquet, then (re)create it in DuckDB from those files."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    frames = {
        "acquisitions": layers.acquisitions,
        "settlements": layers.settlements,
        "rejected_rows": layers.rejected_rows,
        "file_ledger": layers.file_ledger,
    }
    for name, df in frames.items():
        df.coalesce(1).write.mode("overwrite").parquet(str(lake / name))
    with duckdb.connect(str(db_path)) as con:
        con.execute("create schema if not exists silver; create schema if not exists meta;")
        for name, table in TABLES.items():
            source = (lake / name).as_posix()
            files = f"read_parquet('{source}/*.parquet')"
            con.execute(f"create or replace table {table} as select * from {files}")


def ingest(
    db_path: Path, landing: Path, funds: list[Fund], up_to: date | None = None
) -> IngestSummary:
    spark = get_spark()
    files = list(discover_files(landing, funds, up_to))
    layers = build_layers(spark, files)
    layers.file_ledger.persist()
    try:
        lake = db_path.parent / f"{db_path.stem}_lake"
        _load_duckdb(db_path, lake, layers)
        totals = layers.file_ledger.agg(
            F.count(F.lit(1)).alias("files"),
            F.coalesce(F.sum("row_count"), F.lit(0)).alias("rows"),
            F.coalesce(F.sum("accepted_count"), F.lit(0)).alias("accepted"),
            F.coalesce(F.sum("rejected_count"), F.lit(0)).alias("rejected"),
        ).first()
    finally:
        layers.file_ledger.unpersist()
    return IngestSummary(
        files=totals["files"],
        identical_skipped=len(files) - totals["files"],
        rows=int(totals["rows"]),
        accepted=int(totals["accepted"]),
        rejected=int(totals["rejected"]),
    )
