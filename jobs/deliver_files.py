# Databricks notebook source
# SIMULATION of the fund administrator. In production the administrator sends files
# straight to the landing zone and nothing here exists. In this demo the generated files
# wait in an archive volume, and this notebook delivers to landing every file dated up to
# as_of_date that landing does not have yet.
#
# Idempotent: a second run copies nothing. Catch-up: a skipped day is delivered by the
# next run. It never overwrites, because a rewritten file would look like a corrected
# version to the pipeline.

import re
from datetime import date

as_of_date = date.fromisoformat(dbutils.widgets.get("as_of_date"))
archive = dbutils.widgets.get("archive_path").rstrip("/")
landing = dbutils.widgets.get("landing_path").rstrip("/")

FILE_DATE = re.compile(r"_(\d{4}-\d{2}-\d{2})\.csv$")  # same pattern as landing.py


def children(folder):
    """The entries of a folder, or none if it does not exist yet."""
    try:
        return dbutils.fs.ls(folder)
    except Exception:  # noqa: BLE001  (a missing folder is normal on the first delivery)
        return []


delivered = 0
for fund in (e for e in children(archive) if e.isDir()):
    fund_name = fund.name.rstrip("/")  # folder names end with "/" in dbutils listings
    for event in (e for e in children(f"{archive}/{fund_name}") if e.isDir()):
        event_name = event.name.rstrip("/")
        source_dir = f"{archive}/{fund_name}/{event_name}"
        target_dir = f"{landing}/{fund_name}/{event_name}"
        already_there = {f.name for f in children(target_dir)}
        for file in children(source_dir):
            match = FILE_DATE.search(file.name)
            if not match or date.fromisoformat(match.group(1)) > as_of_date:
                continue
            if file.name in already_there:
                continue
            dbutils.fs.mkdirs(target_dir)
            dbutils.fs.cp(file.path, f"{target_dir}/{file.name}")
            delivered += 1

print(f"Delivered {delivered} file(s) dated up to {as_of_date}")
