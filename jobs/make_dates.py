# Databricks notebook source
# Lists the business days of a range, read from the calendar table dbt builds
# (dim_calendar, which comes from the b3_holidays seed). The backfill job loops over them.

import json

start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")
calendar = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('marts_schema')}.dim_calendar"

# Named parameters (:start, :end) keep the values out of the SQL text.
rows = spark.sql(
    f"""
    select cast(calendar_date as string) as day
    from {calendar}
    where is_business_day
      and calendar_date between cast(:start as date) and cast(:end as date)
    order by calendar_date
    """,
    args={"start": start_date, "end": end_date},
).collect()

dates = [row.day for row in rows]
if not dates:
    raise ValueError(f"No business days between {start_date} and {end_date} in {calendar}")

print(f"{len(dates)} business days: {dates[0]} .. {dates[-1]}")
# The for_each task reads this list as {{tasks.make_dates.values.dates}}.
dbutils.jobs.taskValues.set(key="dates", value=dates)
