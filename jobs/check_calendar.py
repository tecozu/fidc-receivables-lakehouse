# Databricks notebook source
# Says whether as_of_date is a business day, using the calendar table dbt builds
# (dim_calendar, which comes from the b3_holidays seed). The daily job reads the answer
# in a condition task and skips everything behind it on weekends and holidays.

as_of_date = dbutils.widgets.get("as_of_date")
calendar = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('marts_schema')}.dim_calendar"

# A named parameter (:day) keeps the value out of the SQL text. The calendar covers
# 2020-2035, so a date outside it is a mistake worth failing on, not a "no".
rows = spark.sql(
    f"select is_business_day from {calendar} where calendar_date = cast(:day as date)",
    args={"day": as_of_date},
).collect()
if not rows:
    raise ValueError(f"{as_of_date} is not in {calendar}")

# Strings, because the condition task compares text: "true" or "false".
answer = "true" if rows[0].is_business_day else "false"
print(f"{as_of_date}: is_business_day = {answer}")
dbutils.jobs.taskValues.set(key="is_business_day", value=answer)
