-- Daily portfolio snapshot: one row per open receivable per reference date.
-- Every run of a date fully replaces that date's snapshot (safe reprocessing after
-- corrected files):
--   * DuckDB (dev/CI): native incremental delete+insert on as_of_date.
--   * Athena (prod):   Iceberg table partitioned by as_of_date; a pre-hook deletes the
--                      reference date, then the new rows are appended.
{% if target.type == 'athena' %}
{{ config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='append',
    partitioned_by=['as_of_date'],
    pre_hook="{{ delete_reference_date() }}",
    on_schema_change='append_new_columns'
) }}
{% else %}
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='as_of_date',
    on_schema_change='append_new_columns'
) }}
{% endif %}

select
    as_of_date,
    fund_code,
    receivable_id,
    receivable_type,
    debtor_doc,
    seller_doc,
    acquisition_date,
    due_date,
    face_value,
    purchase_price,
    amount_paid_to_date,
    outstanding_amount,
    lifecycle_status,
    term_business_days,
    remaining_business_days,
    days_overdue,
    aging_status,
    discount_rate,
    present_value,
    provision_bucket,
    provision_rate,
    provision_amount,
    net_value,
    current_timestamp as processed_at
from {{ ref('int_receivable_valuation') }}
