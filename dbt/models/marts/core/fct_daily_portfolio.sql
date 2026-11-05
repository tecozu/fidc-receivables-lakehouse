-- Daily portfolio snapshot: one row per open receivable per reference date.
-- delete+insert on as_of_date makes every run of a date a full, idempotent
-- replacement of that date's snapshot (safe reprocessing after corrected files).
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='as_of_date',
    on_schema_change='append_new_columns'
) }}

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
