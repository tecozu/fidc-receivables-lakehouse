-- Daily portfolio snapshot: one row per open installment per reference date.
-- Every run of a date fully replaces that date's snapshot (safe reprocessing after
-- corrected files):
--   * DuckDB (dev/CI): native incremental delete+insert on as_of_date.
--   * Databricks (prod): Delta replace_where on the reference date, an atomic overwrite
--                        of just that date (rows no longer open disappear too).
{% if target.type == 'databricks' %}
{{ config(
    materialized='incremental',
    file_format='delta',
    incremental_strategy='replace_where',
    incremental_predicates=["as_of_date = date '" ~ var('as_of_date') ~ "'"],
    liquid_clustered_by=['as_of_date'],
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
    installment_id,
    contract_id,
    installment_number,
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
from {{ ref('int_installment_valuation') }}
