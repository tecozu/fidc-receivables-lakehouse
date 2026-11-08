with latest_files as (
    select file_id, ingested_at from {{ ref('stg_file_ledger') }} where is_latest_version
),

source as (
    select s.*, l.ingested_at
    from {{ source('silver', 'settlements') }} s
    join latest_files l on l.file_id = s.file_id
),

-- A payment has a real key (transaction_id), so the same payment can arrive in more
-- than one file (e.g. a corrected re-send under a new name). Counting it twice would
-- overstate what the fund received, so the most recently ingested occurrence wins.
ranked as (
    select
        *,
        row_number() over (
            partition by fund_code, transaction_id
            order by ingested_at desc, file_id desc, row_number desc
        ) as occurrence_rank
    from source
)

select
    fund_code,
    transaction_id,
    installment_id,
    settlement_date,
    settlement_type,
    amount_paid,
    file_id
from ranked
where occurrence_rank = 1
