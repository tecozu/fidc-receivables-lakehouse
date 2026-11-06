with latest_files as (
    select file_id from {{ ref('stg_file_ledger') }} where is_latest_version
)

select
    {{ surrogate_key(['s.file_id', 's.row_number']) }} as settlement_id,
    s.fund_code,
    s.receivable_id,
    s.settlement_date,
    s.settlement_type,
    s.amount_paid,
    s.file_id
from {{ source('raw', 'settlements') }} s
join latest_files l on l.file_id = s.file_id
