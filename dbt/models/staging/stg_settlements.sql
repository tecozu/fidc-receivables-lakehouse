with latest_files as (
    select file_id from {{ ref('stg_file_ledger') }} where is_latest_version
)

select
    md5(concat_ws('|', s.file_id, s.row_number)) as settlement_id,
    s.fund_code,
    s.receivable_id,
    s.settlement_date,
    s.settlement_type,
    s.amount_paid,
    s.file_id
from {{ source('raw', 'settlements') }} s
join latest_files using (file_id)
