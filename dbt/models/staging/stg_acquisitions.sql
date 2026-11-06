with latest_files as (
    select file_id from {{ ref('stg_file_ledger') }} where is_latest_version
),

source as (
    select a.*
    from {{ source('raw', 'acquisitions') }} a
    join latest_files l on l.file_id = a.file_id
),

-- The same receivable can appear in more than one file (e.g. a corrected re-send
-- under a new name). The most recently ingested occurrence wins.
ranked as (
    select
        *,
        row_number() over (
            partition by fund_code, receivable_id
            order by file_id desc, row_number desc
        ) as occurrence_rank
    from source
),

deduplicated as (
    select * from ranked where occurrence_rank = 1
)

select
    fund_code,
    receivable_id,
    contract_id,
    receivable_type,
    seller_name,
    {{ only_digits('seller_doc') }}     as seller_doc,
    originator_name,
    {{ only_digits('originator_doc') }} as originator_doc,
    debtor_name,
    {{ only_digits('debtor_doc') }}     as debtor_doc,
    case when length({{ only_digits('debtor_doc') }}) = 11
         then 'INDIVIDUAL' else 'COMPANY' end as debtor_type,
    issue_date,
    due_date,
    acquisition_date,
    face_value,
    purchase_price,
    file_id
from deduplicated
