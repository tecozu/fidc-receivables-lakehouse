with latest_files as (
    select file_id from {{ ref('stg_file_ledger') }} where is_latest_version
),

source as (
    select a.*
    from {{ source('raw', 'acquisitions') }} a
    join latest_files using (file_id)
),

-- The same receivable can appear in more than one file (e.g. a corrected re-send
-- under a new name). The most recently ingested occurrence wins.
deduplicated as (
    select *
    from source
    qualify row_number() over (
        partition by fund_code, receivable_id
        order by file_id desc, row_number desc
    ) = 1
)

select
    fund_code,
    receivable_id,
    contract_id,
    receivable_type,
    seller_name,
    regexp_replace(seller_doc, '\D', '', 'g')     as seller_doc,
    originator_name,
    regexp_replace(originator_doc, '\D', '', 'g') as originator_doc,
    debtor_name,
    regexp_replace(debtor_doc, '\D', '', 'g')     as debtor_doc,
    case when length(regexp_replace(debtor_doc, '\D', '', 'g')) = 11
         then 'INDIVIDUAL' else 'COMPANY' end    as debtor_type,
    issue_date,
    due_date,
    acquisition_date,
    face_value,
    purchase_price,
    file_id
from deduplicated
