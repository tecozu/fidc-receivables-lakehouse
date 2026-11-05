select
    fund_code,
    receivable_id,
    contract_id,
    receivable_type,
    seller_doc,
    originator_doc,
    debtor_doc,
    issue_date,
    due_date,
    acquisition_date,
    face_value,
    purchase_price
from {{ ref('stg_acquisitions') }}
