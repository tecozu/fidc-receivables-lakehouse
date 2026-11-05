select
    fund_code,
    fund_name,
    provision_policy_id,
    inception_date
from {{ ref('funds') }}
