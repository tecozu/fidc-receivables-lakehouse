-- Every company or person the funds deal with, with the roles they play.
-- A counterparty can be a seller in one deal and a debtor in another.
with roles as (
    select seller_doc as doc, seller_name as name, 'SELLER' as role, acquisition_date
    from {{ ref('stg_acquisitions') }}
    union all
    select originator_doc, originator_name, 'ORIGINATOR', acquisition_date
    from {{ ref('stg_acquisitions') }}
    union all
    select debtor_doc, debtor_name, 'DEBTOR', acquisition_date
    from {{ ref('stg_acquisitions') }}
)

select
    doc                                          as counterparty_doc,
    arg_max(name, acquisition_date)              as counterparty_name,  -- most recent name
    case when length(doc) = 11 then 'INDIVIDUAL' else 'COMPANY' end as counterparty_type,
    bool_or(role = 'SELLER')                     as is_seller,
    bool_or(role = 'ORIGINATOR')                 as is_originator,
    bool_or(role = 'DEBTOR')                     as is_debtor,
    min(acquisition_date)                        as first_seen_date
from roles
where doc is not null and doc <> ''
group by doc
