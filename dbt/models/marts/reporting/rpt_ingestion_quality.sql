-- Data-quality view of the landing zone: what arrived, what was rejected and why.
with rejections as (
    select file_id, reason, count(*) as n
    from {{ source('meta', 'rejected_rows') }}
    group by all
)

select
    l.file_date,
    l.fund_code,
    l.event_type,
    l.file_name,
    l.is_latest_version,
    l.row_count,
    l.accepted_count,
    l.rejected_count,
    l.rejected_count / nullif(l.row_count, 0)                   as rejection_rate,
    string_agg(r.reason || ' (' || r.n || ')', ', ' order by r.n desc) as rejection_reasons,
    l.ingested_at
from {{ ref('stg_file_ledger') }} l
left join rejections r using (file_id)
group by all
