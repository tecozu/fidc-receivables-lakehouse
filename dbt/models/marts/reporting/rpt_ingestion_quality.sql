-- Data-quality view of the landing zone: what arrived, what was rejected and why.
with rejections as (
    select file_id, reason, count(*) as n
    from {{ source('meta', 'rejected_rows') }}
    group by file_id, reason
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
    cast(l.rejected_count as double) / nullif(l.row_count, 0)   as rejection_rate,
    {{ string_agg_ordered("r.reason || ' (' || cast(r.n as varchar) || ')'", ', ', 'r.n desc') }}
                                                                as rejection_reasons,
    l.ingested_at
from {{ ref('stg_file_ledger') }} l
left join rejections r on r.file_id = l.file_id
group by
    l.file_date, l.fund_code, l.event_type, l.file_name, l.is_latest_version,
    l.row_count, l.accepted_count, l.rejected_count, l.ingested_at
