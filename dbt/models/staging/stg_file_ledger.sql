-- One row per file version. A file re-sent with the same name but different
-- content supersedes the previous version (is_latest_version = false for the old one).
select
    file_id,
    fund_code,
    event_type,
    file_name,
    file_date,
    row_count,
    accepted_count,
    rejected_count,
    ingested_at,
    row_number() over (
        partition by fund_code, event_type, file_name
        order by file_id desc
    ) = 1 as is_latest_version
from {{ source('meta', 'file_ledger') }}
