-- One row per file version. A file re-sent with the same name but different
-- content supersedes the previous version (is_latest_version = false for the old one).
-- file_id is a hash of the content, so it says nothing about age: versions are ordered
-- by when the file arrived (ingested_at), with file_id only to break an exact tie.
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
        order by ingested_at desc, file_id desc
    ) = 1 as is_latest_version
from {{ source('meta', 'file_ledger') }}
