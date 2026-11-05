-- Completeness: no business day may be missing between the first and last
-- snapshot of each fund (a gap means a failed or skipped daily run).
with bounds as (
    select fund_code, min(as_of_date) as first_day, max(as_of_date) as last_day
    from {{ ref('rpt_fund_daily_summary') }}
    group by fund_code
),

expected as (
    select b.fund_code, c.calendar_date as as_of_date
    from bounds b
    join {{ ref('dim_calendar') }} c
        on c.calendar_date between b.first_day and b.last_day
       and c.is_business_day
)

select e.*
from expected e
left join {{ ref('rpt_fund_daily_summary') }} s using (fund_code, as_of_date)
where s.as_of_date is null
