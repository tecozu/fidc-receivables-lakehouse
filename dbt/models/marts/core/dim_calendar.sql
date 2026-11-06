-- Day-level calendar with a running business-day counter.
-- Business days between two dates = business_day_seq(end) - business_day_seq(start),
-- which turns "count business days" into a cheap join instead of a correlated subquery.
with days as (
    {{ date_series('2020-01-01', '2036-01-01') }}
),

flagged as (
    select
        d.calendar_date,
        not {{ is_weekend('d.calendar_date') }} and h.holiday_date is null as is_business_day,
        h.holiday_name
    from days d
    left join {{ ref('b3_holidays') }} h on h.holiday_date = d.calendar_date
)

select
    calendar_date,
    year(calendar_date)                  as year,
    month(calendar_date)                 as month,
    {{ day_name('calendar_date') }}      as day_name,
    is_business_day,
    holiday_name,
    sum(case when is_business_day then 1 else 0 end)
        over (order by calendar_date rows between unbounded preceding and current row)
        as business_day_seq
from flagged
