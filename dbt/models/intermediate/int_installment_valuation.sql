-- Mark-to-model valuation of the open portfolio on var('as_of_date'):
--   1. implied discount rate from the purchase (face vs. price, 252 business-day basis)
--   2. present value of the outstanding amount at that rate
--   3. loss provision by days overdue, using the fund's policy in force on that date
with open_positions as (
    select *
    from {{ ref('int_installment_position') }}
    where lifecycle_status in ('OPEN', 'PARTIALLY_SETTLED')
      and outstanding_amount > 0
),

calendar as (
    select calendar_date, business_day_seq from {{ ref('dim_calendar') }}
),

terms as (
    select
        p.*,
        due.business_day_seq - acq.business_day_seq                  as term_business_days,
        greatest(due.business_day_seq - cur.business_day_seq, 0)     as remaining_business_days,
        greatest({{ days_between('p.due_date', 'p.as_of_date') }}, 0) as days_overdue
    from open_positions p
    join calendar acq on acq.calendar_date = p.acquisition_date
    join calendar due on due.calendar_date = p.due_date
    join calendar cur on cur.calendar_date = p.as_of_date
),

priced as (
    select
        *,
        {{ implied_annual_rate_252('face_value', 'purchase_price', 'term_business_days') }}
            as discount_rate
    from terms
),

valued as (
    select
        *,
        round({{ present_value_252('outstanding_amount', 'discount_rate', 'remaining_business_days') }}, 2)
            as present_value
    from priced
),

with_policy as (
    select
        v.*,
        b.bucket                as provision_bucket,
        b.provision_rate
    from valued v
    join {{ ref('dim_fund') }} f on f.fund_code = v.fund_code
    left join {{ ref('provision_buckets') }} b
        on  b.provision_policy_id = f.provision_policy_id
        and v.days_overdue between b.min_days_overdue and b.max_days_overdue
        and v.as_of_date between b.valid_from and coalesce(b.valid_to, date '9999-12-31')
)

select
    *,
    round(present_value * provision_rate, 2)                         as provision_amount,
    present_value - round(present_value * provision_rate, 2)         as net_value,
    case when days_overdue > 0 then 'OVERDUE' else 'CURRENT' end     as aging_status
from with_policy
