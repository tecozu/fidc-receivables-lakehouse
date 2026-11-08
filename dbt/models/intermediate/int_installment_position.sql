-- Position of every receivable as of var('as_of_date'): how much is still owed
-- and its lifecycle status. Built from the event ledger, so any past date can be
-- reconstructed exactly (point-in-time correctness for backfills).
{% set as_of = var('as_of_date') %}

with events as (
    select *
    from {{ ref('fct_installment_events') }}
    where event_date <= date '{{ as_of }}'
),

per_receivable as (
    select
        fund_code,
        installment_id,
        sum(balance_delta)                                   as ledger_balance,
        sum(case when event_type in ('FULL', 'PARTIAL') then cash_flow else 0 end)
                                                             as amount_paid_to_date,
        bool_or(event_type = 'FULL')                         as has_full_settlement,
        bool_or(event_type = 'CANCELLATION')                 as is_cancelled,
        bool_or(event_type = 'PARTIAL')                      as has_partial_settlement
    from events
    group by fund_code, installment_id
    having bool_or(event_type = 'ACQUISITION')
)

select
    date '{{ as_of }}'                         as as_of_date,
    r.*,
    p.amount_paid_to_date,
    greatest(p.ledger_balance, 0)              as outstanding_amount,
    case
        when p.is_cancelled          then 'CANCELLED'
        when p.has_full_settlement   then 'SETTLED'
        when p.has_partial_settlement then 'PARTIALLY_SETTLED'
        else 'OPEN'
    end                                        as lifecycle_status
from per_receivable p
join {{ ref('dim_installment') }} r
    on  r.fund_code = p.fund_code
    and r.installment_id = p.installment_id
