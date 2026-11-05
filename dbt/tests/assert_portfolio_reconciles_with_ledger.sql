-- Every snapshot's outstanding amount must equal the event-ledger balance on that
-- date. Catches any drift between the valuation logic and the source of truth.
with ledger as (
    select
        p.as_of_date,
        p.fund_code,
        p.receivable_id,
        p.outstanding_amount,
        sum(e.balance_delta) as ledger_balance
    from {{ ref('fct_daily_portfolio') }} p
    join {{ ref('fct_receivable_events') }} e
        on  e.fund_code = p.fund_code
        and e.receivable_id = p.receivable_id
        and e.event_date <= p.as_of_date
    group by all
)

select *
from ledger
where abs(outstanding_amount - ledger_balance) > 0.01
