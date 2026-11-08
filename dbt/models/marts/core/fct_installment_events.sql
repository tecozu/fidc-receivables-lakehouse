-- Event ledger: the full life of every installment as an append-only list of events.
-- balance_delta is the change in amount still owed to the fund, so the outstanding
-- balance at any date is simply sum(balance_delta) up to that date.
with acquisitions as (
    select
        {{ surrogate_key(['fund_code', 'installment_id', "'ACQUISITION'"]) }} as event_id,
        fund_code,
        installment_id,
        acquisition_date       as event_date,
        'ACQUISITION'          as event_type,
        face_value             as balance_delta,
        -purchase_price        as cash_flow,      -- cash paid by the fund
        file_id
    from {{ ref('stg_acquisitions') }}
),

settlements as (
    select
        {{ surrogate_key(['s.fund_code', 's.transaction_id']) }} as event_id,
        s.fund_code,
        s.installment_id,
        s.settlement_date      as event_date,
        s.settlement_type      as event_type,
        -s.amount_paid         as balance_delta,
        s.amount_paid          as cash_flow,      -- cash received by the fund
        s.file_id
    from {{ ref('stg_settlements') }} s
    -- settlements for installments that never made it into the fund (e.g. their
    -- acquisition row was rejected) are excluded here and reported in rpt_orphan_settlements
    join {{ ref('stg_acquisitions') }} a
        on  a.fund_code = s.fund_code
        and a.installment_id = s.installment_id
    where s.settlement_date >= a.acquisition_date
)

select * from acquisitions
union all
select * from settlements
