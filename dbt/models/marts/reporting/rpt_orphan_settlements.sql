-- Payments received for receivables the fund does not hold (typically because the
-- acquisition row was rejected at ingestion). These need operational follow-up.
select s.*
from {{ ref('stg_settlements') }} s
left join {{ ref('stg_acquisitions') }} a
    on  a.fund_code = s.fund_code
    and a.receivable_id = s.receivable_id
where a.receivable_id is null
   or s.settlement_date < a.acquisition_date
