-- Fund-level NAV components per day: what a fund administrator reports daily.
select
    p.as_of_date,
    p.fund_code,
    f.fund_name,
    count(*)                                                        as open_receivables,
    sum(p.outstanding_amount)                                       as outstanding_amount,
    sum(p.present_value)                                            as present_value,
    sum(p.provision_amount)                                         as provision_amount,
    sum(p.net_value)                                                as net_portfolio_value,
    sum(case when p.aging_status = 'OVERDUE' then p.outstanding_amount else 0 end)
        / nullif(sum(p.outstanding_amount), 0)                      as overdue_ratio,
    sum(p.provision_amount) / nullif(sum(p.present_value), 0)       as provision_coverage,
    sum(p.present_value * p.remaining_business_days)
        / nullif(sum(p.present_value), 0)                           as weighted_avg_term_bd,
    count(distinct p.debtor_doc)                                    as distinct_debtors
from {{ ref('fct_daily_portfolio') }} p
join {{ ref('dim_fund') }} f using (fund_code)
group by all
