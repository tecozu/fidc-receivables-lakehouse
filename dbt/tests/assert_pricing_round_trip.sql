-- On the acquisition date, discounting the face value at the implied rate over the
-- full term must give back the purchase price (validates both pricing macros).
select *
from {{ ref('fct_daily_portfolio') }}
where as_of_date = acquisition_date
  and lifecycle_status = 'OPEN'
  and abs(
        round({{ present_value_252('face_value', 'discount_rate', 'term_business_days') }}, 2)
        - purchase_price
      ) > 0.02
