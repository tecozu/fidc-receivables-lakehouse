{#
  Brazilian fixed-income convention: rates are annual, compounded over 252
  business days.  price = face / (1 + rate) ^ (business_days / 252)

  Everything is cast to double on purpose: `252.0` is a DECIMAL literal on several
  engines, and decimal division could silently round the exponent.
#}

{# Annual rate implied by buying `face` for `price` with `business_days` to maturity. #}
{% macro implied_annual_rate_252(face, price, business_days) %}
    case
        when {{ business_days }} <= 0 or {{ price }} <= 0 then cast(0 as double)
        else power(
            cast({{ face }} as double) / cast({{ price }} as double),
            cast(252 as double) / cast({{ business_days }} as double)
        ) - 1
    end
{% endmacro %}

{# Present value of `amount` discounted at `annual_rate` for `business_days`.
   Overdue receivables (no business days left) are carried at their outstanding amount;
   their impairment is handled by the loss provision, not by discounting. #}
{% macro present_value_252(amount, annual_rate, business_days) %}
    case
        when {{ business_days }} <= 0 then cast({{ amount }} as double)
        else cast({{ amount }} as double) / power(
            1 + cast({{ annual_rate }} as double),
            cast({{ business_days }} as double) / cast(252 as double)
        )
    end
{% endmacro %}
