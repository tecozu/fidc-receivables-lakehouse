{#
  Brazilian fixed-income convention: rates are annual, compounded over 252
  business days.  price = face / (1 + rate) ^ (business_days / 252)
#}

{# Annual rate implied by buying `face` for `price` with `business_days` to maturity. #}
{% macro implied_annual_rate_252(face, price, business_days) %}
    case
        when {{ business_days }} <= 0 or {{ price }} <= 0 then 0
        else power({{ face }}::double / {{ price }}::double, 252.0 / {{ business_days }}) - 1
    end
{% endmacro %}

{# Present value of `amount` discounted at `annual_rate` for `business_days`.
   Overdue receivables (no business days left) are carried at their outstanding amount;
   their impairment is handled by the loss provision, not by discounting. #}
{% macro present_value_252(amount, annual_rate, business_days) %}
    case
        when {{ business_days }} <= 0 then {{ amount }}::double
        else {{ amount }}::double / power(1 + {{ annual_rate }}::double, {{ business_days }} / 252.0)
    end
{% endmacro %}
