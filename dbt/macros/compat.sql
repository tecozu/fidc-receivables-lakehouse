{#
  Cross-engine helpers. The same models run on DuckDB (local dev and CI) and on
  Databricks SQL (production). Only the few constructs whose syntax differs live
  here; everything else is SQL that both engines accept.
  Each macro dispatches to `<adapter>__<name>`, with the DuckDB form as default.
#}

{# Keep only the digits of a taxpayer ID (CPF/CNPJ). #}
{% macro only_digits(expr) %}{{ return(adapter.dispatch('only_digits')(expr)) }}{% endmacro %}
{% macro default__only_digits(expr) %}regexp_replace({{ expr }}, '[^0-9]', '', 'g'){% endmacro %}
{% macro databricks__only_digits(expr) %}regexp_replace({{ expr }}, '[^0-9]', ''){% endmacro %}

{# Deterministic surrogate key: MD5 of the '|'-joined columns. Same syntax on both engines. #}
{% macro surrogate_key(columns) -%}
    md5(concat_ws('|', {% for c in columns %}cast({{ c }} as string){% if not loop.last %}, {% endif %}{% endfor %}))
{%- endmacro %}

{# One row per calendar day in [start_date, end_date), as column `calendar_date`. #}
{% macro date_series(start_date, end_date) %}{{ return(adapter.dispatch('date_series')(start_date, end_date)) }}{% endmacro %}
{% macro default__date_series(start_date, end_date) -%}
    select cast(range as date) as calendar_date
    from range(date '{{ start_date }}', date '{{ end_date }}', interval 1 day)
{%- endmacro %}
{% macro databricks__date_series(start_date, end_date) -%}
    select explode(sequence(date '{{ start_date }}', date_sub(date '{{ end_date }}', 1), interval 1 day)) as calendar_date
{%- endmacro %}

{# Saturday or Sunday. DuckDB numbers days 0 (Sun) to 6 (Sat); Spark uses 1 (Sun) to 7 (Sat). #}
{% macro is_weekend(expr) %}{{ return(adapter.dispatch('is_weekend')(expr)) }}{% endmacro %}
{% macro default__is_weekend(expr) %}(dayofweek({{ expr }}) in (0, 6)){% endmacro %}
{% macro databricks__is_weekend(expr) %}(dayofweek({{ expr }}) in (1, 7)){% endmacro %}

{% macro day_name(expr) %}{{ return(adapter.dispatch('day_name')(expr)) }}{% endmacro %}
{% macro default__day_name(expr) %}dayname({{ expr }}){% endmacro %}
{% macro databricks__day_name(expr) %}date_format({{ expr }}, 'EEEE'){% endmacro %}

{# Calendar days from `start` to `end` (negative if `end` comes first). #}
{% macro days_between(start, end) %}{{ return(adapter.dispatch('days_between')(start, end)) }}{% endmacro %}
{% macro default__days_between(start, end) %}date_diff('day', {{ start }}, {{ end }}){% endmacro %}
{% macro databricks__days_between(start, end) %}datediff({{ end }}, {{ start }}){% endmacro %}

{# Concatenate `expr` over a group, ordered by `order_by`. #}
{% macro string_agg_ordered(expr, sep, order_by) %}{{ return(adapter.dispatch('string_agg_ordered')(expr, sep, order_by)) }}{% endmacro %}
{% macro default__string_agg_ordered(expr, sep, order_by) %}string_agg({{ expr }}, '{{ sep }}' order by {{ order_by }}){% endmacro %}
{% macro databricks__string_agg_ordered(expr, sep, order_by) %}listagg({{ expr }}, '{{ sep }}') within group (order by {{ order_by }}){% endmacro %}
