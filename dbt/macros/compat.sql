{#
  Cross-engine helpers. The same models run on DuckDB (local dev and CI) and on
  Amazon Athena (Trino, production). Only the few constructs whose syntax differs
  live here; everything else is plain ANSI SQL that both engines accept.
  Each macro dispatches to `<adapter>__<name>`, with the DuckDB form as default.
#}

{# Keep only the digits of a taxpayer ID (CPF/CNPJ). #}
{% macro only_digits(expr) %}{{ return(adapter.dispatch('only_digits')(expr)) }}{% endmacro %}
{% macro default__only_digits(expr) %}regexp_replace({{ expr }}, '\D', '', 'g'){% endmacro %}
{% macro athena__only_digits(expr) %}regexp_replace({{ expr }}, '\D', ''){% endmacro %}

{# Deterministic surrogate key (lower-case hex MD5 of the '|'-joined columns). #}
{% macro surrogate_key(columns) %}{{ return(adapter.dispatch('surrogate_key')(columns)) }}{% endmacro %}
{% macro default__surrogate_key(columns) -%}
    md5(concat_ws('|', {% for c in columns %}cast({{ c }} as varchar){% if not loop.last %}, {% endif %}{% endfor %}))
{%- endmacro %}
{% macro athena__surrogate_key(columns) -%}
    lower(to_hex(md5(to_utf8(concat_ws('|', {% for c in columns %}cast({{ c }} as varchar){% if not loop.last %}, {% endif %}{% endfor %})))))
{%- endmacro %}

{# One row per calendar day in [start_date, end_date), as column `calendar_date`. #}
{% macro date_series(start_date, end_date) %}{{ return(adapter.dispatch('date_series')(start_date, end_date)) }}{% endmacro %}
{% macro default__date_series(start_date, end_date) -%}
    select cast(range as date) as calendar_date
    from range(date '{{ start_date }}', date '{{ end_date }}', interval 1 day)
{%- endmacro %}
{% macro athena__date_series(start_date, end_date) -%}
    select calendar_date
    from unnest(sequence(date '{{ start_date }}', date '{{ end_date }}' - interval '1' day, interval '1' day))
        as t(calendar_date)
{%- endmacro %}

{# Saturday or Sunday. DuckDB numbers days 0 (Sun) to 6 (Sat); Trino uses ISO 1 (Mon) to 7 (Sun). #}
{% macro is_weekend(expr) %}{{ return(adapter.dispatch('is_weekend')(expr)) }}{% endmacro %}
{% macro default__is_weekend(expr) %}(dayofweek({{ expr }}) in (0, 6)){% endmacro %}
{% macro athena__is_weekend(expr) %}(day_of_week({{ expr }}) in (6, 7)){% endmacro %}

{% macro day_name(expr) %}{{ return(adapter.dispatch('day_name')(expr)) }}{% endmacro %}
{% macro default__day_name(expr) %}dayname({{ expr }}){% endmacro %}
{% macro athena__day_name(expr) %}date_format({{ expr }}, '%W'){% endmacro %}

{# Concatenate `expr` over a group, ordered by `order_by`. #}
{% macro string_agg_ordered(expr, sep, order_by) %}{{ return(adapter.dispatch('string_agg_ordered')(expr, sep, order_by)) }}{% endmacro %}
{% macro default__string_agg_ordered(expr, sep, order_by) %}string_agg({{ expr }}, '{{ sep }}' order by {{ order_by }}){% endmacro %}
{% macro athena__string_agg_ordered(expr, sep, order_by) %}array_join(array_agg({{ expr }} order by {{ order_by }}), '{{ sep }}'){% endmacro %}

{#
  Pre-hook of the daily snapshot on Athena. DuckDB gets `delete+insert` natively; on
  an Iceberg table we reproduce it as "delete the reference date, then append", so a
  rerun of a date replaces its snapshot (rows that are no longer open disappear too).
#}
{% macro delete_reference_date() %}
    {%- if target.type == 'athena' and load_relation(this) is not none -%}
        delete from {{ this }} where as_of_date = date '{{ var("as_of_date") }}'
    {%- endif -%}
{% endmacro %}
