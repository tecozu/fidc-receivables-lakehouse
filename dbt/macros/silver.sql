{#
  The silver and meta tables written by the ingestion.

  Locally the runner writes one table of each kind (silver.acquisitions, meta.file_ledger...).
  On Databricks the pipeline writes one table per fund (alpha_acquisitions, beta_acquisitions...)
  so that a fund's files, size and failures never touch another fund's. Models read them
  through this macro and see a single relation either way.

  The funds come from var('funds'), set by the runner from funds.yaml. They are not read
  from the warehouse, so the project still compiles offline (CI does that).
#}
{% macro silver_table(source_name, table) %}
    {{ return(adapter.dispatch('silver_table')(source_name, table)) }}
{% endmacro %}

{% macro default__silver_table(source_name, table) -%}
    {{ source(source_name, table) }}
{%- endmacro %}

{% macro databricks__silver_table(source_name, table) -%}
    {%- set schema = var('silver_schema') if source_name == 'silver' else var('meta_schema') -%}
    {%- set selects = [] -%}
    {%- for fund_code in var('funds') -%}
        {%- set relation = api.Relation.create(
                database=target.catalog,
                schema=schema,
                identifier=(fund_code | lower) ~ '_' ~ table) -%}
        {%- do selects.append('select * from ' ~ relation) -%}
    {%- endfor -%}
    ({{ selects | join(' union all ') }})
{%- endmacro %}
