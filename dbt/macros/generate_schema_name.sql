{# Use the configured schema name as-is (staging, marts...) instead of prefixing it
   with the target schema (main_staging, main_marts...). On Athena every schema is a
   Glue database in a shared account catalog, so there they get the project prefix
   (fidc_staging, fidc_marts...). #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif target.type == 'athena' -%}
        {{ target.schema }}_{{ custom_schema_name }}
    {%- else -%}
        {{ custom_schema_name }}
    {%- endif -%}
{%- endmacro %}
