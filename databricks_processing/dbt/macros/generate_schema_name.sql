{% macro generate_schema_name(custom_schema_name, node) -%}
    {# Trả về tên schema custom (không prefix target.schema)
       để có đúng workspace.bronze_cosmetics / silver_cosmetics / gold_cosmetics. #}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
