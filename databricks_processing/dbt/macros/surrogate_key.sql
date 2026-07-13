{% macro surrogate_key(field_list) -%}
    {# Sinh surrogate key md5 (deterministic, ổn định giữa các run) từ list field. #}
    md5(concat_ws('||',
        {%- for f in field_list -%}
            coalesce(cast({{ f }} as string), '')
            {%- if not loop.last %}, {% endif -%}
        {%- endfor -%}
    ))
{%- endmacro %}
