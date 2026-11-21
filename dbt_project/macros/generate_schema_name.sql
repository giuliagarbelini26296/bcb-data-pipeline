{# Usa o nome da camada direto (staging, marts), sem o prefixo padrão do dbt (analytics_staging). #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name | trim if custom_schema_name else target.schema }}
{%- endmacro %}
