{# 2026-11-04 -> "4 de novembro de 2026". Perguntas citam datas assim, não como 04/11/2026;
   a busca por similaridade precisa ver as mesmas palavras. #}
{% macro mes_por_extenso(col) -%}
(array['janeiro','fevereiro','março','abril','maio','junho','julho','agosto',
       'setembro','outubro','novembro','dezembro'])[extract(month from {{ col }})::int]
{%- endmacro %}

{% macro data_por_extenso(col) -%}
extract(day from {{ col }})::int || ' de ' || {{ mes_por_extenso(col) }} || ' de ' || extract(year from {{ col }})::int
{%- endmacro %}
