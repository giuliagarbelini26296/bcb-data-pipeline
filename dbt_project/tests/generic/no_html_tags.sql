{# Reprova textos que ainda têm tag HTML ou entidade (&nbsp;) depois da limpeza. #}
{% test no_html_tags(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} ~ '<[a-zA-Z/][^>]*>|&[a-z]+;|&#\d+;'
{% endtest %}
