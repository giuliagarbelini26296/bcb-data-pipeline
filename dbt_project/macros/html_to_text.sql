{#
  Converte o HTML do site do BC em texto: quebra de parágrafo vira linha em branco, tags somem,
  entidades comuns (nomeadas e numéricas) são decodificadas e espaços repetidos (inclusive o espaço de largura zero,
  U+200B, que aparece no início de vários comunicados) são removidos.
#}
{% macro html_to_text(col) -%}
trim(
  regexp_replace(
    regexp_replace(
      replace(replace(replace(replace(replace(replace(replace(replace(replace(
        regexp_replace(
          regexp_replace({{ col }}, '</p>|<br\s*/?>|</li>|</h\d>', E'\n\n', 'gi'),
          '<[^>]+>', ' ', 'g'),
        '&nbsp;', ' '), '&amp;', '&'), '&quot;', '"'), '&#39;', ''''),
        '&lt;', '<'), '&gt;', '>'), U&'\200B', ''),
        -- entidades numéricas achadas pelo teste no_html_tags: &#160; (espaço) e &#58; (dois-pontos)
        '&#160;', ' '), '&#58;', ':'),
      '[ \t\r ]+', ' ', 'g'),
    '\s*\n\s*\n\s*', E'\n\n', 'g')
)
{%- endmacro %}
