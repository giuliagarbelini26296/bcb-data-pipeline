-- Silver: comunicado do Copom em texto limpo, com a taxa anunciada extraída do texto.
with source as (
    select * from {{ source('raw', 'copom_statements') }}
),

cleaned as (
    select
        meeting,
        ref_date                                                   as meeting_date,
        -- Títulos novos vêm como "282ª reunião - Copom mantém..."; deixamos só a frase.
        trim(regexp_replace(title, '^\s*\d+ª\s*reuni[aã]o\s*-\s*', '', 'i')) as title,
        {{ html_to_text('html') }}                                 as body,
        updated_at                                                 as source_updated_at
    from source
)

select
    *,
    -- Primeira taxa no formato "para 13,75% a.a." ou "em 13,75% ao ano", no título ou no texto.
    -- Exigir "a.a."/"ao ano" evita capturar variações ("elevação em 0,25 p.p."), erro que o
    -- teste de reconciliação achou na reunião 96.
    replace(
        coalesce(
            substring(title from '(?:para|em)\s+(\d{1,2},\d{2})\s*%\s*(?:a\.a\.|ao ano)'),
            substring(body from '(?:para|em)\s+(\d{1,2},\d{2})\s*%\s*(?:a\.a\.|ao ano)')
        ),
        ',', '.'
    )::numeric(6, 2)                                                as announced_rate,
    length(body)                                                    as body_chars
from cleaned
