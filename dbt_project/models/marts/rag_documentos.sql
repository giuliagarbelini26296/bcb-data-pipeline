-- Gold para o RAG: cada linha vira um arquivo Markdown que o assistente indexa no pgvector.
--
-- Dois tipos de documento:
--   * um por reunião do Copom: a decisão (dado estruturado, da fct_copom_decisoes), o contexto
--     econômico do mês e o texto do comunicado;
--   * um por ano: a tabela mensal de Selic, IPCA, dólar e juro real.
--
-- O `doc_id` vira o caminho do arquivo e carrega a data (copom/2026-11-04-reuniao-282).
-- Mas o nome do arquivo NÃO entra no embedding: a 1ª avaliação mostrou que a busca não achava
-- "novembro de 2026". Por isso o título traz a data por extenso, e o assistente roda com
-- CHUNK_TITLE_PREFIX=true (o título vai no início de cada pedaço de 120 palavras).
-- O content_hash permite reindexar só o que mudou.

{% set pct = "replace(to_char({}, 'FM990.00'), '.', ',')" %}

with decisoes as (
    select * from {{ ref('fct_copom_decisoes') }}
),

comunicados as (
    select meeting, body from {{ ref('stg_copom__statements') }}
),

indicadores as (
    select * from {{ ref('fct_indicadores_mensais') }}
),

copom as (
    select
        'copom/' || to_char(d.data_reuniao, 'YYYY-MM-DD') || '-reuniao-' || d.reuniao  as doc_id,
        'copom'                                                                        as tipo,
        d.data_reuniao                                                                 as data_ref,
        concat_ws(E'\n',
            '# Copom: ' || d.reuniao || 'ª reunião, ' || {{ data_por_extenso('d.data_reuniao') }}
                || ' (' || to_char(d.data_reuniao, 'DD/MM/YYYY') || ')',
            '',
            'Decisão: ' || case d.decisao
                when 'elevou'  then 'elevou a Selic de ' || {{ pct.format('d.selic_anterior') }} || '% para ' || {{ pct.format('d.selic_nova') }} || '% ao ano'
                when 'reduziu' then 'reduziu a Selic de ' || {{ pct.format('d.selic_anterior') }} || '% para ' || {{ pct.format('d.selic_nova') }} || '% ao ano'
                else 'manteve a Selic em ' || {{ pct.format('d.selic_nova') }} || '% ao ano'
            end || ' (variação de ' || {{ pct.format('d.variacao_pp') }} || ' ponto percentual).',
            case when ctx.ipca_12m is not null then
                'Contexto: IPCA acumulado em 12 meses de ' || {{ pct.format('ctx.ipca_12m') }}
                || '% (dado de ' || to_char(ctx.mes, 'MM/YYYY') || ') e juro real de '
                || {{ pct.format('ctx.juro_real_12m') }} || '% ao ano.'
            end,
            '',
            '## Comunicado',
            '',
            c.body
        )                                                                              as conteudo
    from decisoes d
    inner join comunicados c on c.meeting = d.reuniao
    -- último mês com IPCA publicado antes da reunião (o IPCA do mês sai no mês seguinte)
    left join lateral (
        select i.mes, i.ipca_12m, i.juro_real_12m
        from indicadores i
        where i.mes < date_trunc('month', d.data_reuniao) and i.ipca_12m is not null
        order by i.mes desc
        limit 1
    ) ctx on true
    where d.decisao <> 'sem_dado'
),

anos as (
    select
        'indicadores/' || extract(year from mes)::int || '-indicadores-mensais'       as doc_id,
        'indicadores'                                                                  as tipo,
        make_date(extract(year from mes)::int, 1, 1)                                   as data_ref,
        '# Indicadores econômicos do Brasil em ' || extract(year from mes)::int
        || E'\n\nFonte: Banco Central do Brasil (séries SGS 432, 433, 13522 e 1). '
        || E'Selic meta no fim do mês, IPCA do mês e acumulado em 12 meses, dólar PTAX médio e juro real.\n\n'
        || E'| Mês | Selic (% a.a.) | IPCA no mês (%) | IPCA 12 meses (%) | Dólar médio (R$) | Juro real (% a.a.) |\n'
        || E'|---|---|---|---|---|---|\n'
        || string_agg(
            '| ' || {{ mes_por_extenso('mes') }} || ' de ' || extract(year from mes)::int
            || ' (' || to_char(mes, 'MM/YYYY') || ')'
            || ' | ' || coalesce({{ pct.format('selic_meta_fim_mes') }}, '-')
            || ' | ' || coalesce({{ pct.format('ipca_mes') }}, '-')
            || ' | ' || coalesce({{ pct.format('ipca_12m') }}, '-')
            || ' | ' || coalesce(replace(to_char(dolar_medio, 'FM990.0000'), '.', ','), '-')
            || ' | ' || coalesce({{ pct.format('juro_real_12m') }}, '-') || ' |',
            E'\n' order by mes
        )                                                                              as conteudo
    from indicadores
    group by extract(year from mes)
),

todos as (
    select * from copom
    union all
    select * from anos
)

select
    doc_id,
    tipo,
    data_ref,
    conteudo,
    md5(conteudo)       as content_hash,
    length(conteudo)    as caracteres
from todos
