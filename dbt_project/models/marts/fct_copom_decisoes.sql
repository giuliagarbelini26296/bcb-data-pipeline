-- Gold: uma reunião do Copom por linha, com a decisão derivada da série oficial da Selic.
--
-- A decisão vem da série (fonte de verdade), não do título: títulos antigos são só
-- "Nota à Imprensa - 46ª Reunião". A taxa escrita no comunicado é usada para conferir
-- (teste assert_copom_taxa_bate_com_selic).
with reunioes as (
    select * from {{ ref('stg_copom__statements') }}
),

selic as (
    select * from {{ ref('int_selic_diaria') }}
),

com_taxas as (
    select
        r.meeting                       as reuniao,
        r.meeting_date                  as data_reuniao,
        r.title                         as titulo,
        antes.selic_meta                as selic_anterior,
        -- A nova meta vale a partir do próximo DIA ÚTIL. Com feriado no meio (Corpus Christi em
        -- 2009, Tiradentes + Sexta-feira Santa em 2011, 7 de Setembro em 2017), a mudança aparece
        -- 2 a 4 dias depois. Regra: a primeira mudança em até 5 dias; se não houver, manteve.
        coalesce(mudanca.selic_meta, antes.selic_meta) as selic_nova,
        r.announced_rate                as taxa_no_comunicado
    from reunioes r
    left join selic antes on antes.ref_date = r.meeting_date
    left join lateral (
        select s.selic_meta
        from selic s
        where s.ref_date between r.meeting_date + 1 and r.meeting_date + 5
          and s.selic_meta <> antes.selic_meta
        order by s.ref_date
        limit 1
    ) mudanca on true
)

select
    *,
    (selic_nova - selic_anterior)::numeric(5, 2) as variacao_pp,
    case
        when selic_nova is null or selic_anterior is null then 'sem_dado'
        when selic_nova > selic_anterior then 'elevou'
        when selic_nova < selic_anterior then 'reduziu'
        else 'manteve'
    end                                 as decisao
from com_taxas
