{{
  config(
    materialized='incremental',
    unique_key='mes',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
  )
}}
-- Gold: um mês por linha, com Selic, IPCA, dólar e juro real.
--
-- Incremental: só os últimos 3 meses são regravados a cada execução (revisões do IPCA e o mês
-- corrente ainda aberto). O cálculo usa todo o histórico, porque o IPCA em 12 meses depende dos
-- 11 meses anteriores; o que o incremental economiza é a escrita.

with obs as (
    select * from {{ ref('stg_sgs__observations') }} where not is_projected
),

meses as (
    select generate_series(
        date_trunc('month', min(ref_date)),
        date_trunc('month', max(ref_date)),
        interval '1 month'
    )::date as mes
    from obs
),

mensal as (
    select
        date_trunc('month', ref_date)::date as mes,
        max(value) filter (where series_name = 'ipca_mensal')          as ipca_mes,
        max(value) filter (where series_name = 'ipca_12m')             as ipca_12m,
        avg(value) filter (where series_name = 'dolar_ptax_venda')     as dolar_medio,
        (array_agg(value order by ref_date desc)
            filter (where series_name = 'dolar_ptax_venda'))[1]        as dolar_fim_mes,
        (array_agg(value order by ref_date desc)
            filter (where series_name = 'selic_meta'))[1]              as selic_meta_fim_mes
    from obs
    group by 1
),

calculado as (
    select
        meses.mes,
        mensal.selic_meta_fim_mes,
        mensal.ipca_mes,
        mensal.ipca_12m,
        -- IPCA de 12 meses recalculado a partir dos meses: (1+i1)*(1+i2)*...*(1+i12) - 1.
        -- Serve para conferir a série oficial (teste assert_ipca_12m_reconcilia).
        case when count(mensal.ipca_mes) over w12 = 12
             then round(((exp(sum(ln(1 + mensal.ipca_mes / 100)) over w12) - 1) * 100)::numeric, 2)
        end                                                            as ipca_12m_calculado,
        round(mensal.dolar_medio, 4)                                   as dolar_medio,
        mensal.dolar_fim_mes,
        -- Juro real ex-post (fórmula de Fisher): quanto a Selic rendeu acima da inflação.
        round(((1 + mensal.selic_meta_fim_mes / 100) / (1 + mensal.ipca_12m / 100) - 1) * 100, 2)
                                                                       as juro_real_12m
    from meses
    left join mensal using (mes)
    window w12 as (order by meses.mes rows between 11 preceding and current row)
)

select
    mes::date                              as mes,
    selic_meta_fim_mes::numeric(8, 4)      as selic_meta_fim_mes,
    ipca_mes::numeric(8, 4)                as ipca_mes,
    ipca_12m::numeric(8, 4)                as ipca_12m,
    ipca_12m_calculado::numeric(8, 4)      as ipca_12m_calculado,
    dolar_medio::numeric(10, 4)            as dolar_medio,
    dolar_fim_mes::numeric(10, 4)          as dolar_fim_mes,
    juro_real_12m::numeric(8, 4)           as juro_real_12m,
    now()::timestamptz                     as atualizado_em
from calculado
{% if is_incremental() %}
where mes >= (select max(mes) - interval '3 months' from {{ this }})
{% endif %}
