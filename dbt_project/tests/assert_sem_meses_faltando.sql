-- Completude: nenhuma lacuna nas séries mensais entre o primeiro e o último mês publicado.
with meses as (
    select series_name, ref_date,
           lag(ref_date) over (partition by series_name order by ref_date) as anterior
    from {{ ref('stg_sgs__observations') }}
    where frequency = 'mensal'
)
select series_name, anterior, ref_date
from meses
where anterior is not null
  and ref_date <> (anterior + interval '1 month')::date
