-- Silver: séries com nome, unidade e frequência; datas futuras marcadas, não apagadas.
with obs as (
    select * from {{ source('raw', 'sgs_observations') }}
),

series as (
    select * from {{ ref('series') }}
)

select
    obs.series_code,
    series.series_name,
    series.unit,
    series.frequency,
    obs.ref_date,
    obs.value::numeric(18, 6)          as value,
    -- A Selic meta é publicada até a próxima reunião do Copom: valor "vigente", ainda não ocorrido.
    obs.ref_date > current_date        as is_projected,
    obs.updated_at                     as source_updated_at
from obs
inner join series using (series_code)
