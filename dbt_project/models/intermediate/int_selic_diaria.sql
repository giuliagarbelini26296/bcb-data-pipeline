-- Selic meta por dia de calendário, só datas já ocorridas.
select ref_date, value as selic_meta
from {{ ref('stg_sgs__observations') }}
where series_name = 'selic_meta'
  and not is_projected
