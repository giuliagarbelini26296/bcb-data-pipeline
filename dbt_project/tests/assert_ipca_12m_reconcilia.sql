-- Reconciliação entre duas séries do BC: o IPCA de 12 meses oficial (13522) precisa bater com o
-- acumulado dos 12 IPCAs mensais (433). Tolerância de 0,05 p.p. por causa do arredondamento.
select mes, ipca_12m, ipca_12m_calculado, abs(ipca_12m - ipca_12m_calculado) as diferenca
from {{ ref('fct_indicadores_mensais') }}
where ipca_12m is not null
  and ipca_12m_calculado is not null
  and abs(ipca_12m - ipca_12m_calculado) > 0.05
