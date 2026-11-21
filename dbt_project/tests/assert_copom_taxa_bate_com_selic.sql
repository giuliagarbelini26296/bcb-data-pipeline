-- Reconciliação entre fontes: a taxa escrita no comunicado do Copom precisa ser a mesma que a
-- série oficial da Selic meta registra no dia seguinte à reunião.
select reuniao, data_reuniao, titulo, taxa_no_comunicado, selic_nova
from {{ ref('fct_copom_decisoes') }}
where taxa_no_comunicado is not null
  and selic_nova is not null
  and taxa_no_comunicado <> selic_nova
