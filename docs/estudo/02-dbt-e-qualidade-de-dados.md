# Etapa 2 — Transformação com dbt e testes de qualidade de dados

A etapa 1 trouxe o dado bruto. Esta etapa transforma esse dado em tabelas confiáveis e, principalmente, **prova** que elas estão certas. Um pipeline sem testes publica erro em silêncio.

## O que é o dbt

O dbt executa **SQL versionado** em camadas, na ordem certa, e testa o resultado:
- cada modelo é um `select` num arquivo `.sql`;
- `{{ ref('outro_modelo') }}` cria a dependência, e o dbt monta o grafo e a ordem de execução;
- testes ficam ao lado dos modelos, em YAML ou SQL;
- `dbt build` roda tudo na ordem: modelo, testes do modelo, modelos seguintes. **Se um teste falha, o que depende dele não é construído.**

## As camadas deste projeto

```
raw (Python)                staging (views)              marts (tabelas)
sgs_observations  ──►  stg_sgs__observations  ──►  int_selic_diaria ──►  fct_copom_decisoes
copom_statements  ──►  stg_copom__statements  ─────────────────────────►      ▲
seeds/series.csv  ──►  (cadastro das séries)          └──────────────►  fct_indicadores_mensais
```

| Modelo | O que faz |
|---|---|
| `stg_sgs__observations` | tipos, nome e unidade de cada série; marca as datas futuras da Selic (`is_projected`) |
| `stg_copom__statements` | HTML para texto (macro `html_to_text`); extrai a taxa escrita no comunicado |
| `int_selic_diaria` | Selic meta por dia, só datas já ocorridas |
| `fct_indicadores_mensais` | um mês por linha: Selic, IPCA, IPCA 12m (oficial e recalculado), dólar, juro real |
| `fct_copom_decisoes` | uma reunião por linha: Selic anterior e nova, variação e decisão (elevou, reduziu, manteve) |

## Tipos de teste usados

| Tipo | Exemplo | Pega o quê |
|---|---|---|
| Genérico pronto | `unique`, `not_null`, `accepted_values`, `relationships` | chave duplicada, campo vazio, categoria inválida, série sem cadastro |
| Genérico próprio | `accepted_range`, `unique_combination`, `no_html_tags` | Selic de 500%, chave composta repetida, sobra de HTML |
| Singular (reconciliação) | `assert_ipca_12m_reconcilia`, `assert_copom_taxa_bate_com_selic` | duas fontes que deveriam concordar e não concordam |
| Completude | `assert_sem_meses_faltando` | mês faltando numa série mensal |
| Teste de unidade do dbt | `decisao_do_copom_vem_da_serie` | lógica do SQL com entrada fictícia, sem tocar nos dados reais |
| Freshness da fonte | `raw.ingestion_runs` | ingestão parada há mais de 2 dias (aviso) ou 7 (erro) |
| Contrato | `fct_indicadores_mensais` | nome ou tipo de coluna mudou sem aviso para quem consome |

Severidade: `error` para o pipeline e bloqueia o que vem depois; `warn` avisa e segue. As linhas reprovadas ficam salvas no schema `dq` (`store_failures`), para investigar com SQL.

## Incremental

`fct_indicadores_mensais` é **incremental**: a primeira execução gravou 323 meses; as seguintes regravam só os **últimos 3 meses** (`INSERT 0 4`), que é onde o IPCA pode ser revisado e o mês corrente ainda está aberto. O cálculo continua usando todo o histórico, porque o IPCA de 12 meses precisa dos 11 meses anteriores. O que o incremental economiza é a escrita.

## O que os testes acharam de verdade

Nenhum desses problemas apareceria olhando a tabela final "por cima":

1. **Sobra de HTML em 120 comunicados.** A limpeza tratava `&nbsp;`, mas o site também usa as formas numéricas `&#160;` (espaço) e `&#58;` (dois-pontos). O teste `no_html_tags` reprovou, e os modelos seguintes foram **pulados** em vez de construídos com texto sujo.
2. **Feriados (o melhor achado).** A reconciliação "taxa do comunicado = Selic no dia seguinte" falhou em 3 reuniões. Elas tinham um feriado no meio: Corpus Christi em 2009, Tiradentes e Sexta-feira Santa em 2011, 7 de Setembro em 2017. A nova taxa vale a partir do próximo **dia útil**. A regra virou "a primeira mudança da série em até 5 dias", e o teste de unidade ganhou um caso com feriado.
3. **Bug na expressão regular.** Na reunião 96, a taxa extraída foi 0,25, que era a **variação** ("em 0,25 p.p."), não a taxa. A regex passou a exigir "% a.a." ou "ao ano".
4. **Comunicado vazio.** A reunião 51 só tem "sem declaração" no site do BC. É dado real, não erro: ficou como **aviso**.
5. **Tipo numérico.** O teste de unidade acusou `1.0` contra `1.000000`. A lógica estava certa; a coluna passou a ter 2 casas decimais.

## Resultados (21/11/2026)

- `dbt build`: **32 nós**: 31 aprovados e 1 aviso (a reunião 51).
- **237 reuniões** classificadas: 90 manteve, 81 reduziu, 66 elevou.
- **192 de 192** taxas escritas nos comunicados batem com a série oficial (depois de tratar os feriados).
- **IPCA 12m:** o recalculado a partir dos meses bate com o oficial em 311 meses, com diferença máxima de 0,01 p.p. (arredondamento).

## Como rodar

```bash
cd dbt_project
dbt build --profiles-dir .            # modelos + testes, na ordem
dbt source freshness --profiles-dir . # a ingestão está em dia?
dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir . --port 8081  # catálogo e grafo
```

## Exercícios
1. Mude a tolerância do `assert_ipca_12m_reconcilia` para 0,001. Quantos meses reprovam? Por quê?
2. Apague a linha da série 1 em `seeds/series.csv` e rode `dbt build` e depois `pytest`. Quem avisa primeiro, e por que o dbt sozinho não avisaria?
3. Crie o modelo `fct_selic_anual` (Selic média e IPCA acumulado por ano) com testes `unique` e `accepted_range`.

## Perguntas de entrevista
- **Como você garante a qualidade dos dados?** Testes em camadas: esquema (unique, not_null), domínio (faixas e valores aceitos), reconciliação entre fontes, completude e freshness, com o pipeline parando antes de publicar.
- **O que é reconciliação?** Conferir o mesmo número por dois caminhos independentes. Aqui: a taxa do comunicado contra a série da Selic e o IPCA de 12 meses oficial contra o recalculado.
- **Error ou warn?** Error quando o dado errado causaria decisão errada; warn quando é uma anomalia conhecida que alguém precisa ver.
- **Para que serve um contrato de dados?** Quem consome a tabela confia nos nomes e tipos das colunas; mudar isso sem aviso quebra dashboards e outros pipelines.
- **Teste de unidade ou teste de dados?** O de unidade testa a **lógica** com entrada inventada; o de dados testa os **dados reais** de cada execução. Um não substitui o outro.
