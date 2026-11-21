# Etapa 1 — Ingestão incremental e idempotente (camada raw)

Primeira peça de qualquer pipeline: trazer o dado da fonte para o banco **sem perder, sem duplicar e sem baixar tudo de novo** a cada execução.

## A fonte

APIs públicas do Banco Central, sem chave:

| Código SGS | Série | Frequência |
|---|---|---|
| 432 | Selic meta (% a.a.) | diária |
| 433 | IPCA no mês (%) | mensal |
| 13522 | IPCA em 12 meses (%) | mensal |
| 1 | Dólar PTAX venda (R$/US$) | diária |

Além disso, o **texto dos comunicados do Copom** (237 reuniões, de 2000 até hoje), que depois vira documento para o RAG.

## Conceitos

### Camadas (arquitetura medalhão)
- **raw / bronze:** o dado **como veio** da fonte. Nada de limpeza aqui; se a regra de limpeza estiver errada, dá para refazer sem baixar de novo.
- **staging / silver:** tipos certos, nomes padronizados, duplicatas e lixo removidos (etapa 2, com dbt).
- **marts / gold:** tabelas prontas para uso: indicadores mensais, decisões do Copom, documentos para o RAG.

### Idempotência
Rodar a mesma carga duas vezes dá o mesmo resultado. Aqui isso vem de:
- **chave primária** `(series_code, ref_date)`;
- **upsert** (`INSERT ... ON CONFLICT DO UPDATE`), que só reescreve a linha se o valor mudou.

Por que importa: pipelines falham no meio e são reexecutados. Sem idempotência, cada nova tentativa duplica dados.

### Carga incremental com marca d'água (watermark)
- **Primeira execução:** carga completa desde 2000 (3,4 min, 17.473 linhas).
- **Depois:** busca só a partir da **última data gravada menos 10 dias** (`LOOKBACK_DAYS`). A margem pega valores que a fonte revisou ou publicou com atraso.
- **Resultado real:** a segunda execução levou **2 s** e gravou **0 linhas novas**.

### Auditoria
Cada linha tem `ingested_at` e `updated_at`, e cada execução vira uma linha em `raw.ingestion_runs`: fonte, janela, linhas lidas, novas, alteradas, status e erro. Isso responde a "quando esse número entrou e de onde veio?".

### Falha isolada
Cada fonte é uma transação. Se a API do dólar cair, as outras séries são gravadas mesmo assim, a falha fica registrada e a execução termina com erro, para o orquestrador avisar.

## Achados reais desta etapa

1. **Duplicatas vindas da própria fonte.** O IPCA retornou 324 linhas, mas só 322 datas diferentes. O SGS limita consultas a 10 anos, então a carga é dividida em janelas. Nas séries mensais, uma janela que começa em 29/12/2009 devolve também 01/12/2009, que já tinha vindo na janela anterior. O upsert já impedia a duplicata no banco; o cliente passou a deduplicar também, e há um teste para isso.
2. **Datas no futuro.** A Selic meta tem 18 linhas com data **depois de hoje**: o BC já publica a taxa vigente até a próxima reunião do Copom. A camada raw guarda como veio; a etapa 2 decide o que fazer com isso.
3. **Bug pego pelos testes:** `series=[]` (nenhuma série) era tratado como "todas", porque em Python `[] or todas` devolve `todas`. Corrigido com `is None`.
4. **Ambiente:** no macOS, arquivos `.pth` com o atributo "oculto" são ignorados pelo Python 3.12, e o pacote instalado em modo editável "sumiu". Resolvido com `chflags nohidden`.

## Como rodar

```bash
docker compose -p bcb-pipeline up -d
python -m bcb_pipeline ingest          # incremental
python -m bcb_pipeline ingest --full   # recarrega tudo
pytest                                 # 11 testes; os de banco usam o bcb_test
```

## Exercícios
1. Rode a ingestão duas vezes e compare `raw.ingestion_runs`. O que muda entre as execuções?
2. Altere à mão um valor do IPCA no banco (`UPDATE raw.sgs_observations ...`) e rode de novo. A linha volta ao valor da fonte? Por quê?
3. Adicione a série 12 (CDI diário) em `config.py`. Quanto tempo leva a primeira carga dela?

## Perguntas de entrevista
- **O que é idempotência num pipeline?** Executar de novo não muda o resultado. Chave natural mais upsert, ou apagar e regravar uma partição inteira.
- **Carga completa ou incremental?** Incremental por marca d'água no dia a dia, com margem para revisões; carga completa para reconstruir ou corrigir.
- **Por que guardar o dado bruto?** Para refazer transformações sem depender da fonte de novo e para auditoria.
- **Como você trata uma API instável?** Retries com backoff só em erros temporários (429 e 5xx), timeout, falha isolada por fonte e registro de cada execução.
