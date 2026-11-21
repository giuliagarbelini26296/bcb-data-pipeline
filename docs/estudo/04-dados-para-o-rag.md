# Etapa 4 — A camada final alimenta o RAG

Aqui o pipeline de dados encontra a engenharia de IA. As tabelas limpas e testadas viram documentos, que viram embeddings no pgvector, que o assistente (repositório `personal-ai-assistant-rag-mcp`) usa para responder perguntas sobre juros e inflação.

```
raw ──► staging ──► marts ──► rag_documentos (dbt) ──► arquivos .md ──► embeddings no pgvector ──► assistente
                                                        (só o que mudou)    (só o que mudou)
```

## O que foi construído

| Peça | O que faz |
|---|---|
| `marts.rag_documentos` (dbt) | 264 documentos: 237 reuniões do Copom (decisão, contexto econômico e comunicado) e 27 anos de indicadores mensais em tabela |
| `rag/arquivos_markdown` (Dagster) | grava um `.md` por documento; só reescreve o que mudou e apaga o que saiu da tabela |
| `rag/indice_pgvector` (Dagster) | chama o assistente (`python -m assistant ingest`) para gerar embeddings só dos documentos novos ou alterados |
| check `indice_completo` | o índice e a tabela têm exatamente os mesmos documentos (nada faltando, nada órfão) |
| `eval/golden_bcb.jsonl` | 12 perguntas de referência sobre os dados do BC, no formato da avaliação do assistente |

Os dois projetos combinam só o **formato** (pasta de Markdown e tabelas `rag_*` no Postgres). Nenhum importa o código do outro.

## Por que o dado estruturado entra no documento

O texto do comunicado nem sempre diz a taxa: os comunicados antigos são só uma "Nota à Imprensa". A **decisão** vem da tabela `fct_copom_decisoes`, que já foi testada e reconciliada na etapa 2. O documento junta as duas coisas:

```
# Copom: 282ª reunião, 4 de novembro de 2026 (04/11/2026)

Decisão: manteve a Selic em 13,75% ao ano (variação de 0,00 ponto percentual).
Contexto: IPCA acumulado em 12 meses de 4,63% (dado de 10/2026) e juro real de 8,72% ao ano.

## Comunicado
...
```

O modelo recebe o número **certo e conferido**, sem precisar deduzi-lo do texto.

## O experimento que mais ensinou

A primeira avaliação da busca acertou o documento em 1º lugar em só **40%** das perguntas. Com as notas pessoais, o número era 81%.

**Diagnóstico:**
- os 237 comunicados usam o mesmo vocabulário ("inflação", "atividade", "expectativas"); o que separa um do outro é a **data**;
- a data estava no **nome do arquivo**, mas o nome não entra no embedding: ajuda o modelo a ler, não ajuda a busca a encontrar;
- no texto, a data só aparecia no 1º pedaço de 120 palavras, e em número (04/11/2026), enquanto a pergunta diz "novembro de 2026".

**Correções, medidas uma de cada vez** (embedder local, 10 perguntas com resposta, 21/11/2026):

| Configuração | hit@1 | hit@4 | MRR |
|---|---|---|---|
| Original (data só no nome do arquivo) | 0,40 | 0,60 | 0,48 |
| + data por extenso no texto ("4 de novembro de 2026") | 0,50 | 0,80 | 0,63 |
| + título repetido no início de cada pedaço | **0,80** | **0,90** | **0,85** |

A segunda correção é a técnica de **cabeçalho contextual** (*contextual chunk headers*). Ela virou a opção `CHUNK_TITLE_PREFIX` no assistente, desligada por padrão, para não mudar os resultados já publicados dele.

**A pergunta que ainda erra:** "Em que reunião de 2026 o Banco Central **começou** a baixar os juros?". Para responder, é preciso **comparar reuniões entre si**. Isso é consulta (SQL na `fct_copom_decisoes`), não busca por texto parecido. A próxima evolução natural é uma ferramenta de consulta estruturada para o agente.

## Resposta real do assistente

> **Pergunta:** O que o Copom decidiu em 4 de novembro de 2026 e por quê?
>
> **Resposta (Gemini, resumida):** manteve a Selic em 13,75% a.a. por convergência da inflação à meta, incertezas maiores, expectativas de inflação mais altas e desaceleração da atividade [fonte: copom/2026-11-04-reuniao-282.md].

Na pergunta sem resposta ("decisão de dezembro de 2026"), ele disse que os registros vão só até novembro de 2026, em vez de inventar.

## Problemas reais desta etapa

1. **Cota diária do Gemini.** O plano gratuito permite **1.000 embeddings por dia**, e cada pedaço conta como um. O corpus tem 1.036 pedaços: uma reindexação completa já passa do limite. Antes disso, apareceu o limite **por minuto**, que o backoff exponencial (60 s, 180 s) não resolvia; passou a ser espera **fixa** de 65 s, com até 8 tentativas. Com o progresso salvo documento por documento, cada tentativa continuava de onde a anterior parou. Decisão: o padrão do pipeline é o embedder local, grátis e sem limite, e o Gemini é opcional (`RAG_EMBEDDING_PROVIDER=gemini`).
2. **A avaliação do assistente indexava as notas de exemplo antes de avaliar.** Rodando contra o banco do pipeline, ela começou a misturar notas pessoais no índice do BC; nada entrou só porque a cota estourou na primeira nota. O assistente ganhou `--skip-ingest` e `--corpus`.
3. **Avaliar um índice vazio não dá erro, dá zero.** Um erro meu de shell deixou dois índices vazios e a avaliação "funcionou", com todas as métricas em 0. Uma métrica zerada precisa ser investigada, não aceita.
4. **Nome do embedder:** usei `hashing` como padrão, mas o assistente chama de `hash`. O erro apareceu na primeira execução, não em produção.

## Números

- Pipeline completo (ingestão, dbt, documentos e índice): **23 s**; a segunda execução leva **17 s** e reindexa 0 documentos.
- Indexação local completa: **2,8 s** (264 documentos, 1.036 pedaços); a segunda vez leva **0,3 s**.
- Testes: **21** no pipeline e **69** no assistente; `dbt build`: 38 aprovados e 1 aviso conhecido.

## Como rodar

```bash
export DAGSTER_HOME=$PWD/.dagster_home PYTHONPATH=src
dagster job execute -m bcb_pipeline.orchestration -j pipeline_diario

# perguntar ao assistente usando o índice do pipeline
cd ../personal-ai-assistant-rag-mcp
VECTOR_STORE=pgvector EMBEDDING_PROVIDER=hash LLM_PROVIDER=gemini \
DATABASE_URL=postgresql://bcb:bcb-local-only@127.0.0.1:55434/bcb \
python -m assistant ask "O que o Copom decidiu em novembro de 2026?"

# avaliar a busca sem reindexar
python -m assistant.evals retrieval --skip-ingest --golden ../bcb-data-pipeline/eval/golden_bcb.jsonl
```

## Exercícios
1. Rode a avaliação com `CHUNK_TITLE_PREFIX=false` e confira o 0,50 de hit@1 da tabela.
2. Quando a cota do Gemini zerar, indexe com `RAG_EMBEDDING_PROVIDER=gemini` num banco separado e compare com o embedder local. A diferença é maior ou menor que nas notas pessoais (0,81 contra 0,95)? Por quê?
3. Desafio: crie a ferramenta `consultar_decisoes(ano)` no agente, que lê a `fct_copom_decisoes`, e veja se a pergunta "quando começou a baixar os juros" passa a acertar.

## Perguntas de entrevista
- **Qual a relação entre engenharia de dados e RAG?** O RAG só é tão bom quanto os documentos: limpeza, deduplicação, metadados, atualização incremental e testes de qualidade vêm antes do embedding.
- **Como manter um índice vetorial atualizado?** Hash do conteúdo por documento, para reindexar só o que mudou; remover o que saiu da fonte; e um check que compara o índice com a fonte.
- **O que é contextual chunk header?** Repetir o título (ou um resumo) do documento em cada pedaço, para que trechos parecidos de documentos diferentes não se confundam na busca.
- **Quando busca vetorial não basta?** Em perguntas de agregação, comparação ou ordem ("a primeira vez que...", "quantas vezes..."): aí entra SQL ou uma ferramenta estruturada.
- **Como lidar com limite de API em embeddings?** Processar e gravar em pequenas partes (o progresso não se perde), esperar de acordo com o tipo de limite (por minuto ou por dia) e ter um embedder alternativo.
