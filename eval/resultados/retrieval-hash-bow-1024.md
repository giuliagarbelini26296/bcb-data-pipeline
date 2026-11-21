# Avaliação: retrieval

Gerado em 2026-11-21T14:42:09+00:00 · configuração: `embedder=hash-bow-1024`, `store=pgvector`, `k=4`

## Resumo

| métrica | valor |
|---|---|
| n | 10 |
| hit_at_1 | 0.8 |
| hit_at_4 | 0.9 |
| recall_at_4 | 0.9 |
| top_score | 0.3709 |
| mrr | 0.85 |
| mean_top_score_unanswerable | 0.3221 |

## Por categoria

| categoria | n | hit_at_1 | hit_at_4 | recall_at_4 | reciprocal_rank | top_score |
|---|---|---|---|---|---|---|
| direta | 4 | 0.75 | 1.0 | 1.0 | 0.875 | 0.4092 |
| multi-fonte | 1 | 1.0 | 1.0 | 1.0 | 1.0 | 0.3268 |
| parafrase | 5 | 0.8 | 0.8 | 0.8 | 0.8 | 0.3491 |

## Itens para olhar

- **copom-277** (parafrase): Em que reunião de 2026 o Banco Central começou a baixar os juros, e para quanto? — recuperado: ['copom/2010-09-01-reuniao-153.md', 'indicadores/2015-indicadores-mensais.md', 'indicadores/2026-indicadores-mensais.md', 'copom/2026-09-16-reuniao-281.md']
- **ind-2026-08** (direta): Qual foi o IPCA de agosto de 2026? — recuperado: ['copom/2026-08-05-reuniao-280.md', 'indicadores/2026-indicadores-mensais.md']
