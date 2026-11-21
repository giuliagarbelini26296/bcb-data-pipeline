"""Orquestração com Dagster: o pipeline inteiro como um grafo de *assets* (tabelas).

Conceitos (detalhes em docs/estudo/03-orquestracao-dagster.md):

- **Asset:** um dado que o pipeline produz e mantém atualizado (uma tabela, um arquivo). O Dagster
  pensa em "o que precisa existir", e não só em "quais tarefas rodar" (como um cron ou o Airflow
  clássico). Assim ele sabe a linhagem: de onde cada tabela vem e o que depende dela.
- **Asset check:** um teste ligado a um asset. Os testes do dbt viram checks automaticamente.
- **Resource:** dependência externa (banco, cliente HTTP), trocada nos testes por uma falsa.
- **Schedule:** quando rodar. **Sensor:** reage a um evento (aqui, a uma execução que falhou).
- **RetryPolicy:** repete uma etapa que falhou, com espera crescente, antes de desistir.
"""

# Sem `from __future__ import annotations`: o Dagster lê as anotações de tipo em tempo de
# execução (para achar o `context` e os resources) e não entende anotações em forma de texto.

import json
import os
import shutil
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import psycopg
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    AssetSpec,
    Backoff,
    ConfigurableResource,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    MaterializeResult,
    ResourceParam,
    RetryPolicy,
    RunFailureSensorContext,
    ScheduleDefinition,
    asset,
    asset_check,
    define_asset_job,
    multi_asset,
    run_failure_sensor,
)
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from .bcb_client import BcbClient
from .config import SERIES, database_url
from .ingest import run_ingestion
from .rag import export_documents, index_documents

ROOT = Path(__file__).resolve().parents[2]
DBT_DIR = ROOT / "dbt_project"
# dbt instalado junto com este Python (funciona sem ativar o ambiente virtual). Também vai para o
# PATH porque o `dagster dev` chama o `dbt parse` por conta própria, sem usar o DBT_EXE.
_VENV_BIN = str(Path(sys.executable).parent)
if _VENV_BIN not in os.environ.get("PATH", "").split(os.pathsep):
    os.environ["PATH"] = _VENV_BIN + os.pathsep + os.environ.get("PATH", "")
DBT_EXE = shutil.which("dbt", path=_VENV_BIN) or "dbt"
ALERTS_FILE = Path(os.getenv("ALERTS_FILE", ROOT / "logs" / "alerts.jsonl"))

# As tabelas da camada raw. As chaves ["raw", tabela] são as mesmas que o dagster-dbt dá às
# `sources` do dbt: é isso que liga a ingestão em Python aos modelos dbt no mesmo grafo.
RAW_SGS = AssetKey(["raw", "sgs_observations"])
RAW_COPOM = AssetKey(["raw", "copom_statements"])
RAW_RUNS = AssetKey(["raw", "ingestion_runs"])


class PostgresResource(ConfigurableResource):
    database_url: str

    def connect(self) -> psycopg.Connection:
        return psycopg.connect(self.database_url)


# --------------------------------------------------------------------------- ingestão (bronze)
@multi_asset(
    specs=[
        AssetSpec(RAW_SGS, group_name="ingestao", description="Séries do SGS (Selic, IPCA, dólar)"),
        AssetSpec(RAW_COPOM, group_name="ingestao", description="Comunicados do Copom (HTML)"),
        AssetSpec(RAW_RUNS, group_name="ingestao", description="Registro de cada execução"),
    ],
    # API instável? Repete 3 vezes. Backoff exponencial do Dagster: delay × (2^n − 1),
    # ou seja, esperas de 30 s, 90 s e 210 s (30 s e 90 s confirmados no teste de falha).
    retry_policy=RetryPolicy(max_retries=3, delay=30, backoff=Backoff.EXPONENTIAL),
)
def raw_bcb(context: AssetExecutionContext, db: PostgresResource, bcb: ResourceParam[BcbClient]):
    """Carga incremental das APIs do Banco Central para a camada raw."""
    with db.connect() as conn:
        stats = run_ingestion(conn, bcb)
    sgs = [s for s in stats if s.source.startswith("sgs.")]
    copom = next(s for s in stats if s.source == "copom")
    yield MaterializeResult(
        asset_key=RAW_SGS,
        metadata={
            "linhas_lidas": sum(s.fetched for s in sgs),
            "linhas_novas": sum(s.inserted for s in sgs),
            "linhas_alteradas": sum(s.updated for s in sgs),
            "por_serie": {s.source: f"{s.inserted} novas / {s.updated} alteradas" for s in sgs},
        },
    )
    yield MaterializeResult(
        asset_key=RAW_COPOM,
        metadata={
            "reunioes_baixadas": copom.fetched,
            "novas": copom.inserted,
            "alteradas": copom.updated,
        },
    )
    yield MaterializeResult(asset_key=RAW_RUNS, metadata={"fontes": len(stats)})


@asset_check(asset=RAW_SGS, description="Cada série tem dado recente (fonte não parou)")
def series_em_dia(db: PostgresResource) -> AssetCheckResult:
    """A ingestão pode terminar "com sucesso" e mesmo assim a fonte ter parado de publicar.

    Diária: último dado até 5 dias atrás (fim de semana + feriado). Mensal: até 75 dias
    (o IPCA de um mês sai por volta do dia 10 do mês seguinte).
    """
    limits = {"diaria": 5, "mensal": 75}
    today = date.today()
    with db.connect() as conn:
        rows = dict(
            conn.execute(
                "SELECT series_code, max(ref_date) FROM raw.sgs_observations "
                "WHERE ref_date <= current_date GROUP BY 1"
            ).fetchall()
        )
    atrasadas, detalhe = [], {}
    for code, s in SERIES.items():
        last = rows.get(code)
        age = (today - last).days if last else None
        detalhe[s.name] = f"{last} ({age} dias)" if last else "sem dados"
        if age is None or age > limits[s.frequency]:
            atrasadas.append(s.name)
    return AssetCheckResult(
        passed=not atrasadas,
        severity=AssetCheckSeverity.WARN,
        metadata={"ultimo_dado": detalhe, "atrasadas": ", ".join(atrasadas) or "nenhuma"},
    )


# --------------------------------------------------------------------------------- dbt
dbt_project = DbtProject(project_dir=DBT_DIR, profiles_dir=DBT_DIR)
# Em `dagster dev` o manifesto do dbt é gerado na hora; fora dele precisa existir antes.
dbt_project.prepare_if_dev()
if not dbt_project.manifest_path.exists():
    DbtCliResource(project_dir=dbt_project, dbt_executable=DBT_EXE).cli(["parse"]).wait()


class Translator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props: dict) -> str | None:
        # a pasta do modelo vira o grupo no grafo: staging, intermediate, marts
        fqn = dbt_resource_props.get("fqn", [])
        return fqn[1] if len(fqn) > 2 else "staging"


@dbt_assets(manifest=dbt_project.manifest_path, dagster_dbt_translator=Translator())
def bcb_dbt(context: AssetExecutionContext, dbt: DbtCliResource):
    """`dbt build`: modelos e testes na ordem. Teste que falha bloqueia o que vem depois."""
    yield from dbt.cli(["build"], context=context).stream()


# --------------------------------------------------------------------------------- RAG
RAG_DOCS = AssetKey(["marts", "rag_documentos"])
RAG_FILES = AssetKey(["rag", "arquivos_markdown"])
RAG_INDEX = AssetKey(["rag", "indice_pgvector"])


@asset(key=RAG_FILES, deps=[RAG_DOCS], group_name="rag", kinds={"markdown"})
def rag_arquivos(db: PostgresResource) -> MaterializeResult:
    """Um arquivo .md por documento; só reescreve o que mudou."""
    with db.connect() as conn:
        stats = export_documents(conn)
    return MaterializeResult(
        metadata={
            "documentos": stats.total,
            "escritos": stats.written,
            "sem_mudanca": stats.unchanged,
            "removidos": stats.removed,
        }
    )


@asset(
    key=RAG_INDEX,
    deps=[RAG_FILES],
    group_name="rag",
    kinds={"pgvector"},
    # O plano gratuito do Gemini limita embeddings POR MINUTO (429). Cada documento é gravado
    # sozinho, então uma nova tentativa continua de onde parou. Backoff exponencial (60, 180 s)
    # fazia esperar demais e desistir cedo; para cota por minuto, o certo é esperar pouco mais
    # de 1 minuto, com espera FIXA, e tentar mais vezes. Achado na primeira indexação real.
    retry_policy=RetryPolicy(max_retries=8, delay=65),
)
def rag_indice(db: PostgresResource) -> MaterializeResult:
    """Embeddings no pgvector, pelo próprio assistente. Só documentos novos ou alterados."""
    stats = index_documents(database_url=db.database_url)
    return MaterializeResult(
        metadata={
            "arquivos": stats["files"],
            "reindexados": stats["indexed"],
            "sem_mudanca": stats["unchanged"],
            "chunks_novos": stats["chunks"],
            "chunks_no_banco": stats["total_chunks"],
        }
    )


@asset_check(asset=RAG_INDEX, description="Índice e tabela rag_documentos têm os mesmos documentos")
def indice_completo(db: PostgresResource) -> AssetCheckResult:
    """Pega documento que ficou de fora do índice e documento "órfão" (apagado da tabela, mas
    ainda no índice: o assistente não remove fontes que sumiram da pasta)."""
    with db.connect() as conn:
        esperado = {r[0] + ".md" for r in conn.execute("SELECT doc_id FROM marts.rag_documentos")}
        indexado = {r[0] for r in conn.execute("SELECT source FROM public.rag_documents")}
    faltando, orfaos = esperado - indexado, indexado - esperado
    return AssetCheckResult(
        passed=not faltando and not orfaos,
        metadata={
            "documentos": len(esperado),
            "faltando_no_indice": len(faltando),
            "orfaos_no_indice": len(orfaos),
            "exemplos": ", ".join(sorted(faltando | orfaos)[:5]) or "nenhum",
        },
    )


# ------------------------------------------------------------------- agenda e alertas
pipeline_diario = define_asset_job(
    "pipeline_diario",
    selection=AssetSelection.all(),
    description="Ingestão + dbt build, ponta a ponta",
)

agenda_diaria = ScheduleDefinition(
    job=pipeline_diario,
    # Dias úteis às 19h: a PTAX do dia sai por volta das 13h; a Selic muda depois do Copom.
    cron_schedule="0 19 * * 1-5",
    execution_timezone="America/Sao_Paulo",
    default_status=DefaultScheduleStatus.RUNNING,
)


# Sensores começam DESLIGADOS no Dagster. Sem `default_status`, o alerta nunca dispararia
# (achado no teste de falha: o job falhou e nenhum alerta foi gravado).
@run_failure_sensor(
    description="Registra cada execução que falhou em logs/alerts.jsonl",
    default_status=DefaultSensorStatus.RUNNING,
)
def alerta_de_falha(context: RunFailureSensorContext) -> None:
    """Em produção, aqui iria uma mensagem para Slack, Teams ou e-mail. Localmente, um arquivo."""
    ALERTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    event = {
        "quando": datetime.now(UTC).isoformat(timespec="seconds"),
        "job": context.dagster_run.job_name,
        "run_id": context.dagster_run.run_id,
        "erro": context.failure_event.message,
    }
    with ALERTS_FILE.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(event, ensure_ascii=False) + "\n")
    context.log.error("pipeline falhou: %s", event["erro"])


def build_defs(bcb_client: BcbClient | None = None, db_url: str | None = None) -> Definitions:
    return Definitions(
        assets=[raw_bcb, bcb_dbt, rag_arquivos, rag_indice],
        asset_checks=[series_em_dia, indice_completo],
        jobs=[pipeline_diario],
        schedules=[agenda_diaria],
        sensors=[alerta_de_falha],
        resources={
            "db": PostgresResource(database_url=db_url or database_url()),
            "bcb": bcb_client or BcbClient(),
            "dbt": DbtCliResource(project_dir=dbt_project, dbt_executable=DBT_EXE),
        },
    )


defs = build_defs()
