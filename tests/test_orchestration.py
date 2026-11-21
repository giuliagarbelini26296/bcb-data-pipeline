"""Testes da orquestração: definições, linhagem, asset de ingestão e check de atualização.

Nada aqui acessa a internet: o cliente do Banco Central é falso.
"""

from datetime import date, timedelta

from conftest import URL, FakeClient
from dagster import AssetKey, materialize

from bcb_pipeline.orchestration import (
    RAW_COPOM,
    RAW_SGS,
    PostgresResource,
    agenda_diaria,
    defs,
    raw_bcb,
    series_em_dia,
)


def test_definitions_load_with_job_schedule_and_sensor():
    assert defs.resolve_job_def("pipeline_diario") is not None
    assert agenda_diaria.cron_schedule == "0 19 * * 1-5"
    assert agenda_diaria.execution_timezone == "America/Sao_Paulo"
    assert defs.resolve_sensor_def("alerta_de_falha") is not None


def test_lineage_connects_python_ingestion_to_dbt_models():
    graph = defs.resolve_asset_graph()
    stg_sgs = AssetKey(["staging", "stg_sgs__observations"])
    stg_copom = AssetKey(["staging", "stg_copom__statements"])
    assert RAW_SGS in graph.get(stg_sgs).parent_keys
    assert RAW_COPOM in graph.get(stg_copom).parent_keys
    # a decisão do Copom depende das duas fontes (texto e série), passando pelas camadas
    upstream, todo = set(), [AssetKey(["marts", "fct_copom_decisoes"])]
    while todo:
        for parent in graph.get(todo.pop()).parent_keys:
            if parent not in upstream:
                upstream.add(parent)
                todo.append(parent)
    assert {RAW_SGS, RAW_COPOM} <= upstream


def test_dbt_tests_become_asset_checks():
    checks = {k.name for k in defs.resolve_asset_graph().asset_check_keys}
    assert "assert_copom_taxa_bate_com_selic" in checks
    assert "series_em_dia" in checks
    assert "indice_completo" in checks


def test_rag_assets_come_after_the_dbt_documents():
    graph = defs.resolve_asset_graph()
    files, index = AssetKey(["rag", "arquivos_markdown"]), AssetKey(["rag", "indice_pgvector"])
    assert AssetKey(["marts", "rag_documentos"]) in graph.get(files).parent_keys
    assert files in graph.get(index).parent_keys


def test_ingestion_asset_materializes_with_fake_client(conn):
    today = date.today()
    client = FakeClient({1: {today - timedelta(days=1): "5.80"}})
    result = materialize(
        [raw_bcb],
        resources={"db": PostgresResource(database_url=URL), "bcb": client},
    )
    assert result.success
    meta = {
        m.asset_key: m.metadata
        for m in (
            e.event_specific_data.materialization for e in result.get_asset_materialization_events()
        )
    }
    assert meta[RAW_SGS]["linhas_novas"].value == 1


def test_freshness_check_flags_stale_series(conn):
    today = date.today()
    fresh = {
        1: {today - timedelta(days=1): "5.8"},
        432: {today - timedelta(days=1): "13.75"},
        433: {today - timedelta(days=40): "0.1"},
        13522: {today - timedelta(days=40): "4.6"},
    }
    db = PostgresResource(database_url=URL)
    materialize([raw_bcb], resources={"db": db, "bcb": FakeClient(fresh)})
    assert series_em_dia(db=db).passed

    # dólar parado há 20 dias: a ingestão "funciona", mas a fonte parou
    conn.execute("DELETE FROM raw.sgs_observations WHERE series_code = 1")
    conn.execute(
        "INSERT INTO raw.sgs_observations (series_code, ref_date, value) VALUES (1, %s, 5.5)",
        (today - timedelta(days=20),),
    )
    conn.commit()
    result = series_em_dia(db=db)
    assert not result.passed
    assert result.metadata["atrasadas"].value == "dolar_ptax_venda"
