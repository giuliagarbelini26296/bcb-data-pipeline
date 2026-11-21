"""Testes da ingestão contra um Postgres real (banco `bcb_test`, separado dos dados do pipeline).

Sem `TEST_DATABASE_URL` acessível, os testes são pulados.
"""

import os
from datetime import date, timedelta
from decimal import Decimal

import psycopg
import pytest

from bcb_pipeline.bcb_client import CopomStatement, Observation
from bcb_pipeline.config import LOOKBACK_DAYS
from bcb_pipeline.ingest import ingest_series, run_ingestion

URL = os.getenv("TEST_DATABASE_URL", "postgresql://bcb:bcb-local-only@127.0.0.1:55434/bcb_test")


@pytest.fixture
def conn():
    try:
        c = psycopg.connect(URL, connect_timeout=3)
    except psycopg.OperationalError:
        pytest.skip("Postgres de teste indisponível")
    c.execute("DROP SCHEMA IF EXISTS raw CASCADE")
    c.commit()
    yield c
    c.close()


class FakeClient:
    """Imita o BcbClient e registra as janelas pedidas."""

    def __init__(self, data: dict[int, dict[date, str]], copom=None, fail=()):
        self.data, self.copom, self.fail, self.calls = data, copom or {}, set(fail), []

    def fetch_series(self, code, start, end):
        self.calls.append((code, start, end))
        if code in self.fail:
            raise RuntimeError("fonte fora do ar")
        return [
            Observation(code, d, Decimal(v))
            for d, v in sorted(self.data.get(code, {}).items())
            if start <= d <= end
        ]

    def list_copom_meetings(self, limit=500):
        return [(m, s.ref_date) for m, s in self.copom.items()]

    def fetch_copom_statement(self, meeting):
        return self.copom.get(meeting)


TODAY = date(2026, 11, 21)


def test_full_load_then_rerun_is_idempotent(conn):
    data = {1: {date(2026, 11, 18): "5.81", date(2026, 11, 19): "5.89"}}
    client = FakeClient(data)
    first = run_ingestion(conn, client, series=[1], include_copom=False)
    second = run_ingestion(conn, client, series=[1], include_copom=False)
    assert (first[0].inserted, second[0].inserted, second[0].updated) == (2, 0, 0)
    assert conn.execute("SELECT count(*) FROM raw.sgs_observations").fetchone()[0] == 2


def test_incremental_starts_at_watermark_minus_lookback(conn):
    data = {1: {date(2026, 11, 19): "5.89"}}
    client = FakeClient(data)
    run_ingestion(conn, client, series=[1], include_copom=False)
    ingest_series(conn, client, 1, today=TODAY)
    _, start, _ = client.calls[-1]
    assert start == date(2026, 11, 19) - timedelta(days=LOOKBACK_DAYS)


def test_revised_value_is_updated_not_duplicated(conn):
    data = {433: {date(2026, 10, 1): "0.40"}}
    client = FakeClient(data)
    run_ingestion(conn, client, series=[433], include_copom=False)
    data[433][date(2026, 10, 1)] = "0.42"  # a fonte revisou o número
    stats = run_ingestion(conn, client, series=[433], include_copom=False)
    assert (stats[0].inserted, stats[0].updated) == (0, 1)
    value = conn.execute("SELECT value FROM raw.sgs_observations").fetchone()[0]
    assert value == Decimal("0.42")


def test_one_failing_source_does_not_block_others_and_is_logged(conn):
    client = FakeClient({1: {date(2026, 11, 19): "5.89"}}, fail={432})
    with pytest.raises(RuntimeError, match="sgs.432"):
        run_ingestion(conn, client, series=[432, 1], include_copom=False)
    assert conn.execute("SELECT count(*) FROM raw.sgs_observations").fetchone()[0] == 1
    runs = dict(conn.execute("SELECT source, status FROM raw.ingestion_runs").fetchall())
    assert runs == {"sgs.432": "failed", "sgs.1": "success"}


def test_copom_only_new_meetings_plus_recent_recheck(conn):
    copom = {
        m: CopomStatement(m, date(2026, 1, 1) + timedelta(days=m), f"reunião {m}", f"<p>{m}</p>")
        for m in (280, 281, 282)
    }
    client = FakeClient({}, copom=copom)
    run_ingestion(conn, client, series=[], include_copom=True)
    copom[282] = CopomStatement(282, copom[282].ref_date, "reunião 282", "<p>corrigido</p>")
    stats = run_ingestion(conn, client, series=[], include_copom=True)
    # só as 2 mais recentes são baixadas de novo; uma delas mudou
    assert (stats[0].fetched, stats[0].updated) == (2, 1)
