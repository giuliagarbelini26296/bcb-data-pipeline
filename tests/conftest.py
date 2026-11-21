"""Fixtures compartilhadas: banco de teste e cliente falso do Banco Central.

Os testes de banco usam `bcb_test` (separado dos dados do pipeline). Sem esse banco acessível,
eles são pulados.
"""

import os
from datetime import date
from decimal import Decimal

import psycopg
import pytest

from bcb_pipeline.bcb_client import Observation

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
