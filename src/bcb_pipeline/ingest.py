"""Ingestão incremental e idempotente para a camada `raw` (bronze).

Regras da camada raw:
- guarda o dado **como veio** da fonte (o HTML do Copom, valores como texto decimal,
  inclusive datas futuras da Selic meta). Limpeza é trabalho do dbt;
- cada linha tem `ingested_at` e `updated_at`, para auditoria;
- **idempotente:** rodar duas vezes seguidas não duplica nada (chave primária + upsert);
- **incremental:** busca só a partir do último dado gravado, menos uma margem
  (`LOOKBACK_DAYS`) para pegar revisões da fonte;
- cada execução fica registrada em `raw.ingestion_runs`.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import date, timedelta

import psycopg

from .bcb_client import BcbClient
from .config import FULL_LOAD_START, LOOKBACK_DAYS, SERIES

logger = logging.getLogger(__name__)

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.sgs_observations (
    series_code  integer      NOT NULL,
    ref_date     date         NOT NULL,
    value        numeric      NOT NULL,
    ingested_at  timestamptz  NOT NULL DEFAULT now(),
    updated_at   timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (series_code, ref_date)
);

CREATE TABLE IF NOT EXISTS raw.copom_statements (
    meeting      integer      PRIMARY KEY,
    ref_date     date         NOT NULL,
    title        text         NOT NULL,
    html         text         NOT NULL,
    ingested_at  timestamptz  NOT NULL DEFAULT now(),
    updated_at   timestamptz  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.ingestion_runs (
    run_id         uuid         NOT NULL,
    source         text         NOT NULL,
    started_at     timestamptz  NOT NULL,
    finished_at    timestamptz,
    window_start   date,
    rows_fetched   integer,
    rows_inserted  integer,
    rows_updated   integer,
    status         text         NOT NULL,
    error          text,
    PRIMARY KEY (run_id, source)
);
"""

# `xmax = 0` só é verdade em linha recém-inserida: assim o mesmo comando diz se inseriu ou
# atualizou. O WHERE evita reescrever (e mudar updated_at de) linhas que não mudaram.
UPSERT_OBS = """
INSERT INTO raw.sgs_observations (series_code, ref_date, value)
VALUES (%s, %s, %s)
ON CONFLICT (series_code, ref_date) DO UPDATE
   SET value = EXCLUDED.value, updated_at = now()
 WHERE raw.sgs_observations.value IS DISTINCT FROM EXCLUDED.value
RETURNING (xmax = 0) AS inserted
"""

UPSERT_COPOM = """
INSERT INTO raw.copom_statements (meeting, ref_date, title, html)
VALUES (%s, %s, %s, %s)
ON CONFLICT (meeting) DO UPDATE
   SET ref_date = EXCLUDED.ref_date, title = EXCLUDED.title, html = EXCLUDED.html,
       updated_at = now()
 WHERE (raw.copom_statements.title, raw.copom_statements.html)
       IS DISTINCT FROM (EXCLUDED.title, EXCLUDED.html)
RETURNING (xmax = 0) AS inserted
"""


@dataclass
class IngestStats:
    source: str
    window_start: date | None = None
    fetched: int = 0
    inserted: int = 0
    updated: int = 0
    extra: dict = field(default_factory=dict)


def ensure_schema(conn: psycopg.Connection) -> None:
    conn.execute(DDL)
    conn.commit()


def _count(cur: psycopg.Cursor, stats: IngestStats) -> None:
    row = cur.fetchone()
    if row is None:  # nada mudou: o WHERE do upsert barrou a escrita
        return
    if row[0]:
        stats.inserted += 1
    else:
        stats.updated += 1


def series_watermark(conn: psycopg.Connection, code: int) -> date | None:
    row = conn.execute(
        "SELECT max(ref_date) FROM raw.sgs_observations WHERE series_code = %s", (code,)
    ).fetchone()
    return row[0] if row else None


def ingest_series(
    conn: psycopg.Connection,
    client: BcbClient,
    code: int,
    today: date | None = None,
    full: bool = False,
) -> IngestStats:
    today = today or date.today()
    mark = None if full else series_watermark(conn, code)
    start = FULL_LOAD_START if mark is None else min(mark, today) - timedelta(days=LOOKBACK_DAYS)
    # Até 60 dias à frente: a Selic meta é publicada até a próxima reunião do Copom.
    end = today + timedelta(days=60)
    stats = IngestStats(source=f"sgs.{code}", window_start=start)
    observations = client.fetch_series(code, start, end)
    stats.fetched = len(observations)
    with conn.cursor() as cur:
        for obs in observations:
            cur.execute(UPSERT_OBS, (obs.series_code, obs.ref_date, obs.value))
            _count(cur, stats)
    return stats


def ingest_copom(
    conn: psycopg.Connection, client: BcbClient, full: bool = False, recheck_last: int = 2
) -> IngestStats:
    """Baixa o texto das reuniões que ainda não estão no banco.

    As `recheck_last` mais recentes são sempre baixadas de novo, porque o site às vezes corrige
    o texto logo depois da publicação.
    """
    stats = IngestStats(source="copom")
    meetings = client.list_copom_meetings()
    known = {r[0] for r in conn.execute("SELECT meeting FROM raw.copom_statements")}
    recent = {m for m, _ in sorted(meetings, reverse=True)[:recheck_last]}
    todo = [m for m, _ in meetings if full or m not in known or m in recent]
    stats.extra["listed"] = len(meetings)
    with conn.cursor() as cur:
        for meeting in sorted(todo):
            st = client.fetch_copom_statement(meeting)
            if st is None:
                continue
            stats.fetched += 1
            cur.execute(UPSERT_COPOM, (st.meeting, st.ref_date, st.title, st.html))
            _count(cur, stats)
    return stats


def _log_run(conn, run_id, stats: IngestStats | None, source, started, status, error=None):
    conn.execute(
        """INSERT INTO raw.ingestion_runs (run_id, source, started_at, finished_at, window_start,
               rows_fetched, rows_inserted, rows_updated, status, error)
           VALUES (%s, %s, %s, now(), %s, %s, %s, %s, %s, %s)""",
        (
            run_id,
            source,
            started,
            stats.window_start if stats else None,
            stats.fetched if stats else None,
            stats.inserted if stats else None,
            stats.updated if stats else None,
            status,
            error,
        ),
    )


def run_ingestion(
    conn: psycopg.Connection,
    client: BcbClient,
    series: list[int] | None = None,
    include_copom: bool = True,
    full: bool = False,
) -> list[IngestStats]:
    """Uma execução completa. Cada fonte é uma transação: se uma falhar, as outras seguem e
    a falha fica registrada em `raw.ingestion_runs` (e a exceção é relançada no fim)."""
    ensure_schema(conn)
    run_id = uuid.uuid4()
    results: list[IngestStats] = []
    failures: list[str] = []
    # `is None`, e não `or`: lista vazia significa "nenhuma série", não "todas".
    codes = list(SERIES) if series is None else series
    jobs = [(f"sgs.{c}", lambda c=c: ingest_series(conn, client, c, full=full)) for c in codes]
    if include_copom:
        jobs.append(("copom", lambda: ingest_copom(conn, client, full=full)))
    for source, job in jobs:
        started = conn.execute("SELECT now()").fetchone()[0]
        try:
            stats = job()
            _log_run(conn, run_id, stats, source, started, "success")
            conn.commit()
            results.append(stats)
            logger.info(
                "%s: %d lidos, %d novos, %d alterados",
                source,
                stats.fetched,
                stats.inserted,
                stats.updated,
            )
        except Exception as exc:  # noqa: BLE001 - registrar e seguir para a próxima fonte
            conn.rollback()
            _log_run(conn, run_id, None, source, started, "failed", repr(exc)[:500])
            conn.commit()
            failures.append(f"{source}: {exc!r}")
            logger.error("%s falhou: %r", source, exc)
    if failures:
        raise RuntimeError("falha na ingestão: " + "; ".join(failures))
    return results
