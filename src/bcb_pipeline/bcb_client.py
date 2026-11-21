"""Cliente das APIs públicas do Banco Central (sem chave).

- SGS (Sistema Gerenciador de Séries Temporais): séries econômicas como Selic, IPCA e dólar.
  `https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=json&dataInicial=...`
- Comunicados do Copom: texto de cada decisão de juros.
  `https://www.bcb.gov.br/api/servico/sitebcb/copom/comunicados...`

Cuidados de uma fonte externa real:
- **Janela máxima:** o SGS limita séries diárias a 10 anos por consulta; a carga completa
  é dividida em janelas.
- **Falhas temporárias:** 429 e 5xx são repetidos com espera crescente (backoff).
- **Sem dados:** o SGS responde 404 quando a janela não tem nenhum valor; isso vira lista vazia.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal

import httpx

logger = logging.getLogger(__name__)

SGS_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados"
COPOM_LIST_URL = "https://www.bcb.gov.br/api/servico/sitebcb/copom/comunicados"
COPOM_DETAIL_URL = "https://www.bcb.gov.br/api/servico/sitebcb/copom/comunicados_detalhes"
MAX_WINDOW_DAYS = 3650  # limite do SGS para séries diárias (10 anos)
RETRY_STATUS = {429, 500, 502, 503, 504}


@dataclass(frozen=True)
class Observation:
    series_code: int
    ref_date: date
    value: Decimal


@dataclass(frozen=True)
class CopomStatement:
    meeting: int
    ref_date: date
    title: str
    html: str


def _windows(start: date, end: date, size: int = MAX_WINDOW_DAYS) -> list[tuple[date, date]]:
    out, cur = [], start
    while cur <= end:
        stop = min(cur + timedelta(days=size - 1), end)
        out.append((cur, stop))
        cur = stop + timedelta(days=1)
    return out


class BcbClient:
    def __init__(
        self,
        client: httpx.Client | None = None,
        max_retries: int = 4,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.http = client or httpx.Client(
            timeout=30, headers={"Accept": "application/json", "User-Agent": "bcb-data-pipeline"}
        )
        self.max_retries = max_retries
        self.sleep = sleep

    def _get(self, url: str, params: dict) -> httpx.Response:
        for attempt in range(self.max_retries + 1):
            try:
                resp = self.http.get(url, params=params)
            except httpx.TransportError as exc:  # timeout, conexão recusada
                if attempt == self.max_retries:
                    raise
                logger.warning("erro de rede (%s), tentativa %d", exc, attempt + 1)
            else:
                if resp.status_code not in RETRY_STATUS or attempt == self.max_retries:
                    return resp
                logger.warning("HTTP %d em %s, tentativa %d", resp.status_code, url, attempt + 1)
            self.sleep(min(2**attempt, 30))
        raise RuntimeError("inalcançável")

    def fetch_series(self, code: int, start: date, end: date) -> list[Observation]:
        """Valores da série entre `start` e `end` (inclusive), em janelas de até 10 anos."""
        # Dicionário por data: em séries mensais o SGS devolve o mês que "encosta" no início da
        # janela (01/12/2009 aparece na janela que começa em 29/12/2009), e a mesma data viria
        # duas vezes. Achado na primeira carga real; o upsert no banco também protege.
        out: dict[date, Observation] = {}
        for w_start, w_end in _windows(start, end):
            resp = self._get(
                SGS_URL.format(code=code),
                {
                    "formato": "json",
                    "dataInicial": w_start.strftime("%d/%m/%Y"),
                    "dataFinal": w_end.strftime("%d/%m/%Y"),
                },
            )
            if resp.status_code == 404:  # janela sem dados
                continue
            resp.raise_for_status()
            for row in resp.json():
                ref = datetime.strptime(row["data"], "%d/%m/%Y").date()
                out[ref] = Observation(series_code=code, ref_date=ref, value=Decimal(row["valor"]))
        return sorted(out.values(), key=lambda o: o.ref_date)

    def list_copom_meetings(self, limit: int = 500) -> list[tuple[int, date]]:
        resp = self._get(COPOM_LIST_URL, {"quantidade": limit})
        resp.raise_for_status()
        return [
            (int(item["nro_reuniao"]), date.fromisoformat(item["dataReferencia"][:10]))
            for item in resp.json()["conteudo"]
        ]

    def fetch_copom_statement(self, meeting: int) -> CopomStatement | None:
        resp = self._get(COPOM_DETAIL_URL, {"nro_reuniao": meeting})
        resp.raise_for_status()
        items = resp.json().get("conteudo") or []
        if not items:
            return None
        item = items[0]
        return CopomStatement(
            meeting=int(item["nro_reuniao"]),
            ref_date=date.fromisoformat(item["dataReferencia"][:10]),
            title=item.get("titulo") or "",
            html=item.get("textoComunicado") or "",
        )
