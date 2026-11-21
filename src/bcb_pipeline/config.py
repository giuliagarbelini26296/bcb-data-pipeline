"""Configuração: séries coletadas e conexão com o banco."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class Series:
    code: int
    name: str
    unit: str
    frequency: str  # "diaria" ou "mensal"


# Códigos do SGS: https://www3.bcb.gov.br/sgspub
SERIES: dict[int, Series] = {
    432: Series(432, "selic_meta", "% a.a.", "diaria"),
    433: Series(433, "ipca_mensal", "% no mês", "mensal"),
    13522: Series(13522, "ipca_12m", "% em 12 meses", "mensal"),
    1: Series(1, "dolar_ptax_venda", "R$/US$", "diaria"),
}

FULL_LOAD_START = date(2000, 1, 1)
# Quantos dias antes do último dado gravado a carga incremental volta a buscar.
# Cobre revisões da fonte e dias que chegaram atrasados.
LOOKBACK_DAYS = 10


def database_url() -> str:
    return os.getenv("DATABASE_URL", "postgresql://bcb:bcb-local-only@127.0.0.1:55434/bcb")
