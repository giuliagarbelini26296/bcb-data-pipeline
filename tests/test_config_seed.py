"""As séries coletadas em Python (config.py) e o cadastro do dbt (seeds/series.csv) não podem
divergir: série sem cadastro some no `inner join` da staging sem nenhum erro."""

import csv
from pathlib import Path

from bcb_pipeline.config import SERIES

SEED = Path(__file__).parents[1] / "dbt_project" / "seeds" / "series.csv"


def test_config_and_dbt_seed_list_the_same_series():
    with SEED.open(encoding="utf-8") as fh:
        seed = {int(r["series_code"]): r for r in csv.DictReader(fh)}
    assert set(seed) == set(SERIES)
    for code, s in SERIES.items():
        assert (seed[code]["series_name"], seed[code]["frequency"]) == (s.name, s.frequency)
