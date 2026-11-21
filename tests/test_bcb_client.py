from datetime import date
from decimal import Decimal

import httpx
import pytest

from bcb_pipeline.bcb_client import BcbClient, _windows


def make_client(handler, **kw) -> BcbClient:
    return BcbClient(
        httpx.Client(transport=httpx.MockTransport(handler)), sleep=lambda s: None, **kw
    )


def test_windows_cover_range_without_overlap():
    ws = _windows(date(2000, 1, 1), date(2026, 11, 21))
    assert ws[0][0] == date(2000, 1, 1) and ws[-1][1] == date(2026, 11, 21)
    for (_, a_end), (b_start, _) in zip(ws, ws[1:], strict=False):
        assert (b_start - a_end).days == 1
    assert all((e - s).days < 3650 for s, e in ws)


def test_fetch_series_parses_and_dedupes_month_overlap():
    def handler(req: httpx.Request) -> httpx.Response:
        # as duas janelas devolvem 01/12/2009, como o SGS faz de verdade
        if req.url.params["dataInicial"] == "01/01/2000":
            return httpx.Response(200, json=[{"data": "01/12/2009", "valor": "0.37"}])
        return httpx.Response(
            200,
            json=[{"data": "01/12/2009", "valor": "0.37"}, {"data": "01/01/2010", "valor": "0.75"}],
        )

    obs = make_client(handler).fetch_series(433, date(2000, 1, 1), date(2012, 1, 1))
    assert [o.ref_date for o in obs] == [date(2009, 12, 1), date(2010, 1, 1)]
    assert obs[1].value == Decimal("0.75")


def test_404_means_empty_window():
    client = make_client(lambda req: httpx.Response(404))
    assert client.fetch_series(1, date(2026, 1, 1), date(2026, 1, 2)) == []


def test_retries_on_429_then_succeeds():
    calls = []

    def handler(req):
        calls.append(1)
        if len(calls) < 3:
            return httpx.Response(429)
        return httpx.Response(200, json=[{"data": "02/01/2026", "valor": "5.5"}])

    obs = make_client(handler).fetch_series(1, date(2026, 1, 1), date(2026, 1, 5))
    assert len(calls) == 3 and len(obs) == 1


def test_gives_up_after_max_retries():
    client = make_client(lambda req: httpx.Response(503), max_retries=2)
    with pytest.raises(httpx.HTTPStatusError):
        client.fetch_series(1, date(2026, 1, 1), date(2026, 1, 5))


def test_copom_statement():
    def handler(req):
        return httpx.Response(
            200,
            json={
                "conteudo": [
                    {
                        "nro_reuniao": 282,
                        "dataReferencia": "2026-11-04",
                        "titulo": "Copom mantém",
                        "textoComunicado": "<p>texto</p>",
                    }
                ]
            },
        )

    st = make_client(handler).fetch_copom_statement(282)
    assert st.meeting == 282 and st.ref_date == date(2026, 11, 4) and st.html == "<p>texto</p>"
