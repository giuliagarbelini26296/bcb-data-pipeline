"""CLI: `python -m bcb_pipeline ingest [--full] [--series 432,433] [--no-copom]`."""

from __future__ import annotations

import argparse
import logging

import psycopg

from .bcb_client import BcbClient
from .config import database_url
from .ingest import run_ingestion


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="bcb_pipeline")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("ingest", help="Carga incremental das APIs do Banco Central")
    p.add_argument("--full", action="store_true", help="recarrega tudo desde 2000")
    p.add_argument("--series", help="códigos do SGS separados por vírgula")
    p.add_argument("--no-copom", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    series = [int(s) for s in args.series.split(",")] if args.series else None
    with psycopg.connect(database_url()) as conn:
        stats = run_ingestion(
            conn, BcbClient(), series=series, include_copom=not args.no_copom, full=args.full
        )
    total = sum(s.inserted for s in stats), sum(s.updated for s in stats)
    print(f"ok: {total[0]} linhas novas, {total[1]} alteradas")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
