#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.crawler import GasolinerasCrawler
from src.data_collection import FuelDataCollector
from src.database import Database
from src.repo_config import get_scope


def main() -> None:
    parser = argparse.ArgumentParser(description="Recolecta histórico de precios y lo persiste en SQLite.")
    parser.add_argument("--scope-id", default="alcantarilla_murcia")
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--start-date", required=True, help="Fecha inicial YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="Fecha final YYYY-MM-DD")
    parser.add_argument("--force", action="store_true", help="Reprocesa fechas ya existentes")
    parser.add_argument("--strict-empty", action="store_true", help="Falla si la API no devuelve datos")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))
    scope = get_scope(args.scope_id)
    db = Database(db_path=args.db_path or str(scope.resolved_db_path))
    crawler = GasolinerasCrawler(
        municipality_name=scope.municipality,
        province_name=scope.province,
        municipality_id=scope.municipality_id,
    )
    collector = FuelDataCollector(db=db, crawler=crawler)
    summary = collector.collect_range(
        start_date=args.start_date,
        end_date=args.end_date,
        force=args.force,
        allow_empty_dates=not args.strict_empty,
    )
    print(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
