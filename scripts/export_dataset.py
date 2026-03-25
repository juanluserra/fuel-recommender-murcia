#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.database import Database
from src.dataset_builder import FuelDatasetBuilder, default_dataset_path
from src.repo_config import get_scope


def main() -> None:
    parser = argparse.ArgumentParser(description="Exporta datasets analíticos desde SQLite.")
    parser.add_argument("--scope-id", default="alcantarilla_murcia")
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--dataset-type", default="panel", choices=["panel", "daily_summary"])
    parser.add_argument("--output", default=None)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--include-full-calendar", action="store_true")
    args = parser.parse_args()

    scope = get_scope(args.scope_id)
    db = Database(db_path=args.db_path or str(scope.resolved_db_path))
    builder = FuelDatasetBuilder(db=db)
    output_path = args.output or default_dataset_path(dataset_type=f"{args.scope_id}_{args.dataset_type}")
    path = builder.export_dataset(
        output_path=output_path,
        dataset_type=args.dataset_type,
        start_date=args.start_date,
        end_date=args.end_date,
        include_full_calendar=args.include_full_calendar,
    )
    print(path)


if __name__ == "__main__":
    main()
