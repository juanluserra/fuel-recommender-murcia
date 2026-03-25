#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.database import Database
from src.model_a import FeatureBuilder, build_default_panel
from src.repo_config import get_scope


def main() -> None:
    parser = argparse.ArgumentParser(description="Construye el dataset analítico del Modelo A.")
    parser.add_argument("--scope-id", default="alcantarilla_murcia")
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--fuel-col", required=True)
    parser.add_argument("--horizon-days", type=int, default=7)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--output", default="data/datasets/model_a_training_frame.csv")
    args = parser.parse_args()

    scope = get_scope(args.scope_id)
    db = Database(db_path=args.db_path or str(scope.resolved_db_path))
    panel = build_default_panel(fuel_col=args.fuel_col, start_date=args.start_date, end_date=args.end_date, db=db)
    builder = FeatureBuilder(fuel_col=args.fuel_col, horizon_days=args.horizon_days)
    frame = builder.build(panel)
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    frame.to_csv(args.output, index=False)
    print(f"Dataset exportado en {args.output} con {len(frame)} filas")


if __name__ == "__main__":
    main()
