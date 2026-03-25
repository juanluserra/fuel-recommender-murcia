#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.database import Database
from src.model_a import FuelDecisionModelA, build_default_panel
from src.repo_config import get_scope


def main() -> None:
    parser = argparse.ArgumentParser(description="Entrena el Modelo A (régimen + cuantiles + política).")
    parser.add_argument("--scope-id", default="alcantarilla_murcia")
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--fuel-col", required=True, help="Columna de combustible, p.ej. precio_gasoleo_a")
    parser.add_argument("--horizon-days", type=int, default=7)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--train-end-date")
    parser.add_argument("--waiting-cost", type=float, default=0.003)
    parser.add_argument("--output-model", default="models/model_a.joblib")
    parser.add_argument("--output-latest", default="data/datasets/model_a_latest_predictions.csv")
    args = parser.parse_args()

    scope = get_scope(args.scope_id)
    db = Database(db_path=args.db_path or str(scope.resolved_db_path))
    panel = build_default_panel(fuel_col=args.fuel_col, start_date=args.start_date, end_date=args.end_date, db=db)
    model = FuelDecisionModelA(
        fuel_col=args.fuel_col,
        horizon_days=args.horizon_days,
        train_end_date=args.train_end_date,
        waiting_cost=args.waiting_cost,
    )
    model.fit(panel)
    model.save(args.output_model)

    latest = model.predict_latest(panel)
    os.makedirs(os.path.dirname(args.output_latest), exist_ok=True)
    latest.to_csv(args.output_latest, index=False)

    print(json.dumps(model.backtest_.to_dict(), indent=2, ensure_ascii=False))
    print(f"Modelo guardado en: {args.output_model}")
    print(f"Predicciones latest guardadas en: {args.output_latest}")


if __name__ == "__main__":
    main()
