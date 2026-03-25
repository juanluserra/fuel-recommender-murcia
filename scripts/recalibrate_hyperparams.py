#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src import config
from src.calibration import calibrate_model_a, run_rolling_backtest
from src.database import Database
from src.history_store import import_history_to_db
from src.model_a import build_default_panel
from src.repo_config import get_production_model, get_scope, load_search_space


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger("recalibrate_hyperparams")


def _ensure_local_db(scope_id: str) -> Database:
    scope = get_scope(scope_id)
    db = Database(db_path=str(scope.resolved_db_path))
    with db._connect() as conn:
        prices_count = conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
    if prices_count == 0:
        imported = import_history_to_db(db=db, history_dir=scope.resolved_history_dir)
        logger.info("SQLite reconstruida desde history para %s: %s", scope_id, imported)
    return db


def main() -> None:
    parser = argparse.ArgumentParser(description="Recalibra hiperparámetros con split simple y rolling backtest.")
    parser.add_argument("--scope-id", default="alcantarilla_murcia")
    parser.add_argument("--fuel-col", default="precio_gasoleo_a")
    parser.add_argument("--train-end-date", default=None)
    parser.add_argument("--show-progress", action="store_true")
    parser.add_argument("--run-rolling", action="store_true")
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    db = _ensure_local_db(args.scope_id)
    search_space = load_search_space()
    fuel_cfg = search_space.get("fuel_search_spaces", {}).get(args.fuel_col, {})
    horizons = fuel_cfg.get("horizon_candidates", search_space.get("default_horizon_candidates", [3, 5, 7, 10, 14]))
    waiting_costs = fuel_cfg.get("waiting_cost_candidates", search_space.get("default_waiting_cost_candidates", [0.001, 0.002, 0.003, 0.005]))

    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.output_dir or config.EXPERIMENTS_DIR / run_id / args.scope_id / args.fuel_col)
    out_dir.mkdir(parents=True, exist_ok=True)

    panel = build_default_panel(fuel_col=args.fuel_col, db=db)
    calibration = calibrate_model_a(
        panel_df=panel,
        fuel_col=args.fuel_col,
        horizon_candidates=horizons,
        waiting_cost_candidates=waiting_costs,
        train_end_date=args.train_end_date,
        show_progress=args.show_progress,
        continue_on_error=True,
    )
    calibration.results.to_csv(out_dir / "calibration.csv", index=False)
    calibration.failures.to_csv(out_dir / "calibration_failures.csv", index=False)

    rolling_summary_path = None
    if args.run_rolling:
        rolling_cfg = search_space.get("rolling", {})
        rolling_summary, rolling_folds, rolling_failures = run_rolling_backtest(
            panel_df=panel,
            fuel_col=args.fuel_col,
            horizon_candidates=horizons,
            waiting_cost_candidates=waiting_costs,
            min_train_days=int(rolling_cfg.get("min_train_days", 365 * 2)),
            test_window_days=int(rolling_cfg.get("test_window_days", 30)),
            step_days=int(rolling_cfg.get("step_days", 30)),
            max_folds=rolling_cfg.get("max_folds", 12),
            show_progress=args.show_progress,
            continue_on_error=True,
        )
        rolling_summary.to_csv(out_dir / "rolling_summary.csv", index=False)
        rolling_folds.to_csv(out_dir / "rolling_folds.csv", index=False)
        rolling_failures.to_csv(out_dir / "rolling_failures.csv", index=False)
        rolling_summary_path = str(out_dir / "rolling_summary.csv")
    else:
        rolling_summary = None

    # Candidate selection: prefer rolling top row if present, else calibration top row.
    if rolling_summary is not None and not rolling_summary.empty:
        best = rolling_summary.iloc[0].to_dict()
        selection_method = "rolling_backtest"
    elif not calibration.results.empty:
        ranked = calibration.results.sort_values(
            ["avg_policy_realized_saving", "decision_accuracy_vs_oracle", "mae_q50"],
            ascending=[False, False, True],
        ).reset_index(drop=True)
        best = ranked.iloc[0].to_dict()
        selection_method = "temporal_split"
    else:
        raise RuntimeError("No se pudo generar ninguna configuración candidata.")

    production = get_production_model(args.scope_id, args.fuel_col)
    candidate = {
        "scope_id": args.scope_id,
        "fuel_col": args.fuel_col,
        "horizon_days": int(best["horizon_days"]),
        "waiting_cost": float(best["waiting_cost"]),
        "selected_on": datetime.utcnow().date().isoformat(),
        "selection_method": selection_method,
        "based_on_run": run_id,
        "current_production": production,
        "metrics": best,
    }
    candidate_dir = config.CONFIG_DIR / "candidates"
    candidate_dir.mkdir(parents=True, exist_ok=True)
    candidate_path = candidate_dir / f"{args.scope_id}__{args.fuel_col}.json"
    with candidate_path.open("w", encoding="utf-8") as fh:
        json.dump(candidate, fh, ensure_ascii=False, indent=2)

    print(json.dumps({
        "scope_id": args.scope_id,
        "fuel_col": args.fuel_col,
        "output_dir": str(out_dir),
        "calibration_rows": int(len(calibration.results)),
        "rolling_summary": rolling_summary_path,
        "candidate_path": str(candidate_path),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
