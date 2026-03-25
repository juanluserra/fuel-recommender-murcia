#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src import config
from src.crawler import GasolinerasCrawler
from src.data_collection import FuelDataCollector
from src.database import Database
from src.history_store import export_history_from_db, import_history_to_db
from src.model_a import FuelDecisionModelA, build_default_panel
from src.publication import build_scope_payload, render_index_html, write_scope_payload
from src.repo_config import get_production_model, get_scope, load_production_models


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger("daily_run")


def _ensure_local_db(scope_id: str) -> tuple[Database, dict]:
    scope = get_scope(scope_id)
    db = Database(db_path=str(scope.resolved_db_path))
    prices_count = 0
    try:
        with db._connect() as conn:
            prices_count = conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
    except Exception:
        prices_count = 0
    if prices_count == 0:
        imported = import_history_to_db(db=db, history_dir=scope.resolved_history_dir)
        logger.info("SQLite reconstruida desde history para %s: %s", scope_id, imported)
    return db, {"scope": scope, "db_path": db.db_path}


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline diario: actualizar datos, entrenar modelo y publicar recomendaciones.")
    parser.add_argument("--scope-id", default="alcantarilla_murcia")
    parser.add_argument("--fuel-col", default=None)
    parser.add_argument("--target-date", default=date.today().isoformat())
    parser.add_argument("--skip-collect", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.getLogger().setLevel(getattr(logging, args.log_level.upper(), logging.INFO))
    db, ctx = _ensure_local_db(args.scope_id)
    scope = ctx["scope"]

    models_cfg = load_production_models()
    scope_models = [m for m in models_cfg.get("models", []) if m.get("scope_id") == args.scope_id]
    if args.fuel_col:
        scope_models = [m for m in scope_models if m.get("fuel_col") == args.fuel_col]
    if not scope_models:
        raise RuntimeError(f"No hay modelos de producción configurados para scope_id={args.scope_id}")

    if not args.skip_collect:
        crawler = GasolinerasCrawler(
            municipality_name=scope.municipality,
            province_name=scope.province,
            municipality_id=scope.municipality_id,
        )
        collector = FuelDataCollector(db=db, crawler=crawler)
        result = collector.collect_date(args.target_date, force=False, allow_empty_dates=True)
        logger.info("Resultado de recolección %s", result)
        export_history_from_db(db=db, history_dir=scope.resolved_history_dir)

    payloads = []
    for model_cfg in scope_models:
        fuel_col = model_cfg["fuel_col"]
        panel = build_default_panel(fuel_col=fuel_col, db=db)
        model = FuelDecisionModelA(
            fuel_col=fuel_col,
            horizon_days=int(model_cfg["horizon_days"]),
            train_end_date=None,
            waiting_cost=float(model_cfg["waiting_cost"]),
        )
        model.fit(panel)

        model_dir = config.MODELS_DIR / scope.scope_id / fuel_col
        model_dir.mkdir(parents=True, exist_ok=True)
        model_path = model_dir / "model_a.joblib"
        model.save(model_path)

        latest = model.predict_latest(panel)
        latest_dir = config.LATEST_DIR
        latest_dir.mkdir(parents=True, exist_ok=True)
        latest_csv_path = latest_dir / f"{scope.scope_id}__{fuel_col}__latest.csv"
        latest.to_csv(latest_csv_path, index=False)

        payload = build_scope_payload(
            scope={
                "scope_id": scope.scope_id,
                "municipality": scope.municipality,
                "province": scope.province,
            },
            model_cfg=model_cfg,
            latest_df=latest,
        )
        payload["artifacts"] = {
            "model_path": str(model_path.relative_to(config.PROJECT_ROOT)),
            "latest_csv": str(latest_csv_path.relative_to(config.PROJECT_ROOT)),
        }
        payloads.append(payload)
        json_path = config.PUBLIC_DIR / "data" / f"{scope.scope_id}__{fuel_col}.json"
        write_scope_payload(json_path, payload)

    # aggregate public index and latest feed
    index_payload = {
        "generated_at": pd.Timestamp.utcnow().isoformat(),
        "items": payloads,
    }
    write_scope_payload(config.PUBLIC_DIR / "data" / "index.json", index_payload)
    (config.PUBLIC_DIR / "index.html").write_text(render_index_html(payloads), encoding="utf-8")

    print(json.dumps({
        "scope_id": args.scope_id,
        "target_date": args.target_date,
        "published_items": len(payloads),
        "public_index": str((config.PUBLIC_DIR / 'index.html').relative_to(config.PROJECT_ROOT)),
        "public_feed": str((config.PUBLIC_DIR / 'data' / 'index.json').relative_to(config.PROJECT_ROOT)),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
