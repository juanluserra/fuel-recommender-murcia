#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src import config
from src.repo_config import load_production_models, load_promotion_policy


def main() -> None:
    parser = argparse.ArgumentParser(description="Promueve una configuración candidata a producción si supera la política mínima.")
    parser.add_argument("--candidate", required=True, help="Ruta al JSON de candidato")
    parser.add_argument("--write", action="store_true", help="Sobrescribe production_models.json")
    args = parser.parse_args()

    candidate = json.loads(Path(args.candidate).read_text(encoding="utf-8"))
    payload = load_production_models()
    policy = load_promotion_policy().get("promote_if", {})

    current = candidate["current_production"]
    metrics = candidate["metrics"]
    current_ref = {
        "horizon_days": current["horizon_days"],
        "waiting_cost": current["waiting_cost"],
    }

    gain = metrics.get("avg_policy_realized_saving")
    acc = metrics.get("decision_accuracy_vs_oracle")
    share_wait = metrics.get("share_wait")

    reasons = []
    if share_wait is not None:
        min_wait, max_wait = policy.get("share_wait_between", [0.0, 1.0])
        if not (min_wait <= share_wait <= max_wait):
            reasons.append(f"share_wait fuera de rango: {share_wait}")
    if gain is None:
        reasons.append("candidate sin avg_policy_realized_saving")
    if acc is None:
        reasons.append("candidate sin decision_accuracy_vs_oracle")

    approved = not reasons
    if approved and args.write:
        for item in payload.get("models", []):
            if item.get("scope_id") == candidate["scope_id"] and item.get("fuel_col") == candidate["fuel_col"]:
                item.update({
                    "horizon_days": int(candidate["horizon_days"]),
                    "waiting_cost": float(candidate["waiting_cost"]),
                    "selected_on": candidate["selected_on"],
                    "selection_method": candidate["selection_method"],
                    "version": f"v{int(str(item.get('version', 'v1')).lstrip('v') or '1') + 1}",
                })
        Path(config.CONFIG_DIR / "production_models.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps({
        "approved": approved,
        "reasons": reasons,
        "candidate": {
            "scope_id": candidate["scope_id"],
            "fuel_col": candidate["fuel_col"],
            "horizon_days": candidate["horizon_days"],
            "waiting_cost": candidate["waiting_cost"],
        },
        "current_production": current_ref,
        "updated": bool(approved and args.write),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
