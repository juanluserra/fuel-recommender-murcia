from __future__ import annotations

import html
import json
import math
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

try:
    import numpy as np
except Exception:  # pragma: no cover
    np = None


def _to_jsonable(value: Any) -> Any:
    """
    Convert common pandas/numpy/python values to JSON-serializable values.

    Handles:
    - pandas.Timestamp / datetime / date -> ISO 8601 string
    - pandas.NA / NaN / NaT -> None
    - numpy scalars -> native Python scalars
    - dict / list / tuple / set -> recursively converted containers
    """
    if value is None:
        return None

    # Containers first
    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_jsonable(v) for v in value]

    # Datetime-like
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.isoformat()
    if isinstance(value, (datetime, date)):
        return value.isoformat()

    # Pandas missing scalars / generic missing values
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    # numpy scalars
    if np is not None:
        if isinstance(value, np.generic):
            return value.item()
        if isinstance(value, np.ndarray):
            return [_to_jsonable(v) for v in value.tolist()]

    # plain floats with non-finite values
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value

    return value


def build_scope_payload(
    scope: dict[str, Any],
    model_cfg: dict[str, Any],
    latest_df: pd.DataFrame,
) -> dict[str, Any]:
    if latest_df.empty:
        return {
            "scope_id": scope["scope_id"],
            "municipality": scope["municipality"],
            "province": scope["province"],
            "fuel_col": model_cfg["fuel_col"],
            "model": model_cfg,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "summary": {"recommendation": "no_data"},
            "stations": [],
        }

    latest_sorted = latest_df.sort_values(["decision", "price", "station_id"]).copy()
    buy_now_df = latest_sorted[latest_sorted["decision"] == "buy_now"].copy()
    cheapest_today = latest_sorted.sort_values(["price", "station_id"]).iloc[0]

    if not buy_now_df.empty:
        selected = buy_now_df.sort_values(["price", "station_id"]).iloc[0]
        municipality_recommendation = "buy_now"
    else:
        selected = cheapest_today
        municipality_recommendation = "wait"

    stations = latest_sorted.to_dict(orient="records")
    return {
        "scope_id": scope["scope_id"],
        "municipality": scope["municipality"],
        "province": scope["province"],
        "fuel_col": model_cfg["fuel_col"],
        "model": {
            "horizon_days": model_cfg["horizon_days"],
            "waiting_cost": model_cfg["waiting_cost"],
            "selected_on": model_cfg.get("selected_on"),
            "selection_method": model_cfg.get("selection_method"),
            "version": model_cfg.get("version", "v1"),
        },
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "recommendation": municipality_recommendation,
            "n_stations": int(len(latest_sorted)),
            "n_buy_now": int((latest_sorted["decision"] == "buy_now").sum()),
            "n_wait": int((latest_sorted["decision"] == "wait").sum()),
            "selected_station_id": str(selected["station_id"]),
            "selected_station_price": float(selected["price"]),
            "cheapest_station_id": str(cheapest_today["station_id"]),
            "cheapest_station_price": float(cheapest_today["price"]),
        },
        "stations": stations,
    }


def write_scope_payload(path: str | Path, payload: dict[str, Any]) -> str:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    serializable_payload = _to_jsonable(payload)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(serializable_payload, fh, ensure_ascii=False, indent=2)
    return str(path)


def render_index_html(payloads: list[dict[str, Any]]) -> str:
    rows = []
    for item in payloads:
        summary = item.get("summary", {})
        rows.append(
            "<tr>"
            f"<td>{html.escape(item.get('municipality', ''))}</td>"
            f"<td>{html.escape(item.get('province', ''))}</td>"
            f"<td>{html.escape(item.get('fuel_col', ''))}</td>"
            f"<td>{html.escape(summary.get('recommendation', ''))}</td>"
            f"<td>{summary.get('selected_station_price', '')}</td>"
            f"<td>{summary.get('cheapest_station_price', '')}</td>"
            f"<td>{html.escape(item.get('generated_at', ''))}</td>"
            "</tr>"
        )

    rows_html = "\n".join(rows)
    return f"""<!doctype html>
<html lang='es'>
<head>
  <meta charset='utf-8'>
  <meta name='viewport' content='width=device-width, initial-scale=1'>
  <title>Fuel Price Predictor</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 2rem; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #ddd; padding: 0.5rem; text-align: left; }}
    th {{ background: #f4f4f4; }}
    code {{ background: #f7f7f7; padding: 0.1rem 0.3rem; }}
  </style>
</head>
<body>
  <h1>Fuel Price Predictor</h1>
  <p>Recomendaciones diarias del modelo. Útil si ya estás entrando en zona de repostaje.</p>
  <table>
    <thead>
      <tr>
        <th>Municipio</th>
        <th>Provincia</th>
        <th>Combustible</th>
        <th>Recomendación</th>
        <th>Precio estación seleccionada</th>
        <th>Precio estación más barata hoy</th>
        <th>Actualizado</th>
      </tr>
    </thead>
    <tbody>
      {rows_html}
    </tbody>
  </table>
</body>
</html>"""
