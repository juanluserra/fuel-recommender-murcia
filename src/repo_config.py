from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src import config


@dataclass(slots=True)
class Scope:
    scope_id: str
    municipality: str
    province: str
    municipality_id: str | None = None
    history_dir: str | None = None
    db_path: str | None = None

    @property
    def resolved_db_path(self) -> Path:
        if self.db_path:
            return (config.PROJECT_ROOT / self.db_path).resolve()
        return (config.DB_DIR / f"{self.scope_id}.db").resolve()

    @property
    def resolved_history_dir(self) -> Path:
        if self.history_dir:
            return (config.PROJECT_ROOT / self.history_dir).resolve()
        return (config.HISTORY_DIR / self.scope_id).resolve()


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def load_scopes(path: str | Path | None = None) -> dict[str, Scope]:
    path = Path(path or config.CONFIG_DIR / "scopes.json")
    payload = _read_json(path)
    scopes = {}
    for item in payload.get("scopes", []):
        scope = Scope(**item)
        scopes[scope.scope_id] = scope
    return scopes


def get_scope(scope_id: str, path: str | Path | None = None) -> Scope:
    scopes = load_scopes(path=path)
    if scope_id not in scopes:
        raise KeyError(f"scope_id desconocido: {scope_id}")
    return scopes[scope_id]


def load_production_models(path: str | Path | None = None) -> dict[str, Any]:
    return _read_json(Path(path or config.CONFIG_DIR / "production_models.json"))


def get_production_model(scope_id: str, fuel_col: str, path: str | Path | None = None) -> dict[str, Any]:
    payload = load_production_models(path=path)
    for item in payload.get("models", []):
        if item.get("scope_id") == scope_id and item.get("fuel_col") == fuel_col:
            return item
    raise KeyError(f"No existe configuración de producción para scope_id={scope_id}, fuel_col={fuel_col}")


def load_search_space(path: str | Path | None = None) -> dict[str, Any]:
    return _read_json(Path(path or config.CONFIG_DIR / "search_space.json"))


def load_promotion_policy(path: str | Path | None = None) -> dict[str, Any]:
    return _read_json(Path(path or config.CONFIG_DIR / "promotion_policy.json"))
