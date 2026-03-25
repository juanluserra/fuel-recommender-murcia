"""
Configuración central del proyecto Fuel Price Predictor.

Este módulo define rutas y valores por defecto seguros para desarrollo local.
La configuración operativa de producción vive en `config/*.json` y se carga
mediante `src.repo_config`.
"""

from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = Path(os.getenv("FUEL_CONFIG_DIR", PROJECT_ROOT / "config"))
DATA_DIR = Path(os.getenv("FUEL_DATA_DIR", PROJECT_ROOT / "data"))
DB_DIR = Path(os.getenv("FUEL_DB_DIR", DATA_DIR / "db"))
HISTORY_DIR = Path(os.getenv("FUEL_HISTORY_DIR", DATA_DIR / "history"))
DATASETS_DIR = Path(os.getenv("FUEL_DATASETS_DIR", DATA_DIR / "datasets"))
LATEST_DIR = Path(os.getenv("FUEL_LATEST_DIR", DATA_DIR / "latest"))
EXPERIMENTS_DIR = Path(os.getenv("FUEL_EXPERIMENTS_DIR", DATA_DIR / "experiments"))
MODELS_DIR = Path(os.getenv("FUEL_MODELS_DIR", PROJECT_ROOT / "models"))
PUBLIC_DIR = Path(os.getenv("FUEL_PUBLIC_DIR", PROJECT_ROOT / "public"))
REPORTS_DIR = Path(os.getenv("FUEL_REPORTS_DIR", PROJECT_ROOT / "reports"))

for directory in [CONFIG_DIR, DATA_DIR, DB_DIR, HISTORY_DIR, DATASETS_DIR, LATEST_DIR, EXPERIMENTS_DIR, MODELS_DIR, PUBLIC_DIR, REPORTS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

API_BASE_URL = (
    "https://sedeaplicaciones.minetur.gob.es"
    "/ServiciosRESTCarburantes/PreciosCarburantes"
)

# Defaults legacy/local.
DEFAULT_SCOPE_ID = os.getenv("FUEL_DEFAULT_SCOPE_ID", "alcantarilla_murcia")
DEFAULT_MUNICIPALITY_NAME = os.getenv("FUEL_DEFAULT_MUNICIPALITY", "Alcantarilla")
DEFAULT_PROVINCE_NAME = os.getenv("FUEL_DEFAULT_PROVINCE", "Murcia")
DEFAULT_MUNICIPALITY_ID = os.getenv("FUEL_DEFAULT_MUNICIPALITY_ID")
DEFAULT_DATABASE_PATH = Path(os.getenv("FUEL_DB_PATH", DB_DIR / f"{DEFAULT_SCOPE_ID}.db"))

FUEL_FIELDS = {
    "diesel": [
        "Precio Gasoleo A",
        "Precio Gasoleo B",
        "Precio Gasoleo Premium",
    ],
    "gasolina": [
        "Precio Gasolina 95 E5",
        "Precio Gasolina 95 E10",
        "Precio Gasolina 95 E5 Premium",
        "Precio Gasolina 98 E5",
        "Precio Gasolina 98 E10",
    ],
}

FUEL_LABELS = {
    "Precio Gasoleo A": "Diésel A",
    "Precio Gasoleo B": "Diésel B",
    "Precio Gasoleo Premium": "Diésel Premium",
    "Precio Gasolina 95 E5": "Gasolina 95",
    "Precio Gasolina 95 E10": "Gasolina 95 E10",
    "Precio Gasolina 95 E5 Premium": "Gasolina 95 Premium",
    "Precio Gasolina 98 E5": "Gasolina 98",
    "Precio Gasolina 98 E10": "Gasolina 98 E10",
}
