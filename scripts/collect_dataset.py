from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.data_pipeline import collect_and_export_dataset

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("collect_dataset")


def main() -> int:
    parser = argparse.ArgumentParser(description="Recolecta histórico y exporta un dataset analítico.")
    parser.add_argument("--start-date", required=True, help="Fecha inicial YYYY-MM-DD")
    parser.add_argument(
        "--end-date",
        default=date.today().isoformat(),
        help="Fecha final YYYY-MM-DD (por defecto: hoy)",
    )
    parser.add_argument(
        "--dataset-type",
        choices=["panel", "daily_summary"],
        default="panel",
        help="Tipo de dataset a exportar.",
    )
    parser.add_argument("--output", default=None, help="Ruta .csv o .parquet")
    parser.add_argument("--force", action="store_true", help="Vuelve a descargar fechas ya existentes")
    parser.add_argument("--strict-empty", action="store_true", help="Falla si la API no devuelve datos")
    parser.add_argument("--include-full-calendar", action="store_true", help="Incluye fechas/estaciones sin dato como NA")
    parser.add_argument("--stop-on-error", action="store_true", help="Detiene el proceso al primer error")
    args = parser.parse_args()

    result = collect_and_export_dataset(
        start_date=args.start_date,
        end_date=args.end_date,
        dataset_type=args.dataset_type,
        output_path=args.output,
        force=args.force,
        allow_empty_dates=not args.strict_empty,
        include_full_calendar=args.include_full_calendar,
        stop_on_error=args.stop_on_error,
    )
    logger.info("Resultado: %s", result.to_dict())
    return 1 if result.collection.failed_dates else 0


if __name__ == "__main__":
    raise SystemExit(main())
