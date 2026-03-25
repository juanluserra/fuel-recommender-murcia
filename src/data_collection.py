from __future__ import annotations

"""
data_collection.py — capa de ingesta reutilizable para notebooks y scripts.
"""

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
import logging
from typing import Callable, Optional

from src.crawler import GasolinerasCrawler
from src.database import Database

logger = logging.getLogger(__name__)

ProgressCallback = Optional[Callable[[dict], None]]


@dataclass(slots=True)
class CollectionSummary:
    start_date: str
    end_date: str
    requested_dates: int = 0
    processed_dates: int = 0
    fetched_dates: int = 0
    skipped_dates: int = 0
    empty_dates: int = 0
    failed_dates: int = 0
    inserted_rows: int = 0
    errors: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "requested_dates": self.requested_dates,
            "processed_dates": self.processed_dates,
            "fetched_dates": self.fetched_dates,
            "skipped_dates": self.skipped_dates,
            "empty_dates": self.empty_dates,
            "failed_dates": self.failed_dates,
            "inserted_rows": self.inserted_rows,
            "errors": list(self.errors),
        }


class FuelDataCollector:
    """Recolector de histórico basado en el crawler y la base SQLite."""

    def __init__(self, db: Optional[Database] = None, crawler: Optional[GasolinerasCrawler] = None) -> None:
        self.db = db or Database()
        self.crawler = crawler or GasolinerasCrawler()

    def collect_today(self, force: bool = False, allow_empty_dates: bool = True) -> dict:
        return self.collect_date(date.today().isoformat(), force=force, allow_empty_dates=allow_empty_dates)

    def collect_date(self, target_date: str, force: bool = False, allow_empty_dates: bool = True) -> dict:
        target_date = self._normalize_date(target_date)

        if not force and self.db.date_already_processed(target_date):
            logger.info("Fecha %s ya procesada en la base de datos. Se omite.", target_date)
            return {
                "date": target_date,
                "status": "skipped",
                "inserted_rows": 0,
                "records": 0,
            }

        records = self.crawler.fetch_by_date(target_date)
        if not records:
            if allow_empty_dates:
                self.db.log_fetch_result(
                    target_date=target_date,
                    status="empty",
                    records_count=0,
                    inserted_rows=0,
                )
                logger.warning("La API no devolvió registros para %s. Se marca como fecha vacía.", target_date)
                return {
                    "date": target_date,
                    "status": "empty",
                    "inserted_rows": 0,
                    "records": 0,
                }
            raise RuntimeError(f"La API no devolvió registros para {target_date}.")

        inserted_rows = self.db.upsert_records(records)
        self.db.log_fetch_result(
            target_date=target_date,
            status="fetched",
            records_count=len(records),
            inserted_rows=inserted_rows,
        )
        logger.info("Fecha %s procesada correctamente: %d registros persistidos.", target_date, inserted_rows)
        return {
            "date": target_date,
            "status": "fetched",
            "inserted_rows": inserted_rows,
            "records": len(records),
        }

    def collect_range(
        self,
        start_date: str,
        end_date: Optional[str] = None,
        force: bool = False,
        allow_empty_dates: bool = True,
        stop_on_error: bool = False,
        progress_callback: ProgressCallback = None,
    ) -> CollectionSummary:
        start = self._parse_date(start_date)
        end = self._parse_date(end_date) if end_date else date.today()
        if start > end:
            raise ValueError("start_date no puede ser posterior a end_date.")

        summary = CollectionSummary(
            start_date=start.isoformat(),
            end_date=end.isoformat(),
            requested_dates=(end - start).days + 1,
        )

        current = start
        while current <= end:
            target_date = current.isoformat()
            if progress_callback:
                progress_callback({
                    "stage": "date_started",
                    "date": target_date,
                    "processed_dates": summary.processed_dates,
                    "requested_dates": summary.requested_dates,
                })
            try:
                result = self.collect_date(target_date, force=force, allow_empty_dates=allow_empty_dates)
                summary.processed_dates += 1
                summary.inserted_rows += int(result["inserted_rows"])
                if result["status"] == "skipped":
                    summary.skipped_dates += 1
                elif result["status"] == "empty":
                    summary.empty_dates += 1
                else:
                    summary.fetched_dates += 1

                if progress_callback:
                    progress_callback({
                        "stage": "date_finished",
                        "result": result,
                        "processed_dates": summary.processed_dates,
                        "requested_dates": summary.requested_dates,
                    })
            except Exception as exc:
                summary.processed_dates += 1
                summary.failed_dates += 1
                error_payload = {
                    "date": target_date,
                    "error": str(exc),
                    "type": exc.__class__.__name__,
                }
                self.db.log_fetch_result(
                    target_date=target_date,
                    status="failed",
                    records_count=0,
                    inserted_rows=0,
                    error_type=exc.__class__.__name__,
                    error_message=str(exc),
                )
                summary.errors.append(error_payload)
                logger.exception("Fallo al recolectar %s", target_date)
                if progress_callback:
                    progress_callback({
                        "stage": "date_failed",
                        "error": error_payload,
                        "processed_dates": summary.processed_dates,
                        "requested_dates": summary.requested_dates,
                    })
                if stop_on_error:
                    raise
            current += timedelta(days=1)

        if progress_callback:
            progress_callback({"stage": "collection_finished", "summary": summary.to_dict()})
        return summary

    def collect_missing_dates(
        self,
        start_date: str,
        end_date: Optional[str] = None,
        allow_empty_dates: bool = True,
        stop_on_error: bool = False,
        progress_callback: ProgressCallback = None,
    ) -> CollectionSummary:
        return self.collect_range(
            start_date=start_date,
            end_date=end_date,
            force=False,
            allow_empty_dates=allow_empty_dates,
            stop_on_error=stop_on_error,
            progress_callback=progress_callback,
        )

    @staticmethod
    def _parse_date(value: str) -> date:
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError(f"Fecha inválida '{value}'. Usa YYYY-MM-DD.") from exc

    @classmethod
    def _normalize_date(cls, value: str) -> str:
        return cls._parse_date(value).isoformat()
