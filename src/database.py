"""
database.py — Gestiona el almacenamiento de precios en SQLite.

Esquema
-------
  stations    → Metadatos de cada gasolinera (id, nombre, dirección, coords).
  prices      → Registro diario de precios por gasolinera.
  fetch_log   → Bitácora de recolección por fecha (fetched / empty / failed).
"""

from __future__ import annotations

import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from typing import Generator, Optional

import pandas as pd

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src import config

logger = logging.getLogger(__name__)

_ALL_FUEL_FIELDS = config.FUEL_FIELDS["diesel"] + config.FUEL_FIELDS["gasolina"]


def _field_to_col(field: str) -> str:
    import re
    col = field.lower()
    col = re.sub(r"[^a-z0-9]+", "_", col)
    return col.strip("_")


PRICE_COLUMNS = [_field_to_col(f) for f in _ALL_FUEL_FIELDS]


class Database:
    """Interfaz SQLite para el proyecto."""

    def __init__(self, db_path: str | os.PathLike[str] = config.DEFAULT_DATABASE_PATH) -> None:
        os.makedirs(os.path.dirname(db_path) if os.path.dirname(db_path) else ".", exist_ok=True)
        self.db_path = db_path
        self._init_schema()

    @contextmanager
    def _connect(self) -> Generator[sqlite3.Connection, None, None]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_schema(self) -> None:
        price_cols = "\n".join(f"    {col} REAL," for col in PRICE_COLUMNS)
        with self._connect() as conn:
            conn.executescript(f"""
                CREATE TABLE IF NOT EXISTS stations (
                    station_id   TEXT PRIMARY KEY,
                    station_name TEXT,
                    address      TEXT,
                    municipality TEXT,
                    province     TEXT,
                    latitude     REAL,
                    longitude    REAL,
                    schedule     TEXT,
                    updated_at   TEXT
                );

                CREATE TABLE IF NOT EXISTS prices (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    date         TEXT NOT NULL,
                    fetched_at   TEXT NOT NULL,
                    station_id   TEXT NOT NULL,
{price_cols}
                    UNIQUE (date, station_id),
                    FOREIGN KEY (station_id) REFERENCES stations(station_id)
                );

                CREATE TABLE IF NOT EXISTS fetch_log (
                    date            TEXT PRIMARY KEY,
                    status          TEXT NOT NULL,
                    records_count   INTEGER NOT NULL DEFAULT 0,
                    inserted_rows   INTEGER NOT NULL DEFAULT 0,
                    error_type      TEXT,
                    error_message   TEXT,
                    created_at      TEXT NOT NULL,
                    updated_at      TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_prices_date
                    ON prices(date);
                CREATE INDEX IF NOT EXISTS idx_prices_station
                    ON prices(station_id);
                CREATE INDEX IF NOT EXISTS idx_fetch_log_status
                    ON fetch_log(status);
            """)
        logger.debug("Schema inicializado en %s", self.db_path)

    def upsert_records(self, records: list[dict]) -> int:
        if not records:
            return 0

        inserted = 0
        now = datetime.utcnow().isoformat()

        with self._connect() as conn:
            for rec in records:
                conn.execute(
                    """
                    INSERT INTO stations
                        (station_id, station_name, address, municipality,
                         province, latitude, longitude, schedule, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(station_id) DO UPDATE SET
                        station_name = excluded.station_name,
                        address      = excluded.address,
                        municipality = excluded.municipality,
                        province     = excluded.province,
                        latitude     = excluded.latitude,
                        longitude    = excluded.longitude,
                        schedule     = excluded.schedule,
                        updated_at   = excluded.updated_at
                    """,
                    (
                        rec["station_id"], rec["station_name"], rec["address"],
                        rec["municipality"], rec["province"],
                        rec["latitude"], rec["longitude"],
                        rec["schedule"], now,
                    ),
                )

                cols = ["date", "fetched_at", "station_id"] + PRICE_COLUMNS
                values = [rec["date"], rec["fetched_at"], rec["station_id"]] + [rec.get(col) for col in PRICE_COLUMNS]
                placeholders = ", ".join("?" * len(cols))
                update_set = ", ".join(f"{c} = excluded.{c}" for c in cols if c != "station_id")
                conn.execute(
                    f"""
                    INSERT INTO prices ({", ".join(cols)})
                    VALUES ({placeholders})
                    ON CONFLICT(date, station_id) DO UPDATE SET {update_set}
                    """,
                    values,
                )
                inserted += 1

        logger.info("Upsert completado: %d registros", inserted)
        return inserted

    def log_fetch_result(
        self,
        target_date: str,
        status: str,
        records_count: int = 0,
        inserted_rows: int = 0,
        error_type: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        now = datetime.utcnow().isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO fetch_log (
                    date, status, records_count, inserted_rows,
                    error_type, error_message, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date) DO UPDATE SET
                    status = excluded.status,
                    records_count = excluded.records_count,
                    inserted_rows = excluded.inserted_rows,
                    error_type = excluded.error_type,
                    error_message = excluded.error_message,
                    updated_at = excluded.updated_at
                """,
                (
                    target_date,
                    status,
                    int(records_count),
                    int(inserted_rows),
                    error_type,
                    error_message,
                    now,
                    now,
                ),
            )

    def get_fetch_status(self, target_date: str) -> Optional[str]:
        with self._connect() as conn:
            row = conn.execute("SELECT status FROM fetch_log WHERE date = ?", (target_date,)).fetchone()
            return row[0] if row else None

    def date_already_processed(self, target_date: Optional[str] = None) -> bool:
        target_date = target_date or date.today().isoformat()
        status = self.get_fetch_status(target_date)
        return status in {"fetched", "empty"}

    def get_fetch_log(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        clauses = []
        params: list[str] = []
        if start_date:
            clauses.append("date >= ?")
            params.append(start_date)
        if end_date:
            clauses.append("date <= ?")
            params.append(end_date)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._connect() as conn:
            return pd.read_sql_query(
                f"SELECT * FROM fetch_log {where} ORDER BY date",
                conn,
                params=tuple(params),
            )


    def bootstrap_fetch_log_from_prices(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        overwrite: bool = False,
    ) -> dict:
        """
        Reconstruye fetch_log a partir de la tabla prices para proyectos existentes.

        - Si un día tiene al menos un registro en prices → status=fetched
        - Si no tiene ninguno dentro del rango → status=empty
        """
        with self._connect() as conn:
            minmax = conn.execute("SELECT MIN(date), MAX(date) FROM prices").fetchone()
            if not minmax or not minmax[0] or not minmax[1]:
                return {"created": 0, "updated": 0, "range_start": None, "range_end": None}

            start = start_date or minmax[0]
            end = end_date or minmax[1]
            observed = pd.read_sql_query(
                """
                SELECT date, COUNT(*) AS records_count
                FROM prices
                WHERE date >= ? AND date <= ?
                GROUP BY date
                ORDER BY date
                """,
                conn,
                params=(start, end),
            )

        calendar = pd.DataFrame({"date": pd.date_range(start=start, end=end, freq="D")})
        calendar["date"] = calendar["date"].dt.strftime("%Y-%m-%d")
        merged = calendar.merge(observed, on="date", how="left")
        merged["records_count"] = merged["records_count"].fillna(0).astype(int)
        merged["status"] = merged["records_count"].gt(0).map({True: "fetched", False: "empty"})

        created = 0
        updated = 0
        for row in merged.itertuples(index=False):
            previous = self.get_fetch_status(row.date)
            if previous is not None and not overwrite:
                continue
            self.log_fetch_result(
                target_date=row.date,
                status=row.status,
                records_count=int(row.records_count),
                inserted_rows=int(row.records_count),
            )
            if previous is None:
                created += 1
            else:
                updated += 1

        return {
            "created": created,
            "updated": updated,
            "range_start": start,
            "range_end": end,
        }

    def get_price_history(
        self,
        fuel_col: str,
        station_id: Optional[str] = None,
        days: int = 180,
        aggregate: str = "min",
    ) -> pd.DataFrame:
        since = (date.today() - timedelta(days=days)).isoformat()
        agg_fn = {"min": "MIN", "mean": "AVG", "max": "MAX"}.get(aggregate, "MIN")

        if station_id:
            query = f"""
                SELECT date, {fuel_col} as price
                FROM prices
                WHERE station_id = ?
                  AND date >= ?
                  AND {fuel_col} IS NOT NULL
                ORDER BY date
            """
            params = (station_id, since)
        else:
            query = f"""
                SELECT date, {agg_fn}({fuel_col}) as price
                FROM prices
                WHERE date >= ?
                  AND {fuel_col} IS NOT NULL
                GROUP BY date
                ORDER BY date
            """
            params = (since,)

        with self._connect() as conn:
            df = pd.read_sql_query(query, conn, params=params)

        if df.empty:
            return df
        df["date"] = pd.to_datetime(df["date"])
        df["price"] = df["price"].astype(float)
        return df

    def get_latest_prices(self, fuel_type: str = "diesel") -> pd.DataFrame:
        fuel_cols = [_field_to_col(f) for f in config.FUEL_FIELDS.get(fuel_type, [])]
        if not fuel_cols:
            raise ValueError(f"fuel_type desconocido: {fuel_type}")

        last_date = self._get_last_date()
        if not last_date:
            return pd.DataFrame()

        col_exprs = ", ".join(f"p.{c}" for c in fuel_cols)
        query = f"""
            SELECT
                s.station_name,
                s.address,
                s.schedule,
                {col_exprs},
                p.date
            FROM prices p
            JOIN stations s ON s.station_id = p.station_id
            WHERE p.date = ?
        """
        with self._connect() as conn:
            df = pd.read_sql_query(query, conn, params=(last_date,))

        df[fuel_cols] = df[fuel_cols].apply(pd.to_numeric, errors="coerce")
        df["mejor_precio"] = df[fuel_cols].min(axis=1, skipna=True)

        label_map = {
            _field_to_col(f): config.FUEL_LABELS.get(f, f)
            for f in config.FUEL_FIELDS.get(fuel_type, [])
        }
        valid_mask = df[fuel_cols].notna().any(axis=1)
        df["mejor_tipo"] = None
        if valid_mask.any():
            df.loc[valid_mask, "mejor_tipo"] = (
                df.loc[valid_mask, fuel_cols].idxmin(axis=1).map(label_map)
            )

        df = df[valid_mask].copy()
        return df.sort_values(["mejor_precio", "station_name"], na_position="last")

    def get_stations(self) -> pd.DataFrame:
        with self._connect() as conn:
            return pd.read_sql_query("SELECT * FROM stations", conn)

    def count_records(self, table: str = "prices") -> int:
        with self._connect() as conn:
            row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            return row[0] if row else 0

    def date_already_fetched(self, target_date: Optional[str] = None) -> bool:
        target_date = target_date or date.today().isoformat()
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) FROM prices WHERE date = ?", (target_date,)).fetchone()
            return (row[0] or 0) > 0

    def _get_last_date(self) -> Optional[str]:
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(date) FROM prices").fetchone()
            return row[0] if row else None

    def export_csv(self, output_path: str, fuel_cols: Optional[list[str]] = None) -> str:
        with self._connect() as conn:
            df = pd.read_sql_query(
                """
                SELECT p.date, s.station_name, s.address, p.*
                FROM prices p
                JOIN stations s ON s.station_id = p.station_id
                ORDER BY p.date
                """,
                conn,
            )
        if fuel_cols:
            base_cols = ["date", "station_name", "address"]
            df = df[[c for c in base_cols + fuel_cols if c in df.columns]]

        os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
        df.to_csv(output_path, index=False)
        logger.info("CSV exportado: %s (%d filas)", output_path, len(df))
        return output_path
