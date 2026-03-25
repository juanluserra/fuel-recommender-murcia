from __future__ import annotations

"""
dataset_builder.py — utilidades para materializar datasets desde SQLite.

Salidas principales:
- panel: una fila por fecha y gasolinera.
- daily_summary: una fila por fecha con agregados diarios del municipio.
"""

from datetime import datetime
from pathlib import Path
import logging
from typing import Optional

import pandas as pd

from src.database import Database, PRICE_COLUMNS

logger = logging.getLogger(__name__)


class FuelDatasetBuilder:
    def __init__(self, db: Optional[Database] = None) -> None:
        self.db = db or Database()

    def build_station_panel(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        include_metadata: bool = True,
        drop_all_null_prices: bool = True,
        include_full_calendar: bool = False,
        include_fetch_status: bool = True,
    ) -> pd.DataFrame:
        where_sql, params = self._build_date_filter(start_date, end_date, table_alias="p")

        station_cols = ""
        if include_metadata:
            station_cols = """
                s.station_name,
                s.address,
                s.municipality,
                s.province,
                s.latitude,
                s.longitude,
                s.schedule,
            """

        select_cols = ",\n                ".join([f"p.{col}" for col in PRICE_COLUMNS])
        query = f"""
            SELECT
                p.date,
                p.fetched_at,
                p.station_id,
                {station_cols}
                {select_cols}
            FROM prices p
            JOIN stations s ON s.station_id = p.station_id
            {where_sql}
            ORDER BY p.date, p.station_id
        """
        df = self._read_sql(query, params)
        if df.empty and not include_full_calendar:
            return self._empty_station_panel(include_metadata=include_metadata, include_fetch_status=include_fetch_status)

        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
            for col in PRICE_COLUMNS:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            df["is_observed_price"] = df[PRICE_COLUMNS].notna().any(axis=1).astype(int)
        else:
            df = pd.DataFrame(columns=["date", "fetched_at", "station_id", *PRICE_COLUMNS])

        if include_full_calendar:
            df = self._expand_to_full_calendar(df, start_date=start_date, end_date=end_date, include_metadata=include_metadata)

        if include_fetch_status:
            df = self._attach_fetch_status(df)

        if drop_all_null_prices:
            df = df[df[PRICE_COLUMNS].notna().any(axis=1)].copy()

        return df.sort_values(["date", "station_id"]).reset_index(drop=True)

    def build_daily_summary(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        include_full_calendar: bool = False,
    ) -> pd.DataFrame:
        panel = self.build_station_panel(
            start_date=start_date,
            end_date=end_date,
            include_metadata=False,
            drop_all_null_prices=False,
            include_full_calendar=include_full_calendar,
            include_fetch_status=True,
        )
        if panel.empty:
            return panel

        agg_map: dict[str, list[str]] = {col: ["min", "mean", "max", "std", "count"] for col in PRICE_COLUMNS}
        summary = panel.groupby("date", as_index=False).agg(agg_map)

        flattened_columns = []
        for column in summary.columns:
            if isinstance(column, tuple):
                left, right = column
                if not right:
                    flattened_columns.append(left)
                elif right == "count":
                    flattened_columns.append(f"{left}_stations")
                else:
                    flattened_columns.append(f"{left}_{right}")
            else:
                flattened_columns.append(column)
        summary.columns = flattened_columns

        if "fetch_status" in panel.columns:
            status_df = (
                panel[["date", "fetch_status"]]
                .drop_duplicates()
                .sort_values("date")
            )
            summary = summary.merge(status_df, on="date", how="left")

        return summary.sort_values("date").reset_index(drop=True)

    def export_dataset(
        self,
        output_path: str,
        dataset_type: str = "panel",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        include_full_calendar: bool = False,
    ) -> str:
        dataset_type = dataset_type.lower().strip()
        if dataset_type == "panel":
            df = self.build_station_panel(
                start_date=start_date,
                end_date=end_date,
                include_full_calendar=include_full_calendar,
            )
        elif dataset_type in {"daily", "daily_summary", "summary"}:
            df = self.build_daily_summary(
                start_date=start_date,
                end_date=end_date,
                include_full_calendar=include_full_calendar,
            )
        else:
            raise ValueError("dataset_type debe ser 'panel' o 'daily_summary'.")

        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._write_dataframe(df, path)
        logger.info("Dataset exportado: %s (%d filas)", path, len(df))
        return str(path)

    def _expand_to_full_calendar(
        self,
        df: pd.DataFrame,
        start_date: Optional[str],
        end_date: Optional[str],
        include_metadata: bool,
    ) -> pd.DataFrame:
        stations = self.db.get_stations()
        if stations.empty:
            return self._ensure_station_panel_schema(df, include_metadata=include_metadata, include_fetch_status=True)

        if start_date:
            start_ts = pd.Timestamp(_normalize_date(start_date))
        elif not df.empty:
            start_ts = df["date"].min()
        else:
            with self.db._connect() as conn:
                row = conn.execute("SELECT MIN(date) FROM fetch_log").fetchone()
            start_ts = pd.Timestamp(row[0]) if row and row[0] else pd.Timestamp.today().normalize()

        if end_date:
            end_ts = pd.Timestamp(_normalize_date(end_date))
        elif not df.empty:
            end_ts = df["date"].max()
        else:
            with self.db._connect() as conn:
                row = conn.execute("SELECT MAX(date) FROM fetch_log").fetchone()
            end_ts = pd.Timestamp(row[0]) if row and row[0] else pd.Timestamp.today().normalize()

        calendar = pd.date_range(start=start_ts, end=end_ts, freq="D")
        grid = (
            pd.MultiIndex.from_product([calendar, stations["station_id"]], names=["date", "station_id"])
            .to_frame(index=False)
        )

        cols_to_merge = ["station_id"]
        if include_metadata:
            cols_to_merge.extend([c for c in [
                "station_name", "address", "municipality", "province", "latitude", "longitude", "schedule"
            ] if c in stations.columns])

        expanded = grid.merge(stations[cols_to_merge], on="station_id", how="left")
        if not df.empty:
            expanded = expanded.merge(df, on=[c for c in expanded.columns if c in ["date", "station_id", "station_name", "address", "municipality", "province", "latitude", "longitude", "schedule"]], how="left")
        else:
            for col in PRICE_COLUMNS + ["fetched_at", "is_observed_price"]:
                expanded[col] = pd.NA

        if "is_observed_price" not in expanded.columns:
            expanded["is_observed_price"] = expanded[PRICE_COLUMNS].notna().any(axis=1).astype(int)
        else:
            expanded["is_observed_price"] = expanded["is_observed_price"].fillna(0).astype(int)

        return expanded


    def _empty_station_panel(self, include_metadata: bool, include_fetch_status: bool) -> pd.DataFrame:
        cols = ["date", "fetched_at", "station_id"]
        if include_metadata:
            cols.extend([
                "station_name",
                "address",
                "municipality",
                "province",
                "latitude",
                "longitude",
                "schedule",
            ])
        cols.extend(PRICE_COLUMNS)
        cols.append("is_observed_price")
        if include_fetch_status:
            cols.append("fetch_status")
        return pd.DataFrame({c: pd.Series(dtype="object") for c in cols})

    def _ensure_station_panel_schema(self, df: pd.DataFrame, include_metadata: bool, include_fetch_status: bool) -> pd.DataFrame:
        out = df.copy()
        template = self._empty_station_panel(include_metadata=include_metadata, include_fetch_status=include_fetch_status)
        for col in template.columns:
            if col not in out.columns:
                out[col] = pd.Series(dtype=template[col].dtype)
        return out[template.columns]

    def _attach_fetch_status(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            out = df.copy()
            if "fetch_status" not in out.columns:
                out["fetch_status"] = pd.Series(dtype="object")
            return out
        fetch_log = self.db.get_fetch_log()
        if fetch_log.empty:
            df["fetch_status"] = pd.NA
            return df
        fetch_log["date"] = pd.to_datetime(fetch_log["date"])
        return df.merge(fetch_log[["date", "status"]].rename(columns={"status": "fetch_status"}), on="date", how="left")

    def _read_sql(self, query: str, params: tuple) -> pd.DataFrame:
        with self.db._connect() as conn:
            return pd.read_sql_query(query, conn, params=params)

    @staticmethod
    def _build_date_filter(start_date: Optional[str], end_date: Optional[str], table_alias: str = "p") -> tuple[str, tuple]:
        clauses = []
        params: list[str] = []
        prefix = f"{table_alias}." if table_alias else ""

        if start_date:
            clauses.append(f"{prefix}date >= ?")
            params.append(_normalize_date(start_date))
        if end_date:
            clauses.append(f"{prefix}date <= ?")
            params.append(_normalize_date(end_date))

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return where_sql, tuple(params)

    @staticmethod
    def _write_dataframe(df: pd.DataFrame, output_path: Path) -> None:
        suffix = output_path.suffix.lower()
        if suffix == ".csv":
            df.to_csv(output_path, index=False)
            return
        if suffix == ".parquet":
            try:
                df.to_parquet(output_path, index=False)
                return
            except Exception as exc:
                raise RuntimeError(
                    "No se pudo exportar a Parquet. Instala pyarrow o fastparquet, o usa CSV."
                ) from exc
        raise ValueError("Formato no soportado. Usa .csv o .parquet")


def default_dataset_path(dataset_type: str = "panel", suffix: str = ".csv") -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_type = dataset_type.lower().strip()
    return str(Path("data") / "datasets" / f"{safe_type}_{timestamp}{suffix}")


def _normalize_date(value: str) -> str:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date().isoformat()
    except ValueError as exc:
        raise ValueError(f"Fecha inválida '{value}'. Usa YYYY-MM-DD.") from exc
