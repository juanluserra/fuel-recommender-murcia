from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from src.database import Database


TABLE_ORDER = ("stations", "prices", "fetch_log")


def export_history_from_db(db: Database, history_dir: str | Path) -> dict[str, str]:
    history_dir = Path(history_dir)
    history_dir.mkdir(parents=True, exist_ok=True)
    outputs: dict[str, str] = {}
    with db._connect() as conn:
        for table in TABLE_ORDER:
            df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
            out_path = history_dir / f"{table}.csv"
            df.to_csv(out_path, index=False)
            outputs[table] = str(out_path)
    return outputs


def import_history_to_db(db: Database, history_dir: str | Path, table_order: Iterable[str] = TABLE_ORDER) -> dict[str, int]:
    history_dir = Path(history_dir)
    imported: dict[str, int] = {}
    with db._connect() as conn:
        conn.execute("DELETE FROM prices")
        conn.execute("DELETE FROM fetch_log")
        conn.execute("DELETE FROM stations")

        for table in table_order:
            csv_path = history_dir / f"{table}.csv"
            if not csv_path.exists():
                imported[table] = 0
                continue
            df = pd.read_csv(csv_path)
            if df.empty:
                imported[table] = 0
                continue
            df.to_sql(table, conn, if_exists="append", index=False)
            imported[table] = int(len(df))
    return imported
