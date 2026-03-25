#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.database import Database
from src.history_store import export_history_from_db
from src.repo_config import get_scope


def main() -> None:
    parser = argparse.ArgumentParser(description="Exporta tables SQLite a data/history/<scope_id>/*.csv")
    parser.add_argument("--scope-id", default="alcantarilla_murcia")
    parser.add_argument("--db-path", default=None)
    args = parser.parse_args()

    scope = get_scope(args.scope_id)
    db = Database(db_path=args.db_path or str(scope.resolved_db_path))
    outputs = export_history_from_db(db=db, history_dir=scope.resolved_history_dir)
    print(json.dumps({"scope_id": scope.scope_id, "db_path": db.db_path, "outputs": outputs}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
