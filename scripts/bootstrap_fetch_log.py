#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.database import Database


def main() -> None:
    parser = argparse.ArgumentParser(description="Reconstruye fetch_log a partir de la tabla prices.")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    db = Database()
    result = db.bootstrap_fetch_log_from_prices(
        start_date=args.start_date,
        end_date=args.end_date,
        overwrite=args.overwrite,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
