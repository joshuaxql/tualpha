"""Isolated worker for memory-bounded Polars bucket materialization."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from ._csv_hdf5 import (
    DAILY_ROLES,
    _bucket_daily_inprocess,
    _bucket_daily_role_inprocess,
    _bucket_finance_inprocess,
)


def main() -> int:
    if len(sys.argv) != 2:
        raise SystemExit("usage: python -m tualpha._csv_bucket_worker CONFIG.json")
    config_path = Path(sys.argv[1])
    config = json.loads(config_path.read_text(encoding="utf-8"))
    action = str(config["action"])
    csv_dir = Path(config["csv_dir"])
    staging = Path(config["staging"])
    bucket_count = int(config["bucket_count"])
    result_path = Path(config["result_path"])
    if action == "daily":
        _, observations = _bucket_daily_inprocess(
            csv_dir,
            staging,
            show_progress=False,
            bucket_count=bucket_count,
        )
        payload = {
            "observations": [
                {
                    "ts_code": code,
                    "asset_type": asset_type,
                    "first_date": first.strftime("%Y%m%d"),
                    "last_date": last.strftime("%Y%m%d"),
                }
                for (code, asset_type), (first, last) in observations.items()
            ]
        }
    elif action == "daily_role":
        role_name = str(config["role"])
        role = next(item for item in DAILY_ROLES if item.role == role_name)
        _, categories = _bucket_daily_role_inprocess(
            csv_dir,
            staging,
            role,
            show_progress=False,
            bucket_count=bucket_count,
        )
        payload = {"categories": categories}
    elif action == "finance":
        _bucket_finance_inprocess(
            csv_dir,
            staging,
            str(config["table"]),
            show_progress=False,
            bucket_count=bucket_count,
        )
        payload = {"completed": True}
    else:
        raise SystemExit(f"unknown bucket worker action: {action}")
    result_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
