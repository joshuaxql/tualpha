from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


class FakeProClient:
    """Serve deterministic Tushare responses from the test fixture tree."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def _daily(self, api_name: str, date: str) -> pd.DataFrame:
        path = self.root / api_name / f"{date}.csv"
        return pd.read_csv(path, dtype={"ts_code": str})

    def query(self, api_name: str, **params: Any) -> pd.DataFrame:
        self.calls.append((api_name, dict(params)))
        offset = int(params.get("offset", 0))
        limit = int(params.get("limit", 5000))
        if api_name == "stock_basic":
            frame = pd.read_csv(self.root / "stock_basic.csv", dtype=str)
            frame = frame[frame["list_status"] == params.get("list_status")]
        elif api_name == "etf_basic":
            frame = pd.read_csv(self.root / "etf_basic.csv", dtype=str)
            frame = frame[frame["list_status"] == params.get("list_status")]
        elif api_name == "index_basic":
            frame = pd.read_csv(self.root / "index_basic.csv", dtype=str)
            frame = frame[frame["market"] == params.get("market")]
        elif api_name == "trade_cal":
            frame = pd.read_csv(self.root / "trade_cal.csv", dtype=str)
            shenzhen = frame.copy()
            shenzhen["exchange"] = "SZSE"
            frame = pd.concat([frame, shenzhen], ignore_index=True)
        elif api_name in {
            "daily",
            "adj_factor",
            "fund_daily",
            "fund_adj",
            "daily_basic",
            "moneyflow",
            "stk_limit",
            "suspend_d",
            "stock_st",
            "index_daily",
        }:
            frame = self._daily(api_name, str(params["trade_date"]))
        elif api_name == "index_member_all":
            source = pd.read_csv(self.root / "industry" / "20240108.csv", dtype=str)
            frame = source.drop(columns=["trade_date"]).copy()
            frame["name"] = "测试银行"
            frame["in_date"] = "20000101"
            frame["out_date"] = ""
            frame["is_new"] = "Y"
        elif api_name.endswith("_vip"):
            directory = api_name.removesuffix("_vip")
            if "period" in params:
                path = self.root / directory / f"{params['period']}.csv"
                frame = (
                    pd.read_csv(path, dtype=str) if path.is_file() else pd.DataFrame()
                )
            else:
                files = sorted((self.root / directory).glob("*.csv"))
                frame = pd.concat(
                    [pd.read_csv(path, dtype=str) for path in files],
                    ignore_index=True,
                    sort=False,
                )
                effective = frame.get("f_ann_date", frame["ann_date"]).fillna(
                    frame["ann_date"]
                )
                frame = frame[
                    (effective >= params["start_date"])
                    & (effective <= params["end_date"])
                ]
        elif api_name == "index_weight":
            files = sorted((self.root / "index_weight").glob("*.csv"))
            frame = (
                pd.concat(
                    [pd.read_csv(path, dtype=str) for path in files],
                    ignore_index=True,
                    sort=False,
                )
                if files
                else pd.DataFrame(
                    columns=["index_code", "con_code", "trade_date", "weight"]
                )
            )
            frame = frame[
                (frame["index_code"] == params["index_code"])
                & (frame["trade_date"] >= params["start_date"])
                & (frame["trade_date"] <= params["end_date"])
            ]
        else:  # pragma: no cover
            raise AssertionError(f"unexpected API: {api_name}")
        return frame.iloc[offset : offset + limit].reset_index(drop=True)
