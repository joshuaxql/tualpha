from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

SESSIONS = ["20240102", "20240103", "20240104", "20240105", "20240108"]


def _write(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


@pytest.fixture(scope="session")
def csv_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("tushare_data")
    stock_columns = [
        "ts_code",
        "symbol",
        "name",
        "area",
        "industry",
        "fullname",
        "enname",
        "cnspell",
        "market",
        "exchange",
        "curr_type",
        "list_status",
        "list_date",
        "delist_date",
        "is_hs",
    ]
    stocks = pd.DataFrame(
        [
            [
                "000001.SZ",
                "000001",
                "测试主板",
                "",
                "",
                "",
                "",
                "",
                "主板",
                "SZSE",
                "CNY",
                "L",
                "20000101",
                "",
                "N",
            ],
            [
                "688001.SH",
                "688001",
                "测试科创",
                "",
                "",
                "",
                "",
                "",
                "科创板",
                "SSE",
                "CNY",
                "L",
                "20190101",
                "",
                "N",
            ],
        ],
        columns=stock_columns,
    )
    _write(stocks, root / "stock_basic.csv")

    etf_columns = [
        "ts_code",
        "csname",
        "extname",
        "cname",
        "index_code",
        "index_name",
        "setup_date",
        "list_date",
        "list_status",
        "exchange",
        "mgr_name",
        "custod_name",
        "mgt_fee",
        "etf_type",
    ]
    etfs = pd.DataFrame(
        [
            [
                "510300.SH",
                "沪深300ETF",
                "300ETF",
                "境内股票ETF",
                "000300.SH",
                "沪深300",
                "20100101",
                "20100102",
                "L",
                "SH",
                "",
                "",
                "0.5",
                "纯境内",
            ],
            [
                "513100.SH",
                "纳指ETF",
                "纳指ETF",
                "纳斯达克ETF",
                "NDX.GI",
                "纳斯达克100",
                "20100101",
                "20100102",
                "L",
                "SH",
                "",
                "",
                "0.5",
                "QDII",
            ],
        ],
        columns=etf_columns,
    )
    _write(etfs, root / "etf_basic.csv")
    _write(
        pd.DataFrame(
            [
                [
                    "000985.CSI",
                    "中证全指",
                    "中证全指",
                    "CSI",
                    "中证指数",
                    "规模",
                    "综合",
                    "20000101",
                    1000,
                    "20000101",
                    "",
                    "",
                    "",
                ]
            ],
            columns=[
                "ts_code",
                "name",
                "fullname",
                "market",
                "publisher",
                "index_type",
                "category",
                "base_date",
                "base_point",
                "list_date",
                "weight_rule",
                "desc",
                "exp_date",
            ],
        ),
        root / "index_basic.csv",
    )
    _write(
        pd.DataFrame(
            {
                "exchange": "SSE",
                "cal_date": SESSIONS,
                "is_open": "1",
                "pretrade_date": ["20231229", *SESSIONS[:-1]],
            }
        ),
        root / "trade_cal.csv",
    )

    main_bars = [
        (10.0, 10.2, 9.8, 10.0, 10.0),
        (10.5, 11.0, 10.4, 11.0, 10.0),
        (12.1, 12.1, 10.9, 11.0, 11.0),
        (10.8, 11.0, 10.5, 10.8, 11.0),
        (10.9, 11.2, 10.8, 11.0, 10.8),
    ]
    main_limits = [(11.0, 9.0), (11.0, 9.0), (12.1, 9.9), (12.1, 9.9), (11.88, 9.72)]
    etf_prices = [4.0, 4.0, 2.0, 2.0, 2.1]
    etf_factors = [1.0, 1.0, 2.0, 2.0, 2.0]

    for index, date in enumerate(SESSIONS):
        open_, high, low, close, pre_close = main_bars[index]
        daily = pd.DataFrame(
            [
                [
                    "000001.SZ",
                    date,
                    open_,
                    high,
                    low,
                    close,
                    pre_close,
                    close - pre_close,
                    0.0,
                    10000.76,
                    10000,
                ],
                [
                    "688001.SH",
                    date,
                    20.0,
                    20.5,
                    19.5,
                    20.0,
                    20.0,
                    0.0,
                    0.0,
                    10000,
                    20000,
                ],
            ],
            columns=[
                "ts_code",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "pre_close",
                "change",
                "pct_chg",
                "vol",
                "amount",
            ],
        )
        _write(daily, root / "daily" / f"{date}.csv")
        _write(
            pd.DataFrame(
                [["000001.SZ", date, 1.0], ["688001.SH", date, 1.0]],
                columns=["ts_code", "trade_date", "adj_factor"],
            ),
            root / "adj_factor" / f"{date}.csv",
        )

        etf_price = etf_prices[index]
        fund_daily = pd.DataFrame(
            [
                [
                    "510300.SH",
                    date,
                    etf_price,
                    etf_price,
                    etf_price,
                    etf_price,
                    etf_price,
                    0.0,
                    0.0,
                    10000,
                    4000,
                ],
                ["513100.SH", date, 1.0, 1.01, 0.99, 1.0, 1.0, 0.0, 0.0, 10000, 1000],
                ["160001.SZ", date, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 100, 100],
            ],
            columns=daily.columns,
        )
        _write(fund_daily, root / "fund_daily" / f"{date}.csv")
        _write(
            pd.DataFrame(
                [["510300.SH", date, etf_factors[index]], ["513100.SH", date, 1.0]],
                columns=["ts_code", "trade_date", "adj_factor"],
            ),
            root / "fund_adj" / f"{date}.csv",
        )

        up, down = main_limits[index]
        limits = pd.DataFrame(
            [
                ["000001.SZ", date, pre_close, up, down],
                ["688001.SH", date, 20.0, 24.0, 16.0],
                ["510300.SH", date, "", etf_price * 1.1, etf_price * 0.9],
                ["513100.SH", date, "", 1.1, 0.9],
            ],
            columns=["ts_code", "trade_date", "pre_close", "up_limit", "down_limit"],
        )
        _write(limits, root / "stk_limit" / f"{date}.csv")

        suspended = (
            pd.DataFrame(
                [["000001.SZ", date, "", "S"]],
                columns=["ts_code", "trade_date", "suspend_timing", "suspend_type"],
            )
            if date == "20240105"
            else pd.DataFrame(
                columns=["ts_code", "trade_date", "suspend_timing", "suspend_type"]
            )
        )
        _write(suspended, root / "suspend_d" / f"{date}.csv")
        stock_st = (
            pd.DataFrame(
                [["000001.SZ", "ST测试", date, "ST", "风险警示板"]],
                columns=["ts_code", "name", "trade_date", "type", "type_name"],
            )
            if date == "20240104"
            else pd.DataFrame(
                columns=["ts_code", "name", "trade_date", "type", "type_name"]
            )
        )
        _write(stock_st, root / "stock_st" / f"{date}.csv")
        _write(
            pd.DataFrame(
                [["000001.SZ", date, close, 1.0, 1.0, 1.0, 10.0, 10.0, 1.0]],
                columns=[
                    "ts_code",
                    "trade_date",
                    "close",
                    "turnover_rate",
                    "turnover_rate_f",
                    "volume_ratio",
                    "pe",
                    "pe_ttm",
                    "pb",
                ],
            ),
            root / "daily_basic" / f"{date}.csv",
        )
        _write(
            pd.DataFrame(
                [["000001.SZ", date, 100, 10.0, 90, 9.0, 10, 1.0]],
                columns=[
                    "ts_code",
                    "trade_date",
                    "buy_sm_vol",
                    "buy_sm_amount",
                    "sell_sm_vol",
                    "sell_sm_amount",
                    "net_mf_vol",
                    "net_mf_amount",
                ],
            ),
            root / "moneyflow" / f"{date}.csv",
        )
        _write(
            pd.DataFrame(
                [
                    [
                        "000001.SZ",
                        date,
                        "801780.SI",
                        "银行",
                        "801783.SI",
                        "银行Ⅱ",
                        "857831.SI",
                        "银行Ⅲ",
                    ]
                ],
                columns=[
                    "ts_code",
                    "trade_date",
                    "l1_code",
                    "l1_name",
                    "l2_code",
                    "l2_name",
                    "l3_code",
                    "l3_name",
                ],
            ),
            root / "industry" / f"{date}.csv",
        )
        _write(
            pd.DataFrame(
                [
                    [
                        "000985.CSI",
                        date,
                        100 + index,
                        101 + index,
                        99 + index,
                        100 + index,
                        99 + index,
                        1,
                        1,
                        1000,
                        1000,
                    ]
                ],
                columns=daily.columns,
            ),
            root / "index_daily" / f"{date}.csv",
        )

    statement_specs = {
        "balancesheet": ("total_assets", [1000.0, 1100.0, 1200.0]),
        "income": ("revenue", [100.0, 110.0, 120.0]),
        "cashflow": ("n_cashflow_act", [10.0, 11.0, 12.0]),
    }
    for directory, (value_column, values) in statement_specs.items():
        _write(
            pd.DataFrame(
                [
                    [
                        "000001.SZ",
                        "20240102",
                        "20240102",
                        "20230930",
                        "1",
                        "1",
                        "3",
                        "0",
                        values[0],
                    ],
                    [
                        "000001.SZ",
                        "20240102",
                        "20240104",
                        "20230930",
                        "1",
                        "1",
                        "3",
                        "1",
                        values[1],
                    ],
                ],
                columns=[
                    "ts_code",
                    "ann_date",
                    "f_ann_date",
                    "end_date",
                    "report_type",
                    "comp_type",
                    "end_type",
                    "update_flag",
                    value_column,
                ],
            ),
            root / directory / "20230930.csv",
        )
        _write(
            pd.DataFrame(
                [
                    [
                        "000001.SZ",
                        "20240105",
                        "20240105",
                        "20231231",
                        "1",
                        "1",
                        "4",
                        "1",
                        values[2],
                    ]
                ],
                columns=[
                    "ts_code",
                    "ann_date",
                    "f_ann_date",
                    "end_date",
                    "report_type",
                    "comp_type",
                    "end_type",
                    "update_flag",
                    value_column,
                ],
            ),
            root / directory / "20231231.csv",
        )

    _write(
        pd.DataFrame(
            [
                ["000001.SZ", "20240102", "20230930", "0", 10.0, 50.0],
                ["000001.SZ", "20240104", "20230930", "1", 11.0, 49.0],
            ],
            columns=[
                "ts_code",
                "ann_date",
                "end_date",
                "update_flag",
                "roe",
                "debt_to_assets",
            ],
        ),
        root / "fina_indicator" / "20230930.csv",
    )
    _write(
        pd.DataFrame(
            [["000001.SZ", "20240105", "20231231", "1", 12.0, 48.0]],
            columns=[
                "ts_code",
                "ann_date",
                "end_date",
                "update_flag",
                "roe",
                "debt_to_assets",
            ],
        ),
        root / "fina_indicator" / "20231231.csv",
    )
    return root


@pytest.fixture(scope="session")
def bundle_root(tmp_path_factory: pytest.TempPathFactory, csv_dir: Path) -> Path:
    from tualpha.bundle import build_bundle

    root = tmp_path_factory.mktemp("bundle_root")
    build_bundle(csv_dir, bundle_root=root, rebuild_normalized=True)
    return root


@pytest.fixture(scope="session")
def data_root(bundle_root: Path) -> Path:
    """Backward-compatible fixture name for runtime-only tests."""

    return bundle_root
