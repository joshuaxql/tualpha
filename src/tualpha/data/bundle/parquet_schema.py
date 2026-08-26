"""Canonical Parquet table registry for the local TuAlpha data lake."""

from __future__ import annotations

from dataclasses import dataclass

from ..tushare_fields import FINANCIAL_FIELDS

INDEX_DAILY_CODES = (
    "000985.CSI",
    "000300.SH",
    "000852.SH",
    "000016.SH",
    "000905.SH",
    "932000.CSI",
    "899050.BJ",
    "399006.SZ",
    "399673.SZ",
    "000688.SH",
    "000698.SH",
    "399330.SZ",
    "399903.SZ",
    "000510.SH",
    "399310.SZ",
)


@dataclass(frozen=True, slots=True)
class TableSpec:
    name: str
    path: str
    columns: tuple[tuple[str, str], ...]
    primary_key: tuple[str, ...]
    date_column: str | None = None
    partition_column: str | None = None
    sparse: bool = False

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(name for name, _ in self.columns)

    @property
    def parquet_glob(self) -> str:
        if self.partition_column == "year":
            return f"{self.path}/year=*/data.parquet"
        if self.partition_column == "report_year":
            return f"{self.path}/report_year=*/data.parquet"
        if self.partition_column == "index_code_year":
            return f"{self.path}/index_code=*/year=*/data.parquet"
        if self.partition_column == "exchange":
            return f"{self.path}/exchange=*/data.parquet"
        return f"{self.path}/data.parquet"


def _columns(
    names: str, *, strings: set[str] | None = None
) -> tuple[tuple[str, str], ...]:
    text = strings or set()
    return tuple(
        (name, "VARCHAR" if name in text else "DOUBLE") for name in names.split()
    )


MASTER_STRING_COLUMNS = {
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
    "act_name",
    "act_ent_type",
    "csname",
    "extname",
    "cname",
    "index_code",
    "index_name",
    "setup_date",
    "mgr_name",
    "custod_name",
    "etf_type",
    "publisher",
    "index_type",
    "category",
    "base_date",
    "weight_rule",
    "desc",
    "exp_date",
}

STOCK_BASIC_COLUMNS = """
ts_code symbol name area industry fullname enname cnspell market exchange curr_type
list_status list_date delist_date is_hs act_name act_ent_type
"""
ETF_BASIC_COLUMNS = """
ts_code csname extname cname index_code index_name setup_date list_date list_status
exchange mgr_name custod_name mgt_fee etf_type
"""
INDEX_BASIC_COLUMNS = """
ts_code name fullname market publisher index_type category base_date base_point list_date
weight_rule desc exp_date
"""
DAILY_COLUMNS = "trade_date ts_code open high low close pre_close volume turnover"

TRADE_CAL = TableSpec(
    "trade_cal",
    "stock/trade_cal",
    (
        ("exchange", "VARCHAR"),
        ("cal_date", "VARCHAR"),
        ("is_open", "UTINYINT"),
        ("pretrade_date", "VARCHAR"),
    ),
    ("exchange", "cal_date"),
    "cal_date",
    "exchange",
)
STOCK_BASIC = TableSpec(
    "stock_basic",
    "stock/basic",
    _columns(STOCK_BASIC_COLUMNS, strings=MASTER_STRING_COLUMNS),
    ("ts_code",),
)
ETF_BASIC = TableSpec(
    "etf_basic",
    "etf/basic",
    _columns(ETF_BASIC_COLUMNS, strings=MASTER_STRING_COLUMNS),
    ("ts_code",),
)
INDEX_BASIC = TableSpec(
    "index_basic",
    "index/basic",
    _columns(INDEX_BASIC_COLUMNS, strings=MASTER_STRING_COLUMNS),
    ("ts_code",),
)
STOCK_DAILY = TableSpec(
    "stock_daily",
    "stock/daily",
    _columns(DAILY_COLUMNS, strings={"trade_date", "ts_code"}),
    ("ts_code", "trade_date"),
    "trade_date",
    "year",
)
ETF_DAILY = TableSpec(
    "etf_daily",
    "etf/daily",
    _columns(DAILY_COLUMNS, strings={"trade_date", "ts_code"}),
    ("ts_code", "trade_date"),
    "trade_date",
    "year",
)
INDEX_DAILY = TableSpec(
    "index_daily",
    "index/daily",
    _columns(DAILY_COLUMNS, strings={"trade_date", "ts_code"}),
    ("ts_code", "trade_date"),
    "trade_date",
    "year",
    sparse=True,
)
ADJ_FACTOR = TableSpec(
    "adj_factor",
    "stock/adj_factor",
    (("trade_date", "VARCHAR"), ("ts_code", "VARCHAR"), ("adj_factor", "DOUBLE")),
    ("ts_code", "trade_date"),
    "trade_date",
    "year",
)
ETF_ADJ_FACTOR = TableSpec(
    "etf_adj_factor",
    "etf/adj_factor",
    (("trade_date", "VARCHAR"), ("ts_code", "VARCHAR"), ("adj_factor", "DOUBLE")),
    ("ts_code", "trade_date"),
    "trade_date",
    "year",
)
DAILY_BASIC = TableSpec(
    "daily_basic",
    "stock/daily_basic",
    _columns(
        "trade_date ts_code close turnover_rate turnover_rate_f volume_ratio pe pe_ttm pb ps ps_ttm "
        "dv_ratio dv_ttm total_share float_share free_share total_mv circ_mv limit_status",
        strings={"trade_date", "ts_code"},
    ),
    ("ts_code", "trade_date"),
    "trade_date",
    "year",
    sparse=True,
)
MONEYFLOW = TableSpec(
    "moneyflow",
    "stock/moneyflow",
    _columns(
        "trade_date ts_code buy_sm_vol buy_sm_amount sell_sm_vol sell_sm_amount buy_md_vol "
        "buy_md_amount sell_md_vol sell_md_amount buy_lg_vol buy_lg_amount sell_lg_vol "
        "sell_lg_amount buy_elg_vol buy_elg_amount sell_elg_vol sell_elg_amount net_mf_vol "
        "net_mf_amount",
        strings={"trade_date", "ts_code"},
    ),
    ("ts_code", "trade_date"),
    "trade_date",
    "year",
    sparse=True,
)
STK_LIMIT = TableSpec(
    "stk_limit",
    "stock/stk_limit",
    _columns(
        "trade_date ts_code pre_close up_limit down_limit",
        strings={"trade_date", "ts_code"},
    ),
    ("ts_code", "trade_date"),
    "trade_date",
    "year",
    sparse=True,
)
SUSPEND_D = TableSpec(
    "suspend_d",
    "stock/suspend_d",
    (
        ("trade_date", "VARCHAR"),
        ("ts_code", "VARCHAR"),
        ("suspend_timing", "VARCHAR"),
        ("suspend_type", "VARCHAR"),
    ),
    ("ts_code", "trade_date", "suspend_type"),
    "trade_date",
    "year",
    sparse=True,
)
STOCK_ST = TableSpec(
    "stock_st",
    "stock/stock_st",
    (
        ("trade_date", "VARCHAR"),
        ("ts_code", "VARCHAR"),
        ("name", "VARCHAR"),
        ("type", "VARCHAR"),
        ("type_name", "VARCHAR"),
    ),
    ("ts_code", "trade_date", "type"),
    "trade_date",
    "year",
    sparse=True,
)
INDUSTRY = TableSpec(
    "industry",
    "stock/industry",
    tuple(
        (name, "VARCHAR")
        for name in [
            "trade_date",
            "ts_code",
            "l1_code",
            "l1_name",
            "l2_code",
            "l2_name",
            "l3_code",
            "l3_name",
        ]
    ),
    ("ts_code", "trade_date"),
    "trade_date",
    "year",
    sparse=True,
)
INDEX_WEIGHT = TableSpec(
    "index_weight",
    "index/weight",
    (
        ("index_code", "VARCHAR"),
        ("con_code", "VARCHAR"),
        ("trade_date", "VARCHAR"),
        ("weight", "DOUBLE"),
    ),
    ("index_code", "trade_date", "con_code"),
    "trade_date",
    "index_code_year",
    sparse=True,
)


def _finance_spec(table: str) -> TableSpec:
    text = {
        "ts_code",
        "ann_date",
        "f_ann_date",
        "effective_ann_date",
        "end_date",
        "report_type",
        "comp_type",
        "end_type",
        "update_flag",
    }
    names = list(FINANCIAL_FIELDS[table])
    if "f_ann_date" not in names:
        names.insert(names.index("ann_date") + 1, "f_ann_date")
    names.insert(names.index("end_date"), "effective_ann_date")
    names.append("source_order")
    columns = tuple(
        (
            name,
            "UBIGINT"
            if name == "source_order"
            else "VARCHAR"
            if name in text
            else "DOUBLE",
        )
        for name in dict.fromkeys(names)
    )
    key_candidates = (
        ("ts_code", "ann_date", "end_date", "update_flag")
        if table == "fina_indicator"
        else (
            "ts_code",
            "ann_date",
            "f_ann_date",
            "end_date",
            "report_type",
            "comp_type",
            "end_type",
            "update_flag",
        )
    )
    column_names = {name for name, _ in columns}
    return TableSpec(
        table,
        f"stock/finance/{table}",
        columns,
        tuple(name for name in key_candidates if name in column_names),
        "end_date",
        "report_year",
        sparse=True,
    )


BALANCESHEET = _finance_spec("balancesheet")
INCOME = _finance_spec("income")
CASHFLOW = _finance_spec("cashflow")
FINA_INDICATOR = _finance_spec("fina_indicator")

TABLE_SPECS = {
    spec.name: spec
    for spec in (
        TRADE_CAL,
        STOCK_BASIC,
        ETF_BASIC,
        INDEX_BASIC,
        STOCK_DAILY,
        ETF_DAILY,
        INDEX_DAILY,
        ADJ_FACTOR,
        ETF_ADJ_FACTOR,
        DAILY_BASIC,
        MONEYFLOW,
        STK_LIMIT,
        SUSPEND_D,
        STOCK_ST,
        INDUSTRY,
        INDEX_WEIGHT,
        BALANCESHEET,
        INCOME,
        CASHFLOW,
        FINA_INDICATOR,
    )
}

DAILY_RUNTIME_TABLES = {
    "daily": (STOCK_DAILY, ETF_DAILY),
    "adj_factor": (ADJ_FACTOR, ETF_ADJ_FACTOR),
    "daily_basic": (DAILY_BASIC,),
    "moneyflow": (MONEYFLOW,),
    "stk_limit": (STK_LIMIT,),
    "suspend_d": (SUSPEND_D,),
    "stock_st": (STOCK_ST,),
    "industry": (INDUSTRY,),
}

FINANCE_SPECS = {
    spec.name: spec for spec in (BALANCESHEET, INCOME, CASHFLOW, FINA_INDICATOR)
}
