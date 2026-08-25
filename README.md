# TuAlpha

TuAlpha 是基于 Tushare 数据、面向中国 A 股股票与 ETF 的日频事件驱动回测框架。框架提供类似 Zipline 的策略 API，但资产、日历、行情、财务和指数权重均由 TuAlpha 自有 HDF5 Reader/Writer 管理。

> 当前开发版本为 `0.8.1`（尚未发布到 PyPI）。仅支持多头现金账户、股票和 ETF；不支持期货、期权、融资融券或 ETF 申赎。

## 功能

- RQAlpha 风格的 HDF5/NumPy/Pickle 固定 Bundle
- 常用横截面字段使用同一 HDF5 文件内的 packed 连续数组加速，旧 schema 7 Bundle 可兼容回退
- 股票、ETF、指数原始日线统一存入 `daily.h5`
- Tushare SSE `trade_cal` 开放日固化到 `trade_dates.npy`
- D 日收盘决策，订单最早 D+1 按开盘或收盘价成交
- 涨停禁止买入、跌停禁止卖出；停牌、无行情、零成交量禁止成交
- 主板、创业板和 ETF 按 100 股/份交易
- 科创板 200 股起，之后按 1 股递增；北交所 100 股起，之后按 1 股递增
- 所有股票和 ETF 统一 T+1
- 支持印花税、佣金、经手费和过户费
- 支持 `raw`、`qfq`、`hfq`，策略价格与成交原始价格分离
- 支持每日指标、资金流、历史行业、历史 ST、PIT 财务和 PIT 指数权重
- 中文 Plotly HTML 报告、每日持仓 CSV 和每日权重贡献归因
- `tualpha update` 增量更新原始 CSV 后整体构建并原子发布新 Bundle

## 安装

推荐 64 位 CPython 3.12：

```bash
uv add tualpha
```

源码开发：

```bash
uv sync --dev
uv run pytest
```

## 数据目录

默认根目录为 `~/.tualpha`，最终 Bundle 固定为 `~/.tualpha/bundle`：

```text
~/.tualpha/
├── bundle/
│   ├── daily.h5          # 股票、ETF、指数原始日线
│   ├── adj_factor.h5     # 股票、ETF 复权因子
│   ├── daily_basic.h5    # 股票每日指标
│   ├── stk_limit.h5      # 每日涨跌停价格
│   ├── finance.h5        # 四类财务宽表及 PIT 元数据
│   ├── industry.h5       # 每日历史行业
│   ├── stock_st.h5       # 每日历史 ST 状态
│   ├── moneyflow.h5      # 每日资金流
│   ├── index_weight.h5   # PIT 指数成分与权重
│   ├── trade_dates.npy   # Tushare SSE 开放交易日
│   ├── assets.pk         # 资产信息、generation 和文件清单
│   └── suspend_d.h5      # 每日停牌状态
├── update-status.json    # 更新、构建、验证和旧数据清理记录
├── .locks/               # 更新锁和 Bundle 发布锁
├── .staging/             # 构建临时区，成功后自动删除
└── .rollback/            # 发布中断恢复目录
```

`bundle/` 必须且只能包含上述 12 个文件。

回测只读取最终 Bundle，不读取原始 CSV 或 staging 文件。详细协议见 [`docs/bundle-format.md`](docs/bundle-format.md)。

## 原始 CSV

CSV 目录没有默认值，必须与 Bundle 根目录完全分离，例如：

```text
E:\data\tushare_data\
├── daily\YYYYMMDD.csv
├── fund_daily\YYYYMMDD.csv
├── index_daily\YYYYMMDD.csv
├── adj_factor\YYYYMMDD.csv
├── fund_adj\YYYYMMDD.csv
├── daily_basic\YYYYMMDD.csv
├── stk_limit\YYYYMMDD.csv
├── suspend_d\YYYYMMDD.csv
├── industry\YYYYMMDD.csv
├── stock_st\YYYYMMDD.csv
├── moneyflow\YYYYMMDD.csv
├── index_weight\YYYYMMDD.csv
├── balancesheet\*.csv
├── income\*.csv
├── cashflow\*.csv
├── fina_indicator\*.csv
├── stock_basic.csv
├── etf_basic.csv
├── index_basic.csv
└── trade_cal.csv
```

## 更新数据

必须通过环境变量提供 Token：

```bash
export TUSHARE_TOKEN="your-token"
tualpha update --csv-dir /e/data/tushare_data
```

常用参数：

```bash
tualpha update --csv-dir /e/data/tushare_data --from 20260101 --to 20260821
tualpha update --csv-dir /e/data/tushare_data --repair-from 20250101
tualpha update --csv-dir /e/data/tushare_data --lookback 20
tualpha update --csv-dir /e/data/tushare_data --index-weight 000016.SH
tualpha update --csv-dir /e/data/tushare_data --dry-run --json
```

更新流程：

1. 增量下载行情、复权、每日指标、资金流、行业、ST、停牌、财务和指数权重 CSV。
2. 对分页接口检测重复页；按公告日期合并财务历史修订。
3. 原子发布原始 CSV，失败时根据 journal 恢复。
4. 将 CSV 流式写入 `.staging` 中按证券代码哈希分桶的临时 Parquet。
5. 每个桶按 `ts_code + 日期 + source_order` 排序，再一次性写入目标 HDF5 dataset。
6. 校验 12 文件集合、generation、dtype、日期、PIT 规则、文件大小和 SHA-256。
7. 持发布锁替换固定 `bundle/`，并从最终路径重新打开验证。
8. 原子写入 `update-status.json`，成功后删除 staging。

`index_weight` 默认维护 `000300.SH`、`000852.SH`、`000905.SH`、`000906.SH` 和 `899050.BJ`，原始权重单位为百分比。

## 快速开始

```python
from tualpha import order_target_percent, record, run_algorithm, symbol


def initialize(context):
    context.asset = symbol("510300.SH")


def handle_data(context, data):
    closes = data.history(context.asset, "close", 20)
    if len(closes) < 20:
        return
    target = 0.95 if closes.iloc[-1] > closes.mean() else 0.0
    order_target_percent(context.asset, target)
    record(close=closes.iloc[-1], ma20=closes.mean())


result = run_algorithm(
    start="2020-01-01",
    end="2025-12-31",
    initialize=initialize,
    handle_data=handle_data,
    capital_base=1_000_000,
    adjustment="qfq",
    execution_time="open",
    benchmark="000300.SH",
    output_dir="outputs/demo",
    strategy_name="沪深300 ETF 趋势策略",
    column_cache_mib=2048,
)
print(result.summary())
```

策略在 D 日回调中最多读取到 D 日数据，新订单最早 D+1 成交。成交、现金、费用和涨跌停判断始终使用原始价格。

日频列缓存默认上限为 2,048 MiB，可通过 `run_algorithm(column_cache_mib=...)` 或环境变量 `TUALPHA_COLUMN_CACHE_MIB` 调整。大型固定资产池优先使用 `data.current_arrays()`，它返回与资产顺序一致的只读 NumPy 数组。

## 核心 API

- `symbol(code)`
- `order()`、`order_value()`、`order_percent()`
- `order_target()`、`order_target_many()`、`order_target_value()`、`order_target_percent()`
- 批量目标买单及单笔 `target_value` / `target_percent` 在 D+1 按实际价格与现金缩量；不足最小单位时取消而非现金不足拒单
- `cancel_order()`、`get_open_orders()`
- `record(**values)`
- `data.current()`、`data.current_arrays()`、`data.raw_current()`、`data.history()`
- `data.fundamental()`、`data.fundamentals()`
- `data.index_constituents()`
- `data.available_fields()`
- `data.can_trade()`

## 日频扩展字段

扩展字段使用 `<数据集>.<字段>`：

```python
pe_ttm = data.current(context.asset, "daily_basic.pe_ttm")
net_flow = data.history(context.asset, "moneyflow.net_mf_amount", 20)
industry = data.current(context.asset, "industry.l1_name")
is_st = data.current(context.asset, "stock_st.is_st")
```

- `daily_basic`：估值、换手率、股本和市值；
- `moneyflow`：大小单量和金额，量为手、金额为万元；
- `industry`：历史申万一至三级行业；
- `stock_st`：历史 ST 名称、类型和 `is_st`；
- `suspended`：当日停牌标志。

行业和 ST 字符串在 HDF5 内使用整型字典编码，DataPortal 自动还原。

## PIT 指数权重

```python
members = data.index_constituents("000300.SH")
```

返回以 `ts_code` 为索引，包含 `asset / weight / snapshot_date`。可见性严格为：

```text
max(snapshot_date) < 当前回测日
```

因此 D 日快照从 D+1 可见，首个快照前为空，不使用未来成分回填历史。数据位于 `index_weight.h5`。

## PIT 财务

```python
roe = data.fundamental(context.asset, "fina_indicator.roe")
reports = data.fundamentals(
    context.asset,
    ["income.revenue", "balancesheet.total_assets"],
    periods=4,
)
```

`finance.h5` 保存 `balancesheet / income / cashflow / fina_indicator`。财务记录必须满足：

```text
effective_ann_date < 当前回测日
end_date <= 当前回测日
```

公告日当天不可见。同一报告期选择当时可见的最新公告、`update_flag=1` 优先版本和最大 `source_order`。利润表和现金流量表保持年初至今累计口径。

## 复权

- 前复权：`raw(D_i) × factor(D_i) / factor(当前回调日)`
- 后复权：`raw(D_i) × factor(D_i)`

公司行动改变持仓数量时，仅同步仍待成交的隔夜全仓卖单；普通买单和部分卖单不会自动改写。

## 报告

指定 `output_dir` 后生成：

```text
outputs/demo/
├── report.html
└── daily_positions.csv
```

报告包含收益、基准、回撤、费用、交易限制和组合归因，不生成逐笔 Trade Analysis 图。贡献收益按 `Σ(日终权重 × 标的当日收益率)` 逐日累计，现金贡献固定为 0。股票和 ETF 持有天数按正市值日终记录统计；`CASH` 仅统计组合没有正市值证券、全部为现金的交易日。

## 实际验证

开发机上的真实 schema 7 Bundle：

- 7,583 个股票/ETF；
- 11,648 个指数；
- 4,040 个交易日；
- `daily.h5` 50,239,528 行；
- `finance.h5` 1,752,376 行；
- `index_weight.h5` 464,246 行；
- 总大小约 11.56 GiB（包含同文件 packed 加速索引）；
- 全量 CSV → HDF5 构建约 373 秒。

`D:/projects/quant/strategies/宽基轮动` 在 2017-01-01 至 2026-08-21、每日扫描全市场、最多持有约 400 只股票、完整报告和 CSV 全部启用时，共处理 2,340 个交易日；项目固定的 CPython 3.12 环境端到端耗时约 38.7 秒。该数据仅用于说明实现量级，实际速度取决于硬件和策略。

## 已知边界

- 涨停不买、跌停不卖是保守流动性假设；
- 日内临时停牌在日频模型中按全天不可交易处理；
- 不模拟盘口深度、排队、流动性部分成交和冲击成本；目标单仅会因 D+1 实际现金约束按有效交易单位部分成交；
- Tushare 不提供指数权重历史修订发布时间，无法还原供应商后续修订前版本；
- 只有复权因子而没有现金分红明细时，公司行动使用分红再投近似。
