# TuAlpha

TuAlpha 是基于tushare数据面向中国 A 股股票与 ETF 的日频事件驱动回测框架。框架借鉴 Zipline 的 `Asset → DataPortal → Blotter → Ledger → Metrics` 分层，使用 **zipline-reloaded 官方 Bundle 格式**和中国市场交易规则。

> 当前版本为 `0.6.0`，只支持多头现金账户、股票和 ETF；不支持期货、期权、融资融券或 ETF 申赎。

## 功能

- Zipline 官方 Writer 生成的 `assets-7.sqlite + daily_equities.bcolz + adjustments.sqlite` Bundle 文件
- 股票、ETF 日线回测，Bundle 默认根目录：`~/.tualpha`
- D 日收盘决策，D+1 开盘或收盘成交，避免未来函数
- 涨停禁止买入、跌停禁止卖出的保守成交模型
- 停牌、无行情、零成交量限制
- 主板/创业板/ETF：买入 100 股（份）整数倍
- 科创板：200 股起、之后按 1 股递增
- 北交所：100 股起、之后按 1 股递增
- **所有股票和 ETF 统一 T+1**
- 股票印花税、佣金、经手费与过户费；避免 all-in 佣金重复计费
- 前复权、后复权或不复权策略数据
- 五个默认宽基指数的 PIT 历史成分与权重
- 中文 Plotly HTML 报告和每日持仓 CSV
- `tualpha update` 增量更新显式指定的 Tushare CSV 缓存并替换固定目录 Bundle

## 安装

推荐 64 位 CPython 3.12：

```bash
uv add tualpha
```

从源码开发：

```bash
uv sync --dev
uv run pytest
```

## 数据目录

Bundle 与原始 CSV 已完全分离。Bundle 根目录默认是 `~/.tualpha`，可通过 `--bundle-root`、Python 的 `bundle_root=` 或环境变量 `TUALPHA_BUNDLE_ROOT` 显式覆盖：

```text
~/.tualpha/                         # Bundle 根目录；可用 --bundle-root 修改
├── bundles/
│   └── tualpha/                    # 固定目录，不再使用更新时间命名
│       ├── assets-7.sqlite
│       ├── daily_equities.bcolz/    # OHLCV + 全部扩展日线列
│       ├── index_daily.bcolz/       # 仅用于报告基准的指数日线
│       ├── minute_equities.bcolz/
│       ├── adjustments.sqlite
│       ├── finance.sqlite           # 公告时点财务宽表
│       ├── index_constituents.sqlite # PIT 指数成分与权重
│       ├── manifest.json
│       └── READY
├── cache/
│   └── tualpha/
│       ├── normalized.duckdb
│       └── sid-map.json
├── update-status.json              # 当前更新状态及最后一次成功更新
├── .locks/                          # 进程锁
└── .staging/                        # 构建临时区；成功后自动清理
```

Bundle schema v5 不生成或读取 `tualpha.duckdb`。`normalized.duckdb` 只存在于 `cache/`，用于更新和构建，不属于可发布 Bundle。旧 schema 必须通过 `tualpha update` 或 `build_bundle()` 整体重建；发布成功后旧目录（含旧数据库）会被原子替换。

CSV 缓存目录**没有默认值**，执行更新时必须显式传入，并且不能与 Bundle 根目录互为父子目录。例如：

```text
E:\data\tushare_data\
├── daily\YYYYMMDD.csv
├── fund_daily\YYYYMMDD.csv
├── adj_factor\YYYYMMDD.csv
├── fund_adj\YYYYMMDD.csv
├── stk_limit\YYYYMMDD.csv
├── suspend_d\YYYYMMDD.csv
├── daily_basic\YYYYMMDD.csv
├── moneyflow\YYYYMMDD.csv
├── industry\YYYYMMDD.csv
├── stock_st\YYYYMMDD.csv
├── index_daily\YYYYMMDD.csv
├── index_weight\YYYYMMDD.csv
├── balancesheet\YYYYMMDD.csv
├── income\YYYYMMDD.csv
├── cashflow\YYYYMMDD.csv
├── fina_indicator\YYYYMMDD.csv
├── stock_basic.csv
├── etf_basic.csv
├── index_basic.csv
└── trade_cal.csv
```

回测只读取 `~/.tualpha/bundles/tualpha`，不会读取 CSV。`fund_daily` 中的 LOF、分级基金和异常代码不会成为可交易资产；Bundle 使用 `etf_basic.csv` 作为 ETF 白名单。

更新时先在 `.staging` 中使用 Zipline 官方 Writer 完整生成并加载验证所有文件，再替换固定的 `bundles/tualpha` 目录。回测和底层 `load_bundle_data()` 会持有读锁，更新将在现有读者关闭后发布，避免 Windows 文件句柄阻断目录替换。由于 Zipline 的 `core.load()` 强制发现时间戳子目录，固定布局不再直接支持 `core.load("tualpha")`；需要底层 Zipline Readers 时使用 `tualpha.bundle.load_bundle_data()`。详细文件、表和字段说明见 [`docs/bundle-format.md`](docs/bundle-format.md)。

## 更新数据

必须通过环境变量提供 Token；没有 Token 时命令立即中止：

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
tualpha update --csv-dir /e/data/tushare_data --bundle-root /d/tualpha-bundle
tualpha update --csv-dir /e/data/tushare_data --dry-run --json
```

更新流程：

1. 增量下载并保留全部现有数据目录，包括行情、资金流、每日指标、行业和财务报表；普通更新会逐数据集检测并补齐缺失的交易日分区。
2. 对超过单次行数上限的接口自动分页，并检测接口忽略分页造成的重复页；财务数据除刷新最近报告期外，还按公告日期增量抓取旧报告期修订并与历史版本合并。
3. 数据写入 `~/.tualpha/.staging`，校验字段、日期、唯一键和内容哈希。
4. 成功后原子替换 CSV，并增量同步 `~/.tualpha/cache/tualpha/normalized.duckdb`。
5. 在临时目录生成新的 Bundle，加载验证成功后替换固定的 `~/.tualpha/bundles/tualpha`。
6. 原子写入 `~/.tualpha/update-status.json`，记录 `running / succeeded / failed / dry_run_succeeded` 状态、CSV 路径、更新时间、更新日期和错误信息。

`index_weight` 官方接口要求逐个指数代码查询。TuAlpha 默认维护 `000300.SH`、`000852.SH`、`000905.SH`、`000906.SH` 和 `899050.BJ`；首次更新从本地最早日线之前一个月开始回补，之后重抓最近两个快照月。重复传入 `--index-weight` 可追加其他指数。原始权重单位为百分比。

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
    # bundle_root="~/.tualpha",  # 默认值；仅自定义时传入
    adjustment="qfq",  # qfq / hfq / raw
    execution_time="open",  # open / close
    benchmark="000300.SH",
    output_dir="outputs/demo",
    strategy_name="沪深300 ETF 趋势策略",
    show_progress=True,  # 默认使用 tqdm 显示交易日进度
)

print(result.summary())
```

回测默认使用 `tqdm` 显示已处理交易日、完成比例、运行速度、预计剩余时间和当前日期。批处理、测试或嵌套运行时可传入 `show_progress=False` 关闭。

运行后生成：

```text
outputs/demo/
├── report.html
└── daily_positions.csv
```

`daily_positions.csv` 使用 UTF-8-SIG 和长表结构。每个交易日都有一条 `CASH` 记录，持仓记录包含数量、可卖数量、成本、原始/复权收盘价、市值、权重和未实现盈亏。HTML 报告的组合归因表按标的展示成交次数、持有总天数、已实现盈亏、贡献占比和总费用；持有总天数按日终持仓大于零的交易日去重统计。

## 核心 API

策略回调内可使用：

- `symbol(code)`
- `order(asset, amount)`
- `order_value(asset, value)`
- `order_target(asset, target)`
- `order_target_value(asset, target)`
- `order_percent(asset, percent)`
- `order_target_percent(asset, target)`
- `cancel_order(order)` / `get_open_orders()`
- `record(**values)`
- `data.current(asset, field)`
- `data.history(asset, field, bar_count)`
- `data.fundamental(asset, field, period="latest")`
- `data.fundamentals(asset, fields, periods=4)`
- `data.index_constituents(index_code)`
- `data.available_fields(namespace=None)`
- `data.can_trade(asset)`

所有新订单默认只在下一交易日尝试一次。未成交订单会记录 `limit_up`、`limit_down`、`suspended`、`t_plus_one`、`invalid_lot` 等原因，并在 HTML 报告中汇总。

## 日频因子、行业和 ST 数据

扩展日频字段使用 `<数据集>.<字段>` 命名，并与价格一样支持 `current()` 和 `history()`：

```python
pe_ttm = data.current(context.asset, "daily_basic.pe_ttm")
net_flow = data.history(context.asset, "moneyflow.net_mf_amount", 20)
industry = data.current(context.asset, "industry.l1_name")
is_st = data.current(context.asset, "stock_st.is_st")
```

支持的数据集：

- `daily_basic`：估值、换手率、股本、市值等；单位保持 Tushare 原始口径；
- `moneyflow`：大小单量和金额，量为手、金额为万元；
- `industry`：申万一至三级行业代码及名称；
- `stock_st`：ST 名称、类型及虚拟字段 `is_st`，非 ST 日返回 `0`。

可通过 `data.available_fields("daily_basic")` 查看当前 Bundle 实际包含的字段。所有扩展日线字段都是 `daily_equities.bcolz` 的物理 CTable 列，例如 `ta_daily_basic__pe_ttm`；行业和 ST 字符串采用字典编码列，并由根属性中的字段注册表透明还原。回测不会读取 CSV 或 `normalized.duckdb`。

## PIT 历史指数成分与权重

```python
members = data.index_constituents("000300.SH")
# 索引：ts_code
# 列：asset、weight、snapshot_date
```

查询返回当前回调日前最新的月度快照，`weight` 保持 Tushare 百分比口径。为避免把快照日收盘后才能确定的数据提前使用，规则严格为 `snapshot_date < 当前回测日`：D 日快照从 D+1 首个回调开始可见；首个可见快照前返回空 DataFrame，快照之间沿用最近历史快照，绝不反向填充未来成分。无法映射到资产库的成分仍保留代码和权重，`asset` 为 `None`。

正式数据位于 Bundle 的 `index_constituents.sqlite`。Tushare 未提供公告时间和历史修订时间，因此该能力保证“快照日期 PIT”，不能还原供应商后续修订前的数据版本。

## 无未来函数的财务查询

财务字段必须使用 `fundamental()` 或 `fundamentals()`，不能通过 `current()` 读取：

```python
roe = data.fundamental(context.asset, "fina_indicator.roe")
revenue_2023 = data.fundamental(
    context.asset,
    "income.revenue",
    period="20231231",
)

reports = data.fundamentals(
    context.asset,
    [
        "fina_indicator.roe",
        "income.revenue",
        "balancesheet.total_assets",
        "cashflow.n_cashflow_act",
    ],
    periods=4,
)
```

四张财务宽表位于 Bundle 根目录的 `finance.sqlite`。为保守避免收盘后公告造成未来函数，财务记录从公告日后的首个交易日才可见，即查询要求 `coalesce(f_ann_date, ann_date) < 当前回测日`；同一报告期按当时可见的最新公告、`update_flag` 和修订顺序选择版本。利润表和现金流量表保留 Tushare 的年初至今累计口径，不自动伪造单季度或 TTM 数据。报表默认选择 `report_type="1"` 的合并报表。

## 复权与账户处理

- 前复权历史窗口：`原始价格 × 当日复权因子 / 当前回调日复权因子`
- 后复权：`原始价格 × 当日复权因子`

前复权窗口以策略当前可见的最后交易日为基准，延长回测结束日期不会改写过去回调看到的数据。成交、现金、费用和涨跌停判断始终使用原始价格。

持仓跨越复权因子变化时，框架按 Tushare 的“分红再投”总收益语义调整经济持仓数量和单位成本。这是只有复权因子、没有分红明细时的近似模型，并不等同于真实现金红利到账。

## 费用默认值

- 券商佣金：成交额 `0.03%`，最低 5 元，默认视为已含交易所经手费
- 股票卖出印花税：2023-08-28 前 `0.1%`，之后 `0.05%`
- 股票过户费：2022-04-29 前 `0.002%`，之后 `0.001%`
- ETF：不收股票印花税和股票过户费

可传入 `ChinaFeeModel` 修改费率、最低佣金和“佣金是否含经手费”的口径。

## Agent 策略编写 Skill

项目提供 [`.agents/skills/tualpha-strategy/`](.agents/skills/tualpha-strategy/) Skill，使 Agent 能按照 TuAlpha 的 D/D+1 时序、A 股交易规则、扩展字段和财务 PIT 语义编写策略。Pi 项目配置会自动发现该目录，也可显式调用：

```text
/skill:tualpha-strategy
```

详见 [`.agents/skills/README.md`](.agents/skills/README.md)。

## 已知边界

- 触及涨跌停并非交易所法律意义上的绝对不能成交；框架采用“涨停不买、跌停不卖”的保守流动性假设。
- 日内临时停牌在日频模型中按全天不可交易处理。
- 不模拟涨跌停排队、盘口深度、部分成交和冲击成本。
- `etf_basic.csv` 不提供退市日期；退市 ETF 在缺少额外清算数据时以最后交易日作为资产结束日。
