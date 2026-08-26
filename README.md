<h1 align="center">TuAlpha</h1>

<p align="center">
  面向中国 A 股与 ETF 的日频事件驱动回测框架
</p>

<p align="center">
  <a href="https://pypi.org/project/tualpha/">
    <img src="https://img.shields.io/pypi/v/tualpha?style=flat-square&color=007ec6" alt="PyPI Version">
  </a>
  <a href="https://tualpha.readthedocs.io/">
    <img src="https://readthedocs.org/projects/tualpha/badge/?version=latest" alt="Documentation Status">
  </a>
  <img src="https://img.shields.io/badge/python-3.12-blue?style=flat-square" alt="Python 3.12">
  <img src="https://img.shields.io/badge/storage-Parquet%20%2B%20DuckDB-orange?style=flat-square" alt="Parquet and DuckDB">
  <img src="https://img.shields.io/badge/data-Tushare-green?style=flat-square" alt="Tushare">
</p>

**TuAlpha** 是一款针对中国 A 股股票与 ETF 的日频量化研究和事件驱动回测框架。它提供简洁的 Python 策略 API，以按年分区的 **Parquet** 作为本地事实数据源，并使用 **DuckDB** 完成查询、分区裁剪和质量检查。

🚀 **核心亮点：**

- **真实市场时序**：D 日读取截至当日的数据并决策，订单最早 D+1 成交；内置 T+1、停牌、涨跌停、交易单位和现金约束。
- **Point-in-time 数据**：财务数据按公告日期可见，指数成分使用严格早于当前回调日的最新权重快照，避免未来函数。
- **高效本地数据层**：日频数据按年分区，DuckDB 执行投影和过滤下推；DataPortal 使用列缓存、Arrow 分批读取和固定资产池预取。
- **原子增量更新**：只重写受影响年度分区，复用未变化文件，并通过 staging、generation、文件锁和 rollback 原子发布。
- **完整数据质量报告**：覆盖 schema、主键、分区、日期、OHLC、复权、财务 PIT、指数权重和跨表引用，输出 HTML、JSON 与 CSV。
- **可解释回测结果**：生成中文 Plotly HTML 报告、每日持仓、订单、成交、已平仓交易、组合归因和用户自定义记录。

👉 **[在线文档](https://tualpha.readthedocs.io/)** | **[架构说明](docs/architecture.md)** | **[Bundle 格式](docs/bundle-format.md)** | **[策略 Skill](.agents/skills/tualpha-strategy/SKILL.md)**

## 安装说明

TuAlpha 要求 **Python 3.12**，推荐使用 [`uv`](https://docs.astral.sh/uv/) 从 PyPI 安装：

```bash
uv add tualpha
```

源码开发：

```bash
git clone https://github.com/joshuaxql/tualpha.git
cd tualpha
uv sync --dev
```

验证安装：

```bash
uv run python -c "import tualpha; print(tualpha.__version__)"
```

## 快速开始

下面是一个 20 日均线 ETF 策略。D 日收盘后计算信号，订单由框架在 D+1 开盘撮合：

```python
from tualpha import order_target_percent, record, run_algorithm, symbol


def initialize(context):
    context.asset = symbol("510300.SH")


def handle_data(context, data):
    closes = data.history(context.asset, "close", 20).dropna()
    if len(closes) < 20:
        record(ready=0, target_weight=0.0)
        return

    ma20 = float(closes.mean())
    close = float(closes.iloc[-1])
    target = 0.95 if close > ma20 else 0.0

    if data.can_trade(context.asset):
        order_target_percent(context.asset, target)

    record(ready=1, close=close, ma20=ma20, target_weight=target)


result = run_algorithm(
    start="2020-01-01",
    end="2026-08-25",
    initialize=initialize,
    handle_data=handle_data,
    capital_base=1_000_000,
    adjustment="qfq",
    execution_time="open",
    benchmark="000300.SH",
    output_dir="outputs/ma20_etf",
    strategy_name="沪深300ETF MA20",
)

print(result.summary())
```

输出目录：

```text
outputs/ma20_etf/
├── report.html
└── daily_positions.csv
```

策略可通过 `result.orders`、`result.transactions`、`result.closed_trades`、`result.records` 和 `result.performance` 继续分析结构化结果。

## 策略语义

### 每日事件顺序

```text
D 日应用公司行动
  → 撮合 D 日之前提交且已到期的订单
  → 使用 D 日原始收盘价盯市
  → handle_data 读取截至 D 日的数据
  → 提交最早 D+1 成交的订单
```

`execution_time="open"` 使用 D+1 原始开盘价，`execution_time="close"` 使用 D+1 原始收盘价。无论策略使用 `raw`、`qfq` 还是 `hfq`，成交、现金、费用和涨跌停判断始终使用原始价格。

### 财务 PIT

```text
coalesce(f_ann_date, ann_date) < 当前回测日
end_date <= 当前回测日
```

公告日当天不可见，从公告日后的首个交易日开始可见。同一报告期选择当时可见的最新公告或修订。

### 指数成分 PIT

```text
max(snapshot_date) < 当前回测日
```

快照日当天不可见。快照之间沿用最近历史权重，不使用未来快照反向填充。

## 本地数据

默认数据根目录为 `~/.tualpha`：

```text
~/.tualpha/
├── bundle/
│   ├── manifest.json
│   ├── catalog.duckdb
│   └── parquet/
│       ├── stock/
│       ├── etf/
│       └── index/
├── reports/quality/<run_id>/
├── backups/
├── .locks/
├── .staging/
├── .rollback/
└── update-status.json
```

日频表使用 `year=YYYY/data.parquet`，财务表使用 `report_year=YYYY/data.parquet`，指数权重使用 `index_code=CODE/year=YYYY/data.parquet`。

### 数据范围

- **A 股**：交易日历、基础信息、日线、复权因子、每日指标、资金流、涨跌停、停复牌、历史 ST、申万行业和四类财务表。
- **ETF**：基础信息、日线和复权因子。
- **指数基础信息**：保留完整本地基础表。
- **指数日线**：仅保存配置的 15 个宽基指数。
- **指数权重**：默认维护 `000300.SH`、`000852.SH`、`000905.SH`、`000906.SH` 和 `899050.BJ`，可通过 CLI 扩展。

## 全量构建与增量更新

Token 只能通过环境变量或标准输入提供：

```bash
export TUSHARE_TOKEN="your-token"
```

### Tushare 全量构建

首次使用或需要完全重建本地数据时，从 Tushare 直接构建完整 Bundle：

```bash
uv run tualpha build --from 20100101
```

可限制结束日期或扩展指数权重：

```bash
uv run tualpha build --from 20100101 --to 20260825
uv run tualpha build --from 20100101 --index-weight 000016.SH
uv run tualpha build --from 20100101 --dry-run --json
```

全量构建与增量更新共用 Tushare 下载、可续传分区缓存、Parquet Writer、Catalog、manifest、校验和原子发布管线。`--from` 必填；长历史构建会产生大量 Tushare 请求，应确认接口权限和配额。

### Tushare 增量更新

```bash
uv run tualpha update
```

常用命令：

```bash
uv run tualpha update --from 20260801 --to 20260825
uv run tualpha update --repair-from 20260701
uv run tualpha update --lookback 20
uv run tualpha update --index-weight 000016.SH
uv run tualpha update --dry-run --json
```

默认只下载各日频数据集缺失的交易日，不再重复刷新最近 10 个交易日。显式使用 `--from`、`--repair-from` 或 `--lookback N` 时，才会强制重下指定区间。交易日北京时间 17:00 起允许更新当日数据；17:00 前以及非交易日自动截止到最近已完成交易日。

财务增量固定获取最近两个已结束季度的报告：资产负债表、利润表、现金流量表和财务指标四个 VIP 接口各按季度调用两次，共 8 次请求。更新失败不会修改活动 generation；成功后从最终目录重新打开 Reader 验证。

## DuckDB 本地查询

```python
from tualpha import local_data

with local_data() as db:
    daily = db.query(
        "stock_daily",
        fields="ts_code,trade_date,close",
        filters={"ts_code": "000001.SZ"},
        start_date="20240101",
        end_date="20241231",
    )

    breadth = db.sql(
        """
        SELECT trade_date, count(*) AS asset_count
        FROM stock_daily
        WHERE trade_date >= ?
        GROUP BY trade_date
        ORDER BY trade_date
        """,
        ["20240101"],
    )
```

SQL 接口只接受只读语句。策略回调不得绕过 DataPortal 直接读取 Parquet 或 Catalog；`local_data()` 面向离线研究、检查和数据维护。

## 数据质量

```bash
uv run tualpha quality
uv run tualpha quality --table stock_daily --table income
uv run tualpha quality --full-hash --json
```

每次报告包含：

```text
summary.csv
findings.csv
metrics.csv
report.json
report.html
```

列级指标只记录部分缺失，不输出零值或数据源默认未返回的全空列。

## 可视化报告

指定 `output_dir` 后，TuAlpha 自动生成包含以下内容的交互式报告：

- 组合与基准累计收益；
- 回撤和月度收益；
- 风险收益指标；
- 费用和拒单原因；
- 已实现盈亏、盈亏贡献占比和每日权重贡献累计；
- 每日持仓 CSV。

佣金被视为包含交易所经手费，不单独计算或展示经手费。股票印花税和过户费按日期分段，ETF 不收股票印花税和股票过户费。

## 文档索引

- 🌐 **[Read the Docs 在线文档](https://tualpha.readthedocs.io/)**：安装、策略、数据、API、CLI、架构和版本文档。
- 📖 **[架构说明](docs/architecture.md)**：模块职责、事件顺序、查询热路径和原子发布。
- 💾 **[Bundle 格式](docs/bundle-format.md)**：Parquet 分区、Catalog、manifest、PIT 和完整性协议。
- 🧠 **[策略 Skill](.agents/skills/tualpha-strategy/SKILL.md)**：策略生成、迁移、审查和验证规范。
- 📚 **[策略 API](.agents/skills/tualpha-strategy/references/api-reference.md)**：资产、行情、订单、财务和结果 API。
- ⏱️ **[时序契约](.agents/skills/tualpha-strategy/references/framework-contract.md)**：D/D+1、T+1、费用和未来函数边界。
- 🧾 **[数据字段](.agents/skills/tualpha-strategy/references/data-fields.md)**：日线、估值、资金流、行业、ST 和财务字段。

## 🧪 测试与质量保证

TuAlpha 使用单元测试和真实 Bundle 验证关键业务规则：

- D 日决策与 D+1 成交；
- A 股/ETF T+1；
- 涨跌停、停牌和交易单位；
- `raw`、`qfq`、`hfq`；
- 财务和指数成分 PIT；
- Parquet 增量更新、原子发布和幂等性；
- DuckDB 查询、质量报告和回测报告。

运行检查：

```bash
uv sync --dev
uv run ruff check .
uv run ruff format --check .
uv run pytest -q
```

文档严格构建：

```bash
uv sync --group docs
uv run mkdocs build --strict
```

当前测试基线：

```text
94 passed
```

## 支持边界

- 仅支持日频回测；
- 仅支持多头现金账户；
- 股票和 ETF 可交易，指数只能作为基准或 PIT 成分来源；
- 不支持卖空、融资融券、期货、期权、分钟撮合或 ETF 申赎；
- 日频成交模型不模拟盘口深度和部分成交队列。

## 免责声明

本项目仅用于量化研究、软件开发和回测验证，不构成任何投资建议。历史回测结果不代表未来收益。
