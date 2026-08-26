# 数据管理

TuAlpha 使用 Tushare 构建本地 Parquet Bundle。默认根目录为 `~/.tualpha`，市场数据以 Parquet 为唯一事实来源，DuckDB 提供 Catalog、查询与质量检查。

## 数据目录

```text
~/.tualpha/
├── bundle/
│   ├── manifest.json
│   ├── catalog.duckdb
│   └── parquet/
├── reports/quality/<run_id>/
├── backups/
├── .locks/
├── .staging/
├── .rollback/
└── update-status.json
```

物理分区和字段协议见 [Bundle 格式](bundle-format.md)。

## Token

```bash
export TUSHARE_TOKEN="your-token"
```

也可以通过标准输入安全提供：

```bash
echo "$TUSHARE_TOKEN" | uv run tualpha update --token-stdin
```

不要把 Token 写入源码、命令输出、`.env` 提交或文档示例。

## 全量构建

首次使用必须指定起始日期：

```bash
uv run tualpha build --from 20100101
```

常用参数：

```bash
uv run tualpha build --from 20100101 --to 20260825
uv run tualpha build --from 20100101 --index-weight 000016.SH
uv run tualpha build --from 20200101 --dry-run --json
```

全量构建会下载完整交易日区间，并在 staging 中生成全新的 Bundle。长历史构建会产生大量 Tushare 请求，应先确认接口权限和配额。

## 增量更新

```bash
uv run tualpha update
```

默认行为：

1. 刷新股票、ETF、指数基础信息和 SSE 交易日历；
2. 按日频数据集检查已有交易日期；
3. 只下载缺失的数据集和交易日；
4. 获取最近两个已结束季度的财务报告；
5. 更新指数权重；
6. 构建、校验并原子发布新 generation。

默认不会重复下载最近 10 个交易日。需要显式修复时使用：

```bash
uv run tualpha update --from 20260801 --to 20260825
uv run tualpha update --repair-from 20260701
uv run tualpha update --lookback 20
```

`--from`、`--repair-from` 和正数 `--lookback` 会强制刷新对应区间。

### 当日可用时间

以北京时间为准：

- 交易日 17:00 前：截止到前一个已完成交易日；
- 交易日 17:00 起：允许更新当天；
- 非交易日：截止到最近交易日。

### 财务请求

增量更新固定获取最近两个已结束季度：

```text
balancesheet_vip    × 2
income_vip          × 2
cashflow_vip        × 2
fina_indicator_vip  × 2
```

共 8 次 VIP 请求，参数使用 `period=YYYYMMDD`。全量构建仍按完整历史季度下载。

### 失败恢复

活动 Bundle 从不原地修改。流程为：

```text
Tushare → 可续传 CSV 缓存 → 年度 Parquet → Catalog/manifest
        → 关键约束校验 → bundle.lock → 原子发布
```

失败时：

- 活动 generation 保持不变；
- staging 和失败说明保留；
- 24 小时内可以续传完整的 CSV 下载缓存；
- 发布中断可通过 rollback 恢复。

## 本地查询

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

`sql()` 只接受只读查询。策略回调不得绕过 DataPortal 使用 `local_data()`，该接口面向离线研究、检查和维护。

## 数据质量

```bash
uv run tualpha quality
uv run tualpha quality --table stock_daily --table income
uv run tualpha quality --full-hash --json
```

报告目录包含：

```text
summary.csv
findings.csv
metrics.csv
report.json
report.html
```

覆盖 schema、主键、分区、日期、OHLC、复权、财务 PIT、指数权重和跨表引用。命令在存在 `fail` 时返回非零退出码。

## Bundle 验证

```bash
uv run tualpha compact
```

该兼容命令不会重新压缩年度 Parquet，只验证活动 Bundle、Catalog 和 manifest。
