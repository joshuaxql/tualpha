# TuAlpha Parquet Bundle 格式

默认位置：`~/.tualpha/bundle/`。协议为 `tualpha.parquet/1`，schema version 为 `1`。

## 目录

```text
bundle/
├── manifest.json
├── catalog.duckdb
└── parquet/
    ├── stock/
    │   ├── trade_cal/exchange=SSE/data.parquet
    │   ├── basic/data.parquet
    │   ├── daily/year=YYYY/data.parquet
    │   ├── adj_factor/year=YYYY/data.parquet
    │   ├── daily_basic/year=YYYY/data.parquet
    │   ├── moneyflow/year=YYYY/data.parquet
    │   ├── stk_limit/year=YYYY/data.parquet
    │   ├── suspend_d/year=YYYY/data.parquet
    │   ├── stock_st/year=YYYY/data.parquet
    │   ├── industry/year=YYYY/data.parquet
    │   └── finance/<table>/report_year=YYYY/data.parquet
    ├── etf/
    │   ├── basic/data.parquet
    │   ├── daily/year=YYYY/data.parquet
    │   └── adj_factor/year=YYYY/data.parquet
    └── index/
        ├── basic/data.parquet
        ├── daily/year=YYYY/data.parquet
        └── weight/index_code=CODE/year=YYYY/data.parquet
```

日频按年分区而不是按日分区，以避免数万小文件。文件按日期和业务主键排序，ZSTD 压缩，行组大小约 122,880 行。

## manifest.json

记录：

- protocol、schema version、generation UUID、生成时间；
- 起止交易日、交易日数、资产数和指数数；
- 构建管线和 PIT 规则；
- 每张表的目录、glob、字段和主键；
- 每个 Parquet/Catalog 文件的大小、行数、行组数和 SHA-256。

Reader 打开 Bundle 时验证 manifest 与 `catalog.duckdb` generation 一致，并检查文件集合和大小。`tualpha quality --full-hash` 重新计算完整 SHA-256。

## catalog.duckdb

Catalog 只保存体积小、需要快速索引的框架元数据：

- `bundle_metadata`：protocol、schema、generation；
- `assets`：稳定 SID、代码、类型、上市区间、交易单位；
- `trade_calendar`：SSE 日历索引；
- `datasets`：表路径、行数、分区数和日期字段。

市场数据仍以 Parquet 为唯一事实来源。打开本地查询客户端时，根据活动 Bundle 绝对路径创建临时 DuckDB views，不在 Catalog 中固化外部绝对路径。

## 行情单位

Parquet 日线统一字段：

```text
trade_date, ts_code, open, high, low, close, pre_close, volume, turnover
```

- 股票 `volume`：股；
- ETF `volume`：份；
- 指数 `volume`：Tushare 原始手数；
- `turnover`：元；
- OHLC 和 `pre_close`：原始未复权值。

复权因子独立保存：

```text
qfq(i, D) = raw(i) × factor(i) / factor(D)
hfq(i)    = raw(i) × factor(i)
```

## 财务 PIT

财务表：`balancesheet`、`income`、`cashflow`、`fina_indicator`。

额外保存：

```text
effective_ann_date = coalesce(f_ann_date, ann_date)
source_order
```

可见性：

```text
effective_ann_date < session
end_date <= session
```

同报告期选择当时可见的最新公告，`update_flag=1` 优先，最后使用 `source_order` 打破平局。

## 指数权重 PIT

目录主键为 `(index_code, trade_date, con_code)`，权重单位为百分比。查询使用：

```text
max(trade_date) WHERE trade_date < session
```

快照日当天不可见，首个快照前返回空表。

## 增量更新

```text
Tushare
  → .staging/update/<run_id>/cache/ 分区 CSV + checksum
  → hardlink 复用未变化 Parquet
  → 权威替换目标日期并重写受影响年份
  → 重建 catalog.duckdb 和 manifest.json
  → 关键约束、文件和 Reader 校验
  → bundle.lock
  → 原子替换 bundle/
```

日频增量按数据集检查，只下载缺失的交易日期；交易日北京时间 17:00 起可纳入当天。显式使用 `--from`、`--repair-from` 或正数 `--lookback` 时才强制刷新区间。财务增量获取最近两个已结束季度，按记录的 `end_date` 重写对应报告年份。指数权重按整个 `(index_code, snapshot_date)` 快照替换。失败时活动 generation 不变；下载缓存可在 24 小时内续传。

## 原子发布

1. 在 staging 构建新 generation；
2. 校验结构、关键约束及 Catalog；
3. 获取 `bundle.lock`；
4. 活动 `bundle/` 移到 `.rollback/`；
5. staging Bundle 原子移动到活动路径；
6. 从最终路径重新打开；
7. 成功后删除 rollback。