# TuAlpha Bundle 文件格式

本文描述 TuAlpha 0.6、Bundle schema v5 的固定目录格式。CSV 目录与 Bundle 根目录必须分离，默认发布位置为：

```text
~/.tualpha/bundles/tualpha/
```

Bundle 不使用 Zipline 的时间戳子目录。需要底层官方 Reader 时使用：

```python
from tualpha.bundle import load_bundle_data

with load_bundle_data() as bundle:
    print(bundle.asset_finder.retrieve_asset(1))
```

## 1. 最终目录

```text
bundles/tualpha/
├── assets-7.sqlite
├── daily_equities.bcolz/
├── index_daily.bcolz/
├── minute_equities.bcolz/
├── adjustments.sqlite
├── finance.sqlite
├── index_constituents.sqlite
├── manifest.json
└── READY
```

schema v5 **不生成、不读取 `tualpha.duckdb`**。`normalized.duckdb` 仅位于 Bundle 外部的 `cache/`，是可重建的构建缓存，不是回测数据源。

## 2. `assets-7.sqlite`

由 Zipline `AssetDBWriter` 创建，保存股票和 ETF 的稳定 `sid`、完整 Tushare 代码、证券名称、生命周期和交易所。

| 表 | 内容 |
|---|---|
| `equities` | `sid`、名称、起止日期、首次交易日、自动关闭日和交易所。日期为 Unix 纳秒整数。 |
| `equity_symbol_mappings` | `sid` 与 `000001.SZ` 等完整代码的有效期映射。 |
| `equity_supplementary_mappings` | TuAlpha 使用 `asset_type`、`board`、`price_tick` 三类补充映射。 |
| `asset_router`、`version_info`、`exchanges` | Zipline 标准资产路由、schema 版本和交易所。 |

股票与 ETF 都属于 Zipline `equity`，但 `asset_type` 仍分别保存为 `stock`、`etf`，供交易单位、费用和规则判断使用。指数不写入资产库，因此不能成为可交易资产。

## 3. `daily_equities.bcolz/`

这是股票与 ETF 的主日线 CTable。前七列由官方 `BcolzDailyBarWriter` 创建；TuAlpha 随后在**同一张 CTable 中直接追加全部扩展日线列**。所有列长度完全相同，同一行对应相同的 `sid + day`。

### 3.1 Zipline 标准列

| 物理列 | dtype | 含义 |
|---|---|---|
| `open`、`high`、`low`、`close` | `uint32` | 原始未复权价格乘 1000；官方 Reader 自动除以 1000，0 表示缺失。 |
| `volume` | `uint32` | 成交股数/份数；超过 `uint32` 上限时封顶。 |
| `day` | `uint32` | UTC Unix 秒表示的交易日。 |
| `id` | `uint32` | 稳定整数 `sid`。 |

根属性中的 `first_row`、`last_row` 和 `calendar_offset` 定位每个资产的连续数据块。

### 3.2 TuAlpha 基础扩展列

| 物理列 | API 字段 | dtype | 含义 |
|---|---|---|---|
| `ta_pre_close` | `pre_close` | `float64` | 原始昨收价，元。 |
| `ta_turnover` | `turnover` | `float64` | 成交额，元。 |
| `ta_volume_exact` | `volume` | `float64` | 精确成交股数/份数，由 Tushare 手数乘 100；不受官方 `uint32` 上限影响。 |
| `ta_up_limit`、`ta_down_limit` | 同名 | `float64` | 涨跌停价格，元。 |
| `ta_adj_factor` | `adj_factor` | `float64` | 股票或 ETF 复权因子。 |
| `ta_suspended` | `suspended` | `uint8` | 0/1 停牌状态。 |

前复权、后复权不会改写原始 OHLC；DataPortal 使用 `ta_adj_factor` 在查询时计算。

### 3.3 命名空间扩展列

公开字段 `<dataset>.<field>` 映射为物理列 `ta_<dataset>__<field>`：

```text
daily_basic.pe_ttm          → ta_daily_basic__pe_ttm
moneyflow.net_mf_amount     → ta_moneyflow__net_mf_amount
industry.l1_name            → ta_industry__l1_name
stock_st.is_st              → ta_stock_st__is_st
```

数据集：

- `daily_basic`：估值、换手率、量比、股本和市值等全部可用字段；
- `moneyflow`：各档买卖量、金额及净流入；
- `industry`：申万一至三级行业代码和名称；
- `stock_st`：风险名称、类型、类型名称及虚拟字段 `is_st`。

数值列使用 `float64`、缺失为 `NaN`；标志列使用 `uint8`。行业和 ST 字符串使用 `int32` 字典编码，`-1` 表示缺失。根属性 `tualpha_fields` 保存：

```json
{
  "industry.l1_name": {
    "column": "ta_industry__l1_name",
    "kind": "categorical",
    "categories": ["交通运输", "银行", "电子"]
  }
}
```

DataPortal 自动解码，策略看到的仍是中文字符串。字典编码避免在 1600 万行上重复保存定长 Unicode，并保持 Bcolz 列式压缩效率。

根属性 `tualpha_schema_version=5` 表示扩展布局版本。构建器会验证每个扩展列与标准 CTable 等长。

## 4. `index_daily.bcolz/`

指数只用于报告基准，不进入交易资产集合。为避免把上万个指数复制成股票/ETF 的稠密扩展行，指数使用独立的稀疏 Bcolz CTable。

| 列 | dtype | 含义 |
|---|---|---|
| `open`、`high`、`low`、`close`、`pre_close` | `float64` | 指数原始点位。 |
| `volume`、`turnover` | `float64` | Tushare 原始指数成交量和换算后的成交额。 |
| `day` | `uint32` | UTC Unix 秒交易日。 |
| `id` | `uint32` | 指数内部稳定 sid。 |

根属性 `first_row`、`last_row` 定位指数块；`manifest.json` 的 `benchmark_sids` 保存指数代码到 sid 的映射。`benchmark_returns()` 直接读取该文件，不允许策略下单指数。

## 5. `index_constituents.sqlite`

标准 SQLite 数据库，保存月度指数成分权重快照。表 `index_constituents` 包含：

| 字段 | 含义 |
|---|---|
| `index_code` | 指数 Tushare 代码。 |
| `snapshot_date` | 权重快照日期，ISO `YYYY-MM-DD`。 |
| `con_code` | 成分证券 Tushare 代码。 |
| `sid` | 可映射时保存稳定资产 ID，否则为空。 |
| `weight` | Tushare 原始百分比权重，通常合计约 100。 |

主键为 `(index_code, snapshot_date, con_code)`，并建立按指数快照和成分代码查询的索引。`index_constituent_metadata` 保存 schema、Bundle generation、权重单位以及 PIT 规则。

策略通过 `data.index_constituents(index_code)` 查询。为采用保守可见时点，SQL 只选择：

```text
max(snapshot_date) where snapshot_date < 当前回测日
```

因此 D 日快照从 D+1 回调开始可见；首个快照前返回空集，两个快照之间沿用最近历史快照。Tushare 没有公告时间或历史修订时间，因此不能还原供应商后续修订前的数据版本。

默认维护 `000300.SH`、`000852.SH`、`000905.SH`、`000906.SH` 和 `899050.BJ`，其他代码可通过 `tualpha update --index-weight CODE` 追加。

## 6. `finance.sqlite`

标准 SQLite 数据库，保存四张公告时点财务宽表：

- `financial_balancesheet`
- `financial_income`
- `financial_cashflow`
- `financial_fina_indicator`

每张表包含 Tushare 文档中的全部可用数值字段，并统一保存：

| 字段 | 含义 |
|---|---|
| `sid` | 与资产库一致的稳定资产 ID。 |
| `ann_date` | 公告日期，ISO `YYYY-MM-DD` 文本。 |
| `f_ann_date` | 实际公告日期；源数据没有时为空。 |
| `effective_ann_date` | `coalesce(f_ann_date, ann_date)`。 |
| `end_date` | 报告期。 |
| `report_type`、`comp_type`、`end_type` | 报表、公司和期间类型。 |
| `update_flag` | Tushare 修订标志。 |
| `source_order` | 同一公告内重复记录的稳定选择顺序。 |

每张表建立 `(sid, effective_ann_date, end_date, report_type, update_flag, source_order)` 索引。

### Point-in-time 规则

回测严格要求：

```text
effective_ann_date < 当前回测日
end_date <= 当前回测日
```

因此公告日当天不可见，从公告日后的首个交易日开始可见。同一报告期按以下顺序选择当时已知版本：

1. 最新 `effective_ann_date`；
2. `update_flag=1` 优先；
3. 最新 `source_order`。

默认只读取 `report_type=1` 的合并报表；`fina_indicator` 没有该过滤。利润表和现金流量表保留 Tushare 年初至今累计口径，不自动伪造单季度或 TTM。

`finance_metadata` 保存 schema 版本和 PIT 规则。发布前执行 `PRAGMA integrity_check`。

## 7. 其他官方文件

### `minute_equities.bcolz/`

TuAlpha 是日频框架，因此这是 Zipline 要求的合法空分钟存储，仅含 `metadata.json`。

### `adjustments.sqlite`

由 `SQLiteAdjustmentWriter` 创建。`mergers` 表记录复权因子变化：

```text
ratio = 前一交易日复权因子 / 当前复权因子
```

`dividends`、`dividend_payouts`、`stock_dividend_payouts` 和 `splits` 当前为空。只有复权因子时，账户层采用“分红再投”经济近似，不等同于真实现金分红到账。

## 8. `manifest.json` 与 `READY`

`manifest.json` 主要字段：

- `schema_version=5`
- `bundle_name`、`generated_at`
- `start_session`、`end_session`
- `asset_count`
- `volume_unit`、`volume_multiplier`、`volume_overflow`
- `settlement_days=1`
- `daily_extensions=daily_equities.bcolz`
- `index_prices=index_daily.bcolz`
- `index_constituents=index_constituents.sqlite`
- `index_constituent_codes`、快照/行数、日期范围、权重单位和 PIT 规则
- `finance=finance.sqlite`
- `benchmark_sids`

`generated_at` 同时是 Bundle generation 标识。公开构造的 `AssetFinder`、`ChinaTradingCalendar` 和 `BundleDataPortal` 会校验 generation，拒绝混合更新前后的文件。

只有在以下检查全部通过后才写 `READY`：

- 必需文件齐全；
- `tualpha.duckdb` 不存在；
- 全部日线扩展列等长且字段注册表完整；
- 指数 Bcolz 列等长；
- `finance.sqlite` 完整性及 schema 正确；
- `index_constituents.sqlite` 完整性、generation、PIT 规则、权重范围和 manifest 计数正确；
- Zipline 官方 Readers 能打开资产、日线、分钟和调整文件。

## 9. Bundle 外构建缓存

### `cache/tualpha/normalized.duckdb`

它把按交易日分区的 CSV 转置为构建友好的表：`prices`、`factors`、`limits`、`suspensions`、`index_prices` 和 `store_metadata`。更新器按日期增量同步。

它不是发布文件。正常回测、资产查询、日频扩展查询、指数基准和财务查询都不会连接 DuckDB。

### `cache/tualpha/sid-map.json`

保存股票、ETF 和内部指数代码的稳定 sid。新增代码只分配新 ID，不改变已有映射。

### `.locks/` 与 `.staging/`

- `.locks/` 防止并行构建和发布；Reader 生命周期内持有同一把锁；
- `.staging/` 先完整生成 schema v5，验证后再原子替换固定 Bundle；
- Windows 上现有 Bcolz/SQLite Reader 关闭前，更新会等待而不是强行重命名打开的文件。

旧 schema 不原地修改，必须从 CSV/normalized cache 在 staging 中重建后整体切换。

## 10. 数据流

```text
显式 CSV 目录
    │ update/sync
    ▼
cache/tualpha/normalized.duckdb       # 仅构建缓存
    │
    ├── Zipline Writers ───────────────┐
    ├── Bcolz extension writer ────────┤
    ├── finance SQLite writer ─────────┤
    └── constituent SQLite writer ─────┤
                                       ▼
bundles/tualpha/
    ├── assets-7.sqlite
    ├── daily_equities.bcolz   # OHLCV + 扩展日线列
    ├── index_daily.bcolz
    ├── adjustments.sqlite
    ├── finance.sqlite
    └── index_constituents.sqlite
             │
             ▼
         回测引擎
```

正常回测不会访问 CSV 或 `normalized.duckdb`。
