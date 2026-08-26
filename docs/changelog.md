# 版本记录

## 1.3.3

- 新增策略回调指数日线读取接口 `data.index_current()`；
- 新增回调可见历史窗口接口 `data.index_history()`；
- 支持原始指数 OHLC、昨收、成交量和成交额；
- `data.available_fields("index")` 可发现支持字段；
- 指数仍不可交易，指数日线不参与 `raw`、`qfq` 或 `hfq` 复权。

## 1.3.2

- 日频增量改为按数据集检查，只下载缺失交易日；
- 默认不再重复下载最近 10 个交易日；
- 交易日北京时间 17:00 起允许更新当天数据；
- 财务增量固定获取最近两个已结束季度，四类 VIP 接口共 8 次请求；
- 保留 `--from`、`--repair-from` 和 `--lookback` 的显式强制刷新能力。

## 1.3.1

- 原生支持 Pandas 3；
- 日期存续判断改用交易日整数，消除微秒/纳秒分辨率差异；
- 兼容 Pandas 3 默认字符串 dtype 和 Arrow 转换；
- DuckDB Arrow Reader 直接写入 NumPy 列缓存。

## 1.3.0

- 数据层从 HDF5 迁移到年度分区 Parquet + DuckDB；
- 增加 `tualpha build`、原子增量更新、rollback 和可续传 CSV 缓存；
- 增加 `local_data()` 只读查询和完整表级质量报告；
- 增加 DataPortal 列缓存、Arrow 分批读取和 `prefetch()`；
- 财务与指数成分保持严格 Point-in-time 语义；
- 佣金统一视为包含交易所经手费。

## 兼容性

| TuAlpha | Python | Pandas | 存储 |
|---|---:|---:|---|
| 1.3.3 | 3.12+ | 3.0+ | Parquet + DuckDB |
| 1.3.2 | 3.12+ | 3.0+ | Parquet + DuckDB |
| 1.3.1 | 3.12+ | 3.0+ | Parquet + DuckDB |
| 1.3.0 | 3.12+ | 2.2+ | Parquet + DuckDB |

完整发行文件见 [PyPI](https://pypi.org/project/tualpha/)，源码历史见 [GitHub Releases](https://github.com/joshuaxql/tualpha/releases)。
