# 版本记录

## 1.4.1

- 长周期、大持仓回测使用紧凑数值缓冲并及时释放中间数据，降低内存峰值；
- 持仓数据预取内存不足时自动回退到有界查询，避免回测进程中断；
- 持仓 CSV 和 HTML 报告改为原子写入，并对 Windows 短暂文件占用进行重试；
- Bundle 更新增强网络重试、断点续传、分区复用和空批次处理，保留文件锁与原子发布语义；
- 性能基准更新为国证 2000（`399303.SZ`）指数增强策略。

## 1.4.0

- DataPortal 自动为实际持仓预热收盘价、昨收价和复权因子，完整中证 1000 性能基准吞吐量提升约 9 倍；
- 日线缓存保留双精度，并继续遵守 D/D+1、T+1、涨跌停、停牌、费用和 PIT 可见性规则；
- 将配置与异常、绩效结果以及报告生成分别迁入 `foundation/`、`analysis/` 和 `report/` 职责包；
- 根目录模块改为轻量兼容门面，既有 `tualpha.config`、`tualpha.metrics`、`tualpha.reporting` 等导入路径保持可用；
- 拆分报告的图表、归因、格式化和 HTML 组装逻辑，并增加模块边界兼容测试。
- 新增可重复的 `--index-daily CODE`，支持定向回填非默认指数日线，并与 `--index-weight CODE` 配合维护自定义指数。

## 1.3.4

- 新增 `data.fundamental_arrays()`，用每张财务表一次查询批量读取大型横截面的最新 PIT 财务值；
- 批量结果保持输入资产顺序并以只读 NumPy 数组返回，财务公告日与修订可见性和单资产接口一致；
- 批量财务查询使用哈希聚合选择最新记录，避免逐资产查询和窗口排序导致的内存峰值；
- 单资产 `fundamental()` / `fundamentals()` 只读取请求字段并增量扩展窄缓存，不再加载财务宽表全部列；
- 指数日线改为请求字段投影和增量缓存，多字段当前值合并为一次读取；
- PIT 指数成分只加载快照日期索引和实际访问的月度快照，不再一次读入指数全部历史成分。

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
| 1.4.1 | 3.12+ | 3.0+ | Parquet + DuckDB |
| 1.4.0 | 3.12+ | 3.0+ | Parquet + DuckDB |
| 1.3.4 | 3.12+ | 3.0+ | Parquet + DuckDB |
| 1.3.3 | 3.12+ | 3.0+ | Parquet + DuckDB |
| 1.3.2 | 3.12+ | 3.0+ | Parquet + DuckDB |
| 1.3.1 | 3.12+ | 3.0+ | Parquet + DuckDB |
| 1.3.0 | 3.12+ | 2.2+ | Parquet + DuckDB |

完整发行文件见 [PyPI](https://pypi.org/project/tualpha/)，源码历史见 [GitHub Releases](https://github.com/joshuaxql/tualpha/releases)。
