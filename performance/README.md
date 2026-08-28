# 国证 2000 指数增强性能策略

## 中证 1000 复杂因子分析基准

`csi1000_complex_factor_analysis.py` 使用价量、动量、波动和市值组合成一个最大回看 79 个交易日的复杂因子，并生成包含 1、5、10 日预测周期的完整因子报告：

```text
RANK(0.30*TS_ZSCORE(RETURNS($close,20),60)
     -0.25*TS_ZSCORE(STD(RETURNS($close,1),20),60)
     +0.20*TS_ZSCORE($volume/MA($volume,20),20)
     -0.15*TS_ZSCORE(ATR($close,14)/$close,60)
     +0.10*RANK(1/$daily_basic.total_mv))
```

默认使用 2017-01-01 至 2026-01-01 的 PIT 中证 1000 成分、前复权价格，剔除 ST、上市不足 365 天和停牌股票，并进行行业与市值联合中性化：

```bash
uv run python performance/csi1000_complex_factor_analysis.py --label after
```

基准分别记录数据初始化、字段加载与表达式求值、因子统计、CSV 导出、HTML 导出及总耗时。结果写入 `outputs/performance/csi1000_complex_factor/benchmark_<label>.json`；同时存在 `benchmark_before.json` 时，`--label after` 还会生成 `stage_comparison.csv`。报告默认写入 `outputs/factor_reports/csi1000_complex_factor/`。

## 中证 1000 全算子因子基准

`csi1000_factor_benchmark.py` 覆盖 `算子.md` 中列出的全部基础与技术算子。默认计算区间为 2017-01-01 至 2026-01-01，资产池使用 `snapshot_date < D` 的中证 1000（`000852.SH`）历史成分，并在每个信号日剔除 ST、上市不足 365 天和停牌股票。

```bash
uv run python performance/csi1000_factor_benchmark.py
```

脚本先预取所有算子共享的物理字段，再按小批次计算，避免一次保留约百个完整因子矩阵。默认结果写入 `outputs/performance/csi1000_factors/benchmark.json`，其中包含总耗时、吞吐量、有效值数、校验和及各批次耗时。可使用 `--batch-size`、`--column-cache-mib`、`--no-prefetch` 和 Bundle 参数固定对比条件。

---

本目录提供一套可重复运行的 TuAlpha 大横截面性能基准。策略文件为 `guozheng2000_enhanced.py`，目的在于同时覆盖 PIT 指数成分、批量日线、批量财务查询、组合构建和大批量下单；回测结果不构成投资建议。

## 策略逻辑

策略在每月首个可用交易日 D 调仓，候选池为 `snapshot_date < D` 的最新国证 2000（`399303.SZ`）成分。先排除 ST、停牌、无成交、当日成交额低于 1000 万元、正 PE/ROE 缺失以及 61 日窗口内有效价格少于 55 个交易日的股票。

对有效股票计算四个横截面百分位因子：

- 61 日价格动量：30%，越高越好；
- 最新 PIT ROE：25%，越高越好；
- PE-TTM：25%，越低越好；
- 61 日日收益年化波动率：20%，越低越好。

按申万一级行业在候选池中的指数权重分配 300 个持仓名额，各行业选择综合得分最高的股票。目标仓位保留 5% 现金；行业目标权重跟随候选池的指数行业权重，行业内部以成分权重为基准并施加温和因子倾斜。调仓时先提交减仓，再提交增仓，忽略小于 2 个基点的权重偏差。

D 日只能读取截至当日可见的数据并提交订单，订单最早 D+1 以原始开盘价成交。真实成交仍受 T+1、涨跌停、停牌、交易单位、现金和最多 300 个持仓限制。策略使用 `qfq` 价格计算信号，以国证 2000 为基准；指数本身不参与交易。

## 性能覆盖

每次月度调仓对约 2000 个成分执行一次 PIT 成分查询、七字段 `current_arrays()` 查询、`61 × 2000` 批量历史窗口、ROE 批量 PIT 财务查询，以及最多 300 个目标仓位的批量下单。成分池会随月度快照变化，且大字段只在调仓日读取，因此策略不建立全历史 `prefetch()` 缓存，避免用更高常驻内存换取很少的重复命中。

## 运行方法

先下载指数日线和成分权重；`--index-daily` 会从 Bundle 起始日定向回填历史，两个参数均可重复指定：

```bash
uv run tualpha update \
  --index-daily 399303.SZ \
  --index-weight 399303.SZ
```

安装开发依赖后执行完整基准：

```bash
uv sync --dev
uv run python performance/guozheng2000_enhanced.py --no-progress
```

仅测引擎与 CSV 导出、排除 HTML 报告生成时间：

```bash
uv run python performance/guozheng2000_enhanced.py \
  --start 2015-01-05 --end 2025-12-31 \
  --no-report --no-progress
```

可通过 `--bundle-root`、`--bundle-name`、`--capital-base`、`--output-dir` 和 `--column-cache-mib` 固定测试条件。比较版本时应使用同一 Bundle、日期、缓存参数和报告选项，分别记录冷启动与热启动结果，不要用短区间线性外推。

默认输出目录为 `outputs/performance/guozheng2000_enhanced/`：

- `benchmark.json`：版本、配置、交易日数、总耗时、吞吐量、订单状态、拒单原因和成交统计；
- `daily_positions.csv`：每日持仓；
- `report.html`：交互报告，使用 `--no-report` 时不生成。

国证 2000 权重只有月度快照，快照之间沿用最近已知值；供应商后续修订无法还原。该基准衡量端到端回测吞吐量，不用于证明策略存在超额收益。
