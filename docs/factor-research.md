# 因子计算与分析

TuAlpha 的因子表达式直接运行在 DataPortal 批量窗口之上。表达式使用 `$字段` 引用日线或扩展日线字段；同一次调用只读取所需物理列一次，并复用公共子表达式。

## 离线因子数据

`factor_data()` 创建一个有明确日期和资产池边界的研究会话。下面的调用正是最简因子获取形式：

```python
from tualpha import factor_data

with factor_data(
    start="2017-01-01",
    end="2026-01-01",
    index_code="000852.SH",
    exclude_st=True,
    min_listed_days=365,
    exclude_suspended=True,
) as data:
    factors = data.history(
        [
            "RANK($close/$open)",
            "1/$daily_basic.total_mv",
        ]
    )
```

多表达式结果的行索引是交易日，列为 `(asset, field)` 两级索引。单表达式返回交易日 × 代码的 DataFrame。`index_code` 使用 `snapshot_date < D` 的最新历史成分快照；ST 和停牌过滤也按 D 日历史状态执行。默认 `adjustment="raw"`，避免离线整段计算把分析终点的复权基准带入更早截面。

固定资产池可传 `assets=[...]`，但不能同时传 `assets` 和 `index_code`。重复分批计算大量表达式时可先调用：

```python
data.prefetch(expressions)
```

它逐字段预取表达式依赖的物理列，并只保留“所需日期范围 × 资产池并集”的紧凑矩阵，不会为 Bundle 全部资产分配稠密缓存。需要流式处理大量因子时，可按小批调用 `data.history_arrays(expressions)`，直接得到“表达式 → 日期 × 代码矩阵”的映射，避免组装大型多级列 DataFrame。

## 回调内表达式

原有 `BarData.history()` 签名保持不变，字段参数可直接混合原始字段和因子表达式：

```python
panel = data.history(
    context.assets,
    ["close", "RANK($close/$open)", "TS_ZSCORE($volume,20)"],
    60,
)
```

系统会在请求窗口前自动加载嵌套算子所需的 warm-up 交易日，最终只返回指定的 `bar_count` 行。回调查询绝不读取当前 D 日之后的数据；因此 `FUTURE_RETURNS` 在回调末端只能得到缺失值，不能用于生成订单。

## 表达式规则

- 截面算子（如 `RANK`、`SCALE`、`ZSCORE`）按每个交易日横向计算；
- 时序算子（如 `MA`、`REF`、`CORR`）按每个资产纵向计算；
- `$vol` 是 `$volume` 的别名，`$price` 是 `$close` 的别名；
- 支持四则运算、幂、比较、布尔组合和嵌套函数；
- 除法无效、无穷值和窗口不足统一输出 `NaN`，不会用 0 参与排名；
- 表达式由受限 AST 解释器执行，不调用 Python `eval`，不能访问属性、导入或任意代码；
- `data.available_operators()` 返回当前实现的全部算子名称。详细公式和示例以仓库根目录 `算子.md` 为准。

## IC、RankIC 与因子报告

`run_factor_analysis()` 负责加载因子、生成未来收益标签、计算逐日指标并导出报告：

```python
from tualpha import run_factor_analysis

result = run_factor_analysis(
    {"日内强度": "RANK($close/$open)"},
    start="2017-01-01",
    end="2026-01-01",
    index_code="000852.SH",
    periods=[1, 5, 10],
    quantiles=5,
    industry_neutral=True,
    market_cap_neutral=True,
    output_dir="outputs/csi1000_intraday_strength",
)

print(result.summary())
```

输出目录包含：

```text
outputs/csi1000_intraday_strength/
├── daily_factor_metrics.csv
└── factor_report.html
```

每份报告只包含一个因子，并在首页写明因子名称与完整计算公式。`periods` 接受正整数数组，报告会展示数组中的每个预测周期。`daily_factor_metrics.csv` 把该因子每个交易日和预测周期的 `ic`、`rank_ic`、有效样本数、头尾分位收益与换手、因子秩自相关、因子加权收益和资产池收益合并到一个文件。

HTML 报告完整覆盖仓库 `alphalens报告样本/` 中的内容：Returns、Information 和 Turnover 汇总表，各分位平均日收益和小提琴分布，每个预测周期的因子加权多空及分位组合累计收益、头尾价差，IC/RankIC 时序和移动平均，IC 直方图、KDE、正态 Q-Q、月度 IC 热力图，头尾换手、因子秩自相关、分行业 IC 与分行业分位收益。多日累计曲线先将对应持有期收益折算为等效日收益，再按交易日累计，避免重叠标签被直接复利放大。需要包含调仓、交易成本和市场约束的累计收益时，应通过事件驱动回测计算。

### 行业与市值中性化

- `industry_neutral=False`：默认关闭；开启后使用 `industry.l1_name` 的当日 PIT 申万一级行业固定效应；
- `market_cap_neutral=False`：默认关闭；开启后对当日 `log(daily_basic.total_mv)` 做横截面残差化；
- 两者同时开启：联合回归行业固定效应和对数市值，不执行依赖先后顺序的两次串行处理；
- 行业或市值缺失、非正市值的股票当日从中性化后的因子样本排除，不填 0；
- 可通过 `industry_field` 和 `market_cap_field` 更换暴露字段。

直接处理中性化矩阵时可调用：

```python
from tualpha import neutralize_factor_values

residual = neutralize_factor_values(
    factor_matrix,
    industries=industry_matrix,
    market_caps=total_mv_matrix,
)
```

多个因子应分别调用 `run_factor_analysis()`，并使用不同的 `output_dir`；传入多个因子并同时请求报告会直接报错，避免产生混合报告。

未来收益只在离线分析中通过 `allow_future=True` 作为标签读取，不暴露给策略回调。因子值始终在 D 日资产池和 D 日过滤条件上计算。

## 中证 1000 全算子性能基准

仓库提供覆盖 `算子.md` 全部算子的长区间基准：

```bash
uv run python performance/csi1000_factor_benchmark.py
```

默认范围是 2017-01-01 至 2026-01-01，严格使用历史中证 1000 成分并剔除 ST、上市不足 365 天和停牌股票。脚本预取共享列、分批计算以控制峰值内存，并把总耗时、吞吐量、有效值数、校验和与各批次耗时写入 `outputs/performance/csi1000_factors/benchmark.json`。
