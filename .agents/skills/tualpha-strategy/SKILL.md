---
name: tualpha-strategy
description: 设计、编写、迁移、调试和审查 TuAlpha 1.3+ 的 A 股或 ETF 日频策略。用户提到 TuAlpha、事件驱动回测、选股、择时、轮动、PIT 财务或指数成分时使用。必须遵守 D 日决策、D+1 成交、T+1、涨跌停、停牌、交易单位和无未来函数约束。
compatibility: TuAlpha >= 1.3.0, Python >= 3.12, uv
metadata:
  version: "1.1.0"
---

# TuAlpha 策略开发规范

为当前 TuAlpha 仓库生成可直接运行、可解释、可验证且不含未来函数的日频策略。优先保持策略简单，不擅自增加优化器、机器学习、动态配置系统或无需求依据的抽象层。

## 1. 加载规范

开始前必须：

1. 从 TuAlpha 仓库根目录工作。
2. 完整阅读 [框架与时序契约](references/framework-contract.md)。
3. 完整阅读 [策略 API 参考](references/api-reference.md)。
4. 使用扩展日线、财务或指数成分时，阅读 [数据字段参考](references/data-fields.md)。
5. 检查 `pyproject.toml`、`README.md` 和相关源码；源码与本 Skill 冲突时以源码为准，并同步修正文档。
6. 优先复用 [单资产模板](assets/single-asset-template.py) 或 [多资产模板](assets/multi-asset-template.py)。

## 2. 前置检查

实施前回答：

- **Real?** 用户规则是否明确且能用现有日频数据实现？
- **Reuse?** 是否已有策略、模板或 API 可复用？
- **Risk?** 哪些改动会改变信号、成交时点、费用、仓位或历史结果？

迁移旧策略时先运行原代码或最小复现，记录报错、运行时间和关键结果；不得在未理解原逻辑时重写。

## 3. 需求澄清

仅在无法安全推断时，一次性确认：

- 策略类型及候选股票/ETF；指数不可交易，但可读取日线、作为基准或 PIT 成分来源；
- 信号公式、窗口、调仓频率、持仓数量和空仓条件；
- 回测日期、初始资金和基准；
- `raw`、`qfq` 或 `hfq`，默认推荐 `qfq`；
- D+1 使用 `open` 或 `close`，默认推荐 `open`；
- 单资产上限、总仓位、现金缓冲、ST/停牌过滤和止损规则；
- 报告目录、是否生成 HTML 及附加 CSV。

用户已明确的规则不得重复询问。

## 4. 不可违反的时序

```text
D 日进入 handle_data
  → 只能读取截至 D 日的数据
  → 计算信号并提交订单
  → 最早 D+1 按 execution_time 成交
```

必须遵守：

- 财务可见性：`coalesce(f_ann_date, ann_date) < D` 且 `end_date <= D`；
- 指数权重：使用 `snapshot_date < D` 的最新快照；
- D 日提交订单后，只有真实成交才能改变持仓和现金；
- 当前日 `data.can_trade()` 不保证 D+1 可以成交；
- 利润表和现金流量表是年初至今累计值，不得直接命名为单季度值。

禁止：

- `shift(-1)`、未来收益、未来最高价或最低价；
- 用 D+1 价格决定 D 日订单；
- 用当前行业、ST 或指数成分回填历史；
- 绕过 DataPortal 读取 CSV、Parquet 或 DuckDB Catalog；
- 在 `initialize()` 中读取行情、下单或 `record()`；
- 提交订单后立即把本地状态标记为已成交。

## 5. 实现结构

默认使用单文件：

```python
def initialize(context): ...
def handle_data(context, data): ...
def analyze(context, result): ...  # 仅在确有附加输出时添加


if __name__ == "__main__":
    result = run_algorithm(...)
```

职责：

- `initialize()`：解析资产、设置常量状态和费用模型；
- `handle_data()`：读取 PIT 数据、计算信号、提交订单和记录指标；
- `analyze()`：导出附加结果，不改变历史交易；
- `run_algorithm()`：显式给出日期、资金、复权、成交端点、基准和输出目录。

## 6. 数据读取

优先使用：

- 当前值：`data.current()`；
- 历史窗口：`data.history()`；
- 大型横截面：`data.current_arrays()`；
- 固定大型资产池：首次 `handle_data` 调用一次 `data.prefetch()`，不能在 `initialize()` 中调用；
- 原始价格：仅确有需要时使用 `data.raw_current()`；
- 财务：单资产使用 `data.fundamental()` / `data.fundamentals()`，大型横截面最新值使用 `data.fundamental_arrays()`；
- 指数日线：`data.index_current()` / `data.index_history()`，始终为原始点位；
- PIT 指数成分：`data.index_constituents()`；
- 字段发现：`data.available_fields()`。

数值必须用 `np.isfinite()` 或 `pd.notna()` 检查。缺失估值、收益和财务数据应排除，不能填 0 后参与排名。行业和 ST 字符串可能返回 `None`。

## 7. 下单规范

优先使用目标仓位接口：

```python
order_target_percent(asset, target)
order_target_percent_many(targets, position_limit=400)
```

要求：

- 只允许多头，目标权重必须在 `[0, 1]`；
- 通常保留现金缓冲，总目标仓位建议不超过 `0.95`；
- 多资产目标权重之和不能超过预定总仓位；
- 大篮子使用 `_many` 接口，避免数百次 Python API 调用；
- `_many` 保持 Mapping 插入顺序，不自动先卖后买；需要释放现金时先提交卖单映射，再提交买单映射；
- percent 类订单在 D+1 使用实际成交端点和撮合前组合权益解析数量；
- 不要因微小权重漂移无条件每日再平衡；
- 接受下一日因停牌、涨跌停、T+1、最小委托或现金不足而拒单。

佣金统一视为包含交易所经手费，TuAlpha 不另计经手费。股票印花税和过户费按日期分段；ETF 不收股票印花税和股票过户费。

## 8. 记录与报告

使用 `record()` 保存关键标量，例如信号、均线、目标权重、候选数量和风险状态。禁止覆盖：

- `cash`
- `positions_value`
- `portfolio_value`
- `returns`
- `algorithm_period_return`

推荐运行配置：

```python
run_algorithm(
    ...,
    adjustment="qfq",
    execution_time="open",
    benchmark="000300.SH",
    output_dir="outputs/<strategy_name>",
    strategy_name="策略名称",
    generate_report=True,
    show_progress=True,
    plotly_js="inline",
)
```

指定 `output_dir` 后生成 `report.html` 和 `daily_positions.csv`。自定义文件必须写入同一输出目录，文件名和字段含义应在 README 中说明。

## 9. 性能规则

- 固定大资产池使用 `current_arrays()`，不要逐资产调用 `current()`；一次性批量查询会只扫描当前日期或请求窗口；
- 仅当固定资产池和字段会在大量回调中重复使用时，首次回调用 `prefetch()` 建立全历史列缓存；`qfq` / `hfq` 价格会自动预取复权因子；
- 固定资产位置、上市满期日期和常量数组在 `initialize()` 中预计算；
- 避免每日创建不必要的大型 DataFrame；
- 只在目标集合或策略信号变化时调仓；
- 性能目标明确时，用完整区间、真实 Bundle、相同报告配置计时，不用短区间外推代替验收。

性能优化不得改变 PIT、D/D+1、费用和市场规则；若改变调仓行为，必须明确告知用户。

## 10. 验证顺序

交付前依次执行：

```bash
uv run python -m py_compile path/to/strategy.py
uv run ruff check path/to/strategy.py
uv run python path/to/strategy.py --start <smoke-start> --end <smoke-end>
```

然后检查：

1. 信号首段不会因窗口不足错误交易；
2. 订单和成交数量符合预期；
3. 拒单中是否大量出现 `limit_up`、`limit_down`、`suspended`、`t_plus_one`、`invalid_lot`、`below_minimum_order` 或 `insufficient_cash`；
4. 总权重、现金缓冲和持仓上限正确；
5. PIT 数据在公告日或快照日当天不可见；
6. HTML 和 CSV 写入用户指定目录；
7. 策略没有直接读取物理数据文件；
8. 长区间和性能要求均使用完整配置实测。

修改 TuAlpha 框架时还必须执行：

```bash
uv run ruff check .
uv run ruff format --check .
uv run pytest -q
```

## 11. 交付内容

说明：

- 策略文件路径和运行命令；
- 信号、窗口、调仓频率、持仓和风险规则；
- 使用的数据字段、单位和缺失值处理；
- D/D+1 时序、成交端点和费用口径；
- 报告目录和输出文件；
- 实际测试、回测时间及关键验证结果；
- 已知边界和行为变化，不夸大回测结论。
