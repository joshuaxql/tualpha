---
name: tualpha-strategy
description: 为 TuAlpha 框架设计、编写、修改、调试和审查 A 股或 ETF 日频回测策略。用户提到 TuAlpha、使用本仓库写策略、事件驱动回测、选股/择时/轮动、估值资金流行业ST或公告时点财务因子时使用。生成的策略必须遵守 D 日决策、D+1 成交、T+1、涨跌停和无未来函数约束。
compatibility: TuAlpha >= 0.5.0, Python >= 3.12, uv
metadata:
  version: "1.0.0"
---

# TuAlpha 策略编写

使用本 Skill 生成能在当前仓库直接运行、符合中国市场规则且不引入未来函数的策略。

## 开始前

1. 从仓库根目录工作。
2. 必须完整阅读：
   - [框架与时序契约](references/framework-contract.md)
   - [策略 API 参考](references/api-reference.md)
3. 使用扩展日线或财务因子时，再阅读 [数据字段参考](references/data-fields.md)。
4. 若代码库版本或 API 可能已经变化，检查 `pyproject.toml`、`README.md` 和实际源码；源码优先于本 Skill。
5. 不读取策略日期之后的数据，不从 CSV、`normalized.duckdb`、`finance.sqlite` 或 Bcolz 物理文件绕过 DataPortal。

## 需求澄清

仅在用户尚未说明且无法安全推断时，一次性确认以下信息：

- 策略目标：择时、轮动、横截面选股、基本面或组合策略；
- 股票/ETF 代码或候选池；指数只能作为基准；
- 信号定义、回看窗口、调仓频率和持仓数量；
- 回测起止日期、初始资金、基准；
- `qfq`、`hfq` 或 `raw`；默认推荐 `qfq`；
- D+1 使用 `open` 还是 `close` 成交；默认推荐 `open`；
- 风险限制：单资产上限、现金缓冲、ST/停牌过滤、止损或最大持仓数；
- 是否输出中文 HTML 报告和每日持仓 CSV。

不要为了显得复杂而增加用户没有要求的优化器、机器学习、动态配置系统或抽象层。

## 实现流程

### 1. 把自然语言规则写成明确时序

在写代码前确认每个信号只依赖当前回调日及更早数据：

```text
D 日进入 handle_data
  → 读取截至 D 日的数据
  → 计算信号并提交订单
  → 最早 D+1 按 execution_time 成交
```

禁止：

- `shift(-1)`、未来收益、未来最高/最低价；
- 用 D+1 开盘价决定 D 日订单；
- 用今天的行业或 ST 状态覆盖历史；
- 按报告期直接读取尚未公告的财务数据；
- 在策略中手工读取 CSV、SQLite、DuckDB 或 Bcolz；
- 假设订单一定成交后立即修改“已持仓”状态。

### 2. 选择最小可行结构

通常生成单个 Python 文件：

```python
def initialize(context): ...
def handle_data(context, data): ...
def analyze(context, result): ...  # 仅需要额外分析时添加


if __name__ == "__main__":
    result = run_algorithm(...)
```

- 在 `initialize()` 中解析资产并设置持久状态；
- 在 `handle_data()` 中读取数据、计算信号、下单和 `record()`；
- 不在 `initialize()` 中下单；
- 不在 `analyze()` 中改变历史交易；
- 优先复用 [单资产模板](assets/single-asset-template.py) 或 [多资产模板](assets/multi-asset-template.py)。

### 3. 正确读取数据

- 价格：`data.current()`、`data.history()`；
- 不复权价格仅在确有需要时使用 `data.raw_current()`；
- 日频扩展字段：`daily_basic.*`、`moneyflow.*`、`industry.*`、`stock_st.*`；
- 财务字段：只能用 `data.fundamental()` 或 `data.fundamentals()`；
- 不确定字段时先调用 `data.available_fields(namespace)`；
- 数值信号先检查 `pd.notna()` / `np.isfinite()`；
- 行业/ST 字符串允许返回 `None`；
- 利润表和现金流量表是年初至今累计值，不能直接当作单季度值。

### 4. 正确下单

优先使用目标仓位 API：

```python
order_target_percent(asset, target)
```

要求：

- `target >= 0`，框架只支持多头现金账户；
- 预留费用和价格波动空间，满仓目标通常不超过 `0.95`；
- 多资产目标权重之和不得超过预定总仓位；
- 调仓前考虑 `data.can_trade(asset)`，但仍必须接受下一交易日可能因停牌、涨跌停或无行情而拒单；
- 不要把“提交订单”当作“成交完成”；需要成交信息时从结果中的 `orders`、`transactions` 查看；
- 避免无条件每日重复细小调仓，除非策略确实需要每日再平衡。

### 5. 记录可解释信号

用 `record()` 保存关键标量，例如信号、均线、目标权重、估值和风险状态。不要使用保留键：

- `cash`
- `positions_value`
- `portfolio_value`
- `returns`
- `algorithm_period_return`

## 推荐策略模式

### 单资产择时

- 在 `initialize()` 解析一个股票或 ETF；
- 使用当前及历史价格生成信号；
- 目标仓位在 `0` 与例如 `0.95` 之间切换；
- 使用 `record()` 保存信号和目标。

### 多资产轮动

- 在 `initialize()` 解析显式候选池；
- 在固定调仓频率计算每个资产的动量/估值/质量得分；
- 丢弃数据不足、ST 或不可交易资产；
- 选前 N 名等权或按明确规则分配；
- 对未入选的现有候选资产设置目标 `0`。

### 基本面选股

- 使用 `data.fundamental(asset, "fina_indicator.roe")` 等 PIT API；
- 用 `daily_basic.pe_ttm` 等当前估值指标组合过滤；
- 财务缺失时排除，不把缺失填成零；
- 清楚说明财务字段是累计值、时点值、比例还是 TTM 指标。

## 运行配置

默认建议：

```python
run_algorithm(
    ...,
    adjustment="qfq",
    execution_time="open",
    benchmark="000300.SH",
    generate_report=True,
    output_dir="outputs/<strategy_name>",
)
```

- 研究收益序列通常使用 `qfq`；
- 成交、费用和涨跌停判断始终使用原始价格；
- 需要严格原始价格信号时才选择 `raw`；
- `plotly_js="inline"` 生成完全离线报告；
- 指定 `output_dir` 后会输出 `report.html` 和 `daily_positions.csv`。

## 验证

交付前依次执行：

```bash
uv run python -m py_compile path/to/strategy.py
uv run python path/to/strategy.py
```

长区间策略先用较短但有代表性的日期范围冒烟验证，再跑完整区间。检查：

1. 策略确实产生预期订单和成交；
2. `orders` 中是否大量出现 `limit_up`、`limit_down`、`suspended`、`t_plus_one`、`invalid_lot` 或 `insufficient_cash`；
3. 目标权重总和与现金缓冲正确；
4. 信号首段没有因窗口不足产生错误交易；
5. 财务数据在公告日前不可见；
6. 报告和持仓 CSV 已生成；
7. 代码没有直接数据文件读取或未来数据操作。

如果修改了框架而不只是新增策略，还必须执行：

```bash
uv run ruff check .
uv run ruff format --check .
uv run pytest -q
```

## 交付格式

向用户说明：

- 策略文件路径；
- 信号、调仓频率和风险规则；
- 数据字段及单位；
- D/D+1 时序和成交端点；
- 回测命令及输出目录；
- 实际验证结果；
- 仍存在的模型边界，不夸大回测结论。
