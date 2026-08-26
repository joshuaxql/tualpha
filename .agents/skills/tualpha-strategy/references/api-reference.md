# TuAlpha 1.3 策略 API 参考

## 回调

```python
def initialize(context): ...
def handle_data(context, data): ...
def analyze(context, result): ...
```

- `initialize`：回测开始前调用一次；解析资产、设置状态和费用模型；不能下单或 `record()`。
- `handle_data`：每个交易日调用一次；读取数据、计算信号、下单和记录指标。
- `analyze`：回测结束后可选调用；读取 `BacktestResult`，不能改变历史交易。

`context.datetime` 在 `handle_data` 中是当前交易日的 `pandas.Timestamp`。用户可以自由增加 `context` 属性。

## 资产

```python
asset = symbol("000001.SZ")
etf = symbol("510300.SH")
```

支持完整 Tushare 代码或唯一六位代码。指数不是可交易资产，不能用 `symbol()` 解析。

常用资产属性：

```python
asset.sid
asset.ts_code
asset.symbol
asset.name
asset.asset_type
asset.board
asset.price_tick
asset.is_stock
asset.is_etf
```

## 订单 API

```python
order(asset, amount)
order_value(asset, value)
order_percent(asset, percent)
order_target(asset, target)
order_target_value(asset, target)
order_target_percent(asset, target)

order_many({asset_a: amount_a, asset_b: amount_b})
order_value_many({asset_a: value_a, asset_b: value_b})
order_percent_many({asset_a: percent_a, asset_b: percent_b})
order_target_many(targets, position_limit=400)
order_target_value_many(targets, position_limit=400)
order_target_percent_many(targets, position_limit=400)

cancel_order(order_object)
get_open_orders()
get_open_orders(asset)
```

- `amount`：正数买入，负数卖出，单位股/份；
- `value`：正数为买入总预算（包含费用），负数为卖出证券市值上限；
- `percent`：相对于 D+1 撮合前、按成交端点价格重估的组合权益；非目标接口允许 `[-1, 1]`，目标接口允许 `[0, 1]`；
- 六个 `_many` 接口接收有序 Mapping，保持映射插入顺序并返回 `list[Order]`；三个目标批量接口支持 `position_limit`；
- `execution_time="open"` 使用 D+1 原始开盘价确定 value/percent 数量并撮合；`close` 使用 D+1 原始收盘价；
- `order` 和单笔 `order_target` 是固定数量订单，买入时可能因 `insufficient_cash` 拒单；
- value/percent 类接口及批量目标买单会按限定金额、实际现金和有效交易单位缩量，不使用 D 日收盘价预估最终数量；
- 批量目标买单不足最小交易单位时状态为 `canceled`，`reject_reason="below_minimum_order"`，不得写成 `insufficient_cash`；
- 所有批量接口保持显式映射顺序；需要先卖后买时，策略必须按该顺序构造 Mapping；
- 下单仅允许在 `handle_data` 中；单笔接口返回 `Order | None`，批量接口返回 `list[Order]`。

`Order` 常用属性：

```python
order.id
order.asset
order.sizing  # quantity/value/percent/target_*
order.requested  # 策略提交的原始参数
order.amount  # D+1 解析后的有符号目标成交数量
order.is_batch
order.is_target
order.created_session
order.eligible_session
order.status
order.filled
order.average_price
order.reject_reason
order.message
```

不要在提交订单后立即把本地状态视为已成交。value/percent 类订单在 D 日提交时最终 `amount` 尚未确定；真实结果应从 `Order`、`result.orders` 和 `result.transactions` 判断。

## 组合状态

```python
context.portfolio.cash
context.portfolio.positions_value
context.portfolio.portfolio_value
context.portfolio.pnl
context.portfolio.returns
context.portfolio.positions
context.portfolio.amount(asset)
context.portfolio.position(asset)
context.portfolio.sellable_amount(asset, context.datetime)
```

`context.portfolio.positions` 是 `Asset → Position` 字典。`Position` 常用属性：

```python
position.amount
position.cost_basis
position.total_cost
position.last_sale_price
position.sellable_amount(context.datetime)
```

组合只允许多头，不应构造负目标仓位。

## 当前数据

```python
value = data.current(asset, "close")
series = data.current(asset, ["close", "daily_basic.pe_ttm"])
series = data.current(assets, "close")
frame = data.current(assets, ["close", "daily_basic.pe_ttm"])
arrays = data.current_arrays(assets, ["close", "daily_basic.total_mv"])
```

`current_arrays()` 面向大型固定资产池，返回字段名到只读 NumPy 数组的映射，数组顺序与传入资产顺序一致；策略不得修改返回数组。

长期重复扫描固定大资产池时，只在首次 `handle_data` 中预热一次：

```python
if not context.prefetched:
    data.prefetch(context.assets, ["close", "volume", "daily_basic.total_mv"])
    context.prefetched = True
```

`prefetch()` 不返回数据，也不能在 `initialize()` 中调用。它只优化物理列加载，不改变回调可见日期。

`current()` 返回形状：

| 输入 | 返回 |
|---|---|
| 单资产、单字段 | 标量 |
| 单资产、多字段 | 以字段为索引的 `Series` |
| 多资产、单字段 | 以代码为索引的 `Series` |
| 多资产、多字段 | 行为代码、列为字段的 `DataFrame` |

策略价格默认按 `run_algorithm(adjustment=...)` 调整。读取原始值：

```python
raw_close = data.raw_current(asset, "close")
```

## 历史窗口

```python
closes = data.history(asset, "close", 20)
fields = data.history(asset, ["close", "volume"], 20)
prices = data.history(assets, "close", 60)
panel = data.history(assets, ["close", "daily_basic.pe_ttm"], 60)
```

返回形状：

| 输入 | 返回 |
|---|---|
| 单资产、单字段 | 日期索引 `Series` |
| 单资产、多字段 | 日期 × 字段 `DataFrame` |
| 多资产、单字段 | 日期 × 代码 `DataFrame` |
| 多资产、多字段 | 日期索引、`(asset, field)` 多级列 `DataFrame` |

窗口包含当前回调日。如果回测刚开始，返回长度可能不足 `bar_count`，策略必须先检查长度和有效观测数。

## 财务数据

最新可见值：

```python
roe = data.fundamental(asset, "fina_indicator.roe")
revenue = data.fundamental(asset, "income.revenue")
```

指定报告期：

```python
revenue = data.fundamental(
    asset,
    "income.revenue",
    period="20231231",
    report_type="1",
)
```

多个字段和报告期：

```python
reports = data.fundamentals(
    asset,
    [
        "fina_indicator.roe",
        "income.revenue",
        "balancesheet.total_assets",
        "cashflow.n_cashflow_act",
    ],
    periods=4,
    report_type="1",
)
```

- `fundamental` 返回 `float`，不可见或缺失时为 `NaN`；
- `fundamentals` 返回以报告期 `end_date` 为索引的 `DataFrame`，最新报告期在前；
- 财务字段不能通过 `current()` 或 `history()` 读取；
- 默认只选择合并报表 `report_type="1"`；
- `fina_indicator` 没有 report type 过滤。

## PIT 指数成分

```python
members = data.index_constituents("000300.SH")
```

返回以 `ts_code` 为索引的 `DataFrame`，列为：

- `asset`：对应的 TuAlpha `Asset`，无法映射时为 `None`；
- `weight`：Tushare 原始百分比权重；
- `snapshot_date`：当前可见快照日期。

仅选择 `snapshot_date < context.datetime` 的最新月度快照，所以 D 日快照从 D+1 回调起可见。首个快照前返回同结构的空表。默认支持 `000300.SH`、`000852.SH`、`000905.SH`、`000906.SH` 和 `899050.BJ`。

## 字段发现

```python
all_fields = data.available_fields()
daily_basic_fields = data.available_fields("daily_basic")
financial_fields = data.available_fields("fina_indicator")
```

返回当前 Bundle 实际支持的字段名元组。编写依赖可选财务字段的策略前应先检查。

## 交易可用性

```python
if data.can_trade(asset):
    ...
```

它检查当前日是否有行情、非停牌且成交量大于零，不检查下一交易日的涨跌停和停牌，因此不能保证新订单一定成交。

## 自定义记录

```python
record(signal=1.0, ma20=ma20, target_weight=0.95)
```

仅在 `handle_data` 中使用。值应为能写入每日记录的标量。禁止覆盖：

- `cash`
- `positions_value`
- `portfolio_value`
- `returns`
- `algorithm_period_return`

## 费用模型

```python
from tualpha import ChinaFeeModel, set_commission


def initialize(context):
    set_commission(
        ChinaFeeModel(
            stock_commission_rate=0.0003,
            etf_commission_rate=0.0003,
            stock_min_commission=5.0,
            etf_min_commission=5.0,
        )
    )
```

佣金统一视为包含交易所经手费，经手费不另计。除非用户明确要求，不要随意覆盖日期分段印花税和过户费规则。

## 运行入口

```python
result = run_algorithm(
    start="2020-01-01",
    end="2025-12-31",
    initialize=initialize,
    handle_data=handle_data,
    analyze=analyze,  # 可选
    capital_base=1_000_000,
    bundle_root="~/.tualpha",
    bundle_name="tualpha",
    adjustment="qfq",  # raw / qfq / hfq
    execution_time="open",  # open / close
    benchmark="000300.SH",
    output_dir="outputs/my_strategy",
    strategy_name="策略名称",
    generate_report=True,
    show_progress=True,  # 使用 tqdm 显示交易日进度
    plotly_js="inline",  # inline / cdn
    fee_model=None,
)
```

`show_progress=True` 为默认值，显示已处理交易日、百分比、速度、预计剩余时间和当前日期。批处理、测试或嵌套运行时可设为 `False`。

## 回测结果

```python
result.performance
result.daily_positions
result.orders
result.transactions
result.closed_trades
result.records
result.metrics
result.final_value
result.summary()
result.report_path
result.positions_path
```

指定 `output_dir` 后自动生成：

```text
output_dir/
├── report.html
└── daily_positions.csv
```

`report.html` 的组合归因表包含每个标的的成交次数、持有总天数、已实现盈亏、盈亏贡献占比、每日权重贡献累计收益和总费用。贡献按 `Σ(日终权重×标的当日收益率)` 计算，现金贡献固定为 0。股票和 ETF 的持有总天数按日终正市值的唯一交易日统计；`CASH` 仅统计没有任何正市值股票或 ETF、组合全部为现金的交易日。报告不再生成逐笔“交易分析”图表。
