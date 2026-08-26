# 策略开发

TuAlpha 策略由初始化、每日回调和可选分析回调组成。框架只支持日频、多头现金账户，股票和 ETF 可交易，指数只能作为基准或 PIT 成分来源。

## 回调结构

```python
def initialize(context): ...
def handle_data(context, data): ...
def analyze(context, result): ...  # 可选
```

| 回调 | 职责 | 禁止事项 |
|---|---|---|
| `initialize` | 解析资产、设置常量状态和费用模型 | 读取行情、下单、`record()` |
| `handle_data` | 读取截至 D 日的数据、计算信号、提交订单 | 读取未来数据、假定订单已成交 |
| `analyze` | 导出附加结果 | 改变历史交易 |

## 每日事件顺序

```text
D 日应用公司行动
  → 撮合 D 日之前提交且已到期的订单
  → 使用 D 日原始收盘价盯市
  → handle_data 读取截至 D 日的数据
  → 提交最早 D+1 成交的订单
```

- `execution_time="open"`：D+1 原始开盘价；
- `execution_time="close"`：D+1 原始收盘价；
- 策略可读取 `raw`、`qfq` 或 `hfq` 价格，但成交、现金、费用和涨跌停判断始终使用原始价格。

!!! danger "禁止未来函数"
    不要使用 `shift(-1)`、未来收益、未来高低价、D+1 价格决定 D 日订单，或绕过 DataPortal 直接读取 Parquet/DuckDB。

## 资产与行情

```python
from tualpha import symbol

stock = symbol("000001.SZ")
etf = symbol("510300.SH")
```

当前值与窗口：

```python
close = data.current(etf, "close")
values = data.current(etf, ["close", "volume"])
closes = data.history(etf, "close", 20)
panel = data.history([stock, etf], ["close", "volume"], 60)
```

大型固定资产池使用批量接口，并在首次回调预热：

```python
if not context.prefetched:
    data.prefetch(context.assets, ["close", "volume", "daily_basic.total_mv"])
    context.prefetched = True

arrays = data.current_arrays(
    context.assets,
    ["close", "volume", "daily_basic.total_mv"],
)
```

数值参与筛选前必须检查 `numpy.isfinite()` 或 `pandas.notna()`。估值、收益和财务缺失值不能填 0 后参与排名。

## 订单

推荐使用目标仓位接口：

```python
from tualpha import order_target_percent, order_target_percent_many

order_target_percent(etf, 0.95)
order_target_percent_many({asset_a: 0.45, asset_b: 0.45})
```

- 目标权重必须在 `[0, 1]`；
- 建议保留现金缓冲，总仓位通常不超过 `0.95`；
- value/percent 类订单在 D+1 按实际成交端点和撮合前权益解析数量；
- `_many` 保持映射插入顺序，不自动先卖后买；
- 拒单不会自动延期，应根据真实持仓决定是否重试；
- 提交订单不会立即改变持仓和现金。

常见拒单包括：`limit_up`、`limit_down`、`suspended`、`t_plus_one`、`invalid_lot`、`below_minimum_order` 和 `insufficient_cash`。

## 组合状态

```python
context.portfolio.cash
context.portfolio.portfolio_value
context.portfolio.positions
context.portfolio.amount(asset)
context.portfolio.sellable_amount(asset, context.datetime)
```

策略本地状态不能在提交订单后立即标记为“已成交”。真实结果应从组合、订单状态和 `result.transactions` 判断。

## 财务 PIT

```python
roe = data.fundamental(stock, "fina_indicator.roe")
reports = data.fundamentals(
    stock,
    ["income.revenue", "balancesheet.total_assets"],
    periods=4,
)
```

可见性规则：

```text
coalesce(f_ann_date, ann_date) < 当前回调日
end_date <= 当前回调日
```

公告日当天不可见。利润表和现金流量表是年初至今累计值，不应直接命名为单季度值。

## 指数成分 PIT

```python
members = data.index_constituents("000300.SH")
```

查询使用 `snapshot_date < 当前回调日` 的最新快照。权重单位是百分比，快照日当天不可见，首个快照前返回空表。

## 费用

默认佣金率为 0.03%，最低佣金 5 元。佣金视为包含交易所经手费；股票印花税和过户费按日期分段，ETF 不收股票印花税和股票过户费。

自定义费用：

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

## 记录与验证

```python
from tualpha import record

record(signal=1.0, close=close, target_weight=0.95)
```

不要覆盖 `cash`、`positions_value`、`portfolio_value`、`returns` 和 `algorithm_period_return`。

策略交付前至少执行：

```bash
uv run python -m py_compile main.py
uv run ruff check main.py
uv run python main.py
```

随后检查订单、成交、拒单、总权重、PIT 边界和输出文件。
