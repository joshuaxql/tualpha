# API 参考

本页只列出面向策略作者和本地研究的稳定公共接口。策略推荐从顶层 `tualpha` 导入；下方模块路径用于生成准确的 API 签名。

## 回测入口

::: tualpha.core.algorithm.run_algorithm

::: tualpha.core.algorithm.TradingAlgorithm

## 资产

::: tualpha.model.asset.Asset

::: tualpha.model.asset.AssetFinder

::: tualpha.apis.strategy.symbol

## 行情数据

`BarData` 只在 `handle_data` 回调内有效。

::: tualpha.data.bar.BarData

### 指数日线

`BarData.index_current()` 和 `BarData.index_history()` 按指数代码读取当前或历史原始点位。指数不可下单，读取结果不参与股票/ETF 复权。

```python
close = data.index_current("000300.SH", "close")
history = data.index_history("000300.SH", ["close", "volume"], 20)
```

## 订单接口

::: tualpha.apis.strategy.order

::: tualpha.apis.strategy.order_value

::: tualpha.apis.strategy.order_percent

::: tualpha.apis.strategy.order_target

::: tualpha.apis.strategy.order_target_value

::: tualpha.apis.strategy.order_target_percent

### 批量订单

::: tualpha.apis.strategy.order_many

::: tualpha.apis.strategy.order_value_many

::: tualpha.apis.strategy.order_percent_many

::: tualpha.apis.strategy.order_target_many

::: tualpha.apis.strategy.order_target_value_many

::: tualpha.apis.strategy.order_target_percent_many

### 订单管理

::: tualpha.apis.strategy.get_open_orders

::: tualpha.apis.strategy.cancel_order

## 组合记录与费用

::: tualpha.apis.strategy.record

::: tualpha.apis.strategy.set_commission

::: tualpha.broker.costs.ChinaFeeModel

::: tualpha.broker.costs.RateSchedule

## 本地数据查询

::: tualpha.data.query.local_data

::: tualpha.data.query.LocalDataClient

## 因子研究

::: tualpha.data.research.factor_data

::: tualpha.data.research.FactorData

::: tualpha.analysis.factor.run_factor_analysis

::: tualpha.analysis.factor.analyze_factor_data

::: tualpha.analysis.factor.neutralize_factor_values

::: tualpha.analysis.factor.FactorAnalysisResult

## 回测结果

::: tualpha.result.BacktestResult
