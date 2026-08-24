# TuAlpha 策略数据字段

策略只使用 API 逻辑字段，不直接读取 Bcolz 或 SQLite。当前 Bundle 的精确字段以 `data.available_fields()` 为准。物理布局见 [Bundle 格式](../../../bundle-format.md)。

## 标准日线字段

| 字段 | 含义 | 单位/备注 |
|---|---|---|
| `open` | 开盘价 | 元，按 `adjustment` 暴露 |
| `high` | 最高价 | 元，按 `adjustment` 暴露 |
| `low` | 最低价 | 元，按 `adjustment` 暴露 |
| `close` / `price` | 收盘价 | 元，按 `adjustment` 暴露 |
| `volume` | 精确成交量 | 股票为股、ETF 为份 |
| `pre_close` | 原始昨收价 | 元 |
| `turnover` | 成交额 | 元 |
| `up_limit` | 涨停价 | 原始价格，元 |
| `down_limit` | 跌停价 | 原始价格，元 |
| `adj_factor` | 复权因子 | 无量纲 |
| `suspended` | 停牌标志 | 0/1 |

`daily_basic.close` 是每日指标接口中的原始收盘价，不参与 qfq/hfq 调整；研究价格通常应使用普通 `close`。

## `daily_basic` 每日指标

这些字段主要适用于 A 股，ETF 通常缺失。

| 字段 | 含义 | 单位/口径 |
|---|---|---|
| `daily_basic.close` | 每日指标接口收盘价 | 元，原始值 |
| `daily_basic.turnover_rate` | 成交量 / 无限售流通股数 | % |
| `daily_basic.turnover_rate_f` | 成交量 / 自由流通股数 | % |
| `daily_basic.volume_ratio` | 量比 | 无量纲 |
| `daily_basic.pe` | 总市值 / 净利润 | 静态 PE，亏损通常为空 |
| `daily_basic.pe_ttm` | 总市值 / 最近12个月净利润 | TTM PE，亏损通常为空 |
| `daily_basic.pb` | 总市值 /（净资产－其他权益工具） | PB |
| `daily_basic.ps` | 总市值 / 最新年报营业收入 | 静态 PS |
| `daily_basic.ps_ttm` | 总市值 / 最近12个月营业收入 | TTM PS |
| `daily_basic.dv_ratio` | 静态股息率 | % |
| `daily_basic.dv_ttm` | 最近12个月股息率 | % |
| `daily_basic.total_share` | 总股本 | 万股 |
| `daily_basic.float_share` | 流通股本 | 万股 |
| `daily_basic.free_share` | 自由流通股本 | 万股 |
| `daily_basic.total_mv` | 总市值 | 万元 |
| `daily_basic.circ_mv` | 流通市值 | 万元 |
| `daily_basic.limit_status` | 收盘状态编码 | 见下表 |

`limit_status`：

| 值 | 含义 |
|---:|---|
| 0 | 平盘 |
| 1 | 上涨，未涨停 |
| 2 | 涨停，非一字涨停 |
| 3 | 一字涨停 |
| 4 | 下跌，未跌停 |
| 5 | 跌停，非一字跌停 |
| 6 | 一字跌停 |

估值筛选时至少检查：

```python
pe = data.current(asset, "daily_basic.pe_ttm")
valid = pd.notna(pe) and pe > 0
```

不要把亏损公司的空 PE 填成 0 后参与低估值排名。

## `moneyflow` 个股资金流向

订单档位：

- `sm`：小单，成交额小于 5 万元；
- `md`：中单，5 万～20 万元；
- `lg`：大单，20 万～100 万元；
- `elg`：特大单，大于等于 100 万元。

量字段单位为手，金额字段单位为万元：

| 字段 | 含义 |
|---|---|
| `moneyflow.buy_sm_vol` | 小单主动买入量，手 |
| `moneyflow.buy_sm_amount` | 小单主动买入金额，万元 |
| `moneyflow.sell_sm_vol` | 小单主动卖出量，手 |
| `moneyflow.sell_sm_amount` | 小单主动卖出金额，万元 |
| `moneyflow.buy_md_vol` | 中单主动买入量，手 |
| `moneyflow.buy_md_amount` | 中单主动买入金额，万元 |
| `moneyflow.sell_md_vol` | 中单主动卖出量，手 |
| `moneyflow.sell_md_amount` | 中单主动卖出金额，万元 |
| `moneyflow.buy_lg_vol` | 大单主动买入量，手 |
| `moneyflow.buy_lg_amount` | 大单主动买入金额，万元 |
| `moneyflow.sell_lg_vol` | 大单主动卖出量，手 |
| `moneyflow.sell_lg_amount` | 大单主动卖出金额，万元 |
| `moneyflow.buy_elg_vol` | 特大单主动买入量，手 |
| `moneyflow.buy_elg_amount` | 特大单主动买入金额，万元 |
| `moneyflow.sell_elg_vol` | 特大单主动卖出量，手 |
| `moneyflow.sell_elg_amount` | 特大单主动卖出金额，万元 |
| `moneyflow.net_mf_vol` | L2 主动买卖净流入量，手 |
| `moneyflow.net_mf_amount` | L2 主动买卖净流入额，万元 |

`net_mf_*` 是 Tushare 基于 L2 主动订单计算的结果，不能简单用各档买卖值相减替代。

## `industry` 申万行业

| 字段 | 含义 |
|---|---|
| `industry.l1_code` | 申万一级行业代码 |
| `industry.l1_name` | 申万一级行业名称 |
| `industry.l2_code` | 申万二级行业代码 |
| `industry.l2_name` | 申万二级行业名称 |
| `industry.l3_code` | 申万三级行业代码 |
| `industry.l3_name` | 申万三级行业名称 |

行业归属按历史 `in_date/out_date` 生成，不使用当前行业分类回填过去。缺失时返回 `None`。

## `stock_st` 风险警示

| 字段 | 含义 |
|---|---|
| `stock_st.name` | 当日风险警示证券名称，如 `*ST天山` |
| `stock_st.type` | 风险类型代码，如 `ST` |
| `stock_st.type_name` | 类型名称，如“风险警示板” |
| `stock_st.is_st` | 当日在 ST 列表中为 1，否则为 0 |

常见过滤：

```python
is_st = data.current(asset, "stock_st.is_st")
if is_st == 1:
    ...
```

## 指数成分与权重

`data.index_constituents(index_code)` 返回月度指数快照，`weight` 单位是百分比而非 0～1 小数。D 日快照严格从 D+1 可见；它不属于 `current()` / `history()` 字段，也不能用于指数下单。

## 财务数据

财务字段只能通过 `fundamental()` / `fundamentals()` 读取。

### 命名空间

| 命名空间 | 内容 |
|---|---|
| `balancesheet.*` | 资产负债表时点值 |
| `income.*` | 利润表年初至今累计值 |
| `cashflow.*` | 现金流量表年初至今累计值 |
| `fina_indicator.*` | Tushare 已计算的财务比率、单季度和增长指标 |

常用示例：

| 字段 | 含义 |
|---|---|
| `balancesheet.total_assets` | 总资产 |
| `balancesheet.total_liab` | 总负债 |
| `balancesheet.total_hldr_eqy_exc_min_int` | 归属母公司股东权益 |
| `income.revenue` | 营业收入累计值 |
| `income.n_income_attr_p` | 归母净利润累计值 |
| `cashflow.n_cashflow_act` | 经营活动现金流量净额累计值 |
| `fina_indicator.roe` | 净资产收益率 |
| `fina_indicator.roe_waa` | 加权平均净资产收益率 |
| `fina_indicator.grossprofit_margin` | 销售毛利率 |
| `fina_indicator.netprofit_margin` | 销售净利率 |
| `fina_indicator.debt_to_assets` | 资产负债率 |
| `fina_indicator.q_netprofit_yoy` | 单季度净利润同比增长率 |

财务宽表字段很多，使用当前 Bundle 检查：

```python
fields = data.available_fields("fina_indicator")
```

不要仅凭字段名猜测单位。新增或少见字段应查 Tushare 对应接口文档。报表金额通常是元，财务指标的百分比/比率口径以 Tushare 文档为准。

## 缺失值规则

| 类型 | 策略返回值 |
|---|---|
| 数值日线字段 | `NaN` |
| 行业/ST 字符串 | `None` |
| `suspended` | 无停牌记录时为 0 |
| `stock_st.is_st` | 不在 ST 列表时为 0 |
| 财务数值 | 不可见或缺失时为 `NaN` |

横截面排序前应显式排除缺失值，并保证各字段单位可比较。
