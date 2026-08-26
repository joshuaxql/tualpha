# TuAlpha 架构

TuAlpha 采用 RQAlpha 风格的职责分层，面向日频 A 股/ETF 回测，并使用 Parquet + DuckDB 本地数据层。

```text
src/tualpha/
├── apis/                  # 策略函数，转发到 ExecutionContext
├── core/                  # TradingAlgorithm、回调阶段、D/D+1 事件循环
├── model/                 # Asset、Order、Transaction、Position、Portfolio
├── broker/                # SimulationBroker、Matcher、费用和市场规则
├── data/
│   ├── bar.py             # DailyBar 与回调 BarData
│   ├── portal.py          # PIT Parquet DataPortal 与 NumPy 列缓存
│   ├── query.py           # DuckDB 本地查询客户端
│   ├── trading_calendar.py
│   ├── quality/           # 表级质检和报告
│   └── bundle/            # Parquet schema、全量构建、更新、Catalog 和发布
├── cmds/                  # build / update / quality
├── reporting.py
├── metrics.py
└── result.py
```

## 依赖方向

```text
apis → core.execution_context
core.algorithm → broker + data + model
broker → data.bar + model
DataPortal → DuckDB query + model.asset + calendar
bundle importer/updater → Parquet schema + Catalog + publication
quality → DuckDB query + table registry
```

`model` 不依赖策略 API 或回测循环；`apis` 不直接操作 Broker；Matcher 不调用策略代码。

## 每日执行顺序

```text
D 日 handle_data
  → 创建订单意图，eligible_session = D+1
D+1
  → 应用公司行动
  → 读取所选 open/close 原始端点
  → 计算撮合前组合权益
  → Matcher 解析 quantity/value/percent/target 意图
  → 检查停牌、成交量、涨跌停、交易单位、T+1 和现金
  → Broker 记账并产生 Transaction
  → 收盘盯市
  → handle_data
```

## 查询热路径

- DuckDB 对 Parquet 执行分区裁剪、字段投影和过滤下推；
- DataPortal 首次请求某字段和资产集合时批量查询；
- 查询结果写入受内存上限约束的二维 NumPy 列缓存；
- 后续 `current()`、`current_arrays()` 和 `history()` 只做向量索引；
- 单资产策略不会扫描并解码全市场字段；全市场扫描只产生一次 DuckDB 查询。

## Bundle 更新

```text
Tushare → 可续传分区 CSV
        → hardlink 复用未变化年度 Parquet
        → 重写受影响年份
        → catalog.duckdb + manifest.json
        → 关键约束和 Reader 校验
        → bundle.lock → 原子发布
```

活动 Bundle 从不原地修改。失败保留上一 generation；首次迁移保留旧 HDF5 备份。
