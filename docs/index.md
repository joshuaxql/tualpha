<div class="ta-hero">
  <div class="ta-kicker">A-SHARE · ETF · DAILY EVENT ENGINE</div>
  <h1>TuAlpha</h1>
  <p>把中国市场的交易约束、Point-in-time 数据和可复现的本地数据工程，收束进一套清晰的日频回测 API。</p>
  <div class="ta-actions">
    <a href="getting-started/">开始第一个回测</a>
    <a href="strategy-guide/">理解策略时序</a>
    <a href="data-management/">构建本地数据</a>
  </div>
</div>

<div class="ta-facts">
  <div class="ta-fact"><strong>D → D+1</strong><span>收盘决策，次日成交</span></div>
  <div class="ta-fact"><strong>Parquet</strong><span>按年分区的事实数据</span></div>
  <div class="ta-fact"><strong>DuckDB</strong><span>投影、裁剪与质量检查</span></div>
  <div class="ta-fact"><strong>Pandas 3</strong><span>Python 3.12 原生支持</span></div>
</div>

# 为中国日频市场而设计

TuAlpha 是面向 A 股股票和 ETF 的事件驱动回测框架。策略在交易日 **D** 读取截至当日的数据并提交订单，订单最早在 **D+1** 的开盘或收盘端点成交。框架统一处理 T+1、停牌、涨跌停、交易单位、现金、佣金、印花税和过户费。

<div class="ta-grid">
  <div class="ta-card">
    <h3>可信时序</h3>
    <p>财务数据按公告日可见，指数成分使用严格早于回调日的最新快照；历史前复权不依赖回测结束日。</p>
  </div>
  <div class="ta-card">
    <h3>本地数据层</h3>
    <p>Tushare 数据写入年度 Parquet，DuckDB 提供查询，DataPortal 使用 Arrow 与 NumPy 列缓存支撑策略热路径。</p>
  </div>
  <div class="ta-card">
    <h3>原子更新</h3>
    <p>增量更新只下载缺失的表和交易日，在 staging 中构建新 generation，校验通过后才替换活动 Bundle。</p>
  </div>
  <div class="ta-card">
    <h3>可解释结果</h3>
    <p>输出回测绩效与归因，也可计算 IC、RankIC、分位收益并生成中文 Plotly 因子报告。</p>
  </div>
</div>

## 最短路径

```bash
uv add tualpha
export TUSHARE_TOKEN="your-token"
uv run tualpha build --from 20100101
```

随后从一个单资产策略开始：

```python
from tualpha import order_target_percent, run_algorithm, symbol


def initialize(context):
    context.asset = symbol("510300.SH")


def handle_data(context, data):
    closes = data.history(context.asset, "close", 20).dropna()
    if len(closes) == 20 and data.can_trade(context.asset):
        order_target_percent(
            context.asset,
            0.95 if closes.iloc[-1] > closes.mean() else 0.0,
        )


result = run_algorithm(
    start="2020-01-01",
    end="2026-08-25",
    initialize=initialize,
    handle_data=handle_data,
    adjustment="qfq",
    execution_time="open",
    output_dir="outputs/ma20",
)
```

## 继续阅读

- [快速开始](getting-started.md)：安装、数据准备和第一次运行；
- [策略开发](strategy-guide.md)：D/D+1、订单、行情、PIT 和性能规则；
- [因子研究](factor-research.md)：表达式算子、PIT 资产池、IC/RankIC 与报告；
- [数据管理](data-management.md)：全量构建、增量更新、查询与质检；
- [架构说明](architecture.md)：核心模块、事件循环和发布路径；
- [Bundle 格式](bundle-format.md)：物理分区、Catalog、manifest 和完整性协议。

!!! warning "研究用途"
    TuAlpha 只用于量化研究、软件开发与回测验证，不构成投资建议。历史回测结果不代表未来收益。
