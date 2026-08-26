# 快速开始

本页从安装 TuAlpha、准备 Tushare 数据到运行第一个 ETF 策略。

## 环境要求

- Python 3.12；
- Pandas 3.0 或更高版本；
- [`uv`](https://docs.astral.sh/uv/)；
- 具有目标接口权限的 Tushare Token。

## 安装

新项目：

```bash
uv init my-strategy
cd my-strategy
uv add tualpha
```

验证版本：

```bash
uv run python -c "import tualpha; print(tualpha.__version__)"
```

从源码参与开发：

```bash
git clone https://github.com/joshuaxql/tualpha.git
cd tualpha
uv sync --dev
```

## 准备数据

Token 只通过环境变量或标准输入提供，不要写入策略、配置文件或 Git 仓库。

Git Bash、Linux 或 macOS：

```bash
export TUSHARE_TOKEN="your-token"
```

PowerShell：

```powershell
$env:TUSHARE_TOKEN = "your-token"
```

首次构建本地 Bundle：

```bash
uv run tualpha build --from 20100101
```

先验证请求而不发布：

```bash
uv run tualpha build --from 20200101 --dry-run --json
```

默认数据目录为 `~/.tualpha`。完整数据结构和更新策略见[数据管理](data-management.md)。

## 第一个策略

创建 `main.py`：

```python
from tualpha import order_target_percent, record, run_algorithm, symbol


def initialize(context):
    context.asset = symbol("510300.SH")


def handle_data(context, data):
    closes = data.history(context.asset, "close", 20).dropna()
    if len(closes) < 20:
        record(ready=0, target_weight=0.0)
        return

    close = float(closes.iloc[-1])
    ma20 = float(closes.mean())
    target = 0.95 if close > ma20 else 0.0

    if data.can_trade(context.asset):
        order_target_percent(context.asset, target)

    record(ready=1, close=close, ma20=ma20, target_weight=target)


if __name__ == "__main__":
    result = run_algorithm(
        start="2020-01-01",
        end="2026-08-25",
        initialize=initialize,
        handle_data=handle_data,
        capital_base=1_000_000,
        adjustment="qfq",
        execution_time="open",
        benchmark="000300.SH",
        output_dir="outputs/ma20_etf",
        strategy_name="沪深300ETF MA20",
    )
    print(result.summary())
```

运行：

```bash
uv run python main.py
```

输出：

```text
outputs/ma20_etf/
├── report.html
└── daily_positions.csv
```

结构化结果包括：

```python
result.performance
result.orders
result.transactions
result.closed_trades
result.daily_positions
result.records
result.metrics
```

## 必须理解的时序

```text
D 日 handle_data 读取截至 D 日的数据
  → D 日提交订单
  → 最早 D+1 按 execution_time 成交
```

`data.can_trade()` 只描述当前日是否可交易，不能保证 D+1 一定成交。次日仍可能因为停牌、涨跌停、T+1、最小交易单位或现金不足而拒单。

下一步阅读[策略开发](strategy-guide.md)。
