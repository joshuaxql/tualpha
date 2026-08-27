# Repository Guidelines

## 项目结构与模块组织

TuAlpha 是面向 Python 3.12 的日频事件驱动回测库。生产代码位于 `src/tualpha/`：`foundation/` 提供配置和异常，`core/` 负责算法调度，`broker/` 实现撮合与市场规则，`data/` 管理查询和 Parquet Bundle，`model/` 定义领域对象，`analysis/` 与 `report/` 分别负责绩效结果和可视化报告，`cmds/` 提供命令行入口。根目录 `.py` 模块仅保留公开接口或兼容门面。测试集中在 `tests/`，文档源码位于 `docs/`；`site/` 和 `dist/` 均为生成目录，不应手工修改。

## 构建、测试与开发命令

- `uv sync --dev`：依据 `uv.lock` 安装项目及开发依赖。
- `uv run pytest -q`：运行完整测试；可传入 `tests/test_engine.py` 等路径执行单文件测试。
- `uv run ruff check .`：检查代码规范与常见错误。
- `uv run ruff format --check .`：验证格式；使用 `uv run ruff format .` 自动格式化。
- `uv sync --group docs`：安装文档依赖。
- `uv run mkdocs build --strict`：严格检查文档、链接和 API 引用。
- `uv build`：在 `dist/` 中构建发布包。

## 编码风格与命名约定

使用四空格缩进；公开接口应提供类型注解和简洁的 Google 风格文档字符串。模块、函数和变量使用 `snake_case`，类使用 `PascalCase`，常量使用 `UPPER_SNAKE_CASE`。提交前确保导入、格式及代码符合 Ruff 检查。不得破坏核心时序：D 日决策最早 D+1 成交，并持续遵守 T+1、停牌、涨跌停、交易单位及 PIT 数据约束，避免未来函数。

## 测试规范

项目使用 pytest。测试文件命名为 `test_<领域>.py`，测试函数命名为 `test_<行为>`。每个行为修复都应补充回归测试，重点覆盖执行顺序、费用、数据可见性、Bundle 原子发布和查询边界。共享 fixture 放入 `tests/conftest.py`，轻量测试替身放入 `tests/fakes.py`。项目未配置硬性覆盖率门槛，但修改涉及的分支必须得到验证。

## 提交与拉取请求规范

近期发布提交采用 `version 1.3.4` 或 `release: version 0.8.1` 等简短格式。普通功能提交应使用简洁的祈使句，并明确受影响模块。拉取请求需说明行为变化、列出已执行的验证命令、关联相关 Issue，并注明兼容性或数据格式影响。仅在报告或文档界面发生变化时附截图。

## 安全与配置

不得提交 Tushare Token、凭据、本地 Bundle 或生成的报告。通过环境变量或标准输入传递 `TUSHARE_TOKEN`。修改迁移或更新流程时，必须保留 staging、文件锁、校验、回滚和原子发布语义，避免损坏用户数据。
