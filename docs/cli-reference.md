# CLI 参考

```bash
uv run tualpha --help
uv run tualpha --version
```

TuAlpha CLI 包含 `build`、`update`、`quality` 和 `compact` 四个子命令。

## `tualpha build`

从 Tushare 全量构建 Bundle。

```bash
uv run tualpha build --from YYYYMMDD [options]
```

| 参数 | 含义 |
|---|---|
| `--from YYYYMMDD` | 必填，全量构建起始交易日 |
| `--to YYYYMMDD` | 可选结束日期，受完成交易日限制 |
| `--bundle-root PATH` | 数据根目录，默认 `~/.tualpha` |
| `--bundle-name NAME` | Bundle 名称，默认 `tualpha` |
| `--index-weight CODE` | 追加指数权重，可重复指定 |
| `--retries N` | API 请求重试次数，默认 3 |
| `--backoff SECONDS` | 重试退避基数，默认 2 秒 |
| `--token-stdin` | 从标准输入读取 Token |
| `--dry-run` | 完成下载和整理，但不发布 Bundle |
| `--no-progress` | 关闭进度条 |
| `--json` | 输出机器可读 JSON |

示例：

```bash
uv run tualpha build \
  --from 20100101 \
  --to 20260825 \
  --index-weight 000016.SH
```

## `tualpha update`

按缺口增量更新活动 Bundle。

```bash
uv run tualpha update [options]
```

| 参数 | 含义 |
|---|---|
| `--from YYYYMMDD` | 强制刷新该日期起的数据 |
| `--to YYYYMMDD` | 指定结束日期 |
| `--repair-from YYYYMMDD` | 从指定日期执行修复更新 |
| `--lookback N` | 强制刷新最近 N 个交易日，默认 0 |
| `--bundle-root PATH` | 数据根目录 |
| `--bundle-name NAME` | Bundle 名称 |
| `--index-weight CODE` | 追加指数权重，可重复指定 |
| `--retries N` | API 请求重试次数 |
| `--backoff SECONDS` | 重试退避基数 |
| `--token-stdin` | 从标准输入读取 Token |
| `--dry-run` | 不发布 Bundle |
| `--no-progress` | 关闭进度条 |
| `--json` | 输出 JSON |
| `--compact` | 兼容参数；年度 Parquet 无需压缩 |

默认仅下载各日频数据集缺失的日期。交易日北京时间 17:00 起可更新当天数据。

```bash
uv run tualpha update --dry-run --json
```

JSON 示例：

```json
{
  "operation": "update",
  "run_id": "...",
  "updated_dates": ["20260826"],
  "updated_files": 0,
  "bundle_path": null,
  "dry_run": true
}
```

## `tualpha quality`

运行表级数据质量检查。

```bash
uv run tualpha quality [options]
```

| 参数 | 含义 |
|---|---|
| `--table TABLE` | 只检查指定表，可重复使用 |
| `--full-hash` | 重新计算所有文件 SHA-256 |
| `--report-dir PATH` | 自定义报告根目录 |
| `--bundle-root PATH` | 数据根目录 |
| `--bundle-name NAME` | Bundle 名称 |
| `--json` | 输出摘要 JSON |

示例：

```bash
uv run tualpha quality \
  --table stock_daily \
  --table income \
  --full-hash
```

存在失败项时命令返回退出码 1。

## `tualpha compact`

验证活动年度 Parquet Bundle 的兼容命令：

```bash
uv run tualpha compact --json
```

该命令验证 Bundle，不执行传统小文件合并或重新压缩。

## Token 优先级

`build` 和 `update` 按以下方式获取 Token：

1. 指定 `--token-stdin` 时从标准输入读取；
2. 否则读取 `TUSHARE_TOKEN` 环境变量；
3. 未提供时返回退出码 2。

`quality` 和 `compact` 只读取本地数据，不需要 Tushare Token。
