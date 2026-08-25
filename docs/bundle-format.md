# TuAlpha Bundle 格式

本文描述 TuAlpha 0.8.0 的 `tualpha.bundle/0.8` 协议和 Bundle schema 7。

默认位置：

```text
~/.tualpha/bundle/
```

CSV 目录必须位于 Bundle 根目录之外。运行时只能通过 AssetFinder、ChinaTradingCalendar 和 DataPortal 读取最终 Bundle。

## 1. 固定文件集合

`bundle/` 必须且只能包含：

```text
bundle/
├── daily.h5
├── adj_factor.h5
├── daily_basic.h5
├── stk_limit.h5
├── finance.h5
├── industry.h5
├── stock_st.h5
├── moneyflow.h5
├── index_weight.h5
├── trade_dates.npy
├── assets.pk
└── suspend_d.h5
```

不存在 manifest、READY、SQLite、DuckDB、Bcolz 或分钟数据旁路。`assets.pk` 同时承担资产清单和不可变 manifest 的职责。

## 2. HDF5 通用协议

除 `trade_dates.npy` 和 `assets.pk` 外，每个 HDF5 文件根属性均包含：

| 属性 | 值 |
|---|---|
| `protocol` | `tualpha.bundle/0.8` |
| `schema_version` | `7` |
| `file_role` | 文件角色 |
| `generation` | 本代 UUID |
| `generated_at` | UTC ISO-8601 |
| `date_encoding` | `YYYYMMDD` |
| `key_layout` | `data/<ts_code>` |

证券数据按 Tushare 代码建立一维 NumPy compound dataset：

```text
/data/000001.SZ
/data/510300.SH
/data/000300.SH
```

每个 dataset 内按日期严格升序，日频主键唯一。构建器整代重建文件，不原地修改活动 Bundle。

常用横截面字段可同时存在于内部 packed 加速组：

```text
/packed/<field>
layout = tradable_sid_dense/v1
```

packed 数组按可交易资产 `sid` 升序拼接，证券内顺序与 `/data/<ts_code>` 完全一致；Reader 使用 `assets.pk` 和日线长度计算切片。它只是同一 HDF5 文件内的冗余加速索引，不是策略可直接访问的新数据源。缺少 packed 组的早期 schema 7 Bundle 仍可通过逐证券回退路径读取。

当前 packed 字段：

```text
daily       : open, high, low, close, pre_close, volume, turnover
daily_basic : total_mv
stk_limit   : up_limit, down_limit
stock_st    : is_st
suspend_d   : suspended
```

数值约定：

- 日期：little-endian `int32 YYYYMMDD`；
- 数值：little-endian `float64`；
- 缺失数值：`NaN`；
- 分类 ID：`int32`，`-1` 表示缺失；
- 标志：`uint8`；
- 代码：ASCII 定长字节；
- 股票成交量：股；ETF 成交量：份；
- 成交额：元。

## 3. `daily.h5`

同时保存股票、ETF 和指数的未复权日线。指数在 `assets.pk` 中标记 `tradable=False`，只能用于基准。

```python
np.dtype(
    [
        ("trade_date", "<i4"),
        ("open", "<f8"),
        ("high", "<f8"),
        ("low", "<f8"),
        ("close", "<f8"),
        ("pre_close", "<f8"),
        ("volume", "<f8"),
        ("turnover", "<f8"),
    ]
)
```

股票和 ETF 在资产生命周期内按交易日历稠密保存；缺失行情保留日期并写 `NaN/0`。指数按供应商实际日期稀疏保存。

OHLC 是唯一参与 `qfq/hfq` 的日频价格域。`pre_close`、涨跌停价格、成交价格、现金和费用始终使用原始值。

## 4. `adj_factor.h5`

股票和 ETF 的复权因子：

```python
np.dtype(
    [
        ("trade_date", "<i4"),
        ("adj_factor", "<f8"),
    ]
)
```

有效因子必须有限且大于零。查询使用不晚于目标日的最新有效因子；首条记录前按 `1.0`。

```text
qfq(i, D) = raw(i) × factor(i) / factor(D)
hfq(i)    = raw(i) × factor(i)
```

## 5. `daily_basic.h5`

字段除 `trade_date` 外均为 `float64`：

```text
close, turnover_rate, turnover_rate_f, volume_ratio,
pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm,
total_share, float_share, free_share, total_mv, circ_mv,
limit_status
```

`daily_basic.close` 是 Tushare 原始值，不参与复权。

## 6. `stk_limit.h5`

```python
np.dtype(
    [
        ("trade_date", "<i4"),
        ("up_limit", "<f8"),
        ("down_limit", "<f8"),
    ]
)
```

D+1 撮合读取 D+1 原始开盘或收盘价：买入端点触及涨停则拒绝，卖出端点触及跌停则拒绝。

## 7. `suspend_d.h5`

```python
np.dtype(
    [
        ("trade_date", "<i4"),
        ("suspended", "u1"),
    ]
)
```

当天存在 Tushare `suspend_type="S"` 记录时为 1，否则为 0。

## 8. `moneyflow.h5`

所有值字段为 `float64`：

```text
buy_sm_vol, buy_sm_amount, sell_sm_vol, sell_sm_amount,
buy_md_vol, buy_md_amount, sell_md_vol, sell_md_amount,
buy_lg_vol, buy_lg_amount, sell_lg_vol, sell_lg_amount,
buy_elg_vol, buy_elg_amount, sell_elg_vol, sell_elg_amount,
net_mf_vol, net_mf_amount
```

量字段单位为手，金额字段单位为万元，保持 Tushare 原始口径。

## 9. `industry.h5`

```python
np.dtype(
    [
        ("trade_date", "<i4"),
        ("l1_code", "<i4"),
        ("l1_name", "<i4"),
        ("l2_code", "<i4"),
        ("l2_name", "<i4"),
        ("l3_code", "<i4"),
        ("l3_name", "<i4"),
    ]
)
```

分类文本位于 `/dictionary/<field>` UTF-8 dataset。行业按历史 `in_date/out_date` 物化，不使用当前行业回填历史。

## 10. `stock_st.h5`

```python
np.dtype(
    [
        ("trade_date", "<i4"),
        ("name", "<i4"),
        ("type", "<i4"),
        ("type_name", "<i4"),
        ("is_st", "u1"),
    ]
)
```

文本字典位于 `/dictionary`。原始 ST 日 `is_st=1`，无记录日期为 0，不使用当前 ST 名单覆盖历史。

## 11. `index_weight.h5`

每个指数一个 dataset：

```python
np.dtype(
    [
        ("snapshot_date", "<i4"),
        ("con_code", "S16"),
        ("sid", "<i8"),  # 无法映射时 -1
        ("weight", "<f8"),  # 百分比
    ]
)
```

逻辑主键为 `(index_code, snapshot_date, con_code)`。查询规则严格为：

```text
max(snapshot_date) WHERE snapshot_date < session
```

快照日当天不可见，首个快照前返回空集，不反向填充未来成分。

## 12. `finance.h5`

组结构：

```text
/data/balancesheet/<ts_code>
/data/income/<ts_code>
/data/cashflow/<ts_code>
/data/fina_indicator/<ts_code>
```

统一元数据：

```text
ann_date, f_ann_date, effective_ann_date, end_date : int32 YYYYMMDD
report_type, comp_type, end_type, update_flag      : S16
source_order                                        : uint64
数值字段                                             : float64
```

数值字段固定来自 `src/tualpha/tushare_fields.py`，不根据偶然 CSV 列动态改变 schema。

PIT 可见性：

```text
effective_ann_date < session
end_date <= session
```

同报告期修订排序：

1. `effective_ann_date` 最新；
2. `update_flag == "1"` 优先；
3. `source_order` 最大。

默认报表选择 `report_type="1"`；`fina_indicator` 不做 report type 过滤。利润表和现金流量表保留年初至今累计值。

## 13. `trade_dates.npy`

```text
dtype: little-endian int32
shape: (session_count,)
value: YYYYMMDD
allow_pickle: False
```

日期严格递增且唯一，只包含 Tushare `trade_cal(exchange="SSE", is_open=1)` 的开放日。所有日频文件日期必须属于该数组。

## 14. `assets.pk`

使用 Pickle protocol 5，但内容只允许原生字典、列表、字符串和数值。Reader 使用 RestrictedUnpickler，禁止加载任意 Python 全局对象。

顶层字段：

```text
protocol, schema_version, generation, generated_at,
start_session, end_session, session_count,
asset_count, index_count, calendar_source,
build_pipeline, packed_acceleration, files, assets, index_codes, pit_rules
```

每个资产：

```text
sid, ts_code, symbol, name, asset_type, tradable,
exchange, board, list_date, delist_date, price_tick,
round_lot, minimum_order, settlement_days
```

`sid` 从现有活动 `assets.pk` 继承；首次迁移可读取旧 sid-map，随后稳定映射只保存在活动 `assets.pk`。

`files` 记录其他 11 个文件的角色、行数、大小和 SHA-256。`packed_acceleration` 记录布局、拼接行数及各文件加速字段。

## 15. generation 和验证

相同 UUID generation 必须存在于 `assets.pk` 和全部 HDF5 根属性。Reader 打开顺序：

1. 持有 Bundle 发布锁；
2. RestrictedUnpickler 加载 `assets.pk`；
3. 校验目录恰好为 12 文件；
4. 校验 schema、generation、文件大小和交易日范围；
5. 打开全部 HDF5；
6. generation 不一致时拒绝回测。

发布前还会执行：

- 全文件 SHA-256；
- HDF5 全 dataset 扫描；
- compound dtype 和日期排序检查；
- 日频日期属于 `trade_dates.npy`；
- 正式 Reader 重新打开；
- 真实策略回归后才清理旧格式。

## 16. CSV 分桶构建

TuAlpha 0.8 不使用数据库中间层：

```text
原始 CSV
  → .staging 中按 hash(ts_code) 分桶的临时 Parquet
  → 桶内按 ts_code + date + source_order 排序
  → 临时有序 NumPy structured binary
  → /data/<ts_code> HDF5 compound dataset
  → 校验并发布 bundle/
```

大规模 Polars 分桶在隔离子进程执行，避免解析器和 HDF5 Writer 共享长生命周期原生内存。所有 staging 文件在成功或正常失败后删除。

## 17. 原子发布和清理

构建阶段不持发布锁。验证完成后：

```text
获取 bundle.lock
当前 bundle/ → .rollback/<generation>/bundle/
staging bundle/ → bundle/
从最终路径重新打开
成功后删除 rollback
```

所有规范 Reader 在整个生命周期持锁，因此不会观察到目录替换中间态。

schema 7 最终发布并验证成功后，清理器只删除 `~/.tualpha` 内部精确匹配的旧 `bundles/`、`cache/`、`*.bcolz` 和 `*.duckdb`；拒绝跟随 symlink。原始 CSV 不删除。

更新与清理证据原子写入：

```text
~/.tualpha/update-status.json
```
