"""中证 1000 全算子因子计算性能基准。

逐日使用严格 PIT 的中证 1000 成分，剔除 ST、上市不足 365 天和停牌股票。
脚本覆盖 ``算子.md`` 中的每一个基础/技术算子，不构成投资建议。
"""

from __future__ import annotations

import argparse
import gc
import json
import platform
from pathlib import Path
from time import perf_counter

import numpy as np

import tualpha
from tualpha import factor_data
from tualpha.data.factors import available_operators

START_DATE = "2017-01-01"
END_DATE = "2026-01-01"
INDEX_CODE = "000852.SH"

OPERATOR_EXPRESSIONS = {
    "ABS": "ABS($close-$open)",
    "ADXR": "ADXR($close,14)",
    "ADV": "ADV($volume,20)",
    "ANGLE": "ANGLE($close,10)",
    "ARBR": "ARBR($open,$close,$high,$low,20)",
    "AROONOSC": "AROONOSC($close,14)",
    "ASI": "ASI($open,$close,$high,$low,20,6)",
    "ASIT": "ASIT($open,$close,$high,$low,20,6)",
    "AS_FLOAT": "AS_FLOAT($close>$open)",
    "ATR": "ATR($close,14)",
    "AVEDEV": "AVEDEV(RETURNS($close,1),20)",
    "BARSLAST": "BARSLAST($close>MA($close,20))",
    "BARSLASTCOUNT": "BARSLASTCOUNT($close>$open)",
    "BARSSINCEN": "BARSSINCEN($volume>MA($volume,20),10)",
    "BBI": "BBI($close,3,6,12,20)",
    "BIAS": "BIAS($close,6)",
    "BOLLINGERDIFF": "BOLLINGERDIFF($high,$low)",
    "BOLL_LOWER": "BOLL_LOWER($close,20,2)",
    "BOLL_MID": "BOLL_MID($close,20,2)",
    "BOLL_UPPER": "BOLL_UPPER($close,20,2)",
    "BOLL_WIDTH": "BOLL_WIDTH($close,20)",
    "BRAR": "BRAR($open,$close,$high,$low,20)",
    "CCI": "CCI($close,14)",
    "CMO": "CMO($close,14)",
    "CONST": "CONST($close)",
    "CORR": "CORR($close,$volume,20)",
    "CORRELATION": "CORRELATION($close,$volume,20)",
    "COUNT": "COUNT($close>$open,20)",
    "COV": "COV($close,$volume,20)",
    "COVARIANCE": "COVARIANCE($close,$volume,20)",
    "CROSS": "CROSS(MA($close,5),MA($close,20))",
    "DECAYLINEAR": "DECAYLINEAR($volume,10)",
    "DELAY": "DELAY($volume,5)",
    "DELTA": "DELTA($close,1)",
    "DEMA": "DEMA($close,14)",
    "DIF": "DIF($close,10,20,6)",
    "DIFF": "DIFF($close,1)",
    "DFMA": "DFMA($close,10,20,6)",
    "DMA": "DMA($close,0.1)",
    "DMI_ADX": "DMI_ADX($close,$high,$low,14,6)",
    "DMI_ADXR": "DMI_ADXR($close,$high,$low,14,6)",
    "DMI_MDI": "DMI_MDI($close,$high,$low,14,6)",
    "DMI_PDI": "DMI_PDI($close,$high,$low,14,6)",
    "DPO": "DPO($close,20,10,6)",
    "DPOMA": "DPOMA($close,20,10,6)",
    "EMA": "EMA($close,12)",
    "EMV": "EMV($high,$low,$volume,14,9)",
    "EMVMA": "EMVMA($high,$low,$volume,14,9)",
    "EQUAL": "EQUAL($close,$open)",
    "EVERY": "EVERY($close>MA($close,5),10)",
    "EXIST": "EXIST($close>=DELAY($close,1)*1.1,20)",
    "EXPMA": "EXPMA($close,12,20)",
    "EXPMA2": "EXPMA2($close,12,20)",
    "FORCAST": "FORCAST($close,10)",
    "FUTURE_RETURNS": "FUTURE_RETURNS($close,5)",
    "HHVBARS": "HHVBARS($high,20)",
    "IF": "IF($close>$open,1,-1)",
    "INTERCEPT": "INTERCEPT($close,20)",
    "KAMA": "KAMA($close,14)",
    "KDJ_D": "KDJ_D($close,$high,$low,9,3,3)",
    "KDJ_J": "KDJ_J($close,$high,$low,9,3,3)",
    "KDJ_K": "KDJ_K($close,$high,$low,9,3,3)",
    "LAST": "LAST($close>MA($close,20),1,10)",
    "LLVBARS": "LLVBARS($low,20)",
    "LOG": "LOG($close/$open)",
    "LOGABS": "LOGABS(RETURNS($close,10))",
    "LONGCROSS": "LONGCROSS($close,MA($close,20),5)",
    "MA": "MA($close,20)",
    "MACD": "MACD($close,12,26,9)",
    "MACD_DEA": "MACD_DEA($close,12,26,9)",
    "MACD_DIF": "MACD_DIF($close,12,26,9)",
    "MASS": "MASS($high,$low,9,20,6)",
    "MASSMA": "MASSMA($high,$low,9,20,6)",
    "MAX": "MAX($close,$open)",
    "MEAN": "MEAN($high,$low)",
    "MEAN_ABS_PRICE_CHANGE": "MEAN_ABS_PRICE_CHANGE($close,10)",
    "MFI": "MFI($close,$high,$low,$volume,14)",
    "MIN": "MIN($close,$open)",
    "MTM": "MTM($close,12,6)",
    "MTMMA": "MTMMA($close,12,6)",
    "OBV": "OBV($close,$volume)",
    "PCT_CHANGE": "PCT_CHANGE($close,1)",
    "POWER": "POWER(RETURNS($close,10),2)",
    "PPO": "PPO(EMA($close,12),EMA($close,26))",
    "PRODUCT": "PRODUCT(1+RETURNS($close,1),20)",
    "PSY": "PSY($close,12)",
    "PSYMA": "PSYMA($close,12,6)",
    "RD": "RD($high/$low,4)",
    "RANK": "RANK($close/$open)",
    "REF": "REF($close,1)",
    "RETURNS": "RETURNS($close,5)",
    "ROC": "ROC($close,5)",
    "ROCMA": "ROCMA($close,12,6)",
    "RSI": "RSI($close,14)",
    "SCALE": "SCALE($volume)",
    "SHARPE": "SHARPE($close,20)",
    "SIGN": "SIGN(RETURNS($close,10))",
    "SIGNEDPOWER": "SIGNEDPOWER(RETURNS($close,10),2)",
    "SLOPE": "SLOPE($close,20)",
    "SMA": "SMA($close,12,1)",
    "STD": "STD(RETURNS($close,1),20)",
    "STDDEV": "STDDEV($close,10)",
    "STOCHASTIC": "STOCHASTIC($close,14)",
    "SUM": "SUM($volume,5)",
    "SUMIF": "SUMIF($close>$open,$volume,10)",
    "SUM_ABS_PRICE_CHANGE": "SUM_ABS_PRICE_CHANGE($close,10)",
    "T3": "T3($close,14)",
    "TEMA": "TEMA($close,14)",
    "TRIMA": "TRIMA($close,12,9)",
    "TRIX": "TRIX($close,12,9)",
    "TS_ARGMAX": "TS_ARGMAX($high,10)",
    "TS_ARGMIN": "TS_ARGMIN($low,10)",
    "TS_KURT": "TS_KURT(RETURNS($close,1),20)",
    "TS_MAD": "TS_MAD(RETURNS($close,1),20)",
    "TS_MAX": "TS_MAX($high,20)",
    "TS_MEAN": "TS_MEAN($volume,10)",
    "TS_MEDIAN": "TS_MEDIAN($close,10)",
    "TS_MIDDLE": "TS_MIDDLE($close,10)",
    "TS_MIN": "TS_MIN($low,20)",
    "TS_RANK": "TS_RANK($volume,20)",
    "TS_REGRESSION": "TS_REGRESSION($close,$volume,20)",
    "TS_SKEW": "TS_SKEW(RETURNS($close,1),20)",
    "TS_ZSCORE": "TS_ZSCORE($close,20)",
    "VALUEWHEN": "VALUEWHEN($volume>MA($volume,20),$close)",
    "VAR": "VAR(RETURNS($close,1),20)",
    "VR": "VR($close,$volume,20)",
    "WMA": "WMA($close,10)",
    "WR": "WR($close,14)",
    "ZSCORE": "ZSCORE(RETURNS($close,10))",
}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="运行中证1000全算子因子性能基准",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--start", default=START_DATE)
    parser.add_argument("--end", default=END_DATE)
    parser.add_argument("--bundle-root", default="~/.tualpha")
    parser.add_argument("--bundle-name", default="tualpha")
    parser.add_argument("--output-dir", default="outputs/performance/csi1000_factors")
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--column-cache-mib", type=int, default=0)
    parser.add_argument(
        "--operator",
        action="append",
        choices=tuple(OPERATOR_EXPRESSIONS),
        help="只运行指定算子，可重复传入；默认运行全部算子",
    )
    parser.add_argument("--no-prefetch", dest="prefetch", action="store_false")
    parser.set_defaults(prefetch=True)
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.batch_size <= 0:
        raise ValueError("batch-size must be positive")
    documented = set(available_operators())
    covered = set(OPERATOR_EXPRESSIONS)
    if documented != covered:
        raise RuntimeError(
            f"算子覆盖不完整，缺少={sorted(documented - covered)}，多余={sorted(covered - documented)}"
        )

    started = perf_counter()
    timings = []
    finite_values = 0
    checksum = 0.0
    selected_names = args.operator or list(OPERATOR_EXPRESSIONS)
    expressions = [OPERATOR_EXPRESSIONS[name] for name in selected_names]
    with factor_data(
        start=args.start,
        end=args.end,
        index_code=INDEX_CODE,
        asset_type="stock",
        bundle_root=args.bundle_root,
        bundle_name=args.bundle_name,
        adjustment="raw",
        exclude_st=True,
        min_listed_days=365,
        exclude_suspended=True,
        column_cache_mib=args.column_cache_mib,
    ) as data:
        if args.prefetch:
            data.prefetch(expressions)
            print(f"已预取 {len(data.assets)} 只股票的共享物理字段", flush=True)
        asset_count = len(data.assets)
        session_count = len(data.sessions)
        for offset in range(0, len(expressions), args.batch_size):
            batch = expressions[offset : offset + args.batch_size]
            batch_started = perf_counter()
            values = data.history_arrays(batch, allow_future=True)
            array = next(iter(values.values())).to_numpy(dtype=float, copy=False)
            finite = np.isfinite(array)
            finite_values += int(finite.sum())
            checksum += float(np.nansum(array))
            timings.append(
                {
                    "first_operator": selected_names[offset],
                    "operator_count": len(batch),
                    "seconds": round(perf_counter() - batch_started, 6),
                }
            )
            if (offset // args.batch_size + 1) % 10 == 0:
                print(
                    f"已完成 {min(offset + args.batch_size, len(expressions))}/"
                    f"{len(expressions)} 个算子",
                    flush=True,
                )
            del array, finite, values
            gc.collect()
    elapsed = perf_counter() - started
    payload = {
        "benchmark": "中证1000全算子因子计算",
        "tualpha_version": tualpha.__version__,
        "python_version": platform.python_version(),
        "start": args.start,
        "end": args.end,
        "index_code": INDEX_CODE,
        "adjustment": "raw",
        "filters": {
            "exclude_st": True,
            "min_listed_days": 365,
            "exclude_suspended": True,
        },
        "sessions": session_count,
        "assets_in_snapshot_union": asset_count,
        "operators": len(expressions),
        "operator_names": selected_names,
        "batch_size": args.batch_size,
        "prefetch": args.prefetch,
        "elapsed_seconds": round(elapsed, 6),
        "operator_asset_sessions_per_second": round(
            len(expressions) * session_count * asset_count / elapsed, 3
        )
        if elapsed
        else None,
        "finite_values": finite_values,
        "checksum": checksum,
        "batches": timings,
    }
    destination = Path(args.output_dir) / "benchmark.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    print(f"\n性能记录：{destination}")


if __name__ == "__main__":
    main()
