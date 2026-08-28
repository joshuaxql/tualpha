"""Safe, vectorized factor-expression parsing and evaluation.

Expressions use ``$field`` references and the operators documented in
``算子.md``.  Every value is represented as a date-by-asset DataFrame, so
time-series operators run down rows and cross-sectional operators run across
columns.
"""

from __future__ import annotations

import ast
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

import numpy as np
import pandas as pd

_FIELD_PATTERN = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)")
_FIELD_ALIASES = {
    "amount": "turnover",
    "close": "close",
    "high": "high",
    "low": "low",
    "open": "open",
    "price": "close",
    "turnover": "turnover",
    "vol": "volume",
    "volume": "volume",
}

_OPERATORS = (
    "ABS",
    "ADXR",
    "ADV",
    "ANGLE",
    "ARBR",
    "AROONOSC",
    "ASI",
    "ASIT",
    "AS_FLOAT",
    "ATR",
    "AVEDEV",
    "BARSLAST",
    "BARSLASTCOUNT",
    "BARSSINCEN",
    "BBI",
    "BIAS",
    "BOLLINGERDIFF",
    "BOLL_LOWER",
    "BOLL_MID",
    "BOLL_UPPER",
    "BOLL_WIDTH",
    "BRAR",
    "CCI",
    "CMO",
    "CONST",
    "CORR",
    "CORRELATION",
    "COUNT",
    "COV",
    "COVARIANCE",
    "CROSS",
    "DECAYLINEAR",
    "DELAY",
    "DELTA",
    "DEMA",
    "DIF",
    "DIFF",
    "DFMA",
    "DMA",
    "DMI_ADX",
    "DMI_ADXR",
    "DMI_MDI",
    "DMI_PDI",
    "DPO",
    "DPOMA",
    "EMA",
    "EMV",
    "EMVMA",
    "EQUAL",
    "EVERY",
    "EXIST",
    "EXPMA",
    "EXPMA2",
    "FORCAST",
    "FUTURE_RETURNS",
    "HHVBARS",
    "IF",
    "INTERCEPT",
    "KAMA",
    "KDJ_D",
    "KDJ_J",
    "KDJ_K",
    "LAST",
    "LLVBARS",
    "LOG",
    "LOGABS",
    "LONGCROSS",
    "MA",
    "MACD",
    "MACD_DEA",
    "MACD_DIF",
    "MASS",
    "MASSMA",
    "MAX",
    "MEAN",
    "MEAN_ABS_PRICE_CHANGE",
    "MFI",
    "MIN",
    "MTM",
    "MTMMA",
    "OBV",
    "PCT_CHANGE",
    "POWER",
    "PPO",
    "PRODUCT",
    "PSY",
    "PSYMA",
    "RD",
    "RANK",
    "REF",
    "RETURNS",
    "ROC",
    "ROCMA",
    "RSI",
    "SCALE",
    "SHARPE",
    "SIGN",
    "SIGNEDPOWER",
    "SLOPE",
    "SMA",
    "STD",
    "STDDEV",
    "STOCHASTIC",
    "SUM",
    "SUMIF",
    "SUM_ABS_PRICE_CHANGE",
    "T3",
    "TEMA",
    "TRIMA",
    "TRIX",
    "TS_ARGMAX",
    "TS_ARGMIN",
    "TS_KURT",
    "TS_MAD",
    "TS_MAX",
    "TS_MEAN",
    "TS_MEDIAN",
    "TS_MIDDLE",
    "TS_MIN",
    "TS_RANK",
    "TS_REGRESSION",
    "TS_SKEW",
    "TS_ZSCORE",
    "VALUEWHEN",
    "VAR",
    "VR",
    "WMA",
    "WR",
    "ZSCORE",
)
_OPERATOR_SET = frozenset(_OPERATORS)


def available_operators() -> tuple[str, ...]:
    """Return the supported factor operator names in stable sorted order."""

    return _OPERATORS


def _canonical_field(name: str) -> str:
    value = name.lower()
    return _FIELD_ALIASES.get(value, value)


class _ExpressionValidator(ast.NodeVisitor):
    _binary = (ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Pow, ast.Mod)
    _unary = (ast.UAdd, ast.USub, ast.Not, ast.Invert)
    _compare = (ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE)

    def generic_visit(self, node: ast.AST) -> None:
        allowed = (
            ast.Expression,
            ast.Call,
            ast.Name,
            ast.Load,
            ast.Constant,
            ast.BinOp,
            ast.UnaryOp,
            ast.Compare,
            ast.BoolOp,
            *self._binary,
            *self._unary,
            *self._compare,
            ast.And,
            ast.Or,
        )
        if not isinstance(node, allowed):
            raise TypeError(
                f"unsupported factor expression syntax: {type(node).__name__}"
            )
        super().generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        if not isinstance(node.func, ast.Name):
            raise TypeError("factor functions must be called by name")
        name = node.func.id.upper()
        if name != "__FIELD__" and name not in _OPERATOR_SET:
            raise ValueError(f"unknown factor operator: {node.func.id}")
        if node.keywords:
            raise ValueError("factor operators do not accept keyword arguments")
        for argument in node.args:
            self.visit(argument)

    def visit_Name(self, node: ast.Name) -> None:
        name = node.id.upper()
        if name not in {value.upper() for value in _FIELD_ALIASES} and name not in {
            "TRUE",
            "FALSE",
        }:
            raise ValueError(f"unknown factor identifier: {node.id}")

    def visit_Constant(self, node: ast.Constant) -> None:
        if not isinstance(node.value, (bool, int, float, str)):
            raise TypeError("factor constants must be numeric, boolean, or field names")


def _literal_number(node: ast.AST) -> float:
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
        return float(node.value)
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
        value = _literal_number(node.operand)
        return value if isinstance(node.op, ast.UAdd) else -value
    raise ValueError("operator window arguments must be numeric constants")


_SHIFT_WINDOWS = frozenset(
    {"REF", "DELAY", "DIFF", "DELTA", "ROC", "PCT_CHANGE", "RETURNS"}
)
_ROLLING_WINDOWS = frozenset(
    {
        "ADV",
        "ANGLE",
        "AROONOSC",
        "ATR",
        "AVEDEV",
        "BARSSINCEN",
        "BIAS",
        "BOLL_LOWER",
        "BOLL_MID",
        "BOLL_UPPER",
        "BOLL_WIDTH",
        "CCI",
        "CMO",
        "COUNT",
        "DECAYLINEAR",
        "EMA",
        "EVERY",
        "EXIST",
        "FORCAST",
        "HHVBARS",
        "INTERCEPT",
        "KAMA",
        "LLVBARS",
        "MA",
        "MEAN_ABS_PRICE_CHANGE",
        "PRODUCT",
        "PSY",
        "RSI",
        "SHARPE",
        "SLOPE",
        "STD",
        "STDDEV",
        "STOCHASTIC",
        "SUM",
        "SUM_ABS_PRICE_CHANGE",
        "TS_ARGMAX",
        "TS_ARGMIN",
        "TS_KURT",
        "TS_MAD",
        "TS_MAX",
        "TS_MEAN",
        "TS_MEDIAN",
        "TS_MIDDLE",
        "TS_MIN",
        "TS_RANK",
        "TS_SKEW",
        "TS_ZSCORE",
        "VAR",
        "WMA",
        "WR",
    }
)


def _dependency(node: ast.AST) -> tuple[int, int]:
    children = list(ast.iter_child_nodes(node))
    dependencies = [_dependency(child) for child in children]
    lookback = max((item[0] for item in dependencies), default=0)
    lookahead = max((item[1] for item in dependencies), default=0)
    if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
        return lookback, lookahead
    name = node.func.id.upper()
    args = node.args
    if name == "FUTURE_RETURNS":
        return lookback, lookahead + int(_literal_number(args[1]))
    if name in _SHIFT_WINDOWS:
        return lookback + int(_literal_number(args[1])), lookahead
    if name in _ROLLING_WINDOWS:
        return lookback + max(0, int(_literal_number(args[1])) - 1), lookahead
    if name in {
        "CORR",
        "CORRELATION",
        "COV",
        "COVARIANCE",
        "TS_REGRESSION",
        "SUMIF",
        "LONGCROSS",
    }:
        return lookback + max(0, int(_literal_number(args[2])) - 1), lookahead
    if name in {"LAST", "SMA"}:
        return lookback + max(0, int(_literal_number(args[1])) - 1), lookahead
    if name in {"MACD", "MACD_DEA", "MACD_DIF"}:
        base = max(int(_literal_number(args[1])), int(_literal_number(args[2]))) - 1
        signal = int(_literal_number(args[3])) - 1
        return lookback + base + (0 if name == "MACD_DIF" else signal), lookahead
    if name in {"KDJ_K", "KDJ_D", "KDJ_J"}:
        base = int(_literal_number(args[3])) + int(_literal_number(args[4])) - 2
        smooth = int(_literal_number(args[5])) - 1
        return lookback + base + (0 if name == "KDJ_K" else smooth), lookahead
    if name in {"BBI"}:
        return lookback + max(
            int(_literal_number(arg)) for arg in args[1:]
        ) - 1, lookahead
    if name in {"DMI_PDI", "DMI_MDI", "DMI_ADX", "DMI_ADXR"}:
        base = int(_literal_number(args[3])) - 1
        smooth = int(_literal_number(args[4])) - 1
        if name in {"DMI_PDI", "DMI_MDI"}:
            return lookback + base, lookahead
        return lookback + base + smooth * (2 if name == "DMI_ADXR" else 1), lookahead
    if name in {"MFI"}:
        return lookback + int(_literal_number(args[4])) - 1, lookahead
    if name in {"EMV", "EMVMA"}:
        base = int(_literal_number(args[3]))
        smooth = int(_literal_number(args[4])) - 1
        return lookback + base + (smooth if name == "EMVMA" else 0), lookahead
    if name in {"TRIX", "TRIMA"}:
        base = 3 * (int(_literal_number(args[1])) - 1) + 1
        smooth = int(_literal_number(args[2])) - 1
        return lookback + base + (smooth if name == "TRIMA" else 0), lookahead
    if name in {"MTM", "MTMMA", "ROCMA"}:
        base = int(_literal_number(args[1]))
        smooth = int(_literal_number(args[2])) - 1
        return lookback + base + (smooth if name != "MTM" else 0), lookahead
    if name in {"DPO", "DPOMA"}:
        base = max(int(_literal_number(args[1])) - 1, int(_literal_number(args[2])))
        smooth = int(_literal_number(args[3])) - 1
        return lookback + base + (smooth if name == "DPOMA" else 0), lookahead
    if name in {"DIF", "DFMA"}:
        base = max(int(_literal_number(args[1])), int(_literal_number(args[2]))) - 1
        smooth = int(_literal_number(args[3])) - 1
        return lookback + base + (smooth if name == "DFMA" else 0), lookahead
    if name in {"MASS", "MASSMA"}:
        base = (
            2 * (int(_literal_number(args[2])) - 1) + int(_literal_number(args[3])) - 1
        )
        smooth = int(_literal_number(args[4])) - 1
        return lookback + base + (smooth if name == "MASSMA" else 0), lookahead
    if name in {"ASI", "ASIT"}:
        base = int(_literal_number(args[4]))
        smooth = int(_literal_number(args[5])) - 1
        return lookback + base + (smooth if name == "ASIT" else 0), lookahead
    if name in {"BRAR", "ARBR"}:
        return lookback + int(_literal_number(args[4])) - 1, lookahead
    if name in {"PSYMA"}:
        return lookback + int(_literal_number(args[1])) + int(
            _literal_number(args[2])
        ) - 1, lookahead
    if name in {"EXPMA", "EXPMA2"}:
        return lookback + max(
            int(_literal_number(args[1])), int(_literal_number(args[2]))
        ) - 1, lookahead
    if name == "DEMA":
        return lookback + 2 * (int(_literal_number(args[1])) - 1), lookahead
    if name == "TEMA":
        return lookback + 3 * (int(_literal_number(args[1])) - 1), lookahead
    if name == "T3":
        return lookback + 6 * (int(_literal_number(args[1])) - 1), lookahead
    return lookback, lookahead


@dataclass(frozen=True, slots=True)
class FactorExpression:
    """One validated expression and its physical data dependencies."""

    source: str
    tree: ast.Expression
    fields: tuple[str, ...]
    lookback: int
    lookahead: int


@lru_cache(maxsize=2_048)
def compile_expression(source: str) -> FactorExpression:
    """Parse and validate one expression without executing arbitrary Python."""

    expression = source.strip()
    if not expression:
        raise ValueError("factor expression must not be empty")
    fields = [
        _canonical_field(match.group(1))
        for match in _FIELD_PATTERN.finditer(expression)
    ]
    rewritten = _FIELD_PATTERN.sub(
        lambda match: f'__field__("{_canonical_field(match.group(1))}")', expression
    )
    try:
        tree = ast.parse(rewritten, mode="eval")
    except SyntaxError as exc:
        raise ValueError(f"invalid factor expression {source!r}: {exc.msg}") from exc
    _ExpressionValidator().visit(tree)
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id.upper() == "__FIELD__"
        ):
            if len(node.args) != 1 or not isinstance(node.args[0], ast.Constant):
                raise ValueError("invalid field reference")
            field = _canonical_field(str(node.args[0].value))
            if field not in fields:
                fields.append(field)
        elif isinstance(node, ast.Name) and node.id.upper() in {
            value.upper() for value in _FIELD_ALIASES
        }:
            field = _canonical_field(node.id)
            if field not in fields:
                fields.append(field)
    lookback, lookahead = _dependency(tree.body)
    return FactorExpression(
        source=expression,
        tree=tree,
        fields=tuple(dict.fromkeys(fields)),
        lookback=max(0, lookback),
        lookahead=max(0, lookahead),
    )


def is_factor_expression(value: str) -> bool:
    """Return whether a history field is an expression rather than a raw field."""

    return "$" in value or any(character in value for character in "()+-*/<>=!")


def expression_fields(expressions: Sequence[str]) -> tuple[str, ...]:
    """Return unique raw fields needed by a collection of expressions."""

    return tuple(
        dict.fromkeys(
            field
            for expression in expressions
            for field in compile_expression(expression).fields
        )
    )


def expression_window(expressions: Sequence[str]) -> tuple[int, int]:
    """Return aggregate lookback and lookahead session counts."""

    compiled = [compile_expression(expression) for expression in expressions]
    return (
        max((item.lookback for item in compiled), default=0),
        max((item.lookahead for item in compiled), default=0),
    )


def _finite(frame: pd.DataFrame) -> pd.DataFrame:
    if all(pd.api.types.is_bool_dtype(dtype) for dtype in frame.dtypes):
        return frame
    return frame.replace([np.inf, -np.inf], np.nan)


def _window(value: Any, *, minimum: int = 1) -> int:
    result = int(value)
    if result < minimum or float(value) != result:
        raise ValueError(f"operator window must be an integer >= {minimum}")
    return result


def _rolling_apply(frame: pd.DataFrame, window: int, function: Any) -> pd.DataFrame:
    values = frame.to_numpy(dtype=float, copy=False)
    output = np.full(values.shape, np.nan, dtype=float)
    if len(frame) < window or not values.shape[1]:
        return pd.DataFrame(output, index=frame.index, columns=frame.columns)
    chunk_columns = max(8, min(64, 1_048_576 // max(1, len(frame) * window)))
    for start in range(0, values.shape[1], chunk_columns):
        stop = min(values.shape[1], start + chunk_columns)
        windows = np.lib.stride_tricks.sliding_window_view(
            values[:, start:stop], window, axis=0
        )
        valid = np.isfinite(windows).all(axis=-1)
        reduced = np.asarray(function(windows), dtype=float)
        reduced[~valid] = np.nan
        output[window - 1 :, start:stop] = reduced
    return pd.DataFrame(output, index=frame.index, columns=frame.columns)


def _slope(frame: pd.DataFrame, window: int) -> pd.DataFrame:
    x = np.arange(window, dtype=float)
    centered = x - x.mean()
    denominator = float(np.dot(centered, centered))
    return _rolling_apply(
        frame,
        window,
        lambda values: np.tensordot(values, centered, axes=([-1], [0])) / denominator,
    )


def _chinese_sma(frame: pd.DataFrame, window: int, weight: float) -> pd.DataFrame:
    alpha = float(weight) / window
    if not 0 < alpha <= 1:
        raise ValueError("SMA requires 0 < M/N <= 1")
    return frame.ewm(alpha=alpha, adjust=False, min_periods=window).mean()


def _barslast(condition: pd.DataFrame) -> pd.DataFrame:
    values = condition.fillna(False).to_numpy(dtype=bool)
    output = np.full(values.shape, np.nan, dtype=float)
    last = np.full(values.shape[1], -1, dtype=np.int64)
    for row in range(values.shape[0]):
        last[values[row]] = row
        visible = last >= 0
        output[row, visible] = row - last[visible]
    return pd.DataFrame(output, index=condition.index, columns=condition.columns)


def _barslastcount(condition: pd.DataFrame) -> pd.DataFrame:
    values = condition.fillna(False).to_numpy(dtype=bool)
    output = np.zeros(values.shape, dtype=float)
    count = np.zeros(values.shape[1], dtype=float)
    for row in range(values.shape[0]):
        count = np.where(values[row], count + 1.0, 0.0)
        output[row] = count
    return pd.DataFrame(output, index=condition.index, columns=condition.columns)


def _dynamic_average(frame: pd.DataFrame, alpha: float | pd.DataFrame) -> pd.DataFrame:
    if np.isscalar(alpha):
        value = float(alpha)
        if not 0 < value < 1:
            raise ValueError("DMA requires 0 < A < 1")
        return frame.ewm(alpha=value, adjust=False).mean()
    weights = alpha.reindex_like(frame).to_numpy(dtype=float)
    if bool(((weights <= 0) | (weights >= 1)).any()):
        raise ValueError("DMA requires every A value to satisfy 0 < A < 1")
    values = frame.to_numpy(dtype=float)
    output = np.full(values.shape, np.nan, dtype=float)
    for row in range(values.shape[0]):
        if row == 0:
            output[row] = values[row]
        else:
            output[row] = (
                weights[row] * values[row] + (1 - weights[row]) * output[row - 1]
            )
    return pd.DataFrame(output, index=frame.index, columns=frame.columns)


def _kama(frame: pd.DataFrame, window: int) -> pd.DataFrame:
    change = (frame - frame.shift(window)).abs()
    volatility = frame.diff().abs().rolling(window, min_periods=window).sum()
    efficiency = (change / volatility).clip(0.0, 1.0)
    smoothing = (efficiency * (2 / 3 - 2 / 31) + 2 / 31) ** 2
    values = frame.to_numpy(dtype=float)
    weights = smoothing.to_numpy(dtype=float)
    output = np.full(values.shape, np.nan, dtype=float)
    if len(frame):
        output[0] = values[0]
    for row in range(1, len(frame)):
        prior = output[row - 1]
        prior = np.where(np.isfinite(prior), prior, values[row - 1])
        output[row] = prior + weights[row] * (values[row] - prior)
    result = pd.DataFrame(output, index=frame.index, columns=frame.columns)
    return result.where(efficiency.notna())


class FactorEngine:
    """Evaluate compiled expressions against shared raw input matrices."""

    def __init__(self, inputs: Mapping[str, pd.DataFrame]) -> None:
        self.inputs = {_canonical_field(name): value for name, value in inputs.items()}
        self._cache: dict[str, Any] = {}
        self._template = next(iter(self.inputs.values()), None)

    def evaluate(self, expressions: str | Sequence[str]) -> dict[str, pd.DataFrame]:
        names = [expressions] if isinstance(expressions, str) else list(expressions)
        output: dict[str, pd.DataFrame] = {}
        for source in names:
            compiled = compile_expression(source)
            value = self._evaluate(compiled.tree.body)
            output[source] = _finite(self._as_frame(value)).astype(float)
        return output

    def _as_frame(self, value: Any) -> pd.DataFrame:
        if isinstance(value, pd.DataFrame):
            return value
        if self._template is None:
            raise ValueError(
                "constant-only factor expressions require at least one input field"
            )
        return pd.DataFrame(
            value, index=self._template.index, columns=self._template.columns
        )

    def _evaluate(self, node: ast.AST) -> Any:
        key = ast.dump(node, annotate_fields=False)
        if key in self._cache:
            return self._cache[key]
        value = self._evaluate_uncached(node)
        self._cache[key] = value
        return value

    def _evaluate_uncached(self, node: ast.AST) -> Any:
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.Name):
            name = node.id.lower()
            if name in {"true", "false"}:
                return name == "true"
            return self._field(name)
        if isinstance(node, ast.UnaryOp):
            value = self._evaluate(node.operand)
            if isinstance(node.op, ast.UAdd):
                return value
            if isinstance(node.op, ast.USub):
                return -value
            if isinstance(node.op, (ast.Not, ast.Invert)):
                return ~self._as_frame(value).astype(bool)
        if isinstance(node, ast.BinOp):
            left, right = self._evaluate(node.left), self._evaluate(node.right)
            with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
                if isinstance(node.op, ast.Add):
                    return left + right
                if isinstance(node.op, ast.Sub):
                    return left - right
                if isinstance(node.op, ast.Mult):
                    return left * right
                if isinstance(node.op, ast.Div):
                    return left / right
                if isinstance(node.op, ast.Pow):
                    return left**right
                if isinstance(node.op, ast.Mod):
                    return left % right
        if isinstance(node, ast.BoolOp):
            values = [
                self._as_frame(self._evaluate(item)).astype(bool)
                for item in node.values
            ]
            result = values[0]
            for item in values[1:]:
                result = (
                    result & item if isinstance(node.op, ast.And) else result | item
                )
            return result
        if isinstance(node, ast.Compare):
            left = self._evaluate(node.left)
            result: pd.DataFrame | None = None
            for operator, comparator in zip(node.ops, node.comparators, strict=True):
                right = self._evaluate(comparator)
                if isinstance(operator, ast.Eq):
                    current = left == right
                elif isinstance(operator, ast.NotEq):
                    current = left != right
                elif isinstance(operator, ast.Lt):
                    current = left < right
                elif isinstance(operator, ast.LtE):
                    current = left <= right
                elif isinstance(operator, ast.Gt):
                    current = left > right
                else:
                    current = left >= right
                current = self._as_frame(current).astype(bool)
                result = current if result is None else result & current
                left = right
            return result
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            name = node.func.id.upper()
            if name == "__FIELD__":
                return self._field(str(node.args[0].value))
            args = [self._evaluate(argument) for argument in node.args]
            return self._call(name, args)
        raise ValueError(f"unsupported factor expression node: {type(node).__name__}")

    def _field(self, name: str) -> pd.DataFrame:
        key = _canonical_field(name)
        try:
            return self.inputs[key]
        except KeyError as exc:
            raise KeyError(f"factor input field is unavailable: {key!r}") from exc

    def _call(self, name: str, args: list[Any]) -> Any:
        frame = lambda value: self._as_frame(value)
        x = frame(args[0]) if args else None
        if name == "ABS":
            return x.abs()
        if name == "LOG":
            return np.log(x.where(x > 0))
        if name == "LOGABS":
            return np.log(x.abs().where(x.abs() > 0))
        if name == "AS_FLOAT":
            return x.astype(float)
        if name == "RD":
            return x.round(int(args[1]) if len(args) > 1 else 2)
        if name == "SIGN":
            return np.sign(x)
        if name == "RANK":
            return x.rank(axis=1, pct=True, method="average", na_option="keep")
        if name == "SCALE":
            minimum, maximum = x.min(axis=1), x.max(axis=1)
            spread = (maximum - minimum).replace(0.0, np.nan)
            return (
                x.sub(minimum, axis=0)
                .div(spread, axis=0)
                .mul(2.0)
                .sub(1.0)
                .fillna(0.0)
                .where(x.notna())
            )
        if name == "ZSCORE":
            return x.sub(x.mean(axis=1), axis=0).div(
                x.std(axis=1, ddof=0).replace(0.0, np.nan), axis=0
            )
        if name == "CONST":
            return pd.DataFrame(
                np.broadcast_to(x.iloc[-1].to_numpy(), x.shape),
                index=x.index,
                columns=x.columns,
            )
        if name == "BARSLAST":
            return _barslast(x.astype(bool))
        if name == "BARSLASTCOUNT":
            return _barslastcount(x.astype(bool))
        if name == "POWER":
            return x ** args[1]
        if name == "SIGNEDPOWER":
            return np.sign(x) * x.abs() ** args[1]
        if name in {"REF", "DELAY"}:
            return x.shift(_window(args[1], minimum=0))
        if name in {"DIFF", "DELTA"}:
            return x.diff(_window(args[1] if len(args) > 1 else 1, minimum=1))
        if name in {"MA", "TS_MEAN", "ADV"}:
            n = _window(args[1])
            return x.rolling(n, min_periods=n).mean()
        if name == "SUM":
            n = _window(args[1])
            return x.rolling(n, min_periods=n).sum()
        if name == "PRODUCT":
            n = _window(args[1])
            return _rolling_apply(x, n, lambda values: np.prod(values, axis=-1))
        if name in {"ROC", "PCT_CHANGE", "RETURNS"}:
            n = _window(args[1])
            return x / x.shift(n) - 1.0
        if name == "FUTURE_RETURNS":
            n = _window(args[1])
            return x.shift(-n) / x - 1.0
        if name in {"STD", "STDDEV"}:
            n = _window(args[1])
            return x.rolling(n, min_periods=n).std(ddof=1)
        if name == "VAR":
            n = _window(args[1])
            return x.rolling(n, min_periods=n).var(ddof=1)
        if name == "TS_MAX":
            n = _window(args[1])
            return x.rolling(n, min_periods=n).max()
        if name == "TS_MIN":
            n = _window(args[1])
            return x.rolling(n, min_periods=n).min()
        if name == "TS_MIDDLE":
            n = _window(args[1])
            rolling = x.rolling(n, min_periods=n)
            return (rolling.max() + rolling.min()) / 2.0
        if name in {"TS_MAD", "AVEDEV"}:
            n = _window(args[1])
            return _rolling_apply(
                x,
                n,
                lambda values: np.mean(
                    np.abs(values - np.mean(values, axis=-1, keepdims=True)),
                    axis=-1,
                ),
            )
        if name == "TS_RANK":
            n = _window(args[1])
            return _rolling_apply(
                x,
                n,
                lambda values: (
                    (
                        (values < values[..., -1:]).sum(axis=-1)
                        + ((values == values[..., -1:]).sum(axis=-1) + 1) / 2.0
                    )
                    / n
                ),
            )
        if name in {"TS_ARGMAX", "TS_ARGMIN"}:
            n = _window(args[1])
            function = (
                (lambda values: np.argmax(values, axis=-1))
                if name == "TS_ARGMAX"
                else (lambda values: np.argmin(values, axis=-1))
            )
            return _rolling_apply(x, n, function)
        if name in {"HHVBARS", "LLVBARS"}:
            n = _window(args[1])
            function = (
                (lambda values: np.argmax(values, axis=-1))
                if name == "HHVBARS"
                else (lambda values: np.argmin(values, axis=-1))
            )
            return n - 1 - _rolling_apply(x, n, function)
        if name == "COUNT":
            n = _window(args[1])
            return x.astype(float).rolling(n, min_periods=n).sum()
        if name in {"EVERY", "EXIST"}:
            n = _window(args[1])
            rolling = x.astype(float).rolling(n, min_periods=n)
            return (rolling.min() > 0) if name == "EVERY" else (rolling.max() > 0)
        if name == "BARSSINCEN":
            n = _window(args[1])
            return _rolling_apply(
                x.astype(float),
                n,
                lambda values: np.where(
                    np.any(values.astype(bool), axis=-1),
                    n - 1 - np.argmax(values.astype(bool), axis=-1),
                    np.nan,
                ),
            )
        if name in {"SLOPE", "ANGLE", "INTERCEPT", "FORCAST"}:
            n = _window(args[1])
            slope = _slope(x, n)
            if name == "SLOPE":
                return slope
            if name == "ANGLE":
                return np.degrees(np.arctan(slope))
            intercept = x.rolling(n, min_periods=n).mean() - slope * ((n - 1) / 2.0)
            return intercept if name == "INTERCEPT" else intercept + slope * n
        if name == "DECAYLINEAR":
            n = _window(args[1])
            weights = np.arange(1, n + 1, dtype=float)
            weights /= weights.sum()
            return _rolling_apply(
                x,
                n,
                lambda values: np.tensordot(values, weights, axes=([-1], [0])),
            )
        if name == "TS_ZSCORE":
            n = _window(args[1])
            mean = x.rolling(n, min_periods=n).mean()
            return (x - mean) / x.rolling(n, min_periods=n).std(ddof=0).replace(
                0.0, np.nan
            )
        if name in {"TS_SKEW", "TS_KURT", "TS_MEDIAN"}:
            n = _window(args[1])
            rolling = x.rolling(n, min_periods=n)
            return (
                rolling.skew()
                if name == "TS_SKEW"
                else rolling.kurt()
                if name == "TS_KURT"
                else rolling.median()
            )
        if name == "EMA":
            n = _window(args[1])
            return x.ewm(span=n, adjust=False, min_periods=n).mean()
        if name == "DMA":
            return _dynamic_average(x, args[1])
        if name == "WMA":
            n = _window(args[1])
            weights = np.arange(1, n + 1, dtype=float)
            weights /= weights.sum()
            return _rolling_apply(
                x,
                n,
                lambda values: np.tensordot(values, weights, axes=([-1], [0])),
            )
        if name == "SHARPE":
            n = _window(args[1])
            returns = x.pct_change(fill_method=None)
            return returns.rolling(n, min_periods=n).mean() / returns.rolling(
                n, min_periods=n
            ).std(ddof=1).replace(0.0, np.nan)
        if name in {"SUM_ABS_PRICE_CHANGE", "MEAN_ABS_PRICE_CHANGE"}:
            n = _window(args[1])
            rolling = x.diff().abs().rolling(n, min_periods=n)
            return rolling.sum() if name == "SUM_ABS_PRICE_CHANGE" else rolling.mean()
        if name in {"MAX", "MIN", "MEAN", "EQUAL", "VALUEWHEN"}:
            y = frame(args[1])
            if name == "MAX":
                return x.combine(y, np.maximum)
            if name == "MIN":
                return x.combine(y, np.minimum)
            if name == "MEAN":
                return (x + y) / 2.0
            if name == "EQUAL":
                return x == y
            return y.where(x.astype(bool))
        if name == "CROSS":
            y = frame(args[1])
            return (x > y) & (x.shift(1) <= y.shift(1))
        if name in {
            "CORR",
            "CORRELATION",
            "COV",
            "COVARIANCE",
            "TS_REGRESSION",
            "SUMIF",
            "LONGCROSS",
        }:
            y = frame(args[1])
            n = _window(args[2])
            if name in {"CORR", "CORRELATION"}:
                return x.rolling(n, min_periods=n).corr(y)
            if name in {"COV", "COVARIANCE"}:
                return x.rolling(n, min_periods=n).cov(y)
            if name == "TS_REGRESSION":
                return x.rolling(n, min_periods=n).cov(y) / y.rolling(
                    n, min_periods=n
                ).var().replace(0.0, np.nan)
            if name == "SUMIF":
                return y.where(x.astype(bool), 0.0).rolling(n, min_periods=n).sum()
            return (
                (x > y)
                & (x.shift(1) <= y.shift(1))
                & (x.shift(1) < y.shift(1)).rolling(n, min_periods=n).min().astype(bool)
            )
        if name == "LAST":
            n, m = _window(args[1], minimum=0), _window(args[2], minimum=0)
            start, stop = min(n, m), max(n, m)
            return (
                x.shift(start)
                .astype(float)
                .rolling(stop - start + 1, min_periods=stop - start + 1)
                .min()
                > 0
            )
        if name == "SMA":
            return _chinese_sma(x, _window(args[1]), float(args[2]))
        if name == "IF":
            return frame(args[1]).where(x.astype(bool), frame(args[2]))
        if name == "MACD_DIF":
            short, long = _window(args[1]), _window(args[2])
            return (
                x.ewm(span=short, adjust=False, min_periods=short).mean()
                - x.ewm(span=long, adjust=False, min_periods=long).mean()
            )
        if name in {"MACD_DEA", "MACD"}:
            short, long, signal = _window(args[1]), _window(args[2]), _window(args[3])
            dif = (
                x.ewm(span=short, adjust=False, min_periods=short).mean()
                - x.ewm(span=long, adjust=False, min_periods=long).mean()
            )
            dea = dif.ewm(span=signal, adjust=False, min_periods=signal).mean()
            return dea if name == "MACD_DEA" else (dif - dea) * 2.0
        if name in {"KDJ_K", "KDJ_D", "KDJ_J"}:
            high, low = frame(args[1]), frame(args[2])
            n, m1, m2 = _window(args[3]), _window(args[4]), _window(args[5])
            lowest = low.rolling(n, min_periods=n).min()
            rsv = (
                (x - lowest)
                / (high.rolling(n, min_periods=n).max() - lowest).replace(0.0, np.nan)
                * 100
            )
            k = rsv.ewm(alpha=1 / m1, adjust=False, min_periods=m1).mean()
            d = k.ewm(alpha=1 / m2, adjust=False, min_periods=m2).mean()
            return k if name == "KDJ_K" else d if name == "KDJ_D" else 3 * k - 2 * d
        if name == "RSI":
            n = _window(args[1])
            change = x.diff()
            gain = change.clip(lower=0).rolling(n, min_periods=n).mean()
            absolute = change.abs().rolling(n, min_periods=n).mean()
            return gain / absolute.replace(0.0, np.nan) * 100
        if name == "WR":
            n = _window(args[1])
            high, low = (
                x.rolling(n, min_periods=n).max(),
                x.rolling(n, min_periods=n).min(),
            )
            return (high - x) / (high - low).replace(0.0, np.nan) * 100
        if name in {"BOLL_UPPER", "BOLL_MID", "BOLL_LOWER"}:
            n, width = _window(args[1]), float(args[2])
            middle = x.rolling(n, min_periods=n).mean()
            if name == "BOLL_MID":
                return middle
            offset = width * x.rolling(n, min_periods=n).std(ddof=1)
            return middle + offset if name == "BOLL_UPPER" else middle - offset
        if name == "BOLL_WIDTH":
            n = _window(args[1])
            return 4.0 * x.rolling(n, min_periods=n).std(ddof=1)
        if name == "BIAS":
            n = _window(args[1])
            average = x.rolling(n, min_periods=n).mean()
            return (x - average) / average.replace(0.0, np.nan) * 100
        if name == "PSY":
            n = _window(args[1])
            return (x.diff() > 0).astype(float).rolling(n, min_periods=n).mean() * 100
        if name == "PSYMA":
            n, m = _window(args[1]), _window(args[2])
            psy = (x.diff() > 0).astype(float).rolling(n, min_periods=n).mean() * 100
            return psy.rolling(m, min_periods=m).mean()
        if name == "CCI":
            n = _window(args[1])
            average = x.rolling(n, min_periods=n).mean()
            deviation = _rolling_apply(
                x,
                n,
                lambda values: np.mean(
                    np.abs(values - np.mean(values, axis=-1, keepdims=True)),
                    axis=-1,
                ),
            )
            return (x - average) / (0.015 * deviation).replace(0.0, np.nan)
        if name == "ATR":
            n = _window(args[1])
            return x.diff().abs().rolling(n, min_periods=n).mean()
        if name == "BBI":
            windows = [_window(value) for value in args[1:5]]
            return (
                sum(
                    (x.rolling(n, min_periods=n).mean() for n in windows), start=x * 0.0
                )
                / 4.0
            )
        if name.startswith("DMI_"):
            high, low = frame(args[1]), frame(args[2])
            n, smoothing = _window(args[3]), _window(args[4])
            up, down = high.diff(), -low.diff()
            plus_dm = up.where((up > down) & (up > 0), 0.0)
            minus_dm = down.where((down > up) & (down > 0), 0.0)
            true_range = pd.DataFrame(
                np.maximum.reduce(
                    [
                        (high - low).to_numpy(dtype=float),
                        (high - x.shift(1)).abs().to_numpy(dtype=float),
                        (low - x.shift(1)).abs().to_numpy(dtype=float),
                    ]
                ),
                index=x.index,
                columns=x.columns,
            )
            atr = true_range.rolling(n, min_periods=n).sum()
            pdi = (
                plus_dm.rolling(n, min_periods=n).sum() / atr.replace(0.0, np.nan) * 100
            )
            mdi = (
                minus_dm.rolling(n, min_periods=n).sum()
                / atr.replace(0.0, np.nan)
                * 100
            )
            dx = (pdi - mdi).abs() / (pdi + mdi).replace(0.0, np.nan) * 100
            adx = dx.rolling(smoothing, min_periods=smoothing).mean()
            if name == "DMI_PDI":
                return pdi
            if name == "DMI_MDI":
                return mdi
            if name == "DMI_ADX":
                return adx
            return (adx + adx.shift(smoothing)) / 2.0
        if name == "DEMA":
            n = _window(args[1])
            ema = x.ewm(span=n, adjust=False, min_periods=n).mean()
            return 2 * ema - ema.ewm(span=n, adjust=False, min_periods=n).mean()
        if name == "TEMA":
            n = _window(args[1])
            first = x.ewm(span=n, adjust=False, min_periods=n).mean()
            second = first.ewm(span=n, adjust=False, min_periods=n).mean()
            third = second.ewm(span=n, adjust=False, min_periods=n).mean()
            return 3 * first - 3 * second + third
        if name == "KAMA":
            return _kama(x, _window(args[1]))
        if name == "T3":
            n = _window(args[1])
            e1 = x.ewm(span=n, adjust=False, min_periods=n).mean()
            e2 = e1.ewm(span=n, adjust=False, min_periods=n).mean()
            e3 = e2.ewm(span=n, adjust=False, min_periods=n).mean()
            e4 = e3.ewm(span=n, adjust=False, min_periods=n).mean()
            e5 = e4.ewm(span=n, adjust=False, min_periods=n).mean()
            e6 = e5.ewm(span=n, adjust=False, min_periods=n).mean()
            v = 0.7
            return (
                -(v**3) * e6
                + (3 * v**2 + 3 * v**3) * e5
                + (-6 * v**2 - 3 * v - 3 * v**3) * e4
                + (1 + 3 * v + v**3 + 3 * v**2) * e3
            )
        if name == "PPO":
            y = frame(args[1])
            return (x - y) / y.replace(0.0, np.nan) * 100
        if name == "AROONOSC":
            n = _window(args[1])
            high_age = (
                n - 1 - _rolling_apply(x, n, lambda values: np.argmax(values, axis=-1))
            )
            low_age = (
                n - 1 - _rolling_apply(x, n, lambda values: np.argmin(values, axis=-1))
            )
            return (low_age - high_age) / n * 100
        if name == "ADXR":
            n = _window(args[1])
            return (x + x.shift(n)) / 2.0
        if name == "CMO":
            n = _window(args[1])
            change = x.diff()
            gains = change.clip(lower=0).rolling(n, min_periods=n).sum()
            losses = (-change.clip(upper=0)).rolling(n, min_periods=n).sum()
            return (gains - losses) / (gains + losses).replace(0.0, np.nan) * 100
        if name == "STOCHASTIC":
            n = _window(args[1])
            low, high = (
                x.rolling(n, min_periods=n).min(),
                x.rolling(n, min_periods=n).max(),
            )
            return (x - low) / (high - low).replace(0.0, np.nan) * 100
        if name == "OBV":
            volume = frame(args[1])
            return (np.sign(x.diff()).fillna(0.0) * volume).cumsum()
        if name == "VR":
            volume, n = frame(args[1]), _window(args[2])
            change = x.diff()
            up = volume.where(change > 0, 0.0).rolling(n, min_periods=n).sum()
            down = volume.where(change < 0, 0.0).rolling(n, min_periods=n).sum()
            flat = volume.where(change == 0, 0.0).rolling(n, min_periods=n).sum()
            return (up + flat / 2) / (down + flat / 2).replace(0.0, np.nan) * 100
        if name == "MFI":
            high, low, volume, n = (
                frame(args[1]),
                frame(args[2]),
                frame(args[3]),
                _window(args[4]),
            )
            typical = (x + high + low) / 3.0
            flow = typical * volume
            positive = (
                flow.where(typical.diff() > 0, 0.0).rolling(n, min_periods=n).sum()
            )
            negative = (
                flow.where(typical.diff() < 0, 0.0).rolling(n, min_periods=n).sum()
            )
            ratio = positive / negative.replace(0.0, np.nan)
            return 100 - 100 / (1 + ratio)
        if name in {"EMV", "EMVMA"}:
            low, volume = frame(args[1]), frame(args[2])
            n, m = _window(args[3]), _window(args[4])
            midpoint = (x + low) / 2.0
            box_ratio = volume / (x - low).replace(0.0, np.nan)
            emv = (midpoint - midpoint.shift(1)) / box_ratio.replace(0.0, np.nan)
            emv = emv.rolling(n, min_periods=n).mean()
            return emv if name == "EMV" else emv.rolling(m, min_periods=m).mean()
        if name in {"TRIX", "TRIMA"}:
            n, m = _window(args[1]), _window(args[2])
            first = x.ewm(span=n, adjust=False, min_periods=n).mean()
            second = first.ewm(span=n, adjust=False, min_periods=n).mean()
            third = second.ewm(span=n, adjust=False, min_periods=n).mean()
            trix = third.pct_change(fill_method=None) * 100
            return trix if name == "TRIX" else trix.rolling(m, min_periods=m).mean()
        if name in {"DPO", "DPOMA"}:
            n, offset, smooth = _window(args[1]), _window(args[2]), _window(args[3])
            dpo = x.shift(offset) - x.rolling(n, min_periods=n).mean()
            return (
                dpo if name == "DPO" else dpo.rolling(smooth, min_periods=smooth).mean()
            )
        if name in {"BRAR", "ARBR"}:
            close, high, low, n = (
                frame(args[1]),
                frame(args[2]),
                frame(args[3]),
                _window(args[4]),
            )
            if name == "BRAR":
                numerator = (
                    (high - close.shift(1))
                    .clip(lower=0)
                    .rolling(n, min_periods=n)
                    .sum()
                )
                denominator = (
                    (close.shift(1) - low).clip(lower=0).rolling(n, min_periods=n).sum()
                )
            else:
                numerator = (high - x).rolling(n, min_periods=n).sum()
                denominator = (x - low).rolling(n, min_periods=n).sum()
            return numerator / denominator.replace(0.0, np.nan) * 100
        if name in {"MTM", "MTMMA"}:
            n, m = _window(args[1]), _window(args[2])
            momentum = x - x.shift(n)
            return (
                momentum if name == "MTM" else momentum.rolling(m, min_periods=m).mean()
            )
        if name in {"MASS", "MASSMA"}:
            low, n1, n2, m = (
                frame(args[1]),
                _window(args[2]),
                _window(args[3]),
                _window(args[4]),
            )
            spread = x - low
            first = spread.ewm(span=n1, adjust=False, min_periods=n1).mean()
            ratio = first / first.ewm(
                span=n1, adjust=False, min_periods=n1
            ).mean().replace(0.0, np.nan)
            mass = ratio.rolling(n2, min_periods=n2).sum()
            return mass if name == "MASS" else mass.rolling(m, min_periods=m).mean()
        if name == "ROCMA":
            n, m = _window(args[1]), _window(args[2])
            return (x / x.shift(n) - 1.0).rolling(m, min_periods=m).mean()
        if name in {"EXPMA", "EXPMA2"}:
            n1, n2 = _window(args[1]), _window(args[2])
            n = n1 if name == "EXPMA" else n2
            return x.ewm(span=n, adjust=False, min_periods=n).mean()
        if name in {"ASI", "ASIT"}:
            close, high, low, n, m = (
                frame(args[1]),
                frame(args[2]),
                frame(args[3]),
                _window(args[4]),
                _window(args[5]),
            )
            previous = close.shift(1)
            a, b, c = (
                (high - previous).abs(),
                (low - previous).abs(),
                (high - low).abs(),
            )
            r = pd.DataFrame(
                np.maximum.reduce(
                    [
                        a.to_numpy(dtype=float),
                        b.to_numpy(dtype=float),
                        c.to_numpy(dtype=float),
                    ]
                ),
                index=x.index,
                columns=x.columns,
            )
            si = (
                close - previous + 0.5 * (close - x) + 0.25 * (previous - x.shift(1))
            ) / r.replace(0.0, np.nan)
            asi = si.rolling(n, min_periods=n).sum()
            return asi if name == "ASI" else asi.rolling(m, min_periods=m).mean()
        if name in {"DIF", "DFMA"}:
            n1, n2, m = _window(args[1]), _window(args[2]), _window(args[3])
            difference = (
                x.rolling(n1, min_periods=n1).mean()
                - x.rolling(n2, min_periods=n2).mean()
            )
            return (
                difference
                if name == "DIF"
                else difference.rolling(m, min_periods=m).mean()
            )
        if name == "BOLLINGERDIFF":
            return 2.0 * (x - frame(args[1]))
        raise ValueError(f"factor operator is not implemented: {name}")


def evaluate_expressions(
    expressions: str | Sequence[str],
    inputs: Mapping[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """Evaluate one or more expressions while sharing subexpression results."""

    return FactorEngine(inputs).evaluate(expressions)


def combine_factor_frames(
    assets: Sequence[Any],
    expressions: Sequence[str],
    sessions: pd.DatetimeIndex,
    values: Mapping[str, pd.DataFrame],
) -> pd.DataFrame:
    """Build the standard ``(asset, factor)`` history column layout."""

    codes = [asset.ts_code for asset in assets]
    names = list(expressions)
    columns = pd.MultiIndex.from_product([codes, names], names=["asset", "field"])
    matrices = [
        values[name].reindex(index=sessions, columns=codes).to_numpy(dtype=float)
        for name in names
    ]
    if not matrices:
        return pd.DataFrame(index=sessions, columns=columns, dtype=float)
    array = np.stack(matrices, axis=2).reshape(len(sessions), len(codes) * len(names))
    result = pd.DataFrame(array, index=sessions, columns=columns)
    result.index.name = "trade_date"
    return result


__all__ = [
    "FactorEngine",
    "FactorExpression",
    "available_operators",
    "combine_factor_frames",
    "compile_expression",
    "evaluate_expressions",
    "expression_fields",
    "expression_window",
    "is_factor_expression",
]
