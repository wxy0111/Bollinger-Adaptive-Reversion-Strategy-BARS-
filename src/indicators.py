"""Indicator helpers used by backtests and strategy experiments.

Only ``build_df`` and ``add_boll`` are used by the current live strategy.
``detect_pin`` and ``weekly_trend`` are kept for experiments and are not wired
into the live entry path.
"""
import pandas as pd
from dataclasses import dataclass
from typing import Optional

from src.config import (
    BOLL_PERIOD, BOLL_STD,
    PIN_WICK_RATIO, PIN_BODY_INSIDE,
    WEEKLY_EMA_PERIOD,
)


def build_df(raw_klines: list, include_unconfirmed: bool = False) -> pd.DataFrame:
    """Convert OKX raw candles to an oldest-first DataFrame.

    Args:
        raw_klines: Raw OKX candle rows.
        include_unconfirmed: Whether to keep the current unconfirmed candle.

    Returns:
        A typed DataFrame sorted oldest-first.
    """
    df = pd.DataFrame(raw_klines, columns=[
        "ts", "open", "high", "low", "close",
        "vol", "volCcy", "volCcyQuote", "confirm"
    ])
    if not include_unconfirmed:
        df = df[df["confirm"] == "1"]
    df = df.iloc[::-1].reset_index(drop=True)
    for col in ("open", "high", "low", "close", "vol"):
        df[col] = df[col].astype(float)
    df["ts"] = pd.to_datetime(df["ts"].astype(int), unit="ms")
    return df


def add_boll(df: pd.DataFrame) -> pd.DataFrame:
    """Add Bollinger-band columns to a candle DataFrame.

    Args:
        df: Candle DataFrame with a ``close`` column.

    Returns:
        DataFrame with Bollinger columns and rows without a valid middle band
        removed.
    """
    close = df["close"]
    df["boll_mid"]   = close.rolling(BOLL_PERIOD).mean()
    rolling_std      = close.rolling(BOLL_PERIOD).std(ddof=0)
    df["boll_upper"] = df["boll_mid"] + BOLL_STD * rolling_std
    df["boll_lower"] = df["boll_mid"] - BOLL_STD * rolling_std
    df["boll_width"] = df["boll_upper"] - df["boll_lower"]
    return df.dropna(subset=["boll_mid"]).reset_index(drop=True)


@dataclass
class PinSignal:
    """Pin-bar signal description.

    Attributes:
        direction: Signal side, either ``"long"`` or ``"short"``.
        entry_price: Reference entry price, usually candle close.
        boll_mid: Bollinger middle band.
        boll_upper: Bollinger upper band.
        boll_lower: Bollinger lower band.
        boll_width: Bollinger width.
        atr_approx: Approximate ATR from recent high-low ranges.
    """

    direction: str
    entry_price: float
    boll_mid: float
    boll_upper: float
    boll_lower: float
    boll_width: float
    atr_approx: float


def detect_pin(df: pd.DataFrame) -> Optional[PinSignal]:
    """Detect whether the latest confirmed candle is a pin-bar signal.

    Args:
        df: Candle DataFrame with Bollinger columns.

    Returns:
        ``PinSignal`` when the latest candle matches a long or short pin-bar
        pattern; otherwise ``None``.
    """
    if len(df) < BOLL_PERIOD + 5:
        return None

    k = df.iloc[-1]
    o, h, l, c = k["open"], k["high"], k["low"], k["close"]
    upper, lower, mid, width = k["boll_upper"], k["boll_lower"], k["boll_mid"], k["boll_width"]

    total_range = h - l
    if total_range < 1e-8:
        return None

    body_top    = max(o, c)
    body_bottom = min(o, c)
    upper_wick  = h - body_top
    lower_wick  = body_bottom - l

    # Approximate ATR from recent candle ranges.
    atr = float(df["high"].tail(20).values - df["low"].tail(20).values) if False else \
          float((df["high"] - df["low"]).tail(20).mean())

    if (l < lower
            and lower_wick / total_range >= PIN_WICK_RATIO
            and (not PIN_BODY_INSIDE or body_bottom >= lower)):
        return PinSignal(
            direction="long",
            entry_price=c,
            boll_mid=mid, boll_upper=upper, boll_lower=lower, boll_width=width,
            atr_approx=atr,
        )

    if (h > upper
            and upper_wick / total_range >= PIN_WICK_RATIO
            and (not PIN_BODY_INSIDE or body_top <= upper)):
        return PinSignal(
            direction="short",
            entry_price=c,
            boll_mid=mid, boll_upper=upper, boll_lower=lower, boll_width=width,
            atr_approx=atr,
        )

    return None


def weekly_trend(raw_weekly: list) -> str:
    """Estimate weekly trend from raw weekly candles.

    Args:
        raw_weekly: Raw OKX weekly candle rows.

    Returns:
        ``"bull"``, ``"bear"``, or ``"sideways"``.
    """
    if not raw_weekly or len(raw_weekly) < WEEKLY_EMA_PERIOD + 2:
        return "sideways"

    df = build_df(raw_weekly)
    if len(df) < WEEKLY_EMA_PERIOD:
        return "sideways"

    closes = df["close"]
    ema = closes.ewm(span=WEEKLY_EMA_PERIOD, adjust=False).mean()
    last_close = closes.iloc[-1]
    last_ema   = ema.iloc[-1]

    slope = ema.iloc[-1] - ema.iloc[-4]

    if last_close > last_ema and slope > 0:
        return "bull"
    if last_close < last_ema and slope < 0:
        return "bear"
    return "sideways"
