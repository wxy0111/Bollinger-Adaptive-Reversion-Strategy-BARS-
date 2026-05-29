"""布林带计算、插针检测、周线趋势分析。"""
import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Optional

from src.config import (
    BOLL_PERIOD, BOLL_STD,
    PIN_WICK_RATIO, PIN_BODY_INSIDE,
    WEEKLY_EMA_PERIOD,
)


# ── DataFrame 构建 ─────────────────────────────────────────────────────────

def build_df(raw_klines: list) -> pd.DataFrame:
    """OKX 原始K线 → DataFrame，oldest-first，只含已确认K线。"""
    df = pd.DataFrame(raw_klines, columns=[
        "ts", "open", "high", "low", "close",
        "vol", "volCcy", "volCcyQuote", "confirm"
    ])
    df = df[df["confirm"] == "1"].iloc[::-1].reset_index(drop=True)
    for col in ("open", "high", "low", "close", "vol"):
        df[col] = df[col].astype(float)
    df["ts"] = pd.to_datetime(df["ts"].astype(int), unit="ms")
    return df


# ── 布林带 ────────────────────────────────────────────────────────────────

def add_boll(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"]
    df["boll_mid"]   = close.rolling(BOLL_PERIOD).mean()
    rolling_std      = close.rolling(BOLL_PERIOD).std(ddof=0)
    df["boll_upper"] = df["boll_mid"] + BOLL_STD * rolling_std
    df["boll_lower"] = df["boll_mid"] - BOLL_STD * rolling_std
    df["boll_width"] = df["boll_upper"] - df["boll_lower"]
    return df.dropna(subset=["boll_mid"]).reset_index(drop=True)


# ── 插针信号 ──────────────────────────────────────────────────────────────

@dataclass
class PinSignal:
    direction: str          # "long" | "short"
    entry_price: float      # 首批参考入场价（K线收盘）
    boll_mid: float
    boll_upper: float
    boll_lower: float
    boll_width: float
    atr_approx: float       # 近20根K线真实波幅均值，用于止损估算


def detect_pin(df: pd.DataFrame) -> Optional[PinSignal]:
    """
    检测最新已确认K线是否为插针信号。

    做多插针（下影线）：
      - 下影线穿过布林下轨
      - 实体（open/close）收回布林下轨以上
      - 下影线长度占K线总幅度 >= PIN_WICK_RATIO

    做空插针（上影线）：
      - 上影线穿过布林上轨
      - 实体（open/close）收回布林上轨以下
      - 上影线长度占K线总幅度 >= PIN_WICK_RATIO
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

    # 近20根ATR近似
    atr = float(df["high"].tail(20).values - df["low"].tail(20).values) if False else \
          float((df["high"] - df["low"]).tail(20).mean())

    # 做多插针：下影线穿下轨，实体收回带内
    if (l < lower                                       # 低点穿越下轨
            and lower_wick / total_range >= PIN_WICK_RATIO   # 下影线足够长
            and (not PIN_BODY_INSIDE or body_bottom >= lower)):  # 实体回到带内
        return PinSignal(
            direction="long",
            entry_price=c,
            boll_mid=mid, boll_upper=upper, boll_lower=lower, boll_width=width,
            atr_approx=atr,
        )

    # 做空插针：上影线穿上轨，实体收回带内
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


# ── 周线趋势分析 ───────────────────────────────────────────────────────────

def weekly_trend(raw_weekly: list) -> str:
    """
    返回 'bull' | 'bear' | 'sideways'
    逻辑：
      - 取近 WEEKLY_EMA_PERIOD 根周线
      - 计算 EMA，若最新收盘 > EMA → bull，< EMA → bear，否则 sideways
      - 另外判断最近2根周线是否同向，增强过滤
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

    # 最近2周涨跌方向
    recent_dir = closes.iloc[-1] - closes.iloc[-3]   # 近2周净变化

    slope = ema.iloc[-1] - ema.iloc[-4]              # EMA 斜率（近3周）

    if last_close > last_ema and slope > 0:
        return "bull"
    if last_close < last_ema and slope < 0:
        return "bear"
    return "sideways"
