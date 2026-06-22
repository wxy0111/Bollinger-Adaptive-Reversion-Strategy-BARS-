"""Optimize entry parameters from live strategy logs.

The script is intentionally offline-only. It reads ``logs/boll_pin_*.log``,
replays the logged mark-price and Bollinger snapshots, tests a parameter grid,
and writes CSV plus Markdown reports. It never edits live configuration.
"""

from __future__ import annotations

import argparse
import csv
import itertools
import math
import os
import random
import re
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field, fields
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config import (
    CONTRACT_STEP,
    COPY_FIXED_LOSS_STOP_ENABLED,
    COPY_FIXED_LOSS_STOP_RATIO,
    COPY_FIXED_LOSS_STOP_USDT,
    CROSS_COPY_DYNAMIC_SIZING_ENABLED,
    CROSS_COPY_PROTECT_ENABLED,
    CROSS_COPY_PROTECT_EQUITY_USDT,
    CT_VAL,
    ADDON_EXTREME_GUARD_ENABLED,
    BOLL_WIDTH_BASE_PRICE,
    BOLL_WIDTH_BASE_USD,
    BOLL_WIDTH_GAP_MULT,
    BOLL_WIDTH_TP_SPACE_ENABLED,
    BOLL_WIDTH_TP_SPACE_MULT,
    DYNAMIC_BASE_ENTRY_RATIO,
    DYNAMIC_ENTRY_GAP_ENABLED,
    DYNAMIC_ENTRY_GAP_MAX_USD,
    DYNAMIC_MAX_ENTRY_RATIO,
    DYNAMIC_MIN_ENTRY_RATIO,
    DYNAMIC_TP_ARM_RETURN,
    BOLL_TP_COMPRESSION_ENABLED,
    BOLL_TP_COMPRESSION_EXIT_OFFSET_USD,
    BOLL_TP_COMPRESSION_MIN_RETURN,
    TREND_RISK_GUARD_ENABLED,
    TREND_RISK_GUARD_CLOSE_ENABLED,
    TREND_RISK_FREEZE_ADDON_ENABLED,
    TREND_RISK_SCORE_THRESHOLD,
    TREND_RISK_HEAD_ADVERSE_PCT,
    TREND_RISK_KLINE_COUNT,
    TREND_RISK_MIN_HOLD_MIN,
    TREND_RISK_SLOPE_WINDOW_MIN,
    TREND_RISK_MID_SLOPE_PCT_PER_HOUR,
    TREND_RISK_EDGE_SLOPE_PCT_PER_HOUR,
    TREND_RISK_WIDTH_EXPAND,
    DISASTER_HEAD_DROP_PCT,
    DISASTER_LOSS_RATIO,
    DISASTER_STOP_ENABLED,
    BOLL_MID_COST_STOP_ENABLED,
    BOLL_MID_COST_TP_RETURN,
    DYNAMIC_TP_ENABLED,
    DYNAMIC_TP_REPRICE_GAP_USD,
    ADDON_DYNAMIC_GAP_ENABLED,
    ADDON_DYNAMIC_GAP_MAX_USD,
    ADDON_DYNAMIC_GAP_BOLL_START,
    ADDON_DYNAMIC_GAP_BOLL_STRONG,
    ADDON_DYNAMIC_GAP_BOLL_MAX_MULT,
    ADDON_DYNAMIC_GAP_HEAD_START_PCT,
    ADDON_DYNAMIC_GAP_HEAD_STRONG_PCT,
    ADDON_DYNAMIC_GAP_HEAD_MAX_MULT,
    ADDON_DYNAMIC_GAP_TREND_KLINES,
    ADDON_DYNAMIC_GAP_TREND_MULT,
    ADDON_MAX_BOLL_WIDTH_FILTER_ENABLED,
    ADDON_MAX_BOLL_WIDTH_PCT,
    ADDON_MAX_BOLL_WIDTH_USD,
    ADDON_TP_IMPROVE_GUARD_ENABLED,
    ADDON_TP_IMPROVE_EXPECTED_RETURN,
    ADDON_TP_IMPROVE_RATIO,
    ADDON_TP_IMPROVE_MIN_USD,
    ENTRY_EXTREME_GAP_ADJUST_ENABLED,
    ENTRY_EXTREME_GAP_BASE_PCT,
    ENTRY_EXTREME_GAP_FULL_PCT,
    ENTRY_EXTREME_GAP_MAX_MULT,
    FIRST_BATCH_RATIO,
    LEVER,
    LIQ_STOP_OFFSET_USD,
    MAX_ENTRY_BATCHES,
    MAX_TOTAL_ENTRY_RATIO,
    MIN_BOLL_WIDTH_USD,
    MIN_BOLL_WIDTH_PCT,
    ENTRY_MAX_BOLL_WIDTH_FILTER_ENABLED,
    ENTRY_MAX_BOLL_WIDTH_PCT,
    ENTRY_MAX_BOLL_WIDTH_USD,
    ENTRY_DISASTER_FILTER_ENABLED,
    ENTRY_DISASTER_SCORE_THRESHOLD,
    ENTRY_DISASTER_KLINE_COUNT,
    ENTRY_DISASTER_WIDTH_EXPAND,
    ENTRY_DISASTER_TP_DISTANCE_MULT,
    ENTRY_DISASTER_EXPECTED_RETURN,
    ENTRY_DISASTER_FAR_MID_RATIO,
    MIN_ENTRY_GAP_USD,
    MIN_BOLL_WIDTH_FLOOR_USD,
    LOW_BOLL_WIDTH_TP_ENABLED,
    LOW_BOLL_WIDTH_REF_PCT,
    LOW_BOLL_WIDTH_MIN_TP_RETURN,
    LOW_BOLL_WIDTH_MAX_TP_RETURN,
    LOW_BOLL_WIDTH_TP_CAPTURE_RATIO,
    LOW_BOLL_WIDTH_SIZE_MULT,
    MIN_HEAD_LIQ_BUFFER_PCT,
    FIXED_LOSS_HEAD_BUFFER_ENABLED,
    FIXED_LOSS_HEAD_BUFFER_PCT,
    MIN_ORDER_CONTRACTS,
    NO_NEW_EXTREME_TICKS,
    OKX_LIQ_FEE_RATE,
    OKX_MAINTENANCE_MARGIN_RATE,
    PENDING_ORDER_BAND_GUARD_ENABLED,
    POLL_INTERVAL,
    REPRICE_GAP_USD,
    SECOND_BATCH_DYNAMIC_BASE_RATIO,
    SECOND_BATCH_DYNAMIC_MIN_RATIO,
    SECOND_BATCH_DYNAMIC_MAX_RATIO,
    SECOND_BATCH_DYNAMIC_FULL_GAP_USD,
    TP_TARGET_MARGIN_RETURN,
    TP_PROFIT_USD,
    TRADING_ACCOUNT_TARGET,
    ROLLING_COMPOUND_ENABLED,
)


CONFIG_PATH = ROOT / "src" / "config.py"
DEFAULT_LOG_DIR = ROOT / "logs"
DEFAULT_OUT_DIR = ROOT / "backtest" / "results" / "log_parameter_optimizer"
DEFAULT_CACHE_DIR = ROOT / "backtest" / "cache"
SOURCE_BOLL_STD = 2.0
TAKER_FEE = 0.0005
INITIAL_TOTAL_EQUITY = 1000.0
DEFAULT_DYNAMIC_TP_ARM_GRID = ",".join(f"{value / 1000:g}" for value in range(150, 241, 5))
_WORKER_TICKS: pd.DataFrame | None = None

# Only these fields are varied by the default optimizer. Width gates, disaster
# guards, sizing rails, and stop switches stay fixed to the live config because
# local replay already showed they are regime rules rather than tuning knobs.
CORE_OPTIMIZED_FIELDS = (
    "min_entry_gap_usd",
    "first_batch_ratio",
    "second_batch_dynamic_base_ratio",
    "dynamic_base_ratio",
    "max_total_entry_ratio",
    "tp_target_margin_return",
    "dynamic_tp_arm_return",
)

TICK_RE = re.compile(
    "^(?P<ts>\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d+).*?"
    "(?:price|\\u4ef7\\u683c)=(?P<price>\\d+(?:\\.\\d+)?)\\s+"
    "(?:Boll|\\u5e03\\u6797)\\[(?P<lower>\\d+(?:\\.\\d+)?)\\s+\\|\\s+"
    "(?P<mid>\\d+(?:\\.\\d+)?)\\s+\\|\\s+"
    "(?P<upper>\\d+(?:\\.\\d+)?)\\]"
)


@dataclass(frozen=True)
class Params:
    """One entry-parameter combination."""

    boll_std: float
    min_width_usd: float
    min_width_pct: float
    min_entry_gap_usd: float
    reprice_gap_usd: float
    entry_max_width_enabled: int = int(ENTRY_MAX_BOLL_WIDTH_FILTER_ENABLED)
    entry_max_width_pct: float = ENTRY_MAX_BOLL_WIDTH_PCT
    entry_max_width_usd: float = ENTRY_MAX_BOLL_WIDTH_USD
    entry_disaster_filter_enabled: int = int(ENTRY_DISASTER_FILTER_ENABLED)
    entry_disaster_score_threshold: int = ENTRY_DISASTER_SCORE_THRESHOLD
    entry_disaster_kline_count: int = ENTRY_DISASTER_KLINE_COUNT
    entry_disaster_width_expand: float = ENTRY_DISASTER_WIDTH_EXPAND
    entry_disaster_tp_distance_mult: float = ENTRY_DISASTER_TP_DISTANCE_MULT
    entry_disaster_expected_return: float = ENTRY_DISASTER_EXPECTED_RETURN
    entry_disaster_far_mid_ratio: float = ENTRY_DISASTER_FAR_MID_RATIO
    addon_max_width_enabled: int = int(ADDON_MAX_BOLL_WIDTH_FILTER_ENABLED)
    addon_max_width_pct: float = ADDON_MAX_BOLL_WIDTH_PCT
    addon_max_width_usd: float = ADDON_MAX_BOLL_WIDTH_USD
    addon_tp_improve_guard_enabled: int = int(ADDON_TP_IMPROVE_GUARD_ENABLED)
    addon_tp_improve_expected_return: float = ADDON_TP_IMPROVE_EXPECTED_RETURN
    addon_tp_improve_ratio: float = ADDON_TP_IMPROVE_RATIO
    addon_tp_improve_min_usd: float = ADDON_TP_IMPROVE_MIN_USD
    first_batch_ratio: float = FIRST_BATCH_RATIO
    second_batch_dynamic_base_ratio: float = SECOND_BATCH_DYNAMIC_BASE_RATIO
    second_batch_dynamic_min_ratio: float = SECOND_BATCH_DYNAMIC_MIN_RATIO
    second_batch_dynamic_max_ratio: float = SECOND_BATCH_DYNAMIC_MAX_RATIO
    second_batch_dynamic_full_gap_usd: float = SECOND_BATCH_DYNAMIC_FULL_GAP_USD
    dynamic_base_ratio: float = DYNAMIC_BASE_ENTRY_RATIO
    dynamic_min_ratio: float = DYNAMIC_MIN_ENTRY_RATIO
    dynamic_max_ratio: float = DYNAMIC_MAX_ENTRY_RATIO
    max_total_entry_ratio: float = MAX_TOTAL_ENTRY_RATIO
    boll_width_base_price: float = BOLL_WIDTH_BASE_PRICE
    boll_width_base_usd: float = BOLL_WIDTH_BASE_USD
    boll_width_floor_usd: float = MIN_BOLL_WIDTH_FLOOR_USD
    boll_width_gap_mult: float = BOLL_WIDTH_GAP_MULT
    boll_width_tp_space_enabled: int = int(BOLL_WIDTH_TP_SPACE_ENABLED)
    boll_width_tp_space_mult: float = BOLL_WIDTH_TP_SPACE_MULT
    tp_target_margin_return: float = TP_TARGET_MARGIN_RETURN
    low_boll_width_tp_enabled: int = int(LOW_BOLL_WIDTH_TP_ENABLED)
    low_boll_width_ref_pct: float = LOW_BOLL_WIDTH_REF_PCT
    low_boll_width_min_tp_return: float = LOW_BOLL_WIDTH_MIN_TP_RETURN
    low_boll_width_max_tp_return: float = LOW_BOLL_WIDTH_MAX_TP_RETURN
    low_boll_width_tp_capture_ratio: float = LOW_BOLL_WIDTH_TP_CAPTURE_RATIO
    low_boll_width_size_mult: float = LOW_BOLL_WIDTH_SIZE_MULT
    min_head_liq_buffer_pct: float = MIN_HEAD_LIQ_BUFFER_PCT
    dynamic_tp_enabled: int = int(DYNAMIC_TP_ENABLED)
    dynamic_tp_arm_return: float = DYNAMIC_TP_ARM_RETURN
    dynamic_tp_reprice_gap_usd: float = DYNAMIC_TP_REPRICE_GAP_USD
    boll_tp_compression_enabled: int = int(BOLL_TP_COMPRESSION_ENABLED)
    boll_tp_compression_min_return: float = BOLL_TP_COMPRESSION_MIN_RETURN
    boll_tp_compression_exit_offset_usd: float = BOLL_TP_COMPRESSION_EXIT_OFFSET_USD
    entry_extreme_gap_adjust_enabled: int = int(ENTRY_EXTREME_GAP_ADJUST_ENABLED)
    entry_extreme_gap_base_pct: float = ENTRY_EXTREME_GAP_BASE_PCT
    entry_extreme_gap_full_pct: float = ENTRY_EXTREME_GAP_FULL_PCT
    entry_extreme_gap_max_mult: float = ENTRY_EXTREME_GAP_MAX_MULT
    copy_fixed_loss_stop_enabled: int = int(COPY_FIXED_LOSS_STOP_ENABLED)
    copy_fixed_loss_stop_usdt: float = COPY_FIXED_LOSS_STOP_USDT
    copy_fixed_loss_stop_ratio: float = COPY_FIXED_LOSS_STOP_RATIO
    fixed_loss_head_buffer_enabled: int = int(FIXED_LOSS_HEAD_BUFFER_ENABLED)
    fixed_loss_head_buffer_pct: float = FIXED_LOSS_HEAD_BUFFER_PCT
    disaster_stop_enabled: int = int(DISASTER_STOP_ENABLED)
    disaster_head_drop_pct: float = DISASTER_HEAD_DROP_PCT
    disaster_loss_ratio: float = DISASTER_LOSS_RATIO
    boll_mid_cost_stop_enabled: int = int(BOLL_MID_COST_STOP_ENABLED)
    boll_mid_cost_tp_return: float = BOLL_MID_COST_TP_RETURN
    trend_risk_guard_enabled: int = int(TREND_RISK_GUARD_ENABLED)
    trend_risk_guard_close_enabled: int = int(TREND_RISK_GUARD_CLOSE_ENABLED)
    trend_risk_freeze_addon_enabled: int = int(TREND_RISK_FREEZE_ADDON_ENABLED)
    trend_risk_score_threshold: int = TREND_RISK_SCORE_THRESHOLD
    trend_risk_head_adverse_pct: float = TREND_RISK_HEAD_ADVERSE_PCT
    trend_risk_kline_count: int = TREND_RISK_KLINE_COUNT
    trend_risk_min_hold_min: float = TREND_RISK_MIN_HOLD_MIN
    trend_risk_slope_window_min: float = TREND_RISK_SLOPE_WINDOW_MIN
    trend_risk_mid_slope_pct_per_hour: float = TREND_RISK_MID_SLOPE_PCT_PER_HOUR
    trend_risk_edge_slope_pct_per_hour: float = TREND_RISK_EDGE_SLOPE_PCT_PER_HOUR
    trend_risk_width_expand: float = TREND_RISK_WIDTH_EXPAND
    addon_dynamic_gap_enabled: int = int(ADDON_DYNAMIC_GAP_ENABLED)
    addon_dynamic_gap_max_usd: float = ADDON_DYNAMIC_GAP_MAX_USD
    addon_dynamic_gap_boll_start: float = ADDON_DYNAMIC_GAP_BOLL_START
    addon_dynamic_gap_boll_strong: float = ADDON_DYNAMIC_GAP_BOLL_STRONG
    addon_dynamic_gap_boll_max_mult: float = ADDON_DYNAMIC_GAP_BOLL_MAX_MULT
    addon_dynamic_gap_head_start_pct: float = ADDON_DYNAMIC_GAP_HEAD_START_PCT
    addon_dynamic_gap_head_strong_pct: float = ADDON_DYNAMIC_GAP_HEAD_STRONG_PCT
    addon_dynamic_gap_head_max_mult: float = ADDON_DYNAMIC_GAP_HEAD_MAX_MULT
    addon_dynamic_gap_trend_klines: int = ADDON_DYNAMIC_GAP_TREND_KLINES
    addon_dynamic_gap_trend_mult: float = ADDON_DYNAMIC_GAP_TREND_MULT


@dataclass
class Batch:
    """One simulated entry batch."""

    idx: int
    price: float
    sz: float


@dataclass
class Position:
    """Simulated local position and one pending entry order."""

    direction: str = "none"
    filled: list[Batch] = field(default_factory=list)
    pending: Batch | None = None
    avg_entry: float = 0.0
    total_sz: float = 0.0
    tp_price: float = 0.0

    def is_active(self) -> bool:
        """Return whether a position has filled size."""
        return self.direction != "none" and self.total_sz > 0

    def has_plan(self) -> bool:
        """Return whether a position or pending entry exists."""
        return self.direction != "none" or self.pending is not None

    def reset(self) -> None:
        """Clear the simulated position."""
        self.__init__()

    def next_idx(self) -> int:
        """Return the next batch index."""
        known = [batch.idx for batch in self.filled]
        if self.pending is not None:
            known.append(self.pending.idx)
        return max(known) + 1 if known else 0

    def last_filled(self) -> Batch | None:
        """Return the latest filled batch."""
        if not self.filled:
            return None
        return max(self.filled, key=lambda batch: batch.idx)

    def recalc(self) -> None:
        """Recalculate average entry and take-profit."""
        self.total_sz = sum(batch.sz for batch in self.filled)
        self.avg_entry = sum(batch.price * batch.sz for batch in self.filled) / self.total_sz
        if self.direction == "long":
            self.tp_price = round(self.avg_entry + TP_PROFIT_USD, 2)
        else:
            self.tp_price = round(self.avg_entry - TP_PROFIT_USD, 2)


@dataclass
class Trade:
    """One simulated closed trade."""

    entry_time: str
    exit_time: str
    direction: str
    avg_entry: float
    exit_price: float
    sz: float
    pnl: float
    batches: int


class LogReplay:
    """Replay logged ticks for one parameter combination."""

    def __init__(self, ticks: pd.DataFrame, params: Params, initial_total: float):
        self.ticks = ticks
        self.params = params
        self.initial_total = initial_total
        if ROLLING_COMPOUND_ENABLED or CROSS_COPY_PROTECT_ENABLED:
            self.trading_balance = initial_total
            self.funding_balance = 0.0
        else:
            self.trading_balance = TRADING_ACCOUNT_TARGET
            self.funding_balance = max(initial_total - TRADING_ACCOUNT_TARGET, 0.0)
        self.pos = Position()
        self.current_price = 0.0
        self._current_row = None
        self.kline_extremes = ticks.groupby("kline_ts").agg(low=("price", "min"), high=("price", "max"))
        self._trend_kline_extremes: dict[pd.Timestamp, dict[str, float]] = {}
        self.addon_extreme_guard_price = 0.0
        self.addon_extreme_guard_kline = None
        self.addon_extreme_guard_started = False
        self.entry_gap_cache: dict[float, float] = {}
        self.recent_prices: list[float] = []
        self.last_plan_price = 0.0
        self.last_batch_kline = None
        self.last_entry_check_kline = None
        self.last_close_kline = None
        self.capital_shortage_active = False
        self.entry_time = ""
        self.trades: list[Trade] = []
        self.events: list[dict] = []
        self.equity_points: list[dict] = []
        self.equity_curve: list[float] = []
        self.signal_count = 0
        self.skipped_entry_cap = 0
        self.skipped_funds = 0
        self.blocked_width = 0
        self.blocked_entry_max_width = 0
        self.blocked_entry_disaster = 0
        self.blocked_addon_max_width = 0
        self.blocked_gap = 0
        self.blocked_extreme = 0
        self.blocked_addon_guard = 0
        self.cross_copy_stop = 0
        self.fixed_loss_stop = 0
        self.fixed_cycle_loss_stop = 0
        self.liquidation_guard_fallback_stop = 0
        self.disaster_stop = 0
        self.boll_mid_cost_stop = 0
        self.trend_risk_guard_signal = 0
        self.trend_risk_guard_stop = 0
        self.trend_risk_guard_active = False
        self.trend_risk_freeze_blocks = 0
        self.trend_risk_freeze_cancels = 0
        self.stopped = False
        self.width_cancel = 0
        self.inside_cancel = 0
        self.reprice_count = 0
        self.same_k_block = 0
        self.close_kline_block = 0
        self.profit_transferred = 0.0
        self.loss_topup = 0.0
        self.dynamic_tp_active = False
        self.boll_mid_cost_tp_active = False
        self.dynamic_tp_activated = 0
        self.cycle_tp_target_margin_return = self.params.tp_target_margin_return
        self.boll_tp_compression_activated = 0
        self.entry_extreme_gap_pct = 0.0
        self.entry_extreme_gap_mult = 1.0
        self.entry_extreme_entries = 0
        self.entry_extreme_gap_total = 0.0
        self.entry_extreme_gap_max = 0.0
        self.entry_extreme_mult_total = 0.0
        self.entry_extreme_mult_max = 1.0
        self.min_liq_distance_pct = 999.0
        self.wipeout_risk = False
        self.boll_history: list[dict] = []
        self.entry_boll_width = 0.0
        self.entry_boll_width_pct = 0.0
        self.dynamic_gap_events = 0
        self.dynamic_gap_mult_total = 0.0
        self.dynamic_gap_mult_max = 1.0
        self.fixed_loss_head_buffer_block = 0
        self.tp_improve_block = 0

    def run(self) -> "LogReplay":
        """Run the replay and return self."""
        for row in self.ticks.itertuples(index=False):
            self._current_row = row
            self._remember_price(float(row.price))
            self._remember_boll(row)
            self._process_tick(row)
            self._track_liq_distance(float(row.price))
            equity = self.total_equity() + self._unrealized(float(row.price))
            self.equity_curve.append(equity)
            self.equity_points.append({"ts": str(row.ts), "equity": round(float(equity), 4)})
        return self

    def total_equity(self) -> float:
        """Return simulated total account equity."""
        return self.trading_balance + self.funding_balance

    def _account_equity(self, price: float | None = None) -> float:
        """Return total account equity including open unrealized PnL."""
        mark = self.current_price if price is None else price
        return self.total_equity() + self._unrealized(mark)

    def _previous_kline_ts(self, kline_ts):
        """Return previous completed 15-minute candle timestamp."""
        if pd.isna(kline_ts):
            return None
        return pd.Timestamp(kline_ts) - pd.Timedelta(minutes=15)

    def _start_addon_guard(self, row) -> None:
        """Start or continue completed-candle extreme tracking after a fill."""
        if not ADDON_EXTREME_GUARD_ENABLED or self.pos.direction not in ("long", "short"):
            return
        self.addon_extreme_guard_started = True
        prev = self._previous_kline_ts(row.kline_ts)
        if prev not in self.kline_extremes.index:
            return
        if self.pos.direction == "long":
            current = float(self.kline_extremes.loc[prev, "low"])
            self.addon_extreme_guard_price = (
                current if self.addon_extreme_guard_price <= 0 else min(self.addon_extreme_guard_price, current)
            )
        else:
            current = float(self.kline_extremes.loc[prev, "high"])
            self.addon_extreme_guard_price = (
                current if self.addon_extreme_guard_price <= 0 else max(self.addon_extreme_guard_price, current)
            )
        self.addon_extreme_guard_kline = prev

    def _update_addon_guard(self, row) -> None:
        """Update add-on guard from the latest completed candle."""
        if not ADDON_EXTREME_GUARD_ENABLED or not self.pos.is_active():
            return
        if not self.addon_extreme_guard_started and self.pos.filled:
            self.addon_extreme_guard_started = True
        if not self.addon_extreme_guard_started:
            return
        prev = self._previous_kline_ts(row.kline_ts)
        if prev not in self.kline_extremes.index or prev == self.addon_extreme_guard_kline:
            return
        if self.pos.direction == "long":
            current = float(self.kline_extremes.loc[prev, "low"])
            self.addon_extreme_guard_price = (
                current if self.addon_extreme_guard_price <= 0 else min(self.addon_extreme_guard_price, current)
            )
        else:
            current = float(self.kline_extremes.loc[prev, "high"])
            self.addon_extreme_guard_price = (
                current if self.addon_extreme_guard_price <= 0 else max(self.addon_extreme_guard_price, current)
            )
        self.addon_extreme_guard_kline = prev

    def _addon_guard_allows(self, price: float) -> bool:
        """Return whether an add-on price breaks the tracked candle extreme."""
        if not ADDON_EXTREME_GUARD_ENABLED:
            return True
        if self.addon_extreme_guard_price <= 0:
            self.blocked_addon_guard += 1
            return False
        if self.pos.direction == "long":
            allowed = price < self.addon_extreme_guard_price
        else:
            allowed = price > self.addon_extreme_guard_price
        if not allowed:
            self.blocked_addon_guard += 1
        return allowed

    def _remember_price(self, price: float) -> None:
        self.recent_prices.append(price)
        keep = max(NO_NEW_EXTREME_TICKS + 1, 3)
        if len(self.recent_prices) > keep:
            self.recent_prices = self.recent_prices[-keep:]

    def _still_making_new_low(self) -> bool:
        if len(self.recent_prices) < NO_NEW_EXTREME_TICKS + 1:
            return True
        recent = self.recent_prices[-(NO_NEW_EXTREME_TICKS + 1):]
        return recent[-1] <= min(recent[:-1])

    def _still_making_new_high(self) -> bool:
        if len(self.recent_prices) < NO_NEW_EXTREME_TICKS + 1:
            return True
        recent = self.recent_prices[-(NO_NEW_EXTREME_TICKS + 1):]
        return recent[-1] >= max(recent[:-1])

    def _bands(self, row) -> tuple[float, float, float, float]:
        unit_std = (float(row.upper_src) - float(row.mid)) / SOURCE_BOLL_STD
        upper = float(row.mid) + self.params.boll_std * unit_std
        lower = float(row.mid) - self.params.boll_std * unit_std
        width = upper - lower
        return lower, float(row.mid), upper, width

    def _remember_boll(self, row) -> None:
        """Keep recent Bollinger-shape data for trend-expansion checks."""
        lower, mid, upper, width = self._bands(row)
        price = float(row.price)
        half_width = upper - mid
        z = (price - mid) / half_width if half_width else 0.0
        kline_ts = pd.Timestamp(row.kline_ts)
        kline_extreme = self._trend_kline_extremes.setdefault(
            kline_ts,
            {"high": price, "low": price},
        )
        kline_extreme["high"] = max(kline_extreme["high"], price)
        kline_extreme["low"] = min(kline_extreme["low"], price)
        self.boll_history.append(
            {
                "ts": pd.Timestamp(row.ts),
                "kline_ts": kline_ts,
                "high": kline_extreme["high"],
                "low": kline_extreme["low"],
                "lower": lower,
                "mid": mid,
                "upper": upper,
                "width": width,
                "width_pct": width / price if price > 0 else 0.0,
                "z": z,
            }
        )
        keep_after = pd.Timestamp(row.ts) - pd.Timedelta(
            minutes=max(self.params.trend_risk_slope_window_min * 2, 180)
        )
        while self.boll_history and self.boll_history[0]["ts"] < keep_after:
            self.boll_history.pop(0)
        active_klines = {item["kline_ts"] for item in self.boll_history}
        self._trend_kline_extremes = {
            ts: value for ts, value in self._trend_kline_extremes.items() if ts in active_klines
        }

    def _width_ok(self, row) -> bool:
        _, _, _, width = self._bands(row)
        return width >= self._effective_min_boll_width(float(row.price))

    def _entry_max_width_ok(self, row) -> bool:
        """Return whether Bollinger width is not too wide for a first batch."""
        if not self.params.entry_max_width_enabled:
            return True
        _, _, _, width = self._bands(row)
        price = float(row.price)
        width_pct = width / price if price > 0 else 0.0
        pct_hit = self.params.entry_max_width_pct > 0 and width_pct >= self.params.entry_max_width_pct
        usd_hit = self.params.entry_max_width_usd > 0 and width >= self.params.entry_max_width_usd
        return not (pct_hit or usd_hit)

    def _entry_disaster_filter_ok(self, row, direction: str) -> bool:
        """Return whether stacked entry-trend risk still allows a first batch."""
        if not self.params.entry_disaster_filter_enabled:
            return True
        signal = self._entry_disaster_signal(row, direction)
        return signal is None

    def _entry_disaster_signal(self, row, direction: str) -> dict | None:
        """Return a disaster-style entry score when a fresh signal is too trend-like."""
        recent = self._recent_unique_boll_history(
            max(int(self.params.entry_disaster_kline_count), 2)
        )
        if len(recent) < int(self.params.entry_disaster_kline_count):
            return None

        price = float(row.price)
        _, mid, _, current_width = self._bands(row)
        lows = [item["low"] for item in recent]
        highs = [item["high"] for item in recent]
        lowers = [item["lower"] for item in recent]
        uppers = [item["upper"] for item in recent]
        mids = [item["mid"] for item in recent]
        widths = [item["width"] for item in recent]
        reasons = []

        if direction == "long":
            if all(lows[i] < lows[i - 1] for i in range(1, len(lows))):
                reasons.append("lower_lows")
            if all(lowers[i] < lowers[i - 1] for i in range(1, len(lowers))):
                reasons.append("lower_band_down")
            if mids[-1] < mids[0]:
                reasons.append("mid_down")
            mid_ratio = (mid - price) / current_width if current_width > 0 else 0.0
            if mid_ratio >= self.params.entry_disaster_far_mid_ratio:
                reasons.append("far_below_mid")
        elif direction == "short":
            if all(highs[i] > highs[i - 1] for i in range(1, len(highs))):
                reasons.append("higher_highs")
            if all(uppers[i] > uppers[i - 1] for i in range(1, len(uppers))):
                reasons.append("upper_band_up")
            if mids[-1] > mids[0]:
                reasons.append("mid_up")
            mid_ratio = (price - mid) / current_width if current_width > 0 else 0.0
            if mid_ratio >= self.params.entry_disaster_far_mid_ratio:
                reasons.append("far_above_mid")
        else:
            return None

        width_expand = widths[-1] / widths[0] if widths and widths[0] > 0 else 0.0
        if width_expand >= self.params.entry_disaster_width_expand:
            reasons.append("width_expand")

        expected_tp_distance = price * (self.params.entry_disaster_expected_return / LEVER)
        mid_distance = abs(price - mid)
        tp_distance_mult = mid_distance / expected_tp_distance if expected_tp_distance > 0 else 0.0
        if tp_distance_mult >= self.params.entry_disaster_tp_distance_mult:
            reasons.append("far_from_mid")

        score = len(reasons)
        if score < self.params.entry_disaster_score_threshold:
            return None
        return {
            "score": score,
            "reasons": reasons,
            "width_expand": width_expand,
            "tp_distance_mult": tp_distance_mult,
        }

    def _addon_max_width_ok(self, row) -> bool:
        """Return whether Bollinger width is not too wide for add-on batches."""
        if not self.params.addon_max_width_enabled:
            return True
        _, _, _, width = self._bands(row)
        price = float(row.price)
        width_pct = width / price if price > 0 else 0.0
        pct_hit = self.params.addon_max_width_pct > 0 and width_pct >= self.params.addon_max_width_pct
        usd_hit = self.params.addon_max_width_usd > 0 and width >= self.params.addon_max_width_usd
        return not (pct_hit or usd_hit)

    def _effective_boll_width_pct(self) -> float:
        base_pct = (
            self.params.boll_width_base_usd / self.params.boll_width_base_price
            if self.params.boll_width_base_price > 0
            else 0.0
        )
        return max(self.params.min_width_pct, base_pct)

    def _head_entry_price(self, fallback_price: float) -> float:
        filled = sorted(self.pos.filled, key=lambda batch: batch.idx)
        if filled:
            return filled[0].price
        if self.pos.pending is not None and self.pos.pending.idx == 0:
            return self.pos.pending.price
        return fallback_price

    def _min_ratio_ladder(self) -> list[float]:
        ratios = [self.params.first_batch_ratio, self.params.second_batch_dynamic_min_ratio]
        while (
            sum(ratios) + self.params.dynamic_min_ratio <= self.params.max_total_entry_ratio + 1e-12
            and len(ratios) < MAX_ENTRY_BATCHES
        ):
            ratios.append(self.params.dynamic_min_ratio)
        if sum(ratios) < self.params.max_total_entry_ratio and len(ratios) < MAX_ENTRY_BATCHES:
            ratios.append(self.params.max_total_entry_ratio - sum(ratios))
        return ratios

    def _liq_price_for_ladder(self, head_price: float, gap: float) -> float | None:
        ratios = self._min_ratio_ladder()
        prices = [head_price - idx * gap for idx in range(len(ratios))]
        if not prices or prices[-1] <= 0:
            return None
        sizing_equity = self._sizing_equity()
        sizes = []
        for ratio, price in zip(ratios, prices):
            raw_sz = sizing_equity * ratio * LEVER / (price * CT_VAL)
            sz = math.floor(raw_sz / CONTRACT_STEP) * CONTRACT_STEP
            if sz <= 0:
                return None
            sizes.append(sz)
        qty = sum(sizes) * CT_VAL
        avg = sum(sz * CT_VAL * price for sz, price in zip(sizes, prices)) / qty
        entry_fee = sum(sz * CT_VAL * price * OKX_LIQ_FEE_RATE for sz, price in zip(sizes, prices))
        margin_balance = max(sizing_equity - entry_fee, 0.0)
        denominator = qty * (OKX_MAINTENANCE_MARGIN_RATE + OKX_LIQ_FEE_RATE - 1)
        if denominator == 0:
            return None
        return (margin_balance - qty * avg) / denominator

    def _liq_price_for_batches(self, batches: list[Batch]) -> float | None:
        """Estimate liquidation price for currently filled batches."""
        if not batches:
            return None
        qty = sum(batch.sz for batch in batches) * CT_VAL
        if qty <= 0:
            return None
        avg = sum(batch.sz * CT_VAL * batch.price for batch in batches) / qty
        entry_fee = sum(batch.sz * CT_VAL * batch.price * OKX_LIQ_FEE_RATE for batch in batches)
        margin_balance = max(self._sizing_equity() - entry_fee, 0.0)
        if self.pos.direction == "short":
            denominator = qty * (1 + OKX_MAINTENANCE_MARGIN_RATE + OKX_LIQ_FEE_RATE)
            return (margin_balance + qty * avg) / denominator if denominator else None
        denominator = qty * (1 - OKX_MAINTENANCE_MARGIN_RATE - OKX_LIQ_FEE_RATE)
        return (qty * avg - margin_balance) / denominator if denominator else None

    def _liq_distance_pct(self, liq_price: float | None, mark_price: float) -> float:
        """Return distance from estimated liquidation as a price percentage."""
        if liq_price is None or liq_price <= 0 or mark_price <= 0:
            return 999.0
        if self.pos.direction == "short":
            return (liq_price - mark_price) / mark_price * 100
        return (mark_price - liq_price) / mark_price * 100

    def _track_liq_distance(self, mark_price: float) -> None:
        """Track the tightest estimated liquidation distance seen in replay."""
        if not self.pos.is_active():
            return
        distance = self._liq_distance_pct(self._liq_price_for_batches(self.pos.filled), mark_price)
        self.min_liq_distance_pct = min(self.min_liq_distance_pct, distance)
        if distance <= 0:
            self.wipeout_risk = True

    def _required_entry_gap_for_head_buffer(self, head_price: float) -> float:
        if not DYNAMIC_ENTRY_GAP_ENABLED or head_price <= 0:
            return self.params.min_entry_gap_usd
        cache_key = round(head_price, 1)
        if cache_key in self.entry_gap_cache:
            return self.entry_gap_cache[cache_key]

        def buffer_pct(gap: float) -> float:
            liq = self._liq_price_for_ladder(head_price, gap)
            if liq is None:
                return 999.0
            return (head_price - liq) / head_price

        if buffer_pct(self.params.min_entry_gap_usd) >= self.params.min_head_liq_buffer_pct:
            self.entry_gap_cache[cache_key] = self.params.min_entry_gap_usd
            return self.params.min_entry_gap_usd
        lo = self.params.min_entry_gap_usd
        hi = DYNAMIC_ENTRY_GAP_MAX_USD
        for _ in range(25):
            mid = (lo + hi) / 2
            if buffer_pct(mid) >= self.params.min_head_liq_buffer_pct:
                hi = mid
            else:
                lo = mid
        result = round(hi, 2)
        self.entry_gap_cache[cache_key] = result
        return result

    def _effective_entry_gap(self, price: float) -> float:
        head_price = self._head_entry_price(price)
        base_gap = max(self.params.min_entry_gap_usd, self._required_entry_gap_for_head_buffer(head_price))
        mult = self.entry_extreme_gap_mult if self.params.entry_extreme_gap_adjust_enabled else 1.0
        mult *= self._addon_dynamic_gap_mult(price)
        return round(base_gap * max(1.0, mult), 2)

    def _addon_dynamic_gap_mult(self, price: float) -> float:
        """Return dynamic add-on spacing multiplier for the current replay tick."""
        if not self.params.addon_dynamic_gap_enabled or not self.pos.is_active():
            return 1.0
        mult = 1.0
        row = self._current_row
        if row is not None and self.entry_boll_width > 0:
            _, _, _, width = self._bands(row)
            expand = width / self.entry_boll_width
            if expand >= self.params.addon_dynamic_gap_boll_strong:
                mult *= self.params.addon_dynamic_gap_boll_max_mult
            elif expand >= self.params.addon_dynamic_gap_boll_start:
                mid_mult = 1.0 + (self.params.addon_dynamic_gap_boll_max_mult - 1.0) * 0.5
                mult *= max(1.0, mid_mult)

        adverse_pct = self._head_adverse_move_pct(price)
        if adverse_pct >= self.params.addon_dynamic_gap_head_strong_pct:
            mult *= self.params.addon_dynamic_gap_head_max_mult
        elif adverse_pct >= self.params.addon_dynamic_gap_head_start_pct:
            mid_mult = 1.0 + (self.params.addon_dynamic_gap_head_max_mult - 1.0) * 0.5
            mult *= max(1.0, mid_mult)

        mult *= self._addon_dynamic_gap_trend_mult()
        if self.params.addon_dynamic_gap_max_usd > 0 and self.params.min_entry_gap_usd > 0:
            mult = min(mult, self.params.addon_dynamic_gap_max_usd / self.params.min_entry_gap_usd)
        mult = max(1.0, mult)
        if mult > 1.0001:
            self.dynamic_gap_events += 1
            self.dynamic_gap_mult_total += mult
            self.dynamic_gap_mult_max = max(self.dynamic_gap_mult_max, mult)
        return mult

    def _addon_dynamic_gap_trend_mult(self) -> float:
        """Return extra multiplier for consecutive completed-candle extremes."""
        if self.params.addon_dynamic_gap_trend_mult <= 1.0:
            return 1.0
        if self.pos.direction not in ("long", "short"):
            return 1.0
        row = self._current_row
        if row is None:
            return 1.0
        count = int(self.params.addon_dynamic_gap_trend_klines)
        if count < 2:
            return 1.0
        prev = self._previous_kline_ts(row.kline_ts)
        if prev is None:
            return 1.0
        idx = [value for value in self.kline_extremes.index if value <= prev]
        if len(idx) < count:
            return 1.0
        recent = self.kline_extremes.loc[idx[-count:]]
        if self.pos.direction == "long":
            lows = [float(value) for value in recent["low"]]
            if all(lows[i] > lows[i + 1] for i in range(len(lows) - 1)):
                return self.params.addon_dynamic_gap_trend_mult
            return 1.0
        highs = [float(value) for value in recent["high"]]
        if all(highs[i] < highs[i + 1] for i in range(len(highs) - 1)):
            return self.params.addon_dynamic_gap_trend_mult
        return 1.0

    def _effective_min_boll_width(self, price: float) -> float:
        if price <= 0:
            return self.params.min_width_usd
        return max(self.params.boll_width_floor_usd, price * self.params.min_width_pct)

    def _clamp_tp_return(self, value: float) -> float:
        low = max(self.params.low_boll_width_min_tp_return, 0.0)
        high = min(self.params.low_boll_width_max_tp_return, self.params.tp_target_margin_return)
        if high <= 0:
            return self.params.tp_target_margin_return
        if low > high:
            low = high
        return max(low, min(high, value))

    def _low_boll_width_tp_return(self, row) -> float:
        if (
            not self.params.low_boll_width_tp_enabled
            or self.params.low_boll_width_ref_pct <= 0
            or LEVER <= 0
        ):
            return self.params.tp_target_margin_return
        price = float(row.price)
        if price <= 0:
            return self.params.tp_target_margin_return
        _, _, _, width = self._bands(row)
        width_pct = width / price
        if width_pct >= self.params.low_boll_width_ref_pct:
            return self.params.tp_target_margin_return
        raw_return = width_pct * LEVER * self.params.low_boll_width_tp_capture_ratio
        return self._clamp_tp_return(raw_return)

    def _set_cycle_tp_target_from_boll(self, row) -> None:
        self.cycle_tp_target_margin_return = self._low_boll_width_tp_return(row)

    def _current_head_size_mult(self) -> float:
        if not self.params.low_boll_width_tp_enabled:
            return 1.0
        if self.cycle_tp_target_margin_return >= self.params.tp_target_margin_return - 1e-9:
            return 1.0
        return max(0.0, min(1.0, self.params.low_boll_width_size_mult))

    def _tp_space_width_rule(self, price: float) -> float:
        """Return the Bollinger width required by the configured TP target."""
        if not self.params.boll_width_tp_space_enabled:
            return 0.0
        if price <= 0 or LEVER <= 0 or self.params.tp_target_margin_return <= 0:
            return 0.0
        if self.params.boll_width_tp_space_mult <= 0:
            return 0.0
        return price * self.params.tp_target_margin_return / LEVER * self.params.boll_width_tp_space_mult

    def _entry_extreme_multiplier(self, gap_pct: float) -> float:
        if not self.params.entry_extreme_gap_adjust_enabled:
            return 1.0
        if gap_pct <= self.params.entry_extreme_gap_base_pct:
            return 1.0
        full = self.params.entry_extreme_gap_full_pct if self.params.entry_extreme_gap_full_pct > 0 else 1e-9
        progress = (gap_pct - self.params.entry_extreme_gap_base_pct) / full
        mult = 1.0 + progress * (self.params.entry_extreme_gap_max_mult - 1.0)
        return round(max(1.0, min(self.params.entry_extreme_gap_max_mult, mult)), 4)

    def _record_entry_extreme_adjustment(self, direction: str, price: float, row) -> None:
        self.entry_extreme_gap_pct = 0.0
        self.entry_extreme_gap_mult = 1.0
        if not self.params.entry_extreme_gap_adjust_enabled or price <= 0:
            return
        low24 = float(getattr(row, "low24", price) or price)
        high24 = float(getattr(row, "high24", price) or price)
        if direction == "long":
            gap_pct = max(0.0, (price - low24) / price)
        elif direction == "short":
            gap_pct = max(0.0, (high24 - price) / price)
        else:
            return
        mult = self._entry_extreme_multiplier(gap_pct)
        self.entry_extreme_gap_pct = gap_pct
        self.entry_extreme_gap_mult = mult
        self.entry_extreme_entries += 1
        self.entry_extreme_gap_total += gap_pct
        self.entry_extreme_gap_max = max(self.entry_extreme_gap_max, gap_pct)
        self.entry_extreme_mult_total += mult
        self.entry_extreme_mult_max = max(self.entry_extreme_mult_max, mult)

    def _dynamic_tp_distance(self, avg_entry: float) -> float:
        if avg_entry <= 0:
            return TP_PROFIT_USD
        target_return = self.cycle_tp_target_margin_return or self.params.tp_target_margin_return
        return round(avg_entry * target_return / LEVER, 2)

    def _refresh_tp(self) -> None:
        if not self.pos.is_active():
            return
        if self.dynamic_tp_active:
            return
        distance = self._dynamic_tp_distance(self.pos.avg_entry)
        self.pos.tp_price = (
            round(self.pos.avg_entry + distance, 2)
            if self.pos.direction == "long"
            else round(self.pos.avg_entry - distance, 2)
        )

    def _position_margin_return(self, price: float) -> float:
        if not self.pos.is_active() or self.pos.avg_entry <= 0:
            return 0.0
        if self.pos.direction == "long":
            move = (price - self.pos.avg_entry) / self.pos.avg_entry
        else:
            move = (self.pos.avg_entry - price) / self.pos.avg_entry
        return move * LEVER

    def _target_tp_price(self) -> float:
        distance = self._dynamic_tp_distance(self.pos.avg_entry)
        return (
            round(self.pos.avg_entry + distance, 2)
            if self.pos.direction == "long"
            else round(self.pos.avg_entry - distance, 2)
        )

    def _tp_price_from_margin_return(self, margin_return: float) -> float:
        if self.pos.avg_entry <= 0 or margin_return <= 0 or LEVER <= 0:
            return 0.0
        distance = round(self.pos.avg_entry * margin_return / LEVER, 2)
        if self.pos.direction == "long":
            return round(self.pos.avg_entry + distance, 2)
        if self.pos.direction == "short":
            return round(self.pos.avg_entry - distance, 2)
        return 0.0

    def _maybe_update_dynamic_tp(self, row) -> None:
        if not self.params.dynamic_tp_enabled:
            return
        price = float(row.price)
        ret = self._position_margin_return(price)
        if self.dynamic_tp_active:
            return
        target_return = self.cycle_tp_target_margin_return or self.params.tp_target_margin_return
        if ret < self.params.dynamic_tp_arm_return or ret >= target_return:
            return
        if self.pos.direction == "long" and self._still_making_new_high():
            return
        if self.pos.direction == "short" and self._still_making_new_low():
            return
        lock_price = round(price, 2)
        if abs(lock_price - self.pos.tp_price) < self.params.dynamic_tp_reprice_gap_usd:
            return
        self.pos.tp_price = lock_price
        self.dynamic_tp_active = True
        self.boll_mid_cost_tp_active = False
        self.dynamic_tp_activated += 1

    def _maybe_update_boll_tp_compression(self, row) -> bool:
        if not self.params.boll_tp_compression_enabled:
            return False
        if not self.pos.is_active() or self.pos.avg_entry <= 0 or self.pos.tp_price <= 0:
            return False
        price = float(row.price)
        ret = self._position_margin_return(price)
        if ret < self.params.boll_tp_compression_min_return:
            return False

        lower, _, upper, _ = self._bands(row)
        if self.pos.direction == "long":
            if upper > self.pos.tp_price or self._still_making_new_high():
                return False
            lock_price = round(price - self.params.boll_tp_compression_exit_offset_usd, 2)
            if lock_price <= self.pos.avg_entry:
                return False
        elif self.pos.direction == "short":
            if lower < self.pos.tp_price or self._still_making_new_low():
                return False
            lock_price = round(price + self.params.boll_tp_compression_exit_offset_usd, 2)
            if lock_price >= self.pos.avg_entry:
                return False
        else:
            return False

        if abs(lock_price - self.pos.tp_price) < self.params.dynamic_tp_reprice_gap_usd:
            return False
        self.pos.tp_price = lock_price
        self.dynamic_tp_active = True
        self.boll_mid_cost_tp_active = False
        self.boll_tp_compression_activated += 1
        return True

    def _outside_direction(self, row) -> str:
        lower, _, upper, _ = self._bands(row)
        price = float(row.price)
        if price < lower:
            return "long"
        if price > upper:
            return "short"
        return "none"

    def _signal(self, row) -> str:
        if not self._width_ok(row):
            self.blocked_width += 1
            return "none"
        direction = self._outside_direction(row)
        if direction == "none":
            return "none"
        if not self.pos.has_plan() and not self._entry_max_width_ok(row):
            self.blocked_entry_max_width += 1
            return "none"
        if not self.pos.has_plan() and not self._entry_disaster_filter_ok(row, direction):
            self.blocked_entry_disaster += 1
            return "none"
        self.signal_count += 1
        if direction == "long" and self._still_making_new_low():
            self.blocked_extreme += 1
            return "none"
        if direction == "short" and self._still_making_new_high():
            self.blocked_extreme += 1
            return "none"
        return direction

    def _head_adverse_move_pct(self, mark_price: float) -> float:
        """Return mark-price adverse move from the first filled batch."""
        filled = sorted(self.pos.filled, key=lambda batch: batch.idx)
        if not filled:
            return 0.0
        head_price = filled[0].price
        if head_price <= 0 or mark_price <= 0:
            return 0.0
        if self.pos.direction == "long":
            return max((head_price - mark_price) / head_price, 0.0)
        return max((mark_price - head_price) / head_price, 0.0)

    def _disaster_stop_triggered(self, mark_price: float) -> bool:
        """Return whether the strategy-cycle disaster stop should close now."""
        if not self.params.disaster_stop_enabled:
            return False
        risk_equity = self._strategy_risk_equity()
        if not self.pos.is_active() or risk_equity <= 0:
            return False
        if self.params.disaster_head_drop_pct <= 0 or self.params.disaster_loss_ratio <= 0:
            return False
        head_move = self._head_adverse_move_pct(mark_price)
        unrealized = self._unrealized(mark_price)
        loss_threshold = risk_equity * self.params.disaster_loss_ratio
        return head_move >= self.params.disaster_head_drop_pct and unrealized <= -loss_threshold

    def _boll_mid_cost_stop_triggered(self, row) -> bool:
        """Return whether the favorable Bollinger boundary reached average entry."""
        if not self.params.boll_mid_cost_stop_enabled:
            return False
        if not self.pos.is_active() or self.pos.avg_entry <= 0:
            return False
        lower, _, upper, _ = self._bands(row)
        if self.pos.direction == "long":
            return upper <= self.pos.avg_entry
        if self.pos.direction == "short":
            return lower >= self.pos.avg_entry
        return False

    def _maybe_reprice_boll_mid_cost_tp(self, row) -> bool:
        """Reprice take-profit when the Bollinger boundary reaches average entry."""
        if not self._boll_mid_cost_stop_triggered(row):
            return self._maybe_restore_boll_mid_cost_tp()
        new_tp = self._tp_price_from_margin_return(self.params.boll_mid_cost_tp_return)
        if new_tp <= 0:
            return False
        if self.pos.tp_price > 0 and abs(new_tp - self.pos.tp_price) < self.params.dynamic_tp_reprice_gap_usd:
            return False
        self.pos.tp_price = new_tp
        self.dynamic_tp_active = True
        self.boll_mid_cost_tp_active = True
        self.boll_mid_cost_stop += 1
        return True

    def _looks_like_boll_mid_cost_tp(self) -> bool:
        if self.boll_mid_cost_tp_active:
            return True
        if not self.dynamic_tp_active or self.pos.tp_price <= 0 or self.pos.avg_entry <= 0:
            return False
        boll_mid_tp = self._tp_price_from_margin_return(self.params.boll_mid_cost_tp_return)
        normal_tp = self._target_tp_price()
        if boll_mid_tp <= 0 or normal_tp <= 0:
            return False
        if abs(self.pos.tp_price - boll_mid_tp) >= self.params.dynamic_tp_reprice_gap_usd:
            return False
        if self.pos.direction == "long":
            return boll_mid_tp < normal_tp
        if self.pos.direction == "short":
            return boll_mid_tp > normal_tp
        return False

    def _maybe_restore_boll_mid_cost_tp(self) -> bool:
        """Restore normal TP after Bollinger mid moves back beyond cost."""
        if not self._looks_like_boll_mid_cost_tp():
            return False
        normal_tp = self._target_tp_price()
        if normal_tp <= 0:
            return False
        if self.pos.tp_price > 0 and abs(normal_tp - self.pos.tp_price) < self.params.dynamic_tp_reprice_gap_usd:
            self.boll_mid_cost_tp_active = False
            self.dynamic_tp_active = False
            return False
        self.pos.tp_price = normal_tp
        self.boll_mid_cost_tp_active = False
        self.dynamic_tp_active = False
        return True

    def _fixed_loss_stop_price(self) -> float:
        """Return the live-style fixed-loss stop trigger price."""
        if not self.params.copy_fixed_loss_stop_enabled:
            return 0.0
        if not self.pos.is_active() or self.pos.avg_entry <= 0 or self.pos.total_sz <= 0:
            return 0.0
        target_loss = self.params.copy_fixed_loss_stop_usdt
        if target_loss <= 0:
            target_loss = self._strategy_risk_equity() * self.params.copy_fixed_loss_stop_ratio
        if target_loss <= 0:
            return 0.0
        price_delta = target_loss / (self.pos.total_sz * CT_VAL)
        if self.pos.direction == "long":
            return self.pos.avg_entry - price_delta
        if self.pos.direction == "short":
            return self.pos.avg_entry + price_delta
        return 0.0

    def _desired_stop_loss_price(self) -> tuple[float, str]:
        """Return live-style L2-first stop price and selected stop mode."""
        if not self.pos.is_active() or self.pos.direction not in ("long", "short"):
            return 0.0, "none"
        fixed_stop = self._fixed_loss_stop_price()
        liq = self._liq_price_for_batches(self.pos.filled)
        liq_guard = 0.0
        if liq and liq > 0:
            if self.pos.direction == "long":
                liq_guard = liq + LIQ_STOP_OFFSET_USD
            else:
                liq_guard = liq - LIQ_STOP_OFFSET_USD

        stop_price = fixed_stop
        mode = "fixed_cycle_loss" if fixed_stop > 0 else "none"
        if stop_price <= 0 and liq_guard > 0:
            stop_price = liq_guard
            mode = "liquidation_guard"
        if liq_guard > 0:
            if self.pos.direction == "long" and stop_price < liq_guard:
                stop_price = liq_guard
                mode = "liquidation_guard_fallback"
            elif self.pos.direction == "short" and stop_price > liq_guard:
                stop_price = liq_guard
                mode = "liquidation_guard_fallback"
        return stop_price, mode

    def _fixed_loss_stop_triggered(self, mark_price: float) -> bool:
        """Return whether the current L2/L1 stop order would trigger."""
        stop_price, _ = self._desired_stop_loss_price()
        if stop_price <= 0:
            return False
        if self.pos.direction == "long":
            return mark_price <= stop_price
        if self.pos.direction == "short":
            return mark_price >= stop_price
        return False

    def _fixed_loss_stop_price_for(self, direction: str, avg_entry: float, total_sz: float) -> float:
        """Return fixed-loss trigger price for a simulated candidate position."""
        if not self.params.copy_fixed_loss_stop_enabled:
            return 0.0
        if avg_entry <= 0 or total_sz <= 0:
            return 0.0
        target_loss = self.params.copy_fixed_loss_stop_usdt
        if target_loss <= 0:
            target_loss = self._strategy_risk_equity() * self.params.copy_fixed_loss_stop_ratio
        if target_loss <= 0:
            return 0.0
        price_delta = target_loss / (total_sz * CT_VAL)
        if direction == "long":
            return avg_entry - price_delta
        if direction == "short":
            return avg_entry + price_delta
        return 0.0

    def _candidate_entry_totals(self, idx: int, price: float, sz: float) -> tuple[float, float]:
        """Return average entry and size after adding/replacing a candidate batch."""
        entries = [batch for batch in self.pos.filled if batch.idx != idx]
        if self.pos.pending is not None and self.pos.pending.idx != idx:
            entries.append(self.pos.pending)
        entries.append(Batch(idx=idx, price=price, sz=sz))
        total_sz = sum(batch.sz for batch in entries)
        if total_sz <= 0:
            return 0.0, 0.0
        avg_entry = sum(batch.price * batch.sz for batch in entries) / total_sz
        return avg_entry, total_sz

    def _fixed_loss_head_buffer_allows(self, idx: int, price: float, sz: float) -> bool:
        """Return whether a candidate add-on keeps fixed-loss stop beyond the head buffer."""
        if not self.params.fixed_loss_head_buffer_enabled or idx <= 0:
            return True
        if self.pos.direction not in ("long", "short") or not self.pos.filled:
            return True
        head_price = sorted(self.pos.filled, key=lambda batch: batch.idx)[0].price
        if head_price <= 0 or price <= 0 or sz <= 0:
            return True
        avg_entry, total_sz = self._candidate_entry_totals(idx, price, sz)
        stop_price = self._fixed_loss_stop_price_for(self.pos.direction, avg_entry, total_sz)
        if stop_price <= 0:
            return True
        if self.pos.direction == "long":
            allowed = stop_price <= head_price * (1 - self.params.fixed_loss_head_buffer_pct)
        else:
            allowed = stop_price >= head_price * (1 + self.params.fixed_loss_head_buffer_pct)
        if not allowed:
            self.fixed_loss_head_buffer_block += 1
        return allowed

    def _addon_expected_tp_price(self, direction: str, avg_entry: float) -> float:
        """Return the add-on guard's expected take-profit price."""
        expected_return = self.params.addon_tp_improve_expected_return
        if avg_entry <= 0 or LEVER <= 0 or expected_return <= 0:
            return 0.0
        distance = avg_entry * expected_return / LEVER
        if direction == "long":
            return avg_entry + distance
        if direction == "short":
            return avg_entry - distance
        return 0.0

    def _addon_tp_improve_allows(self, idx: int, price: float, sz: float) -> bool:
        """Return whether a candidate add-on meaningfully improves expected TP."""
        if not self.params.addon_tp_improve_guard_enabled or idx <= 0:
            return True
        if self.pos.direction not in ("long", "short") or not self.pos.is_active():
            return True
        if self.pos.avg_entry <= 0 or self.pos.total_sz <= 0 or sz <= 0:
            return True

        old_avg = self.pos.avg_entry
        old_tp = self._addon_expected_tp_price(self.pos.direction, old_avg)
        new_avg, _ = self._candidate_entry_totals(idx, price, sz)
        new_tp = self._addon_expected_tp_price(self.pos.direction, new_avg)
        if old_tp <= 0 or new_tp <= 0:
            return True

        improve = old_tp - new_tp if self.pos.direction == "long" else new_tp - old_tp
        expected_distance = abs(old_tp - old_avg)
        required = max(
            self.params.addon_tp_improve_min_usd,
            expected_distance * self.params.addon_tp_improve_ratio,
        )
        if improve + 1e-9 >= required:
            return True

        self.tp_improve_block += 1
        return False

    def _head_adverse_move_pct(self, mark_price: float) -> float:
        """Return adverse move from the first filled batch as a ratio."""
        head = self.pos.filled[0].price if self.pos.filled else 0.0
        if head <= 0 or mark_price <= 0:
            return 0.0
        if self.pos.direction == "long":
            return max((head - mark_price) / head, 0.0)
        if self.pos.direction == "short":
            return max((mark_price - head) / head, 0.0)
        return 0.0

    def _series_slope_pct_per_hour(self, values: list[float], start_ts, end_ts) -> float:
        """Return percent-per-hour slope across a window."""
        if len(values) < 2 or not values[0]:
            return 0.0
        hours = (pd.Timestamp(end_ts) - pd.Timestamp(start_ts)).total_seconds() / 3600
        if hours <= 0:
            return 0.0
        return (values[-1] - values[0]) / values[0] * 100 / hours

    def _recent_unique_boll_history(self, count: int) -> list[dict]:
        """Return recent unique candle snapshots from Bollinger history."""
        unique = []
        seen = set()
        for item in reversed(self.boll_history):
            ts = item.get("kline_ts", item["ts"])
            if ts in seen:
                continue
            unique.append(item)
            seen.add(ts)
            if len(unique) >= count:
                break
        return list(reversed(unique))

    def _trend_risk_guard_triggered(self, row) -> bool:
        """Detect stacked trend-risk conditions after entry."""
        if not self.params.trend_risk_guard_enabled:
            return False
        if not self.pos.is_active():
            return False
        if not self.entry_time or self.entry_boll_width <= 0:
            return False

        now = pd.Timestamp(row.ts)
        entry_ts = pd.Timestamp(self.entry_time)
        hold_min = (now - entry_ts).total_seconds() / 60
        if hold_min < self.params.trend_risk_min_hold_min:
            return False

        mark_price = float(row.price)
        adverse_pct = self._head_adverse_move_pct(mark_price)
        if adverse_pct < self.params.trend_risk_head_adverse_pct:
            return False

        _, mid, _, width = self._bands(row)
        width_pct = width / mark_price if mark_price > 0 else 0.0
        width_expand = width / self.entry_boll_width if self.entry_boll_width > 0 else 0.0
        window_start = now - pd.Timedelta(minutes=self.params.trend_risk_slope_window_min)
        window = [item for item in self.boll_history if item["ts"] >= window_start]
        if len(window) < 2:
            return False

        mid_slope = self._series_slope_pct_per_hour(
            [item["mid"] for item in window],
            window[0]["ts"],
            window[-1]["ts"],
        )
        lower_slope = self._series_slope_pct_per_hour(
            [item["lower"] for item in window],
            window[0]["ts"],
            window[-1]["ts"],
        )
        upper_slope = self._series_slope_pct_per_hour(
            [item["upper"] for item in window],
            window[0]["ts"],
            window[-1]["ts"],
        )

        recent = self._recent_unique_boll_history(max(self.params.trend_risk_kline_count, 2))
        lows = [item["low"] for item in recent]
        highs = [item["high"] for item in recent]
        lower_lows = len(lows) >= self.params.trend_risk_kline_count and all(
            lows[i] < lows[i - 1] for i in range(1, len(lows))
        )
        higher_highs = len(highs) >= self.params.trend_risk_kline_count and all(
            highs[i] > highs[i - 1] for i in range(1, len(highs))
        )

        score = 1
        if width_expand >= self.params.trend_risk_width_expand:
            score += 1
        if self.pos.direction == "long":
            score += int(mark_price < mid)
            score += int(lower_lows)
            score += int(mid_slope <= -self.params.trend_risk_mid_slope_pct_per_hour)
            score += int(lower_slope <= -self.params.trend_risk_edge_slope_pct_per_hour)

        elif self.pos.direction == "short":
            score += int(mark_price > mid)
            score += int(higher_highs)
            score += int(mid_slope >= self.params.trend_risk_mid_slope_pct_per_hour)
            score += int(upper_slope >= self.params.trend_risk_edge_slope_pct_per_hour)

        return score >= self.params.trend_risk_score_threshold

    def _process_tick(self, row) -> None:
        if self.stopped:
            return
        self.current_price = float(row.price)
        if CROSS_COPY_PROTECT_ENABLED and self._account_equity(float(row.price)) <= CROSS_COPY_PROTECT_EQUITY_USDT:
            if self.pos.is_active():
                self._close(
                    float(row.price),
                    str(row.ts),
                    float(row.price),
                    row.kline_ts,
                    rebalance=False,
                    reason="鍏ㄤ粨淇濇姢姝㈡崯骞充粨",
                )
            self.pos.reset()
            self.pos.pending = None
            self.cross_copy_stop += 1
            self.stopped = True
            return
        if self.pos.pending is not None:
            self._try_fill_pending(row)
        if self.pos.is_active():
            self._try_take_profit(row)
        if self.pos.is_active() and self._fixed_loss_stop_triggered(float(row.price)):
            _, stop_mode = self._desired_stop_loss_price()
            self._close(
                float(row.price),
                str(row.ts),
                float(row.price),
                row.kline_ts,
                rebalance=True,
                reason="鍥哄畾浜忔崯淇濇姢骞充粨",
            )
            self.fixed_loss_stop += 1
            if stop_mode == "fixed_cycle_loss":
                self.fixed_cycle_loss_stop += 1
            elif stop_mode == "liquidation_guard_fallback":
                self.liquidation_guard_fallback_stop += 1
        if self.pos.is_active() and self._trend_risk_guard_triggered(row):
            if not self.trend_risk_guard_active:
                self.trend_risk_guard_signal += 1
                self.trend_risk_guard_active = True
            if self.params.trend_risk_guard_close_enabled:
                self._close(
                    float(row.price),
                    str(row.ts),
                    float(row.price),
                    row.kline_ts,
                    rebalance=True,
                    reason="Trend risk guard close",
                )
                self.trend_risk_guard_stop += 1
        if self.pos.is_active() and self._disaster_stop_triggered(float(row.price)):
            self._close(
                float(row.price),
                str(row.ts),
                float(row.price),
                row.kline_ts,
                rebalance=False,
                reason="鐏鹃毦姝㈡崯骞充粨",
            )
            self.disaster_stop += 1
            return
        if self.pos.is_active() and self._maybe_reprice_boll_mid_cost_tp(row):
            return
        if self.pos.is_active():
            compressed = self._maybe_update_boll_tp_compression(row)
            if not compressed:
                self._maybe_update_dynamic_tp(row)
        if self.capital_shortage_active and self.trading_balance + 0.01 >= TRADING_ACCOUNT_TARGET:
            self.capital_shortage_active = False
        if self.pos.has_plan():
            self._maintain_plan(row)
        else:
            self._try_open(row)

    def _try_open(self, row) -> None:
        if self.capital_shortage_active:
            return
        direction = self._signal(row)
        if direction == "none":
            return
        price = float(row.price)
        if self.last_plan_price > 0 and abs(price - self.last_plan_price) < self._effective_entry_gap(price):
            self.blocked_gap += 1
            return
        if self.last_batch_kline is not None and row.kline_ts == self.last_batch_kline:
            self.same_k_block += 1
            return
        if self.last_close_kline is not None:
            if row.kline_ts == self.last_close_kline:
                self.close_kline_block += 1
                return
            self.last_close_kline = None
        self._set_cycle_tp_target_from_boll(row)
        order = self._order_at(0, price)
        if order is None:
            return
        self._record_entry_extreme_adjustment(direction, price, row)
        self.pos.direction = direction
        self.pos.pending = order
        self.events.append(
            {
                "ts": str(row.ts),
                "type": "entry_order",
                "direction": direction,
                "batch": 1,
                "price": order.price,
                "sz": order.sz,
                "pnl": 0.0,
                "note": "澶翠粨鎸傚崟",
            }
        )
        self.last_plan_price = price
        self.last_entry_check_kline = row.kline_ts
        self.entry_time = str(row.ts)
        self._try_fill_pending(row)

    def _pending_order_still_breaks_band(self, row) -> bool:
        if self.pos.pending is None:
            return True
        lower, _, upper, _ = self._bands(row)
        if self.pos.direction == "long":
            return self.pos.pending.price <= lower
        if self.pos.direction == "short":
            return self.pos.pending.price >= upper
        return True

    def _cancel_pending_if_order_returns_inside_band(self, row) -> bool:
        if not PENDING_ORDER_BAND_GUARD_ENABLED:
            return False
        if self.pos.pending is None:
            return False
        if self._pending_order_still_breaks_band(row):
            return False
        self.inside_cancel += 1
        self.pos.pending = None
        self.last_entry_check_kline = None
        if not self.pos.is_active():
            self.pos.reset()
        return True

    def _maintain_plan(self, row) -> None:
        if self.pos.pending is not None:
            if self._cancel_pending_if_order_returns_inside_band(row):
                return
            if not self._width_ok(row):
                self.width_cancel += 1
                self.pos.pending = None
                if not self.pos.is_active():
                    self.pos.reset()
                return
            if self.pos.pending.idx == 0 and not self.pos.is_active() and not self._entry_max_width_ok(row):
                self.blocked_entry_max_width += 1
                self.pos.pending = None
                self.pos.reset()
                return
            if (
                self.pos.pending.idx == 0
                and not self.pos.is_active()
                and not self._entry_disaster_filter_ok(row, self.pos.direction)
            ):
                self.blocked_entry_disaster += 1
                self.pos.pending = None
                self.pos.reset()
                return
            if self.pos.pending.idx > 0 and not self._addon_max_width_ok(row):
                self.blocked_addon_max_width += 1
                self.pos.pending = None
                return
            if (
                self.pos.pending.idx > 0
                and self.params.trend_risk_freeze_addon_enabled
                and self.trend_risk_guard_active
            ):
                self.trend_risk_freeze_cancels += 1
                self.pos.pending = None
                return
            if self._outside_direction(row) != self.pos.direction:
                if row.kline_ts != self.last_entry_check_kline:
                    self.inside_cancel += 1
                    self.last_entry_check_kline = row.kline_ts
                return
            if row.kline_ts != self.last_entry_check_kline:
                self._maybe_reprice(row)
                self.last_entry_check_kline = row.kline_ts
            return

        if not self.pos.is_active():
            return
        self._maybe_place_next_batch(row)

    def _maybe_place_next_batch(self, row) -> None:
        if self.capital_shortage_active:
            return
        if self.params.trend_risk_freeze_addon_enabled and self.trend_risk_guard_active:
            self.trend_risk_freeze_blocks += 1
            return
        if not self._width_ok(row):
            return
        if not self._addon_max_width_ok(row):
            self.blocked_addon_max_width += 1
            return
        if self._outside_direction(row) != self.pos.direction:
            return
        if self.pos.direction == "long" and self._still_making_new_low():
            return
        if self.pos.direction == "short" and self._still_making_new_high():
            return
        if self.last_batch_kline is not None and row.kline_ts == self.last_batch_kline:
            self.same_k_block += 1
            return

        last = self.pos.last_filled()
        if last is None:
            return
        price = float(row.price)
        if self.pos.direction == "long" and price > last.price:
            return
        if self.pos.direction == "short" and price < last.price:
            return
        if abs(price - last.price) < self._effective_entry_gap(price):
            self.blocked_gap += 1
            return
        self._update_addon_guard(row)
        if not self._addon_guard_allows(price):
            return
        order = self._order_at(self.pos.next_idx(), price)
        if order is None:
            return
        self.pos.pending = order
        self.events.append(
            {
                "ts": str(row.ts),
                "type": "entry_order",
                "direction": self.pos.direction,
                "batch": order.idx + 1,
                "price": order.price,
                "sz": order.sz,
                "pnl": 0.0,
                "note": "琛ヤ粨鎸傚崟",
            }
        )
        self.last_entry_check_kline = row.kline_ts
        self._try_fill_pending(row)

    def _maybe_reprice(self, row) -> None:
        if self.pos.pending is None:
            return
        if self.pos.pending.idx > 0 and not self._addon_max_width_ok(row):
            self.blocked_addon_max_width += 1
            self.pos.pending = None
            return
        if self.pos.direction == "long" and self._still_making_new_low():
            return
        if self.pos.direction == "short" and self._still_making_new_high():
            return
        replacement = self._order_at(self.pos.pending.idx, float(row.price))
        if replacement is None:
            if self.pos.pending is not None and self.pos.pending.idx > 0:
                self.pos.pending = None
            return
        last = self.pos.last_filled()
        if last is not None and abs(replacement.price - last.price) < self._effective_entry_gap(float(row.price)):
            return
        if abs(replacement.price - self.pos.pending.price) < self.params.reprice_gap_usd:
            return
        if self.pos.pending.idx > 0:
            self._update_addon_guard(row)
            if not self._addon_guard_allows(replacement.price):
                return
        if self.pos.pending.idx == 0:
            self._record_entry_extreme_adjustment(self.pos.direction, float(row.price), row)
            self._set_cycle_tp_target_from_boll(row)
        self.pos.pending = replacement
        self.events.append(
            {
                "ts": str(row.ts),
                "type": "reprice",
                "direction": self.pos.direction,
                "batch": replacement.idx + 1,
                "price": replacement.price,
                "sz": replacement.sz,
                "pnl": 0.0,
                "note": "鎸傚崟閲嶆寕",
            }
        )
        self.reprice_count += 1

    def _sizing_equity(self) -> float:
        if ROLLING_COMPOUND_ENABLED:
            protected = CROSS_COPY_PROTECT_EQUITY_USDT if CROSS_COPY_PROTECT_ENABLED else 0.0
            return max(self._account_equity() - protected, 0.0)
        if CROSS_COPY_DYNAMIC_SIZING_ENABLED:
            protected = CROSS_COPY_PROTECT_EQUITY_USDT if CROSS_COPY_PROTECT_ENABLED else 0.0
            available = max(self._account_equity() - protected, 0.0)
            return min(TRADING_ACCOUNT_TARGET, available) if TRADING_ACCOUNT_TARGET > 0 else available
        return TRADING_ACCOUNT_TARGET if TRADING_ACCOUNT_TARGET > 0 else max(self.trading_balance, 0.0)

    def _strategy_risk_equity(self) -> float:
        """Return the capital base used by cycle-level risk controls."""
        if ROLLING_COMPOUND_ENABLED:
            return max(self._sizing_equity(), 0.0)
        if TRADING_ACCOUNT_TARGET > 0:
            return TRADING_ACCOUNT_TARGET
        return max(self._sizing_equity(), 0.0)

    def _used_entry_ratio(self, exclude_idx: int | None = None) -> float:
        equity = self._sizing_equity()
        if equity <= 0:
            return 0.0
        return self._used_entry_margin(exclude_idx=exclude_idx) / equity

    def _used_entry_margin(self, exclude_idx: int | None = None) -> float:
        batches = list(self.pos.filled)
        if self.pos.pending is not None:
            batches.append(self.pos.pending)
        used_margin = 0.0
        for batch in batches:
            if exclude_idx is not None and batch.idx == exclude_idx:
                continue
            used_margin += batch.sz * CT_VAL * batch.price / LEVER
        return used_margin

    def _dynamic_entry_ratio(self, idx: int, price: float) -> float:
        if idx == 0:
            return self.params.first_batch_ratio * self._current_head_size_mult()
        if idx == 1:
            head = next((batch for batch in self.pos.filled if batch.idx == 0), None)
            if head is None or head.price <= 0 or self.params.second_batch_dynamic_full_gap_usd <= 0:
                return 0.0
            gap = abs(price - head.price)
            ratio = (
                self.params.second_batch_dynamic_base_ratio
                * gap
                / self.params.second_batch_dynamic_full_gap_usd
            )
            return min(
                self.params.second_batch_dynamic_max_ratio,
                max(self.params.second_batch_dynamic_min_ratio, ratio),
            )
        filled = sorted(self.pos.filled, key=lambda batch: batch.idx)
        if len(filled) < 2:
            ratio = self.params.dynamic_base_ratio
        else:
            prev_batch = filled[-2]
            last_batch = filled[-1]
            prev_gap = abs(last_batch.price - prev_batch.price)
            current_gap = abs(price - last_batch.price)
            ratio = self.params.dynamic_base_ratio if prev_gap <= 0 else self.params.dynamic_base_ratio * current_gap / prev_gap
        return min(self.params.dynamic_max_ratio, max(self.params.dynamic_min_ratio, ratio))

    def _order_at(self, idx: int, price: float) -> Batch | None:
        if idx >= MAX_ENTRY_BATCHES:
            return None
        equity = self._sizing_equity()
        if equity <= 0:
            return None
        ratio = self._dynamic_entry_ratio(idx, price)
        margin_budget = equity * ratio
        raw_sz = margin_budget * LEVER / (price * CT_VAL)
        sz = math.floor(raw_sz / CONTRACT_STEP) * CONTRACT_STEP
        sz = round(sz, 8)
        if sz < MIN_ORDER_CONTRACTS:
            return None
        candidate_margin = price * sz * CT_VAL / LEVER
        free_margin = max(self.trading_balance - self._used_entry_margin(exclude_idx=idx), 0.0)
        if candidate_margin > free_margin + 1e-9:
            self.skipped_funds += 1
            return None
        candidate_ratio = candidate_margin / equity
        if self._used_entry_ratio(exclude_idx=idx) + candidate_ratio > self.params.max_total_entry_ratio:
            self.skipped_entry_cap += 1
            return None
        if not self._fixed_loss_head_buffer_allows(idx, round(price, 2), sz):
            return None
        if not self._addon_tp_improve_allows(idx, round(price, 2), sz):
            return None
        return Batch(idx=idx, price=round(price, 2), sz=sz)

    def _try_fill_pending(self, row) -> None:
        if self.pos.pending is None:
            return
        price = float(row.price)
        pending = self.pos.pending
        if self.pos.direction == "long" and price > pending.price:
            return
        if self.pos.direction == "short" and price < pending.price:
            return
        fee = pending.sz * CT_VAL * pending.price * TAKER_FEE
        self.trading_balance -= fee
        self.pos.filled.append(pending)
        self.pos.pending = None
        self.dynamic_tp_active = False
        self.pos.recalc()
        self._refresh_tp()
        self.last_batch_kline = row.kline_ts
        self._start_addon_guard(row)
        event_type = "first_fill" if pending.idx == 0 else "add_fill"
        if pending.idx == 0:
            _, _, _, width = self._bands(row)
            self.entry_boll_width = width
            self.entry_boll_width_pct = width / float(row.price) if float(row.price) > 0 else 0.0
        self.events.append(
            {
                "ts": str(row.ts),
                "type": event_type,
                "direction": self.pos.direction,
                "batch": pending.idx + 1,
                "price": pending.price,
                "sz": pending.sz,
                "pnl": 0.0,
                "note": "澶翠粨鎴愪氦" if pending.idx == 0 else "琛ヤ粨鎴愪氦",
            }
        )

    def _try_take_profit(self, row) -> None:
        price = float(row.price)
        if self.pos.direction == "long" and price < self.pos.tp_price:
            return
        if self.pos.direction == "short" and price > self.pos.tp_price:
            return
        self._close(self.pos.tp_price, str(row.ts), price, row.kline_ts)

    def _close(
        self,
        exit_price: float,
        ts: str,
        mark_price: float,
        kline_ts,
        rebalance: bool = True,
        reason: str = "姝㈢泩骞充粨",
    ) -> None:
        avg = self.pos.avg_entry
        sz = self.pos.total_sz
        if self.pos.direction == "long":
            raw_pnl = (exit_price - avg) * sz * CT_VAL
        else:
            raw_pnl = (avg - exit_price) * sz * CT_VAL
        fee = sz * CT_VAL * exit_price * TAKER_FEE
        pnl = raw_pnl - fee
        self.trading_balance += pnl
        if rebalance:
            self._rebalance(pnl)
        self.events.append(
            {
                "ts": ts,
                "type": "close",
                "direction": self.pos.direction,
                "batch": len(self.pos.filled),
                "price": round(exit_price, 4),
                "sz": round(sz, 8),
                "pnl": round(pnl, 4),
                "note": reason,
            }
        )
        self.trades.append(
            Trade(
                entry_time=self.entry_time,
                exit_time=ts,
                direction=self.pos.direction,
                avg_entry=round(avg, 4),
                exit_price=round(exit_price, 4),
                sz=round(sz, 8),
                pnl=round(pnl, 4),
                batches=len(self.pos.filled),
            )
        )
        self.pos.reset()
        self.dynamic_tp_active = False
        self.boll_mid_cost_tp_active = False
        self.cycle_tp_target_margin_return = self.params.tp_target_margin_return
        self.entry_extreme_gap_pct = 0.0
        self.entry_extreme_gap_mult = 1.0
        self.addon_extreme_guard_price = 0.0
        self.addon_extreme_guard_kline = None
        self.addon_extreme_guard_started = False
        self.entry_boll_width = 0.0
        self.entry_boll_width_pct = 0.0
        self.trend_risk_guard_active = False
        self.entry_time = ""
        self.last_batch_kline = None
        self.last_entry_check_kline = None
        self.last_close_kline = kline_ts

    def _rebalance(self, actual_pnl: float | None = None) -> None:
        if ROLLING_COMPOUND_ENABLED:
            self.capital_shortage_active = False
            return
        if CROSS_COPY_PROTECT_ENABLED:
            if actual_pnl is None:
                return
            if actual_pnl > 0.01:
                amount = min(actual_pnl, self.trading_balance)
                self.trading_balance -= amount
                self.funding_balance += amount
                self.profit_transferred += amount
            elif actual_pnl < -0.01:
                topup = min(abs(actual_pnl), max(self.funding_balance, 0.0))
                self.trading_balance += topup
                self.funding_balance -= topup
                self.loss_topup += topup
            return
        diff = self.trading_balance - TRADING_ACCOUNT_TARGET
        if diff > 0.01:
            self.trading_balance -= diff
            self.funding_balance += diff
            self.profit_transferred += diff
        elif diff < -0.01:
            topup = min(abs(diff), max(self.funding_balance, 0.0))
            self.trading_balance += topup
            self.funding_balance -= topup
            self.loss_topup += topup
            if self.trading_balance + 0.01 < TRADING_ACCOUNT_TARGET:
                self.capital_shortage_active = True

    def _unrealized(self, price: float) -> float:
        if not self.pos.is_active():
            return 0.0
        if self.pos.direction == "long":
            return (price - self.pos.avg_entry) * self.pos.total_sz * CT_VAL
        return (self.pos.avg_entry - price) * self.pos.total_sz * CT_VAL

    def _risk_adjusted_score(self, total_pnl: float, max_dd_pct: float, end_unrealized: float) -> float:
        """Return a balanced score for risk-aware parameter ranking."""
        min_liq = self.min_liq_distance_pct if self.min_liq_distance_pct != 999.0 else 99.0
        wipeout_penalty = 1000.0 if self.wipeout_risk else 0.0
        if min_liq <= 0:
            liq_penalty = 1000.0
        elif min_liq < 0.5:
            liq_penalty = 240.0 + (0.5 - min_liq) * 300.0
        elif min_liq < 1.0:
            liq_penalty = 80.0 + (1.0 - min_liq) * 320.0
        elif min_liq < 1.5:
            liq_penalty = (1.5 - min_liq) * 120.0
        else:
            liq_penalty = 0.0
        drawdown_penalty = max_dd_pct * 4.0
        open_loss_penalty = abs(min(0.0, end_unrealized)) * 1.2
        skip_penalty = (self.skipped_entry_cap + self.skipped_funds) * 0.05
        stop_penalty = (
            self.disaster_stop * 30.0
            + self.fixed_loss_stop * 50.0
            + self.boll_mid_cost_stop * 40.0
        )
        low_trade_penalty = max(0, 8 - len(self.trades)) * 10.0
        return round(
            total_pnl
            - drawdown_penalty
            - liq_penalty
            - wipeout_penalty
            - open_loss_penalty
            - skip_penalty
            - stop_penalty
            - low_trade_penalty,
            4,
        )

    def _risk_quality_score(self, max_dd_pct: float) -> float:
        """Return a risk-only score where higher means safer."""
        min_liq = self.min_liq_distance_pct if self.min_liq_distance_pct != 999.0 else 99.0
        return round(
            min_liq * 100.0
            - max_dd_pct * 5.0
            - self.disaster_stop * 80.0
            - self.fixed_loss_stop * 100.0
            - self.boll_mid_cost_stop * 80.0
            - (1000.0 if self.wipeout_risk else 0.0),
            4,
        )

    def report(self) -> dict:
        """Return one summary row."""
        pnls = [trade.pnl for trade in self.trades]
        wins = [pnl for pnl in pnls if pnl > 0]
        peak = self.initial_total
        max_dd = 0.0
        for equity in self.equity_curve:
            peak = max(peak, equity)
            max_dd = max(max_dd, (peak - equity) / peak if peak else 0.0)
        end_price = float(self.ticks["price"].iloc[-1]) if len(self.ticks) else 0.0
        end_unrealized = round(self._unrealized(end_price), 4)
        total_pnl = round(self.total_equity() - self.initial_total, 4)
        max_drawdown_pct = round(max_dd * 100, 4)
        min_liq = round(self.min_liq_distance_pct if self.min_liq_distance_pct != 999.0 else 0.0, 4)
        balanced_score = self._risk_adjusted_score(total_pnl, max_drawdown_pct, end_unrealized)
        risk_score = self._risk_quality_score(max_drawdown_pct)
        durations = [
            (pd.Timestamp(trade.exit_time) - pd.Timestamp(trade.entry_time)).total_seconds() / 60
            for trade in self.trades
        ]
        return {
            "boll_std": self.params.boll_std,
            "min_width_usd": self.params.min_width_usd,
            "min_width_pct": self.params.min_width_pct,
            "entry_max_width_enabled": self.params.entry_max_width_enabled,
            "entry_max_width_pct": self.params.entry_max_width_pct,
            "entry_max_width_usd": self.params.entry_max_width_usd,
            "entry_disaster_filter_enabled": self.params.entry_disaster_filter_enabled,
            "entry_disaster_score_threshold": self.params.entry_disaster_score_threshold,
            "entry_disaster_kline_count": self.params.entry_disaster_kline_count,
            "entry_disaster_width_expand": self.params.entry_disaster_width_expand,
            "entry_disaster_tp_distance_mult": self.params.entry_disaster_tp_distance_mult,
            "entry_disaster_expected_return": self.params.entry_disaster_expected_return,
            "entry_disaster_far_mid_ratio": self.params.entry_disaster_far_mid_ratio,
            "addon_max_width_enabled": self.params.addon_max_width_enabled,
            "addon_max_width_pct": self.params.addon_max_width_pct,
            "addon_max_width_usd": self.params.addon_max_width_usd,
            "addon_tp_improve_guard_enabled": self.params.addon_tp_improve_guard_enabled,
            "addon_tp_improve_expected_return": self.params.addon_tp_improve_expected_return,
            "addon_tp_improve_ratio": self.params.addon_tp_improve_ratio,
            "addon_tp_improve_min_usd": self.params.addon_tp_improve_min_usd,
            "min_entry_gap_usd": self.params.min_entry_gap_usd,
            "reprice_gap_usd": self.params.reprice_gap_usd,
            "first_batch_ratio": self.params.first_batch_ratio,
            "second_batch_dynamic_base_ratio": self.params.second_batch_dynamic_base_ratio,
            "second_batch_dynamic_min_ratio": self.params.second_batch_dynamic_min_ratio,
            "second_batch_dynamic_max_ratio": self.params.second_batch_dynamic_max_ratio,
            "second_batch_dynamic_full_gap_usd": self.params.second_batch_dynamic_full_gap_usd,
            "dynamic_base_ratio": self.params.dynamic_base_ratio,
            "dynamic_min_ratio": self.params.dynamic_min_ratio,
            "dynamic_max_ratio": self.params.dynamic_max_ratio,
            "max_total_entry_ratio": self.params.max_total_entry_ratio,
            "boll_width_base_price": self.params.boll_width_base_price,
            "boll_width_base_usd": self.params.boll_width_base_usd,
            "boll_width_floor_usd": self.params.boll_width_floor_usd,
            "boll_width_gap_mult": self.params.boll_width_gap_mult,
            "boll_width_tp_space_enabled": self.params.boll_width_tp_space_enabled,
            "boll_width_tp_space_mult": self.params.boll_width_tp_space_mult,
            "tp_target_margin_return": self.params.tp_target_margin_return,
            "min_head_liq_buffer_pct": self.params.min_head_liq_buffer_pct,
            "dynamic_tp_enabled": self.params.dynamic_tp_enabled,
            "dynamic_tp_arm_return": self.params.dynamic_tp_arm_return,
            "dynamic_tp_reprice_gap_usd": self.params.dynamic_tp_reprice_gap_usd,
            "boll_tp_compression_enabled": self.params.boll_tp_compression_enabled,
            "boll_tp_compression_min_return": self.params.boll_tp_compression_min_return,
            "boll_tp_compression_exit_offset_usd": self.params.boll_tp_compression_exit_offset_usd,
            "entry_extreme_gap_adjust_enabled": self.params.entry_extreme_gap_adjust_enabled,
            "entry_extreme_gap_base_pct": self.params.entry_extreme_gap_base_pct,
            "entry_extreme_gap_full_pct": self.params.entry_extreme_gap_full_pct,
            "entry_extreme_gap_max_mult": self.params.entry_extreme_gap_max_mult,
            "copy_fixed_loss_stop_enabled": self.params.copy_fixed_loss_stop_enabled,
            "copy_fixed_loss_stop_usdt": self.params.copy_fixed_loss_stop_usdt,
            "copy_fixed_loss_stop_ratio": self.params.copy_fixed_loss_stop_ratio,
            "fixed_loss_head_buffer_enabled": self.params.fixed_loss_head_buffer_enabled,
            "fixed_loss_head_buffer_pct": self.params.fixed_loss_head_buffer_pct,
            "disaster_stop_enabled": self.params.disaster_stop_enabled,
            "disaster_head_drop_pct": self.params.disaster_head_drop_pct,
            "disaster_loss_ratio": self.params.disaster_loss_ratio,
            "boll_mid_cost_stop_enabled": self.params.boll_mid_cost_stop_enabled,
            "boll_mid_cost_tp_return": self.params.boll_mid_cost_tp_return,
            "trend_risk_guard_enabled": self.params.trend_risk_guard_enabled,
            "trend_risk_guard_close_enabled": self.params.trend_risk_guard_close_enabled,
            "trend_risk_freeze_addon_enabled": self.params.trend_risk_freeze_addon_enabled,
            "trend_risk_score_threshold": self.params.trend_risk_score_threshold,
            "trend_risk_head_adverse_pct": self.params.trend_risk_head_adverse_pct,
            "trend_risk_kline_count": self.params.trend_risk_kline_count,
            "trend_risk_min_hold_min": self.params.trend_risk_min_hold_min,
            "trend_risk_slope_window_min": self.params.trend_risk_slope_window_min,
            "trend_risk_mid_slope_pct_per_hour": self.params.trend_risk_mid_slope_pct_per_hour,
            "trend_risk_edge_slope_pct_per_hour": self.params.trend_risk_edge_slope_pct_per_hour,
            "trend_risk_width_expand": self.params.trend_risk_width_expand,
            "addon_dynamic_gap_enabled": self.params.addon_dynamic_gap_enabled,
            "addon_dynamic_gap_max_usd": self.params.addon_dynamic_gap_max_usd,
            "addon_dynamic_gap_boll_start": self.params.addon_dynamic_gap_boll_start,
            "addon_dynamic_gap_boll_strong": self.params.addon_dynamic_gap_boll_strong,
            "addon_dynamic_gap_boll_max_mult": self.params.addon_dynamic_gap_boll_max_mult,
            "addon_dynamic_gap_head_start_pct": self.params.addon_dynamic_gap_head_start_pct,
            "addon_dynamic_gap_head_strong_pct": self.params.addon_dynamic_gap_head_strong_pct,
            "addon_dynamic_gap_head_max_mult": self.params.addon_dynamic_gap_head_max_mult,
            "addon_dynamic_gap_trend_klines": self.params.addon_dynamic_gap_trend_klines,
            "addon_dynamic_gap_trend_mult": self.params.addon_dynamic_gap_trend_mult,
            "entry_extreme_entries": self.entry_extreme_entries,
            "entry_extreme_avg_gap_pct": round(
                self.entry_extreme_gap_total / self.entry_extreme_entries * 100, 4
            ) if self.entry_extreme_entries else 0.0,
            "entry_extreme_max_gap_pct": round(self.entry_extreme_gap_max * 100, 4),
            "entry_extreme_avg_mult": round(
                self.entry_extreme_mult_total / self.entry_extreme_entries, 4
            ) if self.entry_extreme_entries else 1.0,
            "entry_extreme_max_mult_seen": round(self.entry_extreme_mult_max, 4),
            "final_total_equity": round(self.total_equity(), 4),
            "total_pnl": total_pnl,
            "return_pct": round((self.total_equity() / self.initial_total - 1) * 100, 4),
            "balanced_score": balanced_score,
            "risk_score": risk_score,
            "max_drawdown_pct": max_drawdown_pct,
            "min_liq_distance_pct": min_liq,
            "wipeout_risk": self.wipeout_risk,
            "end_unrealized": end_unrealized,
            "trades": len(self.trades),
            "entry_signals": self.signal_count,
            "win_rate_pct": round(len(wins) / len(pnls) * 100, 4) if pnls else 0.0,
            "avg_pnl": round(float(np.mean(pnls)) if pnls else 0.0, 4),
            "avg_batches": round(float(np.mean([trade.batches for trade in self.trades])) if self.trades else 0.0),
            "avg_hold_minutes": round(float(np.mean(durations)) if durations else 0.0, 2),
            "width_cancel": self.width_cancel,
            "inside_cancel": self.inside_cancel,
            "reprice_count": self.reprice_count,
            "same_k_block": self.same_k_block,
            "close_kline_block": self.close_kline_block,
            "dynamic_tp_activated": self.dynamic_tp_activated,
            "boll_tp_compression_activated": self.boll_tp_compression_activated,
            "blocked_width": self.blocked_width,
            "blocked_entry_max_width": self.blocked_entry_max_width,
            "blocked_entry_disaster": self.blocked_entry_disaster,
            "blocked_addon_max_width": self.blocked_addon_max_width,
            "blocked_gap": self.blocked_gap,
            "blocked_extreme": self.blocked_extreme,
            "blocked_addon_guard": self.blocked_addon_guard,
            "fixed_loss_head_buffer_block": self.fixed_loss_head_buffer_block,
            "tp_improve_block": self.tp_improve_block,
            "cross_copy_stop": self.cross_copy_stop,
            "fixed_loss_stop": self.fixed_loss_stop,
            "fixed_cycle_loss_stop": self.fixed_cycle_loss_stop,
            "liquidation_guard_fallback_stop": self.liquidation_guard_fallback_stop,
            "disaster_stop": self.disaster_stop,
            "boll_mid_cost_stop": self.boll_mid_cost_stop,
            "trend_risk_guard_signal": self.trend_risk_guard_signal,
            "trend_risk_guard_stop": self.trend_risk_guard_stop,
            "trend_risk_freeze_blocks": self.trend_risk_freeze_blocks,
            "trend_risk_freeze_cancels": self.trend_risk_freeze_cancels,
            "dynamic_gap_events": self.dynamic_gap_events,
            "dynamic_gap_avg_mult": round(
                self.dynamic_gap_mult_total / self.dynamic_gap_events, 4
            ) if self.dynamic_gap_events else 1.0,
            "dynamic_gap_max_mult": round(self.dynamic_gap_mult_max, 4),
            "skipped_entry_cap": self.skipped_entry_cap,
            "skipped_funds": self.skipped_funds,
            "profit_transferred": round(self.profit_transferred, 4),
            "loss_topup": round(self.loss_topup, 4),
        }


def parse_logs(log_dir: Path, sample_sec: int = 0) -> pd.DataFrame:
    """Parse live tick snapshots from strategy logs."""
    rows = []
    for path in sorted(log_dir.glob("boll_pin_*.log")):
        with path.open("r", encoding="utf-8", errors="ignore") as file:
            for line in file:
                match = TICK_RE.search(line)
                if not match:
                    continue
                rows.append(
                    {
                        "ts": pd.Timestamp(match.group("ts")),
                        "price": float(match.group("price")),
                        "lower_src": float(match.group("lower")),
                        "mid": float(match.group("mid")),
                        "upper_src": float(match.group("upper")),
                    }
                )
    if not rows:
        raise RuntimeError(f"娌℃湁浠?{log_dir} 瑙ｆ瀽鍒扮瓥鐣?tick 鏃ュ織")
    df = pd.DataFrame(rows).sort_values("ts").drop_duplicates("ts")
    if sample_sec and sample_sec > 0:
        df = (
            df.set_index("ts")
            .resample(f"{int(sample_sec)}s")
            .last()
            .dropna(subset=["price"])
            .reset_index()
        )
    indexed = df.set_index("ts")
    df["low24"] = indexed["price"].rolling("24h", min_periods=1).min().to_numpy()
    df["high24"] = indexed["price"].rolling("24h", min_periods=1).max().to_numpy()
    df["kline_ts"] = df["ts"].dt.floor("15min")
    return df.reset_index(drop=True)


def _log_cache_key(log_dir: Path, sample_sec: int) -> str:
    """Return a stable cache key for log files and sampling settings."""
    import hashlib

    digest = hashlib.sha1()
    digest.update(str(log_dir.resolve()).encode("utf-8", errors="ignore"))
    digest.update(f"|sample={sample_sec}|".encode())
    for path in sorted(log_dir.glob("boll_pin_*.log")):
        stat = path.stat()
        digest.update(path.name.encode("utf-8", errors="ignore"))
        digest.update(str(stat.st_size).encode())
        digest.update(str(int(stat.st_mtime_ns)).encode())
    return digest.hexdigest()[:16]


def load_or_parse_logs(log_dir: Path, sample_sec: int, cache_dir: Path | None, quiet: bool) -> pd.DataFrame:
    """Load parsed ticks from cache when possible, otherwise parse and cache."""
    if cache_dir is None:
        return parse_logs(log_dir, sample_sec=sample_sec)
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / f"ticks_{_log_cache_key(log_dir, sample_sec)}.pkl"
    if cache_path.exists():
        if not quiet:
            print(f"Loading parsed tick cache: {cache_path}", flush=True)
        return pd.read_pickle(cache_path)
    ticks = parse_logs(log_dir, sample_sec=sample_sec)
    ticks.to_pickle(cache_path)
    if not quiet:
        print(f"Saved parsed tick cache: {cache_path}", flush=True)
    return ticks


def parse_float_list(text: str) -> list[float]:
    """Parse comma-separated floats or start:end:step ranges."""
    text = text.strip()
    if ":" in text and "," not in text:
        start, end, step = (float(item.strip()) for item in text.split(":"))
        if step <= 0:
            raise ValueError("Range step must be positive")
        values = []
        current = start
        while current <= end + step / 2:
            values.append(round(current, 10))
            current += step
        return values
    return [float(item.strip()) for item in text.split(",") if item.strip()]


def parse_int_list(text: str) -> list[int]:
    """Parse comma-separated integers."""
    return [int(float(item.strip())) for item in text.split(",") if item.strip()]


def build_grid(args) -> list[Params]:
    """Build parameter combinations from CLI arguments."""
    value_lists = _param_value_lists(args)
    return [Params(*values) for values in itertools.product(*value_lists)]


def _param_value_lists(args) -> list[list[float] | list[int]]:
    """Return parsed parameter value lists in Params field order."""
    return [
        [SOURCE_BOLL_STD],
        [MIN_BOLL_WIDTH_USD],
        parse_float_list(args.min_boll_width_pct),
        parse_float_list(args.min_entry_gap_usd),
        [REPRICE_GAP_USD],
        [int(ENTRY_MAX_BOLL_WIDTH_FILTER_ENABLED)],
        parse_float_list(args.entry_max_width_pct),
        [ENTRY_MAX_BOLL_WIDTH_USD],
        [int(ENTRY_DISASTER_FILTER_ENABLED)],
        parse_int_list(args.entry_disaster_score_threshold),
        [ENTRY_DISASTER_KLINE_COUNT],
        [ENTRY_DISASTER_WIDTH_EXPAND],
        [ENTRY_DISASTER_TP_DISTANCE_MULT],
        [ENTRY_DISASTER_EXPECTED_RETURN],
        [ENTRY_DISASTER_FAR_MID_RATIO],
        [int(ADDON_MAX_BOLL_WIDTH_FILTER_ENABLED)],
        [ADDON_MAX_BOLL_WIDTH_PCT],
        [ADDON_MAX_BOLL_WIDTH_USD],
        [int(ADDON_TP_IMPROVE_GUARD_ENABLED)],
        [ADDON_TP_IMPROVE_EXPECTED_RETURN],
        [ADDON_TP_IMPROVE_RATIO],
        [ADDON_TP_IMPROVE_MIN_USD],
        parse_float_list(args.first_batch_ratio),
        parse_float_list(args.second_batch_dynamic_base_ratio),
        parse_float_list(args.second_batch_dynamic_min_ratio),
        parse_float_list(args.second_batch_dynamic_max_ratio),
        parse_float_list(args.second_batch_dynamic_full_gap_usd),
        parse_float_list(args.dynamic_base_ratio),
        parse_float_list(args.dynamic_min_ratio),
        parse_float_list(args.dynamic_max_ratio),
        parse_float_list(args.max_total_entry_ratio),
        [BOLL_WIDTH_BASE_PRICE],
        [BOLL_WIDTH_BASE_USD],
        [MIN_BOLL_WIDTH_FLOOR_USD],
        [BOLL_WIDTH_GAP_MULT],
        [int(BOLL_WIDTH_TP_SPACE_ENABLED)],
        [BOLL_WIDTH_TP_SPACE_MULT],
        parse_float_list(args.tp_target_margin_return),
        [int(LOW_BOLL_WIDTH_TP_ENABLED)],
        [LOW_BOLL_WIDTH_REF_PCT],
        [LOW_BOLL_WIDTH_MIN_TP_RETURN],
        [LOW_BOLL_WIDTH_MAX_TP_RETURN],
        [LOW_BOLL_WIDTH_TP_CAPTURE_RATIO],
        [LOW_BOLL_WIDTH_SIZE_MULT],
        [MIN_HEAD_LIQ_BUFFER_PCT],
        [int(DYNAMIC_TP_ENABLED)],
        parse_float_list(args.dynamic_tp_arm_return),
        [DYNAMIC_TP_REPRICE_GAP_USD],
        [int(BOLL_TP_COMPRESSION_ENABLED)],
        [BOLL_TP_COMPRESSION_MIN_RETURN],
        [BOLL_TP_COMPRESSION_EXIT_OFFSET_USD],
        [int(ENTRY_EXTREME_GAP_ADJUST_ENABLED)],
        [ENTRY_EXTREME_GAP_BASE_PCT],
        [ENTRY_EXTREME_GAP_FULL_PCT],
        [ENTRY_EXTREME_GAP_MAX_MULT],
        [int(COPY_FIXED_LOSS_STOP_ENABLED)],
        [COPY_FIXED_LOSS_STOP_USDT],
        [COPY_FIXED_LOSS_STOP_RATIO],
        [int(FIXED_LOSS_HEAD_BUFFER_ENABLED)],
        [FIXED_LOSS_HEAD_BUFFER_PCT],
        [int(DISASTER_STOP_ENABLED)],
        [DISASTER_HEAD_DROP_PCT],
        [DISASTER_LOSS_RATIO],
        parse_int_list(args.boll_mid_cost_stop_enabled),
        [BOLL_MID_COST_TP_RETURN],
        [int(TREND_RISK_GUARD_ENABLED)],
        [int(TREND_RISK_GUARD_CLOSE_ENABLED)],
        [int(TREND_RISK_FREEZE_ADDON_ENABLED)],
        [TREND_RISK_SCORE_THRESHOLD],
        [TREND_RISK_HEAD_ADVERSE_PCT],
        [TREND_RISK_KLINE_COUNT],
        [TREND_RISK_MIN_HOLD_MIN],
        [TREND_RISK_SLOPE_WINDOW_MIN],
        [TREND_RISK_MID_SLOPE_PCT_PER_HOUR],
        [TREND_RISK_EDGE_SLOPE_PCT_PER_HOUR],
        [TREND_RISK_WIDTH_EXPAND],
        [int(ADDON_DYNAMIC_GAP_ENABLED)],
        [ADDON_DYNAMIC_GAP_MAX_USD],
        [ADDON_DYNAMIC_GAP_BOLL_START],
        [ADDON_DYNAMIC_GAP_BOLL_STRONG],
        [ADDON_DYNAMIC_GAP_BOLL_MAX_MULT],
        [ADDON_DYNAMIC_GAP_HEAD_START_PCT],
        [ADDON_DYNAMIC_GAP_HEAD_STRONG_PCT],
        [ADDON_DYNAMIC_GAP_HEAD_MAX_MULT],
        [ADDON_DYNAMIC_GAP_TREND_KLINES],
        [ADDON_DYNAMIC_GAP_TREND_MULT],
    ]


def build_random_grid(args) -> list[Params]:
    """Build a deduplicated random sample from the configured parameter ranges."""
    value_lists = _param_value_lists(args)
    max_combinations = math.prod(len(values) for values in value_lists)
    target = min(max(1, int(args.random_trials)), max_combinations)
    rng = random.Random(args.random_seed)
    seen: set[tuple] = set()
    params: list[Params] = []
    attempts = 0
    while len(params) < target and attempts < target * 20:
        attempts += 1
        values = tuple(rng.choice(values) for values in value_lists)
        if values in seen:
            continue
        seen.add(values)
        params.append(Params(*values))
    if len(params) < target:
        for values in itertools.product(*value_lists):
            if values in seen:
                continue
            params.append(Params(*values))
            if len(params) >= target:
                break
    return params


def build_optuna_rows(
    ticks: pd.DataFrame,
    args,
    baseline_params: Params,
    initial_total: float,
) -> list[dict]:
    """Run a TPE/Optuna search and return replay reports."""
    try:
        import optuna
    except ImportError as exc:
        raise RuntimeError(
            "Optuna is not installed. Run `pip install -r requirements.txt` "
            "inside the project virtual environment, then retry."
        ) from exc

    value_lists = _param_value_lists(args)
    param_names = [item.name for item in fields(Params)]
    if len(param_names) != len(value_lists):
        raise RuntimeError("Params fields and optimizer value lists are out of sync")

    reports: list[dict] = []
    seen: set[Params] = set()

    def replay(params: Params) -> dict:
        report = LogReplay(ticks, params, initial_total).run().report()
        reports.append(report)
        seen.add(params)
        return report

    baseline_report = replay(baseline_params)
    optuna.logging.set_verbosity(optuna.logging.WARNING if args.quiet else optuna.logging.INFO)
    sampler = optuna.samplers.TPESampler(
        seed=int(args.random_seed),
        n_startup_trials=max(1, int(args.optuna_startup_trials)),
    )
    study = optuna.create_study(direction="maximize", sampler=sampler)

    def objective(trial) -> float:
        values = {}
        for name, candidates in zip(param_names, value_lists):
            if len(candidates) == 1:
                values[name] = candidates[0]
            else:
                values[name] = trial.suggest_categorical(name, candidates)
        params = Params(**values)
        if params in seen:
            return float(baseline_report["balanced_score"]) - 1e-9
        return float(replay(params)["balanced_score"])

    if not args.quiet:
        print(
            f"Starting Optuna TPE search for {args.optuna_trials} trials "
            f"(startup={args.optuna_startup_trials})...",
            flush=True,
        )
    study.optimize(
        objective,
        n_trials=max(1, int(args.optuna_trials)),
        show_progress_bar=not args.quiet,
    )
    return reports


def params_from_rows(rows: list[dict], top_n: int) -> list[Params]:
    """Return unique Params rebuilt from top report rows."""
    params: list[Params] = []
    seen: set[Params] = set()
    for row in rows[: max(1, top_n)]:
        param = params_from_report_row(row)
        if param in seen:
            continue
        seen.add(param)
        params.append(param)
    return params


def current_config_params(args) -> Params:
    """Return a baseline Params object from the current live config values."""
    return Params(
        boll_std=SOURCE_BOLL_STD,
        min_width_usd=MIN_BOLL_WIDTH_USD,
        min_width_pct=MIN_BOLL_WIDTH_PCT,
        min_entry_gap_usd=MIN_ENTRY_GAP_USD,
        reprice_gap_usd=REPRICE_GAP_USD,
        entry_max_width_enabled=int(ENTRY_MAX_BOLL_WIDTH_FILTER_ENABLED),
        entry_max_width_pct=ENTRY_MAX_BOLL_WIDTH_PCT,
        entry_max_width_usd=ENTRY_MAX_BOLL_WIDTH_USD,
        entry_disaster_filter_enabled=int(ENTRY_DISASTER_FILTER_ENABLED),
        entry_disaster_score_threshold=ENTRY_DISASTER_SCORE_THRESHOLD,
        entry_disaster_kline_count=ENTRY_DISASTER_KLINE_COUNT,
        entry_disaster_width_expand=ENTRY_DISASTER_WIDTH_EXPAND,
        entry_disaster_tp_distance_mult=ENTRY_DISASTER_TP_DISTANCE_MULT,
        entry_disaster_expected_return=ENTRY_DISASTER_EXPECTED_RETURN,
        entry_disaster_far_mid_ratio=ENTRY_DISASTER_FAR_MID_RATIO,
        addon_max_width_enabled=int(ADDON_MAX_BOLL_WIDTH_FILTER_ENABLED),
        addon_max_width_pct=ADDON_MAX_BOLL_WIDTH_PCT,
        addon_max_width_usd=ADDON_MAX_BOLL_WIDTH_USD,
        addon_tp_improve_guard_enabled=int(ADDON_TP_IMPROVE_GUARD_ENABLED),
        addon_tp_improve_expected_return=ADDON_TP_IMPROVE_EXPECTED_RETURN,
        addon_tp_improve_ratio=ADDON_TP_IMPROVE_RATIO,
        addon_tp_improve_min_usd=ADDON_TP_IMPROVE_MIN_USD,
        first_batch_ratio=FIRST_BATCH_RATIO,
        second_batch_dynamic_base_ratio=SECOND_BATCH_DYNAMIC_BASE_RATIO,
        second_batch_dynamic_min_ratio=SECOND_BATCH_DYNAMIC_MIN_RATIO,
        second_batch_dynamic_max_ratio=SECOND_BATCH_DYNAMIC_MAX_RATIO,
        second_batch_dynamic_full_gap_usd=SECOND_BATCH_DYNAMIC_FULL_GAP_USD,
        dynamic_base_ratio=DYNAMIC_BASE_ENTRY_RATIO,
        dynamic_min_ratio=DYNAMIC_MIN_ENTRY_RATIO,
        dynamic_max_ratio=DYNAMIC_MAX_ENTRY_RATIO,
        max_total_entry_ratio=MAX_TOTAL_ENTRY_RATIO,
        boll_width_base_price=BOLL_WIDTH_BASE_PRICE,
        boll_width_base_usd=BOLL_WIDTH_BASE_USD,
        boll_width_floor_usd=MIN_BOLL_WIDTH_FLOOR_USD,
        boll_width_gap_mult=BOLL_WIDTH_GAP_MULT,
        boll_width_tp_space_enabled=int(BOLL_WIDTH_TP_SPACE_ENABLED),
        boll_width_tp_space_mult=BOLL_WIDTH_TP_SPACE_MULT,
        tp_target_margin_return=TP_TARGET_MARGIN_RETURN,
        low_boll_width_tp_enabled=int(LOW_BOLL_WIDTH_TP_ENABLED),
        low_boll_width_ref_pct=LOW_BOLL_WIDTH_REF_PCT,
        low_boll_width_min_tp_return=LOW_BOLL_WIDTH_MIN_TP_RETURN,
        low_boll_width_max_tp_return=LOW_BOLL_WIDTH_MAX_TP_RETURN,
        low_boll_width_tp_capture_ratio=LOW_BOLL_WIDTH_TP_CAPTURE_RATIO,
        low_boll_width_size_mult=LOW_BOLL_WIDTH_SIZE_MULT,
        min_head_liq_buffer_pct=MIN_HEAD_LIQ_BUFFER_PCT,
        dynamic_tp_enabled=int(DYNAMIC_TP_ENABLED),
        dynamic_tp_arm_return=DYNAMIC_TP_ARM_RETURN,
        dynamic_tp_reprice_gap_usd=DYNAMIC_TP_REPRICE_GAP_USD,
        boll_tp_compression_enabled=int(BOLL_TP_COMPRESSION_ENABLED),
        boll_tp_compression_min_return=BOLL_TP_COMPRESSION_MIN_RETURN,
        boll_tp_compression_exit_offset_usd=BOLL_TP_COMPRESSION_EXIT_OFFSET_USD,
        entry_extreme_gap_adjust_enabled=int(ENTRY_EXTREME_GAP_ADJUST_ENABLED),
        entry_extreme_gap_base_pct=ENTRY_EXTREME_GAP_BASE_PCT,
        entry_extreme_gap_full_pct=ENTRY_EXTREME_GAP_FULL_PCT,
        entry_extreme_gap_max_mult=ENTRY_EXTREME_GAP_MAX_MULT,
        copy_fixed_loss_stop_enabled=int(COPY_FIXED_LOSS_STOP_ENABLED),
        copy_fixed_loss_stop_usdt=COPY_FIXED_LOSS_STOP_USDT,
        copy_fixed_loss_stop_ratio=COPY_FIXED_LOSS_STOP_RATIO,
        fixed_loss_head_buffer_enabled=int(FIXED_LOSS_HEAD_BUFFER_ENABLED),
        fixed_loss_head_buffer_pct=FIXED_LOSS_HEAD_BUFFER_PCT,
        disaster_stop_enabled=int(DISASTER_STOP_ENABLED),
        disaster_head_drop_pct=DISASTER_HEAD_DROP_PCT,
        disaster_loss_ratio=DISASTER_LOSS_RATIO,
        boll_mid_cost_stop_enabled=int(BOLL_MID_COST_STOP_ENABLED),
        boll_mid_cost_tp_return=BOLL_MID_COST_TP_RETURN,
        trend_risk_guard_enabled=int(TREND_RISK_GUARD_ENABLED),
        trend_risk_guard_close_enabled=int(TREND_RISK_GUARD_CLOSE_ENABLED),
        trend_risk_freeze_addon_enabled=int(TREND_RISK_FREEZE_ADDON_ENABLED),
        trend_risk_score_threshold=TREND_RISK_SCORE_THRESHOLD,
        trend_risk_head_adverse_pct=TREND_RISK_HEAD_ADVERSE_PCT,
        trend_risk_kline_count=TREND_RISK_KLINE_COUNT,
        trend_risk_min_hold_min=TREND_RISK_MIN_HOLD_MIN,
        trend_risk_slope_window_min=TREND_RISK_SLOPE_WINDOW_MIN,
        trend_risk_mid_slope_pct_per_hour=TREND_RISK_MID_SLOPE_PCT_PER_HOUR,
        trend_risk_edge_slope_pct_per_hour=TREND_RISK_EDGE_SLOPE_PCT_PER_HOUR,
        trend_risk_width_expand=TREND_RISK_WIDTH_EXPAND,
        addon_dynamic_gap_enabled=int(ADDON_DYNAMIC_GAP_ENABLED),
        addon_dynamic_gap_max_usd=ADDON_DYNAMIC_GAP_MAX_USD,
        addon_dynamic_gap_boll_start=ADDON_DYNAMIC_GAP_BOLL_START,
        addon_dynamic_gap_boll_strong=ADDON_DYNAMIC_GAP_BOLL_STRONG,
        addon_dynamic_gap_boll_max_mult=ADDON_DYNAMIC_GAP_BOLL_MAX_MULT,
        addon_dynamic_gap_head_start_pct=ADDON_DYNAMIC_GAP_HEAD_START_PCT,
        addon_dynamic_gap_head_strong_pct=ADDON_DYNAMIC_GAP_HEAD_STRONG_PCT,
        addon_dynamic_gap_head_max_mult=ADDON_DYNAMIC_GAP_HEAD_MAX_MULT,
        addon_dynamic_gap_trend_klines=ADDON_DYNAMIC_GAP_TREND_KLINES,
        addon_dynamic_gap_trend_mult=ADDON_DYNAMIC_GAP_TREND_MULT,
    )


def ensure_baseline_in_grid(grid: list[Params], baseline: Params) -> list[Params]:
    """Return grid with the current-config baseline included exactly once."""
    if baseline in grid:
        return grid
    return [baseline, *grid]


def _init_worker(ticks: pd.DataFrame) -> None:
    """Store parsed ticks once per worker process."""
    global _WORKER_TICKS
    _WORKER_TICKS = ticks


def _run_replay_worker(payload: tuple[Params, float]) -> dict:
    """Run one replay inside a worker process."""
    params, initial_total = payload
    if _WORKER_TICKS is None:
        raise RuntimeError("Worker ticks are not initialized")
    return LogReplay(_WORKER_TICKS, params, initial_total).run().report()


def _resolve_worker_count(requested: int, total: int) -> int:
    """Return an efficient worker count for the parameter grid size."""
    if total <= 1:
        return 1
    if requested > 0:
        return max(1, min(requested, total))
    cpu_count = os.cpu_count() or 1
    return max(1, min(total, max(cpu_count - 1, 1)))


def run_replays(ticks: pd.DataFrame, grid: list[Params], initial_total: float, workers: int, quiet: bool) -> list[dict]:
    """Run all parameter replays, using multiple processes when requested."""
    total = len(grid)
    worker_count = _resolve_worker_count(workers, total)
    progress_step = max(1, total // 20)
    rows: list[dict] = []
    if not quiet:
        mode = "single process" if worker_count == 1 else f"{worker_count} worker processes"
        print(f"Starting replay for {total} parameter sets using {mode}...", flush=True)
    if worker_count == 1:
        for idx, params in enumerate(grid, start=1):
            rows.append(LogReplay(ticks, params, initial_total).run().report())
            if not quiet and (idx == 1 or idx == total or idx % progress_step == 0):
                pct = idx / total * 100
                print(f"Progress {idx}/{total} ({pct:.1f}%)", flush=True)
        return rows

    payloads = [(params, initial_total) for params in grid]
    with ProcessPoolExecutor(max_workers=worker_count, initializer=_init_worker, initargs=(ticks,)) as executor:
        futures = [executor.submit(_run_replay_worker, payload) for payload in payloads]
        for idx, future in enumerate(as_completed(futures), start=1):
            rows.append(future.result())
            if not quiet and (idx == 1 or idx == total or idx % progress_step == 0):
                pct = idx / total * 100
                print(f"Progress {idx}/{total} ({pct:.1f}%)", flush=True)
    return rows


def params_from_report_row(row: dict) -> Params:
    """Rebuild Params from a report row."""
    return Params(
        boll_std=row["boll_std"],
        min_width_usd=row["min_width_usd"],
        min_width_pct=row["min_width_pct"],
        min_entry_gap_usd=row["min_entry_gap_usd"],
        reprice_gap_usd=row["reprice_gap_usd"],
        entry_max_width_enabled=row["entry_max_width_enabled"],
        entry_max_width_pct=row["entry_max_width_pct"],
        entry_max_width_usd=row["entry_max_width_usd"],
        entry_disaster_filter_enabled=row.get(
            "entry_disaster_filter_enabled", int(ENTRY_DISASTER_FILTER_ENABLED)
        ),
        entry_disaster_score_threshold=row.get(
            "entry_disaster_score_threshold", ENTRY_DISASTER_SCORE_THRESHOLD
        ),
        entry_disaster_kline_count=row.get("entry_disaster_kline_count", ENTRY_DISASTER_KLINE_COUNT),
        entry_disaster_width_expand=row.get(
            "entry_disaster_width_expand", ENTRY_DISASTER_WIDTH_EXPAND
        ),
        entry_disaster_tp_distance_mult=row.get(
            "entry_disaster_tp_distance_mult", ENTRY_DISASTER_TP_DISTANCE_MULT
        ),
        entry_disaster_expected_return=row.get(
            "entry_disaster_expected_return", ENTRY_DISASTER_EXPECTED_RETURN
        ),
        entry_disaster_far_mid_ratio=row.get(
            "entry_disaster_far_mid_ratio", ENTRY_DISASTER_FAR_MID_RATIO
        ),
        addon_max_width_enabled=row.get("addon_max_width_enabled", int(ADDON_MAX_BOLL_WIDTH_FILTER_ENABLED)),
        addon_max_width_pct=row.get("addon_max_width_pct", ADDON_MAX_BOLL_WIDTH_PCT),
        addon_max_width_usd=row.get("addon_max_width_usd", ADDON_MAX_BOLL_WIDTH_USD),
        first_batch_ratio=row["first_batch_ratio"],
        second_batch_dynamic_base_ratio=row.get("second_batch_dynamic_base_ratio", SECOND_BATCH_DYNAMIC_BASE_RATIO),
        second_batch_dynamic_min_ratio=row.get("second_batch_dynamic_min_ratio", SECOND_BATCH_DYNAMIC_MIN_RATIO),
        second_batch_dynamic_max_ratio=row.get("second_batch_dynamic_max_ratio", SECOND_BATCH_DYNAMIC_MAX_RATIO),
        second_batch_dynamic_full_gap_usd=row.get("second_batch_dynamic_full_gap_usd", SECOND_BATCH_DYNAMIC_FULL_GAP_USD),
        dynamic_base_ratio=row["dynamic_base_ratio"],
        dynamic_min_ratio=row["dynamic_min_ratio"],
        dynamic_max_ratio=row["dynamic_max_ratio"],
        max_total_entry_ratio=row["max_total_entry_ratio"],
        boll_width_base_price=row["boll_width_base_price"],
        boll_width_base_usd=row["boll_width_base_usd"],
        boll_width_floor_usd=row["boll_width_floor_usd"],
        boll_width_gap_mult=row["boll_width_gap_mult"],
        boll_width_tp_space_enabled=row.get("boll_width_tp_space_enabled", int(BOLL_WIDTH_TP_SPACE_ENABLED)),
        boll_width_tp_space_mult=row.get("boll_width_tp_space_mult", BOLL_WIDTH_TP_SPACE_MULT),
        tp_target_margin_return=row["tp_target_margin_return"],
        low_boll_width_tp_enabled=row.get("low_boll_width_tp_enabled", int(LOW_BOLL_WIDTH_TP_ENABLED)),
        low_boll_width_ref_pct=row.get("low_boll_width_ref_pct", LOW_BOLL_WIDTH_REF_PCT),
        low_boll_width_min_tp_return=row.get("low_boll_width_min_tp_return", LOW_BOLL_WIDTH_MIN_TP_RETURN),
        low_boll_width_max_tp_return=row.get("low_boll_width_max_tp_return", LOW_BOLL_WIDTH_MAX_TP_RETURN),
        low_boll_width_tp_capture_ratio=row.get("low_boll_width_tp_capture_ratio", LOW_BOLL_WIDTH_TP_CAPTURE_RATIO),
        low_boll_width_size_mult=row.get("low_boll_width_size_mult", LOW_BOLL_WIDTH_SIZE_MULT),
        min_head_liq_buffer_pct=row["min_head_liq_buffer_pct"],
        dynamic_tp_enabled=row["dynamic_tp_enabled"],
        dynamic_tp_arm_return=row["dynamic_tp_arm_return"],
        dynamic_tp_reprice_gap_usd=row["dynamic_tp_reprice_gap_usd"],
        boll_tp_compression_enabled=row["boll_tp_compression_enabled"],
        boll_tp_compression_min_return=row["boll_tp_compression_min_return"],
        boll_tp_compression_exit_offset_usd=row["boll_tp_compression_exit_offset_usd"],
        entry_extreme_gap_adjust_enabled=row["entry_extreme_gap_adjust_enabled"],
        entry_extreme_gap_base_pct=row["entry_extreme_gap_base_pct"],
        entry_extreme_gap_full_pct=row["entry_extreme_gap_full_pct"],
        entry_extreme_gap_max_mult=row["entry_extreme_gap_max_mult"],
        copy_fixed_loss_stop_enabled=row["copy_fixed_loss_stop_enabled"],
        copy_fixed_loss_stop_usdt=row["copy_fixed_loss_stop_usdt"],
        copy_fixed_loss_stop_ratio=row["copy_fixed_loss_stop_ratio"],
        fixed_loss_head_buffer_enabled=row.get("fixed_loss_head_buffer_enabled", int(FIXED_LOSS_HEAD_BUFFER_ENABLED)),
        fixed_loss_head_buffer_pct=row.get("fixed_loss_head_buffer_pct", FIXED_LOSS_HEAD_BUFFER_PCT),
        disaster_stop_enabled=row["disaster_stop_enabled"],
        disaster_head_drop_pct=row["disaster_head_drop_pct"],
        disaster_loss_ratio=row["disaster_loss_ratio"],
        boll_mid_cost_stop_enabled=row.get("boll_mid_cost_stop_enabled", int(BOLL_MID_COST_STOP_ENABLED)),
        boll_mid_cost_tp_return=row.get("boll_mid_cost_tp_return", BOLL_MID_COST_TP_RETURN),
        trend_risk_guard_enabled=row.get("trend_risk_guard_enabled", int(TREND_RISK_GUARD_ENABLED)),
        trend_risk_guard_close_enabled=row.get(
            "trend_risk_guard_close_enabled", int(TREND_RISK_GUARD_CLOSE_ENABLED)
        ),
        trend_risk_freeze_addon_enabled=row.get(
            "trend_risk_freeze_addon_enabled", int(TREND_RISK_FREEZE_ADDON_ENABLED)
        ),
        trend_risk_score_threshold=row.get("trend_risk_score_threshold", TREND_RISK_SCORE_THRESHOLD),
        trend_risk_head_adverse_pct=row.get("trend_risk_head_adverse_pct", TREND_RISK_HEAD_ADVERSE_PCT),
        trend_risk_kline_count=row.get("trend_risk_kline_count", TREND_RISK_KLINE_COUNT),
        trend_risk_min_hold_min=row.get("trend_risk_min_hold_min", TREND_RISK_MIN_HOLD_MIN),
        trend_risk_slope_window_min=row.get("trend_risk_slope_window_min", TREND_RISK_SLOPE_WINDOW_MIN),
        trend_risk_mid_slope_pct_per_hour=row.get(
            "trend_risk_mid_slope_pct_per_hour", TREND_RISK_MID_SLOPE_PCT_PER_HOUR
        ),
        trend_risk_edge_slope_pct_per_hour=row.get(
            "trend_risk_edge_slope_pct_per_hour", TREND_RISK_EDGE_SLOPE_PCT_PER_HOUR
        ),
        trend_risk_width_expand=row.get("trend_risk_width_expand", TREND_RISK_WIDTH_EXPAND),
        addon_dynamic_gap_enabled=row["addon_dynamic_gap_enabled"],
        addon_dynamic_gap_max_usd=row["addon_dynamic_gap_max_usd"],
        addon_dynamic_gap_boll_start=row["addon_dynamic_gap_boll_start"],
        addon_dynamic_gap_boll_strong=row["addon_dynamic_gap_boll_strong"],
        addon_dynamic_gap_boll_max_mult=row["addon_dynamic_gap_boll_max_mult"],
        addon_dynamic_gap_head_start_pct=row["addon_dynamic_gap_head_start_pct"],
        addon_dynamic_gap_head_strong_pct=row["addon_dynamic_gap_head_strong_pct"],
        addon_dynamic_gap_head_max_mult=row["addon_dynamic_gap_head_max_mult"],
        addon_dynamic_gap_trend_klines=row["addon_dynamic_gap_trend_klines"],
        addon_dynamic_gap_trend_mult=row["addon_dynamic_gap_trend_mult"],
    )


def run_walk_forward(
    ticks: pd.DataFrame,
    rows: list[dict],
    initial_total: float,
    ratio: float,
    top_n: int,
) -> list[dict]:
    """Validate top-ranked rows on earlier and later log segments."""
    if ratio <= 0 or ratio >= 1 or len(ticks) < 100:
        return []
    split_idx = int(len(ticks) * ratio)
    train = ticks.iloc[:split_idx].reset_index(drop=True)
    validate = ticks.iloc[split_idx:].reset_index(drop=True)
    if train.empty or validate.empty:
        return []

    summaries = []
    for rank, row in enumerate(rows[: max(1, top_n)], start=1):
        params = params_from_report_row(row)
        train_row = LogReplay(train, params, initial_total).run().report()
        val_row = LogReplay(validate, params, initial_total).run().report()
        robustness_score = round(
            min(train_row["balanced_score"], val_row["balanced_score"])
            - abs(train_row["total_pnl"] - val_row["total_pnl"]) * 0.1,
            4,
        )
        summaries.append(
            {
                "rank": rank,
                "robustness_score": robustness_score,
                "min_width_pct": row["min_width_pct"],
                "entry_max_width_pct": row["entry_max_width_pct"],
                "min_entry_gap_usd": row["min_entry_gap_usd"],
                "boll_width_gap_mult": row["boll_width_gap_mult"],
                "boll_width_tp_space_mult": row["boll_width_tp_space_mult"],
                "first_batch_ratio": row["first_batch_ratio"],
                "max_total_entry_ratio": row["max_total_entry_ratio"],
                "head_second_dynamic": (
                    f"{row['first_batch_ratio']} | "
                    f"{row['second_batch_dynamic_base_ratio']}/"
                    f"{row['second_batch_dynamic_min_ratio']}/"
                    f"{row['second_batch_dynamic_max_ratio']}/"
                    f"{row['second_batch_dynamic_full_gap_usd']}"
                ),
                "dynamic_ratios": f"{row['dynamic_base_ratio']}/{row['dynamic_min_ratio']}/{row['dynamic_max_ratio']}",
                "train_score": train_row["balanced_score"],
                "train_pnl": train_row["total_pnl"],
                "train_trades": train_row["trades"],
                "train_dd": train_row["max_drawdown_pct"],
                "val_score": val_row["balanced_score"],
                "val_pnl": val_row["total_pnl"],
                "val_trades": val_row["trades"],
                "val_dd": val_row["max_drawdown_pct"],
                "val_dstop": val_row["disaster_stop"],
                "val_wipeout": val_row["wipeout_risk"],
            }
        )
    return sorted(summaries, key=lambda item: item["robustness_score"], reverse=True)


def _metric_delta(best: dict, baseline: dict, key: str) -> float:
    """Return best minus baseline for a numeric report metric."""
    return round(float(best.get(key, 0.0)) - float(baseline.get(key, 0.0)), 4)


def _markdown_param_rows(rows: list[dict], limit: int = 10) -> list[str]:
    """Return compact Markdown rows for parameter rankings."""
    lines: list[str] = []
    for idx, row in enumerate(rows[:limit], start=1):
        lines.append(
            f"|{idx}|{row['balanced_score']}|{row['risk_score']}|{row['total_pnl']}|"
            f"{row['max_drawdown_pct']}|{row['min_liq_distance_pct']}|{row['wipeout_risk']}|"
            f"{row['min_width_pct']}|{row['entry_max_width_pct']}|{row['min_entry_gap_usd']}|"
            f"{row['first_batch_ratio']}/"
            f"{row['max_total_entry_ratio']}|"
            f"{row['second_batch_dynamic_base_ratio']}/"
            f"{row['second_batch_dynamic_min_ratio']}/"
            f"{row['second_batch_dynamic_max_ratio']}/"
            f"{row['second_batch_dynamic_full_gap_usd']}|"
            f"{row['dynamic_base_ratio']}/{row['dynamic_min_ratio']}/{row['dynamic_max_ratio']}|"
            f"{row['tp_target_margin_return']}|"
            f"{row['dynamic_tp_arm_return']}|"
            f"{row['boll_mid_cost_stop_enabled']}/{row.get('boll_mid_cost_tp_return', BOLL_MID_COST_TP_RETURN)}|"
            f"{row['trades']}|{row['disaster_stop']}|{row['fixed_loss_stop']}|"
            f"{row['boll_mid_cost_stop']}|"
        )
    return lines


def write_markdown_report(
    path: Path,
    rows: list[dict],
    ticks: pd.DataFrame,
    baseline_row: dict,
    walk_forward_rows: list[dict] | None = None,
) -> None:
    """Write a concise Markdown report."""
    best = rows[0]
    lines = [
        "# Log Parameter Report",
        "",
        f"- Data range: {ticks['ts'].min()} -> {ticks['ts'].max()}",
        f"- Ticks: {len(ticks)}",
        "- Mode: manual offline replay",
        f"- Rolling compound: enabled={ROLLING_COMPOUND_ENABLED}",
        f"- Trading account target: {TRADING_ACCOUNT_TARGET:.2f} USDT",
        (
            f"- Cross copy protection: enabled={CROSS_COPY_PROTECT_ENABLED}, "
            f"protected={CROSS_COPY_PROTECT_EQUITY_USDT:.2f} USDT"
        ),
        "- Sizing model: live cross-copy sizing, max total entry ratio cap, second-batch dynamic sizing, and add-on guards",
        "",
        "## Best Balanced Parameters",
        "",
        (
            f"`MIN_BOLL_WIDTH_PCT={best['min_width_pct']}`, "
            f"`ENTRY_MAX_BOLL_WIDTH={bool(best['entry_max_width_enabled'])}/"
            f"{best['entry_max_width_pct']}/{best['entry_max_width_usd']}`, "
            f"`ENTRY_DISASTER_SCORE_THRESHOLD={best['entry_disaster_score_threshold']}`, "
            f"`MIN_ENTRY_GAP_USD={best['min_entry_gap_usd']}`, "
            f"`FIRST_BATCH_RATIO={best['first_batch_ratio']}`, "
            f"`MAX_TOTAL_ENTRY_RATIO={best['max_total_entry_ratio']}`, "
            f"`SECOND_BATCH_DYNAMIC="
            f"{best['second_batch_dynamic_base_ratio']}/"
            f"{best['second_batch_dynamic_min_ratio']}/"
            f"{best['second_batch_dynamic_max_ratio']}/"
            f"{best['second_batch_dynamic_full_gap_usd']}`, "
            f"`DYNAMIC_BASE_ENTRY_RATIO={best['dynamic_base_ratio']}`, "
            f"`DYNAMIC_MIN_ENTRY_RATIO={best['dynamic_min_ratio']}`, "
            f"`DYNAMIC_MAX_ENTRY_RATIO={best['dynamic_max_ratio']}`, "
            f"`TP_TARGET_MARGIN_RETURN={best['tp_target_margin_return']}`, "
            f"`DYNAMIC_TP_ARM_RETURN={best['dynamic_tp_arm_return']}`, "
            f"`BOLL_MID_COST={bool(best['boll_mid_cost_stop_enabled'])}/{best.get('boll_mid_cost_tp_return', BOLL_MID_COST_TP_RETURN)}`"
        ),
        "",
        (
            f"Score `{best['balanced_score']}`, PnL `{best['total_pnl']}` USDT, trades `{best['trades']}`, "
            f"win rate `{best['win_rate_pct']}%`, max drawdown `{best['max_drawdown_pct']}%`, "
            f"min liquidation distance `{best['min_liq_distance_pct']}%`, "
            f"wipeout risk `{best['wipeout_risk']}`, avg hold `{best['avg_hold_minutes']}` minutes."
        ),
        "",
        "## Current Config Baseline",
        "",
        (
            f"`MIN_BOLL_WIDTH_PCT={baseline_row['min_width_pct']}`, "
            f"`ENTRY_MAX={bool(baseline_row['entry_max_width_enabled'])}/"
            f"{baseline_row['entry_max_width_pct']}/{baseline_row['entry_max_width_usd']}`, "
            f"`ENTRY_DISASTER_SCORE_THRESHOLD={baseline_row['entry_disaster_score_threshold']}`, "
            f"`MIN_ENTRY_GAP_USD={baseline_row['min_entry_gap_usd']}`, "
            f"`HEAD={baseline_row['first_batch_ratio']}`, "
            f"`MAX_TOTAL={baseline_row['max_total_entry_ratio']}`, "
            f"`SECOND_DYNAMIC="
            f"{baseline_row['second_batch_dynamic_base_ratio']}/"
            f"{baseline_row['second_batch_dynamic_min_ratio']}/"
            f"{baseline_row['second_batch_dynamic_max_ratio']}/"
            f"{baseline_row['second_batch_dynamic_full_gap_usd']}`, "
            f"`DYNAMIC={baseline_row['dynamic_base_ratio']}/{baseline_row['dynamic_min_ratio']}/{baseline_row['dynamic_max_ratio']}`"
            f", `TP={baseline_row['tp_target_margin_return']}`"
            f", `DYNAMIC_TP_ARM={baseline_row['dynamic_tp_arm_return']}`"
            f", `BOLL_MID_COST={bool(baseline_row['boll_mid_cost_stop_enabled'])}/{baseline_row.get('boll_mid_cost_tp_return', BOLL_MID_COST_TP_RETURN)}`"
        ),
        "",
        (
            f"Baseline score `{baseline_row['balanced_score']}`, PnL `{baseline_row['total_pnl']}` USDT, "
            f"trades `{baseline_row['trades']}`, win rate `{baseline_row['win_rate_pct']}%`, "
            f"drawdown `{baseline_row['max_drawdown_pct']}%`, "
            f"min liquidation distance `{baseline_row['min_liq_distance_pct']}%`, "
            f"DStop `{baseline_row['disaster_stop']}`, "
            f"TrendRisk signal `{baseline_row['trend_risk_guard_signal']}`, "
            f"TrendRisk stop `{baseline_row['trend_risk_guard_stop']}`, "
            f"FStop `{baseline_row['fixed_loss_stop']}`."
        ),
        "",
        "## Best Versus Current",
        "",
        "|Metric|Current|Best|Delta|",
        "|-|-:|-:|-:|",
        f"|Score|{baseline_row['balanced_score']}|{best['balanced_score']}|{_metric_delta(best, baseline_row, 'balanced_score')}|",
        f"|PnL USDT|{baseline_row['total_pnl']}|{best['total_pnl']}|{_metric_delta(best, baseline_row, 'total_pnl')}|",
        f"|Trades|{baseline_row['trades']}|{best['trades']}|{_metric_delta(best, baseline_row, 'trades')}|",
        f"|Win Rate %|{baseline_row['win_rate_pct']}|{best['win_rate_pct']}|{_metric_delta(best, baseline_row, 'win_rate_pct')}|",
        f"|Drawdown %|{baseline_row['max_drawdown_pct']}|{best['max_drawdown_pct']}|{_metric_delta(best, baseline_row, 'max_drawdown_pct')}|",
        f"|Min Liq %|{baseline_row['min_liq_distance_pct']}|{best['min_liq_distance_pct']}|{_metric_delta(best, baseline_row, 'min_liq_distance_pct')}|",
        f"|DStop|{baseline_row['disaster_stop']}|{best['disaster_stop']}|{_metric_delta(best, baseline_row, 'disaster_stop')}|",
        f"|TrendRisk Signal|{baseline_row['trend_risk_guard_signal']}|{best['trend_risk_guard_signal']}|{_metric_delta(best, baseline_row, 'trend_risk_guard_signal')}|",
        f"|TrendRisk Stop|{baseline_row['trend_risk_guard_stop']}|{best['trend_risk_guard_stop']}|{_metric_delta(best, baseline_row, 'trend_risk_guard_stop')}|",
        f"|FStop|{baseline_row['fixed_loss_stop']}|{best['fixed_loss_stop']}|{_metric_delta(best, baseline_row, 'fixed_loss_stop')}|",
        "",
        "## Top 20",
        "",
        "|#|Score|Risk|PnL|DD|MinLiq|Wipeout|MinWidth|EntryMax|EntryGap|Head/Cap|SecondDyn|LaterDyn|TP|DynTP|BollMidStop|Trades|DStop|FStop|BMidStop|",
        "|-:|-:|-:|-:|-:|-:|-|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|",
    ]
    lines.extend(_markdown_param_rows(rows, 20))
    profit_rows = sorted(rows, key=lambda row: (row["total_pnl"], row["min_liq_distance_pct"]), reverse=True)
    risk_rows = sorted(
        [row for row in rows if row["total_pnl"] > 0],
        key=lambda row: (row["risk_score"], row["total_pnl"]),
        reverse=True,
    )
    lines.extend(
        [
            "",
            "## Highest Profit Top 10",
            "",
            "|#|Score|Risk|PnL|DD|MinLiq|Wipeout|MinWidth|EntryMax|EntryGap|Head/Cap|SecondDyn|LaterDyn|TP|DynTP|BollMidStop|Trades|DStop|FStop|BMidStop|",
            "|-:|-:|-:|-:|-:|-:|-|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|",
            *_markdown_param_rows(profit_rows, 10),
            "",
            "## Safest Positive-PnL Top 10",
            "",
            "|#|Score|Risk|PnL|DD|MinLiq|Wipeout|MinWidth|EntryMax|EntryGap|Head/Cap|SecondDyn|LaterDyn|TP|DynTP|BollMidStop|Trades|DStop|FStop|BMidStop|",
            "|-:|-:|-:|-:|-:|-:|-|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|",
            *_markdown_param_rows(risk_rows, 10),
        ]
    )
    if walk_forward_rows:
        lines.extend(
            [
                "## Walk-Forward Validation",
                "",
                "Top-ranked full-sample parameters are replayed on the earlier training segment and later validation segment.",
                "",
                "|Rank|Robust|EntryMax|Entry Gap|TPSpace|Head/SecondDyn|Dyn Base/Min/Max|Train PnL|Train DD|Train Trades|Val PnL|Val DD|Val Trades|Val DStop|Val Wipeout|",
                "|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-|",
            ]
        )
        for row in walk_forward_rows:
            lines.append(
                f"|{row['rank']}|{row['robustness_score']}|{row['entry_max_width_pct']}|"
                f"{row['min_entry_gap_usd']}|{row['boll_width_tp_space_mult']}|"
                    f"{row['head_second_dynamic']}|{row['dynamic_ratios']}|"
                f"{row['train_pnl']}|{row['train_dd']}|{row['train_trades']}|"
                f"{row['val_pnl']}|{row['val_dd']}|{row['val_trades']}|"
                f"{row['val_dstop']}|{row['val_wipeout']}|"
            )
        lines.append("")
    lines.extend(
        [
            "## Notes",
            "",
            "- This replay uses logged mark price and Bollinger snapshots, not order-book level fills.",
            "- It follows the current live sizing mode. In rolling compound mode, sizing equity is live account equity minus protected equity when cross-copy protection is enabled.",
            "- It includes the completed-candle extreme add-on guard: long add-ons must break the tracked low; short add-ons must break the tracked high.",
            "- It optimizes first add-on dynamic sizing and later add-on dynamic sizing as separate parameter groups.",
            "- It includes the optional disaster stop: head-entry adverse move plus strategy-cycle unrealized loss against the active risk-equity base.",
            "- It can optionally test Trend Risk Guard: stacked adverse move, Bollinger slope, candle extremes, and width-expansion checks.",
            "- Config is changed only after manual confirmation in the selection prompt.",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def _format_config_value(value: float | str, as_bool: bool = False) -> str:
    """Format a config value with stable Python syntax."""
    if isinstance(value, str):
        return repr(value)
    if as_bool:
        return "True" if int(float(value)) else "False"
    if float(value).is_integer():
        return str(int(value))
    return str(float(value))


def apply_params_to_config(row: dict, config_path: Path = CONFIG_PATH) -> Path:
    """Write selected optimizer parameters into ``src/config.py``."""
    replacements = {
        "MIN_BOLL_WIDTH_PCT": row["min_width_pct"],
        "MIN_BOLL_WIDTH_FLOOR_USD": row["boll_width_floor_usd"],
        "ENTRY_MAX_BOLL_WIDTH_PCT": row["entry_max_width_pct"],
        "ENTRY_DISASTER_SCORE_THRESHOLD": row["entry_disaster_score_threshold"],
        "MIN_ENTRY_GAP_USD": row["min_entry_gap_usd"],
        "FIRST_BATCH_RATIO": row["first_batch_ratio"],
        "SECOND_BATCH_DYNAMIC_BASE_RATIO": row["second_batch_dynamic_base_ratio"],
        "SECOND_BATCH_DYNAMIC_MIN_RATIO": row["second_batch_dynamic_min_ratio"],
        "SECOND_BATCH_DYNAMIC_MAX_RATIO": row["second_batch_dynamic_max_ratio"],
        "SECOND_BATCH_DYNAMIC_FULL_GAP_USD": row["second_batch_dynamic_full_gap_usd"],
        "DYNAMIC_BASE_ENTRY_RATIO": row["dynamic_base_ratio"],
        "DYNAMIC_MIN_ENTRY_RATIO": row["dynamic_min_ratio"],
        "DYNAMIC_MAX_ENTRY_RATIO": row["dynamic_max_ratio"],
        "MAX_TOTAL_ENTRY_RATIO": row["max_total_entry_ratio"],
        "TP_TARGET_MARGIN_RETURN": row["tp_target_margin_return"],
        "LOW_BOLL_WIDTH_TP_ENABLED": row["low_boll_width_tp_enabled"],
        "LOW_BOLL_WIDTH_REF_PCT": row["low_boll_width_ref_pct"],
        "LOW_BOLL_WIDTH_MIN_TP_RETURN": row["low_boll_width_min_tp_return"],
        "LOW_BOLL_WIDTH_MAX_TP_RETURN": row["low_boll_width_max_tp_return"],
        "LOW_BOLL_WIDTH_TP_CAPTURE_RATIO": row["low_boll_width_tp_capture_ratio"],
        "LOW_BOLL_WIDTH_SIZE_MULT": row["low_boll_width_size_mult"],
        "DYNAMIC_TP_ARM_RETURN": row["dynamic_tp_arm_return"],
        "BOLL_MID_COST_STOP_ENABLED": row["boll_mid_cost_stop_enabled"],
        "BOLL_MID_COST_TP_RETURN": row.get("boll_mid_cost_tp_return", BOLL_MID_COST_TP_RETURN),
    }
    bool_keys = {
        "BOLL_MID_COST_STOP_ENABLED",
        "LOW_BOLL_WIDTH_TP_ENABLED",
    }
    text = config_path.read_text(encoding="utf-8")
    backup_path = config_path.with_suffix(".py.bak")
    backup_path.write_text(text, encoding="utf-8")

    for key, value in replacements.items():
        pattern = re.compile(rf"^({key}\s*=\s*)(True|False|[-+]?\d+(?:\.\d+)?|[\"'][^\"']*[\"'])(.*)$", re.MULTILINE)
        text, count = pattern.subn(
            rf"\g<1>{_format_config_value(value, key in bool_keys)}\3",
            text,
            count=1,
        )
        if count != 1:
            raise RuntimeError(f"Missing config key {key} in {config_path}")

    config_path.write_text(text, encoding="utf-8")
    return backup_path


def prompt_apply_params(rows: list[dict], top_n: int = 20) -> None:
    """Prompt the user to apply one ranked parameter set to live config."""
    limit = min(top_n, len(rows))
    print()
    print("Parameter sync")
    print(f"Enter 1-{limit} to write that ranked set into src/config.py; enter 0 or press Enter to keep current settings.")
    choice = input("Select: ").strip()
    if choice in ("", "0"):
        print("Kept current config; src/config.py not changed.")
        return
    if not choice.isdigit() or not (1 <= int(choice) <= limit):
        print("Invalid input; src/config.py not changed.")
        return

    selected = rows[int(choice) - 1]
    print(
        "Will apply: "
        f"SCORE={selected['balanced_score']}, "
        f"PNL={selected['total_pnl']}, "
        f"DD={selected['max_drawdown_pct']}%, "
        f"MIN_LIQ={selected['min_liq_distance_pct']}%, "
        f"MIN_BOLL_WIDTH_PCT={selected['min_width_pct']}, "
        f"ENTRY_MAX_WIDTH={bool(selected['entry_max_width_enabled'])}/"
        f"{selected['entry_max_width_pct']}/{selected['entry_max_width_usd']}, "
        f"ENTRY_DISASTER_SCORE_THRESHOLD={selected['entry_disaster_score_threshold']}, "
        f"MIN_ENTRY_GAP_USD={selected['min_entry_gap_usd']}, "
        f"FIRST_BATCH_RATIO={selected['first_batch_ratio']}, "
        f"MAX_TOTAL_ENTRY_RATIO={selected['max_total_entry_ratio']}, "
        f"SECOND_BATCH_DYNAMIC={selected['second_batch_dynamic_base_ratio']}/"
        f"{selected['second_batch_dynamic_min_ratio']}/"
        f"{selected['second_batch_dynamic_max_ratio']}/"
        f"{selected['second_batch_dynamic_full_gap_usd']}, "
        f"DYNAMIC_BASE_ENTRY_RATIO={selected['dynamic_base_ratio']}, "
        f"DYNAMIC_MIN_ENTRY_RATIO={selected['dynamic_min_ratio']}, "
        f"DYNAMIC_MAX_ENTRY_RATIO={selected['dynamic_max_ratio']}, "
        f"TP_TARGET_MARGIN_RETURN={selected['tp_target_margin_return']}, "
        f"DYNAMIC_TP_ARM_RETURN={selected['dynamic_tp_arm_return']}, "
        f"BOLL_MID_COST={bool(selected['boll_mid_cost_stop_enabled'])}/"
        f"{selected.get('boll_mid_cost_tp_return', BOLL_MID_COST_TP_RETURN)}"
    )
    confirm = input("Type y to confirm: ").strip().lower()
    if confirm != "y":
        print("Canceled; src/config.py not changed.")
        return

    backup_path = apply_params_to_config(selected)
    print(f"Updated src/config.py; backup saved to {backup_path}")


def print_rankings(rows: list[dict], baseline_row: dict, top_n: int = 10) -> None:
    """Print a compact, readable optimizer ranking."""
    best = rows[0]
    print()
    print("=" * 108)
    print("Best Balanced Parameters")
    print("=" * 108)
    print(
        f"MIN_WIDTH_PCT={best['min_width_pct']}  "
        f"ENTRY_MAX_WIDTH_PCT={best['entry_max_width_pct']}  "
        f"ENTRY_DISASTER_SCORE={best['entry_disaster_score_threshold']}  "
        f"MIN_ENTRY_GAP_USD={best['min_entry_gap_usd']}  "
        f"TP_TARGET={best['tp_target_margin_return']}  "
        f"DYN_TP_ARM={best['dynamic_tp_arm_return']}  "
        f"BOLL_MID={bool(best['boll_mid_cost_stop_enabled'])}/"
        f"{best.get('boll_mid_cost_tp_return', BOLL_MID_COST_TP_RETURN)}"
    )
    print(
        f"HEAD={best['first_batch_ratio']}  "
        f"MAX_TOTAL={best['max_total_entry_ratio']}  "
        f"SECOND_DYNAMIC={best['second_batch_dynamic_base_ratio']}/"
        f"{best['second_batch_dynamic_min_ratio']}/"
        f"{best['second_batch_dynamic_max_ratio']}/"
        f"{best['second_batch_dynamic_full_gap_usd']}  "
        f"DYNAMIC={best['dynamic_base_ratio']}/{best['dynamic_min_ratio']}/{best['dynamic_max_ratio']}"
    )
    print(
        "Fixed live-only guards are replayed but not searched: "
        f"fixed_loss={best['copy_fixed_loss_stop_ratio']}, "
        f"disaster={best['disaster_head_drop_pct']}/{best['disaster_loss_ratio']}, "
        f"trend={best['trend_risk_guard_enabled']}/{best['trend_risk_score_threshold']}, "
        f"dynamic-gap detail={best['addon_dynamic_gap_boll_start']}/"
        f"{best['addon_dynamic_gap_boll_strong']}/"
        f"{best['addon_dynamic_gap_boll_max_mult']} and "
        f"{best['addon_dynamic_gap_head_start_pct']}/"
        f"{best['addon_dynamic_gap_head_strong_pct']}/"
        f"{best['addon_dynamic_gap_head_max_mult']}"
    )
    print(
        f"Score={best['balanced_score']:+.2f}  "
        f"PnL={best['total_pnl']:+.2f} USDT  "
        f"trades={best['trades']}  "
        f"win={best['win_rate_pct']:.1f}%  "
        f"drawdown={best['max_drawdown_pct']:.2f}%  "
        f"min_liq={best['min_liq_distance_pct']:.2f}%  "
        f"wipeout={best['wipeout_risk']}  "
        f"avg_hold={best['avg_hold_minutes']:.0f}m"
    )
    print()
    print("Current config baseline")
    print("-" * 108)
    print(
        f"MIN_WIDTH_PCT={baseline_row['min_width_pct']}  "
        f"ENTRY_MAX_WIDTH_PCT={baseline_row['entry_max_width_pct']}  "
        f"ENTRY_DISASTER_SCORE={baseline_row['entry_disaster_score_threshold']}  "
        f"ENTRY_GAP={baseline_row['min_entry_gap_usd']}  "
        f"TP_TARGET={baseline_row['tp_target_margin_return']}  "
        f"DYN_TP_ARM={baseline_row['dynamic_tp_arm_return']}  "
        f"BOLL_MID={bool(baseline_row['boll_mid_cost_stop_enabled'])}/"
        f"{baseline_row.get('boll_mid_cost_tp_return', BOLL_MID_COST_TP_RETURN)}  "
        f"HEAD={baseline_row['first_batch_ratio']}  "
        f"MAX_TOTAL={baseline_row['max_total_entry_ratio']}  "
        f"SECOND_DYNAMIC={baseline_row['second_batch_dynamic_base_ratio']}/"
        f"{baseline_row['second_batch_dynamic_min_ratio']}/"
        f"{baseline_row['second_batch_dynamic_max_ratio']}/"
        f"{baseline_row['second_batch_dynamic_full_gap_usd']}  "
        f"DYNAMIC={baseline_row['dynamic_base_ratio']}/{baseline_row['dynamic_min_ratio']}/{baseline_row['dynamic_max_ratio']}"
    )
    print(
        f"Score={baseline_row['balanced_score']:+.2f}  "
        f"PnL={baseline_row['total_pnl']:+.2f} USDT  "
        f"trades={baseline_row['trades']}  "
        f"win={baseline_row['win_rate_pct']:.1f}%  "
        f"drawdown={baseline_row['max_drawdown_pct']:.2f}%  "
        f"min_liq={baseline_row['min_liq_distance_pct']:.2f}%  "
        f"DStop={baseline_row['disaster_stop']}  "
        f"TrendSig={baseline_row['trend_risk_guard_signal']}  "
        f"TrendStop={baseline_row['trend_risk_guard_stop']}  "
        f"FStop={baseline_row['fixed_loss_stop']}"
    )
    print(
        f"Best delta: Score={_metric_delta(best, baseline_row, 'balanced_score'):+.2f}  "
        f"PnL={_metric_delta(best, baseline_row, 'total_pnl'):+.2f} USDT  "
        f"DD={_metric_delta(best, baseline_row, 'max_drawdown_pct'):+.2f}%  "
        f"MinLiq={_metric_delta(best, baseline_row, 'min_liq_distance_pct'):+.2f}%"
    )
    print()
    print("Top balanced parameter comparison")
    print("-" * 156)
    print("Rank  Score     Risk     PnL USDT  DD     MinLiq  Wipeout  MinW    EntryMax  EntryGap  TP/DynTPArm    Head/Cap   SecondDyn          LaterDyn       BMid  Trades  D/F/B")
    print("-" * 156)
    for idx, row in enumerate(rows[:top_n], start=1):
        dyn = f"{row['dynamic_base_ratio']:g}/{row['dynamic_min_ratio']:g}/{row['dynamic_max_ratio']:g}"
        second_dyn = (
            f"{row['second_batch_dynamic_base_ratio']:g}/"
            f"{row['second_batch_dynamic_min_ratio']:g}/"
            f"{row['second_batch_dynamic_max_ratio']:g}/"
            f"{row['second_batch_dynamic_full_gap_usd']:g}"
        )
        trend_risk = (
            f"{row['trend_risk_guard_enabled']}/"
            f"{row['trend_risk_guard_close_enabled']}/"
            f"{row['trend_risk_score_threshold']}/"
            f"{row['trend_risk_head_adverse_pct']:g}/"
            f"{row['trend_risk_width_expand']:g}"
        )
        print(
            f"{idx:>2}  "
            f"{row['balanced_score']:>8.2f}  "
            f"{row['risk_score']:>7.2f}  "
            f"{row['total_pnl']:>9.2f}  "
            f"{row['max_drawdown_pct']:>5.2f}%  "
            f"{row['min_liq_distance_pct']:>6.2f}%  "
            f"{str(row['wipeout_risk']):<7}  "
            f"{row['min_width_pct']:<8.4f}"
            f"{row['entry_max_width_pct']:<9.4f}"
            f"{row['min_entry_gap_usd']:<9g}"
            f"{row['tp_target_margin_return']:g}/"
            f"{row['dynamic_tp_arm_return']:<7g}"
            f"{row['first_batch_ratio']:g}/{row['max_total_entry_ratio']:<8g}"
            f"{second_dyn:<19}"
            f"{dyn:<15}"
            f"{str(row['boll_mid_cost_stop_enabled']) + '/' + str(row.get('boll_mid_cost_tp_return', BOLL_MID_COST_TP_RETURN)):<6}"
            f"{row['trades']:>3}  "
            f"{row['disaster_stop']}/{row['fixed_loss_stop']}/{row['boll_mid_cost_stop']}"
        )
    print("-" * 156)
    print()
    profit_best = max(rows, key=lambda row: (row["total_pnl"], row["min_liq_distance_pct"]))
    safe_candidates = [row for row in rows if row["total_pnl"] > 0]
    risk_best = max(safe_candidates, key=lambda row: (row["risk_score"], row["total_pnl"])) if safe_candidates else None
    print(
        "Profit best: "
        f"PnL={profit_best['total_pnl']:+.2f} Score={profit_best['balanced_score']:+.2f} "
        f"Risk={profit_best['risk_score']:+.2f} MinLiq={profit_best['min_liq_distance_pct']:.2f}% "
        f"DD={profit_best['max_drawdown_pct']:.2f}%"
    )
    if risk_best:
        print(
            "Risk best positive-PnL: "
            f"PnL={risk_best['total_pnl']:+.2f} Score={risk_best['balanced_score']:+.2f} "
            f"Risk={risk_best['risk_score']:+.2f} MinLiq={risk_best['min_liq_distance_pct']:.2f}% "
            f"DD={risk_best['max_drawdown_pct']:.2f}%"
        )
    print()
    print("Full fields, current baseline, and walk-forward validation are saved to CSV/Markdown.")


def print_walk_forward(rows: list[dict], top_n: int = 10) -> None:
    """Print compact walk-forward validation rows."""
    if not rows:
        return
    print()
    print("Walk-forward validation")
    print("-" * 120)
    print("Rank  Robust    MinW      EntryMax  EntryGap  Head/Cap  TrainPnL  TrainDD  ValPnL    ValDD   ValTrades  Wipeout")
    print("-" * 120)
    for row in rows[:top_n]:
        print(
            f"{row['rank']:>2}  "
            f"{row['robustness_score']:>8.2f}  "
            f"{row['min_width_pct']:<8.4f}  "
            f"{row['entry_max_width_pct']:<8.4f}  "
            f"{row['min_entry_gap_usd']:<8g}  "
            f"{row['first_batch_ratio']:g}/{row['max_total_entry_ratio']:<7g}  "
            f"{row['train_pnl']:>8.2f}  "
            f"{row['train_dd']:>7.2f}%  "
            f"{row['val_pnl']:>8.2f}  "
            f"{row['val_dd']:>6.2f}%  "
            f"{row['val_trades']:>9}  "
            f"{row['val_wipeout']}"
        )
    print("-" * 120)


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Optimize live-entry parameters from strategy logs.")
    parser.add_argument("--log-dir", default=str(DEFAULT_LOG_DIR))
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--initial-total-equity", type=float, default=INITIAL_TOTAL_EQUITY)
    parser.add_argument("--sample-sec", type=int, default=POLL_INTERVAL, help="Replay resample interval in seconds; 0 uses every parsed tick.")
    parser.add_argument("--use-cache", action="store_true", help="Cache parsed log ticks by log file metadata and sample interval.")
    parser.add_argument("--cache-dir", default=str(DEFAULT_CACHE_DIR))
    parser.add_argument("--two-stage", action="store_true", help="Run coarse optimization on sample-sec, then replay top params on final-sample-sec.")
    parser.add_argument("--final-sample-sec", type=int, default=POLL_INTERVAL)
    parser.add_argument("--refine-top-n", type=int, default=8)
    parser.add_argument("--min-boll-width-pct", default=str(MIN_BOLL_WIDTH_PCT))
    parser.add_argument("--min-entry-gap-usd", default="5,6,7")
    parser.add_argument("--entry-max-width-pct", default=str(ENTRY_MAX_BOLL_WIDTH_PCT))
    parser.add_argument("--entry-disaster-score-threshold", default=str(ENTRY_DISASTER_SCORE_THRESHOLD))
    parser.add_argument("--entry-disaster-tp-distance-mult", default=str(ENTRY_DISASTER_TP_DISTANCE_MULT))
    parser.add_argument("--first-batch-ratio", default="0.08,0.10,0.12")
    parser.add_argument("--second-batch-dynamic-base-ratio", default="0.12,0.14,0.16")
    parser.add_argument("--second-batch-dynamic-min-ratio", default=str(SECOND_BATCH_DYNAMIC_MIN_RATIO))
    parser.add_argument("--second-batch-dynamic-max-ratio", default=str(SECOND_BATCH_DYNAMIC_MAX_RATIO))
    parser.add_argument("--second-batch-dynamic-full-gap-usd", default=str(SECOND_BATCH_DYNAMIC_FULL_GAP_USD))
    parser.add_argument("--dynamic-base-ratio", default="0.06,0.08,0.10")
    parser.add_argument("--dynamic-min-ratio", default=str(DYNAMIC_MIN_ENTRY_RATIO))
    parser.add_argument("--dynamic-max-ratio", default=str(DYNAMIC_MAX_ENTRY_RATIO))
    parser.add_argument("--max-total-entry-ratio", default="0.70,0.80")
    parser.add_argument("--boll-width-tp-space-mult", default=str(BOLL_WIDTH_TP_SPACE_MULT))
    parser.add_argument("--tp-target-margin-return", default="0.25,0.28,0.32")
    parser.add_argument("--dynamic-tp-arm-return", default="0.20,0.22,0.24")
    parser.add_argument("--boll-mid-cost-stop-enabled", default=str(int(BOLL_MID_COST_STOP_ENABLED)))
    parser.add_argument("--copy-fixed-loss-stop-ratio", default=str(COPY_FIXED_LOSS_STOP_RATIO))
    parser.add_argument("--fixed-loss-head-buffer-pct", default=str(FIXED_LOSS_HEAD_BUFFER_PCT))
    parser.add_argument("--disaster-head-drop-pct", default=str(DISASTER_HEAD_DROP_PCT))
    parser.add_argument("--disaster-loss-ratio", default=str(DISASTER_LOSS_RATIO))
    parser.add_argument("--trend-risk-guard-enabled", default=str(int(TREND_RISK_GUARD_ENABLED)))
    parser.add_argument("--trend-risk-guard-close-enabled", default=str(int(TREND_RISK_GUARD_CLOSE_ENABLED)))
    parser.add_argument("--trend-risk-score-threshold", default=str(TREND_RISK_SCORE_THRESHOLD))
    parser.add_argument("--trend-risk-head-adverse-pct", default=str(TREND_RISK_HEAD_ADVERSE_PCT))
    parser.add_argument("--trend-risk-width-expand", default=str(TREND_RISK_WIDTH_EXPAND))
    parser.add_argument("--addon-dynamic-gap-max-usd", default=str(ADDON_DYNAMIC_GAP_MAX_USD))
    parser.add_argument("--addon-dynamic-gap-boll-start", default=str(ADDON_DYNAMIC_GAP_BOLL_START))
    parser.add_argument("--addon-dynamic-gap-boll-strong", default=str(ADDON_DYNAMIC_GAP_BOLL_STRONG))
    parser.add_argument("--addon-dynamic-gap-boll-max-mult", default=str(ADDON_DYNAMIC_GAP_BOLL_MAX_MULT))
    parser.add_argument("--addon-dynamic-gap-head-start-pct", default=str(ADDON_DYNAMIC_GAP_HEAD_START_PCT))
    parser.add_argument("--addon-dynamic-gap-head-strong-pct", default=str(ADDON_DYNAMIC_GAP_HEAD_STRONG_PCT))
    parser.add_argument("--addon-dynamic-gap-head-max-mult", default=str(ADDON_DYNAMIC_GAP_HEAD_MAX_MULT))
    parser.add_argument("--addon-dynamic-gap-trend-klines", default=str(ADDON_DYNAMIC_GAP_TREND_KLINES))
    parser.add_argument("--addon-dynamic-gap-trend-mult", default=str(ADDON_DYNAMIC_GAP_TREND_MULT))
    parser.add_argument("--search-mode", choices=("grid", "random", "optuna"), default="random")
    parser.add_argument("--random-trials", type=int, default=96)
    parser.add_argument("--random-seed", type=int, default=42)
    parser.add_argument("--optuna-trials", type=int, default=80)
    parser.add_argument("--optuna-startup-trials", type=int, default=16)
    parser.add_argument("--walk-forward-ratio", type=float, default=0.7)
    parser.add_argument("--walk-forward-top-n", type=int, default=10)
    parser.add_argument("--workers", type=int, default=0, help="Replay worker processes; 0 auto-detects, 1 disables multiprocessing.")
    parser.add_argument("--no-prompt", action="store_true", help="Generate report only; do not show config sync prompt.")
    parser.add_argument("--quiet", action="store_true", help="Hide progress output and print only final results.")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not args.quiet:
        print("Reading strategy logs...", flush=True)
    log_dir = Path(args.log_dir)
    cache_dir = Path(args.cache_dir) if args.use_cache else None
    ticks = load_or_parse_logs(log_dir, sample_sec=args.sample_sec, cache_dir=cache_dir, quiet=args.quiet)
    if not args.quiet:
        print(
            f"Loaded {len(ticks)} ticks, range {ticks['ts'].min()} -> {ticks['ts'].max()}",
            flush=True,
        )

    baseline_params = current_config_params(args)
    if args.search_mode == "optuna":
        rows = build_optuna_rows(ticks, args, baseline_params, args.initial_total_equity)
    else:
        grid = build_random_grid(args) if args.search_mode == "random" else build_grid(args)
        grid = ensure_baseline_in_grid(grid, baseline_params)
        rows = run_replays(ticks, grid, args.initial_total_equity, args.workers, args.quiet)

    rows.sort(
        key=lambda row: (
            row["balanced_score"],
            row["total_pnl"],
            -row["max_drawdown_pct"],
            row["min_liq_distance_pct"],
        ),
        reverse=True,
    )
    report_ticks = ticks
    if args.two_stage:
        if not args.quiet:
            print(
                f"Two-stage refine: replaying top {args.refine_top_n} params "
                f"with sample-sec={args.final_sample_sec}...",
                flush=True,
            )
        fine_ticks = load_or_parse_logs(
            log_dir,
            sample_sec=args.final_sample_sec,
            cache_dir=cache_dir,
            quiet=args.quiet,
        )
        fine_grid = ensure_baseline_in_grid(params_from_rows(rows, args.refine_top_n), baseline_params)
        rows = run_replays(fine_ticks, fine_grid, args.initial_total_equity, args.workers, args.quiet)
        rows.sort(
            key=lambda row: (
                row["balanced_score"],
                row["total_pnl"],
                -row["max_drawdown_pct"],
                row["min_liq_distance_pct"],
            ),
            reverse=True,
        )
        report_ticks = fine_ticks

    if not args.quiet:
        print("Sorting and writing report...", flush=True)
    baseline_row = LogReplay(report_ticks, baseline_params, args.initial_total_equity).run().report()

    walk_forward_rows = run_walk_forward(
        report_ticks,
        rows,
        args.initial_total_equity,
        args.walk_forward_ratio,
        args.walk_forward_top_n,
    )

    stamp = report_ticks["ts"].max().strftime("%Y%m%d_%H%M%S")
    csv_path = out_dir / f"log_param_report_{stamp}.csv"
    md_path = out_dir / f"log_param_report_{stamp}.md"
    wf_csv_path = out_dir / f"log_param_walk_forward_{stamp}.csv"
    out_dir.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    if walk_forward_rows:
        with wf_csv_path.open("w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=list(walk_forward_rows[0].keys()))
            writer.writeheader()
            writer.writerows(walk_forward_rows)
    write_markdown_report(md_path, rows, report_ticks, baseline_row, walk_forward_rows)

    print_rankings(rows, baseline_row)
    print_walk_forward(walk_forward_rows)
    print(f"csv={csv_path}")
    if walk_forward_rows:
        print(f"walk_forward_csv={wf_csv_path}")
    print(f"report={md_path}")
    if not args.no_prompt:
        prompt_apply_params(rows)


if __name__ == "__main__":
    main()
