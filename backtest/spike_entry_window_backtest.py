"""Evaluate spike-entry window variants on local strategy logs.

This offline script compares four first-entry policies while keeping the rest
of the replay logic unchanged:

* A current: current single-tick band break plus 2-tick no-new-extreme filter.
* B no_new_extreme_3: current break logic plus a 3-tick filter.
* C spike_memory_outside: remember a recent spike, but still require current
  price to be outside the Bollinger band before entering.
* D spike_memory_near_band: remember a recent spike and allow entry near the
  band after price stops making a fresh extreme.
* D+ spike_memory_near_band_plus: D plus a local liquidity sweep condition,
  stricter back-inside-band confirmation in extreme volatility, and no add-on
  after trend-risk activation.
"""

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import replace
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtest.log_parameter_optimizer import (  # noqa: E402
    INITIAL_TOTAL_EQUITY,
    LogReplay,
    SOURCE_BOLL_STD,
    current_config_params,
    load_or_parse_logs,
    parse_logs,
)


DEFAULT_LOCAL_LOG_DIR = ROOT / "logs"
DEFAULT_OUT_DIR = ROOT / "backtest" / "results" / "spike_entry_window_backtest"


class LabeledReplay(LogReplay):
    """Replay with a policy label in the report."""

    BOLL_HISTORY_MAX_ROWS = 5000

    def __init__(self, ticks: pd.DataFrame, initial_total: float, policy: str):
        super().__init__(ticks, current_config_params(None), initial_total)
        self.policy = policy

    def _remember_boll(self, row) -> None:
        """Keep Bollinger history with cheaper periodic trimming for 3s replay."""
        unit_std = (float(row.upper_src) - float(row.mid)) / SOURCE_BOLL_STD
        upper = float(row.mid) + self.params.boll_std * unit_std
        lower = float(row.mid) - self.params.boll_std * unit_std
        width = upper - lower
        price = float(row.price)
        half_width = upper - float(row.mid)
        z = (price - float(row.mid)) / half_width if half_width else 0.0
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
                "mid": float(row.mid),
                "upper": upper,
                "width": width,
                "width_pct": width / price if price > 0 else 0.0,
                "z": z,
            }
        )
        if len(self.boll_history) > self.BOLL_HISTORY_MAX_ROWS:
            keep_after = pd.Timestamp(row.ts) - pd.Timedelta(
                minutes=max(self.params.trend_risk_slope_window_min * 2, 180)
            )
            self.boll_history = [item for item in self.boll_history if item["ts"] >= keep_after]

    def report(self) -> dict:
        row = super().report()
        row.update({"policy": self.policy, "initial_total_equity": self.initial_total})
        return row


class NoNewExtreme3Replay(LabeledReplay):
    """Current entry logic with a longer no-new-extreme window."""

    WINDOW_TICKS = 3

    def _remember_price(self, price: float) -> None:
        self.recent_prices.append(price)
        keep = max(self.WINDOW_TICKS + 1, 3)
        if len(self.recent_prices) > keep:
            self.recent_prices = self.recent_prices[-keep:]

    def _still_making_new_low(self) -> bool:
        if len(self.recent_prices) < self.WINDOW_TICKS + 1:
            return True
        recent = self.recent_prices[-(self.WINDOW_TICKS + 1):]
        return recent[-1] <= min(recent[:-1])

    def _still_making_new_high(self) -> bool:
        if len(self.recent_prices) < self.WINDOW_TICKS + 1:
            return True
        recent = self.recent_prices[-(self.WINDOW_TICKS + 1):]
        return recent[-1] >= max(recent[:-1])


class SpikeMemoryReplay(NoNewExtreme3Replay):
    """First-entry replay that remembers recent band spikes."""

    MEMORY_SEC = 9.0
    CANDIDATE_TTL_SEC = 12.0
    ENTRY_TOLERANCE_USD = 0.5
    ENTRY_TOLERANCE_BOLL_RATIO = 0.05
    ORDER_REBOUND_OFFSET_USD = 0.3
    ORDER_REBOUND_OFFSET_BOLL_RATIO = 0.03

    def __init__(self, ticks: pd.DataFrame, initial_total: float, policy: str):
        super().__init__(ticks, initial_total, policy)
        self._spike_candidate: dict | None = None
        self._addon_spike_candidate: dict | None = None
        self._tp_spike_candidate: dict | None = None
        self.spike_seen = 0
        self.spike_expired = 0
        self.spike_rejected_extreme = 0
        self.spike_rejected_not_entry_zone = 0
        self.spike_entries = 0
        self.addon_spike_seen = 0
        self.addon_spike_entries = 0
        self.addon_spike_expired = 0
        self.tp_spike_seen = 0
        self.tp_spike_locks = 0

    def _candidate_age_sec(self, row) -> float:
        if not self._spike_candidate:
            return 999999.0
        return max(
            (pd.Timestamp(row.ts) - pd.Timestamp(self._spike_candidate["ts"])).total_seconds(),
            0.0,
        )

    def _candidate_age_sec_for(self, row, candidate: dict | None) -> float:
        if not candidate:
            return 999999.0
        return max((pd.Timestamp(row.ts) - pd.Timestamp(candidate["ts"])).total_seconds(), 0.0)

    def _clear_expired_candidate(self, row) -> None:
        if self._spike_candidate and self._candidate_age_sec(row) > self.CANDIDATE_TTL_SEC:
            self.spike_expired += 1
            self._spike_candidate = None
        if self._addon_spike_candidate and self._candidate_age_sec_for(row, self._addon_spike_candidate) > self.CANDIDATE_TTL_SEC:
            self.addon_spike_expired += 1
            self._addon_spike_candidate = None
        if self._tp_spike_candidate and self._candidate_age_sec_for(row, self._tp_spike_candidate) > self.CANDIDATE_TTL_SEC:
            self._tp_spike_candidate = None

    def _remember_price(self, price: float) -> None:
        self.recent_prices.append(price)
        keep = max(self.WINDOW_TICKS + 1, int(self.MEMORY_SEC // 3) + 2, 3)
        if len(self.recent_prices) > keep:
            self.recent_prices = self.recent_prices[-keep:]

    def _record_spike_candidate(self, row) -> None:
        if not self._width_ok(row):
            return
        direction = self._outside_direction(row)
        if direction == "none":
            return
        if not self._entry_max_width_ok(row):
            return
        if not self._entry_disaster_filter_ok(row, direction):
            return

        lower, _, upper, width = self._bands(row)
        price = float(row.price)
        candidate = self._spike_candidate
        should_replace = candidate is None or candidate["direction"] != direction
        if candidate is not None and candidate["direction"] == direction:
            if direction == "long" and price < candidate["extreme"]:
                should_replace = True
            if direction == "short" and price > candidate["extreme"]:
                should_replace = True
        if should_replace:
            self.spike_seen += 1
            self._spike_candidate = {
                "direction": direction,
                "ts": pd.Timestamp(row.ts),
                "kline_ts": row.kline_ts,
                "extreme": price,
                "lower": lower,
                "upper": upper,
                "width": width,
            }

    def _candidate_stable(self) -> bool:
        if not self._spike_candidate:
            return False
        if self._spike_candidate["direction"] == "long":
            return not self._still_making_new_low()
        return not self._still_making_new_high()

    def _entry_zone_ok(self, row) -> bool:
        if not self._spike_candidate:
            return False
        direction = self._spike_candidate["direction"]
        outside = self._outside_direction(row)
        if self.policy == "spike_memory_outside":
            return outside == direction

        lower, _, upper, width = self._bands(row)
        tolerance = max(self.ENTRY_TOLERANCE_USD, width * self.ENTRY_TOLERANCE_BOLL_RATIO)
        price = float(row.price)
        if direction == "long":
            return price <= lower + tolerance
        return price >= upper - tolerance

    def _entry_order_price(self, row) -> float:
        return self._candidate_order_price(row, self._spike_candidate)

    def _candidate_order_price(self, row, candidate: dict | None) -> float:
        if not candidate:
            return float(row.price)
        if self.policy == "spike_memory_outside":
            return float(row.price)
        direction = candidate["direction"]
        width = float(candidate["width"])
        offset = max(self.ORDER_REBOUND_OFFSET_USD, width * self.ORDER_REBOUND_OFFSET_BOLL_RATIO)
        price = float(row.price)
        extreme = float(candidate["extreme"])
        if direction == "long":
            return min(price, extreme + offset)
        return max(price, extreme - offset)

    def _record_addon_spike_candidate(self, row) -> None:
        if not self.pos.is_active() or self.pos.direction not in ("long", "short"):
            return
        if not self._width_ok(row) or not self._addon_max_width_ok(row):
            return
        direction = self._outside_direction(row)
        if direction != self.pos.direction:
            return
        lower, _, upper, width = self._bands(row)
        price = float(row.price)
        candidate = self._addon_spike_candidate
        should_replace = candidate is None
        if candidate is not None:
            if direction == "long" and price < candidate["extreme"]:
                should_replace = True
            if direction == "short" and price > candidate["extreme"]:
                should_replace = True
        if should_replace:
            self.addon_spike_seen += 1
            self._addon_spike_candidate = {
                "direction": direction,
                "ts": pd.Timestamp(row.ts),
                "kline_ts": row.kline_ts,
                "extreme": price,
                "lower": lower,
                "upper": upper,
                "width": width,
            }

    def _addon_candidate_stable(self) -> bool:
        if not self._addon_spike_candidate:
            return False
        if self._addon_spike_candidate["direction"] == "long":
            return not self._still_making_new_low()
        return not self._still_making_new_high()

    def _addon_entry_zone_ok(self, row) -> bool:
        candidate = self._addon_spike_candidate
        if not candidate:
            return False
        direction = candidate["direction"]
        outside = self._outside_direction(row)
        if self.policy == "spike_memory_outside":
            return outside == direction
        lower, _, upper, width = self._bands(row)
        tolerance = max(self.ENTRY_TOLERANCE_USD, width * self.ENTRY_TOLERANCE_BOLL_RATIO)
        price = float(row.price)
        if direction == "long":
            return price <= lower + tolerance
        return price >= upper - tolerance

    def _try_open(self, row) -> None:
        if self.capital_shortage_active:
            return

        self._clear_expired_candidate(row)
        self._record_spike_candidate(row)
        if self._spike_candidate is None:
            return

        direction = self._spike_candidate["direction"]
        if not self._candidate_stable():
            self.spike_rejected_extreme += 1
            return
        if not self._entry_zone_ok(row):
            self.spike_rejected_not_entry_zone += 1
            return

        price = float(row.price)
        order_price = round(self._entry_order_price(row), 2)
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
        order = self._order_at(0, order_price)
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
                "note": self.policy,
            }
        )
        self.last_plan_price = price
        self.last_entry_check_kline = row.kline_ts
        self.entry_time = str(row.ts)
        self.spike_entries += 1
        self._spike_candidate = None
        self._try_fill_pending(row)

    def _maybe_place_next_batch(self, row) -> None:
        if self.capital_shortage_active:
            return
        if self.params.trend_risk_freeze_addon_enabled and self.trend_risk_guard_active:
            self.trend_risk_freeze_blocks += 1
            return

        self._clear_expired_candidate(row)
        self._record_addon_spike_candidate(row)
        candidate = self._addon_spike_candidate
        if candidate is None:
            return
        if not self._addon_candidate_stable() or not self._addon_entry_zone_ok(row):
            return

        if self.last_batch_kline is not None and row.kline_ts == self.last_batch_kline:
            self.same_k_block += 1
            return
        last = self.pos.last_filled()
        if last is None:
            return

        price = float(row.price)
        order_price = round(self._candidate_order_price(row, candidate), 2)
        if self.pos.direction == "long" and order_price > last.price:
            return
        if self.pos.direction == "short" and order_price < last.price:
            return
        if abs(order_price - last.price) < self._effective_entry_gap(price):
            self.blocked_gap += 1
            return

        self._update_addon_guard(row)
        if not self._addon_guard_allows(order_price):
            return
        order = self._order_at(self.pos.next_idx(), order_price)
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
                "note": f"{self.policy}_addon",
            }
        )
        self.last_entry_check_kline = row.kline_ts
        self.addon_spike_entries += 1
        self._addon_spike_candidate = None
        self._try_fill_pending(row)

    def _record_tp_spike_candidate(self, row) -> None:
        if self.policy == "spike_memory_outside":
            return
        if not self.params.dynamic_tp_enabled or not self.pos.is_active() or self.dynamic_tp_active:
            return
        price = float(row.price)
        ret = self._position_margin_return(price)
        target_return = self.cycle_tp_target_margin_return or self.params.tp_target_margin_return
        if ret < self.params.dynamic_tp_arm_return or ret >= target_return:
            return
        direction = self.pos.direction
        candidate = self._tp_spike_candidate
        should_replace = candidate is None
        if candidate is not None:
            if direction == "long" and price > candidate["extreme"]:
                should_replace = True
            if direction == "short" and price < candidate["extreme"]:
                should_replace = True
        if should_replace:
            self.tp_spike_seen += 1
            self._tp_spike_candidate = {
                "direction": direction,
                "ts": pd.Timestamp(row.ts),
                "extreme": price,
            }

    def _tp_candidate_stable(self) -> bool:
        if not self._tp_spike_candidate:
            return False
        if self._tp_spike_candidate["direction"] == "long":
            return not self._still_making_new_high()
        return not self._still_making_new_low()

    def _maybe_update_dynamic_tp(self, row) -> None:
        if self.policy == "spike_memory_outside":
            return super()._maybe_update_dynamic_tp(row)
        self._clear_expired_candidate(row)
        self._record_tp_spike_candidate(row)
        if not self._tp_candidate_stable():
            return
        price = float(row.price)
        lock_price = round(price, 2)
        if self.pos.direction == "long" and lock_price <= self.pos.avg_entry:
            return
        if self.pos.direction == "short" and lock_price >= self.pos.avg_entry:
            return
        if abs(lock_price - self.pos.tp_price) < self.params.dynamic_tp_reprice_gap_usd:
            return
        self.pos.tp_price = lock_price
        self.dynamic_tp_active = True
        self.boll_mid_cost_tp_active = False
        self.dynamic_tp_activated += 1
        self.tp_spike_locks += 1
        self._tp_spike_candidate = None

    def report(self) -> dict:
        row = super().report()
        row.update(
            {
                "spike_seen": self.spike_seen,
                "spike_expired": self.spike_expired,
                "spike_rejected_extreme": self.spike_rejected_extreme,
                "spike_rejected_not_entry_zone": self.spike_rejected_not_entry_zone,
                "spike_entries": self.spike_entries,
                "addon_spike_seen": self.addon_spike_seen,
                "addon_spike_entries": self.addon_spike_entries,
                "addon_spike_expired": self.addon_spike_expired,
                "tp_spike_seen": self.tp_spike_seen,
                "tp_spike_locks": self.tp_spike_locks,
            }
        )
        return row


class SpikeMemoryPlusReplay(SpikeMemoryReplay):
    """D+ replay with liquidity-sweep and volatility confirmation filters."""

    SWEEP_LOOKBACK_MIN = 10.0
    SWEEP_MARGIN_USD = 0.1
    SWEEP_MIN_OBS = 20
    EXTREME_WIDTH_PCT = 0.015
    EXTREME_WIDTH_USD = 45.0
    SAFE_TREND_KLINES = 3

    def __init__(self, ticks: pd.DataFrame, initial_total: float, policy: str):
        super().__init__(ticks, initial_total, policy)
        self.sweep_rejected = 0
        self.extreme_back_inside_rejected = 0
        self.plus_trend_addon_blocks = 0

    @property
    def _plus_sweep_enabled(self) -> bool:
        return self.policy in {"spike_memory_plus_sweep", "spike_memory_near_band_plus"}

    @property
    def _plus_inside_enabled(self) -> bool:
        return self.policy in {
            "spike_memory_plus_inside",
            "spike_memory_plus_inside_no_entry_max",
            "spike_memory_plus_inside_entry_max_030",
            "spike_memory_plus_inside_entry_max_032",
            "spike_memory_near_band_plus",
        }

    @property
    def _safe_inside_enabled(self) -> bool:
        return self.policy == "spike_memory_d_safe"

    @property
    def _plus_trend_enabled(self) -> bool:
        return self.policy in {"spike_memory_plus_trend", "spike_memory_near_band_plus"}

    def _local_sweep_ok(self, row, direction: str, price: float) -> bool:
        """Return whether price swept a recent local high/low."""
        now = pd.Timestamp(row.ts)
        start = now - pd.Timedelta(minutes=self.SWEEP_LOOKBACK_MIN)
        window = [
            item for item in self.boll_history
            if start <= item["ts"] < now
        ]
        if len(window) < self.SWEEP_MIN_OBS:
            return False
        if direction == "long":
            local_low = min(float(item["low"]) for item in window)
            return price <= local_low - self.SWEEP_MARGIN_USD
        if direction == "short":
            local_high = max(float(item["high"]) for item in window)
            return price >= local_high + self.SWEEP_MARGIN_USD
        return False

    def _record_spike_candidate(self, row) -> None:
        before = self.spike_seen
        if not self._width_ok(row):
            return
        direction = self._outside_direction(row)
        if direction == "none":
            return
        if self._plus_sweep_enabled and not self._local_sweep_ok(row, direction, float(row.price)):
            self.sweep_rejected += 1
            return
        super()._record_spike_candidate(row)
        if self.spike_seen > before and self._spike_candidate is not None:
            self._spike_candidate["local_sweep"] = True

    def _record_addon_spike_candidate(self, row) -> None:
        before = self.addon_spike_seen
        if not self.pos.is_active() or self.pos.direction not in ("long", "short"):
            return
        direction = self._outside_direction(row)
        if direction != self.pos.direction:
            return
        if self._plus_sweep_enabled and not self._local_sweep_ok(row, direction, float(row.price)):
            self.sweep_rejected += 1
            return
        super()._record_addon_spike_candidate(row)
        if self.addon_spike_seen > before and self._addon_spike_candidate is not None:
            self._addon_spike_candidate["local_sweep"] = True

    def _extreme_volatility(self, row) -> bool:
        """Return whether current band is wide enough to demand band re-entry."""
        _, _, _, width = self._bands(row)
        price = float(row.price)
        width_pct = width / price if price > 0 else 0.0
        return width_pct >= self.EXTREME_WIDTH_PCT or width >= self.EXTREME_WIDTH_USD

    def _safe_single_side_risk(self, row, direction: str) -> bool:
        """Return whether a spike is in a one-sided expansion regime."""
        if not self._extreme_volatility(row):
            return False
        recent = self._recent_unique_boll_history(self.SAFE_TREND_KLINES)
        if len(recent) < self.SAFE_TREND_KLINES:
            return False
        lows = [float(item["low"]) for item in recent]
        highs = [float(item["high"]) for item in recent]
        mids = [float(item["mid"]) for item in recent]
        lowers = [float(item["lower"]) for item in recent]
        uppers = [float(item["upper"]) for item in recent]
        if direction == "long":
            lower_lows = all(lows[i] < lows[i - 1] for i in range(1, len(lows)))
            mid_down = mids[-1] < mids[0]
            lower_down = lowers[-1] < lowers[0]
            return lower_lows and mid_down and lower_down
        if direction == "short":
            higher_highs = all(highs[i] > highs[i - 1] for i in range(1, len(highs)))
            mid_up = mids[-1] > mids[0]
            upper_up = uppers[-1] > uppers[0]
            return higher_highs and mid_up and upper_up
        return False

    def _requires_back_inside(self, row, direction: str) -> bool:
        """Return whether the current policy requires band re-entry."""
        if self._plus_inside_enabled and self._extreme_volatility(row):
            return True
        if self._safe_inside_enabled and self._safe_single_side_risk(row, direction):
            return True
        return False

    def _back_inside_band(self, row, direction: str) -> bool:
        lower, _, upper, _ = self._bands(row)
        price = float(row.price)
        if direction == "long":
            return price >= lower
        if direction == "short":
            return price <= upper
        return False

    def _entry_zone_ok(self, row) -> bool:
        ok = super()._entry_zone_ok(row)
        if not ok or not self._spike_candidate:
            return ok
        direction = self._spike_candidate["direction"]
        if self._requires_back_inside(row, direction) and not self._back_inside_band(row, direction):
            self.extreme_back_inside_rejected += 1
            return False
        return True

    def _addon_entry_zone_ok(self, row) -> bool:
        ok = super()._addon_entry_zone_ok(row)
        if not ok or not self._addon_spike_candidate:
            return ok
        direction = self._addon_spike_candidate["direction"]
        if self._requires_back_inside(row, direction) and not self._back_inside_band(row, direction):
            self.extreme_back_inside_rejected += 1
            return False
        return True

    def _maybe_place_next_batch(self, row) -> None:
        if self._plus_trend_enabled and self.trend_risk_guard_active:
            self.plus_trend_addon_blocks += 1
            self.trend_risk_freeze_blocks += 1
            return
        super()._maybe_place_next_batch(row)

    def report(self) -> dict:
        row = super().report()
        row.update(
            {
                "sweep_rejected": self.sweep_rejected,
                "extreme_back_inside_rejected": self.extreme_back_inside_rejected,
                "plus_trend_addon_blocks": self.plus_trend_addon_blocks,
            }
        )
        return row


class NoEntryMaxWidthMixin:
    """Disable the first-entry max Bollinger width filter for one replay."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params = replace(self.params, entry_max_width_enabled=0)


class SpikeMemoryNoEntryMaxReplay(NoEntryMaxWidthMixin, SpikeMemoryReplay):
    """D replay without the first-entry max-width filter."""


class SpikeMemoryPlusNoEntryMaxReplay(NoEntryMaxWidthMixin, SpikeMemoryPlusReplay):
    """D+ style replay without the first-entry max-width filter."""


class EntryMaxWidthOverrideMixin:
    """Override the first-entry max Bollinger width pct for one replay."""

    ENTRY_MAX_WIDTH_OVERRIDES = {
        "spike_memory_near_band_entry_max_030": 0.030,
        "spike_memory_near_band_entry_max_032": 0.032,
        "spike_memory_plus_inside_entry_max_030": 0.030,
        "spike_memory_plus_inside_entry_max_032": 0.032,
    }

    def __init__(self, ticks: pd.DataFrame, initial_total: float, policy: str):
        super().__init__(ticks, initial_total, policy)
        self.params = replace(
            self.params,
            entry_max_width_enabled=1,
            entry_max_width_pct=self.ENTRY_MAX_WIDTH_OVERRIDES[policy],
        )


class SpikeMemoryEntryMaxWidthReplay(EntryMaxWidthOverrideMixin, SpikeMemoryReplay):
    """D replay with a custom first-entry max-width pct."""


class SpikeMemoryPlusEntryMaxWidthReplay(EntryMaxWidthOverrideMixin, SpikeMemoryPlusReplay):
    """D-inside replay with a custom first-entry max-width pct."""


class SpikeMemoryTieredInsideReplay(SpikeMemoryPlusReplay):
    """D replay that requires band re-entry only above a width-pct threshold."""

    INSIDE_WIDTH_THRESHOLDS = {
        "spike_memory_tier_inside_022": 0.022,
        "spike_memory_tier_inside_024": 0.024,
    }

    @property
    def _plus_inside_enabled(self) -> bool:
        return False

    def _requires_back_inside(self, row, direction: str) -> bool:
        _, _, _, width = self._bands(row)
        price = float(row.price)
        width_pct = width / price if price > 0 else 0.0
        return width_pct >= self.INSIDE_WIDTH_THRESHOLDS[self.policy]


def replay_policy(ticks: pd.DataFrame, policy: str, initial_total: float) -> dict:
    """Run one policy."""
    if policy == "current":
        replay = LabeledReplay(ticks, initial_total, policy)
    elif policy == "no_new_extreme_3":
        replay = NoNewExtreme3Replay(ticks, initial_total, policy)
    elif policy in {"spike_memory_outside", "spike_memory_near_band"}:
        replay = SpikeMemoryReplay(ticks, initial_total, policy)
    elif policy == "spike_memory_near_band_no_entry_max":
        replay = SpikeMemoryNoEntryMaxReplay(ticks, initial_total, policy)
    elif policy == "spike_memory_plus_inside_no_entry_max":
        replay = SpikeMemoryPlusNoEntryMaxReplay(ticks, initial_total, policy)
    elif policy in {
        "spike_memory_near_band_entry_max_030",
        "spike_memory_near_band_entry_max_032",
    }:
        replay = SpikeMemoryEntryMaxWidthReplay(ticks, initial_total, policy)
    elif policy in {
        "spike_memory_plus_inside_entry_max_030",
        "spike_memory_plus_inside_entry_max_032",
    }:
        replay = SpikeMemoryPlusEntryMaxWidthReplay(ticks, initial_total, policy)
    elif policy in {
        "spike_memory_tier_inside_022",
        "spike_memory_tier_inside_024",
    }:
        replay = SpikeMemoryTieredInsideReplay(ticks, initial_total, policy)
    elif policy in {
        "spike_memory_plus_sweep",
        "spike_memory_plus_inside",
        "spike_memory_plus_trend",
        "spike_memory_d_safe",
        "spike_memory_near_band_plus",
    }:
        replay = SpikeMemoryPlusReplay(ticks, initial_total, policy)
    else:
        raise ValueError(f"Unknown policy: {policy}")
    replay.run()
    return replay.report()


def write_csv(path: Path, rows: list[dict]) -> None:
    """Write dictionaries to CSV."""
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_report(path: Path, rows: list[dict], sample_sec: int) -> None:
    """Write a Markdown report."""
    lines = [
        "# Spike Entry Window Backtest",
        "",
        f"Local logs only. sample_sec={sample_sec}.",
        "",
        "|Policy|PnL|Return|Trades|Win|Avg PnL|Avg Batches|Avg Hold|Max DD|Min Liq|Wipeout|L2 Stop|L1 Fallback|DTP|Signals|GapBlock|SameK|CloseK|HeadSeen|HeadEntries|AddonSeen|AddonEntries|TPSeen|TPLocks|SweepBlock|InsideBlock|TrendAddonBlock|",
        "|-|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|",
    ]
    for row in rows:
        lines.append(
            f"|{row['policy']}|{row['total_pnl']}|{row['return_pct']}|{row['trades']}|"
            f"{row['win_rate_pct']}|{row['avg_pnl']}|{row['avg_batches']}|"
            f"{row['avg_hold_minutes']}|{row['max_drawdown_pct']}|"
            f"{row['min_liq_distance_pct']}|{row['wipeout_risk']}|"
            f"{row.get('fixed_cycle_loss_stop', 0)}|{row.get('liquidation_guard_fallback_stop', 0)}|"
            f"{row.get('dynamic_tp_activated', 0)}|"
            f"{row.get('entry_signals', 0)}|{row['blocked_gap']}|"
            f"{row['same_k_block']}|{row['close_kline_block']}|"
            f"{row.get('spike_seen', 0)}|{row.get('spike_entries', 0)}|"
            f"{row.get('addon_spike_seen', 0)}|{row.get('addon_spike_entries', 0)}|"
            f"{row.get('tp_spike_seen', 0)}|{row.get('tp_spike_locks', 0)}|"
            f"{row.get('sweep_rejected', 0)}|{row.get('extreme_back_inside_rejected', 0)}|"
            f"{row.get('plus_trend_addon_blocks', 0)}|"
        )
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Evaluate spike-entry variants on local logs.")
    parser.add_argument("--local-log-dir", default=str(DEFAULT_LOCAL_LOG_DIR))
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--sample-sec", type=int, default=3)
    parser.add_argument("--cache-dir", default=str(ROOT / "backtest" / ".cache" / "spike_entry_window"))
    parser.add_argument("--ticks-path", default="")
    parser.add_argument("--initial-total-equity", type=float, default=INITIAL_TOTAL_EQUITY)
    parser.add_argument(
        "--policies",
        default="current,no_new_extreme_3,spike_memory_outside,spike_memory_near_band",
    )
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    policies = [item.strip() for item in args.policies.split(",") if item.strip()]
    ticks_path = Path(args.ticks_path) if args.ticks_path else out_dir / f"local_ticks_{args.sample_sec}s.pkl"
    if ticks_path.exists():
        print(f"Loading extracted ticks: {ticks_path}", flush=True)
        ticks = pd.read_pickle(ticks_path)
    else:
        print("Extracting local log ticks", flush=True)
        ticks = load_or_parse_logs(
            Path(args.local_log_dir),
            sample_sec=args.sample_sec,
            cache_dir=Path(args.cache_dir) if args.cache_dir else None,
            quiet=False,
        )
        ticks.to_pickle(ticks_path)
        print(
            f"Saved extracted ticks: {ticks_path} rows={len(ticks)} "
            f"range={ticks['ts'].min()} -> {ticks['ts'].max()}",
            flush=True,
        )
    rows = []
    for policy in policies:
        print(f"Testing policy: {policy}", flush=True)
        rows.append(replay_policy(ticks, policy, args.initial_total_equity))
    write_csv(out_dir / "spike_entry_window_rows.csv", rows)
    write_report(out_dir / "spike_entry_window_report.md", rows, args.sample_sec)
    print(f"rows={out_dir / 'spike_entry_window_rows.csv'}")
    print(f"report={out_dir / 'spike_entry_window_report.md'}")


if __name__ == "__main__":
    main()
