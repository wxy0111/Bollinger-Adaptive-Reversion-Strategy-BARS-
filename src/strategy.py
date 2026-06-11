"""Live Bollinger-band mean-reversion strategy.

The strategy watches 15-minute Bollinger bands on ``ETH-USDT-SWAP``. When mark
price moves outside the band and stops making new extremes, it places a batch
limit-entry plan. Filled positions receive dynamic take-profit orders based on
the exchange average entry price and a liquidation-line stop order.
"""
import asyncio
import json
import math
import time
import aiohttp
import pandas as pd
from pathlib import Path
from loguru import logger

from src.config import (
    INST_ID, BAR_15M, LEVER, KLINE_LIMIT,
    BOLL_INCLUDE_CURRENT,
    PRICE_LOG_INTERVAL, POLL_INTERVAL, BOLL_PERIOD,
    TP_PROFIT_USD, MIN_ENTRY_GAP_USD,
    MIN_BOLL_WIDTH_USD, MIN_BOLL_WIDTH_PCT,
    BOLL_WIDTH_BASE_PRICE, BOLL_WIDTH_BASE_USD,
    MIN_BOLL_WIDTH_FLOOR_USD, BOLL_WIDTH_GAP_MULT,
    BOLL_WIDTH_TP_SPACE_ENABLED, BOLL_WIDTH_TP_SPACE_MULT,
    ENTRY_MAX_BOLL_WIDTH_FILTER_ENABLED, ENTRY_MAX_BOLL_WIDTH_PCT,
    ENTRY_MAX_BOLL_WIDTH_USD,
    ENTRY_DISASTER_FILTER_ENABLED, ENTRY_DISASTER_SCORE_THRESHOLD,
    ENTRY_DISASTER_KLINE_COUNT, ENTRY_DISASTER_WIDTH_EXPAND,
    ENTRY_DISASTER_TP_DISTANCE_MULT, ENTRY_DISASTER_EXPECTED_RETURN,
    TP_TARGET_MARGIN_RETURN, DYNAMIC_TP_ENABLED,
    DYNAMIC_TP_ARM_RETURN, DYNAMIC_TP_RESTORE_RETURN,
    DYNAMIC_TP_REPRICE_GAP_USD,
    BOLL_TP_COMPRESSION_ENABLED, BOLL_TP_COMPRESSION_MIN_RETURN,
    BOLL_TP_COMPRESSION_EXIT_OFFSET_USD,
    MIN_HEAD_LIQ_BUFFER_PCT, DYNAMIC_ENTRY_GAP_ENABLED,
    DYNAMIC_ENTRY_GAP_MAX_USD, OKX_MAINTENANCE_MARGIN_RATE,
    OKX_LIQ_FEE_RATE,
    ADDON_DYNAMIC_GAP_ENABLED, ADDON_DYNAMIC_GAP_MAX_USD,
    ADDON_DYNAMIC_GAP_BOLL_START, ADDON_DYNAMIC_GAP_BOLL_STRONG,
    ADDON_DYNAMIC_GAP_BOLL_MAX_MULT,
    ADDON_DYNAMIC_GAP_HEAD_START_PCT, ADDON_DYNAMIC_GAP_HEAD_STRONG_PCT,
    ADDON_DYNAMIC_GAP_HEAD_MAX_MULT,
    ADDON_DYNAMIC_GAP_TREND_KLINES, ADDON_DYNAMIC_GAP_TREND_MULT,
    ADDON_MAX_BOLL_WIDTH_FILTER_ENABLED, ADDON_MAX_BOLL_WIDTH_PCT,
    ADDON_MAX_BOLL_WIDTH_USD,
    ADDON_TP_IMPROVE_GUARD_ENABLED, ADDON_TP_IMPROVE_EXPECTED_RETURN,
    ADDON_TP_IMPROVE_RATIO, ADDON_TP_IMPROVE_MIN_USD,
    ADDON_EXTREME_GUARD_ENABLED,
    ENTRY_EXTREME_GAP_ADJUST_ENABLED, ENTRY_EXTREME_GAP_BASE_PCT,
    ENTRY_EXTREME_GAP_FULL_PCT, ENTRY_EXTREME_GAP_MAX_MULT,
    ENTRY_24H_TICKER_CACHE_SEC,
    LIQ_STOP_OFFSET_USD, LIQ_STOP_REPRICE_GAP_USD, LIQ_WARNING_DISTANCE_USD,
    LIQ_WARNING_REPEAT_SEC,
    NO_NEW_EXTREME_TICKS,
    REPRICE_GAP_USD, INSIDE_BAND_CANCEL_KLINES,
    STRATEGY_EQUITY_CAP_USDT, CT_VAL, CONTRACT_STEP,
    TRADING_ACCOUNT_TARGET,
    ROLLING_COMPOUND_ENABLED,
    CROSS_COPY_PROTECT_ENABLED, CROSS_COPY_PROTECT_EQUITY_USDT,
    CROSS_COPY_DYNAMIC_SIZING_ENABLED,
    SIZING_EQUITY_LOG_THRESHOLD_USDT,
    CAPITAL_REBALANCE_TOLERANCE_USDT, CAPITAL_REBALANCE_DELAY_SEC,
    COPY_FIXED_LOSS_STOP_ENABLED, COPY_FIXED_LOSS_STOP_USDT,
    COPY_FIXED_LOSS_STOP_RATIO,
    FIXED_LOSS_HEAD_BUFFER_ENABLED, FIXED_LOSS_HEAD_BUFFER_PCT,
    DISASTER_STOP_ENABLED, DISASTER_HEAD_DROP_PCT, DISASTER_LOSS_RATIO,
    BOLL_MID_COST_STOP_ENABLED,
    TREND_RISK_GUARD_ENABLED, TREND_RISK_GUARD_CLOSE_ENABLED,
    TREND_RISK_FREEZE_ADDON_ENABLED,
    TREND_RISK_SCORE_THRESHOLD,
    TREND_RISK_HEAD_ADVERSE_PCT, TREND_RISK_KLINE_COUNT,
    TREND_RISK_MIN_HOLD_MIN, TREND_RISK_SLOPE_WINDOW_MIN,
    TREND_RISK_MID_SLOPE_PCT_PER_HOUR, TREND_RISK_EDGE_SLOPE_PCT_PER_HOUR,
    TREND_RISK_WIDTH_EXPAND, TREND_RISK_NOTIFY_INTERVAL_SEC,
    MAX_ENTRY_BATCHES, MAX_TOTAL_ENTRY_RATIO,
    FIRST_BATCH_RATIO,
    SECOND_BATCH_DYNAMIC_BASE_RATIO,
    SECOND_BATCH_DYNAMIC_MIN_RATIO, SECOND_BATCH_DYNAMIC_MAX_RATIO,
    SECOND_BATCH_DYNAMIC_FULL_GAP_USD,
    DYNAMIC_BASE_ENTRY_RATIO, DYNAMIC_MIN_ENTRY_RATIO, DYNAMIC_MAX_ENTRY_RATIO,
)
from src.okx_client import OKXClient
from src.indicators import build_df, add_boll
from src.risk import build_batch_plan
from src.position_manager import PositionState, OpenBatch
from src.notify import (
    notify_entry_order, notify_open, notify_close, notify_liq_warning,
    notify_capital_shortage, notify_capital_restored,
    notify_cross_copy_protect, notify_trend_risk_guard,
)
from src.logging_utils import log_action, log_check, log_market
import src.dashboard as dashboard


STATE_FILE = Path("logs/runtime_state.json")
COOLDOWN_FILE = Path("logs/close_cooldown.json")


class BollPinStrategy:
    """Stateful live trading strategy.

    The exchange position is treated as the source of truth for size, average
    entry, and liquidation price. Local state is used to track strategy-owned
    entry batches, exit orders, and restart recovery metadata.
    """

    def __init__(self):
        """Initialize local strategy state."""
        self._state   = PositionState(direction="none")
        self._peak_eq = 0.0
        self._running = False
        self._last_plan_kline_ts = None
        self._last_plan_entry_price = 0.0
        self._last_batch_kline_ts = None
        self._last_recovery_kline_ts = None
        self._last_entry_check_kline_ts = None
        self._last_close_kline_ts = None
        self._probe_kline_ts = None
        self._probe_direction = "none"
        self._probe_entry_price = 0.0
        self._inside_band_kline_count = 0
        self._last_inside_band_kline_ts = None
        self._recent_prices = []
        self._sizing_equity = 0.0
        self._fixed_batch_sizes = []
        self._restored_from_file = False
        self._capital_shortage_active = False
        self._last_liq_warning_ts = 0.0
        self._last_liq_warning_gap_usd = None
        self._dynamic_tp_active = False
        self._entry_extreme_gap_pct = 0.0
        self._entry_extreme_gap_mult = 1.0
        self._ticker_24h_cache = None
        self._ticker_24h_cache_ts = 0.0
        self._addon_extreme_guard_price = 0.0
        self._addon_extreme_guard_kline_ts = None
        self._addon_extreme_guard_batch_idx = -1
        self._addon_extreme_guard_started = False
        self._boll_history = []
        self._gap_context_df = None
        self._gap_context_row = None
        self._trend_entry_width = 0.0
        self._trend_entry_width_pct = 0.0
        self._trend_entry_time = None
        self._trend_last_notify_ts = 0.0
        self._trend_risk_guard_active = False

    async def run(self):
        """Run the strategy loop until stopped."""
        self._running = True
        logger.info("Strategy started: {} Bollinger mean-reversion {}x", INST_ID, LEVER)

        async with aiohttp.ClientSession() as session:
            client = OKXClient(session)
            try:
                await client.set_leverage(INST_ID, LEVER)
            except Exception as e:
                logger.warning(f"Set leverage failed; please verify {LEVER}x in OKX App: {e}")
            self._load_runtime_state()
            self._load_close_cooldown()
            await self._ensure_fixed_batch_sizes(client)
            await self._sync_state(client)

            last_strategy_tick = 0.0
            while self._running:
                try:
                    now = time.monotonic()
                    if now - last_strategy_tick >= POLL_INTERVAL:
                        await self._tick(client)
                        last_strategy_tick = time.monotonic()
                    else:
                        await self._log_market_snapshot(client)
                except Exception as e:
                    logger.exception(f"tick 异常: {e}")
                await asyncio.sleep(PRICE_LOG_INTERVAL)


    def _desired_sizing_equity(self, account_equity: float) -> float:
        """Return the fixed equity base used to size strategy batches."""
        if ROLLING_COMPOUND_ENABLED:
            protected = CROSS_COPY_PROTECT_EQUITY_USDT if CROSS_COPY_PROTECT_ENABLED else 0.0
            base_equity = max(account_equity - protected, 0.0)
        elif CROSS_COPY_DYNAMIC_SIZING_ENABLED:
            protected = CROSS_COPY_PROTECT_EQUITY_USDT if CROSS_COPY_PROTECT_ENABLED else 0.0
            available_for_strategy = max(account_equity - protected, 0.0)
            if TRADING_ACCOUNT_TARGET > 0:
                base_equity = min(TRADING_ACCOUNT_TARGET, available_for_strategy)
            else:
                base_equity = available_for_strategy
        elif self._capital_shortage_active and TRADING_ACCOUNT_TARGET > 0:
            base_equity = min(account_equity, TRADING_ACCOUNT_TARGET)
        else:
            base_equity = TRADING_ACCOUNT_TARGET if TRADING_ACCOUNT_TARGET > 0 else account_equity
        if STRATEGY_EQUITY_CAP_USDT > 0:
            return min(base_equity, STRATEGY_EQUITY_CAP_USDT)
        return base_equity

    def _sizing_equity_log_threshold(self) -> float:
        """Return the minimum sizing-equity change worth printing."""
        target_threshold = TRADING_ACCOUNT_TARGET * 0.10 if TRADING_ACCOUNT_TARGET > 0 else 0.0
        return max(SIZING_EQUITY_LOG_THRESHOLD_USDT, target_threshold)

    async def _sizing_account_equity(self, client: OKXClient) -> float:
        """Return the account value used for sizing decisions."""
        if ROLLING_COMPOUND_ENABLED or CROSS_COPY_DYNAMIC_SIZING_ENABLED:
            return await client.get_equity("USDT")
        return await client.get_balance("USDT")

    def _strategy_risk_equity(self) -> float:
        """Return the capital base used by cycle-level risk controls."""
        if ROLLING_COMPOUND_ENABLED:
            return max(self._sizing_equity, 0.0)
        if TRADING_ACCOUNT_TARGET > 0:
            return TRADING_ACCOUNT_TARGET
        return max(self._sizing_equity, 0.0)

    async def _refresh_sizing_equity(self, client: OKXClient, account_equity: float) -> None:
        """Refresh dynamic sizing equity from the latest account equity."""
        desired_equity = self._desired_sizing_equity(account_equity)
        if abs(self._sizing_equity - desired_equity) <= 0.01:
            return

        previous = self._sizing_equity
        self._sizing_equity = desired_equity
        if self._state.batches:
            if self._sync_known_batch_sizes():
                self._save_runtime_state()
            if abs(previous - desired_equity) >= self._sizing_equity_log_threshold():
                log_check(
                    f"Sizing equity refreshed {previous:.2f} -> {desired_equity:.2f}; "
                    f"known sizes={self._fixed_batch_sizes}"
                )
            return

        await self._init_fixed_batch_sizes(
            client,
            account_equity=account_equity,
            sizing_equity=desired_equity,
        )

    async def _check_cross_copy_protection(self, client: OKXClient, account_equity: float) -> bool:
        """Close and stop when account equity reaches the protected line."""
        if not CROSS_COPY_PROTECT_ENABLED:
            return False
        if CROSS_COPY_PROTECT_EQUITY_USDT <= 0:
            return False
        if account_equity <= 0:
            logger.warning(
                "Account equity read as 0; skip cross copy protection for this tick"
            )
            return False
        if account_equity > CROSS_COPY_PROTECT_EQUITY_USDT:
            return False

        log_action(
            f"Cross copy protection triggered equity={account_equity:.2f} "
            f"protected={CROSS_COPY_PROTECT_EQUITY_USDT:.2f}; cancel orders and stop"
        )
        direction = self._state.direction
        total_sz = self._state.total_sz
        await self._cancel_entry_orders(client)
        await self._cancel_exchange_exit_orders(client)
        await self._cancel_exit_orders(client)
        self._reset_probe_state()
        self._state.reset()
        self._clear_runtime_state()
        self._running = False
        await notify_cross_copy_protect(
            account_equity,
            CROSS_COPY_PROTECT_EQUITY_USDT,
            direction,
            total_sz,
        )
        return True

    def _head_adverse_move_pct(self, mark_price: float) -> float:
        """Return mark-price adverse move from the first filled batch."""
        head_price = self._head_entry_price(mark_price)
        if head_price <= 0 or mark_price <= 0:
            return 0.0
        if self._state.direction == "long":
            return max((head_price - mark_price) / head_price, 0.0)
        if self._state.direction == "short":
            return max((mark_price - head_price) / head_price, 0.0)
        return 0.0

    def _remember_boll_snapshot(self, row, mark_price: float) -> None:
        """Keep recent Bollinger and candle-shape data for trend-risk checks."""
        if mark_price <= 0:
            return
        lower = float(row["boll_lower"])
        mid = float(row["boll_mid"])
        upper = float(row["boll_upper"])
        width = upper - lower
        half_width = upper - mid
        z_score = (mark_price - mid) / half_width if half_width else 0.0
        self._boll_history.append(
            {
                "ts": pd.Timestamp(row["ts"]),
                "high": float(row.get("high", mark_price) or mark_price),
                "low": float(row.get("low", mark_price) or mark_price),
                "lower": lower,
                "mid": mid,
                "upper": upper,
                "width": width,
                "width_pct": width / mark_price,
                "z": z_score,
            }
        )
        keep_after = pd.Timestamp(row["ts"]) - pd.Timedelta(
            minutes=max(TREND_RISK_SLOPE_WINDOW_MIN * 2, 180)
        )
        while self._boll_history and self._boll_history[0]["ts"] < keep_after:
            self._boll_history.pop(0)

    def _record_trend_entry_reference(self, row, mark_price: float) -> None:
        """Record the Bollinger width at the first filled batch."""
        if not self._state.is_active() or self._trend_entry_width > 0:
            return
        width = float(row["boll_upper"] - row["boll_lower"])
        if width <= 0 or mark_price <= 0:
            return
        self._trend_entry_width = width
        self._trend_entry_width_pct = width / mark_price
        self._trend_entry_time = pd.Timestamp(row["ts"])
        self._trend_last_notify_ts = 0.0
        self._save_runtime_state()

    def _series_slope_pct_per_hour(self, values: list[float], start_ts, end_ts) -> float:
        """Return percent-per-hour slope across a time window."""
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
        for item in reversed(self._boll_history):
            ts = item["ts"]
            if ts in seen:
                continue
            unique.append(item)
            seen.add(ts)
            if len(unique) >= count:
                break
        return list(reversed(unique))

    def _trend_risk_signal(self, row, mark_price: float) -> dict | None:
        """Return trend-risk metrics when adverse trend conditions stack up."""
        if not TREND_RISK_GUARD_ENABLED:
            return None
        if not self._state.is_active():
            return None
        if self._trend_entry_width <= 0:
            self._record_trend_entry_reference(row, mark_price)
            return None
        if self._trend_entry_time is None:
            self._trend_entry_time = pd.Timestamp(row["ts"])
            return None

        now = pd.Timestamp(row["ts"])
        hold_min = (now - pd.Timestamp(self._trend_entry_time)).total_seconds() / 60
        if hold_min < TREND_RISK_MIN_HOLD_MIN:
            return None

        adverse_pct = self._head_adverse_move_pct(mark_price)
        if adverse_pct < TREND_RISK_HEAD_ADVERSE_PCT:
            return None

        width = float(row["boll_upper"] - row["boll_lower"])
        width_expand = width / self._trend_entry_width if self._trend_entry_width > 0 else 0.0
        width_pct = width / mark_price if mark_price > 0 else 0.0
        mid = float(row["boll_mid"])

        window_start = now - pd.Timedelta(minutes=TREND_RISK_SLOPE_WINDOW_MIN)
        window = [item for item in self._boll_history if item["ts"] >= window_start]
        if len(window) < 2:
            return None

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

        recent = self._recent_unique_boll_history(max(TREND_RISK_KLINE_COUNT, 2))
        lows = [item["low"] for item in recent]
        highs = [item["high"] for item in recent]
        lower_lows = len(lows) >= TREND_RISK_KLINE_COUNT and all(
            lows[i] < lows[i - 1] for i in range(1, len(lows))
        )
        higher_highs = len(highs) >= TREND_RISK_KLINE_COUNT and all(
            highs[i] > highs[i - 1] for i in range(1, len(highs))
        )

        reasons = ["head_adverse"]
        if width_expand >= TREND_RISK_WIDTH_EXPAND:
            reasons.append("width_expand")

        if self._state.direction == "long":
            if mark_price < mid:
                reasons.append("below_mid")
            if lower_lows:
                reasons.append("lower_lows")
            if mid_slope <= -TREND_RISK_MID_SLOPE_PCT_PER_HOUR:
                reasons.append("mid_slope_down")
            if lower_slope <= -TREND_RISK_EDGE_SLOPE_PCT_PER_HOUR:
                reasons.append("lower_band_down")
        elif self._state.direction == "short":
            if mark_price > mid:
                reasons.append("above_mid")
            if higher_highs:
                reasons.append("higher_highs")
            if mid_slope >= TREND_RISK_MID_SLOPE_PCT_PER_HOUR:
                reasons.append("mid_slope_up")
            if upper_slope >= TREND_RISK_EDGE_SLOPE_PCT_PER_HOUR:
                reasons.append("upper_band_up")
        else:
            return None

        score = len(reasons)
        if score < TREND_RISK_SCORE_THRESHOLD:
            return None

        return {
            "head_price": self._head_entry_price(mark_price),
            "mark_price": mark_price,
            "adverse_pct": adverse_pct,
            "width_expand": width_expand,
            "width_pct": width_pct,
            "mid_slope": mid_slope,
            "lower_slope": lower_slope,
            "upper_slope": upper_slope,
            "score": score,
            "reasons": reasons,
            "hold_min": hold_min,
        }

    async def _check_trend_risk_guard(self, client: OKXClient, row, mark_price: float) -> bool:
        """Handle stacked trend-risk signals for the current position."""
        signal = self._trend_risk_signal(row, mark_price)
        if signal is None:
            return False

        if not self._trend_risk_guard_active:
            self._trend_risk_guard_active = True
            log_check(
                "Trend risk guard active; freeze add-on orders "
                f"direction={self._state.direction} score={signal['score']}"
            )
            self._save_runtime_state()

        now = time.time()
        should_notify = now - self._trend_last_notify_ts >= TREND_RISK_NOTIFY_INTERVAL_SEC
        if should_notify:
            log_action(
                "Trend risk guard triggered "
                f"direction={self._state.direction} mark={mark_price:.2f} "
                f"head={signal['head_price']:.2f} adverse={signal['adverse_pct']:.2%} "
                f"score={signal['score']} reasons={','.join(signal['reasons'])} "
                f"width_expand={signal['width_expand']:.3f} "
                f"width_pct={signal['width_pct']:.2%} "
                f"mid_slope={signal['mid_slope']:.3f}%/h "
                f"lower_slope={signal['lower_slope']:.3f}%/h "
                f"upper_slope={signal['upper_slope']:.3f}%/h"
            )
            self._trend_last_notify_ts = now
            self._save_runtime_state()
            await notify_trend_risk_guard(
                self._state.direction,
                mark_price,
                signal["head_price"],
                signal["adverse_pct"],
                signal["score"],
                signal["reasons"],
                signal["width_expand"],
                signal["width_pct"],
                signal["mid_slope"],
                signal["lower_slope"],
                signal["upper_slope"],
                TREND_RISK_GUARD_CLOSE_ENABLED,
            )

        if not TREND_RISK_GUARD_CLOSE_ENABLED:
            return False

        log_action("Trend risk guard close; strategy keeps running")
        await self._emergency_close(client, reason="trend_risk_guard")
        return True

    def _strategy_unrealized_pnl(self, mark_price: float) -> float:
        """Return current unrealized PnL for this strategy position."""
        if self._state.avg_entry <= 0 or self._state.total_sz <= 0:
            return 0.0
        if self._state.direction == "long":
            return (mark_price - self._state.avg_entry) * self._state.total_sz * CT_VAL
        if self._state.direction == "short":
            return (self._state.avg_entry - mark_price) * self._state.total_sz * CT_VAL
        return 0.0

    async def _check_disaster_stop(self, client: OKXClient, mark_price: float) -> bool:
        """Close the current cycle when disaster risk limits are reached."""
        if not DISASTER_STOP_ENABLED:
            return False
        if not self._state.is_active():
            return False
        risk_equity = self._strategy_risk_equity()
        if risk_equity <= 0:
            return False
        if DISASTER_HEAD_DROP_PCT <= 0 or DISASTER_LOSS_RATIO <= 0:
            return False

        head_move_pct = self._head_adverse_move_pct(mark_price)
        unrealized_pnl = self._strategy_unrealized_pnl(mark_price)
        loss_threshold = risk_equity * DISASTER_LOSS_RATIO
        if head_move_pct < DISASTER_HEAD_DROP_PCT:
            return False
        if unrealized_pnl > -loss_threshold:
            return False

        log_action(
            "Disaster stop triggered "
            f"direction={self._state.direction} mark={mark_price:.2f} "
            f"head={self._head_entry_price(mark_price):.2f} "
            f"head_move={head_move_pct:.2%} "
            f"unrealized={unrealized_pnl:+.4f} USDT "
            f"threshold={loss_threshold:.4f} USDT"
        )
        await self._emergency_close(client, reason="disaster_stop")
        return True

    async def _check_boll_mid_cost_stop(self, client: OKXClient, row) -> bool:
        """Market-close when Bollinger mid crosses the position average entry."""
        if not BOLL_MID_COST_STOP_ENABLED:
            return False
        if not self._state.is_active():
            return False
        if self._state.avg_entry <= 0:
            return False

        direction = self._state.direction
        if direction not in ("long", "short"):
            return False

        try:
            boll_mid = float(row.get("boll_mid", 0) or 0)
        except (TypeError, ValueError, AttributeError):
            return False
        if boll_mid <= 0:
            return False

        avg_entry = self._state.avg_entry
        triggered = (
            (direction == "long" and boll_mid <= avg_entry)
            or (direction == "short" and boll_mid >= avg_entry)
        )
        if not triggered:
            return False

        log_action(
            "Boll mid cost stop triggered "
            f"direction={direction} boll_mid={boll_mid:.2f} "
            f"avg_entry={avg_entry:.2f} sz={self._state.total_sz}"
        )
        await self._emergency_close(client, reason="boll_mid_cost_stop")
        return True

    def _floor_contract_size(self, raw_sz: float) -> float:
        """Floor a raw contract size to the exchange contract step."""
        step_count = int(raw_sz / CONTRACT_STEP)
        return round(step_count * CONTRACT_STEP, 8)

    def _set_fixed_batch_size(self, batch_idx: int, sz: float) -> None:
        """Store a planned size for a batch index."""
        while len(self._fixed_batch_sizes) <= batch_idx:
            self._fixed_batch_sizes.append(0.0)
        self._fixed_batch_sizes[batch_idx] = sz

    def _used_entry_ratio(self, exclude_batch_idx: int | None = None) -> float:
        """Estimate used entry margin ratio from filled and pending batches."""
        if self._sizing_equity <= 0:
            return 0.0
        used_margin = 0.0
        for batch in self._state.batches:
            if exclude_batch_idx is not None and batch.batch_idx == exclude_batch_idx:
                continue
            if batch.sz <= 0 or batch.price <= 0:
                continue
            used_margin += batch.sz * CT_VAL * batch.price / LEVER
        return used_margin / self._sizing_equity

    def _dynamic_entry_ratio(self, batch_idx: int, candidate_price: float) -> float:
        """Calculate the next entry margin ratio from recent price gaps."""
        if batch_idx == 0:
            return FIRST_BATCH_RATIO
        if batch_idx == 1:
            head_batch = next(
                (batch for batch in self._state.filled_batches() if batch.batch_idx == 0),
                None,
            )
            if head_batch is None or head_batch.price <= 0 or SECOND_BATCH_DYNAMIC_FULL_GAP_USD <= 0:
                return 0.0
            gap = abs(candidate_price - head_batch.price)
            dynamic_ratio = SECOND_BATCH_DYNAMIC_BASE_RATIO * gap / SECOND_BATCH_DYNAMIC_FULL_GAP_USD
            dynamic_ratio = max(SECOND_BATCH_DYNAMIC_MIN_RATIO, dynamic_ratio)
            dynamic_ratio = min(SECOND_BATCH_DYNAMIC_MAX_RATIO, dynamic_ratio)
            return dynamic_ratio

        filled = sorted(self._state.filled_batches(), key=lambda batch: batch.batch_idx)
        if len(filled) < 2:
            dynamic_ratio = DYNAMIC_BASE_ENTRY_RATIO
        else:
            prev_batch = filled[-2]
            last_batch = filled[-1]
            prev_gap = abs(last_batch.price - prev_batch.price)
            current_gap = abs(candidate_price - last_batch.price)
            if prev_gap <= 0:
                dynamic_ratio = DYNAMIC_BASE_ENTRY_RATIO
            else:
                dynamic_ratio = DYNAMIC_BASE_ENTRY_RATIO * (current_gap / prev_gap)
        dynamic_ratio = max(DYNAMIC_MIN_ENTRY_RATIO, dynamic_ratio)
        dynamic_ratio = min(DYNAMIC_MAX_ENTRY_RATIO, dynamic_ratio)
        return dynamic_ratio

    def _prepare_dynamic_batch_size(self, batch_idx: int, candidate_price: float) -> bool:
        """Calculate and store the dynamic size for the next planned batch."""
        if batch_idx >= MAX_ENTRY_BATCHES:
            return False
        if candidate_price <= 0 or self._sizing_equity <= 0:
            return False

        ratio = self._dynamic_entry_ratio(batch_idx, candidate_price)
        if ratio <= 0:
            return False

        margin_budget = self._sizing_equity * ratio
        raw_sz = margin_budget * LEVER / (candidate_price * CT_VAL)
        sz = self._floor_contract_size(raw_sz)
        if sz <= 0:
            return False

        used_ratio = self._used_entry_ratio(exclude_batch_idx=batch_idx)
        candidate_margin = candidate_price * sz * CT_VAL / LEVER
        candidate_ratio = candidate_margin / self._sizing_equity
        if used_ratio + candidate_ratio > MAX_TOTAL_ENTRY_RATIO:
            log_check(
                f"Dynamic batch skipped: batch={batch_idx + 1} "
                f"used={used_ratio:.2%} candidate={candidate_ratio:.2%} "
                f"limit={MAX_TOTAL_ENTRY_RATIO:.2%}"
            )
            return False

        if not self._fixed_loss_head_buffer_allows(batch_idx, candidate_price, sz):
            return False

        if not self._addon_tp_improve_allows(batch_idx, candidate_price, sz):
            return False

        self._set_fixed_batch_size(batch_idx, sz)
        log_check(
            f"Dynamic batch prepared: batch={batch_idx + 1} "
            f"ratio={ratio:.2%} price={candidate_price:.2f} sz={sz}"
        )
        return True

    def _fixed_loss_target_usdt(self) -> float:
        """Return the configured fixed-loss amount for one strategy cycle."""
        if not COPY_FIXED_LOSS_STOP_ENABLED:
            return 0.0
        target_loss = COPY_FIXED_LOSS_STOP_USDT
        if target_loss <= 0:
            target_loss = self._strategy_risk_equity() * COPY_FIXED_LOSS_STOP_RATIO
        return max(target_loss, 0.0)

    def _fixed_loss_stop_price(self, direction: str, avg_entry: float, total_sz: float) -> float:
        """Return the fixed-loss stop price for a simulated position."""
        target_loss = self._fixed_loss_target_usdt()
        if target_loss <= 0 or avg_entry <= 0 or total_sz <= 0:
            return 0.0
        price_delta = target_loss / (total_sz * CT_VAL)
        if direction == "long":
            return avg_entry - price_delta
        if direction == "short":
            return avg_entry + price_delta
        return 0.0

    def _simulated_entry_totals(self, batch_idx: int, candidate_price: float, candidate_sz: float):
        """Return average entry and size after replacing/adding one batch."""
        entries = [
            batch for batch in self._state.batches
            if batch.batch_idx != batch_idx and batch.price > 0 and batch.sz > 0
        ]
        entries.append(OpenBatch(
            batch_idx=batch_idx,
            ord_id="simulated",
            price=candidate_price,
            sz=candidate_sz,
            filled=False,
        ))
        total_sz = sum(batch.sz for batch in entries)
        if total_sz <= 0:
            return 0.0, 0.0
        avg_entry = sum(batch.price * batch.sz for batch in entries) / total_sz
        return avg_entry, total_sz

    def _fixed_loss_head_buffer_allows(self, batch_idx: int, candidate_price: float, candidate_sz: float) -> bool:
        """Return whether an add-on keeps fixed-loss stop beyond head buffer."""
        if not FIXED_LOSS_HEAD_BUFFER_ENABLED or batch_idx <= 0:
            return True
        if self._state.direction not in ("long", "short"):
            return True
        head_price = self._head_entry_price(candidate_price)
        if head_price <= 0 or candidate_price <= 0 or candidate_sz <= 0:
            return True

        avg_entry, total_sz = self._simulated_entry_totals(batch_idx, candidate_price, candidate_sz)
        stop_price = self._fixed_loss_stop_price(self._state.direction, avg_entry, total_sz)
        if stop_price <= 0:
            return True

        if self._state.direction == "long":
            required_stop = head_price * (1 - FIXED_LOSS_HEAD_BUFFER_PCT)
            if stop_price <= required_stop:
                return True
            log_check(
                f"Fixed-loss head buffer skipped: batch={batch_idx + 1} "
                f"stop={stop_price:.2f} must<= {required_stop:.2f} "
                f"head={head_price:.2f} buffer={FIXED_LOSS_HEAD_BUFFER_PCT:.2%}"
            )
            return False

        required_stop = head_price * (1 + FIXED_LOSS_HEAD_BUFFER_PCT)
        if stop_price >= required_stop:
            return True
        log_check(
            f"Fixed-loss head buffer skipped: batch={batch_idx + 1} "
            f"stop={stop_price:.2f} must>= {required_stop:.2f} "
            f"head={head_price:.2f} buffer={FIXED_LOSS_HEAD_BUFFER_PCT:.2%}"
        )
        return False

    def _addon_expected_tp_price(self, direction: str, avg_entry: float) -> float:
        """Return the guard's expected take-profit price for an average entry."""
        if avg_entry <= 0 or LEVER <= 0 or ADDON_TP_IMPROVE_EXPECTED_RETURN <= 0:
            return 0.0
        distance = avg_entry * ADDON_TP_IMPROVE_EXPECTED_RETURN / LEVER
        if direction == "long":
            return avg_entry + distance
        if direction == "short":
            return avg_entry - distance
        return 0.0

    def _addon_tp_improve_allows(self, batch_idx: int, candidate_price: float, candidate_sz: float) -> bool:
        """Return whether an add-on meaningfully improves the expected TP price."""
        if not ADDON_TP_IMPROVE_GUARD_ENABLED or batch_idx <= 0:
            return True
        if self._state.direction not in ("long", "short") or not self._state.is_active():
            return True
        if self._state.avg_entry <= 0 or self._state.total_sz <= 0 or candidate_sz <= 0:
            return True

        old_avg = self._state.avg_entry
        old_tp = self._addon_expected_tp_price(self._state.direction, old_avg)
        new_avg, _ = self._simulated_entry_totals(batch_idx, candidate_price, candidate_sz)
        new_tp = self._addon_expected_tp_price(self._state.direction, new_avg)
        if old_tp <= 0 or new_tp <= 0:
            return True

        if self._state.direction == "long":
            improve = old_tp - new_tp
        else:
            improve = new_tp - old_tp
        expected_distance = abs(old_tp - old_avg)
        required = max(ADDON_TP_IMPROVE_MIN_USD, expected_distance * ADDON_TP_IMPROVE_RATIO)
        if improve + 1e-9 >= required:
            return True

        log_check(
            f"Add-on skipped: TP improvement too small batch={batch_idx + 1} "
            f"old_tp={old_tp:.2f} new_tp={new_tp:.2f} "
            f"improve={improve:.2f} required={required:.2f} "
            f"avg={old_avg:.2f} new_avg={new_avg:.2f} "
            f"price={candidate_price:.2f} sz={candidate_sz}"
        )
        return False

    def _sync_known_batch_sizes(self) -> bool:
        """Keep only known filled or pending batch sizes in local runtime state."""
        known_batches = [batch for batch in self._state.batches if batch.batch_idx >= 0 and batch.sz > 0]
        if not known_batches:
            changed = bool(self._fixed_batch_sizes)
            self._fixed_batch_sizes = []
            return changed

        max_idx = max(batch.batch_idx for batch in known_batches)
        synced_sizes = [0.0] * (max_idx + 1)
        for batch in known_batches:
            synced_sizes[batch.batch_idx] = batch.sz

        if synced_sizes == self._fixed_batch_sizes:
            return False
        self._fixed_batch_sizes = synced_sizes
        return True

    async def _ensure_fixed_batch_sizes(self, client: OKXClient):
        """Keep known batch sizes aligned with the current sizing target."""
        account_equity = await self._sizing_account_equity(client)
        desired_equity = self._desired_sizing_equity(account_equity)
        previous_sizing_equity = self._sizing_equity
        self._sizing_equity = desired_equity
        if self._state.batches:
            changed = self._sync_known_batch_sizes()
            log_check(
                f"Known batch sizes synced sizing_equity={self._sizing_equity:.2f} "
                f"sizes={self._fixed_batch_sizes}; future add-ons use dynamic sizing"
            )
            if changed:
                self._save_runtime_state()
            return

        has_valid_sizes = len(self._fixed_batch_sizes) >= 2 and all(sz > 0 for sz in self._fixed_batch_sizes[:2])
        if has_valid_sizes and abs(previous_sizing_equity - desired_equity) <= 0.01:
            log_check(
                f"Using saved first/second batch sizes sizing_equity={self._sizing_equity:.2f} "
                f"sizes={self._fixed_batch_sizes}"
            )
            return

        if self._fixed_batch_sizes:
            log_check(
                f"Saved batch sizing_equity={previous_sizing_equity:.2f} "
                f"differs from target={desired_equity:.2f}; recalculating first/second sizes"
            )
        await self._init_fixed_batch_sizes(client, account_equity=account_equity, sizing_equity=desired_equity)

    async def _init_fixed_batch_sizes(self, client: OKXClient, account_equity: float | None = None, sizing_equity: float | None = None):
        """Calculate first and second batch sizes; later add-ons are dynamic."""
        equity = account_equity if account_equity is not None else await self._sizing_account_equity(client)
        self._sizing_equity = sizing_equity if sizing_equity is not None else self._desired_sizing_equity(equity)
        mark_price = await client.get_mark_price(INST_ID)
        batch_sizes = []
        margin_budget = self._sizing_equity * FIRST_BATCH_RATIO
        raw_sz = margin_budget * LEVER / (mark_price * CT_VAL)
        batch_sizes.append(self._floor_contract_size(raw_sz))
        self._fixed_batch_sizes = batch_sizes
        log_check(
            f"Head batch reference size available={equity:.2f} "
            f"sizing_equity={self._sizing_equity:.2f} sizes={self._fixed_batch_sizes}; "
            "actual order size is recalculated from live price before placing"
        )

    def _ts_to_str(self, value):
        """Serialize a timestamp-like value for runtime-state JSON."""
        if value is None:
            return None
        try:
            return pd.Timestamp(value).isoformat()
        except Exception:
            return str(value)

    def _str_to_ts(self, value):
        """Parse a timestamp from runtime-state JSON."""
        if not value:
            return None
        try:
            return pd.to_datetime(value)
        except Exception:
            return None

    def _state_payload(self) -> dict:
        """Build the runtime-state payload persisted to disk."""
        self._sanitize_runtime_state()
        return {
            "version": 1,
            "inst_id": INST_ID,
            "saved_at": pd.Timestamp.utcnow().isoformat(),
            "state": {
                "direction": self._state.direction,
                "batches": [
                    {
                        "batch_idx": b.batch_idx,
                        "ord_id": b.ord_id,
                        "price": b.price,
                        "sz": b.sz,
                        "filled": b.filled,
                    }
                    for b in self._state.batches
                ],
                "tp_ord_id": self._state.tp_ord_id,
                "sl_ord_id": self._state.sl_ord_id,
                "plan_liq_price": self._state.plan_liq_price,
                "plan_sl_price": self._state.plan_sl_price,
                "plan_tp_price": self._state.plan_tp_price,
                "avg_entry": self._state.avg_entry,
                "total_sz": self._state.total_sz,
                "remaining_batches_placed": self._state.remaining_batches_placed,
                "cycle_start_account_value": self._state.cycle_start_account_value,
                "cycle_start_ts": self._state.cycle_start_ts,
            },
            "strategy": {
                "peak_eq": self._peak_eq,
                "last_plan_kline_ts": self._ts_to_str(self._last_plan_kline_ts),
                "last_plan_entry_price": self._last_plan_entry_price,
                "last_batch_kline_ts": self._ts_to_str(self._last_batch_kline_ts),
                "last_recovery_kline_ts": self._ts_to_str(self._last_recovery_kline_ts),
                "last_entry_check_kline_ts": self._ts_to_str(self._last_entry_check_kline_ts),
                "probe_kline_ts": self._ts_to_str(self._probe_kline_ts),
                "probe_direction": self._probe_direction,
                "probe_entry_price": self._probe_entry_price,
                "inside_band_kline_count": self._inside_band_kline_count,
                "last_inside_band_kline_ts": self._ts_to_str(self._last_inside_band_kline_ts),
                "sizing_equity": self._sizing_equity,
                "fixed_batch_sizes": self._fixed_batch_sizes,
                "capital_shortage_active": self._capital_shortage_active,
                "dynamic_tp_active": self._dynamic_tp_active and self._state.is_active(),
                "entry_extreme_gap_pct": self._entry_extreme_gap_pct if self._state.has_working_plan() else 0.0,
                "entry_extreme_gap_mult": self._entry_extreme_gap_mult if self._state.has_working_plan() else 1.0,
                "addon_extreme_guard_price": (
                    self._addon_extreme_guard_price if self._state.has_working_plan() else 0.0
                ),
                "addon_extreme_guard_kline_ts": (
                    self._ts_to_str(self._addon_extreme_guard_kline_ts)
                    if self._state.has_working_plan() else None
                ),
                "addon_extreme_guard_batch_idx": (
                    self._addon_extreme_guard_batch_idx if self._state.has_working_plan() else -1
                ),
                "addon_extreme_guard_started": (
                    self._addon_extreme_guard_started if self._state.has_working_plan() else False
                ),
                "trend_entry_width": self._trend_entry_width if self._state.is_active() else 0.0,
                "trend_entry_width_pct": self._trend_entry_width_pct if self._state.is_active() else 0.0,
                "trend_entry_time": self._ts_to_str(self._trend_entry_time) if self._state.is_active() else None,
                "trend_last_notify_ts": self._trend_last_notify_ts if self._state.is_active() else 0.0,
                "trend_risk_guard_active": (
                    self._trend_risk_guard_active if self._state.is_active() else False
                ),
            },
        }

    def _save_runtime_state(self):
        """Persist local strategy state to disk."""
        try:
            STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
            tmp = STATE_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps(self._state_payload(), ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(STATE_FILE)
        except Exception as e:
            logger.warning(f"Save runtime state failed: {e}")

    def _filled_batch_summary(self) -> str:
        """Return a compact readable summary of filled strategy batches."""
        filled = sorted(self._state.filled_batches(), key=lambda b: b.batch_idx)
        if not filled:
            return "none"
        return ", ".join(
            f"#{batch.batch_idx + 1} px={batch.price:.2f} sz={batch.sz:g} ord={batch.ord_id or '--'}"
            for batch in filled
        )

    def _log_runtime_state_summary(self, label: str) -> None:
        """Log the restored local-vs-exchange state in one scannable line."""
        log_check(
            f"{label}: direction={self._state.direction} avg={self._state.avg_entry:.2f} "
            f"sz={self._state.total_sz:g} tp={self._state.plan_tp_price:.2f} "
            f"sl={self._state.plan_sl_price:.2f} liq={self._state.plan_liq_price:.2f} "
            f"filled=[{self._filled_batch_summary()}]"
        )

    def _clear_runtime_state(self):
        """Delete the persisted runtime-state file."""
        self._dynamic_tp_active = False
        self._entry_extreme_gap_pct = 0.0
        self._entry_extreme_gap_mult = 1.0
        self._addon_extreme_guard_price = 0.0
        self._addon_extreme_guard_kline_ts = None
        self._addon_extreme_guard_batch_idx = -1
        self._addon_extreme_guard_started = False
        self._trend_entry_width = 0.0
        self._trend_entry_width_pct = 0.0
        self._trend_entry_time = None
        self._trend_last_notify_ts = 0.0
        self._trend_risk_guard_active = False
        try:
            if STATE_FILE.exists():
                STATE_FILE.unlink()
        except Exception as e:
            logger.warning(f"Clear runtime state failed: {e}")

    def _save_close_cooldown(self):
        """Persist the last close kline so restart cannot re-enter too soon."""
        if self._last_close_kline_ts is None:
            return
        try:
            COOLDOWN_FILE.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "inst_id": INST_ID,
                "last_close_kline_ts": self._ts_to_str(self._last_close_kline_ts),
            }
            COOLDOWN_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as e:
            logger.warning(f"Save close cooldown failed: {e}")

    def _load_close_cooldown(self):
        """Load the last close-kline cooldown marker if it exists."""
        if not COOLDOWN_FILE.exists():
            return
        try:
            payload = json.loads(COOLDOWN_FILE.read_text(encoding="utf-8"))
            if payload.get("inst_id") != INST_ID:
                return
            self._last_close_kline_ts = self._str_to_ts(payload.get("last_close_kline_ts"))
        except Exception as e:
            logger.warning(f"Load close cooldown failed: {e}")

    def _clear_close_cooldown(self):
        """Clear the close-kline cooldown marker after the next kline arrives."""
        try:
            if COOLDOWN_FILE.exists():
                COOLDOWN_FILE.unlink()
        except Exception as e:
            logger.warning(f"Clear close cooldown failed: {e}")

    def _sanitize_runtime_state(self):
        """Drop impossible local position residue before persisting or using it."""
        has_position = self._state.total_sz > 0
        has_batch = bool(self._state.batches)
        if self._state.direction in ("long", "short") and not has_position and not has_batch:
            logger.info("Local direction has no position or batches; clearing residue")
            self._reset_probe_state()
            self._state.reset()

    def _reset_probe_state(self) -> None:
        """Clear first-batch probe metadata."""
        self._probe_kline_ts = None
        self._probe_direction = "none"
        self._probe_entry_price = 0.0

    def _load_runtime_state(self):
        """Load local strategy state from disk when available."""
        if not STATE_FILE.exists():
            return
        try:
            payload = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            if payload.get("inst_id") != INST_ID:
                logger.warning("Local strategy state inst_id mismatch; ignoring saved state")
                return

            state = payload.get("state", {})
            self._state = PositionState(direction=state.get("direction", "none"))
            self._state.batches = [
                OpenBatch(
                    batch_idx=int(b.get("batch_idx", 0)),
                    ord_id=str(b.get("ord_id", "")),
                    price=float(b.get("price", 0) or 0),
                    sz=float(b.get("sz", 0) or 0),
                    filled=bool(b.get("filled", False)),
                )
                for b in state.get("batches", [])
            ]
            self._state.tp_ord_id = state.get("tp_ord_id")
            self._state.sl_ord_id = state.get("sl_ord_id")
            self._state.plan_liq_price = float(state.get("plan_liq_price", 0) or 0)
            self._state.plan_sl_price = float(state.get("plan_sl_price", 0) or 0)
            self._state.plan_tp_price = float(state.get("plan_tp_price", 0) or 0)
            self._state.avg_entry = float(state.get("avg_entry", 0) or 0)
            self._state.total_sz = float(state.get("total_sz", 0) or 0)
            self._state.remaining_batches_placed = bool(state.get("remaining_batches_placed", False))
            self._state.cycle_start_account_value = float(state.get("cycle_start_account_value", 0) or 0)
            self._state.cycle_start_ts = str(state.get("cycle_start_ts", "") or "")

            strategy = payload.get("strategy", {})
            self._peak_eq = float(strategy.get("peak_eq", 0) or 0)
            self._last_plan_kline_ts = self._str_to_ts(strategy.get("last_plan_kline_ts"))
            self._last_plan_entry_price = float(strategy.get("last_plan_entry_price", 0) or 0)
            self._last_batch_kline_ts = self._str_to_ts(strategy.get("last_batch_kline_ts"))
            self._last_recovery_kline_ts = self._str_to_ts(strategy.get("last_recovery_kline_ts"))
            self._last_entry_check_kline_ts = self._str_to_ts(strategy.get("last_entry_check_kline_ts"))
            self._probe_kline_ts = self._str_to_ts(strategy.get("probe_kline_ts"))
            self._probe_direction = strategy.get("probe_direction", "none")
            self._probe_entry_price = float(strategy.get("probe_entry_price", 0) or 0)
            self._inside_band_kline_count = int(strategy.get("inside_band_kline_count", 0) or 0)
            self._last_inside_band_kline_ts = self._str_to_ts(strategy.get("last_inside_band_kline_ts"))
            self._sizing_equity = float(strategy.get("sizing_equity", 0) or 0)
            self._fixed_batch_sizes = [float(x) for x in strategy.get("fixed_batch_sizes", [])]
            self._capital_shortage_active = bool(strategy.get("capital_shortage_active", False))
            if ROLLING_COMPOUND_ENABLED and self._capital_shortage_active:
                self._capital_shortage_active = False
            self._dynamic_tp_active = bool(strategy.get("dynamic_tp_active", False))
            self._entry_extreme_gap_pct = float(strategy.get("entry_extreme_gap_pct", 0) or 0)
            self._entry_extreme_gap_mult = float(strategy.get("entry_extreme_gap_mult", 1) or 1)
            self._addon_extreme_guard_price = float(strategy.get("addon_extreme_guard_price", 0) or 0)
            self._addon_extreme_guard_kline_ts = self._str_to_ts(strategy.get("addon_extreme_guard_kline_ts"))
            self._addon_extreme_guard_batch_idx = int(strategy.get("addon_extreme_guard_batch_idx", -1) or -1)
            self._addon_extreme_guard_started = bool(strategy.get("addon_extreme_guard_started", False))
            self._trend_entry_width = float(
                strategy.get("trend_entry_width", strategy.get("btg_entry_width", 0)) or 0
            )
            self._trend_entry_width_pct = float(
                strategy.get("trend_entry_width_pct", strategy.get("btg_entry_width_pct", 0)) or 0
            )
            self._trend_entry_time = self._str_to_ts(
                strategy.get("trend_entry_time", strategy.get("btg_entry_time"))
            )
            self._trend_last_notify_ts = float(
                strategy.get("trend_last_notify_ts", strategy.get("btg_last_notify_ts", 0)) or 0
            )
            self._trend_risk_guard_active = bool(strategy.get("trend_risk_guard_active", False))
            self._sanitize_runtime_state()
            if self._sync_known_batch_sizes():
                self._save_runtime_state()
            self._restored_from_file = True
            logger.info(
                f"Loaded local strategy state: direction={self._state.direction} "
                f"batches={len(self._state.batches)} known_sizes={self._fixed_batch_sizes}"
            )
            self._log_runtime_state_summary("Loaded local runtime snapshot")
        except Exception as e:
            logger.warning(f"Load local runtime state failed: {e}")

    async def _fetch_market_snapshot(self, client: OKXClient):
        """Return latest candles, Bollinger row, and mark price."""
        raw = await client.get_klines(INST_ID, BAR_15M, KLINE_LIMIT)
        if not raw:
            logger.warning("Kline data is empty; skip this cycle")
            return None
        current_kline_ts = pd.to_datetime(int(raw[0][0]), unit="ms")
        df = build_df(raw, include_unconfirmed=BOLL_INCLUDE_CURRENT)
        df = add_boll(df)
        if df.empty:
            logger.warning("Kline or Bollinger data is not ready; skip this cycle")
            return None
        mark_price = await client.get_mark_price(INST_ID)
        last = df.iloc[-1].copy()
        last["ts"] = current_kline_ts
        return df, last, mark_price

    async def _log_market_snapshot(self, client: OKXClient):
        """Write a market snapshot without running trading decisions."""
        snapshot = await self._fetch_market_snapshot(client)
        if snapshot is None:
            return
        _, last, mark_price = snapshot
        equity = dashboard.state.equity
        width = float(last["boll_upper"] - last["boll_lower"])
        width_pct = width / mark_price if mark_price > 0 else 0.0
        log_market(
            f"price={mark_price:.2f}  Boll[{last['boll_lower']:.2f}"
            f" | {last['boll_mid']:.2f} | {last['boll_upper']:.2f}]"
            f"  position={self._state.direction}  equity={equity:.2f}"
            f"  kline={last['ts']} width={width:.2f} width_pct={width_pct:.4%}"
        )
        self._update_dashboard(mark_price, last, equity)

    async def _tick(self, client: OKXClient):
        """Run one strategy iteration."""
        # 1. Kline and Bollinger data.
        raw = await client.get_klines(INST_ID, BAR_15M, KLINE_LIMIT)
        if not raw:
            logger.warning("Kline data is empty; skip this tick")
            return
        current_kline_ts = pd.to_datetime(int(raw[0][0]), unit="ms")
        df  = build_df(raw, include_unconfirmed=BOLL_INCLUDE_CURRENT)
        df  = add_boll(df)
        if df.empty:
            logger.warning("Kline or Bollinger data is insufficient; skip this tick")
            return

        # 2. Account balances: trading balance funds entries; equity guards total risk.
        trading_balance = await client.get_balance("USDT")
        account_equity = await client.get_equity("USDT")
        if await self._check_cross_copy_protection(client, account_equity):
            return
        await self._check_capital_restored(client, trading_balance)
        if self._peak_eq == 0:
            self._peak_eq = account_equity
        self._peak_eq = max(self._peak_eq, account_equity)

        mark_price = await client.get_mark_price(INST_ID)
        await self._refresh_sizing_equity(client, account_equity)
        self._remember_price(mark_price)
        last = df.iloc[-1].copy()
        last["ts"] = current_kline_ts
        width = float(last["boll_upper"] - last["boll_lower"])
        width_pct = width / mark_price if mark_price > 0 else 0.0
        log_market(
            f"price={mark_price:.2f}  Boll[{last['boll_lower']:.2f}"
            f" | {last['boll_mid']:.2f} | {last['boll_upper']:.2f}]"
            f"  position={self._state.direction}  equity={account_equity:.2f}"
            f"  kline={last['ts']} width={width:.2f} width_pct={width_pct:.4%}"
            f" trading_balance={trading_balance:.2f}",
            terminal=True,
        )
        self._remember_boll_snapshot(last, mark_price)
        self._gap_context_df = df
        self._gap_context_row = last
        await self._sync_fills(client, mark_price, last["ts"], df)
        if self._state.is_active():
            await self._check_position_closed(client, mark_price, last["ts"])
        if self._state.is_active():
            await self._maybe_refresh_stop_after_liq_change(client)
        if self._state.is_active():
            self._record_trend_entry_reference(last, mark_price)
        if self._state.is_active() and self._update_addon_extreme_guard_from_completed_kline(df, last):
            self._save_runtime_state()
        if await self._check_trend_risk_guard(client, last, mark_price):
            self._update_dashboard(mark_price, last, account_equity)
            return
        if await self._check_disaster_stop(client, mark_price):
            self._update_dashboard(mark_price, last, account_equity)
            return
        if await self._check_boll_mid_cost_stop(client, last):
            self._update_dashboard(mark_price, last, account_equity)
            return
        if self._state.is_active():
            await self._recover_missing_entry_orders(client, df, last, self._sizing_equity, mark_price)
        await self._maybe_notify_liq_warning(mark_price)
        if self._state.is_active():
            compressed = await self._maybe_update_boll_tp_compression(client, mark_price, last)
            if not compressed:
                await self._maybe_update_dynamic_tp(client, mark_price)
        if self._capital_shortage_active:
            pending_batch = self._state.pending_batch()
            if pending_batch is not None:
                logger.warning("Capital shortage active; cancel pending entry order and pause new entries")
                await self._cancel_entry_orders(client)
                if not self._state.is_active():
                    self._reset_probe_state()
                    self._state.reset()
                    self._save_runtime_state()
            else:
                logger.info("Capital shortage active; pause new entries until trading balance reaches target")
            self._update_dashboard(mark_price, last, account_equity)
            return

        if self._state.has_working_plan():
            if self._state.is_active():
                await self._maybe_place_next_batch(client, df, last, self._sizing_equity, mark_price)
            else:
                await self._maybe_reprice_probe_batch(client, df, last, self._sizing_equity, mark_price)
        else:
            if not self._boll_width_ok(last, mark_price):
                self._log_boll_width_skip("entry_skip", last, mark_price)
            elif not self._entry_max_boll_width_ok(last, mark_price):
                self._log_entry_max_boll_width_skip("entry_skip", last, mark_price)
            elif not self._entry_disaster_filter_ok(last, mark_price):
                pass
            else:
                await self._maybe_place_probe_batch(client, df, last, mark_price, self._sizing_equity)
        self._update_dashboard(mark_price, last, account_equity)

    def _boll_width_ok(self, last, mark_price: float) -> bool:
        """Return whether current Bollinger width allows new entries."""
        width = float(last["boll_width"])
        return width >= self._effective_min_boll_width(mark_price)

    def _log_boll_width_skip(self, reason: str, last, mark_price: float) -> None:
        """Log a contextual reason when Bollinger width blocks an action."""
        width = float(last["boll_width"])
        width_pct = width / mark_price if mark_price > 0 else 0.0
        required = self._effective_min_boll_width(mark_price)
        required_pct = required / mark_price if mark_price > 0 else 0.0
        log_check(
            f"{reason}: Bollinger width too narrow "
            f"width={width:.2f} < {required:.2f} "
            f"width_pct={width_pct:.2%} threshold={required_pct:.2%} "
            f"tp_space={self._tp_space_width_rule(mark_price):.2f}"
        )

    def _entry_max_boll_width_ok(self, last, mark_price: float) -> bool:
        """Return whether Bollinger width is not too wide for a first batch."""
        if not ENTRY_MAX_BOLL_WIDTH_FILTER_ENABLED:
            return True
        width = float(last["boll_width"])
        width_pct = width / mark_price if mark_price > 0 else 0.0
        pct_hit = ENTRY_MAX_BOLL_WIDTH_PCT > 0 and width_pct >= ENTRY_MAX_BOLL_WIDTH_PCT
        usd_hit = ENTRY_MAX_BOLL_WIDTH_USD > 0 and width >= ENTRY_MAX_BOLL_WIDTH_USD
        return not (pct_hit or usd_hit)

    def _log_entry_max_boll_width_skip(self, reason: str, last, mark_price: float) -> None:
        """Log why the maximum-width filter blocked a first-batch action."""
        width = float(last["boll_width"])
        width_pct = width / mark_price if mark_price > 0 else 0.0
        log_check(
            f"{reason}: Bollinger width too wide for first batch "
            f"width={width:.2f} max={ENTRY_MAX_BOLL_WIDTH_USD:.2f} "
            f"width_pct={width_pct:.2%} max_pct={ENTRY_MAX_BOLL_WIDTH_PCT:.2%}"
        )

    def _entry_disaster_filter_ok(self, last, mark_price: float) -> bool:
        """Return whether stacked entry-trend risk still allows a first batch."""
        if not ENTRY_DISASTER_FILTER_ENABLED:
            return True
        lower = float(last["boll_lower"])
        upper = float(last["boll_upper"])
        if mark_price < lower:
            direction = "long"
        elif mark_price > upper:
            direction = "short"
        else:
            return True
        signal = self._entry_disaster_signal(last, mark_price, direction)
        if signal is None:
            return True
        log_check(
            "Entry disaster filter blocked first batch "
            f"direction={direction} score={signal['score']} "
            f"reasons={','.join(signal['reasons'])} "
            f"width_expand={signal['width_expand']:.3f} "
            f"tp_distance_mult={signal['tp_distance_mult']:.3f}"
        )
        return False

    def _entry_disaster_signal(self, last, mark_price: float, direction: str) -> dict | None:
        """Return a disaster-style entry score when a fresh signal is too trend-like."""
        recent = self._recent_unique_boll_history(max(ENTRY_DISASTER_KLINE_COUNT, 2))
        if len(recent) < ENTRY_DISASTER_KLINE_COUNT:
            return None

        lows = [item["low"] for item in recent]
        highs = [item["high"] for item in recent]
        lowers = [item["lower"] for item in recent]
        uppers = [item["upper"] for item in recent]
        mids = [item["mid"] for item in recent]
        widths = [item["width"] for item in recent]
        mid = float(last["boll_mid"])
        reasons = []

        if direction == "long":
            if all(lows[i] < lows[i - 1] for i in range(1, len(lows))):
                reasons.append("lower_lows")
            if all(lowers[i] < lowers[i - 1] for i in range(1, len(lowers))):
                reasons.append("lower_band_down")
            if mids[-1] < mids[0]:
                reasons.append("mid_down")
            if mark_price < mid:
                reasons.append("below_mid")
        elif direction == "short":
            if all(highs[i] > highs[i - 1] for i in range(1, len(highs))):
                reasons.append("higher_highs")
            if all(uppers[i] > uppers[i - 1] for i in range(1, len(uppers))):
                reasons.append("upper_band_up")
            if mids[-1] > mids[0]:
                reasons.append("mid_up")
            if mark_price > mid:
                reasons.append("above_mid")
        else:
            return None

        width_expand = widths[-1] / widths[0] if widths and widths[0] > 0 else 0.0
        if width_expand >= ENTRY_DISASTER_WIDTH_EXPAND:
            reasons.append("width_expand")

        expected_tp_distance = mark_price * (ENTRY_DISASTER_EXPECTED_RETURN / LEVER)
        mid_distance = abs(mark_price - mid)
        tp_distance_mult = mid_distance / expected_tp_distance if expected_tp_distance > 0 else 0.0
        if tp_distance_mult >= ENTRY_DISASTER_TP_DISTANCE_MULT:
            reasons.append("far_from_mid")

        score = len(reasons)
        if score < ENTRY_DISASTER_SCORE_THRESHOLD:
            return None
        return {
            "score": score,
            "reasons": reasons,
            "width_expand": width_expand,
            "tp_distance_mult": tp_distance_mult,
        }

    def _addon_max_boll_width_ok(self, last, mark_price: float) -> bool:
        """Return whether Bollinger width still allows add-on orders."""
        if not ADDON_MAX_BOLL_WIDTH_FILTER_ENABLED:
            return True
        width = float(last["boll_width"])
        width_pct = width / mark_price if mark_price > 0 else 0.0
        pct_hit = ADDON_MAX_BOLL_WIDTH_PCT > 0 and width_pct >= ADDON_MAX_BOLL_WIDTH_PCT
        usd_hit = ADDON_MAX_BOLL_WIDTH_USD > 0 and width >= ADDON_MAX_BOLL_WIDTH_USD
        return not (pct_hit or usd_hit)

    def _log_addon_max_boll_width_skip(self, reason: str, last, mark_price: float) -> None:
        """Log why the maximum-width filter blocked an add-on action."""
        width = float(last["boll_width"])
        width_pct = width / mark_price if mark_price > 0 else 0.0
        log_check(
            f"{reason}: Bollinger width too wide for add-on "
            f"width={width:.2f} max={ADDON_MAX_BOLL_WIDTH_USD:.2f} "
            f"width_pct={width_pct:.2%} max_pct={ADDON_MAX_BOLL_WIDTH_PCT:.2%}"
        )

    def _effective_boll_width_pct(self) -> float:
        """Return the active percentage width threshold."""
        base_pct = BOLL_WIDTH_BASE_USD / BOLL_WIDTH_BASE_PRICE if BOLL_WIDTH_BASE_PRICE > 0 else 0.0
        return max(MIN_BOLL_WIDTH_PCT, base_pct)

    def _head_entry_price(self, fallback_price: float | None = None) -> float:
        """Return the first filled batch price for dynamic risk calculations."""
        filled = sorted(self._state.filled_batches(), key=lambda b: b.batch_idx)
        if filled:
            return filled[0].price
        pending = self._state.pending_batch()
        if pending is not None and pending.batch_idx == 0:
            return pending.price
        if fallback_price is not None:
            return fallback_price
        return self._state.avg_entry

    def _min_ratio_ladder(self) -> list[float]:
        """Return the assumed minimum-size ladder up to the total-entry cap."""
        ratios = [FIRST_BATCH_RATIO, SECOND_BATCH_DYNAMIC_MIN_RATIO]
        while (
            sum(ratios) + DYNAMIC_MIN_ENTRY_RATIO <= MAX_TOTAL_ENTRY_RATIO + 1e-12
            and len(ratios) < MAX_ENTRY_BATCHES
        ):
            ratios.append(DYNAMIC_MIN_ENTRY_RATIO)
        if sum(ratios) < MAX_TOTAL_ENTRY_RATIO and len(ratios) < MAX_ENTRY_BATCHES:
            ratios.append(MAX_TOTAL_ENTRY_RATIO - sum(ratios))
        return ratios

    def _liq_price_for_ladder(self, head_price: float, gap: float) -> float | None:
        """Estimate OKX long liquidation price after minimum-ratio ladder fills."""
        ratios = self._min_ratio_ladder()
        prices = [head_price - idx * gap for idx in range(len(ratios))]
        if not prices or prices[-1] <= 0:
            return None

        risk_equity = self._strategy_risk_equity()
        if risk_equity <= 0:
            return None

        sizes = []
        for ratio, price in zip(ratios, prices):
            raw_sz = risk_equity * ratio * LEVER / (price * CT_VAL)
            sz = math.floor(raw_sz / CONTRACT_STEP) * CONTRACT_STEP
            if sz <= 0:
                return None
            sizes.append(sz)

        qty = sum(sizes) * CT_VAL
        avg = sum(sz * CT_VAL * price for sz, price in zip(sizes, prices)) / qty
        entry_fee = sum(sz * CT_VAL * price * OKX_LIQ_FEE_RATE for sz, price in zip(sizes, prices))
        margin_balance = max(risk_equity - entry_fee, 0.0)
        denominator = qty * (OKX_MAINTENANCE_MARGIN_RATE + OKX_LIQ_FEE_RATE - 1)
        if denominator == 0:
            return None
        return (margin_balance - qty * avg) / denominator

    def _required_entry_gap_for_head_buffer(self, head_price: float) -> float:
        """Return minimum gap that keeps full-ladder liq distance above target."""
        if not DYNAMIC_ENTRY_GAP_ENABLED or head_price <= 0:
            return MIN_ENTRY_GAP_USD

        def buffer_pct(gap: float) -> float:
            liq = self._liq_price_for_ladder(head_price, gap)
            if liq is None:
                return 999.0
            return (head_price - liq) / head_price

        if buffer_pct(MIN_ENTRY_GAP_USD) >= MIN_HEAD_LIQ_BUFFER_PCT:
            return MIN_ENTRY_GAP_USD

        lo = MIN_ENTRY_GAP_USD
        hi = DYNAMIC_ENTRY_GAP_MAX_USD
        for _ in range(25):
            mid = (lo + hi) / 2
            if buffer_pct(mid) >= MIN_HEAD_LIQ_BUFFER_PCT:
                hi = mid
            else:
                lo = mid
        return round(hi, 2)

    def _effective_entry_gap(self, mark_price: float) -> float:
        """Return current entry spacing after head-price liquidation buffer."""
        head_price = self._head_entry_price(mark_price)
        base_gap = max(MIN_ENTRY_GAP_USD, self._required_entry_gap_for_head_buffer(head_price))
        mult = self._entry_extreme_gap_mult if ENTRY_EXTREME_GAP_ADJUST_ENABLED else 1.0
        mult *= self._addon_dynamic_gap_mult(mark_price)
        return round(base_gap * max(1.0, mult), 2)

    def _addon_dynamic_gap_mult(self, mark_price: float) -> float:
        """Return the add-on spacing multiplier from Bollinger and candle trend."""
        if not ADDON_DYNAMIC_GAP_ENABLED or not self._state.is_active():
            return 1.0

        mult = 1.0
        row = self._gap_context_row
        if row is not None and self._trend_entry_width > 0:
            width = float(row["boll_upper"] - row["boll_lower"])
            expand = width / self._trend_entry_width
            if expand >= ADDON_DYNAMIC_GAP_BOLL_STRONG:
                mult *= ADDON_DYNAMIC_GAP_BOLL_MAX_MULT
            elif expand >= ADDON_DYNAMIC_GAP_BOLL_START:
                mid_mult = 1.0 + (ADDON_DYNAMIC_GAP_BOLL_MAX_MULT - 1.0) * 0.5
                mult *= max(1.0, mid_mult)

        adverse_pct = self._head_adverse_move_pct(mark_price)
        if adverse_pct >= ADDON_DYNAMIC_GAP_HEAD_STRONG_PCT:
            mult *= ADDON_DYNAMIC_GAP_HEAD_MAX_MULT
        elif adverse_pct >= ADDON_DYNAMIC_GAP_HEAD_START_PCT:
            mid_mult = 1.0 + (ADDON_DYNAMIC_GAP_HEAD_MAX_MULT - 1.0) * 0.5
            mult *= max(1.0, mid_mult)

        mult *= self._addon_dynamic_gap_trend_mult()

        if ADDON_DYNAMIC_GAP_MAX_USD > 0 and MIN_ENTRY_GAP_USD > 0:
            mult = min(mult, ADDON_DYNAMIC_GAP_MAX_USD / MIN_ENTRY_GAP_USD)
        return max(1.0, mult)

    def _addon_dynamic_gap_trend_mult(self) -> float:
        """Return extra multiplier when completed candles keep moving against us."""
        if ADDON_DYNAMIC_GAP_TREND_MULT <= 1.0:
            return 1.0
        if self._state.direction not in ("long", "short"):
            return 1.0
        count = int(ADDON_DYNAMIC_GAP_TREND_KLINES)
        if count < 2:
            return 1.0
        df = self._gap_context_df
        if df is None or len(df) < count + 1:
            return 1.0
        completed = df.iloc[-(count + 1):-1]
        if len(completed) < count:
            return 1.0
        if self._state.direction == "long":
            lows = [float(value) for value in completed["low"].tail(count)]
            if all(lows[idx] > lows[idx + 1] for idx in range(len(lows) - 1)):
                return ADDON_DYNAMIC_GAP_TREND_MULT
            return 1.0
        highs = [float(value) for value in completed["high"].tail(count)]
        if all(highs[idx] < highs[idx + 1] for idx in range(len(highs) - 1)):
            return ADDON_DYNAMIC_GAP_TREND_MULT
        return 1.0

    def _effective_min_boll_width(self, mark_price: float) -> float:
        """Return the fixed-percent Bollinger-width threshold."""
        if mark_price <= 0:
            return MIN_BOLL_WIDTH_USD
        return max(MIN_BOLL_WIDTH_FLOOR_USD, mark_price * MIN_BOLL_WIDTH_PCT)

    def _tp_space_width_rule(self, mark_price: float) -> float:
        """Return the minimum Bollinger width implied by the take-profit target."""
        if not BOLL_WIDTH_TP_SPACE_ENABLED:
            return 0.0
        if mark_price <= 0 or LEVER <= 0 or TP_TARGET_MARGIN_RETURN <= 0 or BOLL_WIDTH_TP_SPACE_MULT <= 0:
            return 0.0
        tp_price_distance = mark_price * TP_TARGET_MARGIN_RETURN / LEVER
        return tp_price_distance * BOLL_WIDTH_TP_SPACE_MULT

    def _previous_kline_row(self, df, kline_ts=None):
        """Return the candle before ``kline_ts`` or before the current candle."""
        if df is None or len(df) < 2:
            return None
        if kline_ts is not None and "ts" in df:
            target = pd.to_datetime(kline_ts)
            matches = df.index[df["ts"] == target]
            if len(matches) and int(matches[0]) > 0:
                return df.iloc[int(matches[0]) - 1]
        return df.iloc[-2]

    def _start_addon_extreme_guard_from_fill(self, df, direction: str, batch_idx: int, fill_kline_ts=None) -> None:
        """Start tracking completed-candle extremes after the first fill."""
        if not ADDON_EXTREME_GUARD_ENABLED or direction not in ("long", "short"):
            return
        self._addon_extreme_guard_started = True
        self._addon_extreme_guard_batch_idx = batch_idx
        prev = self._previous_kline_row(df, fill_kline_ts)
        if prev is None:
            logger.warning("Add-on extreme guard start failed: not enough kline data")
            return
        old_guard = self._addon_extreme_guard_price
        if direction == "long":
            current_extreme = float(prev["low"])
            guard_price = (
                current_extreme
                if old_guard <= 0
                else min(old_guard, current_extreme)
            )
            label = "prev_low"
        else:
            current_extreme = float(prev["high"])
            guard_price = (
                current_extreme
                if old_guard <= 0
                else max(old_guard, current_extreme)
            )
            label = "prev_high"
        self._addon_extreme_guard_price = guard_price
        self._addon_extreme_guard_kline_ts = prev.get("ts")
        self._addon_extreme_guard_batch_idx = batch_idx
        action = "started" if old_guard <= 0 else "continued"
        log_check(
            f"Add-on extreme guard {action}: batch={batch_idx + 1} "
            f"direction={direction} {label}={guard_price:.2f} "
            f"kline={self._addon_extreme_guard_kline_ts}"
        )

    def _update_addon_extreme_guard_from_completed_kline(self, df, last) -> bool:
        """Update the tracked extreme with the latest completed candle."""
        if not ADDON_EXTREME_GUARD_ENABLED:
            return False
        if not self._state.is_active():
            return False
        if not self._addon_extreme_guard_started and self._state.filled_batches():
            self._addon_extreme_guard_started = True
        if not self._addon_extreme_guard_started:
            return False
        prev = self._previous_kline_row(df, last["ts"] if last is not None and "ts" in last else None)
        if prev is None:
            return False
        prev_ts = prev.get("ts")
        if self._addon_extreme_guard_kline_ts is not None and prev_ts == self._addon_extreme_guard_kline_ts:
            return False

        old = self._addon_extreme_guard_price
        if self._state.direction == "long":
            current = float(prev["low"])
            new_guard = current if old <= 0 else min(old, current)
            label = "low"
        elif self._state.direction == "short":
            current = float(prev["high"])
            new_guard = current if old <= 0 else max(old, current)
            label = "high"
        else:
            return False

        self._addon_extreme_guard_price = new_guard
        self._addon_extreme_guard_kline_ts = prev_ts
        last_batch = self._state.last_filled_batch()
        if last_batch is not None:
            self._addon_extreme_guard_batch_idx = last_batch.batch_idx
        log_check(
            f"Add-on extreme guard kline update: direction={self._state.direction} "
            f"kline={prev_ts} {label}={current:.2f} guard={new_guard:.2f}"
        )
        return True

    def _addon_extreme_guard_allows(self, order_price: float, batch_idx: int) -> bool:
        """Return whether an add-on order breaks the stored candle extreme."""
        if not ADDON_EXTREME_GUARD_ENABLED:
            return True
        if self._addon_extreme_guard_price <= 0:
            log_check("Add-on extreme guard missing; skip add-on until guard is initialized")
            return False
        if self._state.direction == "long":
            if order_price < self._addon_extreme_guard_price:
                return True
            log_check(
                f"Add-on skipped: long price has not broken previous guard low "
                f"batch={batch_idx + 1} next={order_price:.2f} "
                f"guard_low={self._addon_extreme_guard_price:.2f}"
            )
            return False
        if self._state.direction == "short":
            if order_price > self._addon_extreme_guard_price:
                return True
            log_check(
                f"Add-on skipped: short price has not broken previous guard high "
                f"batch={batch_idx + 1} next={order_price:.2f} "
                f"guard_high={self._addon_extreme_guard_price:.2f}"
            )
            return False
        return False

    async def _cancel_pending_batch_due_to_guard(self, client: OKXClient, pending_batch, reason: str) -> None:
        """Cancel a pending add-on when lifecycle guards no longer allow it."""
        log_check(
            f"Cancel pending batch {pending_batch.batch_idx + 1}: {reason} "
            f"ordId={pending_batch.ord_id}"
        )
        try:
            await client.cancel_order(INST_ID, pending_batch.ord_id)
        except Exception as exc:
            log_check(
                f"Cancel pending batch skipped, order may be filled/canceled/missing "
                f"ordId={pending_batch.ord_id}: {exc}"
            )
        self._state.remove_batch(pending_batch.ord_id)
        self._save_runtime_state()

    async def _cancel_pending_if_addon_guards_fail(self, client: OKXClient, df, last, pending_batch) -> bool:
        """Cancel pending add-on orders when add-on guards fail on a new candle."""
        if pending_batch.batch_idx <= 0:
            return False
        if not self._fixed_loss_head_buffer_allows(
            pending_batch.batch_idx,
            pending_batch.price,
            pending_batch.sz,
        ):
            await self._cancel_pending_batch_due_to_guard(client, pending_batch, "fixed-loss head buffer failed")
            return True
        return False

    async def _cancel_pending_if_addon_width_too_wide(
        self,
        client: OKXClient,
        last,
        mark_price: float,
        pending_batch,
    ) -> bool:
        """Cancel pending add-on orders when Bollinger width becomes too wide."""
        if pending_batch.batch_idx <= 0:
            return False
        if self._addon_max_boll_width_ok(last, mark_price):
            return False
        self._log_addon_max_boll_width_skip(
            f"cancel_pending_batch_{pending_batch.batch_idx + 1}",
            last,
            mark_price,
        )
        await self._cancel_pending_batch_due_to_guard(
            client,
            pending_batch,
            "add-on Bollinger width too wide",
        )
        return True

    async def _cancel_pending_if_trend_risk_freeze(self, client: OKXClient, pending_batch) -> bool:
        """Cancel pending add-on orders after the trend-risk guard freezes adds."""
        if not TREND_RISK_FREEZE_ADDON_ENABLED:
            return False
        if not self._trend_risk_guard_active:
            return False
        if pending_batch.batch_idx <= 0:
            return False
        log_check(
            f"Trend risk guard freeze; cancel pending add-on batch {pending_batch.batch_idx + 1}"
        )
        await self._cancel_pending_batch_due_to_guard(
            client,
            pending_batch,
            "trend risk freeze",
        )
        return True

    def _entry_extreme_multiplier(self, gap_pct: float) -> float:
        """Return entry-gap multiplier from first-entry 24h extreme distance."""
        if not ENTRY_EXTREME_GAP_ADJUST_ENABLED:
            return 1.0
        if gap_pct <= ENTRY_EXTREME_GAP_BASE_PCT:
            return 1.0
        full = ENTRY_EXTREME_GAP_FULL_PCT if ENTRY_EXTREME_GAP_FULL_PCT > 0 else 1e-9
        progress = (gap_pct - ENTRY_EXTREME_GAP_BASE_PCT) / full
        mult = 1.0 + progress * (ENTRY_EXTREME_GAP_MAX_MULT - 1.0)
        return round(max(1.0, min(ENTRY_EXTREME_GAP_MAX_MULT, mult)), 4)

    async def _get_24h_ticker_cached(self, client: OKXClient) -> dict | None:
        """Return cached 24h ticker high/low data when the feature is enabled."""
        if not ENTRY_EXTREME_GAP_ADJUST_ENABLED:
            return None
        now = time.time()
        if self._ticker_24h_cache and now - self._ticker_24h_cache_ts < ENTRY_24H_TICKER_CACHE_SEC:
            return self._ticker_24h_cache
        try:
            ticker = await client.get_ticker_24h(INST_ID)
        except Exception as e:
            logger.warning(f"Fetch 24h ticker failed: {e}")
            return None
        self._ticker_24h_cache = ticker
        self._ticker_24h_cache_ts = now
        return ticker

    async def _prepare_entry_extreme_adjustment(self, client: OKXClient, direction: str, entry_price: float) -> None:
        """Lock 24h extreme-distance adjustment for the current first-entry plan."""
        self._entry_extreme_gap_pct = 0.0
        self._entry_extreme_gap_mult = 1.0
        if not ENTRY_EXTREME_GAP_ADJUST_ENABLED or direction not in ("long", "short") or entry_price <= 0:
            return
        ticker = await self._get_24h_ticker_cached(client)
        if not ticker:
            return
        high24 = float(ticker.get("high24h") or 0)
        low24 = float(ticker.get("low24h") or 0)
        if direction == "long" and low24 > 0:
            gap_pct = max(0.0, (entry_price - low24) / entry_price)
        elif direction == "short" and high24 > 0:
            gap_pct = max(0.0, (high24 - entry_price) / entry_price)
        else:
            return
        self._entry_extreme_gap_pct = gap_pct
        self._entry_extreme_gap_mult = self._entry_extreme_multiplier(gap_pct)
        log_check(
            f"24h extreme gap input direction={direction} "
            f"gap={gap_pct:.2%} mult={self._entry_extreme_gap_mult:.2f}"
        )

    def _dynamic_tp_distance(self, avg_entry: float) -> float:
        """Return take-profit distance targeting a margin-return percentage."""
        if avg_entry <= 0:
            return TP_PROFIT_USD
        return round(avg_entry * TP_TARGET_MARGIN_RETURN / LEVER, 2)

    def _tp_price_from_avg(self, direction: str, avg_entry: float) -> float:
        """Return dynamic take-profit price from average entry."""
        distance = self._dynamic_tp_distance(avg_entry)
        return round(avg_entry + distance, 2) if direction == "long" else round(avg_entry - distance, 2)

    def _position_margin_return(self, mark_price: float) -> float:
        """Return current leveraged return from average entry."""
        if not self._state.is_active() or self._state.avg_entry <= 0:
            return 0.0
        if self._state.direction == "long":
            move = (mark_price - self._state.avg_entry) / self._state.avg_entry
        else:
            move = (self._state.avg_entry - mark_price) / self._state.avg_entry
        return move * LEVER

    async def _maybe_update_dynamic_tp(self, client: OKXClient, mark_price: float) -> None:
        """Switch take-profit to a live-price lock when profit momentum stalls."""
        if not DYNAMIC_TP_ENABLED or not self._state.is_active():
            return
        ret = self._position_margin_return(mark_price)
        target_tp = self._tp_price_from_avg(self._state.direction, self._state.avg_entry)

        if self._dynamic_tp_active:
            if ret < DYNAMIC_TP_RESTORE_RETURN:
                self._state.plan_tp_price = target_tp
                self._dynamic_tp_active = False
                log_check(
                    f"Dynamic TP restored: return={ret:.2%} tp={target_tp:.2f}"
                )
                await self._update_tp(client)
                self._save_runtime_state()
            return

        if ret < DYNAMIC_TP_ARM_RETURN or ret >= TP_TARGET_MARGIN_RETURN:
            return
        if self._state.direction == "long" and self._still_making_new_high():
            return
        if self._state.direction == "short" and self._still_making_new_low():
            return

        lock_price = round(mark_price, 2)
        if abs(lock_price - self._state.plan_tp_price) < DYNAMIC_TP_REPRICE_GAP_USD:
            return

        old_tp = self._state.plan_tp_price
        self._state.plan_tp_price = lock_price
        self._dynamic_tp_active = True
        log_action(
            f"Dynamic TP lock: return={ret:.2%} old_tp={old_tp:.2f} "
            f"new_tp={lock_price:.2f}"
        )
        await self._update_tp(client)
        self._save_runtime_state()

    async def _maybe_update_boll_tp_compression(self, client: OKXClient, mark_price: float, last) -> bool:
        """Replace default take-profit when the target band compresses inside it."""
        if not BOLL_TP_COMPRESSION_ENABLED or not self._state.is_active():
            return False
        if self._state.avg_entry <= 0 or self._state.plan_tp_price <= 0:
            return False

        ret = self._position_margin_return(mark_price)
        if ret < BOLL_TP_COMPRESSION_MIN_RETURN:
            return False

        direction = self._state.direction
        tp_price = self._state.plan_tp_price
        upper = float(last["boll_upper"])
        lower = float(last["boll_lower"])

        if direction == "long":
            if upper > tp_price or self._still_making_new_high():
                return False
            lock_price = round(mark_price - BOLL_TP_COMPRESSION_EXIT_OFFSET_USD, 2)
            if lock_price <= self._state.avg_entry:
                return False
        elif direction == "short":
            if lower < tp_price or self._still_making_new_low():
                return False
            lock_price = round(mark_price + BOLL_TP_COMPRESSION_EXIT_OFFSET_USD, 2)
            if lock_price >= self._state.avg_entry:
                return False
        else:
            return False

        if abs(lock_price - tp_price) < DYNAMIC_TP_REPRICE_GAP_USD:
            return False

        self._state.plan_tp_price = lock_price
        self._dynamic_tp_active = True
        log_action(
            f"Boll TP compression: direction={direction} return={ret:.2%} "
            f"old_tp={tp_price:.2f} new_tp={lock_price:.2f} "
            f"boll_lower={lower:.2f} boll_upper={upper:.2f}"
        )
        await self._update_tp(client)
        self._save_runtime_state()
        return True

    async def _maybe_notify_liq_warning(self, mark_price: float) -> None:
        """Send liquidation warning with throttling to avoid message spam."""
        if not self._state.is_active() or self._state.plan_liq_price <= 0:
            self._last_liq_warning_gap_usd = None
            return

        liq = self._state.plan_liq_price
        gap_usd = mark_price - liq if self._state.direction == "long" else liq - mark_price
        gap_pct = gap_usd / mark_price * 100

        if gap_usd <= 0 or gap_usd > LIQ_WARNING_DISTANCE_USD:
            self._last_liq_warning_gap_usd = None
            return

        now = time.time()
        first_warning = self._last_liq_warning_gap_usd is None
        repeat_due = now - self._last_liq_warning_ts >= LIQ_WARNING_REPEAT_SEC
        if not (first_warning or repeat_due):
            return

        self._last_liq_warning_ts = now
        self._last_liq_warning_gap_usd = gap_usd
        await notify_liq_warning(self._state.direction, mark_price, liq, gap_pct, gap_usd)

    def _remember_price(self, mark_price: float):
        """Store recent mark prices for no-new-extreme checks."""
        self._recent_prices.append(mark_price)
        keep = max(NO_NEW_EXTREME_TICKS + 1, 3)
        if len(self._recent_prices) > keep:
            self._recent_prices = self._recent_prices[-keep:]

    async def _maybe_place_probe_batch(self, client: OKXClient, df, last, mark_price: float, equity: float):
        """Place the first probe batch when the current signal qualifies."""
        direction = self._intrabar_probe_direction(df, last, mark_price)

        if direction == "none":
            return

        if not self._entry_max_boll_width_ok(last, mark_price):
            self._log_entry_max_boll_width_skip("probe_skip", last, mark_price)
            return

        if not self._entry_disaster_filter_ok(last, mark_price):
            return

        if direction == "long" and self._still_making_new_low():
            logger.info("Price is still making new lows; skip first long batch")
            return
        if direction == "short" and self._still_making_new_high():
            logger.info("Price is still making new highs; skip first short batch")
            return

        if not self._can_open_new_plan(last["ts"], mark_price):
            return

        if not self._prepare_dynamic_batch_size(0, mark_price):
            return

        boll_std_val = float(df["close"].tail(BOLL_PERIOD).std(ddof=0))
        plan = build_batch_plan(
            direction   = direction,
            first_price = mark_price,
            boll_width  = float(last["boll_width"]),
            boll_mid    = float(last["boll_mid"]),
            boll_lower  = float(last["boll_lower"]),
            boll_upper  = float(last["boll_upper"]),
            boll_std    = boll_std_val,
            equity      = equity,
            fixed_batch_sizes = self._fixed_batch_sizes,
        )
        if not plan.safe:
            logger.warning("Probe signal rejected by risk checks; skip this signal")
            return

        first_order = self._plan_order_at(plan, 0)
        if first_order is None:
            return

        log_check(f"Probe entry prepared direction={direction} first_ref_price={mark_price:.2f}")
        await self._prepare_entry_extreme_adjustment(client, direction, mark_price)
        self._log_plan(first_order, mark_price)
        if await self._place_batch_orders(client, first_order, remaining_batches_placed=False):
            self._probe_kline_ts = last["ts"]
            self._probe_direction = direction
            self._probe_entry_price = mark_price
            self._last_plan_kline_ts = last["ts"]
            self._last_plan_entry_price = mark_price
            self._last_batch_kline_ts = last["ts"]

    def _intrabar_probe_direction(self, df, last, mark_price: float) -> str:
        """Return signal side when mark price is outside the current band."""
        lower = float(last["boll_lower"])
        upper = float(last["boll_upper"])
        if mark_price < lower:
            return "long"
        if mark_price > upper:
            return "short"

        return "none"

    async def _cancel_pending_if_inside_too_long(self, client: OKXClient, last, mark_price: float) -> bool:
        """Cancel a pending entry after enough consecutive inside-band candles."""
        pending_batch = self._state.pending_batch()
        if pending_batch is None:
            self._inside_band_kline_count = 0
            self._last_inside_band_kline_ts = None
            return False

        direction = self._intrabar_probe_direction(None, last, mark_price)
        if direction == self._state.direction:
            self._inside_band_kline_count = 0
            self._last_inside_band_kline_ts = None
            return False

        kline_ts = last["ts"]
        if self._last_inside_band_kline_ts is not None and kline_ts == self._last_inside_band_kline_ts:
            return False
        self._last_inside_band_kline_ts = kline_ts
        self._inside_band_kline_count += 1
        self._save_runtime_state()
        if self._inside_band_kline_count < INSIDE_BAND_CANCEL_KLINES:
            log_check(
                f"Pending order inside band {self._inside_band_kline_count}/"
                f"{INSIDE_BAND_CANCEL_KLINES} klines; keep waiting"
            )
            return False

        log_check(
            f"Pending order stayed inside band for {self._inside_band_kline_count} klines; "
            f"cancel batch {pending_batch.batch_idx + 1}"
        )
        await self._cancel_entry_orders(client)
        self._inside_band_kline_count = 0
        self._last_inside_band_kline_ts = None
        if not self._state.is_active():
            self._last_plan_kline_ts = None
            self._last_batch_kline_ts = None
            self._last_plan_entry_price = 0.0
            self._reset_probe_state()
            self._state.reset()
            self._clear_runtime_state()
        else:
            self._save_runtime_state()
        return True

    async def _cancel_pending_if_width_too_narrow(self, client: OKXClient, last, mark_price: float) -> bool:
        """Cancel a pending entry immediately when Bollinger width is too narrow."""
        pending_batch = self._state.pending_batch()
        if pending_batch is None:
            return False
        if self._boll_width_ok(last, mark_price):
            return False

        self._log_boll_width_skip(
            f"cancel_pending_batch_{pending_batch.batch_idx + 1}",
            last,
            mark_price,
        )
        await self._cancel_entry_orders(client)
        self._inside_band_kline_count = 0
        self._last_inside_band_kline_ts = None
        if not self._state.is_active():
            self._last_plan_kline_ts = None
            self._last_batch_kline_ts = None
            self._last_plan_entry_price = 0.0
            self._reset_probe_state()
            self._state.reset()
            self._clear_runtime_state()
        else:
            self._save_runtime_state()
        return True

    async def _cancel_probe_if_width_too_wide(self, client: OKXClient, last, mark_price: float) -> bool:
        """Cancel an unfilled first batch when first-entry risk becomes too high."""
        pending_batch = self._state.pending_batch()
        if pending_batch is None or pending_batch.batch_idx != 0 or self._state.is_active():
            return False
        width_ok = self._entry_max_boll_width_ok(last, mark_price)
        if not width_ok:
            self._log_entry_max_boll_width_skip("cancel_pending_first_batch", last, mark_price)
        elif self._entry_disaster_filter_ok(last, mark_price):
            return False

        await self._cancel_entry_orders(client)
        self._inside_band_kline_count = 0
        self._last_inside_band_kline_ts = None
        self._last_plan_kline_ts = None
        self._last_batch_kline_ts = None
        self._last_plan_entry_price = 0.0
        self._reset_probe_state()
        self._state.reset()
        self._clear_runtime_state()
        return True

    def _still_making_new_low(self) -> bool:
        """Return whether recent mark prices are still making new lows."""
        if len(self._recent_prices) < NO_NEW_EXTREME_TICKS + 1:
            return True
        recent = self._recent_prices[-(NO_NEW_EXTREME_TICKS + 1):]
        return recent[-1] <= min(recent[:-1])

    def _still_making_new_high(self) -> bool:
        """Return whether recent mark prices are still making new highs."""
        if len(self._recent_prices) < NO_NEW_EXTREME_TICKS + 1:
            return True
        recent = self._recent_prices[-(NO_NEW_EXTREME_TICKS + 1):]
        return recent[-1] >= max(recent[:-1])

    def _can_open_new_plan(self, kline_ts, entry_price: float) -> bool:
        """Return whether a new first-batch plan can be opened."""
        if self._last_close_kline_ts is not None:
            if kline_ts == self._last_close_kline_ts:
                log_check(f"Close-cooldown active; skip opening on same candle ts={kline_ts}")
                return False
            self._last_close_kline_ts = None
            self._clear_close_cooldown()

        if self._last_plan_kline_ts is not None and kline_ts == self._last_plan_kline_ts:
            log_check(f"This kline already opened one plan; skip signal ts={kline_ts}")
            return False

        if self._last_plan_entry_price > 0:
            gap = abs(entry_price - self._last_plan_entry_price)
            required_gap = self._effective_entry_gap(entry_price)
            if gap < required_gap:
                log_check(
                    f"Entry plan gap below {required_gap:.2f} USDT: "
                    f"last={self._last_plan_entry_price:.2f} current={entry_price:.2f} gap={gap:.2f}"
                )
                return False

        return True

    async def _maybe_place_next_batch(self, client: OKXClient, df, last, equity: float, mark_price: float):
        """Place or maintain the next batch after the first batch has filled."""
        if not self._state.is_active():
            return

        pending_batch = self._state.pending_batch()
        if pending_batch is not None:
            if await self._cancel_pending_if_trend_risk_freeze(client, pending_batch):
                return
            if self._last_batch_kline_ts is not None and last["ts"] == self._last_batch_kline_ts:
                return
            if await self._cancel_pending_if_width_too_narrow(client, last, mark_price):
                return
            if await self._cancel_pending_if_addon_width_too_wide(client, last, mark_price, pending_batch):
                return
            if await self._cancel_pending_if_inside_too_long(client, last, mark_price):
                return
            if await self._cancel_pending_if_addon_guards_fail(client, df, last, pending_batch):
                return
            await self._maybe_reprice_pending_batch(client, df, last, equity, mark_price, pending_batch)
            return

        if not self._boll_width_ok(last, mark_price):
            self._log_boll_width_skip("addon_skip", last, mark_price)
            return

        if TREND_RISK_FREEZE_ADDON_ENABLED and self._trend_risk_guard_active:
            log_check("Trend risk guard freeze; skip new add-on batch")
            return

        trigger_direction = self._intrabar_probe_direction(df, last, mark_price)
        if trigger_direction != self._state.direction:
            return

        if self._state.direction == "long" and self._still_making_new_low():
            log_check("Price is still making new lows; delay next long batch")
            return
        if self._state.direction == "short" and self._still_making_new_high():
            log_check("Price is still making new highs; delay next short batch")
            return

        next_idx = self._state.next_batch_idx()
        if next_idx >= MAX_ENTRY_BATCHES:
            self._state.remaining_batches_placed = True
            return

        if not self._addon_max_boll_width_ok(last, mark_price):
            self._log_addon_max_boll_width_skip(f"addon_skip_batch_{next_idx + 1}", last, mark_price)
            return

        kline_ts = last["ts"]
        if self._last_batch_kline_ts is not None and kline_ts == self._last_batch_kline_ts:
            return

        last_batch = self._state.last_filled_batch()
        if last_batch is None:
            return
        if self._state.direction == "long" and self._still_making_new_low():
            log_check(f"Price is still making new lows; delay long batch {next_idx + 1}")
            return
        if self._state.direction == "short" and self._still_making_new_high():
            log_check(f"Price is still making new highs; delay short batch {next_idx + 1}")
            return

        if not self._prepare_dynamic_batch_size(next_idx, mark_price):
            self._save_runtime_state()
            return

        boll_std_val = float(df["close"].tail(BOLL_PERIOD).std(ddof=0))
        plan = build_batch_plan(
            direction   = self._state.direction,
            first_price = mark_price,
            boll_width  = float(last["boll_width"]),
            boll_mid    = float(last["boll_mid"]),
            boll_lower  = float(last["boll_lower"]),
            boll_upper  = float(last["boll_upper"]),
            boll_std    = boll_std_val,
            equity      = equity,
            max_batch_idx = next_idx,
            fixed_batch_sizes = self._fixed_batch_sizes,
        )
        if not plan.safe:
            logger.warning("Next add-on batch rejected by risk checks; skip")
            return

        next_plan = self._plan_order_at_price(plan, next_idx, mark_price)
        if next_plan is None:
            self._state.remaining_batches_placed = True
            return

        next_order = next_plan.orders[0]
        gap = abs(next_order.price - last_batch.price)
        required_gap = self._effective_entry_gap(mark_price)
        if gap < required_gap:
            log_check(
                f"Next batch gap below {required_gap:.2f} USDT: "
                f"last_fill={last_batch.price:.2f} next={next_order.price:.2f} gap={gap:.2f}"
            )
            return

        if self._state.direction == "long" and mark_price > last_batch.price:
            return
        if self._state.direction == "short" and mark_price < last_batch.price:
            return

        self._update_addon_extreme_guard_from_completed_kline(df, last)
        if not self._addon_extreme_guard_allows(next_order.price, next_idx):
            return

        log_check(f"Add-on batch triggered: batch={next_idx + 1} direction={self._state.direction}")
        self._log_plan(next_plan, mark_price)
        if await self._place_batch_orders(client, next_plan, remaining_batches_placed=False):
            self._last_batch_kline_ts = kline_ts

    async def _maybe_reprice_pending_batch(self, client: OKXClient, df, last, equity: float,
                                           mark_price: float, pending_batch):
        """Reprice one pending batch once per candle when plan price moves."""
        kline_ts = last["ts"]
        if self._last_entry_check_kline_ts is not None and kline_ts == self._last_entry_check_kline_ts:
            return
        self._last_entry_check_kline_ts = kline_ts

        last_filled = self._state.last_filled_batch()
        if last_filled is None:
            self._save_runtime_state()
            return

        if not self._boll_width_ok(last, mark_price):
            self._log_boll_width_skip("reprice_skip", last, mark_price)
            self._save_runtime_state()
            return
        if pending_batch.batch_idx > 0 and not self._addon_max_boll_width_ok(last, mark_price):
            self._log_addon_max_boll_width_skip(
                f"reprice_skip_batch_{pending_batch.batch_idx + 1}",
                last,
                mark_price,
            )
            self._save_runtime_state()
            return

        trigger_direction = self._intrabar_probe_direction(df, last, mark_price)
        if trigger_direction != self._state.direction:
            self._save_runtime_state()
            return

        if self._state.direction == "long" and self._still_making_new_low():
            self._save_runtime_state()
            return
        if self._state.direction == "short" and self._still_making_new_high():
            self._save_runtime_state()
            return

        pending_idx = pending_batch.batch_idx
        if pending_idx >= MAX_ENTRY_BATCHES:
            self._state.remaining_batches_placed = True
            return
        if not self._prepare_dynamic_batch_size(pending_idx, mark_price):
            self._save_runtime_state()
            return

        boll_std_val = float(df["close"].tail(BOLL_PERIOD).std(ddof=0))
        plan = build_batch_plan(
            direction   = self._state.direction,
            first_price = mark_price,
            boll_width  = float(last["boll_width"]),
            boll_mid    = float(last["boll_mid"]),
            boll_lower  = float(last["boll_lower"]),
            boll_upper  = float(last["boll_upper"]),
            boll_std    = boll_std_val,
            equity      = equity,
            max_batch_idx = pending_batch.batch_idx,
            fixed_batch_sizes = self._fixed_batch_sizes,
        )
        next_plan = self._plan_order_at_price(plan, pending_batch.batch_idx, mark_price)
        if next_plan is None:
            return

        next_order = next_plan.orders[0]
        gap_from_filled = abs(next_order.price - last_filled.price)
        if gap_from_filled < self._effective_entry_gap(mark_price):
            self._save_runtime_state()
            return

        if abs(next_order.price - pending_batch.price) < REPRICE_GAP_USD:
            self._save_runtime_state()
            return

        self._update_addon_extreme_guard_from_completed_kline(df, last)
        if not self._addon_extreme_guard_allows(next_order.price, pending_batch.batch_idx):
            self._save_runtime_state()
            return

        log_check(
            f"Reprice pending batch {pending_batch.batch_idx + 1}: "
            f"old={pending_batch.price:.2f} new={next_order.price:.2f}"
        )
        await client.cancel_order(INST_ID, pending_batch.ord_id)
        self._state.remove_batch(pending_batch.ord_id)
        if await self._place_batch_orders(client, next_plan, remaining_batches_placed=False):
            self._last_batch_kline_ts = kline_ts
        self._save_runtime_state()

    async def _maybe_reprice_probe_batch(self, client: OKXClient, df, last, equity: float, mark_price: float):
        """Reprice an unfilled first batch once per candle when price moves."""
        pending_batch = self._state.pending_batch()
        if pending_batch is None or pending_batch.batch_idx != 0:
            return

        if await self._cancel_pending_if_width_too_narrow(client, last, mark_price):
            return
        if await self._cancel_probe_if_width_too_wide(client, last, mark_price):
            return

        kline_ts = last["ts"]
        if self._last_entry_check_kline_ts is not None and kline_ts == self._last_entry_check_kline_ts:
            return
        self._last_entry_check_kline_ts = kline_ts

        if await self._cancel_pending_if_inside_too_long(client, last, mark_price):
            return

        if not self._boll_width_ok(last, mark_price):
            self._log_boll_width_skip("probe_reprice_skip", last, mark_price)
            self._save_runtime_state()
            return

        direction = self._intrabar_probe_direction(df, last, mark_price)
        if direction != self._state.direction:
            self._save_runtime_state()
            return
        if direction == "long" and self._still_making_new_low():
            self._save_runtime_state()
            return
        if direction == "short" and self._still_making_new_high():
            self._save_runtime_state()
            return

        if not self._prepare_dynamic_batch_size(0, mark_price):
            self._save_runtime_state()
            return

        boll_std_val = float(df["close"].tail(BOLL_PERIOD).std(ddof=0))
        plan = build_batch_plan(
            direction=direction,
            first_price=mark_price,
            boll_width=float(last["boll_width"]),
            boll_mid=float(last["boll_mid"]),
            boll_lower=float(last["boll_lower"]),
            boll_upper=float(last["boll_upper"]),
            boll_std=boll_std_val,
            equity=equity,
            fixed_batch_sizes=self._fixed_batch_sizes,
        )
        if not plan.safe:
            self._save_runtime_state()
            return

        first_plan = self._plan_order_at(plan, 0)
        if first_plan is None:
            self._save_runtime_state()
            return

        next_order = first_plan.orders[0]
        if abs(next_order.price - pending_batch.price) < REPRICE_GAP_USD:
            self._save_runtime_state()
            return

        log_check(
            f"Reprice first batch after kline update "
            f"old={pending_batch.price:.2f} new={next_order.price:.2f}"
        )
        await self._prepare_entry_extreme_adjustment(client, direction, mark_price)
        await client.cancel_order(INST_ID, pending_batch.ord_id)
        self._state.remove_batch(pending_batch.ord_id)
        if await self._place_batch_orders(client, first_plan, remaining_batches_placed=False):
            self._probe_kline_ts = kline_ts
            self._probe_direction = direction
            self._probe_entry_price = mark_price
            self._last_plan_kline_ts = kline_ts
            self._last_plan_entry_price = mark_price
            self._last_batch_kline_ts = kline_ts
        self._save_runtime_state()

    def _plan_order_at(self, plan, batch_idx: int):
        """Return a copy of ``plan`` containing only one batch order."""
        from dataclasses import replace

        orders = [o for o in plan.orders if o.batch_idx == batch_idx]
        if not orders:
            return None
        return replace(
            plan,
            orders=orders,
            total_margin=round(sum(o.margin for o in orders), 2),
        )

    def _plan_order_at_price(self, plan, batch_idx: int, price: float):
        """Return one batch order while using the current trigger price."""
        from dataclasses import replace

        one_order_plan = self._plan_order_at(plan, batch_idx)
        if one_order_plan is None:
            return None

        order = one_order_plan.orders[0]
        price = round(price, 2)
        notional = order.sz * CT_VAL * price
        margin = notional / LEVER
        updated_order = replace(
            order,
            price=price,
            notional=notional,
            margin=margin,
        )
        if plan.direction == "long":
            liq_price = price - ((margin * 0.9) / (order.sz * CT_VAL))
            tp_price = price + self._dynamic_tp_distance(price)
        else:
            liq_price = price + ((margin * 0.9) / (order.sz * CT_VAL))
            tp_price = price - self._dynamic_tp_distance(price)

        return replace(
            one_order_plan,
            orders=[updated_order],
            avg_entry=price,
            liq_price=round(liq_price, 2),
            sl_price=round(liq_price, 2),
            tp_price=round(tp_price, 2),
            total_margin=round(margin, 2),
        )


    async def _place_batch_orders(self, client: OKXClient, plan, remaining_batches_placed: bool = True) -> bool:
        """Submit all entry orders in a batch plan."""
        had_batches = bool(self._state.batches)
        if not had_batches and any(bo.batch_idx == 0 for bo in plan.orders):
            await self._record_cycle_start_account_value(client, reason="before_head_order")

        self._state.direction      = plan.direction
        self._state.plan_liq_price = plan.liq_price
        self._state.plan_sl_price  = plan.sl_price
        self._state.plan_tp_price  = plan.tp_price

        side     = "buy"  if plan.direction == "long"  else "sell"
        pos_side = plan.direction
        placed_any = False

        for bo in plan.orders:
            try:
                result = await client.place_order(
                    INST_ID, side, pos_side,
                    sz=str(bo.sz),
                    ord_type="limit",
                    px=str(bo.price),
                )
                ord_id = result.get("ordId", "")
                self._state.add_batch(OpenBatch(
                    batch_idx=bo.batch_idx,
                    ord_id=ord_id,
                    price=bo.price,
                    sz=bo.sz,
                ))
                placed_any = True
                await notify_entry_order(
                    direction=plan.direction,
                    price=bo.price,
                    sz=bo.sz,
                    batch=bo.batch_idx + 1,
                    total=MAX_ENTRY_BATCHES,
                    ord_id=ord_id,
                )
                log_action(f"Batch {bo.batch_idx + 1} order placed price={bo.price} sz={bo.sz} ordId={ord_id}")
            except Exception as e:
                logger.error(f"Batch {bo.batch_idx + 1} order failed: {e}")

        if not placed_any:
            if had_batches:
                logger.warning("New batch order failed; keep current position state")
                return False
            logger.warning("No batch order placed; reset strategy state")
            self._state.reset()
            self._last_batch_kline_ts = None
            self._last_entry_check_kline_ts = None
            self._clear_runtime_state()
            return False

        self._state.remaining_batches_placed = remaining_batches_placed
        self._save_runtime_state()
        return True

    async def _recover_missing_entry_orders(self, client: OKXClient, df, last, equity: float, mark_price: float):
        """Restore a missing next-batch entry order after restart or cancel."""
        if not self._state.is_active():
            return
        if not self._boll_width_ok(last, mark_price):
            return
        if any(not b.filled for b in self._state.batches):
            return
        if self._state.avg_entry <= 0:
            return
        if self._last_batch_kline_ts is not None and last["ts"] == self._last_batch_kline_ts:
            return
        if self._last_recovery_kline_ts is not None and last["ts"] == self._last_recovery_kline_ts:
            return

        trigger_direction = self._intrabar_probe_direction(df, last, mark_price)
        if trigger_direction != self._state.direction:
            return
        if self._state.direction == "long" and self._still_making_new_low():
            logger.info("Recovery add-on skipped: price is still making new lows")
            return
        if self._state.direction == "short" and self._still_making_new_high():
            logger.info("Recovery add-on skipped: price is still making new highs")
            return

        try:
            open_orders = await client.get_open_orders(INST_ID)
        except Exception as e:
            logger.warning(f"Query pending orders before recovery failed: {e}")
            return

        entry_side = "buy" if self._state.direction == "long" else "sell"
        has_exchange_entry = any(
            o.get("side") == entry_side
            and o.get("posSide") == self._state.direction
            and o.get("reduceOnly") != "true"
            for o in open_orders
        )
        if has_exchange_entry:
            self._state.remaining_batches_placed = True
            return

        next_idx = self._state.next_batch_idx()
        if next_idx >= MAX_ENTRY_BATCHES:
            self._state.remaining_batches_placed = True
            self._save_runtime_state()
            return

        if not self._prepare_dynamic_batch_size(next_idx, mark_price):
            self._save_runtime_state()
            return

        boll_std_val = float(df["close"].tail(BOLL_PERIOD).std(ddof=0))
        plan = build_batch_plan(
            direction   = self._state.direction,
            first_price = mark_price,
            boll_width  = float(last["boll_width"]),
            boll_mid    = float(last["boll_mid"]),
            boll_lower  = float(last["boll_lower"]),
            boll_upper  = float(last["boll_upper"]),
            boll_std    = boll_std_val,
            equity      = equity,
            max_batch_idx = next_idx,
            fixed_batch_sizes = self._fixed_batch_sizes,
        )
        if not plan.safe:
            logger.warning("Recovery add-on order rejected by risk checks; skip")
            self._last_recovery_kline_ts = last["ts"]
            return

        recovery_plan = self._plan_order_at_price(plan, next_idx, mark_price)
        if recovery_plan is None:
            self._state.remaining_batches_placed = True
            return

        last_batch = self._state.last_filled_batch()
        if last_batch is not None:
            next_order = recovery_plan.orders[0]
            gap = abs(next_order.price - last_batch.price)
            required_gap = self._effective_entry_gap(mark_price)
            if gap < required_gap:
                logger.info(
                    f"Recovery add-on gap below {required_gap:.2f} USDT: "
                    f"last_fill={last_batch.price:.2f} next={next_order.price:.2f} gap={gap:.2f}"
                )
                self._last_recovery_kline_ts = last["ts"]
                return
            if self._state.direction == "long" and mark_price > last_batch.price:
                self._last_recovery_kline_ts = last["ts"]
                return
            if self._state.direction == "short" and mark_price < last_batch.price:
                self._last_recovery_kline_ts = last["ts"]
                return
            self._update_addon_extreme_guard_from_completed_kline(df, last)
            if not self._addon_extreme_guard_allows(next_order.price, next_idx):
                self._last_recovery_kline_ts = last["ts"]
                return

        logger.info(
            f"Recovered missing add-on order: direction={self._state.direction} "
            f"avg_entry={self._state.avg_entry:.2f} batch={next_idx + 1}"
        )
        self._log_plan(recovery_plan, mark_price)
        if await self._place_batch_orders(client, recovery_plan, remaining_batches_placed=False):
            self._last_batch_kline_ts = last["ts"]
            self._last_recovery_kline_ts = last["ts"]


    def _kline_floor_freq(self) -> str:
        """Return a pandas floor frequency matching the configured bar size."""
        if BAR_15M.endswith("m"):
            return f"{BAR_15M[:-1]}min"
        if BAR_15M.endswith("H"):
            return f"{BAR_15M[:-1]}h"
        return BAR_15M

    def _extract_order_fill(self, order_info: dict, fallback_sz: float, fallback_price: float):
        """Extract real filled size, average fill price, and fill candle."""
        raw_sz = order_info.get("accFillSz") or order_info.get("fillSz") or fallback_sz
        raw_price = order_info.get("avgPx") or order_info.get("fillPx") or order_info.get("px") or fallback_price
        try:
            fill_sz = float(raw_sz or fallback_sz)
        except (TypeError, ValueError):
            fill_sz = fallback_sz
        try:
            fill_price = float(raw_price or fallback_price)
        except (TypeError, ValueError):
            fill_price = fallback_price

        fill_kline_ts = None
        raw_time = order_info.get("fillTime") or order_info.get("uTime")
        try:
            if raw_time:
                fill_kline_ts = pd.to_datetime(int(raw_time), unit="ms").floor(self._kline_floor_freq())
        except Exception:
            fill_kline_ts = None
        return fill_sz, fill_price, fill_kline_ts

    async def _sync_fills(self, client: OKXClient, mark_price: float, kline_ts=None, df=None):
        """Synchronize filled and canceled entry orders from OKX."""
        if not self._state.batches:
            return

        prev_sz = self._state.total_sz
        filled_this_tick = False
        filled_kline_ts = None
        newly_filled = []
        canceled_batches = []
        for batch in self._state.batches:
            if batch.filled:
                continue
            try:
                order_info = await client.get_order(INST_ID, batch.ord_id)
                if order_info.get("state") == "filled":
                    fill_sz, fill_price, fill_ts = self._extract_order_fill(order_info, batch.sz, batch.price)
                    batch_idx = batch.batch_idx
                    self._state.mark_filled(batch.ord_id, fill_sz, fill_price)
                    log_action(
                        f"Entry fill synced batch={batch_idx + 1} ordId={batch.ord_id} "
                        f"fill_px={fill_price:.2f} fill_sz={fill_sz:g} "
                        f"fill_kline={fill_ts or kline_ts or '--'}"
                    )
                    if fill_ts is not None:
                        filled_kline_ts = fill_ts
                    newly_filled.append((batch_idx, fill_ts or kline_ts))
                    filled_this_tick = True
                elif order_info.get("state") in ("canceled", "cancelled"):
                    canceled_batches.append(batch)
                    logger.info(f"Batch {batch.batch_idx + 1} canceled ordId={batch.ord_id}")
            except Exception as e:
                logger.warning(f"Query order {batch.ord_id} failed: {e}")

        for batch in canceled_batches:
            self._state.remove_batch(batch.ord_id)
        if canceled_batches:
            self._save_runtime_state()

        if filled_this_tick:
            self._last_batch_kline_ts = filled_kline_ts or kline_ts
            for batch_idx, fill_ts in newly_filled:
                self._start_addon_extreme_guard_from_fill(
                    df,
                    self._state.direction,
                    batch_idx,
                    fill_ts,
                )
            if kline_ts is not None and self._last_batch_kline_ts == kline_ts:
                logger.info(f"This kline already has an entry fill; wait for next kline ts={kline_ts}")
            elif filled_kline_ts is not None:
                logger.info(f"Synced historical fill at kline={filled_kline_ts}; current kline can continue")

        if self._state.total_sz != prev_sz:
            self._dynamic_tp_active = False
            avg = await self._sync_exchange_position(client)
            if avg <= 0:
                avg = self._recalc_tp()
            await self._replace_exit_orders(client)
            self._save_runtime_state()
            filled_count = len(self._state.filled_batches())
            await notify_open(
                direction=self._state.direction,
                avg_entry=avg,
                sz=self._state.total_sz,
                tp=self._state.plan_tp_price,
                liq=self._state.plan_liq_price,
                batch=filled_count,
                total=len(self._state.batches),
            )

        await self._reset_if_plan_has_no_live_orders(client)

    def _recalc_tp(self) -> float:
        """Recalculate local average entry and take-profit from filled batches."""
        filled = self._state.filled_batches()
        if not filled:
            return 0.0
        total_sz       = sum(b.sz for b in filled)
        weighted_price = sum(b.price * b.sz for b in filled)
        avg_entry      = weighted_price / total_sz
        self._state.avg_entry = avg_entry
        self._dynamic_tp_active = False
        self._state.plan_tp_price = self._tp_price_from_avg(self._state.direction, avg_entry)
        log_check(f"Average entry={avg_entry:.2f} new_tp={self._state.plan_tp_price}")
        return avg_entry

    async def _replace_exit_orders(self, client: OKXClient):
        """Cancel old exit orders and place fresh take-profit and stop orders."""
        await self._cancel_exchange_exit_orders(client)
        self._state.tp_ord_id = None
        self._state.sl_ord_id = None
        await self._update_tp(client)
        await self._update_sl(client)
        self._save_runtime_state()

    async def _sync_exchange_position(self, client: OKXClient) -> float:
        """Synchronize local position fields from the exchange position."""
        pos = await client.get_position(INST_ID)
        if pos is None or float(pos.get("pos", 0)) == 0:
            return 0.0

        avg_entry = float(pos.get("avgPx") or 0)
        liq_price = float(pos.get("liqPx") or 0)
        total_sz = float(pos.get("pos", 0))
        pos_side = pos.get("posSide") or self._state.direction

        self._state.direction = pos_side
        self._state.update_position(total_sz, avg_entry, liq_price)
        self._seed_existing_position_batch()

        if avg_entry > 0 and (not self._dynamic_tp_active or self._state.plan_tp_price <= 0):
            self._state.plan_tp_price = self._tp_price_from_avg(pos_side, avg_entry)

        log_check(
            f"Exchange position synced avg={avg_entry:.2f} sz={total_sz:g} "
            f"real_liq={liq_price:.2f} new_tp={self._state.plan_tp_price:.2f}"
        )
        self._log_runtime_state_summary("Post-exchange sync state")
        return avg_entry

    def _desired_stop_loss_order(self) -> tuple[float, str, float, float]:
        """Return desired stop trigger, mode, target loss, and estimated loss."""
        pos_side = self._state.direction
        if self._state.total_sz <= 0 or pos_side not in ("long", "short"):
            return 0.0, "", 0.0, 0.0

        sl_price = 0.0
        stop_mode = "liquidation_guard"
        target_loss = 0.0

        if COPY_FIXED_LOSS_STOP_ENABLED and self._state.avg_entry > 0:
            target_loss = self._fixed_loss_target_usdt()
            if target_loss > 0:
                sl_price = self._fixed_loss_stop_price(
                    pos_side,
                    self._state.avg_entry,
                    self._state.total_sz,
                )
                stop_mode = "fixed_loss"

        liq_guard_price = 0.0
        if self._state.plan_liq_price > 0:
            if pos_side == "long":
                liq_guard_price = self._state.plan_liq_price + LIQ_STOP_OFFSET_USD
            else:
                liq_guard_price = self._state.plan_liq_price - LIQ_STOP_OFFSET_USD

        if sl_price <= 0 and liq_guard_price > 0:
            sl_price = liq_guard_price

        if liq_guard_price > 0:
            if pos_side == "long" and sl_price < liq_guard_price:
                sl_price = liq_guard_price
                stop_mode = "fixed_loss_clamped_to_liq_guard"
            elif pos_side == "short" and sl_price > liq_guard_price:
                sl_price = liq_guard_price
                stop_mode = "fixed_loss_clamped_to_liq_guard"

        sl_price = round(sl_price, 2) if sl_price > 0 else 0.0
        if sl_price <= 0:
            return 0.0, stop_mode, target_loss, 0.0

        if pos_side == "long":
            estimated_loss = max((self._state.avg_entry - sl_price) * self._state.total_sz * CT_VAL, 0.0)
        else:
            estimated_loss = max((sl_price - self._state.avg_entry) * self._state.total_sz * CT_VAL, 0.0)

        return sl_price, stop_mode, target_loss, estimated_loss

    async def _maybe_refresh_stop_after_liq_change(self, client: OKXClient) -> None:
        """Refresh the stop order when exchange liquidation price changes."""
        pos = await client.get_position(INST_ID)
        if pos is None or float(pos.get("pos", 0) or 0) == 0:
            return

        old_liq = self._state.plan_liq_price
        old_sl = self._state.plan_sl_price
        avg_entry = float(pos.get("avgPx") or 0)
        liq_price = float(pos.get("liqPx") or 0)
        total_sz = float(pos.get("pos", 0) or 0)
        pos_side = pos.get("posSide") or self._state.direction

        self._state.direction = pos_side
        self._state.update_position(total_sz, avg_entry, liq_price)
        self._seed_existing_position_batch()

        desired_sl, _, _, _ = self._desired_stop_loss_order()
        if desired_sl <= 0:
            return

        sl_missing = not self._state.sl_ord_id
        sl_changed = old_sl <= 0 or abs(desired_sl - old_sl) >= LIQ_STOP_REPRICE_GAP_USD
        liq_changed = old_liq > 0 and abs(liq_price - old_liq) >= LIQ_STOP_REPRICE_GAP_USD

        if sl_missing or sl_changed:
            log_action(
                f"Refresh stop after liquidation update "
                f"liq={old_liq:.2f}->{liq_price:.2f} sl={old_sl:.2f}->{desired_sl:.2f}"
            )
            await self._update_sl(client)
            self._save_runtime_state()
        elif liq_changed:
            self._save_runtime_state()

    def _seed_existing_position_batch(self):
        """Create a synthetic filled batch for a pre-existing position."""
        if not self._state.is_active():
            return
        if self._state.batches:
            return
        if self._state.avg_entry <= 0:
            return

        self._state.add_batch(OpenBatch(
            batch_idx=0,
            ord_id="existing-position",
            price=self._state.avg_entry,
            sz=self._state.total_sz,
            filled=True,
        ))
        self._probe_entry_price = self._state.avg_entry
        self._last_plan_entry_price = self._state.avg_entry
        logger.info(
            f"Recovered local first batch from existing position "
            f"avg_entry={self._state.avg_entry:.2f} sz={self._state.total_sz}"
        )

    async def _update_tp(self, client: OKXClient):
        """Place the current reduce-only take-profit limit order."""
        pos_side   = self._state.direction
        close_side = "sell" if pos_side == "long" else "buy"

        if self._state.total_sz <= 0 or self._state.plan_tp_price <= 0:
            return

        if self._state.tp_ord_id:
            try:
                await client.cancel_order(INST_ID, self._state.tp_ord_id)
            except Exception as e:
                logger.warning(f"Cancel old take-profit order failed: {e}")
            self._state.tp_ord_id = None

        try:
            r = await client.place_order(
                INST_ID, close_side, pos_side,
                sz=str(self._state.total_sz),
                ord_type="limit",
                px=str(self._state.plan_tp_price),
                reduce_only=True,
            )
            self._state.tp_ord_id = r.get("ordId", "")
            log_action(
                f"止盈挂单 price={self._state.plan_tp_price} sz={self._state.total_sz}"
            )
        except Exception as e:
            logger.error(f"Place take-profit order failed: {e}")

        log_check(f"Current liquidation price={self._state.plan_liq_price} final risk boundary")

    async def _update_sl(self, client: OKXClient):
        """Place the current reduce-only stop order."""
        pos_side = self._state.direction
        if self._state.total_sz <= 0 or pos_side not in ("long", "short"):
            return

        close_side = "sell" if pos_side == "long" else "buy"
        sl_price, stop_mode, target_loss, estimated_loss = self._desired_stop_loss_order()
        if sl_price <= 0:
            logger.warning(f"Invalid stop-loss price; skip sl={sl_price}")
            return

        if self._state.sl_ord_id:
            await client.cancel_algo_order(INST_ID, self._state.sl_ord_id)
            self._state.sl_ord_id = None

        try:
            r = await client.place_algo_order(
                INST_ID,
                close_side,
                pos_side,
                sz=str(self._state.total_sz),
                sl_trigger_px=str(sl_price),
            )
            self._state.sl_ord_id = r.get("algoId", "")
            self._state.plan_sl_price = sl_price
            if stop_mode.startswith("fixed_loss"):
                log_action(
                    f"Fixed-loss stop order trigger={sl_price} mode={stop_mode} "
                    f"target_loss={target_loss:.2f} est_loss={estimated_loss:.2f} "
                    f"avg={self._state.avg_entry:.2f} sz={self._state.total_sz}"
                )
            else:
                log_action(
                    f"Liquidation stop order trigger={sl_price} "
                    f"liq={self._state.plan_liq_price} sz={self._state.total_sz}"
                )
        except Exception as e:
            logger.error(f"Place stop-loss order failed: {e}")

    async def _fetch_actual_close_pnl(
        self,
        client: OKXClient,
        direction: str,
        total_sz: float,
        close_ord_id: str = "",
    ) -> dict | None:
        """Return realized close details from recent close fills when available."""
        if direction not in ("long", "short") or total_sz <= 0:
            return None
        close_side = "sell" if direction == "long" else "buy"
        end_ms = int(time.time() * 1000)
        begin_ms = end_ms - 10 * 60 * 1000
        try:
            fills = await client.get_fills_history(INST_ID, begin=begin_ms, end=end_ms, limit=100)
        except Exception as e:
            logger.warning(f"Fetch close fills failed; fallback to balance diff: {e}")
            return None

        matched = []
        if close_ord_id:
            matched = [fill for fill in fills if str(fill.get("ordId", "")) == str(close_ord_id)]

        if not matched:
            close_fills = [
                fill for fill in fills
                if fill.get("side") == close_side and fill.get("posSide") == direction
            ]
            close_fills.sort(key=lambda fill: int(fill.get("fillTime") or fill.get("ts") or 0), reverse=True)
            filled_sz = 0.0
            for fill in close_fills:
                matched.append(fill)
                try:
                    filled_sz += float(fill.get("fillSz") or fill.get("sz") or 0)
                except (TypeError, ValueError):
                    pass
                if filled_sz + CONTRACT_STEP >= total_sz:
                    break

        if not matched:
            logger.warning("No close fills found; fallback to balance diff for capital rebalance")
            return None

        gross_pnl = 0.0
        total_fill_sz = 0.0
        weighted_px = 0.0
        total_fee = 0.0
        for fill in matched:
            try:
                fill_pnl = float(fill.get("fillPnl") or 0)
            except (TypeError, ValueError):
                fill_pnl = 0.0
            try:
                fee = float(fill.get("fee") or 0)
            except (TypeError, ValueError):
                fee = 0.0
            try:
                fill_sz = float(fill.get("fillSz") or fill.get("sz") or 0)
            except (TypeError, ValueError):
                fill_sz = 0.0
            try:
                fill_px = float(fill.get("fillPx") or fill.get("px") or 0)
            except (TypeError, ValueError):
                fill_px = 0.0
            gross_pnl += fill_pnl
            total_fee += fee
            if fill_sz > 0 and fill_px > 0:
                total_fill_sz += fill_sz
                weighted_px += fill_sz * fill_px

        net_pnl = round(gross_pnl + total_fee, 4)
        avg_fill_px = round(weighted_px / total_fill_sz, 4) if total_fill_sz > 0 else 0.0
        log_action(
            f"Actual close PnL from fills net_pnl={net_pnl:+.4f} USDT "
            f"gross_pnl={gross_pnl:+.4f} fee={total_fee:+.4f} "
            f"avg_px={avg_fill_px or '--'} sz={total_fill_sz or '--'} "
            f"fills={len(matched)} ordId={close_ord_id or '--'}"
        )
        return {
            "pnl": net_pnl,
            "avg_price": avg_fill_px,
            "sz": round(total_fill_sz, 8),
            "fee": round(total_fee, 4),
            "gross_pnl": round(gross_pnl, 4),
            "fills": len(matched),
        }


    async def _check_position_closed(self, client: OKXClient, mark_price: float, kline_ts=None):
        """Detect external position close and reset local state."""
        pos = await client.get_position(INST_ID)
        if pos is None or float(pos.get("pos", 0)) == 0:
            self._last_close_kline_ts = kline_ts
            self._save_close_cooldown()
            filled = self._state.filled_batches()
            avg_entry = self._state.avg_entry
            total_sz = self._state.total_sz
            direction = self._state.direction
            close_price = self._state.plan_tp_price if self._state.plan_tp_price > 0 else mark_price
            if filled and avg_entry <= 0:
                total_sz       = sum(b.sz for b in filled)
                avg_entry      = sum(b.price * b.sz for b in filled) / total_sz
            close_ord_id = self._state.tp_ord_id or ""
            actual_close = await self._fetch_actual_close_pnl(client, direction, total_sz, close_ord_id)
            equity_close = await self._fetch_account_equity_close_pnl(client)
            actual_pnl = equity_close["pnl"] if equity_close else (actual_close["pnl"] if actual_close else None)
            pnl_source = "account_equity_diff" if equity_close else ("fills" if actual_close else "estimate")
            if total_sz > 0 and avg_entry > 0:
                if actual_close:
                    display_close_price = actual_close["avg_price"] or close_price
                    display_sz = actual_close["sz"] or total_sz
                    pnl = actual_pnl
                    log_action(
                        f"Position closed {direction} avg_entry={avg_entry:.2f} "
                        f"close_avg={display_close_price:.2f} sz={display_sz:.2f} "
                        f"actual_pnl={pnl:+.4f} USDT fills={actual_close['fills']} "
                        f"fee={actual_close['fee']:+.4f} source={pnl_source}"
                    )
                elif equity_close:
                    display_close_price = close_price
                    display_sz = total_sz
                    pnl = actual_pnl
                    log_action(
                        f"Position closed {direction} avg_entry={avg_entry:.2f} "
                        f"close_ref={close_price:.2f} sz={total_sz:.2f} "
                        f"actual_pnl={pnl:+.4f} USDT source={pnl_source}"
                    )
                else:
                    from src.config import CT_VAL
                    display_close_price = close_price
                    display_sz = total_sz
                    if direction == "long":
                        pnl = (close_price - avg_entry) * total_sz * CT_VAL
                    else:
                        pnl = (avg_entry - close_price) * total_sz * CT_VAL
                    log_action(
                        f"Position closed {direction} avg_entry={avg_entry:.2f} "
                        f"close_ref={close_price:.2f} sz={total_sz:.2f} "
                        f"estimated_pnl={pnl:+.4f} USDT"
                    )
                dashboard.state.add_trade(
                    action="close_long" if direction == "long" else "close_short",
                    price=display_close_price,
                    sz=display_sz,
                    pnl=pnl,
                )
                await notify_close(direction, avg_entry, display_close_price, pnl, display_sz)

            await self._cancel_entry_orders(client)
            await self._cancel_exchange_exit_orders(client)
            await self._cancel_exit_orders(client)
            log_action("Position state reset")
            self._last_plan_kline_ts = None
            self._last_batch_kline_ts = None
            self._last_plan_entry_price = 0.0
            self._reset_probe_state()
            self._state.reset()
            self._clear_runtime_state()
            actual_profit = await self._rebalance_accounts(client, actual_pnl=actual_pnl)
            if actual_profit:
                log_action(f"Capital actual PnL confirmed {actual_profit:+.4f} USDT")
            await self._calibrate_capital_after_close(client)
            await self._init_fixed_batch_sizes(client)


    async def _sync_state(self, client: OKXClient):
        """Reconcile local state with the exchange on startup."""
        pos = await client.get_position(INST_ID)
        if pos and float(pos.get("pos", 0)) != 0:
            pos_side = pos.get("posSide", "")
            sz       = float(pos.get("pos", 0))
            logger.info(f"Existing position detected: {pos_side} {sz} contracts; continue monitoring")
            self._state.direction = pos_side
            await self._sync_exchange_position(client)
            await self._refresh_filled_batches_from_orders(client)
            self._repair_filled_batches_after_restart()
            if self._sync_known_batch_sizes():
                self._save_runtime_state()
            if self._state.cycle_start_account_value <= 0:
                await self._record_cycle_start_account_value(client, reason="restart_existing_position")
            await self._reconcile_entry_orders_after_restart(client)
            await self._replace_exit_orders(client)
        else:
            await self._cancel_exchange_open_orders(client)
            self._reset_probe_state()
            self._state.reset()
            self._clear_runtime_state()
            if not self._fixed_batch_sizes:
                await self._init_fixed_batch_sizes(client)
            logger.info("No existing position; strategy ready")


    async def _emergency_close(self, client: OKXClient, reason: str = "emergency_close"):
        """Close the active position and clear local state after drawdown stop."""
        if self._state.direction in ("long", "short"):
            try:
                direction = self._state.direction
                avg_entry = self._state.avg_entry
                total_sz = self._state.total_sz
                close_price = await client.get_mark_price(INST_ID)
                log_action(
                    f"Emergency close start reason={reason} direction={direction} "
                    f"mark={close_price:.2f} avg_entry={avg_entry:.2f} sz={total_sz}"
                )
                await self._cancel_entry_orders(client)
                await self._cancel_exchange_exit_orders(client)
                await self._cancel_exit_orders(client)
                await client.close_position(INST_ID, direction)
                await asyncio.sleep(1)
                equity_close = await self._fetch_account_equity_close_pnl(client)
                actual_pnl = equity_close["pnl"] if equity_close else None
                if avg_entry > 0 and total_sz > 0:
                    if direction == "long":
                        pnl = (close_price - avg_entry) * total_sz * CT_VAL
                    else:
                        pnl = (avg_entry - close_price) * total_sz * CT_VAL
                    if actual_pnl is not None:
                        pnl = actual_pnl
                        log_action(
                            f"Emergency close actual_pnl={pnl:+.4f} USDT "
                            f"source=account_equity_diff reason={reason}"
                        )
                    await notify_close(direction, avg_entry, close_price, pnl, total_sz)
                self._reset_probe_state()
                self._state.reset()
                self._clear_runtime_state()
                actual_profit = await self._rebalance_accounts(client, actual_pnl=actual_pnl)
                if actual_profit:
                    log_action(f"Capital actual PnL confirmed {actual_profit:+.4f} USDT")
                await self._calibrate_capital_after_close(client)
                await self._init_fixed_batch_sizes(client)
            except Exception as e:
                logger.error(f"Emergency close failed: {e}")

    async def _cancel_entry_orders(self, client: OKXClient):
        """Cancel all local unfilled entry orders."""
        kept_batches = []
        for batch in self._state.batches:
            if batch.filled:
                kept_batches.append(batch)
                continue
            try:
                await client.cancel_order(INST_ID, batch.ord_id)
            except Exception as e:
                logger.warning(f"Cancel remaining add-on order failed ordId={batch.ord_id}: {e}")
        self._state.batches = kept_batches
        self._save_runtime_state()

    async def _cancel_untriggered_entry_orders(self, client: OKXClient, last, mark_price: float):
        """Cancel pending add-on orders when price returns inside the band."""
        if not self._state.is_active():
            return

        trigger_direction = self._intrabar_probe_direction(None, last, mark_price)
        if trigger_direction == self._state.direction:
            return

        has_local_pending = any(not b.filled for b in self._state.batches)

        entry_side = "buy" if self._state.direction == "long" else "sell"
        try:
            orders = await client.get_open_orders(INST_ID)
        except Exception as e:
            logger.warning(f"Fetch open entry orders failed: {e}")
            return

        entry_orders = [
            o for o in orders
            if o.get("side") == entry_side
            and o.get("posSide") == self._state.direction
            and o.get("reduceOnly") != "true"
        ]
        if not has_local_pending and not entry_orders:
            return

        log_check("Price no longer breaks Bollinger band; cancel unfilled add-on order")
        await self._cancel_entry_orders(client)
        self._last_batch_kline_ts = None
        self._last_recovery_kline_ts = None

        for order in entry_orders:
            ord_id = order.get("ordId")
            if ord_id:
                await client.cancel_order(INST_ID, ord_id)

    async def _cancel_untriggered_probe_order(self, client: OKXClient, last, mark_price: float):
        """Cancel the first probe order when price returns inside the band."""
        if self._state.is_active():
            return
        if self._state.direction not in ("long", "short"):
            return

        trigger_direction = self._intrabar_probe_direction(None, last, mark_price)
        if trigger_direction == self._state.direction:
            return

        pending = self._state.pending_batch()
        if pending is None:
            return

        log_check("Price returned inside Bollinger band; cancel unfilled first batch")
        await client.cancel_order(INST_ID, pending.ord_id)
        self._state.remove_batch(pending.ord_id)
        if pending.batch_idx == 0 and self._last_plan_kline_ts == last["ts"]:
            self._last_plan_kline_ts = None
            self._last_batch_kline_ts = None
            self._last_plan_entry_price = 0.0
            logger.info("Unfilled first batch canceled; current kline entry lock released")
        self._reset_probe_state()
        self._state.reset()
        self._clear_runtime_state()

    async def _cancel_exchange_open_orders(self, client: OKXClient):
        """Cancel all exchange open orders for the instrument."""
        try:
            orders = await client.get_open_orders(INST_ID)
        except Exception as e:
            logger.warning(f"Query pending orders failed: {e}")
            return

        for order in orders:
            ord_id = order.get("ordId")
            if not ord_id:
                continue
            try:
                await client.cancel_order(INST_ID, ord_id)
            except Exception as e:
                logger.warning(f"Cancel stale exchange entry order failed ordId={ord_id}: {e}")

    async def _reconcile_entry_orders_after_restart(self, client: OKXClient):
        """Match local pending entry orders with exchange orders after restart."""
        entry_side = "buy" if self._state.direction == "long" else "sell"
        try:
            orders = await client.get_open_orders(INST_ID)
        except Exception as e:
            logger.warning(f"Restart reconciliation for exchange add-on orders failed: {e}")
            return

        entry_orders = [
            o for o in orders
            if o.get("side") == entry_side
            and o.get("posSide") == self._state.direction
            and o.get("reduceOnly") != "true"
        ]
        live_ids = {o.get("ordId") for o in entry_orders if o.get("ordId")}
        local_pending = [b for b in self._state.batches if not b.filled and b.ord_id]
        local_pending_ids = {b.ord_id for b in local_pending}

        removed = []
        known_filled_sz = sum(b.sz for b in self._state.batches if b.filled)
        unmatched_filled_sz = max(self._state.total_sz - known_filled_sz, 0.0)
        for batch in sorted(local_pending, key=lambda b: b.batch_idx):
            if batch.ord_id not in live_ids:
                if unmatched_filled_sz + 1e-8 >= batch.sz:
                    try:
                        order_info = await client.get_order(INST_ID, batch.ord_id)
                        fill_sz, fill_price, fill_kline_ts = self._extract_order_fill(
                            order_info, batch.sz, batch.price
                        )
                        batch.sz = fill_sz
                        batch.price = fill_price
                        if fill_kline_ts is not None:
                            self._last_batch_kline_ts = fill_kline_ts
                    except Exception:
                        pass
                    batch.filled = True
                    unmatched_filled_sz -= batch.sz
                    logger.info(f"Local batch {batch.batch_idx + 1} filled while offline ordId={batch.ord_id}")
                else:
                    removed.append(batch)
                    logger.info(f"Local batch {batch.batch_idx + 1} no longer on exchange; remove ordId={batch.ord_id}")
        for batch in removed:
            self._state.remove_batch(batch.ord_id)
        local_pending_ids = {b.ord_id for b in self._state.batches if not b.filled and b.ord_id}

        for order in entry_orders:
            ord_id = order.get("ordId")
            if not ord_id or ord_id in local_pending_ids:
                continue
            logger.info(f"Cancel unmatched exchange entry order ordId={ord_id}")
            await client.cancel_order(INST_ID, ord_id)

        if local_pending_ids:
            logger.info(f"Restart kept exchange pending add-on orders ordId={sorted(local_pending_ids)}")
        self._save_runtime_state()

    async def _refresh_filled_batches_from_orders(self, client: OKXClient):
        """Refresh filled batch prices and sizes from OKX order details."""
        changed = False
        for batch in self._state.filled_batches():
            if not batch.ord_id or batch.ord_id == "existing-position":
                continue
            try:
                order_info = await client.get_order(INST_ID, batch.ord_id)
                fill_sz, fill_price, fill_kline_ts = self._extract_order_fill(order_info, batch.sz, batch.price)
            except Exception as e:
                logger.warning(f"Restart refresh batch {batch.batch_idx + 1} fill failed ordId={batch.ord_id}: {e}")
                continue
            if fill_kline_ts is not None:
                self._last_batch_kline_ts = fill_kline_ts
                changed = True
            if fill_sz > 0 and abs(fill_sz - batch.sz) > 1e-8:
                logger.info(
                    f"Restart refreshed batch {batch.batch_idx + 1} fill size "
                    f"{batch.sz:g} -> {fill_sz:g}"
                )
                batch.sz = fill_sz
                changed = True
            if fill_price > 0 and abs(fill_price - batch.price) > 1e-8:
                logger.info(
                    f"Restart refreshed batch {batch.batch_idx + 1} fill price "
                    f"{batch.price:.2f} -> {fill_price:.2f}"
                )
                batch.price = fill_price
                changed = True
        if changed:
            self._save_runtime_state()

    def _repair_filled_batches_after_restart(self):
        """Replace stale filled batches when they do not match the exchange position."""
        if not self._state.is_active():
            return

        filled = self._state.filled_batches()
        pending = [batch for batch in self._state.batches if not batch.filled]
        filled_sz = sum(batch.sz for batch in filled)
        has_invalid_order_id = any(
            batch.ord_id
            and batch.ord_id != "existing-position"
            and not str(batch.ord_id).isdigit()
            for batch in filled
        )
        size_mismatch = abs(filled_sz - self._state.total_sz) > CONTRACT_STEP
        if not has_invalid_order_id and not size_mismatch:
            return

        logger.warning(
            "Local filled batches mismatch exchange position; "
            f"rebuild from exchange avg filled_sz={filled_sz:g} real_sz={self._state.total_sz:g}. "
            "Future add-on gap checks will use the exchange average until real fill history is available."
        )
        self._state.batches = [
            OpenBatch(
                batch_idx=0,
                ord_id="existing-position",
                price=self._state.avg_entry,
                sz=self._state.total_sz,
                filled=True,
            )
        ] + pending
        self._probe_entry_price = self._state.avg_entry
        self._last_plan_entry_price = self._state.avg_entry
        self._save_runtime_state()
        self._log_runtime_state_summary("Rebuilt runtime state from exchange position")

    async def _cancel_exchange_entry_orders(self, client: OKXClient):
        """Cancel exchange entry orders for the current side.

        Reserved helper. It is not called by the current live path.
        """
        return

    async def _rebalance_accounts(self, client: OKXClient, actual_pnl: float | None = None) -> float:
        """Keep capital by transferring the latest realized PnL when known."""
        if ROLLING_COMPOUND_ENABLED:
            if self._capital_shortage_active:
                self._capital_shortage_active = False
                self._save_runtime_state()
            if actual_pnl is not None:
                logger.info(
                    f"[Capital] Rolling compound enabled; keep realized PnL "
                    f"{actual_pnl:+.4f} USDT in trading account"
                )
                return actual_pnl
            logger.info("[Capital] Rolling compound enabled; skip balance-diff transfer")
            return 0.0

        if TRADING_ACCOUNT_TARGET <= 0:
            return 0.0
        try:
            if actual_pnl is not None:
                actual_pnl = round(actual_pnl, 4)
                if abs(actual_pnl) <= 0.01:
                    logger.debug("[Capital] Actual PnL is within 0.01 USDT; skip transfer")
                    return 0.0

                if actual_pnl > 0:
                    trading_bal = await client.get_balance("USDT")
                    transfer_amt = round(min(actual_pnl, trading_bal), 4)
                    if transfer_amt < 0.01:
                        logger.warning("[Capital] Trading balance too low to transfer realized profit")
                        return 0.0
                    logger.info(
                        f"[Capital] Profit +{actual_pnl:.4f} USDT; realized; "
                        f"transfer {transfer_amt:.4f} USDT to funding"
                    )
                    await client.transfer(amt=transfer_amt, from_acct="18", to_acct="6")
                    return transfer_amt

                needed = round(abs(actual_pnl), 4)
                trading_bal = await client.get_balance("USDT")
                funding_bal = await client.get_funding_balance("USDT")
                top_up = round(min(needed, funding_bal), 4)
                shortage = top_up + 0.0001 < needed
                if shortage and not self._capital_shortage_active:
                    self._capital_shortage_active = True
                    await notify_capital_shortage(trading_bal + top_up, TRADING_ACCOUNT_TARGET, funding_bal, top_up)
                    self._save_runtime_state()
                if top_up < 0.01:
                    if not self._capital_shortage_active:
                        self._capital_shortage_active = True
                        await notify_capital_shortage(trading_bal, TRADING_ACCOUNT_TARGET, funding_bal, 0.0)
                        self._save_runtime_state()
                    logger.warning(
                        f"[Capital] Funding balance insufficient ({funding_bal:.4f} USDT); "
                        f"cannot top up realized loss"
                    )
                    return actual_pnl
                partial = " (partial top-up; funding insufficient)" if shortage else ""
                logger.info(
                    f"[Capital] Loss {actual_pnl:.4f} USDT; realized; "
                    f"transfer {top_up:.4f} USDT from funding to trading{partial}"
                )
                await client.transfer(amt=top_up, from_acct="6", to_acct="18")
                return actual_pnl

            if CROSS_COPY_PROTECT_ENABLED:
                logger.warning(
                    "[Capital] Actual PnL unavailable in cross-copy mode; "
                    "skip balance-diff rebalance to avoid moving protected trading equity"
                )
                return 0.0

            trading_bal = await client.get_balance("USDT")
            diff = round(trading_bal - TRADING_ACCOUNT_TARGET, 4)

            if diff > 0.01:
                logger.info(
                    f"[Capital] Profit +{diff:.4f} USDT; "
                    f"trading {trading_bal:.4f} -> {TRADING_ACCOUNT_TARGET:.4f}; transfer to funding"
                )
                await client.transfer(amt=diff, from_acct="18", to_acct="6")
                return diff

            elif diff < -0.01:
                needed = abs(diff)
                funding_bal = await client.get_funding_balance("USDT")
                top_up = round(min(needed, funding_bal), 4)
                shortage = top_up < needed
                if shortage and top_up >= 0.01 and not self._capital_shortage_active:
                    self._capital_shortage_active = True
                    await notify_capital_shortage(trading_bal + top_up, TRADING_ACCOUNT_TARGET, funding_bal, top_up)
                    self._save_runtime_state()
                if top_up < 0.01:
                    if not self._capital_shortage_active:
                        self._capital_shortage_active = True
                        await notify_capital_shortage(trading_bal, TRADING_ACCOUNT_TARGET, funding_bal, 0.0)
                        self._save_runtime_state()
                    logger.warning(
                        f"[Capital] Funding balance insufficient ({funding_bal:.4f} USDT); "
                        f"cannot top up trading account"
                    )
                    return diff
                partial = " (partial top-up; funding insufficient)" if top_up < needed else ""
                logger.info(
                    f"[Capital] Loss {diff:.4f} USDT; "
                    f"transfer {top_up:.4f} USDT from funding to trading{partial}"
                )
                await client.transfer(amt=top_up, from_acct="6", to_acct="18")
                return diff

            else:
                logger.debug("[Capital] Balance is within 0.01 USDT of target; skip transfer")

        except Exception as e:
            logger.warning(f"[Capital] Transfer failed; strategy continues: {e}")
        return 0.0

    async def _check_capital_restored(self, client: OKXClient, trading_balance: float | None = None) -> None:
        """Notify once when trading capital recovers after a shortage."""
        if ROLLING_COMPOUND_ENABLED:
            if self._capital_shortage_active:
                self._capital_shortage_active = False
                self._save_runtime_state()
            return
        if not self._capital_shortage_active or TRADING_ACCOUNT_TARGET <= 0:
            return
        if self._state.is_active():
            return

        target = self._capital_target_equity()
        if target <= 0:
            return
        if CROSS_COPY_PROTECT_ENABLED:
            trading_balance = await self._capital_account_value(client)
        elif trading_balance is None:
            trading_balance = await client.get_balance("USDT")
        tolerance = max(CAPITAL_REBALANCE_TOLERANCE_USDT, 0.0)
        if trading_balance + tolerance < target:
            return

        excess = round(trading_balance - target, 4)
        if excess > tolerance:
            available = await client.get_balance("USDT")
            transfer_amt = round(min(excess, available), 4)
            if transfer_amt < 0.01:
                logger.warning(
                    f"[Capital] Account above target but no transferable balance "
                    f"value={trading_balance:.4f} target={target:.4f} avail={available:.4f}"
                )
                return
            logger.info(
                f"[Capital] Account above target after top-up; "
                f"{trading_balance:.4f} -> {target:.4f}; transfer {transfer_amt:.4f} USDT to funding"
            )
            await client.transfer(amt=transfer_amt, from_acct="18", to_acct="6")
            trading_balance = target

        self._capital_shortage_active = False
        self._save_runtime_state()
        logger.info(
            f"[Capital] Trading account restored to target "
            f"{trading_balance:.4f}/{target:.4f} USDT"
        )
        await notify_capital_restored(trading_balance, target)
        await self._ensure_fixed_batch_sizes(client)

    def stop(self):
        """Request the main strategy loop to stop."""
        self._running = False
        logger.info("Strategy stopped")
