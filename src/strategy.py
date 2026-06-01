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
    PRICE_LOG_INTERVAL, POLL_INTERVAL, MAX_DRAWDOWN, BOLL_PERIOD,
    TP_PROFIT_USD, MIN_ENTRY_GAP_USD,
    MIN_BOLL_WIDTH_USD, MIN_BOLL_WIDTH_PCT,
    BOLL_WIDTH_BASE_PRICE, BOLL_WIDTH_BASE_USD,
    MIN_BOLL_WIDTH_FLOOR_USD, BOLL_WIDTH_GAP_MULT,
    TP_TARGET_MARGIN_RETURN, DYNAMIC_TP_ENABLED,
    DYNAMIC_TP_ARM_RETURN, DYNAMIC_TP_RESTORE_RETURN,
    DYNAMIC_TP_REPRICE_GAP_USD,
    MIN_HEAD_LIQ_BUFFER_PCT, DYNAMIC_ENTRY_GAP_ENABLED,
    DYNAMIC_ENTRY_GAP_MAX_USD, OKX_MAINTENANCE_MARGIN_RATE,
    OKX_LIQ_FEE_RATE,
    LIQ_STOP_OFFSET_USD, LIQ_WARNING_DISTANCE_USD,
    LIQ_WARNING_REPEAT_SEC,
    NO_NEW_EXTREME_TICKS,
    REPRICE_GAP_USD, INSIDE_BAND_CANCEL_KLINES,
    STRATEGY_EQUITY_CAP_USDT, CT_VAL, CONTRACT_STEP,
    TRADING_ACCOUNT_TARGET,
    MAX_ENTRY_BATCHES, MAX_TOTAL_ENTRY_RATIO,
    FIRST_BATCH_RATIO, SECOND_BATCH_RATIO,
    DYNAMIC_BASE_ENTRY_RATIO, DYNAMIC_MIN_ENTRY_RATIO, DYNAMIC_MAX_ENTRY_RATIO,
)
from src.okx_client import OKXClient
from src.indicators import build_df, add_boll
from src.risk import build_batch_plan, check_drawdown
from src.position_manager import PositionState, OpenBatch
from src.notify import (
    notify_entry_order, notify_open, notify_close, notify_liq_warning,
    notify_drawdown, notify_capital_shortage, notify_capital_restored,
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

    # ┢┢ 单次 tick ┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢

    def _desired_sizing_equity(self, account_equity: float) -> float:
        """Return the fixed equity base used to size strategy batches."""
        if self._capital_shortage_active and TRADING_ACCOUNT_TARGET > 0:
            base_equity = min(account_equity, TRADING_ACCOUNT_TARGET)
        else:
            base_equity = TRADING_ACCOUNT_TARGET if TRADING_ACCOUNT_TARGET > 0 else account_equity
        if STRATEGY_EQUITY_CAP_USDT > 0:
            return min(base_equity, STRATEGY_EQUITY_CAP_USDT)
        return base_equity

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
            return SECOND_BATCH_RATIO

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

        self._set_fixed_batch_size(batch_idx, sz)
        log_check(
            f"Dynamic batch prepared: batch={batch_idx + 1} "
            f"ratio={ratio:.2%} price={candidate_price:.2f} sz={sz}"
        )
        return True
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
        account_equity = await client.get_balance("USDT")
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
        equity = account_equity if account_equity is not None else await client.get_balance("USDT")
        self._sizing_equity = sizing_equity if sizing_equity is not None else self._desired_sizing_equity(equity)
        mark_price = await client.get_mark_price(INST_ID)
        batch_sizes = []
        for i in range(2):
            ratio = FIRST_BATCH_RATIO if i == 0 else SECOND_BATCH_RATIO
            margin_budget = self._sizing_equity * ratio
            raw_sz = margin_budget * LEVER / (mark_price * CT_VAL)
            sz = self._floor_contract_size(raw_sz)
            batch_sizes.append(sz)
        self._fixed_batch_sizes = batch_sizes
        log_check(
            f"First/second batch sizes calculated available={equity:.2f} sizing_equity={self._sizing_equity:.2f} "
            f"sizes={self._fixed_batch_sizes}; future add-ons use dynamic sizing"
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

    def _clear_runtime_state(self):
        """Delete the persisted runtime-state file."""
        self._dynamic_tp_active = False
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
            logger.info("本地策略状只有方向没有持仓或批次，已自动清理残留方向")
            self._reset_probe_state()
            self._state.reset()

    def _load_runtime_state(self):
        """Load local strategy state from disk when available."""
        if not STATE_FILE.exists():
            return
        try:
            payload = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            if payload.get("inst_id") != INST_ID:
                logger.warning("本地策略状交易对不匹配，忽略")
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
            self._dynamic_tp_active = bool(strategy.get("dynamic_tp_active", False))
            self._sanitize_runtime_state()
            if self._sync_known_batch_sizes():
                self._save_runtime_state()
            self._restored_from_file = True
            logger.info(
                f"Loaded local strategy state: direction={self._state.direction} "
                f"batches={len(self._state.batches)} known_sizes={self._fixed_batch_sizes}"
            )
        except Exception as e:
            logger.warning(f"读取本地策略状失败，忽略: {e}")

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
        log_market(
            f"price={mark_price:.2f}  Boll[{last['boll_lower']:.2f}"
            f" | {last['boll_mid']:.2f} | {last['boll_upper']:.2f}]"
            f"  position={self._state.direction}  equity={equity:.2f}"
        )
        self._update_dashboard(mark_price, last, equity)

    async def _tick(self, client: OKXClient):
        """Run one strategy iteration."""
        # 1. K?+ 布林?
        raw = await client.get_klines(INST_ID, BAR_15M, KLINE_LIMIT)
        if not raw:
            logger.warning("K线数据为空，本轮跳过")
            return
        current_kline_ts = pd.to_datetime(int(raw[0][0]), unit="ms")
        df  = build_df(raw, include_unconfirmed=BOLL_INCLUDE_CURRENT)
        df  = add_boll(df)
        if df.empty:
            logger.warning("K线或布林带数据不足，本轮跳过")
            return

        # 2. 账户权益 & 回撤
        equity = await client.get_balance("USDT")
        await self._check_capital_restored(client, equity)
        if self._peak_eq == 0:
            self._peak_eq = equity
        self._peak_eq = max(self._peak_eq, equity)

        if check_drawdown(equity, self._peak_eq, MAX_DRAWDOWN):
            logger.error("触发朢大回撤，清仓停机")
            dd = (self._peak_eq - equity) / self._peak_eq
            await notify_drawdown(equity, self._peak_eq, dd)
            await self._emergency_close(client)
            self._running = False
            return

        mark_price = await client.get_mark_price(INST_ID)
        self._remember_price(mark_price)
        last = df.iloc[-1].copy()
        last["ts"] = current_kline_ts
        log_market(
            f"价格={mark_price:.2f}  布林[{last['boll_lower']:.2f}"
            f" | {last['boll_mid']:.2f} | {last['boll_upper']:.2f}]"
            f"  持仓={self._state.direction}  权益={equity:.2f}",
            terminal=True,
        )

        # 3. 同步成交
        await self._sync_fills(client, mark_price, last["ts"])

        # 4. 棢查持仓是否已平，并恢复旧仓位缺失的补仓单
        if self._state.is_active():
            await self._check_position_closed(client, mark_price, last["ts"])
        if self._state.is_active():
            await self._recover_missing_entry_orders(client, df, last, equity, mark_price)

        # 5. 强平预警（距强平?< 3%?
        await self._maybe_notify_liq_warning(mark_price)
        if self._state.is_active():
            await self._maybe_update_dynamic_tp(client, mark_price)

        # 6. 盘中评分通过后批挂单，每根K线最多新增一?
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
            self._update_dashboard(mark_price, last, equity)
            return

        if self._state.has_working_plan():
            if self._state.is_active():
                await self._maybe_place_next_batch(client, df, last, equity, mark_price)
            else:
                await self._maybe_reprice_probe_batch(client, df, last, equity, mark_price)
        else:
            if not self._boll_width_ok(last, mark_price):
                self._log_boll_width_skip("开仓跳过", last, mark_price)
            else:
                await self._maybe_place_probe_batch(client, df, last, mark_price, equity)

        # 7. 更新看板
        self._update_dashboard(mark_price, last, equity)

    def _boll_width_ok(self, last, mark_price: float) -> bool:
        """Return whether current Bollinger width allows new entries."""
        width = float(last["boll_width"])
        return width >= self._effective_min_boll_width(mark_price)

    def _log_boll_width_skip(self, reason: str, last, mark_price: float) -> None:
        """Log a contextual reason when Bollinger width blocks an action."""
        width = float(last["boll_width"])
        width_pct = width / mark_price if mark_price > 0 else 0.0
        required = self._effective_min_boll_width(mark_price)
        required_pct = self._effective_boll_width_pct()
        log_check(
            f"{reason}: Bollinger width too narrow "
            f"width={width:.2f} < {required:.2f} "
            f"width_pct={width_pct:.2%} threshold={required_pct:.2%}"
        )
        return
        _old_log_check_disabled(
            f"{reason}：布林宽度不足 width={width:.2f} < {MIN_BOLL_WIDTH_USD:.2f} "
            f"width_pct={width_pct:.2%} threshold={MIN_BOLL_WIDTH_PCT:.2%}"
        )

    def _effective_boll_width_pct(self) -> float:
        """Return the active percentage width threshold."""
        base_pct = BOLL_WIDTH_BASE_USD / BOLL_WIDTH_BASE_PRICE if BOLL_WIDTH_BASE_PRICE > 0 else 0.0
        return max(MIN_BOLL_WIDTH_PCT, base_pct)

    def _head_entry_price(self, fallback_price: float) -> float:
        """Return the first filled batch price for dynamic risk calculations."""
        filled = sorted(self._state.filled_batches(), key=lambda b: b.batch_idx)
        if filled:
            return filled[0].price
        pending = self._state.pending_batch()
        if pending is not None and pending.batch_idx == 0:
            return pending.price
        return fallback_price

    def _min_ratio_ladder(self) -> list[float]:
        """Return the assumed minimum-size ladder up to the total-entry cap."""
        ratios = [FIRST_BATCH_RATIO, SECOND_BATCH_RATIO]
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

        sizes = []
        for ratio, price in zip(ratios, prices):
            raw_sz = TRADING_ACCOUNT_TARGET * ratio * LEVER / (price * CT_VAL)
            sz = math.floor(raw_sz / CONTRACT_STEP) * CONTRACT_STEP
            if sz <= 0:
                return None
            sizes.append(sz)

        qty = sum(sizes) * CT_VAL
        avg = sum(sz * CT_VAL * price for sz, price in zip(sizes, prices)) / qty
        entry_fee = sum(sz * CT_VAL * price * OKX_LIQ_FEE_RATE for sz, price in zip(sizes, prices))
        margin_balance = max(TRADING_ACCOUNT_TARGET - entry_fee, 0.0)
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
        return max(MIN_ENTRY_GAP_USD, self._required_entry_gap_for_head_buffer(head_price))

    def _effective_min_boll_width(self, mark_price: float) -> float:
        """Return dynamic Bollinger-width threshold."""
        pct_rule = mark_price * self._effective_boll_width_pct() if mark_price > 0 else 0.0
        gap_rule = self._effective_entry_gap(mark_price) * BOLL_WIDTH_GAP_MULT
        return max(MIN_BOLL_WIDTH_USD, MIN_BOLL_WIDTH_FLOOR_USD, pct_rule, gap_rule)

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

        if direction == "long" and self._still_making_new_low():
            logger.info("价格仍在继续创新低，暂不弢首批多单")
            return
        if direction == "short" and self._still_making_new_high():
            logger.info("价格仍在继续创新高，暂不弢首批空单")
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
            f"挂单撤销：第{pending_batch.batch_idx + 1}批",
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
                log_check(f"平仓同K跳过：本根K线刚平仓，等待下一根K线再开仓 ts={kline_ts}")
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
            if self._last_batch_kline_ts is not None and last["ts"] == self._last_batch_kline_ts:
                return
            if await self._cancel_pending_if_width_too_narrow(client, last, mark_price):
                return
            if await self._cancel_pending_if_inside_too_long(client, last, mark_price):
                return
            await self._maybe_reprice_pending_batch(client, df, last, equity, mark_price, pending_batch)
            return

        if not self._boll_width_ok(last, mark_price):
            self._log_boll_width_skip("补仓跳过", last, mark_price)
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
            self._state.remaining_batches_placed = True
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
            self._log_boll_width_skip("补仓重挂跳过", last, mark_price)
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

        kline_ts = last["ts"]
        if self._last_entry_check_kline_ts is not None and kline_ts == self._last_entry_check_kline_ts:
            return
        self._last_entry_check_kline_ts = kline_ts

        if await self._cancel_pending_if_inside_too_long(client, last, mark_price):
            return

        if not self._boll_width_ok(last, mark_price):
            self._log_boll_width_skip("首批重挂跳过", last, mark_price)
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
            f"Reprice first batch after kline update"
            f"旧价={pending_batch.price:.2f} 新价={next_order.price:.2f}"
        )
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

    # ┢┢ 下批次限价单 ┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢

    async def _place_batch_orders(self, client: OKXClient, plan, remaining_batches_placed: bool = True) -> bool:
        """Submit all entry orders in a batch plan."""
        self._state.direction      = plan.direction
        self._state.plan_liq_price = plan.liq_price
        self._state.plan_sl_price  = plan.sl_price
        self._state.plan_tp_price  = plan.tp_price

        side     = "buy"  if plan.direction == "long"  else "sell"
        pos_side = plan.direction
        had_batches = bool(self._state.batches)
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
            logger.info("价格仍在继续创新低，暂不恢复补挂多单")
            return
        if self._state.direction == "short" and self._still_making_new_high():
            logger.info("价格仍在继续创新高，暂不恢复补挂空单")
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

        logger.info(
            f"Recovered missing add-on order: direction={self._state.direction} "
            f"avg_entry={self._state.avg_entry:.2f} batch={next_idx + 1}"
        )
        self._log_plan(recovery_plan, mark_price)
        if await self._place_batch_orders(client, recovery_plan, remaining_batches_placed=False):
            self._last_batch_kline_ts = last["ts"]
            self._last_recovery_kline_ts = last["ts"]

    # ┢┢ 棢查挂单成?┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢

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

    async def _sync_fills(self, client: OKXClient, mark_price: float, kline_ts=None):
        """Synchronize filled and canceled entry orders from OKX."""
        if not self._state.batches:
            return

        prev_sz = self._state.total_sz
        filled_this_tick = False
        filled_kline_ts = None
        canceled_batches = []
        for batch in self._state.batches:
            if batch.filled:
                continue
            try:
                order_info = await client.get_order(INST_ID, batch.ord_id)
                if order_info.get("state") == "filled":
                    fill_sz, fill_price, fill_ts = self._extract_order_fill(order_info, batch.sz, batch.price)
                    self._state.mark_filled(batch.ord_id, fill_sz, fill_price)
                    if fill_ts is not None:
                        filled_kline_ts = fill_ts
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

        logger.info(
            f"交易扢持仓同步 均价={avg_entry:.2f} 张数={total_sz} "
            f"real_liq={liq_price:.2f} new_tp={self._state.plan_tp_price:.2f}"
        )
        return avg_entry

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
            f"Recovered local first batch from existing position avg_entry={self._state.avg_entry:.2f} "
            f"张数={self._state.total_sz}"
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
            log_action(f"止盈挂单 价格={self._state.plan_tp_price}  张数={self._state.total_sz}")
        except Exception as e:
            logger.error(f"Place take-profit order failed: {e}")

        log_check(f"Current liquidation price={self._state.plan_liq_price} final risk boundary")

    async def _update_sl(self, client: OKXClient):
        """Place the current reduce-only liquidation-line stop order."""
        pos_side = self._state.direction
        if self._state.total_sz <= 0 or self._state.plan_liq_price <= 0:
            return

        close_side = "sell" if pos_side == "long" else "buy"
        if pos_side == "long":
            sl_price = round(self._state.plan_liq_price + LIQ_STOP_OFFSET_USD, 2)
        else:
            sl_price = round(self._state.plan_liq_price - LIQ_STOP_OFFSET_USD, 2)

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
            log_action(
                f"Liquidation stop order trigger={sl_price} "
                f"liq={self._state.plan_liq_price} sz={self._state.total_sz}"
            )
        except Exception as e:
            logger.error(f"挂强平线止损失败: {e}")

    # ┢┢ 棢查持仓是否已?┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢

    async def _check_position_closed(self, client: OKXClient, mark_price: float, kline_ts=None):
        """Detect external position close and reset local state."""
        pos = await client.get_position(INST_ID)
        if pos is None or float(pos.get("pos", 0)) == 0:
            self._last_close_kline_ts = kline_ts
            self._save_close_cooldown()
            # 计算本次盈亏
            filled = self._state.filled_batches()
            avg_entry = self._state.avg_entry
            total_sz = self._state.total_sz
            direction = self._state.direction
            close_price = self._state.plan_tp_price if self._state.plan_tp_price > 0 else mark_price
            if filled and avg_entry <= 0:
                total_sz       = sum(b.sz for b in filled)
                avg_entry      = sum(b.price * b.sz for b in filled) / total_sz
            if total_sz > 0 and avg_entry > 0:
                from src.config import CT_VAL
                if direction == "long":
                    pnl = (close_price - avg_entry) * total_sz * CT_VAL
                else:
                    pnl = (avg_entry - close_price) * total_sz * CT_VAL
                log_action(
                    f"止盈平仓 {direction} 均价={avg_entry:.2f} "
                    f"平仓价={close_price:.2f} 张数={total_sz:.2f} 估算收益={pnl:+.4f} USDT"
                )
                dashboard.state.add_trade(
                    action="平多" if direction == "long" else "平空",
                    price=close_price,
                    sz=total_sz,
                    pnl=pnl,
                )
                await notify_close(direction, avg_entry, close_price, pnl, total_sz)

            await self._cancel_entry_orders(client)
            await self._cancel_exchange_exit_orders(client)
            await self._cancel_exit_orders(client)
            log_action("持仓状态已重置")
            self._last_plan_kline_ts = None
            self._last_batch_kline_ts = None
            self._last_plan_entry_price = 0.0
            self._reset_probe_state()
            self._state.reset()
            self._clear_runtime_state()
            actual_profit = await self._rebalance_accounts(client)
            if actual_profit > 0:
                log_action(f"固本收益确认 实际落袋=+{actual_profit:.4f} USDT")
            await self._init_fixed_batch_sizes(client)

    # ┢┢ 恢复状?┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢

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

    # ┢┢ 紧平?┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢

    async def _emergency_close(self, client: OKXClient):
        """Close the active position and clear local state after drawdown stop."""
        if self._state.direction in ("long", "short"):
            try:
                direction = self._state.direction
                avg_entry = self._state.avg_entry
                total_sz = self._state.total_sz
                close_price = await client.get_mark_price(INST_ID)
                await self._cancel_entry_orders(client)
                await client.close_position(INST_ID, direction)
                await self._cancel_exchange_exit_orders(client)
                await self._cancel_exit_orders(client)
                if avg_entry > 0 and total_sz > 0:
                    if direction == "long":
                        pnl = (close_price - avg_entry) * total_sz * CT_VAL
                    else:
                        pnl = (avg_entry - close_price) * total_sz * CT_VAL
                    await notify_close(direction, avg_entry, close_price, pnl, total_sz)
                self._reset_probe_state()
                self._state.reset()
                self._clear_runtime_state()
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
            logger.warning(f"查询未成交补仓单失败: {e}")
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
                logger.warning(f"撤销历史未成交单失败 ordId={ord_id}: {e}")

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
            logger.info(f"重启发现非本地记录的补仓挂单，撤锢 ordId={ord_id}")
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
            "本地已成交批次与交易扢持仓不一致，"
            f"按交易所均价重建本地批次 filled_sz={filled_sz} real_sz={self._state.total_sz}"
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

    async def _cancel_exchange_entry_orders(self, client: OKXClient):
        """Cancel exchange entry orders for the current side.

        Reserved helper. It is not called by the current live path.
        """
        entry_side = "buy" if self._state.direction == "long" else "sell"
        try:
            orders = await client.get_open_orders(INST_ID)
        except Exception as e:
            logger.warning(f"Query exchange add-on orders failed: {e}")
            return

        for order in orders:
            if order.get("side") != entry_side:
                continue
            if order.get("posSide") != self._state.direction:
                continue
            if order.get("reduceOnly") == "true":
                continue
            ord_id = order.get("ordId")
            if ord_id:
                logger.info(f"启动/恢复时撤锢交易扢未成交补仓单 ordId={ord_id}")
                await client.cancel_order(INST_ID, ord_id)

    async def _cancel_exchange_exit_orders(self, client: OKXClient):
        """Cancel exchange take-profit and stop-loss orders for the position."""
        try:
            orders = await client.get_open_orders(INST_ID)
        except Exception as e:
            logger.warning(f"查询旧止盈单失败: {e}")
            orders = []

        close_side = "sell" if self._state.direction == "long" else "buy"
        for order in orders:
            if order.get("reduceOnly") != "true":
                continue
            if order.get("side") != close_side:
                continue
            ord_id = order.get("ordId")
            if ord_id:
                await client.cancel_order(INST_ID, ord_id)

        try:
            algos = await client.get_open_algo_orders(INST_ID)
        except Exception as e:
            logger.warning(f"查询旧止损条件单失败: {e}")
            return

        for algo in algos:
            algo_id = algo.get("algoId")
            if algo_id:
                await client.cancel_algo_order(INST_ID, algo_id)

    async def _cancel_invalid_entry_orders(self, client: OKXClient):
        """Cancel entry orders that violate spacing or side rules.

        Reserved helper. It is not called by the current live path.
        """
        last_batch = self._state.last_filled_batch()
        if last_batch is None:
            return

        entry_side = "buy" if self._state.direction == "long" else "sell"
        try:
            orders = await client.get_open_orders(INST_ID)
        except Exception as e:
            logger.warning(f"Query add-on orders failed: {e}")
            return

        for order in orders:
            if order.get("side") != entry_side or order.get("posSide") != self._state.direction:
                continue
            if order.get("reduceOnly") == "true":
                continue

            try:
                px = float(order.get("px") or 0)
            except ValueError:
                continue

            gap = abs(px - last_batch.price)
            invalid = gap < self._effective_entry_gap(px)
            if self._state.direction == "long" and px >= last_batch.price:
                invalid = True
            if self._state.direction == "short" and px <= last_batch.price:
                invalid = True

            if invalid:
                ord_id = order.get("ordId")
                if ord_id:
                    logger.info(
                        f"Cancel invalid add-on order ordId={ord_id} price={px:.2f} "
                        f"last_fill={last_batch.price:.2f} gap={gap:.2f}"
                    )
                    await client.cancel_order(INST_ID, ord_id)

    async def _cancel_exit_orders(self, client: OKXClient):
        """Cancel locally tracked exit orders."""
        if self._state.tp_ord_id:
            await client.cancel_order(INST_ID, self._state.tp_ord_id)
            self._state.tp_ord_id = None
        if self._state.sl_ord_id:
            await client.cancel_algo_order(INST_ID, self._state.sl_ord_id)
            self._state.sl_ord_id = None

    async def _reset_if_plan_has_no_live_orders(self, client: OKXClient):
        """Reset local plan state when no live position or entry order exists."""
        if self._state.is_active() or not self._state.batches:
            return

        try:
            orders = await client.get_open_orders(INST_ID)
        except Exception as e:
            logger.warning(f"棢查未成交订单失败: {e}")
            return

        live_ids = {o.get("ordId") for o in orders}
        has_live_entry = any(
            (not b.filled) and b.ord_id in live_ids
            for b in self._state.batches
        )
        if not has_live_entry:
            self._last_plan_kline_ts = None
            self._last_batch_kline_ts = None
            self._last_plan_entry_price = 0.0
            logger.info("No position or pending entry orders; reset strategy state")
            self._reset_probe_state()
            self._state.reset()
            self._clear_runtime_state()

    def _reset_probe_state(self):
        """Clear first-probe metadata."""
        self._probe_kline_ts = None
        self._probe_direction = "none"
        self._probe_entry_price = 0.0

    # ┢┢ 更新看板状?┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢

    def _update_dashboard(self, mark_price: float, last, equity: float):
        """Copy the current strategy snapshot into dashboard state."""
        s = dashboard.state
        s.mark_price  = mark_price
        s.boll_lower  = float(last["boll_lower"])
        s.boll_mid    = float(last["boll_mid"])
        s.boll_upper  = float(last["boll_upper"])
        s.equity      = equity
        s.peak_equity = self._peak_eq
        s.direction   = self._state.direction
        s.total_sz    = self._state.total_sz
        s.tp_price    = self._state.plan_tp_price
        s.liq_price   = self._state.plan_liq_price
        s.batches     = [
            {"batch_idx": b.batch_idx, "price": b.price, "sz": b.sz, "filled": b.filled}
            for b in self._state.batches
        ]
        # 浮动盈亏
        filled = self._state.filled_batches()
        if self._state.avg_entry > 0 and self._state.total_sz > 0 and mark_price > 0:
            from src.config import CT_VAL
            total_sz   = self._state.total_sz
            avg_entry  = self._state.avg_entry
            s.avg_entry = avg_entry
            if self._state.direction == "long":
                s.unrealized_pnl = (mark_price - avg_entry) * total_sz * CT_VAL
            else:
                s.unrealized_pnl = (avg_entry - mark_price) * total_sz * CT_VAL
        else:
            s.avg_entry      = 0.0
            s.unrealized_pnl = 0.0
        s.update_time()

    # ┢┢ 日志输出建仓计划 ┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢

    def _log_plan(self, plan, mark_price: float):
        """Log a human-readable batch-entry plan."""
        log_check("=" * 60)
        log_check(f"Entry plan direction={plan.direction} mark_price={mark_price:.2f}")
        log_check(f"  tp={plan.tp_price} estimated_liq={plan.liq_price}")
        log_check(f"  total_margin={plan.total_margin:.2f} USDT")
        for bo in plan.orders:
            log_check(
                f"  batch={bo.batch_idx + 1} price={bo.price} sz={bo.sz}"
                f" notional={bo.notional:.2f} margin={bo.margin:.2f}"
            )
        log_check("=" * 60)

    # ┢┢ 资金账户再平?┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢┢

    async def _rebalance_accounts(self, client: OKXClient) -> float:
        """Keep the trading account near ``TRADING_ACCOUNT_TARGET``."""
        """
        每次平仓后调用：
          盈利 -> 将超?TRADING_ACCOUNT_TARGET 的部分划转到资金账户
          亏损 -> 从资金账户补回交易账户，使可用余额恢复到 TRADING_ACCOUNT_TARGET
        TRADING_ACCOUNT_TARGET = 0 时跳过?
        """
        if TRADING_ACCOUNT_TARGET <= 0:
            return 0.0
        try:
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
        if not self._capital_shortage_active or TRADING_ACCOUNT_TARGET <= 0:
            return

        if trading_balance is None:
            trading_balance = await client.get_balance("USDT")
        if trading_balance + 0.01 < TRADING_ACCOUNT_TARGET:
            return

        excess = round(trading_balance - TRADING_ACCOUNT_TARGET, 4)
        if excess > 0.01:
            logger.info(
                f"[Capital] Balance above target after top-up; "
                f"{trading_balance:.4f} -> {TRADING_ACCOUNT_TARGET:.4f}; transfer {excess:.4f} USDT to funding"
            )
            await client.transfer(amt=excess, from_acct="18", to_acct="6")
            trading_balance = TRADING_ACCOUNT_TARGET

        self._capital_shortage_active = False
        self._save_runtime_state()
        logger.info(
            f"[Capital] Trading balance restored to target "
            f"{trading_balance:.4f}/{TRADING_ACCOUNT_TARGET:.4f} USDT"
        )
        await notify_capital_restored(trading_balance, TRADING_ACCOUNT_TARGET)
        await self._ensure_fixed_batch_sizes(client)

    def stop(self):
        """Request the main strategy loop to stop."""
        self._running = False
        logger.info("Strategy stopped")
