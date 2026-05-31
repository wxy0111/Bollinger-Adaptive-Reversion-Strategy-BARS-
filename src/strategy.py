"""Live Bollinger-band mean-reversion strategy.

The strategy watches 15-minute Bollinger bands on ``ETH-USDT-SWAP``. When mark
price moves outside the band and stops making new extremes, it places a batch
limit-entry plan. Filled positions receive dynamic take-profit orders based on
the exchange average entry price and a liquidation-line stop order.
"""
import asyncio
import json
import aiohttp
import pandas as pd
from pathlib import Path
from loguru import logger

from src.config import (
    INST_ID, BAR_15M, LEVER, KLINE_LIMIT,
    BOLL_INCLUDE_CURRENT,
    POLL_INTERVAL, MAX_DRAWDOWN, BOLL_PERIOD,
    TP_PROFIT_USD, MIN_ENTRY_GAP_USD,
    MIN_BOLL_WIDTH_USD, MIN_BOLL_WIDTH_PCT,
    LIQ_STOP_OFFSET_USD,
    NO_NEW_EXTREME_TICKS,
    BATCH_COUNT,
    REPRICE_GAP_USD, INSIDE_BAND_CANCEL_KLINES,
    BATCH_SIZE_RATIO, STRATEGY_EQUITY_CAP_USDT, CT_VAL, CONTRACT_STEP,
    TRADING_ACCOUNT_TARGET,
)
from src.okx_client import OKXClient
from src.indicators import build_df, add_boll
from src.risk import build_batch_plan, check_drawdown
from src.position_manager import PositionState, OpenBatch
from src.notify import notify_entry_order, notify_open, notify_close, notify_liq_warning, notify_drawdown
import src.dashboard as dashboard


STATE_FILE = Path("logs/runtime_state.json")


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
        self._block_recovery_on_first_tick = False
        self._probe_kline_ts = None
        self._probe_direction = "none"
        self._probe_entry_price = 0.0
        self._inside_band_kline_count = 0
        self._last_inside_band_kline_ts = None
        self._recent_prices = []
        self._sizing_equity = 0.0
        self._fixed_batch_sizes = []
        self._restored_from_file = False

    async def run(self):
        """Run the strategy loop until stopped."""
        self._running = True
        logger.info("策略启动  {} 布林破轨均值回归 {}x", INST_ID, LEVER)

        async with aiohttp.ClientSession() as session:
            client = OKXClient(session)
            try:
                await client.set_leverage(INST_ID, LEVER)
            except Exception as e:
                logger.warning(f"设置杠杆失败（请在OKX App手动设置为{LEVER}x）: {e}")
            self._load_runtime_state()
            if not self._fixed_batch_sizes:
                await self._init_fixed_batch_sizes(client)
            else:
                logger.info(
                    f"使用本地保存的固定分批张数继续运行 计入资金={self._sizing_equity:.2f} "
                    f"张数={self._fixed_batch_sizes}"
                )
            await self._sync_state(client)

            while self._running:
                try:
                    await self._tick(client)
                except Exception as e:
                    logger.exception(f"tick 异常: {e}")
                await asyncio.sleep(POLL_INTERVAL)

    # ── 单次 tick ─────────────────────────────────────────────────────────

    async def _init_fixed_batch_sizes(self, client: OKXClient):
        """Calculate fixed batch sizes from current available trading balance."""
        equity = await client.get_balance("USDT")
        self._sizing_equity = min(equity, STRATEGY_EQUITY_CAP_USDT) if STRATEGY_EQUITY_CAP_USDT > 0 else equity
        mark_price = await client.get_mark_price(INST_ID)
        batch_sizes = []
        for i in range(BATCH_COUNT):
            margin_budget = self._sizing_equity * BATCH_SIZE_RATIO[i]
            raw_sz = margin_budget * LEVER / (mark_price * CT_VAL)
            step_count = int(raw_sz / CONTRACT_STEP)
            sz = round(step_count * CONTRACT_STEP, 8)
            batch_sizes.append(sz)
        self._fixed_batch_sizes = batch_sizes
        logger.info(
            f"固定分批张数已计算 可用资金={equity:.2f} 计入资金={self._sizing_equity:.2f} "
            f"张数={self._fixed_batch_sizes}"
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
            logger.warning(f"保存本地策略状态失败: {e}")

    def _clear_runtime_state(self):
        """Delete the persisted runtime-state file."""
        try:
            if STATE_FILE.exists():
                STATE_FILE.unlink()
        except Exception as e:
            logger.warning(f"清理本地策略状态失败: {e}")

    def _load_runtime_state(self):
        """Load local strategy state from disk when available."""
        if not STATE_FILE.exists():
            return
        try:
            payload = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            if payload.get("inst_id") != INST_ID:
                logger.warning("本地策略状态交易对不匹配，忽略")
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
            self._restored_from_file = True
            logger.info(
                f"已读取本地策略状态: 方向={self._state.direction} "
                f"批次={len(self._state.batches)} 固定张数={self._fixed_batch_sizes}"
            )
        except Exception as e:
            logger.warning(f"读取本地策略状态失败，忽略: {e}")

    async def _tick(self, client: OKXClient):
        """Run one strategy iteration."""
        # 1. K线 + 布林带
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
        if self._peak_eq == 0:
            self._peak_eq = equity
        self._peak_eq = max(self._peak_eq, equity)

        if check_drawdown(equity, self._peak_eq, MAX_DRAWDOWN):
            logger.error("触发最大回撤，清仓停机")
            dd = (self._peak_eq - equity) / self._peak_eq
            await notify_drawdown(equity, self._peak_eq, dd)
            await self._emergency_close(client)
            self._running = False
            return

        mark_price = await client.get_mark_price(INST_ID)
        self._remember_price(mark_price)
        last = df.iloc[-1].copy()
        last["ts"] = current_kline_ts
        if self._block_recovery_on_first_tick and self._state.is_active():
            self._last_batch_kline_ts = last["ts"]
            self._last_recovery_kline_ts = last["ts"]
            self._block_recovery_on_first_tick = False
            logger.info("启动恢复持仓，本根K线不再新增补仓单，等待下一根K重新判断")

        logger.info(
            f"价格={mark_price:.2f}  布林[{last['boll_lower']:.2f}"
            f" | {last['boll_mid']:.2f} | {last['boll_upper']:.2f}]"
            f"  持仓={self._state.direction}  权益={equity:.2f}"
        )

        # 3. 同步成交
        await self._sync_fills(client, mark_price, last["ts"])

        # 4. 检查持仓是否已平，并恢复旧仓位缺失的补仓单
        if self._state.is_active():
            await self._check_position_closed(client, mark_price)
        if self._state.is_active():
            await self._recover_missing_entry_orders(client, df, last, equity, mark_price)

        # 5. 强平预警（距强平价 < 3%）
        if self._state.is_active() and self._state.plan_liq_price > 0:
            liq = self._state.plan_liq_price
            if self._state.direction == "long":
                gap_pct = (mark_price - liq) / mark_price * 100
            else:
                gap_pct = (liq - mark_price) / mark_price * 100
            if 0 < gap_pct < 3:
                await notify_liq_warning(self._state.direction, mark_price, liq, gap_pct)

        # 6. 盘中评分通过后逐批挂单，每根K线最多新增一批
        if self._state.has_working_plan():
            if self._state.is_active():
                await self._maybe_place_next_batch(client, df, last, equity, mark_price)
            else:
                await self._maybe_reprice_probe_batch(client, df, last, equity, mark_price)
        else:
            if not self._boll_width_ok(last, mark_price):
                pass
            else:
                await self._maybe_place_probe_batch(client, df, last, mark_price, equity)

        # 7. 更新看板
        self._update_dashboard(mark_price, last, equity)

    def _boll_width_ok(self, last, mark_price: float) -> bool:
        """Return whether current Bollinger width allows new entries."""
        width = float(last["boll_width"])
        width_pct = width / mark_price if mark_price > 0 else 0.0

        if width < MIN_BOLL_WIDTH_USD or width_pct < MIN_BOLL_WIDTH_PCT:
            logger.info(
                f"布林带过窄，跳过开仓 width={width:.2f} "
                f"width_pct={width_pct:.2%} 阈值={MIN_BOLL_WIDTH_USD:.2f}/{MIN_BOLL_WIDTH_PCT:.2%}"
            )
            return False

        return True

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
            logger.info("价格仍在继续创新低，暂不开首批多单")
            return
        if direction == "short" and self._still_making_new_high():
            logger.info("价格仍在继续创新高，暂不开首批空单")
            return

        if not self._can_open_new_plan(last["ts"], mark_price):
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
            logger.warning("盘中破轨信号风控未通过，放弃本次信号")
            return

        first_order = self._plan_order_at(plan, 0)
        if first_order is None:
            return

        logger.info(f"盘中破轨预入场 方向={direction}  第一批参考价≈{mark_price:.2f}")
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
            logger.info(
                f"挂单已回到布林带内 {self._inside_band_kline_count}/"
                f"{INSIDE_BAND_CANCEL_KLINES} 根K线，暂不撤单"
            )
            return False

        logger.info(
            f"挂单连续 {self._inside_band_kline_count} 根K线未重新触发轨外，"
            f"撤销第{pending_batch.batch_idx+1}批挂单"
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
        if self._last_plan_kline_ts is not None and kline_ts == self._last_plan_kline_ts:
            logger.info(f"本根K线已开过一套分批计划，跳过信号 ts={kline_ts}")
            return False

        if self._last_plan_entry_price > 0:
            gap = abs(entry_price - self._last_plan_entry_price)
            if gap < MIN_ENTRY_GAP_USD:
                logger.info(
                    f"入场价与上次计划差距不足 {MIN_ENTRY_GAP_USD:.2f} USDT，"
                    f"上次={self._last_plan_entry_price:.2f} 本次={entry_price:.2f} 差距={gap:.2f}"
                )
                return False

        return True

    async def _maybe_place_next_batch(self, client: OKXClient, df, last, equity: float, mark_price: float):
        """Place or maintain the next batch after the first batch has filled."""
        if not self._state.is_active():
            return

        pending_batch = self._state.pending_batch()
        if pending_batch is not None:
            if await self._cancel_pending_if_inside_too_long(client, last, mark_price):
                return
            await self._maybe_reprice_pending_batch(client, df, last, equity, mark_price, pending_batch)
            return

        if not self._boll_width_ok(last, mark_price):
            return

        trigger_direction = self._intrabar_probe_direction(df, last, mark_price)
        if trigger_direction != self._state.direction:
            return

        if self._state.direction == "long" and self._still_making_new_low():
            logger.info("价格仍在继续创新低，暂不挂下一批多单")
            return
        if self._state.direction == "short" and self._still_making_new_high():
            logger.info("价格仍在继续创新高，暂不挂下一批空单")
            return

        next_idx = self._state.next_batch_idx()
        if next_idx >= BATCH_COUNT:
            self._state.remaining_batches_placed = True
            return

        kline_ts = last["ts"]
        if self._last_batch_kline_ts is not None and kline_ts == self._last_batch_kline_ts:
            return

        last_batch = self._state.last_filled_batch()
        if last_batch is None:
            return
        if self._state.direction == "long" and self._still_making_new_low():
            logger.info(f"价格仍在继续创新低，暂不挂第{next_idx+1}批多单")
            return
        if self._state.direction == "short" and self._still_making_new_high():
            logger.info(f"价格仍在继续创新高，暂不挂第{next_idx+1}批空单")
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
            logger.warning("下一批补仓风控未通过，暂不挂新批次")
            return

        next_plan = self._plan_order_at_price(plan, next_idx, mark_price)
        if next_plan is None:
            self._state.remaining_batches_placed = True
            return

        next_order = next_plan.orders[0]
        gap = abs(next_order.price - last_batch.price)
        if gap < MIN_ENTRY_GAP_USD:
            logger.info(
                f"下一批与上一批价格差距不足 {MIN_ENTRY_GAP_USD:.2f} USDT，"
                f"上一批={last_batch.price:.2f} 下一批={next_order.price:.2f} 差距={gap:.2f}"
            )
            return

        if self._state.direction == "long" and mark_price > last_batch.price:
            return
        if self._state.direction == "short" and mark_price < last_batch.price:
            return

        logger.info(f"逐批补仓触发 第{next_idx+1}批 方向={self._state.direction}")
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
        if gap_from_filled < MIN_ENTRY_GAP_USD:
            self._save_runtime_state()
            return

        if abs(next_order.price - pending_batch.price) < REPRICE_GAP_USD:
            self._save_runtime_state()
            return

        logger.info(
            f"K线更新后重挂第{pending_batch.batch_idx+1}批挂单 "
            f"旧价={pending_batch.price:.2f} 新价={next_order.price:.2f}"
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

        kline_ts = last["ts"]
        if self._last_entry_check_kline_ts is not None and kline_ts == self._last_entry_check_kline_ts:
            return
        self._last_entry_check_kline_ts = kline_ts

        if await self._cancel_pending_if_inside_too_long(client, last, mark_price):
            return

        if not self._boll_width_ok(last, mark_price):
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

        logger.info(
            f"K线更新后重挂第1批头仓挂单 "
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
            tp_price = price + TP_PROFIT_USD
        else:
            liq_price = price + ((margin * 0.9) / (order.sz * CT_VAL))
            tp_price = price - TP_PROFIT_USD

        return replace(
            one_order_plan,
            orders=[updated_order],
            avg_entry=price,
            liq_price=round(liq_price, 2),
            sl_price=round(liq_price, 2),
            tp_price=round(tp_price, 2),
            total_margin=round(margin, 2),
        )

    # ── 下批次限价单 ──────────────────────────────────────────────────────

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
                    total=BATCH_COUNT,
                    ord_id=ord_id,
                )
                logger.info(f"第{bo.batch_idx+1}批挂单 价格={bo.price}  张数={bo.sz}  ordId={ord_id}")
            except Exception as e:
                logger.error(f"第{bo.batch_idx+1}批下单失败: {e}")

        if not placed_any:
            if had_batches:
                logger.warning("本次新批次下单未成功，保留现有持仓状态")
                return False
            logger.warning("本次计划没有任何批次挂单成功，重置状态")
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
            logger.warning(f"恢复补仓单前查询未成交订单失败: {e}")
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
            max_batch_idx = max(1, self._state.next_batch_idx()),
            fixed_batch_sizes = self._fixed_batch_sizes,
        )
        if not plan.safe:
            logger.warning("已有持仓恢复补仓单风控未通过，暂不补挂")
            self._last_recovery_kline_ts = last["ts"]
            return

        next_idx = max(1, self._state.next_batch_idx())
        recovery_plan = self._plan_order_at_price(plan, next_idx, mark_price)
        if recovery_plan is None:
            self._state.remaining_batches_placed = True
            return

        last_batch = self._state.last_batch()
        if last_batch is not None:
            next_order = recovery_plan.orders[0]
            gap = abs(next_order.price - last_batch.price)
            if gap < MIN_ENTRY_GAP_USD:
                logger.info(
                    f"恢复下一批与上一批价格差距不足 {MIN_ENTRY_GAP_USD:.2f} USDT，"
                    f"上一批={last_batch.price:.2f} 下一批={next_order.price:.2f} 差距={gap:.2f}"
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
            f"检测到已有{self._state.direction}持仓但无补仓单，"
            f"按真实均价={self._state.avg_entry:.2f} 恢复第{next_idx+1}批"
        )
        self._log_plan(recovery_plan, mark_price)
        if await self._place_batch_orders(client, recovery_plan, remaining_batches_placed=False):
            self._last_batch_kline_ts = last["ts"]
            self._last_recovery_kline_ts = last["ts"]

    # ── 检查挂单成交 ──────────────────────────────────────────────────────

    async def _sync_fills(self, client: OKXClient, mark_price: float, kline_ts=None):
        """Synchronize filled and canceled entry orders from OKX."""
        if not self._state.batches:
            return

        prev_sz = self._state.total_sz
        filled_this_tick = False
        canceled_batches = []
        for batch in self._state.batches:
            if batch.filled:
                continue
            try:
                order_info = await client.get_order(INST_ID, batch.ord_id)
                if order_info.get("state") == "filled":
                    self._state.mark_filled(batch.ord_id, batch.sz)
                    filled_this_tick = True
                elif order_info.get("state") in ("canceled", "cancelled"):
                    canceled_batches.append(batch)
                    logger.info(f"第{batch.batch_idx+1}批已撤销 ordId={batch.ord_id}")
            except Exception as e:
                logger.warning(f"查询订单 {batch.ord_id} 失败: {e}")

        for batch in canceled_batches:
            self._state.remove_batch(batch.ord_id)
        if canceled_batches:
            self._save_runtime_state()

        if filled_this_tick and kline_ts is not None:
            self._last_batch_kline_ts = kline_ts
            logger.info(f"本根K线已有入场批次成交，后续补仓等待下一根K线 ts={kline_ts}")

        if self._state.total_sz != prev_sz:
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
        if self._state.direction == "long":
            self._state.plan_tp_price = round(avg_entry + TP_PROFIT_USD, 2)
        else:
            self._state.plan_tp_price = round(avg_entry - TP_PROFIT_USD, 2)
        logger.info(f"均价={avg_entry:.2f}  新止盈={self._state.plan_tp_price}")
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

        if avg_entry > 0:
            if pos_side == "long":
                self._state.plan_tp_price = round(avg_entry + TP_PROFIT_USD, 2)
            else:
                self._state.plan_tp_price = round(avg_entry - TP_PROFIT_USD, 2)

        logger.info(
            f"交易所持仓同步 均价={avg_entry:.2f} 张数={total_sz} "
            f"真实强平={liq_price:.2f} 新止盈={self._state.plan_tp_price:.2f}"
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
            f"按现有持仓恢复本地第1批记录 均价={self._state.avg_entry:.2f} "
            f"张数={self._state.total_sz}"
        )

    async def _update_tp(self, client: OKXClient):
        """Place the current reduce-only take-profit limit order."""
        pos_side   = self._state.direction
        close_side = "sell" if pos_side == "long" else "buy"

        if self._state.total_sz <= 0 or self._state.plan_tp_price <= 0:
            return

        if self._state.tp_ord_id:
            await client.cancel_order(INST_ID, self._state.tp_ord_id)
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
            logger.info(f"止盈挂单 价格={self._state.plan_tp_price}  张数={self._state.total_sz}")
        except Exception as e:
            logger.error(f"挂止盈失败: {e}")

        logger.info(f"当前强平价={self._state.plan_liq_price}（强平线作为最终风险边界）")

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
            logger.warning(f"止损价无效，跳过挂止损 sl={sl_price}")
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
            logger.info(
                f"强平线止损挂单 触发价={sl_price} "
                f"强平价={self._state.plan_liq_price} 张数={self._state.total_sz}"
            )
        except Exception as e:
            logger.error(f"挂强平线止损失败: {e}")

    # ── 检查持仓是否已平 ──────────────────────────────────────────────────

    async def _check_position_closed(self, client: OKXClient, mark_price: float):
        """Detect external position close and reset local state."""
        pos = await client.get_position(INST_ID)
        if pos is None or float(pos.get("pos", 0)) == 0:
            # 计算本次盈亏
            filled = self._state.filled_batches()
            avg_entry = self._state.avg_entry
            total_sz = self._state.total_sz
            if filled and avg_entry <= 0:
                total_sz       = sum(b.sz for b in filled)
                avg_entry      = sum(b.price * b.sz for b in filled) / total_sz
            if total_sz > 0 and avg_entry > 0:
                from src.config import CT_VAL
                if self._state.direction == "long":
                    pnl = (mark_price - avg_entry) * total_sz * CT_VAL
                else:
                    pnl = (avg_entry - mark_price) * total_sz * CT_VAL
                dashboard.state.add_trade(
                    action="平多" if self._state.direction == "long" else "平空",
                    price=mark_price,
                    sz=total_sz,
                    pnl=pnl,
                )
                await notify_close(self._state.direction, avg_entry, mark_price, pnl, total_sz)

            await self._cancel_entry_orders(client)
            await self._cancel_exchange_exit_orders(client)
            await self._cancel_exit_orders(client)
            logger.info("持仓已关闭，重置状态")
            self._last_plan_kline_ts = None
            self._last_batch_kline_ts = None
            self._last_plan_entry_price = 0.0
            self._reset_probe_state()
            self._state.reset()
            self._clear_runtime_state()
            await self._rebalance_accounts(client)
            await self._init_fixed_batch_sizes(client)

    # ── 恢复状态 ──────────────────────────────────────────────────────────

    async def _sync_state(self, client: OKXClient):
        """Reconcile local state with the exchange on startup."""
        pos = await client.get_position(INST_ID)
        if pos and float(pos.get("pos", 0)) != 0:
            pos_side = pos.get("posSide", "")
            sz       = float(pos.get("pos", 0))
            logger.info(f"检测到现有持仓: {pos_side} {sz}张，继续监控")
            self._state.direction = pos_side
            self._block_recovery_on_first_tick = True
            await self._sync_exchange_position(client)
            await self._reconcile_entry_orders_after_restart(client)
            await self._replace_exit_orders(client)
        else:
            await self._cancel_exchange_open_orders(client)
            self._reset_probe_state()
            self._state.reset()
            self._clear_runtime_state()
            if not self._fixed_batch_sizes:
                await self._init_fixed_batch_sizes(client)
            logger.info("无现有持仓，策略就绪")

    # ── 紧急平仓 ──────────────────────────────────────────────────────────

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
                logger.error(f"紧急平仓失败: {e}")

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
                logger.warning(f"撤销剩余补仓单失败 ordId={batch.ord_id}: {e}")
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

        logger.info("价格未突破布林轨，撤销未成交补仓单，等待下一次突破")
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

        logger.info("价格收回布林带内，撤销未成交第一批挂单")
        await client.cancel_order(INST_ID, pending.ord_id)
        self._state.remove_batch(pending.ord_id)
        if pending.batch_idx == 0 and self._last_plan_kline_ts == last["ts"]:
            self._last_plan_kline_ts = None
            self._last_batch_kline_ts = None
            self._last_plan_entry_price = 0.0
            logger.info("首批未成交已撤销，本根K线开仓限制已释放")
        self._reset_probe_state()
        self._state.reset()
        self._clear_runtime_state()

    async def _cancel_exchange_open_orders(self, client: OKXClient):
        """Cancel all exchange open orders for the instrument."""
        try:
            orders = await client.get_open_orders(INST_ID)
        except Exception as e:
            logger.warning(f"查询未成交订单失败: {e}")
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
            logger.warning(f"重启校验交易所补仓单失败: {e}")
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
                    batch.filled = True
                    unmatched_filled_sz -= batch.sz
                    logger.info(f"本地记录的第{batch.batch_idx+1}批挂单已在停机期间成交 ordId={batch.ord_id}")
                else:
                    removed.append(batch)
                    logger.info(f"本地记录的第{batch.batch_idx+1}批挂单已不在交易所，移除 ordId={batch.ord_id}")
        for batch in removed:
            self._state.remove_batch(batch.ord_id)
        local_pending_ids = {b.ord_id for b in self._state.batches if not b.filled and b.ord_id}

        for order in entry_orders:
            ord_id = order.get("ordId")
            if not ord_id or ord_id in local_pending_ids:
                continue
            logger.info(f"重启发现非本地记录的补仓挂单，撤销 ordId={ord_id}")
            await client.cancel_order(INST_ID, ord_id)

        if local_pending_ids:
            logger.info(f"重启已接续交易所未成交补仓单 ordId={sorted(local_pending_ids)}")
        self._save_runtime_state()

    async def _cancel_exchange_entry_orders(self, client: OKXClient):
        """Cancel exchange entry orders for the current side.

        Reserved helper. It is not called by the current live path.
        """
        entry_side = "buy" if self._state.direction == "long" else "sell"
        try:
            orders = await client.get_open_orders(INST_ID)
        except Exception as e:
            logger.warning(f"查询交易所补仓单失败: {e}")
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
                logger.info(f"启动/恢复时撤销交易所未成交补仓单 ordId={ord_id}")
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
        last_batch = self._state.last_batch()
        if last_batch is None:
            return

        entry_side = "buy" if self._state.direction == "long" else "sell"
        try:
            orders = await client.get_open_orders(INST_ID)
        except Exception as e:
            logger.warning(f"查询补仓单失败: {e}")
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
            invalid = gap < MIN_ENTRY_GAP_USD
            if self._state.direction == "long" and px >= last_batch.price:
                invalid = True
            if self._state.direction == "short" and px <= last_batch.price:
                invalid = True

            if invalid:
                ord_id = order.get("ordId")
                if ord_id:
                    logger.info(
                        f"撤销无效补仓单 ordId={ord_id} 价格={px:.2f} "
                        f"上一批={last_batch.price:.2f} 差距={gap:.2f}"
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
            logger.warning(f"检查未成交订单失败: {e}")
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
            logger.info("分批计划没有持仓和未成交补仓单，重置状态")
            self._reset_probe_state()
            self._state.reset()
            self._clear_runtime_state()

    def _reset_probe_state(self):
        """Clear first-probe metadata."""
        self._probe_kline_ts = None
        self._probe_direction = "none"
        self._probe_entry_price = 0.0

    # ── 更新看板状态 ──────────────────────────────────────────────────────

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

    # ── 日志输出建仓计划 ──────────────────────────────────────────────────

    def _log_plan(self, plan, mark_price: float):
        """Log a human-readable batch-entry plan."""
        logger.info("=" * 60)
        logger.info(f"建仓计划 方向={plan.direction}  当前价={mark_price:.2f}")
        logger.info(f"  止盈={plan.tp_price}  估算强平={plan.liq_price}")
        logger.info(f"  合计保证金={plan.total_margin:.2f} USDT")
        for bo in plan.orders:
            logger.info(
                f"  第{bo.batch_idx+1}批: 价格={bo.price}  张数={bo.sz}"
                f"  名义={bo.notional:.2f}  保证金={bo.margin:.2f}"
            )
        logger.info("=" * 60)

    # ── 资金账户再平衡 ────────────────────────────────────────────────────

    async def _rebalance_accounts(self, client: OKXClient) -> None:
        """Keep the trading account near ``TRADING_ACCOUNT_TARGET``."""
        """
        每次平仓后调用：
          盈利 -> 将超出 TRADING_ACCOUNT_TARGET 的部分划转到资金账户
          亏损 -> 从资金账户补回交易账户，使可用余额恢复到 TRADING_ACCOUNT_TARGET
        TRADING_ACCOUNT_TARGET = 0 时跳过。
        """
        if TRADING_ACCOUNT_TARGET <= 0:
            return
        try:
            trading_bal = await client.get_balance("USDT")
            diff = round(trading_bal - TRADING_ACCOUNT_TARGET, 4)

            if diff > 0.01:
                logger.info(
                    f"[资金管理] 盈利 +{diff:.4f} USDT，"
                    f"交易账户 {trading_bal:.4f} -> {TRADING_ACCOUNT_TARGET:.4f}，"
                    f"划入资金账户"
                )
                await client.transfer(amt=diff, from_acct="18", to_acct="6")

            elif diff < -0.01:
                needed = abs(diff)
                funding_bal = await client.get_funding_balance("USDT")
                top_up = round(min(needed, funding_bal), 4)
                if top_up < 0.01:
                    logger.warning(
                        f"[资金管理] 资金账户余额不足（{funding_bal:.4f} USDT），"
                        f"无法补充交易账户"
                    )
                    return
                partial = "（资金账户不足，仅补部分）" if top_up < needed else ""
                logger.info(
                    f"[资金管理] 亏损 {diff:.4f} USDT，"
                    f"从资金账户划入 {top_up:.4f} USDT{partial}"
                )
                await client.transfer(amt=top_up, from_acct="6", to_acct="18")

            else:
                logger.debug("[资金管理] 余额与目标相差不足 0.01 USDT，跳过划转")

        except Exception as e:
            logger.warning(f"[资金管理] 划转失败，不影响策略继续运行: {e}")

    def stop(self):
        """Request the main strategy loop to stop."""
        self._running = False
        logger.info("策略停止")
