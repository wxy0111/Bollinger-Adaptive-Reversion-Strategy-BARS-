"""
布林带插针均值回归策略
  - 15m K线检测插针 → 生成分批建仓计划
  - 50x 全仓，分批限价挂单
  - 实时跟踪成交 → 动态更新止盈（均价 ± 10 USDT）
  - 强平价作为隐性止损
  - 微信推送 + 本地看板
"""
import asyncio
import aiohttp
from loguru import logger

from src.config import (
    INST_ID, BAR_15M, LEVER, KLINE_LIMIT,
    POLL_INTERVAL, MAX_DRAWDOWN, BOLL_PERIOD,
    TP_PROFIT_USD,
)
from src.okx_client import OKXClient
from src.indicators import build_df, add_boll, detect_pin
from src.risk import build_batch_plan, check_drawdown
from src.position_manager import PositionState, OpenBatch
from src.notify import notify_open, notify_close, notify_liq_warning, notify_drawdown
import src.dashboard as dashboard


class BollPinStrategy:
    def __init__(self):
        self._state   = PositionState(direction="none")
        self._peak_eq = 0.0
        self._running = False

    # ── 主循环 ────────────────────────────────────────────────────────────

    async def run(self):
        self._running = True
        logger.info("策略启动  {} 布林插针均值回归 {}x", INST_ID, LEVER)

        async with aiohttp.ClientSession() as session:
            client = OKXClient(session)
            try:
                await client.set_leverage(INST_ID, LEVER)
            except Exception as e:
                logger.warning(f"设置杠杆失败（请在OKX App手动设置为{LEVER}x）: {e}")
            await self._sync_state(client)

            while self._running:
                try:
                    await self._tick(client)
                except Exception as e:
                    logger.exception(f"tick 异常: {e}")
                await asyncio.sleep(POLL_INTERVAL)

    # ── 单次 tick ─────────────────────────────────────────────────────────

    async def _tick(self, client: OKXClient):
        # 1. K线 + 布林带
        raw = await client.get_klines(INST_ID, BAR_15M, KLINE_LIMIT)
        df  = build_df(raw)
        df  = add_boll(df)

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
        last = df.iloc[-1]

        logger.info(
            f"价格={mark_price:.2f}  布林[{last['boll_lower']:.2f}"
            f" | {last['boll_mid']:.2f} | {last['boll_upper']:.2f}]"
            f"  持仓={self._state.direction}  权益={equity:.2f}"
        )

        # 3. 同步成交
        await self._sync_fills(client, mark_price)

        # 4. 检查持仓是否已平
        if self._state.is_active():
            await self._check_position_closed(client, mark_price)

        # 5. 强平预警（距强平价 < 3%）
        if self._state.is_active() and self._state.plan_liq_price > 0:
            liq = self._state.plan_liq_price
            if self._state.direction == "long":
                gap_pct = (mark_price - liq) / mark_price * 100
            else:
                gap_pct = (liq - mark_price) / mark_price * 100
            if 0 < gap_pct < 3:
                await notify_liq_warning(self._state.direction, mark_price, liq, gap_pct)

        # 6. 无持仓时检测插针信号
        if not self._state.is_active():
            pin = detect_pin(df)
            if pin is not None:
                logger.info(f"插针信号！方向={pin.direction}  入场价≈{pin.entry_price:.2f}")
                boll_std_val = float(df["close"].tail(BOLL_PERIOD).std(ddof=0))
                plan = build_batch_plan(
                    direction   = pin.direction,
                    first_price = pin.entry_price,
                    boll_width  = pin.boll_width,
                    boll_mid    = pin.boll_mid,
                    boll_lower  = pin.boll_lower,
                    boll_upper  = pin.boll_upper,
                    boll_std    = boll_std_val,
                    equity      = equity,
                )
                if not plan.safe:
                    logger.warning("风控未通过，放弃本次信号")
                else:
                    self._log_plan(plan, mark_price)
                    await self._place_batch_orders(client, plan)

        # 7. 更新看板
        self._update_dashboard(mark_price, last, equity)

    # ── 下批次限价单 ──────────────────────────────────────────────────────

    async def _place_batch_orders(self, client: OKXClient, plan):
        self._state.direction      = plan.direction
        self._state.plan_liq_price = plan.liq_price
        self._state.plan_sl_price  = plan.sl_price
        self._state.plan_tp_price  = plan.tp_price

        side     = "buy"  if plan.direction == "long"  else "sell"
        pos_side = plan.direction

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
                logger.info(f"第{bo.batch_idx+1}批挂单 价格={bo.price}  张数={bo.sz}  ordId={ord_id}")
            except Exception as e:
                logger.error(f"第{bo.batch_idx+1}批下单失败: {e}")

    # ── 检查挂单成交 ──────────────────────────────────────────────────────

    async def _sync_fills(self, client: OKXClient, mark_price: float):
        if not self._state.batches:
            return

        prev_sz = self._state.total_sz
        for batch in self._state.batches:
            if batch.filled:
                continue
            try:
                order_info = await client.get_order(INST_ID, batch.ord_id)
                if order_info.get("state") == "filled":
                    self._state.mark_filled(batch.ord_id, batch.sz)
            except Exception as e:
                logger.warning(f"查询订单 {batch.ord_id} 失败: {e}")

        if self._state.total_sz != prev_sz:
            avg = self._recalc_tp()
            await self._update_tp(client)
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

    def _recalc_tp(self) -> float:
        filled = self._state.filled_batches()
        if not filled:
            return 0.0
        total_sz       = sum(b.sz for b in filled)
        weighted_price = sum(b.price * b.sz for b in filled)
        avg_entry      = weighted_price / total_sz
        if self._state.direction == "long":
            self._state.plan_tp_price = round(avg_entry + TP_PROFIT_USD, 2)
        else:
            self._state.plan_tp_price = round(avg_entry - TP_PROFIT_USD, 2)
        logger.info(f"均价={avg_entry:.2f}  新止盈={self._state.plan_tp_price}")
        return avg_entry

    async def _update_tp(self, client: OKXClient):
        pos_side   = self._state.direction
        close_side = "sell" if pos_side == "long" else "buy"

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

        logger.info(f"估算强平价={self._state.plan_liq_price}（无止损挂单）")

    # ── 检查持仓是否已平 ──────────────────────────────────────────────────

    async def _check_position_closed(self, client: OKXClient, mark_price: float):
        pos = await client.get_position(INST_ID)
        if pos is None or float(pos.get("pos", 0)) == 0:
            # 计算本次盈亏
            filled = self._state.filled_batches()
            if filled:
                total_sz       = sum(b.sz for b in filled)
                avg_entry      = sum(b.price * b.sz for b in filled) / total_sz
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

            logger.info("持仓已关闭，重置状态")
            self._state.reset()

    # ── 恢复状态 ──────────────────────────────────────────────────────────

    async def _sync_state(self, client: OKXClient):
        pos = await client.get_position(INST_ID)
        if pos and float(pos.get("pos", 0)) != 0:
            pos_side = pos.get("posSide", "")
            sz       = int(float(pos.get("pos", 0)))
            logger.info(f"检测到现有持仓: {pos_side} {sz}张，继续监控")
            self._state.direction = pos_side
            self._state.total_sz  = sz
        else:
            logger.info("无现有持仓，策略就绪")

    # ── 紧急平仓 ──────────────────────────────────────────────────────────

    async def _emergency_close(self, client: OKXClient):
        if self._state.direction in ("long", "short"):
            try:
                await client.close_position(INST_ID, self._state.direction)
                self._state.reset()
            except Exception as e:
                logger.error(f"紧急平仓失败: {e}")

    # ── 更新看板状态 ──────────────────────────────────────────────────────

    def _update_dashboard(self, mark_price: float, last, equity: float):
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
        if filled and mark_price > 0:
            from src.config import CT_VAL
            total_sz   = sum(b.sz for b in filled)
            avg_entry  = sum(b.price * b.sz for b in filled) / total_sz
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

    def stop(self):
        self._running = False
        logger.info("策略停止")
