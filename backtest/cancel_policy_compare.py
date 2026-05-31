"""Compare pending-entry cancel policies on 15-minute history.

This backtest is intentionally isolated from the live strategy. It compares:
1. old_inside_cancel: cancel pending entries when price closes back inside the
   Bollinger band, and cancel when band width becomes too narrow.
2. keep_pending_no_reprice: keep pending entries through inside-band closes.
3. kline_threshold_reprice: keep pending entries through inside-band closes;
   only reprice on a new candle when the theoretical price moves by a threshold.
"""

from __future__ import annotations

import argparse
import csv
import math
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd


BOLL_PERIOD = 20
BOLL_STD = 2.0
CT_VAL = 0.1
LEVER = 50
BATCH_COUNT = 4
BATCH_SIZE_RATIO = [0.2, 0.25, 0.25, 0.25]
BATCH_SPACING = [0.0, 0.2, 0.4, 0.6]
TP_PROFIT_USD = 10.0
MIN_BOLL_WIDTH_USD = 25.0
MIN_BOLL_WIDTH_PCT = 0.006
MIN_ENTRY_GAP_USD = 3.0
REPRICE_GAP_USD = 1.0
CONTRACT_STEP = 0.01
MIN_ORDER_CONTRACTS = 0.01
TAKER_FEE = 0.0005
TARGET_EQUITY = 100.0
INITIAL_TOTAL_EQUITY = 1000.0


@dataclass
class Batch:
    idx: int
    price: float
    sz: float


@dataclass
class Trade:
    entry_time: str
    exit_time: str
    direction: str
    avg_entry: float
    exit_price: float
    sz: float
    pnl: float
    reason: str
    batches: int


@dataclass
class Position:
    direction: str = "none"
    filled: list[Batch] = field(default_factory=list)
    pending: Batch | None = None
    first_price: float = 0.0
    avg_entry: float = 0.0
    total_sz: float = 0.0
    tp_price: float = 0.0
    liq_price: float = 0.0

    def is_active(self) -> bool:
        return self.direction != "none" and self.total_sz > 0

    def reset(self) -> None:
        self.__init__()

    def next_idx(self) -> int:
        if self.pending is not None:
            return self.pending.idx
        if not self.filled:
            return 0
        return max(batch.idx for batch in self.filled) + 1

    def last_filled(self) -> Batch | None:
        if not self.filled:
            return None
        return max(self.filled, key=lambda batch: batch.idx)

    def recalc(self) -> None:
        self.total_sz = sum(batch.sz for batch in self.filled)
        self.avg_entry = sum(batch.price * batch.sz for batch in self.filled) / self.total_sz
        margin = self.total_sz * CT_VAL * self.avg_entry / LEVER
        margin_per_eth = (margin * 0.9) / (self.total_sz * CT_VAL)
        if self.direction == "long":
            self.tp_price = round(self.avg_entry + TP_PROFIT_USD, 2)
            self.liq_price = round(self.avg_entry - margin_per_eth, 2)
        else:
            self.tp_price = round(self.avg_entry - TP_PROFIT_USD, 2)
            self.liq_price = round(self.avg_entry + margin_per_eth, 2)


def floor_to_step(value: float, step: float = CONTRACT_STEP) -> float:
    return math.floor(value / step) * step


def add_boll(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    close = df["close"]
    df["boll_mid"] = close.rolling(BOLL_PERIOD).mean()
    std = close.rolling(BOLL_PERIOD).std(ddof=0)
    df["boll_upper"] = df["boll_mid"] + BOLL_STD * std
    df["boll_lower"] = df["boll_mid"] - BOLL_STD * std
    df["boll_width"] = df["boll_upper"] - df["boll_lower"]
    return df


class CancelPolicyBacktest:
    def __init__(self, df: pd.DataFrame, policy: str):
        self.df = add_boll(df).reset_index(drop=True)
        self.policy = policy
        self.pos = Position()
        self.total_equity = INITIAL_TOTAL_EQUITY
        self.last_plan_price = 0.0
        self.entry_time = ""
        self.trades: list[Trade] = []
        self.equity_curve: list[float] = []
        self.cancel_inside = 0
        self.cancel_width = 0
        self.reprice_count = 0
        self.pending_candle_age = 0
        self.pending_age_samples: list[int] = []

    def run(self) -> "CancelPolicyBacktest":
        for i in range(BOLL_PERIOD, len(self.df)):
            self._process_candle(self.df.iloc[i])
        return self

    def _width_ok(self, row) -> bool:
        width = float(row.boll_width)
        close = float(row.close)
        return width >= MIN_BOLL_WIDTH_USD and width / close >= MIN_BOLL_WIDTH_PCT

    def _signal(self, row) -> str:
        if pd.isna(row.boll_lower) or not self._width_ok(row):
            return "none"
        close = float(row.close)
        if close < float(row.boll_lower):
            return "long"
        if close > float(row.boll_upper):
            return "short"
        return "none"

    def _fixed_size(self, idx: int, price: float) -> float:
        margin_budget = TARGET_EQUITY * BATCH_SIZE_RATIO[idx]
        raw_sz = margin_budget * LEVER / (price * CT_VAL)
        return floor_to_step(raw_sz)

    def _order_for_idx(self, direction: str, first_price: float, boll_width: float, idx: int) -> Batch | None:
        spacing = boll_width * BATCH_SPACING[idx]
        price = first_price - spacing if direction == "long" else first_price + spacing
        price = round(price, 2)
        if price <= 0:
            return None
        sz = self._fixed_size(idx, price)
        if sz < MIN_ORDER_CONTRACTS:
            return None
        return Batch(idx=idx, price=price, sz=sz)

    def _process_candle(self, row) -> None:
        if self.pos.direction != "none":
            self._try_fill_pending(row)
            if self.pos.is_active():
                self._try_exit(row)
            if self.pos.direction != "none":
                self._maintain_pending(row)

        if self.pos.direction == "none":
            self._try_open(row)

        self.equity_curve.append(self.total_equity + self._unrealized(float(row.close)))

    def _try_open(self, row) -> None:
        direction = self._signal(row)
        if direction == "none":
            return
        close = float(row.close)
        if self.last_plan_price > 0 and abs(close - self.last_plan_price) < MIN_ENTRY_GAP_USD:
            return
        first = self._order_for_idx(direction, close, float(row.boll_width), 0)
        if first is None:
            return

        self.pos.direction = direction
        self.pos.first_price = close
        self.entry_time = str(row.datetime_utc)
        self.last_plan_price = close
        self._fill(first)
        self._maintain_pending(row)

    def _maintain_pending(self, row) -> None:
        if not self.pos.is_active():
            return

        outside_same_side = (
            self.pos.direction == "long"
            and float(row.close) < float(row.boll_lower)
        ) or (
            self.pos.direction == "short"
            and float(row.close) > float(row.boll_upper)
        )

        if self.policy == "old_inside_cancel":
            if self.pos.pending is not None and not self._width_ok(row):
                self.cancel_width += 1
                self.pos.pending = None
                self.pending_candle_age = 0
                return
            if self.pos.pending is not None and not outside_same_side:
                self.cancel_inside += 1
                self.pos.pending = None
                self.pending_candle_age = 0
                return

        if self.pos.pending is None:
            self._place_next_if_allowed(row, outside_same_side)
            return

        self.pending_candle_age += 1
        if self.policy != "kline_threshold_reprice":
            return
        if not self._width_ok(row) or not outside_same_side:
            return

        current = self.pos.pending
        replacement = self._theoretical_next_order(row, current.idx)
        if replacement is None:
            return
        last_filled = self.pos.last_filled()
        if last_filled is None:
            return
        if abs(replacement.price - last_filled.price) < MIN_ENTRY_GAP_USD:
            return
        if abs(replacement.price - current.price) < REPRICE_GAP_USD:
            return

        self.reprice_count += 1
        self.pos.pending = replacement
        self.pending_candle_age = 0

    def _place_next_if_allowed(self, row, outside_same_side: bool) -> None:
        if not self._width_ok(row) or not outside_same_side:
            return
        idx = self.pos.next_idx()
        if idx >= BATCH_COUNT:
            return
        order = self._theoretical_next_order(row, idx)
        if order is None:
            return
        last = self.pos.last_filled()
        if last is None:
            return
        if abs(order.price - last.price) < MIN_ENTRY_GAP_USD:
            return
        close = float(row.close)
        if self.pos.direction == "long" and close > last.price:
            return
        if self.pos.direction == "short" and close < last.price:
            return
        self.pos.pending = order
        self.pending_candle_age = 0

    def _theoretical_next_order(self, row, idx: int) -> Batch | None:
        return self._order_for_idx(
            self.pos.direction,
            self.pos.first_price,
            float(row.boll_width),
            idx,
        )

    def _try_fill_pending(self, row) -> None:
        if self.pos.pending is None:
            return
        pending = self.pos.pending
        if self.pos.direction == "long":
            filled = float(row.low) <= pending.price
        else:
            filled = float(row.high) >= pending.price
        if not filled:
            return
        self.pending_age_samples.append(self.pending_candle_age)
        self._fill(pending)
        self.pos.pending = None
        self.pending_candle_age = 0

    def _fill(self, batch: Batch) -> None:
        fee = batch.sz * CT_VAL * batch.price * TAKER_FEE
        self.total_equity -= fee
        self.pos.filled.append(batch)
        self.pos.recalc()

    def _try_exit(self, row) -> None:
        if self.pos.direction == "long":
            liq_hit = float(row.low) <= self.pos.liq_price
            tp_hit = float(row.high) >= self.pos.tp_price
        else:
            liq_hit = float(row.high) >= self.pos.liq_price
            tp_hit = float(row.low) <= self.pos.tp_price
        if liq_hit:
            self._close(self.pos.liq_price, str(row.datetime_utc), "LIQ")
        elif tp_hit:
            self._close(self.pos.tp_price, str(row.datetime_utc), "TP")

    def _close(self, exit_price: float, ts: str, reason: str) -> None:
        avg = self.pos.avg_entry
        sz = self.pos.total_sz
        if self.pos.direction == "long":
            raw_pnl = (exit_price - avg) * sz * CT_VAL
        else:
            raw_pnl = (avg - exit_price) * sz * CT_VAL
        fee = sz * CT_VAL * exit_price * TAKER_FEE
        pnl = raw_pnl - fee
        self.total_equity += pnl
        self.trades.append(
            Trade(
                entry_time=self.entry_time,
                exit_time=ts,
                direction=self.pos.direction,
                avg_entry=round(avg, 4),
                exit_price=round(exit_price, 4),
                sz=round(sz, 8),
                pnl=round(pnl, 4),
                reason=reason,
                batches=len(self.pos.filled),
            )
        )
        self.pos.reset()
        self.entry_time = ""
        self.pending_candle_age = 0

    def _unrealized(self, mark_price: float) -> float:
        if not self.pos.is_active():
            return 0.0
        if self.pos.direction == "long":
            return (mark_price - self.pos.avg_entry) * self.pos.total_sz * CT_VAL
        return (self.pos.avg_entry - mark_price) * self.pos.total_sz * CT_VAL

    def report(self) -> dict:
        pnls = [trade.pnl for trade in self.trades]
        wins = [pnl for pnl in pnls if pnl > 0]
        losses = [pnl for pnl in pnls if pnl <= 0]
        tp = [trade for trade in self.trades if trade.reason == "TP"]
        liq = [trade for trade in self.trades if trade.reason == "LIQ"]
        long_trades = [trade for trade in self.trades if trade.direction == "long"]
        short_trades = [trade for trade in self.trades if trade.direction == "short"]
        peak = INITIAL_TOTAL_EQUITY
        max_dd = 0.0
        for equity in self.equity_curve:
            peak = max(peak, equity)
            max_dd = max(max_dd, (peak - equity) / peak if peak else 0.0)
        profit_factor = abs(sum(wins) / sum(losses)) if losses else float("inf")
        return {
            "policy": self.policy,
            "final_equity": round(self.total_equity, 4),
            "total_pnl": round(self.total_equity - INITIAL_TOTAL_EQUITY, 4),
            "return_pct": round((self.total_equity / INITIAL_TOTAL_EQUITY - 1) * 100, 4),
            "max_drawdown_pct": round(max_dd * 100, 4),
            "trades": len(self.trades),
            "long_trades": len(long_trades),
            "short_trades": len(short_trades),
            "tp": len(tp),
            "liq": len(liq),
            "win_rate_pct": round(len(wins) / len(pnls) * 100, 4) if pnls else 0.0,
            "avg_win": round(float(np.mean(wins)) if wins else 0.0, 4),
            "avg_loss": round(float(np.mean(losses)) if losses else 0.0, 4),
            "profit_factor": round(float(profit_factor), 4),
            "avg_batches": round(float(np.mean([trade.batches for trade in self.trades])) if self.trades else 0.0, 4),
            "cancel_inside": self.cancel_inside,
            "cancel_width": self.cancel_width,
            "reprice_count": self.reprice_count,
            "avg_pending_age_candles": round(float(np.mean(self.pending_age_samples)) if self.pending_age_samples else 0.0, 4),
        }


def load_history(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    for col in ["open", "high", "low", "close", "vol"]:
        df[col] = df[col].astype(float)
    df["timestamp_ms"] = df["timestamp_ms"].astype("int64")
    return df


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default="ETH_USDT_15m_history.csv")
    parser.add_argument("--out", default="backtest/results/cancel_policy_compare.csv")
    args = parser.parse_args()

    df = load_history(Path(args.csv))
    rows = []
    tests = []
    for policy in ["old_inside_cancel", "keep_pending_no_reprice", "kline_threshold_reprice"]:
        bt = CancelPolicyBacktest(df, policy).run()
        rows.append(bt.report())
        tests.append(bt)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    trades_out = out.with_name(out.stem + "_trades.csv")
    with trades_out.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["policy", "entry_time", "exit_time", "direction", "avg_entry", "exit_price", "sz", "pnl", "reason", "batches"])
        for bt in tests:
            for trade in bt.trades:
                writer.writerow([
                    bt.policy,
                    trade.entry_time,
                    trade.exit_time,
                    trade.direction,
                    trade.avg_entry,
                    trade.exit_price,
                    trade.sz,
                    trade.pnl,
                    trade.reason,
                    trade.batches,
                ])

    print(pd.DataFrame(rows).to_string(index=False))
    print(f"summary={out}")
    print(f"trades={trades_out}")


if __name__ == "__main__":
    main()
