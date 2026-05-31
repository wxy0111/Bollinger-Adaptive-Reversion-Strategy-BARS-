"""Compare cancel policies with fixed-capital account rebalancing.

This script models the live strategy's capital behavior more closely than the
simple historical backtest:

- Trading account starts at TRADING_ACCOUNT_TARGET.
- Funding account holds the remaining starting equity.
- After a position closes, profit above the target is moved to funding.
- Losses are topped up from funding when possible.
- Batch sizes are recalculated from the trading account after every close.
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
NO_NEW_EXTREME_TICKS = 2
CONTRACT_STEP = 0.01
MIN_ORDER_CONTRACTS = 0.01
TAKER_FEE = 0.0005
TRADING_ACCOUNT_TARGET = 100.0
INITIAL_TOTAL_EQUITY = 1000.0


@dataclass
class Batch:
    idx: int
    price: float
    sz: float


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
        known = [batch.idx for batch in self.filled]
        if self.pending is not None:
            known.append(self.pending.idx)
        return max(known) + 1 if known else 0

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


@dataclass
class Trade:
    policy: str
    entry_time: str
    exit_time: str
    direction: str
    avg_entry: float
    exit_price: float
    sz: float
    pnl: float
    reason: str
    batches: int
    trading_balance: float
    funding_balance: float


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


class FixedCapitalCompare:
    def __init__(self, df: pd.DataFrame, policy: str):
        self.df = add_boll(df).reset_index(drop=True)
        self.policy = policy
        self.pos = Position()
        self.trading_balance = TRADING_ACCOUNT_TARGET
        self.funding_balance = INITIAL_TOTAL_EQUITY - TRADING_ACCOUNT_TARGET
        self.fixed_batch_sizes: list[float] = []
        self.last_plan_price = 0.0
        self.last_batch_ts = None
        self.entry_time = ""
        self.recent_closes: list[float] = []
        self.trades: list[Trade] = []
        self.equity_curve: list[float] = []
        self.cancel_inside = 0
        self.cancel_extreme = 0
        self.cancel_width = 0
        self.reprice_count = 0
        self.rebalance_profit = 0.0
        self.rebalance_topup = 0.0

    def run(self) -> "FixedCapitalCompare":
        first_valid = self.df.iloc[BOLL_PERIOD]
        self._init_fixed_batch_sizes(float(first_valid.close))
        for i in range(BOLL_PERIOD, len(self.df)):
            row = self.df.iloc[i]
            self._remember_close(float(row.close))
            self._process_candle(row)
        return self

    def total_equity(self) -> float:
        return self.trading_balance + self.funding_balance

    def _init_fixed_batch_sizes(self, mark_price: float) -> None:
        self.fixed_batch_sizes = []
        equity = max(self.trading_balance, 0.0)
        for idx in range(BATCH_COUNT):
            margin_budget = equity * BATCH_SIZE_RATIO[idx]
            raw_sz = margin_budget * LEVER / (mark_price * CT_VAL)
            self.fixed_batch_sizes.append(floor_to_step(raw_sz))

    def _remember_close(self, close: float) -> None:
        self.recent_closes.append(close)
        keep = max(NO_NEW_EXTREME_TICKS + 1, 3)
        if len(self.recent_closes) > keep:
            self.recent_closes = self.recent_closes[-keep:]

    def _still_making_new_low(self) -> bool:
        if len(self.recent_closes) < NO_NEW_EXTREME_TICKS + 1:
            return True
        recent = self.recent_closes[-(NO_NEW_EXTREME_TICKS + 1):]
        return recent[-1] <= min(recent[:-1])

    def _still_making_new_high(self) -> bool:
        if len(self.recent_closes) < NO_NEW_EXTREME_TICKS + 1:
            return True
        recent = self.recent_closes[-(NO_NEW_EXTREME_TICKS + 1):]
        return recent[-1] >= max(recent[:-1])

    def _width_ok(self, row) -> bool:
        width = float(row.boll_width)
        close = float(row.close)
        return width >= MIN_BOLL_WIDTH_USD and width / close >= MIN_BOLL_WIDTH_PCT

    def _outside_direction(self, row) -> str:
        close = float(row.close)
        if close < float(row.boll_lower):
            return "long"
        if close > float(row.boll_upper):
            return "short"
        return "none"

    def _signal(self, row) -> str:
        if pd.isna(row.boll_lower) or not self._width_ok(row):
            return "none"
        direction = self._outside_direction(row)
        if direction == "long" and self._still_making_new_low():
            return "none"
        if direction == "short" and self._still_making_new_high():
            return "none"
        return direction

    def _order_at(self, direction: str, first_price: float, boll_width: float, idx: int) -> Batch | None:
        if idx >= BATCH_COUNT:
            return None
        spacing = boll_width * BATCH_SPACING[idx]
        price = first_price - spacing if direction == "long" else first_price + spacing
        price = round(price, 2)
        if price <= 0:
            return None
        sz = self.fixed_batch_sizes[idx] if idx < len(self.fixed_batch_sizes) else 0.0
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

        self.equity_curve.append(self.total_equity() + self._unrealized(float(row.close)))

    def _try_open(self, row) -> None:
        direction = self._signal(row)
        if direction == "none":
            return
        close = float(row.close)
        if self.last_plan_price > 0 and abs(close - self.last_plan_price) < MIN_ENTRY_GAP_USD:
            return

        first = self._order_at(direction, close, float(row.boll_width), 0)
        if first is None:
            return

        self.pos.direction = direction
        self.pos.first_price = close
        self.entry_time = str(row.datetime_utc)
        self.last_plan_price = close
        self.last_batch_ts = row.timestamp_ms
        self._fill(first)

    def _maintain_pending(self, row) -> None:
        if not self.pos.is_active():
            return

        outside = self._outside_direction(row) == self.pos.direction
        making_extreme = (
            self.pos.direction == "long"
            and self._still_making_new_low()
        ) or (
            self.pos.direction == "short"
            and self._still_making_new_high()
        )

        if self.policy == "old_inside_cancel" and self.pos.pending is not None:
            if not self._width_ok(row):
                self.cancel_width += 1
                self.pos.pending = None
                return
            if making_extreme:
                self.cancel_extreme += 1
                self.pos.pending = None
                return
            if not outside:
                self.cancel_inside += 1
                self.pos.pending = None
                return

        if self.pos.pending is None:
            self._place_next(row, outside, making_extreme)
            return

        if self.policy != "kline_threshold_reprice":
            return
        if not self._width_ok(row) or not outside or making_extreme:
            return
        replacement = self._order_at(
            self.pos.direction,
            self.pos.first_price,
            float(row.boll_width),
            self.pos.pending.idx,
        )
        if replacement is None:
            return
        last = self.pos.last_filled()
        if last is None:
            return
        if abs(replacement.price - last.price) < MIN_ENTRY_GAP_USD:
            return
        if abs(replacement.price - self.pos.pending.price) < REPRICE_GAP_USD:
            return
        self.pos.pending = replacement
        self.reprice_count += 1

    def _place_next(self, row, outside: bool, making_extreme: bool) -> None:
        if not self._width_ok(row) or not outside or making_extreme:
            return
        if self.last_batch_ts is not None and row.timestamp_ms == self.last_batch_ts:
            return
        idx = self.pos.next_idx()
        order = self._order_at(self.pos.direction, self.pos.first_price, float(row.boll_width), idx)
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
        self.last_batch_ts = row.timestamp_ms

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
        self._fill(pending)
        self.pos.pending = None

    def _fill(self, batch: Batch) -> None:
        fee = batch.sz * CT_VAL * batch.price * TAKER_FEE
        self.trading_balance -= fee
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
            self._close(self.pos.liq_price, str(row.datetime_utc), "LIQ", float(row.close))
        elif tp_hit:
            self._close(self.pos.tp_price, str(row.datetime_utc), "TP", float(row.close))

    def _close(self, exit_price: float, ts: str, reason: str, mark_price: float) -> None:
        avg = self.pos.avg_entry
        sz = self.pos.total_sz
        if self.pos.direction == "long":
            raw_pnl = (exit_price - avg) * sz * CT_VAL
        else:
            raw_pnl = (avg - exit_price) * sz * CT_VAL
        fee = sz * CT_VAL * exit_price * TAKER_FEE
        pnl = raw_pnl - fee
        self.trading_balance += pnl
        self._rebalance()
        self.trades.append(
            Trade(
                policy=self.policy,
                entry_time=self.entry_time,
                exit_time=ts,
                direction=self.pos.direction,
                avg_entry=round(avg, 4),
                exit_price=round(exit_price, 4),
                sz=round(sz, 8),
                pnl=round(pnl, 4),
                reason=reason,
                batches=len(self.pos.filled),
                trading_balance=round(self.trading_balance, 4),
                funding_balance=round(self.funding_balance, 4),
            )
        )
        self.pos.reset()
        self.entry_time = ""
        self.last_batch_ts = None
        self._init_fixed_batch_sizes(mark_price)

    def _rebalance(self) -> None:
        diff = self.trading_balance - TRADING_ACCOUNT_TARGET
        if diff > 0.01:
            self.trading_balance -= diff
            self.funding_balance += diff
            self.rebalance_profit += diff
        elif diff < -0.01:
            topup = min(abs(diff), max(self.funding_balance, 0.0))
            self.trading_balance += topup
            self.funding_balance -= topup
            self.rebalance_topup += topup

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
        peak = INITIAL_TOTAL_EQUITY
        max_dd = 0.0
        for equity in self.equity_curve:
            peak = max(peak, equity)
            max_dd = max(max_dd, (peak - equity) / peak if peak else 0.0)
        tp = [trade for trade in self.trades if trade.reason == "TP"]
        liq = [trade for trade in self.trades if trade.reason == "LIQ"]
        profit_factor = abs(sum(wins) / sum(losses)) if losses else float("inf")
        return {
            "policy": self.policy,
            "final_total_equity": round(self.total_equity(), 4),
            "trading_balance": round(self.trading_balance, 4),
            "funding_balance": round(self.funding_balance, 4),
            "total_pnl": round(self.total_equity() - INITIAL_TOTAL_EQUITY, 4),
            "return_pct": round((self.total_equity() / INITIAL_TOTAL_EQUITY - 1) * 100, 4),
            "max_drawdown_pct": round(max_dd * 100, 4),
            "trades": len(self.trades),
            "tp": len(tp),
            "liq": len(liq),
            "win_rate_pct": round(len(wins) / len(pnls) * 100, 4) if pnls else 0.0,
            "avg_win": round(float(np.mean(wins)) if wins else 0.0, 4),
            "avg_loss": round(float(np.mean(losses)) if losses else 0.0, 4),
            "profit_factor": round(float(profit_factor), 4),
            "avg_batches": round(float(np.mean([trade.batches for trade in self.trades])) if self.trades else 0.0),
            "cancel_inside": self.cancel_inside,
            "cancel_extreme": self.cancel_extreme,
            "cancel_width": self.cancel_width,
            "reprice_count": self.reprice_count,
            "profit_transferred": round(self.rebalance_profit, 4),
            "loss_topup": round(self.rebalance_topup, 4),
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
    parser.add_argument("--out", default="backtest/results/fixed_capital_cancel_policy_compare.csv")
    args = parser.parse_args()

    df = load_history(Path(args.csv))
    policies = ["old_inside_cancel", "keep_pending_no_reprice", "kline_threshold_reprice"]
    tests = [FixedCapitalCompare(df, policy).run() for policy in policies]
    rows = [test.report() for test in tests]

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    trades_out = out.with_name(out.stem + "_trades.csv")
    with trades_out.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow([
            "policy", "entry_time", "exit_time", "direction", "avg_entry",
            "exit_price", "sz", "pnl", "reason", "batches",
            "trading_balance", "funding_balance",
        ])
        for test in tests:
            for trade in test.trades:
                writer.writerow([
                    trade.policy, trade.entry_time, trade.exit_time,
                    trade.direction, trade.avg_entry, trade.exit_price,
                    trade.sz, trade.pnl, trade.reason, trade.batches,
                    trade.trading_balance, trade.funding_balance,
                ])

    print(pd.DataFrame(rows).to_string(index=False))
    print(f"summary={out}")
    print(f"trades={trades_out}")


if __name__ == "__main__":
    main()
