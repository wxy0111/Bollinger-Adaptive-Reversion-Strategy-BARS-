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
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config import (
    CONTRACT_STEP,
    CT_VAL,
    DYNAMIC_BASE_ENTRY_RATIO,
    DYNAMIC_MAX_ENTRY_RATIO,
    DYNAMIC_MIN_ENTRY_RATIO,
    FIRST_BATCH_RATIO,
    LEVER,
    MAX_ENTRY_BATCHES,
    MAX_TOTAL_ENTRY_RATIO,
    MIN_BOLL_WIDTH_PCT,
    MIN_ORDER_CONTRACTS,
    NO_NEW_EXTREME_TICKS,
    SECOND_BATCH_RATIO,
    TP_PROFIT_USD,
    TRADING_ACCOUNT_TARGET,
)


CONFIG_PATH = ROOT / "src" / "config.py"
DEFAULT_LOG_DIR = ROOT / "logs"
DEFAULT_OUT_DIR = ROOT / "backtest" / "results" / "log_parameter_optimizer"
SOURCE_BOLL_STD = 2.0
TAKER_FEE = 0.0005
INITIAL_TOTAL_EQUITY = 1000.0

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
    min_entry_gap_usd: float
    reprice_gap_usd: float
    first_batch_ratio: float = FIRST_BATCH_RATIO
    second_batch_ratio: float = SECOND_BATCH_RATIO
    dynamic_base_ratio: float = DYNAMIC_BASE_ENTRY_RATIO
    dynamic_min_ratio: float = DYNAMIC_MIN_ENTRY_RATIO
    dynamic_max_ratio: float = DYNAMIC_MAX_ENTRY_RATIO
    max_total_entry_ratio: float = MAX_TOTAL_ENTRY_RATIO


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
        self.trading_balance = TRADING_ACCOUNT_TARGET
        self.funding_balance = max(initial_total - TRADING_ACCOUNT_TARGET, 0.0)
        self.pos = Position()
        self.recent_prices: list[float] = []
        self.last_plan_price = 0.0
        self.last_batch_kline = None
        self.last_entry_check_kline = None
        self.capital_shortage_active = False
        self.entry_time = ""
        self.trades: list[Trade] = []
        self.equity_curve: list[float] = []
        self.signal_count = 0
        self.skipped_entry_cap = 0
        self.skipped_funds = 0
        self.blocked_width = 0
        self.blocked_gap = 0
        self.blocked_extreme = 0
        self.width_cancel = 0
        self.inside_cancel = 0
        self.reprice_count = 0
        self.same_k_block = 0
        self.profit_transferred = 0.0
        self.loss_topup = 0.0

    def run(self) -> "LogReplay":
        """Run the replay and return self."""
        for row in self.ticks.itertuples(index=False):
            self._remember_price(float(row.price))
            self._process_tick(row)
            self.equity_curve.append(self.total_equity() + self._unrealized(float(row.price)))
        return self

    def total_equity(self) -> float:
        """Return simulated total account equity."""
        return self.trading_balance + self.funding_balance

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

    def _width_ok(self, row) -> bool:
        _, _, _, width = self._bands(row)
        price = float(row.price)
        return width >= self.params.min_width_usd and width / price >= MIN_BOLL_WIDTH_PCT

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
        self.signal_count += 1
        if direction == "long" and self._still_making_new_low():
            self.blocked_extreme += 1
            return "none"
        if direction == "short" and self._still_making_new_high():
            self.blocked_extreme += 1
            return "none"
        return direction

    def _process_tick(self, row) -> None:
        if self.pos.pending is not None:
            self._try_fill_pending(row)
        if self.pos.is_active():
            self._try_take_profit(row)
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
        if self.last_plan_price > 0 and abs(price - self.last_plan_price) < self.params.min_entry_gap_usd:
            self.blocked_gap += 1
            return
        if self.last_batch_kline is not None and row.kline_ts == self.last_batch_kline:
            self.same_k_block += 1
            return
        order = self._order_at(0, price)
        if order is None:
            return
        self.pos.direction = direction
        self.pos.pending = order
        self.last_plan_price = price
        self.last_batch_kline = row.kline_ts
        self.last_entry_check_kline = row.kline_ts
        self.entry_time = str(row.ts)
        self._try_fill_pending(row)

    def _maintain_plan(self, row) -> None:
        if self.pos.pending is not None:
            if not self._width_ok(row):
                self.width_cancel += 1
                self.pos.pending = None
                if not self.pos.is_active():
                    self.pos.reset()
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
        if not self._width_ok(row):
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
        if abs(price - last.price) < self.params.min_entry_gap_usd:
            self.blocked_gap += 1
            return
        order = self._order_at(self.pos.next_idx(), price)
        if order is None:
            return
        self.pos.pending = order
        self.last_batch_kline = row.kline_ts
        self.last_entry_check_kline = row.kline_ts
        self._try_fill_pending(row)

    def _maybe_reprice(self, row) -> None:
        if self.pos.pending is None:
            return
        if self.pos.direction == "long" and self._still_making_new_low():
            return
        if self.pos.direction == "short" and self._still_making_new_high():
            return
        replacement = self._order_at(self.pos.pending.idx, float(row.price))
        if replacement is None:
            return
        last = self.pos.last_filled()
        if last is not None and abs(replacement.price - last.price) < self.params.min_entry_gap_usd:
            return
        if abs(replacement.price - self.pos.pending.price) < self.params.reprice_gap_usd:
            return
        self.pos.pending = replacement
        self.reprice_count += 1

    def _sizing_equity(self) -> float:
        return TRADING_ACCOUNT_TARGET if TRADING_ACCOUNT_TARGET > 0 else max(self.trading_balance, 0.0)

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
            return self.params.first_batch_ratio
        if idx == 1:
            return self.params.second_batch_ratio
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
        self.pos.recalc()
        self.last_batch_kline = row.kline_ts

    def _try_take_profit(self, row) -> None:
        price = float(row.price)
        if self.pos.direction == "long" and price < self.pos.tp_price:
            return
        if self.pos.direction == "short" and price > self.pos.tp_price:
            return
        self._close(self.pos.tp_price, str(row.ts), price)

    def _close(self, exit_price: float, ts: str, mark_price: float) -> None:
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
        self.entry_time = ""
        self.last_batch_kline = None
        self.last_entry_check_kline = None

    def _rebalance(self) -> None:
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

    def report(self) -> dict:
        """Return one summary row."""
        pnls = [trade.pnl for trade in self.trades]
        wins = [pnl for pnl in pnls if pnl > 0]
        peak = self.initial_total
        max_dd = 0.0
        for equity in self.equity_curve:
            peak = max(peak, equity)
            max_dd = max(max_dd, (peak - equity) / peak if peak else 0.0)
        durations = [
            (pd.Timestamp(trade.exit_time) - pd.Timestamp(trade.entry_time)).total_seconds() / 60
            for trade in self.trades
        ]
        return {
            "boll_std": self.params.boll_std,
            "min_width_usd": self.params.min_width_usd,
            "min_entry_gap_usd": self.params.min_entry_gap_usd,
            "reprice_gap_usd": self.params.reprice_gap_usd,
            "first_batch_ratio": self.params.first_batch_ratio,
            "second_batch_ratio": self.params.second_batch_ratio,
            "dynamic_base_ratio": self.params.dynamic_base_ratio,
            "dynamic_min_ratio": self.params.dynamic_min_ratio,
            "dynamic_max_ratio": self.params.dynamic_max_ratio,
            "max_total_entry_ratio": self.params.max_total_entry_ratio,
            "final_total_equity": round(self.total_equity(), 4),
            "total_pnl": round(self.total_equity() - self.initial_total, 4),
            "return_pct": round((self.total_equity() / self.initial_total - 1) * 100, 4),
            "max_drawdown_pct": round(max_dd * 100, 4),
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
            "blocked_width": self.blocked_width,
            "blocked_gap": self.blocked_gap,
            "blocked_extreme": self.blocked_extreme,
            "skipped_entry_cap": self.skipped_entry_cap,
            "skipped_funds": self.skipped_funds,
            "profit_transferred": round(self.profit_transferred, 4),
            "loss_topup": round(self.loss_topup, 4),
        }


def parse_logs(log_dir: Path) -> pd.DataFrame:
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
        raise RuntimeError(f"没有从 {log_dir} 解析到策略 tick 日志")
    df = pd.DataFrame(rows).sort_values("ts").drop_duplicates("ts")
    df["kline_ts"] = df["ts"].dt.floor("15min")
    return df.reset_index(drop=True)


def parse_float_list(text: str) -> list[float]:
    """Parse comma-separated floats."""
    return [float(item.strip()) for item in text.split(",") if item.strip()]


def build_grid(args) -> list[Params]:
    """Build parameter combinations from CLI arguments."""
    return [
        Params(*values)
        for values in itertools.product(
            parse_float_list(args.boll_std),
            parse_float_list(args.min_width_usd),
            parse_float_list(args.min_entry_gap_usd),
            parse_float_list(args.reprice_gap_usd),
            parse_float_list(args.first_batch_ratio),
            parse_float_list(args.second_batch_ratio),
            parse_float_list(args.dynamic_base_ratio),
            parse_float_list(args.dynamic_min_ratio),
            parse_float_list(args.dynamic_max_ratio),
            parse_float_list(args.max_total_entry_ratio),
        )
    ]


FOCUSED_GROUPS = [
    ("Width", ("min_width_usd",)),
    ("Entry Gap", ("min_entry_gap_usd",)),
    ("First/Second", ("first_batch_ratio", "second_batch_ratio")),
    ("Dyn Base/Min/Max", ("dynamic_base_ratio", "dynamic_min_ratio", "dynamic_max_ratio")),
    ("Max Total", ("max_total_entry_ratio",)),
]


def _format_group_value(row: dict, fields: tuple[str, ...]) -> str:
    """Format grouped parameter values for reports."""
    return "/".join(f"{float(row[field]):g}" for field in fields)


def summarize_parameter_group(rows: list[dict], fields: tuple[str, ...]) -> list[dict]:
    """Summarize ranked results by one parameter group."""
    grouped: dict[str, dict] = {}
    for rank, row in enumerate(rows, start=1):
        label = _format_group_value(row, fields)
        group = grouped.setdefault(label, {"rows": [], "best_rank": rank})
        group["rows"].append(row)
        group["best_rank"] = min(group["best_rank"], rank)

    summaries = []
    for label, group in grouped.items():
        group_rows = group["rows"]
        best = max(
            group_rows,
            key=lambda item: (item["total_pnl"], item["trades"], -item["max_drawdown_pct"]),
        )
        summaries.append(
            {
                "value": label,
                "runs": len(group_rows),
                "best_rank": group["best_rank"],
                "best_pnl": round(best["total_pnl"], 4),
                "avg_pnl": round(float(np.mean([row["total_pnl"] for row in group_rows])), 4),
                "avg_trades": round(float(np.mean([row["trades"] for row in group_rows])), 2),
                "avg_drawdown": round(float(np.mean([row["max_drawdown_pct"] for row in group_rows])), 4),
                "cap_skip": int(sum(row.get("skipped_entry_cap", 0) for row in group_rows)),
                "fund_skip": int(sum(row.get("skipped_funds", 0) for row in group_rows)),
            }
        )
    return sorted(
        summaries,
        key=lambda item: (item["best_pnl"], item["avg_pnl"], -item["avg_drawdown"]),
        reverse=True,
    )


def write_markdown_report(path: Path, rows: list[dict], ticks: pd.DataFrame) -> None:
    """Write a concise Markdown report."""
    best = rows[0]
    lines = [
        "# Log Parameter Report",
        "",
        f"- Data range: {ticks['ts'].min()} -> {ticks['ts'].max()}",
        f"- Ticks: {len(ticks)}",
        "- Mode: manual offline replay",
        f"- Trading account target: {TRADING_ACCOUNT_TARGET:.2f} USDT",
        "- Sizing model: dynamic live strategy, with max total entry ratio cap",
        "",
        "## Best Parameters",
        "",
        (
            f"`BOLL_STD={best['boll_std']}`, `MIN_BOLL_WIDTH_USD={best['min_width_usd']}`, "
            f"`MIN_ENTRY_GAP_USD={best['min_entry_gap_usd']}`, `REPRICE_GAP_USD={best['reprice_gap_usd']}`, "
            f"`FIRST_BATCH_RATIO={best['first_batch_ratio']}`, `SECOND_BATCH_RATIO={best['second_batch_ratio']}`, "
            f"`DYNAMIC_BASE_ENTRY_RATIO={best['dynamic_base_ratio']}`, "
            f"`DYNAMIC_MIN_ENTRY_RATIO={best['dynamic_min_ratio']}`, "
            f"`DYNAMIC_MAX_ENTRY_RATIO={best['dynamic_max_ratio']}`, "
            f"`MAX_TOTAL_ENTRY_RATIO={best['max_total_entry_ratio']}`"
        ),
        "",
        (
            f"PnL `{best['total_pnl']}` USDT, trades `{best['trades']}`, "
            f"win rate `{best['win_rate_pct']}%`, max drawdown `{best['max_drawdown_pct']}%`, "
            f"avg hold `{best['avg_hold_minutes']}` minutes."
        ),
        "",
        "## Top 20",
        "",
        "|#|BOLL_STD|Width|Entry Gap|Reprice|First/Second|Dyn Base/Min/Max|Max Total|PnL|Trades|Win|Drawdown|Cap/Funds Skip|",
        "|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|",
    ]
    for idx, row in enumerate(rows[:20], start=1):
        lines.append(
            f"|{idx}|{row['boll_std']}|{row['min_width_usd']}|{row['min_entry_gap_usd']}|"
            f"{row['reprice_gap_usd']}|{row['first_batch_ratio']}/{row['second_batch_ratio']}|"
            f"{row['dynamic_base_ratio']}/{row['dynamic_min_ratio']}/{row['dynamic_max_ratio']}|"
            f"{row['max_total_entry_ratio']}|{row['total_pnl']}|{row['trades']}|"
            f"{row['win_rate_pct']}|{row['max_drawdown_pct']}|"
            f"{row['skipped_entry_cap']}/{row['skipped_funds']}|"
        )
    lines.extend(["", "## Focused Parameter Impact", ""])
    for title, fields in FOCUSED_GROUPS:
        lines.extend(
            [
                f"### {title}",
                "",
                "|Value|Runs|Best Rank|Best PnL|Avg PnL|Avg Trades|Avg Drawdown|Cap/Funds Skip|",
                "|-:|-:|-:|-:|-:|-:|-:|-:|",
            ]
        )
        for summary in summarize_parameter_group(rows, fields):
            lines.append(
                f"|{summary['value']}|{summary['runs']}|{summary['best_rank']}|"
                f"{summary['best_pnl']}|{summary['avg_pnl']}|{summary['avg_trades']}|"
                f"{summary['avg_drawdown']}|{summary['cap_skip']}/{summary['fund_skip']}|"
            )
        lines.append("")
    lines.extend(
        [
            "## Notes",
            "",
            "- This replay uses logged mark price and Bollinger snapshots, not order-book level fills.",
            "- It follows the current dynamic entry sizing: first/second fixed ratios, later gap-based ratios, and no shrinking when the 80% cap would be exceeded.",
            "- Config is changed only after manual confirmation in the selection prompt.",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def _format_config_value(value: float) -> str:
    """Format a numeric config value with stable Python syntax."""
    if float(value).is_integer():
        return str(int(value))
    return str(float(value))


def apply_params_to_config(row: dict, config_path: Path = CONFIG_PATH) -> Path:
    """Write selected optimizer parameters into ``src/config.py``."""
    replacements = {
        "BOLL_STD": row["boll_std"],
        "MIN_BOLL_WIDTH_USD": row["min_width_usd"],
        "MIN_ENTRY_GAP_USD": row["min_entry_gap_usd"],
        "REPRICE_GAP_USD": row["reprice_gap_usd"],
        "FIRST_BATCH_RATIO": row["first_batch_ratio"],
        "SECOND_BATCH_RATIO": row["second_batch_ratio"],
        "DYNAMIC_BASE_ENTRY_RATIO": row["dynamic_base_ratio"],
        "DYNAMIC_MIN_ENTRY_RATIO": row["dynamic_min_ratio"],
        "DYNAMIC_MAX_ENTRY_RATIO": row["dynamic_max_ratio"],
        "MAX_TOTAL_ENTRY_RATIO": row["max_total_entry_ratio"],
    }
    text = config_path.read_text(encoding="utf-8")
    backup_path = config_path.with_suffix(".py.bak")
    backup_path.write_text(text, encoding="utf-8")

    for key, value in replacements.items():
        pattern = re.compile(rf"^({key}\s*=\s*)([-+]?\d+(?:\.\d+)?)(.*)$", re.MULTILINE)
        text, count = pattern.subn(rf"\g<1>{_format_config_value(value)}\3", text, count=1)
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
        f"BOLL_STD={selected['boll_std']}, "
        f"MIN_BOLL_WIDTH_USD={selected['min_width_usd']}, "
        f"MIN_ENTRY_GAP_USD={selected['min_entry_gap_usd']}, "
        f"REPRICE_GAP_USD={selected['reprice_gap_usd']}, "
        f"FIRST_BATCH_RATIO={selected['first_batch_ratio']}, "
        f"SECOND_BATCH_RATIO={selected['second_batch_ratio']}, "
        f"DYNAMIC_BASE_ENTRY_RATIO={selected['dynamic_base_ratio']}, "
        f"DYNAMIC_MIN_ENTRY_RATIO={selected['dynamic_min_ratio']}, "
        f"DYNAMIC_MAX_ENTRY_RATIO={selected['dynamic_max_ratio']}, "
        f"MAX_TOTAL_ENTRY_RATIO={selected['max_total_entry_ratio']}"
    )
    confirm = input("Type y to confirm: ").strip().lower()
    if confirm != "y":
        print("Canceled; src/config.py not changed.")
        return

    backup_path = apply_params_to_config(selected)
    print(f"Updated src/config.py; backup saved to {backup_path}")


def print_rankings(rows: list[dict], top_n: int = 10) -> None:
    """Print a compact, readable optimizer ranking."""
    best = rows[0]
    print()
    print("=" * 108)
    print("Best Parameters")
    print("=" * 108)
    print(
        f"BOLL_STD={best['boll_std']}  "
        f"MIN_BOLL_WIDTH_USD={best['min_width_usd']}  "
        f"MIN_ENTRY_GAP_USD={best['min_entry_gap_usd']}  "
        f"REPRICE_GAP_USD={best['reprice_gap_usd']}"
    )
    print(
        f"FIRST/SECOND={best['first_batch_ratio']}/{best['second_batch_ratio']}  "
        f"DYNAMIC={best['dynamic_base_ratio']}/{best['dynamic_min_ratio']}/{best['dynamic_max_ratio']}  "
        f"MAX_TOTAL={best['max_total_entry_ratio']}"
    )
    print(
        f"PnL={best['total_pnl']:+.2f} USDT  "
        f"trades={best['trades']}  "
        f"win={best['win_rate_pct']:.1f}%  "
        f"drawdown={best['max_drawdown_pct']:.2f}%  "
        f"avg_hold={best['avg_hold_minutes']:.0f}m"
    )
    print()
    print("Top parameter comparison")
    print("-" * 108)
    print("Rank  BOLL/W/G/R        First/Second  DynBase/Min/Max  Cap   PnL USDT  Trades  Win    DD     SkipCap/Funds")
    print("-" * 108)
    for idx, row in enumerate(rows[:top_n], start=1):
        params = f"{row['boll_std']:g}/{row['min_width_usd']:g}/{row['min_entry_gap_usd']:g}/{row['reprice_gap_usd']:g}"
        dyn = f"{row['dynamic_base_ratio']:g}/{row['dynamic_min_ratio']:g}/{row['dynamic_max_ratio']:g}"
        print(
            f"{idx:>2}    {params:<17}"
            f"{row['first_batch_ratio']:>5.2f}/{row['second_batch_ratio']:<5.2f}  "
            f"{dyn:<16}"
            f"{row['max_total_entry_ratio']:<5.2f}"
            f"{row['total_pnl']:>9.2f}  "
            f"{row['trades']:>3}  "
            f"{row['win_rate_pct']:>5.1f}%  "
            f"{row['max_drawdown_pct']:>5.2f}%  "
            f"{row['skipped_entry_cap']:>5}/{row['skipped_funds']:<5}"
        )
    print("-" * 108)
    print()
    print("Focused parameter impact")
    print("-" * 108)
    for title, fields in FOCUSED_GROUPS:
        print(f"{title}:")
        for summary in summarize_parameter_group(rows, fields)[:8]:
            print(
                f"  {summary['value']:<14} "
                f"best_rank={summary['best_rank']:<3} "
                f"best_pnl={summary['best_pnl']:>8.2f} "
                f"avg_pnl={summary['avg_pnl']:>8.2f} "
                f"avg_trades={summary['avg_trades']:>5.2f} "
                f"avg_dd={summary['avg_drawdown']:>5.2f}% "
                f"skip={summary['cap_skip']}/{summary['fund_skip']}"
            )
    print("-" * 108)
    print("Full fields and grouped tables are saved to CSV/Markdown.")


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Optimize live-entry parameters from strategy logs.")
    parser.add_argument("--log-dir", default=str(DEFAULT_LOG_DIR))
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--initial-total-equity", type=float, default=INITIAL_TOTAL_EQUITY)
    parser.add_argument("--boll-std", default="1.8,2.0,2.2")
    parser.add_argument("--min-width-usd", default="10,12,15,18,20,25")
    parser.add_argument("--min-entry-gap-usd", default="3,4,5,6,8")
    parser.add_argument("--reprice-gap-usd", default="0.5,1,2")
    parser.add_argument("--first-batch-ratio", default=str(FIRST_BATCH_RATIO))
    parser.add_argument("--second-batch-ratio", default=str(SECOND_BATCH_RATIO))
    parser.add_argument("--dynamic-base-ratio", default=str(DYNAMIC_BASE_ENTRY_RATIO))
    parser.add_argument("--dynamic-min-ratio", default=str(DYNAMIC_MIN_ENTRY_RATIO))
    parser.add_argument("--dynamic-max-ratio", default=str(DYNAMIC_MAX_ENTRY_RATIO))
    parser.add_argument("--max-total-entry-ratio", default=str(MAX_TOTAL_ENTRY_RATIO))
    parser.add_argument("--no-prompt", action="store_true", help="Generate report only; do not show config sync prompt.")
    parser.add_argument("--quiet", action="store_true", help="Hide progress output and print only final results.")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not args.quiet:
        print("Reading strategy logs...", flush=True)
    ticks = parse_logs(Path(args.log_dir))
    if not args.quiet:
        print(
            f"Loaded {len(ticks)} ticks, range {ticks['ts'].min()} -> {ticks['ts'].max()}",
            flush=True,
        )

    grid = build_grid(args)
    total = len(grid)
    progress_step = max(1, total // 20)
    if not args.quiet:
        print(f"Starting replay for {total} parameter sets...", flush=True)
    rows = []
    for idx, params in enumerate(grid, start=1):
        rows.append(LogReplay(ticks, params, args.initial_total_equity).run().report())
        if not args.quiet and (idx == 1 or idx == total or idx % progress_step == 0):
            pct = idx / total * 100
            print(f"Progress {idx}/{total} ({pct:.1f}%)", flush=True)

    if not args.quiet:
        print("Sorting and writing report...", flush=True)
    rows.sort(key=lambda row: (row["total_pnl"], row["trades"], -row["max_drawdown_pct"]), reverse=True)

    stamp = ticks["ts"].max().strftime("%Y%m%d_%H%M%S")
    csv_path = out_dir / f"log_param_report_{stamp}.csv"
    md_path = out_dir / f"log_param_report_{stamp}.md"
    out_dir.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    write_markdown_report(md_path, rows, ticks)

    print_rankings(rows)
    print(f"csv={csv_path}")
    print(f"report={md_path}")
    if not args.no_prompt:
        prompt_apply_params(rows)


if __name__ == "__main__":
    main()
