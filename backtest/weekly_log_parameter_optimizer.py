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
    BATCH_COUNT,
    BATCH_SIZE_RATIO,
    CONTRACT_STEP,
    CT_VAL,
    LEVER,
    MIN_BOLL_WIDTH_PCT,
    MIN_ORDER_CONTRACTS,
    NO_NEW_EXTREME_TICKS,
    TP_PROFIT_USD,
    TRADING_ACCOUNT_TARGET,
)


DEFAULT_LOG_DIR = ROOT / "logs"
DEFAULT_OUT_DIR = ROOT / "backtest" / "results" / "weekly_log_optimizer"
SOURCE_BOLL_STD = 2.0
TAKER_FEE = 0.0005
INITIAL_TOTAL_EQUITY = 1000.0

TICK_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+).*?"
    r"价格=(?P<price>\d+(?:\.\d+)?)\s+"
    r"布林\[(?P<lower>\d+(?:\.\d+)?)\s+\|\s+"
    r"(?P<mid>\d+(?:\.\d+)?)\s+\|\s+"
    r"(?P<upper>\d+(?:\.\d+)?)\]",
)


@dataclass(frozen=True)
class Params:
    """One entry-parameter combination."""

    boll_std: float
    min_width_usd: float
    min_entry_gap_usd: float
    reprice_gap_usd: float


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
        self.fixed_batch_sizes: list[float] = []
        self.recent_prices: list[float] = []
        self.last_plan_price = 0.0
        self.last_batch_kline = None
        self.last_entry_check_kline = None
        self.entry_time = ""
        self.trades: list[Trade] = []
        self.equity_curve: list[float] = []
        self.signal_count = 0
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
        first_price = float(self.ticks.iloc[0].price)
        self._init_fixed_batch_sizes(first_price)
        for row in self.ticks.itertuples(index=False):
            self._remember_price(float(row.price))
            self._process_tick(row)
            self.equity_curve.append(self.total_equity() + self._unrealized(float(row.price)))
        return self

    def total_equity(self) -> float:
        """Return simulated total account equity."""
        return self.trading_balance + self.funding_balance

    def _init_fixed_batch_sizes(self, mark_price: float) -> None:
        self.fixed_batch_sizes = []
        equity = max(self.trading_balance, 0.0)
        for idx in range(BATCH_COUNT):
            margin_budget = equity * BATCH_SIZE_RATIO[idx]
            raw_sz = margin_budget * LEVER / (mark_price * CT_VAL)
            sz = math.floor(raw_sz / CONTRACT_STEP) * CONTRACT_STEP
            self.fixed_batch_sizes.append(round(sz, 8))

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
        if self.pos.has_plan():
            self._maintain_plan(row)
        else:
            self._try_open(row)

    def _try_open(self, row) -> None:
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

    def _order_at(self, idx: int, price: float) -> Batch | None:
        if idx >= BATCH_COUNT:
            return None
        sz = self.fixed_batch_sizes[idx] if idx < len(self.fixed_batch_sizes) else 0.0
        if sz < MIN_ORDER_CONTRACTS:
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
        self._init_fixed_batch_sizes(mark_price)

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
        )
    ]


def write_markdown_report(path: Path, rows: list[dict], ticks: pd.DataFrame) -> None:
    """Write a concise Markdown report."""
    best = rows[0]
    lines = [
        "# Weekly Log Parameter Report",
        "",
        f"- 数据范围: {ticks['ts'].min()} -> {ticks['ts'].max()}",
        f"- Tick 数: {len(ticks)}",
        "- 运行方式: 手动运行",
        f"- 当前实盘资金目标: {TRADING_ACCOUNT_TARGET:.2f} USDT",
        "",
        "## 最优组合",
        "",
        (
            f"`BOLL_STD={best['boll_std']}`, `MIN_BOLL_WIDTH_USD={best['min_width_usd']}`, "
            f"`MIN_ENTRY_GAP_USD={best['min_entry_gap_usd']}`, `REPRICE_GAP_USD={best['reprice_gap_usd']}`"
        ),
        "",
        (
            f"总收益 `{best['total_pnl']}` USDT，交易 `{best['trades']}` 次，"
            f"胜率 `{best['win_rate_pct']}%`，最大回撤 `{best['max_drawdown_pct']}%`，"
            f"平均持仓 `{best['avg_hold_minutes']}` 分钟。"
        ),
        "",
        "## Top 20",
        "",
        "|#|BOLL_STD|宽度|入场距离|重挂阈值|收益|交易|胜率|回撤|均持仓分钟|宽度撤单|重挂|",
        "|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|-:|",
    ]
    for idx, row in enumerate(rows[:20], start=1):
        lines.append(
            f"|{idx}|{row['boll_std']}|{row['min_width_usd']}|{row['min_entry_gap_usd']}|"
            f"{row['reprice_gap_usd']}|{row['total_pnl']}|{row['trades']}|"
            f"{row['win_rate_pct']}|{row['max_drawdown_pct']}|{row['avg_hold_minutes']}|"
            f"{row['width_cancel']}|{row['reprice_count']}|"
        )
    lines.extend(
        [
            "",
            "## 注意",
            "",
            "- 这个报告只使用日志里记录到的 mark price 和布林带快照，不能替代真实盘口撮合回测。",
            "- 脚本只输出建议和对比数据，不会修改 `src/config.py`。",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="用实盘日志每周优化开仓参数，并生成报告。")
    parser.add_argument("--log-dir", default=str(DEFAULT_LOG_DIR))
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--initial-total-equity", type=float, default=INITIAL_TOTAL_EQUITY)
    parser.add_argument("--boll-std", default="1.8,2.0,2.2")
    parser.add_argument("--min-width-usd", default="10,12,15,18,20,25")
    parser.add_argument("--min-entry-gap-usd", default="3,4,5,6,8")
    parser.add_argument("--reprice-gap-usd", default="0.5,1,2")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    ticks = parse_logs(Path(args.log_dir))

    rows = []
    for params in build_grid(args):
        rows.append(LogReplay(ticks, params, args.initial_total_equity).run().report())
    rows.sort(key=lambda row: (row["total_pnl"], row["trades"], -row["max_drawdown_pct"]), reverse=True)

    stamp = ticks["ts"].max().strftime("%Y%m%d_%H%M%S")
    csv_path = out_dir / f"log_param_report_{stamp}.csv"
    md_path = out_dir / f"log_param_report_{stamp}.md"
    with csv_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    write_markdown_report(md_path, rows, ticks)

    print(pd.DataFrame(rows[:20]).to_string(index=False))
    print(f"csv={csv_path}")
    print(f"report={md_path}")


if __name__ == "__main__":
    main()
