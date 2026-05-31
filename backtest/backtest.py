"""
BollPinStrategy 历史回测
基于 ETH_USDT_15m_history.csv，完整模拟分批建仓、动态止盈、强平逻辑

使用：
    python backtest.py                    # 默认1000 USDT起始资金
    python backtest.py --equity 5000      # 自定义起始资金
    python backtest.py --csv ../ETH_USDT_15m_history.csv
"""

import argparse
import csv
import os
import sys
from dataclasses import dataclass, field
from typing import Optional
import math

import numpy as np
import pandas as pd

# ─── 策略参数（与 src/config.py 保持一致）──────────────────────────────────────
BOLL_PERIOD        = 20
BOLL_STD           = 2.0
BOLL_INCLUDE_CURRENT = True   # 含当前未确认K线参与布林计算
CT_VAL             = 0.1      # 每张0.1 ETH
LEVER              = 50
BATCH_COUNT        = 4
BATCH_SIZE_RATIO   = [0.2, 0.25, 0.25, 0.25]
BATCH_SPACING      = [0.0, 0.3, 0.6, 1.0]   # 各批与首批的间距（以boll_width为单位）
TP_PROFIT_USD      = 10.0     # 止盈：均价 ± 10 USDT
MIN_BOLL_WIDTH_USD = 20.0
MIN_BOLL_WIDTH_PCT = 0.004
MIN_BREAK_USD      = 1.5      # 价格需突破布林带至少1.5 USDT
MIN_ENTRY_GAP_USD  = 5.0      # 两次开仓参考价至少相差5 USDT
CONTRACT_STEP      = 0.01
MIN_ORDER_CONTRACTS = 0.01
# ────────────────────────────────────────────────────────────────────────────────


# ─── 工具函数 ────────────────────────────────────────────────────────────────────
def floor_to_step(v: float, step: float = CONTRACT_STEP) -> float:
    return math.floor(v / step) * step


def liq_price_est(direction, avg_entry, total_sz, total_margin):
    if total_sz == 0:
        return 0.0
    margin_per_eth = (total_margin * 0.9) / (total_sz * CT_VAL)
    return avg_entry - margin_per_eth if direction == "long" else avg_entry + margin_per_eth


# ─── 数据结构 ────────────────────────────────────────────────────────────────────
@dataclass
class FilledBatch:
    batch_idx: int
    price: float
    sz: float


@dataclass
class Position:
    direction: str = "none"
    batches: list = field(default_factory=list)          # List[FilledBatch] (filled)
    pending: list = field(default_factory=list)          # List[tuple(batch_idx, price, sz)]
    tp_price: float = 0.0
    liq_price: float = 0.0
    avg_entry: float = 0.0
    total_sz: float = 0.0
    total_margin: float = 0.0
    plan_first_price: float = 0.0
    plan_boll_width: float = 0.0
    last_plan_close: float = 0.0   # 上次开仓计划时的close（用于MIN_ENTRY_GAP）

    def is_active(self):
        return self.direction != "none" and self.total_sz > 0

    def has_pending(self):
        return len(self.pending) > 0

    def reset(self):
        self.__init__()

    def recalc(self):
        """重新计算均价、止盈、强平"""
        if not self.batches:
            return
        self.total_sz     = sum(b.sz for b in self.batches)
        weighted          = sum(b.price * b.sz for b in self.batches)
        self.avg_entry    = weighted / self.total_sz
        self.total_margin = (self.total_sz * CT_VAL * self.avg_entry) / LEVER
        if self.direction == "long":
            self.tp_price = round(self.avg_entry + TP_PROFIT_USD, 2)
        else:
            self.tp_price = round(self.avg_entry - TP_PROFIT_USD, 2)
        self.liq_price = round(liq_price_est(
            self.direction, self.avg_entry, self.total_sz, self.total_margin), 2)


@dataclass
class TradeRecord:
    entry_time: str
    exit_time: str
    direction: str
    avg_entry: float
    exit_price: float
    total_sz: float
    pnl_usdt: float       # 含手续费估算
    exit_reason: str      # "TP" | "LIQ"
    batches_filled: int


# ─── 回测引擎 ────────────────────────────────────────────────────────────────────
class Backtest:
    TAKER_FEE = 0.0005    # OKX 合约 taker 0.05%

    def __init__(self, df: pd.DataFrame, initial_equity: float = 1000.0):
        self.df             = df.reset_index(drop=True)
        self.equity         = initial_equity
        self.initial_equity = initial_equity
        self.pos            = Position()
        self.trades: list[TradeRecord] = []
        self.equity_curve: list[float] = []
        self.last_plan_close = 0.0   # 上次开仓时的收盘价

    # ── 布林带（已在df中预计算）──────────────────────────────────────────────
    @staticmethod
    def _precompute_boll(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["boll_mid"]   = df["close"].rolling(BOLL_PERIOD).mean()
        std              = df["close"].rolling(BOLL_PERIOD).std(ddof=0)
        df["boll_upper"] = df["boll_mid"] + BOLL_STD * std
        df["boll_lower"] = df["boll_mid"] - BOLL_STD * std
        df["boll_width"] = df["boll_upper"] - df["boll_lower"]
        return df

    # ── 开仓批次计划 ─────────────────────────────────────────────────────────
    def _build_plan(self, direction, first_price, boll_width):
        """返回 [(batch_idx, price, sz), ...]"""
        orders = []
        for i in range(BATCH_COUNT):
            spacing = boll_width * BATCH_SPACING[i]
            price = round(first_price - spacing if direction == "long"
                          else first_price + spacing, 2)
            if price <= 0:
                break
            margin_budget = self.equity * BATCH_SIZE_RATIO[i]
            notional_budget = margin_budget * LEVER
            raw_sz = notional_budget / (price * CT_VAL)
            sz = floor_to_step(raw_sz)
            if sz < MIN_ORDER_CONTRACTS:
                continue
            orders.append((i, price, sz))
        return orders

    # ── 信号判断 ─────────────────────────────────────────────────────────────
    @staticmethod
    def _signal(row) -> str:
        """盘中突破信号（以K线收盘价模拟实时price）"""
        if pd.isna(row["boll_lower"]):
            return "none"
        width = row["boll_width"]
        close = row["close"]
        if width < MIN_BOLL_WIDTH_USD or width / close < MIN_BOLL_WIDTH_PCT:
            return "none"
        if close < row["boll_lower"] - MIN_BREAK_USD:
            return "long"
        if close > row["boll_upper"] + MIN_BREAK_USD:
            return "short"
        return "none"

    # ── 单根K线处理 ──────────────────────────────────────────────────────────
    def _process_candle(self, i: int):
        row = self.df.iloc[i]
        o, h, l, c = row["open"], row["high"], row["low"], row["close"]
        ts = row["datetime_utc"]

        # 1. 持仓中：先检查挂单成交 + 止盈 + 强平
        if self.pos.direction != "none":
            self._try_fill_pending(row, i)

            if self.pos.is_active():
                # 强平优先于止盈（更极端时触发）
                liq_hit = (self.pos.direction == "long" and l <= self.pos.liq_price) or \
                          (self.pos.direction == "short" and h >= self.pos.liq_price)
                tp_hit  = (self.pos.direction == "long" and h >= self.pos.tp_price) or \
                          (self.pos.direction == "short" and l <= self.pos.tp_price)

                if liq_hit:
                    self._close_position(self.pos.liq_price, ts, "LIQ")
                elif tp_hit:
                    self._close_position(self.pos.tp_price, ts, "TP")

        # 2. 无持仓：检测信号
        if self.pos.direction == "none":
            direction = self._signal(row)
            if direction == "none":
                return

            # MIN_ENTRY_GAP 过滤
            if self.last_plan_close > 0 and abs(c - self.last_plan_close) < MIN_ENTRY_GAP_USD:
                return

            boll_width = row["boll_width"]
            plan = self._build_plan(direction, c, boll_width)
            if not plan:
                return

            self.pos.direction        = direction
            self.pos.plan_first_price = c
            self.pos.plan_boll_width  = boll_width
            self.pos.pending          = list(plan)  # [(idx, price, sz)]
            self.last_plan_close      = c
            self._entry_time          = ts

            # 首批当根K线即可成交（信号产生时已在带外）
            self._try_fill_pending(row, i)

        self.equity_curve.append(self.equity + self._unrealized_pnl(c))

    def _try_fill_pending(self, row, candle_idx: int):
        """检查未成交批次是否在本根K线成交"""
        if not self.pos.pending:
            return

        h, l, c = row["high"], row["low"], row["close"]
        remaining = []
        for (batch_idx, price, sz) in self.pos.pending:
            filled = (self.pos.direction == "long"  and l <= price) or \
                     (self.pos.direction == "short" and h >= price)
            if filled:
                # 手续费（开仓taker）
                fee = sz * CT_VAL * price * self.TAKER_FEE
                self.equity -= fee
                self.pos.batches.append(FilledBatch(batch_idx, price, sz))
                self.pos.recalc()
            else:
                remaining.append((batch_idx, price, sz))

        # 已无任何成交且价格回到带内，清除计划
        if not self.pos.batches and not remaining:
            self.pos.reset()
            return

        self.pos.pending = remaining

        # 如果价格已不在带外，撤销未成交批次
        if self.pos.batches:
            boll_lower = row.get("boll_lower", float("nan"))
            boll_upper = row.get("boll_upper", float("nan"))
            if not pd.isna(boll_lower):
                still_outside = (self.pos.direction == "long"  and c < boll_lower) or \
                                (self.pos.direction == "short" and c > boll_upper)
                if not still_outside:
                    self.pos.pending = []  # 撤销未成交补仓单

    def _close_position(self, exit_price: float, ts: str, reason: str):
        if not self.pos.batches:
            self.pos.reset()
            return

        avg  = self.pos.avg_entry
        sz   = self.pos.total_sz
        if self.pos.direction == "long":
            pnl_raw = (exit_price - avg) * sz * CT_VAL
        else:
            pnl_raw = (avg - exit_price) * sz * CT_VAL

        # 手续费（平仓taker）
        fee = sz * CT_VAL * exit_price * self.TAKER_FEE
        pnl = pnl_raw - fee
        self.equity += pnl

        self.trades.append(TradeRecord(
            entry_time    = self._entry_time,
            exit_time     = ts,
            direction     = self.pos.direction,
            avg_entry     = round(avg, 4),
            exit_price    = round(exit_price, 4),
            total_sz      = sz,
            pnl_usdt      = round(pnl, 4),
            exit_reason   = reason,
            batches_filled= len(self.pos.batches),
        ))
        self.pos.reset()
        self.equity_curve.append(self.equity)

    def _unrealized_pnl(self, mark_price: float) -> float:
        if not self.pos.is_active():
            return 0.0
        avg = self.pos.avg_entry
        sz  = self.pos.total_sz
        if self.pos.direction == "long":
            return (mark_price - avg) * sz * CT_VAL
        else:
            return (avg - mark_price) * sz * CT_VAL

    # ── 运行 ─────────────────────────────────────────────────────────────────
    def run(self):
        self.df = self._precompute_boll(self.df)
        n = len(self.df)
        for i in range(BOLL_PERIOD, n):
            self._process_candle(i)
            if i % 5000 == 0:
                print(f"  进度: {i}/{n}  交易数: {len(self.trades)}  "
                      f"资金: {self.equity:.2f}", end="\r")
        print()
        return self

    # ── 统计报告 ──────────────────────────────────────────────────────────────
    def report(self) -> dict:
        trades = self.trades
        if not trades:
            return {"error": "无任何交易"}

        pnls = [t.pnl_usdt for t in trades]
        wins = [p for p in pnls if p > 0]
        lses = [p for p in pnls if p <= 0]

        total_pnl   = sum(pnls)
        win_rate    = len(wins) / len(trades) * 100
        avg_win     = sum(wins) / len(wins) if wins else 0
        avg_loss    = sum(lses) / len(lses) if lses else 0
        profit_factor = abs(sum(wins) / sum(lses)) if lses else float("inf")

        # 最大回撤（基于equity_curve）
        curve = self.equity_curve
        peak = self.initial_equity
        max_dd = 0.0
        for eq in curve:
            peak = max(peak, eq)
            dd = (peak - eq) / peak
            max_dd = max(max_dd, dd)

        # 按方向统计
        long_trades  = [t for t in trades if t.direction == "long"]
        short_trades = [t for t in trades if t.direction == "short"]
        tp_trades    = [t for t in trades if t.exit_reason == "TP"]
        liq_trades   = [t for t in trades if t.exit_reason == "LIQ"]

        # 夏普比率（简化，按交易PnL）
        if len(pnls) > 1:
            mu    = np.mean(pnls)
            sigma = np.std(pnls, ddof=1)
            sharpe = (mu / sigma * np.sqrt(len(pnls))) if sigma > 0 else 0
        else:
            sharpe = 0

        return {
            "初始资金":         f"{self.initial_equity:.2f} USDT",
            "最终资金":         f"{self.equity:.2f} USDT",
            "总收益":           f"{total_pnl:+.2f} USDT ({total_pnl/self.initial_equity*100:+.2f}%)",
            "最大回撤":         f"{max_dd*100:.2f}%",
            "总交易次数":       len(trades),
            "  多头交易":       len(long_trades),
            "  空头交易":       len(short_trades),
            "  止盈平仓":       f"{len(tp_trades)} 次",
            "  强平/止损平仓":  f"{len(liq_trades)} 次",
            "胜率":             f"{win_rate:.1f}%",
            "平均盈利":         f"+{avg_win:.2f} USDT",
            "平均亏损":         f"{avg_loss:.2f} USDT",
            "盈亏比":           f"{profit_factor:.2f}",
            "夏普比率(简化)":   f"{sharpe:.2f}",
        }

    # ── 输出结果到文件 ────────────────────────────────────────────────────────
    def save(self, out_dir: str):
        os.makedirs(out_dir, exist_ok=True)

        # 逐笔交易记录
        trades_path = os.path.join(out_dir, "trades.csv")
        with open(trades_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["entry_time", "exit_time", "direction",
                             "avg_entry", "exit_price", "total_sz",
                             "pnl_usdt", "exit_reason", "batches_filled"])
            for t in self.trades:
                writer.writerow([t.entry_time, t.exit_time, t.direction,
                                 t.avg_entry, t.exit_price, t.total_sz,
                                 t.pnl_usdt, t.exit_reason, t.batches_filled])

        # 权益曲线
        curve_path = os.path.join(out_dir, "equity_curve.csv")
        with open(curve_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["step", "equity"])
            for i, eq in enumerate(self.equity_curve):
                writer.writerow([i, round(eq, 4)])

        # 汇总报告
        report = self.report()
        summary_path = os.path.join(out_dir, "summary.txt")
        with open(summary_path, "w", encoding="utf-8") as f:
            f.write("=" * 50 + "\n")
            f.write("BollPinStrategy 回测报告\n")
            f.write("=" * 50 + "\n\n")
            for k, v in report.items():
                f.write(f"  {k:<20} {v}\n")
            f.write("\n")

        print(f"\n回测结果已保存：")
        print(f"  {trades_path}")
        print(f"  {curve_path}")
        print(f"  {summary_path}")
        return trades_path, curve_path, summary_path


# ─── 入口 ────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv",    default=os.path.join(os.path.dirname(__file__), "../ETH_USDT_15m_history.csv"))
    parser.add_argument("--equity", type=float, default=1000.0, help="初始资金 USDT")
    parser.add_argument("--out",    default=os.path.join(os.path.dirname(__file__), "results"))
    args = parser.parse_args()

    csv_path = os.path.abspath(args.csv)
    if not os.path.exists(csv_path):
        print(f"❌ 找不到数据文件: {csv_path}")
        print("   请先运行: python okx_data_provider.py --mode history")
        sys.exit(1)

    print(f"加载数据: {csv_path}")
    df = pd.read_csv(csv_path)
    print(f"数据范围: {df['datetime_utc'].iloc[0]} ~ {df['datetime_utc'].iloc[-1]}  ({len(df)} 根K线)")
    print(f"初始资金: {args.equity:.2f} USDT\n")

    bt = Backtest(df, initial_equity=args.equity)
    print("运行回测...")
    bt.run()

    # 打印报告
    print("\n" + "=" * 50)
    print("回测报告")
    print("=" * 50)
    for k, v in bt.report().items():
        print(f"  {k:<20} {v}")

    bt.save(args.out)


if __name__ == "__main__":
    main()
