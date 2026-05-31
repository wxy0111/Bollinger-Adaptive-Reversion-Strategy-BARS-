"""
Evaluate first-entry opponent-price impact for BollPinStrategy.

This script has three modes:
  1. signals: build first-entry signal rows from 15m history.
  2. analyze: join signal rows with bid1/ask1 snapshots and estimate cost.
  3. record: record live bid1/ask1 snapshots from OKX ticker endpoint.

Examples:
  python backtest/orderbook_impact.py signals --csv ETH_USDT_15m_history.csv
  python backtest/orderbook_impact.py analyze --signals backtest/results/orderbook_signals.csv --book data.csv
  python backtest/orderbook_impact.py record --out logs/eth_bidask_live.csv
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
import os
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_HISTORY = ROOT / "ETH_USDT_15m_history.csv"
DEFAULT_SIGNALS = ROOT / "backtest" / "results" / "orderbook_signals.csv"
DEFAULT_ANALYSIS = ROOT / "backtest" / "results" / "orderbook_impact_summary.csv"
DEFAULT_TARDIS_BBO = ROOT / "backtest" / "results" / "tardis_bbo_signal_quotes.csv"

INST_ID = "ETH-USDT-SWAP"
OKX_TICKER_URL = "https://www.okx.com/api/v5/market/ticker"

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
CONTRACT_STEP = 0.01
MIN_ORDER_CONTRACTS = 0.01
TAKER_FEE = 0.0005
TRADING_ACCOUNT_TARGET = 100.0


def floor_to_step(value: float, step: float = CONTRACT_STEP) -> float:
    return math.floor(value / step) * step


def liq_price(direction: str, avg_entry: float, total_sz: float, total_margin: float) -> float:
    if total_sz <= 0:
        return 0.0
    margin_per_eth = (total_margin * 0.9) / (total_sz * CT_VAL)
    if direction == "long":
        return avg_entry - margin_per_eth
    return avg_entry + margin_per_eth


@dataclass
class Position:
    direction: str = "none"
    batches: list[tuple[int, float, float]] = field(default_factory=list)
    pending: list[tuple[int, float, float]] = field(default_factory=list)
    avg_entry: float = 0.0
    total_sz: float = 0.0
    total_margin: float = 0.0
    tp_price: float = 0.0
    liq_price: float = 0.0

    def is_active(self) -> bool:
        return self.direction != "none" and self.total_sz > 0

    def reset(self) -> None:
        self.__init__()

    def recalc(self) -> None:
        self.total_sz = sum(sz for _, _, sz in self.batches)
        self.avg_entry = sum(price * sz for _, price, sz in self.batches) / self.total_sz
        self.total_margin = self.total_sz * CT_VAL * self.avg_entry / LEVER
        self.tp_price = round(
            self.avg_entry + TP_PROFIT_USD
            if self.direction == "long"
            else self.avg_entry - TP_PROFIT_USD,
            2,
        )
        self.liq_price = round(
            liq_price(self.direction, self.avg_entry, self.total_sz, self.total_margin),
            2,
        )


class SignalBuilder:
    def __init__(self, df: pd.DataFrame, target_equity: float):
        self.df = df.copy().reset_index(drop=True)
        self.target_equity = target_equity
        self.pos = Position()
        self.last_plan_price = 0.0
        self.signals: list[dict] = []

    def run(self) -> list[dict]:
        self._precompute_boll()
        for _, row in self.df.iloc[BOLL_PERIOD:].iterrows():
            self._process_position(row)

            if self.pos.direction == "none":
                direction = self._signal(row)
                if direction == "none":
                    continue
                if (
                    self.last_plan_price > 0
                    and abs(float(row.close) - self.last_plan_price) < MIN_ENTRY_GAP_USD
                ):
                    continue

                plan = self._build_plan(direction, float(row.close), float(row.boll_width))
                if not plan:
                    continue

                first_idx, first_price, first_sz = plan[0]
                signal = {
                    "timestamp_ms": int(row.timestamp_ms),
                    "datetime_utc": row.datetime_utc,
                    "direction": direction,
                    "reference_price": round(float(row.close), 4),
                    "first_plan_price": first_price,
                    "first_sz": first_sz,
                    "first_notional": round(first_sz * CT_VAL * first_price, 4),
                    "boll_lower": round(float(row.boll_lower), 4),
                    "boll_mid": round(float(row.boll_mid), 4),
                    "boll_upper": round(float(row.boll_upper), 4),
                    "boll_width": round(float(row.boll_width), 4),
                    "low": round(float(row.low), 4),
                    "high": round(float(row.high), 4),
                    "close": round(float(row.close), 4),
                    "batch_idx": first_idx,
                }
                self.signals.append(signal)

                self.pos.direction = direction
                self.last_plan_price = float(row.close)
                self._fill(plan[0])
                self.pos.pending = list(plan[1:])
                self._fill_pending(row)
        return self.signals

    def _precompute_boll(self) -> None:
        close = self.df["close"]
        self.df["boll_mid"] = close.rolling(BOLL_PERIOD).mean()
        std = close.rolling(BOLL_PERIOD).std(ddof=0)
        self.df["boll_upper"] = self.df["boll_mid"] + BOLL_STD * std
        self.df["boll_lower"] = self.df["boll_mid"] - BOLL_STD * std
        self.df["boll_width"] = self.df["boll_upper"] - self.df["boll_lower"]

    def _signal(self, row) -> str:
        if pd.isna(row.boll_lower):
            return "none"
        width = float(row.boll_width)
        close = float(row.close)
        if width < MIN_BOLL_WIDTH_USD or width / close < MIN_BOLL_WIDTH_PCT:
            return "none"
        if close < float(row.boll_lower):
            return "long"
        if close > float(row.boll_upper):
            return "short"
        return "none"

    def _build_plan(self, direction: str, first_price: float, boll_width: float):
        orders = []
        for idx in range(BATCH_COUNT):
            spacing = boll_width * BATCH_SPACING[idx]
            price = first_price - spacing if direction == "long" else first_price + spacing
            price = round(price, 2)
            if price <= 0:
                break
            margin_budget = self.target_equity * BATCH_SIZE_RATIO[idx]
            raw_sz = margin_budget * LEVER / (price * CT_VAL)
            sz = floor_to_step(raw_sz)
            if sz >= MIN_ORDER_CONTRACTS:
                orders.append((idx, price, sz))
        return orders

    def _process_position(self, row) -> None:
        if self.pos.direction == "none":
            return
        self._fill_pending(row)
        if not self.pos.is_active():
            return

        if self.pos.direction == "long":
            liq_hit = float(row.low) <= self.pos.liq_price
            tp_hit = float(row.high) >= self.pos.tp_price
        else:
            liq_hit = float(row.high) >= self.pos.liq_price
            tp_hit = float(row.low) <= self.pos.tp_price
        if liq_hit or tp_hit:
            self.pos.reset()

    def _fill_pending(self, row) -> None:
        remaining = []
        for batch in self.pos.pending:
            _, price, _ = batch
            if self.pos.direction == "long":
                filled = float(row.low) <= price
            else:
                filled = float(row.high) >= price
            if filled:
                self._fill(batch)
            else:
                remaining.append(batch)
        self.pos.pending = remaining

        if self.pos.batches:
            outside = (
                self.pos.direction == "long"
                and float(row.close) < float(row.boll_lower)
            ) or (
                self.pos.direction == "short"
                and float(row.close) > float(row.boll_upper)
            )
            if not outside:
                self.pos.pending = []

    def _fill(self, batch: tuple[int, float, float]) -> None:
        self.pos.batches.append(batch)
        self.pos.recalc()


def load_history(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    required = {"timestamp_ms", "datetime_utc", "open", "high", "low", "close", "vol"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"history csv missing columns: {sorted(missing)}")
    for col in ["open", "high", "low", "close", "vol"]:
        df[col] = df[col].astype(float)
    df["timestamp_ms"] = df["timestamp_ms"].astype("int64")
    return df


def write_signals(args) -> None:
    df = load_history(Path(args.csv))
    signals = SignalBuilder(df, args.target_equity).run()
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(signals).to_csv(out, index=False, encoding="utf-8")
    print(f"signals={len(signals)}")
    print(f"saved={out}")


def normalize_book(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    rename = {}
    aliases = {
        "timestamp_ms": ["timestamp_ms", "ts", "time", "local_timestamp", "exchange_timestamp"],
        "bid1": ["bid1", "bid_px", "bidPx", "best_bid", "bid_price"],
        "ask1": ["ask1", "ask_px", "askPx", "best_ask", "ask_price"],
    }
    for target, names in aliases.items():
        for name in names:
            if name in df.columns:
                rename[name] = target
                break
    df = df.rename(columns=rename)
    missing = {"timestamp_ms", "bid1", "ask1"} - set(df.columns)
    if missing:
        raise ValueError(
            "book csv needs timestamp_ms,bid1,ask1 columns "
            f"(or recognized aliases); missing {sorted(missing)}"
        )
    df = df[["timestamp_ms", "bid1", "ask1"]].copy()
    df["timestamp_ms"] = df["timestamp_ms"].astype("int64")
    df["bid1"] = df["bid1"].astype(float)
    df["ask1"] = df["ask1"].astype(float)
    return df.sort_values("timestamp_ms")


def analyze(args) -> None:
    signals = pd.read_csv(args.signals).sort_values("timestamp_ms")
    book = normalize_book(Path(args.book))

    merged = pd.merge_asof(
        signals,
        book,
        on="timestamp_ms",
        direction=args.direction,
        tolerance=args.tolerance_ms,
    )
    matched = merged.dropna(subset=["bid1", "ask1"]).copy()
    if matched.empty:
        raise RuntimeError("no orderbook rows matched signals; increase --tolerance-ms")

    matched["opponent_price"] = np.where(
        matched["direction"] == "long",
        matched["ask1"],
        matched["bid1"],
    )
    matched["opponent_gap"] = np.where(
        matched["direction"] == "long",
        matched["ask1"] - matched["reference_price"],
        matched["reference_price"] - matched["bid1"],
    )
    matched["spread"] = matched["ask1"] - matched["bid1"]

    rows = []
    for protection in args.protection:
        used = matched[
            (matched["opponent_gap"] >= 0)
            & (matched["opponent_gap"] <= protection)
        ].copy()
        used["entry_cost_usdt"] = used["opponent_gap"] * used["first_sz"] * CT_VAL
        used["extra_fee_usdt"] = used["opponent_gap"] * used["first_sz"] * CT_VAL * TAKER_FEE
        total_entry_cost = float(used["entry_cost_usdt"].sum())
        total_fee_cost = float(used["extra_fee_usdt"].sum())
        rows.append(
            {
                "protection_usdt": protection,
                "signals_total": len(signals),
                "book_matched": len(matched),
                "used_opponent": len(used),
                "fallback": len(matched) - len(used),
                "used_ratio": round(len(used) / len(matched), 6),
                "avg_gap": round(float(used["opponent_gap"].mean()) if len(used) else 0, 6),
                "p50_gap": round(float(used["opponent_gap"].median()) if len(used) else 0, 6),
                "p95_gap": round(float(used["opponent_gap"].quantile(0.95)) if len(used) else 0, 6),
                "max_gap": round(float(used["opponent_gap"].max()) if len(used) else 0, 6),
                "avg_spread": round(float(used["spread"].mean()) if len(used) else 0, 6),
                "entry_cost_usdt": round(total_entry_cost, 6),
                "extra_fee_usdt": round(total_fee_cost, 6),
                "total_cost_usdt": round(total_entry_cost + total_fee_cost, 6),
            }
        )

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out, index=False, encoding="utf-8")

    detail_out = out.with_name(out.stem + "_details.csv")
    matched.to_csv(detail_out, index=False, encoding="utf-8")
    print(f"matched={len(matched)}/{len(signals)}")
    print(f"summary={out}")
    print(f"details={detail_out}")


def timestamp_ms_to_utc(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)


def floor_minute(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


def ceil_minute(dt: datetime) -> datetime:
    floored = floor_minute(dt)
    if floored == dt:
        return floored
    return floored + timedelta(minutes=1)


def build_signal_windows(signals: pd.DataFrame, window_seconds: float):
    window = timedelta(seconds=window_seconds)
    rows = []
    for row in signals.itertuples(index=False):
        ts_ms = int(row.timestamp_ms)
        center = timestamp_ms_to_utc(ts_ms)
        start = floor_minute(center - window)
        end = ceil_minute(center + window)
        if end <= start:
            end = start + timedelta(minutes=1)
        rows.append(
            {
                "signal_ts": ts_ms,
                "center": center,
                "start": start,
                "end": end,
            }
        )

    rows.sort(key=lambda item: item["start"])
    windows = []
    for item in rows:
        if not windows or item["start"] > windows[-1]["end"]:
            windows.append(
                {
                    "start": item["start"],
                    "end": item["end"],
                    "signals": [item["signal_ts"]],
                }
            )
        else:
            windows[-1]["end"] = max(windows[-1]["end"], item["end"])
            windows[-1]["signals"].append(item["signal_ts"])
    return windows


def extract_bbo(message: dict):
    data = message.get("data")
    if not data:
        return None
    item = data[0]

    if "bids" in item and "asks" in item and item["bids"] and item["asks"]:
        bid1 = float(item["bids"][0][0])
        ask1 = float(item["asks"][0][0])
    elif "bidPx" in item and "askPx" in item:
        bid1 = float(item["bidPx"])
        ask1 = float(item["askPx"])
    else:
        return None

    ts = item.get("ts") or item.get("timestamp")
    ts_ms = int(ts) if ts is not None else None
    return ts_ms, bid1, ask1


async def download_tardis_bbo_async(args) -> None:
    try:
        from tardis_dev import Channel, replay
    except ImportError as exc:
        raise RuntimeError(
            "tardis-dev is not installed. Run: .\\.venv\\Scripts\\pip.exe install tardis-dev"
        ) from exc

    api_key = args.api_key or os.getenv("TARDIS_API_KEY", "")
    if not api_key and not args.allow_no_api_key:
        raise RuntimeError(
            "Missing Tardis API key. Set TARDIS_API_KEY or pass --api-key. "
            "Use --allow-no-api-key only for public sample data."
        )

    signals = pd.read_csv(args.signals).sort_values("timestamp_ms")
    if args.max_signals:
        signals = signals.head(args.max_signals)
    signal_meta = {
        int(row.timestamp_ms): {
            "datetime_utc": row.datetime_utc,
            "direction": row.direction,
            "reference_price": float(row.reference_price),
            "first_sz": float(row.first_sz),
        }
        for row in signals.itertuples(index=False)
    }
    windows = build_signal_windows(signals, args.window_seconds)

    best_by_signal = {
        ts: {
            "distance_ms": None,
            "source_timestamp_ms": None,
            "bid1": None,
            "ask1": None,
        }
        for ts in signal_meta
    }
    window_ms = int(args.window_seconds * 1000)

    for idx, window in enumerate(windows, start=1):
        print(
            f"window {idx}/{len(windows)} "
            f"{window['start'].isoformat()} -> {window['end'].isoformat()} "
            f"signals={len(window['signals'])}"
        )
        window_signal_set = set(window["signals"])
        async for response in replay(
            exchange=args.exchange,
            from_date=window["start"],
            to_date=window["end"],
            filters=[Channel(name=args.channel, symbols=[args.symbol])],
            api_key=api_key,
            cache_dir=args.cache_dir,
            timeout=args.timeout,
        ):
            if response is None:
                continue
            extracted = extract_bbo(response.message)
            if extracted is None:
                continue
            source_ts_ms, bid1, ask1 = extracted
            if source_ts_ms is None:
                source_ts_ms = int(response.local_timestamp.timestamp() * 1000)

            for signal_ts in window_signal_set:
                distance = abs(source_ts_ms - signal_ts)
                if distance > window_ms:
                    continue
                current = best_by_signal[signal_ts]["distance_ms"]
                if current is None or distance < current:
                    best_by_signal[signal_ts] = {
                        "distance_ms": distance,
                        "source_timestamp_ms": source_ts_ms,
                        "bid1": bid1,
                        "ask1": ask1,
                    }

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for signal_ts, quote in sorted(best_by_signal.items()):
        meta = signal_meta[signal_ts]
        bid1 = quote["bid1"]
        ask1 = quote["ask1"]
        if bid1 is None or ask1 is None:
            opponent_price = None
            opponent_gap = None
            spread = None
        else:
            opponent_price = ask1 if meta["direction"] == "long" else bid1
            opponent_gap = (
                ask1 - meta["reference_price"]
                if meta["direction"] == "long"
                else meta["reference_price"] - bid1
            )
            spread = ask1 - bid1
        rows.append(
            {
                "timestamp_ms": signal_ts,
                "datetime_utc": meta["datetime_utc"],
                "direction": meta["direction"],
                "reference_price": meta["reference_price"],
                "first_sz": meta["first_sz"],
                "source_timestamp_ms": quote["source_timestamp_ms"],
                "distance_ms": quote["distance_ms"],
                "bid1": bid1,
                "ask1": ask1,
                "spread": spread,
                "opponent_price": opponent_price,
                "opponent_gap": opponent_gap,
            }
        )

    pd.DataFrame(rows).to_csv(out, index=False, encoding="utf-8")
    matched = sum(1 for row in rows if row["bid1"] is not None and row["ask1"] is not None)
    print(f"matched={matched}/{len(rows)}")
    print(f"saved={out}")


def download_tardis_bbo(args) -> None:
    asyncio.run(download_tardis_bbo_async(args))


def fetch_ticker(inst_id: str) -> dict:
    query = urllib.parse.urlencode({"instId": inst_id})
    with urllib.request.urlopen(f"{OKX_TICKER_URL}?{query}", timeout=10) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    if data.get("code") != "0":
        raise RuntimeError(data)
    item = data["data"][0]
    return {
        "local_timestamp_ms": int(time.time() * 1000),
        "timestamp_ms": int(item["ts"]),
        "inst_id": item["instId"],
        "last": float(item["last"]),
        "bid1": float(item["bidPx"]),
        "bid1_size": float(item["bidSz"]),
        "ask1": float(item["askPx"]),
        "ask1_size": float(item["askSz"]),
        "spread": float(item["askPx"]) - float(item["bidPx"]),
    }


def record(args) -> None:
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    exists = out.exists()
    fields = [
        "local_timestamp_ms",
        "timestamp_ms",
        "inst_id",
        "last",
        "bid1",
        "bid1_size",
        "ask1",
        "ask1_size",
        "spread",
    ]
    with out.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        if not exists:
            writer.writeheader()
        end_at = time.time() + args.seconds if args.seconds > 0 else None
        count = 0
        while end_at is None or time.time() < end_at:
            try:
                row = fetch_ticker(args.inst_id)
                writer.writerow(row)
                f.flush()
                count += 1
                print(
                    f"{count} bid1={row['bid1']:.2f} ask1={row['ask1']:.2f} "
                    f"spread={row['spread']:.4f}",
                    end="\r",
                )
            except Exception as exc:
                print(f"\nrecord error: {exc}")
            time.sleep(args.interval)
    print(f"\nsaved={out}")


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="mode", required=True)

    p = sub.add_parser("signals")
    p.add_argument("--csv", default=str(DEFAULT_HISTORY))
    p.add_argument("--out", default=str(DEFAULT_SIGNALS))
    p.add_argument("--target-equity", type=float, default=TRADING_ACCOUNT_TARGET)
    p.set_defaults(func=write_signals)

    p = sub.add_parser("analyze")
    p.add_argument("--signals", default=str(DEFAULT_SIGNALS))
    p.add_argument("--book", required=True)
    p.add_argument("--out", default=str(DEFAULT_ANALYSIS))
    p.add_argument("--tolerance-ms", type=int, default=60_000)
    p.add_argument("--direction", choices=["nearest", "backward", "forward"], default="nearest")
    p.add_argument("--protection", type=float, nargs="+", default=[0.1, 0.2, 0.3, 0.5])
    p.set_defaults(func=analyze)

    p = sub.add_parser("record")
    p.add_argument("--out", default=str(ROOT / "logs" / "eth_bidask_live.csv"))
    p.add_argument("--inst-id", default=INST_ID)
    p.add_argument("--interval", type=float, default=1.0)
    p.add_argument("--seconds", type=float, default=0.0)
    p.set_defaults(func=record)

    p = sub.add_parser("tardis-bbo")
    p.add_argument("--signals", default=str(DEFAULT_SIGNALS))
    p.add_argument("--out", default=str(DEFAULT_TARDIS_BBO))
    p.add_argument("--exchange", default="okex-swap")
    p.add_argument("--channel", default="bbo-tbt")
    p.add_argument("--symbol", default=INST_ID)
    p.add_argument("--window-seconds", type=float, default=10.0)
    p.add_argument("--api-key", default="")
    p.add_argument("--allow-no-api-key", action="store_true")
    p.add_argument("--cache-dir", default=str(ROOT / ".tardis-cache"))
    p.add_argument("--timeout", type=int, default=60)
    p.add_argument("--max-signals", type=int, default=0)
    p.set_defaults(func=download_tardis_bbo)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
