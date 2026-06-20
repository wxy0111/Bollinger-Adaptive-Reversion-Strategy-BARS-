"""Audit OKX close-order PnL from fill history.

This is a read-only helper. It does not place orders, cancel orders, transfer
funds, or withdraw funds.

Examples:

    python tools/check_close_pnl.py --close-ord-id 3672470175281569794
    python tools/check_close_pnl.py --direction short --close-ord-id 3672470175281569794 --entry-ord-id 367246...
    python tools/check_close_pnl.py --direction short --latest
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path
from typing import Iterable

import aiohttp

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config import INST_ID  # noqa: E402
from src.okx_client import OKXClient  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Read-only OKX close PnL audit from fill history.")
    parser.add_argument("--inst-id", default=INST_ID, help=f"Instrument id. Default: {INST_ID}.")
    parser.add_argument("--direction", choices=("long", "short"), default="", help="Position direction.")
    parser.add_argument("--close-ord-id", default="", help="Close order id to audit.")
    parser.add_argument(
        "--entry-ord-id",
        action="append",
        default=[],
        help="Entry order id for the same cycle. Can be passed multiple times.",
    )
    parser.add_argument(
        "--latest",
        action="store_true",
        help="Use the latest close fill matching --direction when --close-ord-id is omitted.",
    )
    parser.add_argument("--lookback-min", type=int, default=180, help="Fill lookup window in minutes. Default: 180.")
    parser.add_argument("--limit", type=int, default=100, help="OKX fill-history limit. Default: 100.")
    return parser.parse_args()


def _float(value) -> float:
    """Convert an OKX numeric field to float."""
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _fill_time(fill: dict) -> int:
    """Return fill timestamp in milliseconds."""
    try:
        return int(fill.get("fillTime") or fill.get("ts") or 0)
    except (TypeError, ValueError):
        return 0


def _fill_sort_key(fill: dict) -> tuple[int, str]:
    """Return stable sort key for a fill."""
    return (_fill_time(fill), str(fill.get("tradeId") or fill.get("ordId") or ""))


def _unique_fills(fills: Iterable[dict]) -> list[dict]:
    """Deduplicate fill rows by order, trade, time, and size."""
    seen = set()
    out = []
    for fill in fills:
        key = (
            str(fill.get("ordId") or ""),
            str(fill.get("tradeId") or ""),
            str(fill.get("fillTime") or fill.get("ts") or ""),
            str(fill.get("fillSz") or fill.get("sz") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(fill)
    return out


def _side_for_direction(direction: str) -> str:
    """Return close side for a position direction."""
    if direction == "long":
        return "sell"
    if direction == "short":
        return "buy"
    return ""


def _summarize(fills: list[dict]) -> dict:
    """Summarize fill PnL, fees, size, and average price."""
    gross_pnl = 0.0
    fee = 0.0
    sz = 0.0
    weighted_px = 0.0
    for fill in fills:
        fill_sz = _float(fill.get("fillSz") or fill.get("sz"))
        fill_px = _float(fill.get("fillPx") or fill.get("px"))
        gross_pnl += _float(fill.get("fillPnl"))
        fee += _float(fill.get("fee"))
        if fill_sz > 0 and fill_px > 0:
            sz += fill_sz
            weighted_px += fill_sz * fill_px
    return {
        "gross_pnl": round(gross_pnl, 8),
        "fee": round(fee, 8),
        "sz": round(sz, 8),
        "avg_px": round(weighted_px / sz, 8) if sz > 0 else 0.0,
        "fills": len(fills),
    }


def _print_fills(title: str, fills: list[dict]) -> None:
    """Print fill rows compactly."""
    print(f"\n{title} ({len(fills)} fill(s))")
    for fill in sorted(fills, key=_fill_sort_key):
        print(
            "  "
            f"time={fill.get('fillTime') or fill.get('ts') or '--'} "
            f"ordId={fill.get('ordId') or '--'} "
            f"tradeId={fill.get('tradeId') or '--'} "
            f"side={fill.get('side') or '--'} "
            f"posSide={fill.get('posSide') or '--'} "
            f"px={fill.get('fillPx') or fill.get('px') or '--'} "
            f"sz={fill.get('fillSz') or fill.get('sz') or '--'} "
            f"fillPnl={fill.get('fillPnl') or '0'} "
            f"fee={fill.get('fee') or '0'}"
        )


async def main() -> int:
    """CLI entry point."""
    args = parse_args()
    if not args.close_ord_id and not args.latest:
        print("Pass --close-ord-id, or use --latest with --direction.")
        return 2
    if args.latest and not args.direction:
        print("--latest requires --direction long|short.")
        return 2

    end_ms = int(time.time() * 1000)
    begin_ms = end_ms - args.lookback_min * 60 * 1000
    async with aiohttp.ClientSession() as session:
        client = OKXClient(session)
        fills = await client.get_fills_history(args.inst_id, begin=begin_ms, end=end_ms, limit=args.limit)

    fills = _unique_fills(fills)
    close_fills = []
    if args.close_ord_id:
        close_fills = [fill for fill in fills if str(fill.get("ordId") or "") == str(args.close_ord_id)]
    else:
        close_side = _side_for_direction(args.direction)
        candidates = [
            fill
            for fill in fills
            if fill.get("side") == close_side and fill.get("posSide") == args.direction
        ]
        candidates.sort(key=_fill_sort_key, reverse=True)
        if candidates:
            close_ord_id = str(candidates[0].get("ordId") or "")
            close_fills = [fill for fill in fills if str(fill.get("ordId") or "") == close_ord_id]
            args.close_ord_id = close_ord_id

    if not close_fills:
        print("No close fills found in the lookup window.")
        return 1

    close_summary = _summarize(close_fills)
    entry_ord_ids = {str(ord_id) for ord_id in args.entry_ord_id if str(ord_id).strip()}
    entry_fills = [fill for fill in fills if str(fill.get("ordId") or "") in entry_ord_ids]
    entry_summary = _summarize(entry_fills)

    net_close_only = round(close_summary["gross_pnl"] + close_summary["fee"], 8)
    net_with_entry_fees = round(close_summary["gross_pnl"] + close_summary["fee"] + entry_summary["fee"], 8)

    print(f"Instrument: {args.inst_id}")
    print(f"Direction:  {args.direction or close_fills[0].get('posSide') or '--'}")
    print(f"Close ord:  {args.close_ord_id}")
    print(f"Entry ords: {', '.join(sorted(entry_ord_ids)) if entry_ord_ids else '--'}")
    print()
    print(f"Close gross PnL:       {close_summary['gross_pnl']:+.8f} USDT")
    print(f"Close fee:             {close_summary['fee']:+.8f} USDT")
    print(f"Entry fee:             {entry_summary['fee']:+.8f} USDT")
    print(f"Net, close fee only:   {net_close_only:+.8f} USDT")
    print(f"Net, entry+close fee:  {net_with_entry_fees:+.8f} USDT")
    print(f"Close avg price/size:  {close_summary['avg_px']:.8f} / {close_summary['sz']:.8f}")

    _print_fills("Close fills", close_fills)
    if entry_ord_ids:
        _print_fills("Entry fills", entry_fills)
        missing = entry_ord_ids - {str(fill.get("ordId") or "") for fill in entry_fills}
        if missing:
            print(f"\nMissing entry fill(s) in lookup window: {', '.join(sorted(missing))}")
    else:
        print("\nNo entry order ids were supplied, so entry fees are not included.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
