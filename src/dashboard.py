"""Local aiohttp dashboard for live strategy state and historical logs."""
import copy
import json
import re
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from time import monotonic
from typing import List

from aiohttp import web
from loguru import logger

from src.config import WEB_HOST, WEB_PORT


LOG_DIR = Path("logs")
ASSET_DIR = Path(__file__).resolve().parent.parent / "assets"
PNL_CORRECTIONS_PATH = LOG_DIR / "pnl_corrections.json"
HISTORY_CACHE_TTL_SECONDS = 30
HISTORY_CACHE: dict[tuple[str, int], tuple[float, tuple[tuple[str, int, int], ...], dict]] = {}
DASHBOARD_RISK_THRESHOLDS = {
    "liquidation_danger_pct": 8,
    "liquidation_warning_pct": 18,
    "stale_seconds": 10,
    "offline_seconds": 30,
}
TICK_RE = re.compile(
    "^(?P<ts>\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d+).*?"
    "(?:price|\\u4ef7\\u683c)=(?P<price>\\d+(?:\\.\\d+)?)\\s+"
    "(?:Boll|\\u5e03\\u6797)\\[(?P<lower>\\d+(?:\\.\\d+)?)\\s+\\|\\s+"
    "(?P<mid>\\d+(?:\\.\\d+)?)\\s+\\|\\s+"
    "(?P<upper>\\d+(?:\\.\\d+)?)\\]\\s+"
    "(?:position|\\u6301\\u4ed3)=(?P<direction>\\w+)\\s+"
    "(?:equity|\\u6743\\u76ca)=(?P<equity>-?\\d+(?:\\.\\d+)?)"
)
TS_RE = re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?)")
PROBE_RE = re.compile(r"Probe entry prepared direction=(?P<direction>long|short)")
ORDER_RE = re.compile(
    r"下单\s+(?P<side>buy|sell)/(?P<pos_side>long|short)\s+sz=(?P<sz>\d+(?:\.\d+)?)\s+px=(?P<price>\d+(?:\.\d+)?)"
)
FILL_RE = re.compile(
    r"第(?P<batch>\d+)批成交\s+price=(?P<price>\d+(?:\.\d+)?)\s+sz=(?P<sz>\d+(?:\.\d+)?)"
)
TP_RE = re.compile(r"止盈挂单\s+价格=(?P<price>\d+(?:\.\d+)?)\s+张数=(?P<sz>\d+(?:\.\d+)?)")
CLOSE_RE = re.compile(r"Position closed; reset strategy state")
RESET_RE = re.compile(r"持仓状态已重置")
CAPITAL_PROFIT_RE = re.compile(r"\[Capital\]\s+Profit\s+\+(?P<pnl>\d+(?:\.\d+)?)\s+USDT")
CAPITAL_LOSS_RE = re.compile(r"\[Capital\]\s+Loss\s+(?P<pnl>-\d+(?:\.\d+)?)\s+USDT")
CLOSE_SUMMARY_RE = re.compile(
    r"止盈平仓\s+(?P<direction>long|short)\s+均价=(?P<avg>\d+(?:\.\d+)?)\s+"
    r"平仓价=(?P<exit>\d+(?:\.\d+)?)\s+张数=(?P<sz>\d+(?:\.\d+)?)\s+"
    r"估算收益=(?P<pnl>[+-]?\d+(?:\.\d+)?)\s+USDT"
)
CT_VAL = 0.1
POSITION_CLOSE_RE = re.compile(
    r"Position closed\s+(?P<direction>long|short)\s+avg_entry=(?P<avg>\d+(?:\.\d+)?)\s+"
    r"(?:close_avg|close_ref)=(?P<exit>\d+(?:\.\d+)?)\s+sz=(?P<sz>\d+(?:\.\d+)?)\s+"
    r"(?P<label>actual_pnl|estimated_pnl)=(?P<pnl>[+-]?\d+(?:\.\d+)?)\s+USDT"
)


@dataclass
class TradeRecord:
    """One dashboard trade-history row."""

    time: str
    action: str
    price: float
    sz: float
    pnl: float = 0.0


@dataclass
class DashboardState:
    """Mutable state displayed by the local dashboard."""

    mark_price: float = 0.0
    boll_lower: float = 0.0
    boll_mid: float = 0.0
    boll_upper: float = 0.0
    equity: float = 0.0
    peak_equity: float = 0.0
    direction: str = "none"
    avg_entry: float = 0.0
    total_sz: float = 0.0
    tp_price: float = 0.0
    sl_price: float = 0.0
    sl_mode: str = ""
    liq_price: float = 0.0
    unrealized_pnl: float = 0.0
    batches: List[dict] = field(default_factory=list)
    today_pnl: float = 0.0
    total_trades: int = 0
    trade_history: List[dict] = field(default_factory=list)
    updated_at: str = ""

    def update_time(self):
        """Refresh the dashboard update timestamp."""
        self.updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def add_trade(self, action: str, price: float, sz: float, pnl: float = 0.0):
        """Add one trade row to the dashboard history."""
        record = {
            "time": datetime.now().strftime("%H:%M:%S"),
            "action": action,
            "price": price,
            "sz": sz,
            "pnl": pnl,
        }
        self.trade_history.insert(0, record)
        self.trade_history = self.trade_history[:20]
        self.total_trades += 1
        self.today_pnl += pnl


state = DashboardState()


def _list_log_files() -> list[dict]:
    """Return available strategy log files."""
    if not LOG_DIR.exists():
        return []
    files = []
    for path in sorted(LOG_DIR.glob("boll_pin_*.log"), reverse=True):
        files.append({
            "name": path.name,
            "size": path.stat().st_size,
            "modified": datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
        })
    return files


def _selected_log_paths(filename: str) -> list[Path]:
    """Return one or all safe log paths selected by the dashboard."""
    if filename == "__all__":
        return sorted(LOG_DIR.glob("boll_pin_*.log"))
    path = LOG_DIR / Path(filename).name
    if not path.exists() or path.parent.resolve() != LOG_DIR.resolve():
        raise web.HTTPNotFound(text="log file not found")
    return [path]


def _date_key(ts: str) -> str:
    """Return YYYY-MM-DD from a log timestamp."""
    return ts[:10] if ts else ""


def _empty_daily_row(day: str) -> dict:
    """Build one daily history aggregate row."""
    return {
        "date": day,
        "orders": 0,
        "entry_fills": 0,
        "first_fills": 0,
        "add_fills": 0,
        "closes": 0,
        "pnl": 0.0,
        "actual_pnl": 0.0,
        "estimated_pnl": 0.0,
        "cum_pnl": 0.0,
        "source": "estimate",
    }


def _load_pnl_corrections() -> dict[str, float]:
    """Load manual exchange-net PnL corrections keyed by close timestamp."""
    if not PNL_CORRECTIONS_PATH.exists():
        return {}
    try:
        raw = json.loads(PNL_CORRECTIONS_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning(f"Load PnL corrections failed: {exc}")
        return {}
    corrections = {}
    for key, value in raw.items():
        try:
            corrections[str(key)[:19]] = float(value)
        except (TypeError, ValueError):
            logger.warning(f"Ignore invalid PnL correction {key}={value}")
    return corrections


def _parse_trade_history(paths: list[Path]) -> dict:
    """Parse strategy-owned entries, closes, and realized PnL from logs."""
    pnl_corrections = _load_pnl_corrections()
    events = []
    trades = []
    daily: dict[str, dict] = {}
    current = {
        "direction": "none",
        "fills": [],
        "entry_time": "",
        "tp_price": 0.0,
        "close_summary": None,
    }
    last_closed_trade = None

    def day_row(ts: str) -> dict:
        day = _date_key(ts)
        if day not in daily:
            daily[day] = _empty_daily_row(day)
        return daily[day]

    for path in paths:
        with path.open("r", encoding="utf-8", errors="ignore") as file:
            for line in file:
                ts_match = TS_RE.search(line)
                if not ts_match:
                    continue
                ts = ts_match.group("ts")[:19]

                position_close_match = POSITION_CLOSE_RE.search(line)
                if position_close_match:
                    pnl = float(position_close_match.group("pnl"))
                    is_actual = position_close_match.group("label") == "actual_pnl"
                    current["close_summary"] = {
                        "time": ts,
                        "direction": position_close_match.group("direction"),
                        "avg_entry": float(position_close_match.group("avg")),
                        "exit_price": float(position_close_match.group("exit")),
                        "sz": float(position_close_match.group("sz")),
                        "estimated_pnl": 0.0 if is_actual else pnl,
                        "actual_pnl": pnl if is_actual else None,
                    }
                    events.append({
                        "time": ts,
                        "type": "close_summary",
                        "direction": current["close_summary"]["direction"],
                        "price": current["close_summary"]["exit_price"],
                        "sz": current["close_summary"]["sz"],
                        "pnl": pnl,
                        "note": "close summary",
                    })
                    continue

                close_summary_match = CLOSE_SUMMARY_RE.search(line)
                if close_summary_match:
                    current["close_summary"] = {
                        "time": ts,
                        "direction": close_summary_match.group("direction"),
                        "avg_entry": float(close_summary_match.group("avg")),
                        "exit_price": float(close_summary_match.group("exit")),
                        "sz": float(close_summary_match.group("sz")),
                        "estimated_pnl": float(close_summary_match.group("pnl")),
                        "actual_pnl": None,
                    }
                    events.append({
                        "time": ts,
                        "type": "close_summary",
                        "direction": current["close_summary"]["direction"],
                        "price": current["close_summary"]["exit_price"],
                        "sz": current["close_summary"]["sz"],
                        "pnl": current["close_summary"]["estimated_pnl"],
                        "note": "平仓摘要",
                    })
                    continue

                capital_match = CAPITAL_PROFIT_RE.search(line) or CAPITAL_LOSS_RE.search(line)
                if capital_match:
                    actual_pnl = float(capital_match.group("pnl"))
                    correction = pnl_corrections.get(ts)
                    if correction is not None:
                        actual_pnl = correction
                    row = day_row(ts)
                    if last_closed_trade is None or _date_key(last_closed_trade.get("exit_time", "")) != _date_key(ts):
                        last_closed_trade = {
                            "record_type": "settlement",
                            "entry_time": "",
                            "exit_time": ts,
                            "direction": "",
                            "avg_entry": 0.0,
                            "exit_price": 0.0,
                            "sz": 0.0,
                            "batches": 0,
                            "estimated_pnl": 0.0,
                            "actual_pnl": 0.0,
                            "pnl": 0.0,
                            "pnl_source": "capital",
                            "note": "跨日固本划转",
                        }
                        trades.append(last_closed_trade)
                    row["actual_pnl"] += actual_pnl
                    row["source"] = "capital"
                    if last_closed_trade is not None and not last_closed_trade.get("actual_locked"):
                        last_closed_trade["actual_pnl"] = round(actual_pnl, 4)
                        last_closed_trade["pnl"] = round(actual_pnl, 4)
                        last_closed_trade["pnl_source"] = "capital"
                        if correction is not None:
                            last_closed_trade["pnl_source"] = "exchange_correction"
                            last_closed_trade["note"] = "交易所净收益修正"
                        last_closed_trade["actual_locked"] = True
                    events.append({
                        "time": ts,
                        "type": "capital_profit" if actual_pnl >= 0 else "capital_loss",
                        "direction": "",
                        "price": 0.0,
                        "sz": 0.0,
                        "pnl": round(actual_pnl, 4),
                        "note": "交易所净收益修正" if correction is not None else "固本实际收益",
                    })
                    continue

                probe_match = PROBE_RE.search(line)
                if probe_match:
                    current["direction"] = probe_match.group("direction")
                    current["entry_time"] = ts
                    events.append({
                        "time": ts,
                        "type": "signal",
                        "direction": current["direction"],
                        "price": 0.0,
                        "sz": 0.0,
                        "pnl": 0.0,
                        "note": "头仓信号",
                    })
                    continue

                order_match = ORDER_RE.search(line)
                if order_match:
                    pos_side = order_match.group("pos_side")
                    side = order_match.group("side")
                    price = float(order_match.group("price"))
                    sz = float(order_match.group("sz"))
                    is_exit = (pos_side == "long" and side == "sell") or (pos_side == "short" and side == "buy")
                    if not is_exit:
                        row = day_row(ts)
                        row["orders"] += 1
                    events.append({
                        "time": ts,
                        "type": "exit_order" if is_exit else "entry_order",
                        "direction": pos_side,
                        "price": price,
                        "sz": sz,
                        "pnl": 0.0,
                        "note": "止盈/减仓挂单" if is_exit else "入场挂单",
                    })
                    continue

                fill_match = FILL_RE.search(line)
                if fill_match:
                    batch = int(fill_match.group("batch"))
                    price = float(fill_match.group("price"))
                    sz = float(fill_match.group("sz"))
                    if current["direction"] == "none":
                        current["direction"] = "unknown"
                    if not current["entry_time"]:
                        current["entry_time"] = ts
                    current["fills"].append({"batch": batch, "price": price, "sz": sz, "time": ts})
                    row = day_row(ts)
                    row["entry_fills"] += 1
                    if batch == 1:
                        row["first_fills"] += 1
                    else:
                        row["add_fills"] += 1
                    events.append({
                        "time": ts,
                        "type": "first_fill" if batch == 1 else "add_fill",
                        "direction": current["direction"],
                        "price": price,
                        "sz": sz,
                        "pnl": 0.0,
                        "note": f"第{batch}批成交",
                    })
                    continue

                tp_match = TP_RE.search(line)
                if tp_match:
                    current["tp_price"] = float(tp_match.group("price"))
                    continue

                if CLOSE_RE.search(line) or RESET_RE.search(line):
                    fills = current["fills"]
                    if not fills and not current.get("close_summary"):
                        current = {
                            "direction": "none",
                            "fills": [],
                            "entry_time": "",
                            "tp_price": 0.0,
                            "close_summary": None,
                        }
                        continue
                    total_sz = sum(item["sz"] for item in fills)
                    avg_entry = (
                        sum(item["price"] * item["sz"] for item in fills) / total_sz
                        if total_sz > 0 else 0.0
                    )
                    exit_price = current["tp_price"]
                    summary = current.get("close_summary")
                    if summary and total_sz <= 0:
                        total_sz = summary["sz"]
                        avg_entry = summary["avg_entry"]
                        exit_price = summary["exit_price"]
                        current["direction"] = summary["direction"]
                    estimated_pnl = 0.0
                    if total_sz > 0 and avg_entry > 0 and exit_price > 0:
                        if current["direction"] == "long":
                            estimated_pnl = (exit_price - avg_entry) * total_sz * CT_VAL
                        elif current["direction"] == "short":
                            estimated_pnl = (avg_entry - exit_price) * total_sz * CT_VAL
                    if summary:
                        estimated_pnl = summary["estimated_pnl"]
                    actual_pnl = summary.get("actual_pnl") if summary else None
                    correction = pnl_corrections.get(ts)
                    if correction is not None:
                        actual_pnl = correction
                    display_pnl = actual_pnl if actual_pnl is not None else estimated_pnl
                    pnl_source = "fill" if actual_pnl is not None else "estimate"
                    if correction is not None:
                        pnl_source = "exchange_correction"
                    row = day_row(ts)
                    row["closes"] += 1
                    row["estimated_pnl"] += estimated_pnl
                    trade = {
                        "record_type": "trade",
                        "entry_time": current["entry_time"],
                        "exit_time": ts,
                        "direction": current["direction"],
                        "avg_entry": round(avg_entry, 4),
                        "exit_price": round(exit_price, 4),
                        "sz": round(total_sz, 8),
                        "batches": len(fills),
                        "estimated_pnl": round(estimated_pnl, 4),
                        "actual_pnl": round(actual_pnl, 4) if actual_pnl is not None else 0.0,
                        "pnl": round(display_pnl, 4),
                        "pnl_source": pnl_source,
                        "note": "交易所净收益修正" if correction is not None else "",
                    }
                    trades.append(trade)
                    last_closed_trade = trade
                    events.append({
                        "time": ts,
                        "type": "close",
                        "direction": current["direction"],
                        "price": round(exit_price, 4),
                        "sz": round(total_sz, 8),
                        "pnl": round(display_pnl, 4),
                        "note": "平仓",
                    })
                    current = {"direction": "none", "fills": [], "entry_time": "", "tp_price": 0.0, "close_summary": None}

    cum = 0.0
    daily_rows = []
    for day in sorted(daily):
        row = daily[day]
        row["actual_pnl"] = round(row["actual_pnl"], 4)
        row["estimated_pnl"] = round(row["estimated_pnl"], 4)
        row["pnl"] = row["actual_pnl"]
        row["pnl"] = round(row["pnl"], 4)
        cum += row["pnl"]
        row["cum_pnl"] = round(cum, 4)
        daily_rows.append(row)

    for trade in trades:
        trade.pop("actual_locked", None)
    closed_trades = [trade for trade in trades if trade.get("record_type") != "settlement"]
    actual_records = [
        trade for trade in trades
        if trade.get("pnl_source") in ("capital", "exchange_correction")
    ]
    wins = [trade for trade in actual_records if trade["pnl"] > 0]
    total_pnl = round(sum(row["actual_pnl"] for row in daily_rows), 4)
    estimated_total = round(sum(row["estimated_pnl"] for row in daily_rows), 4)
    actual_total = round(sum(row["actual_pnl"] for row in daily_rows), 4)
    summary = {
        "orders": sum(row["orders"] for row in daily_rows),
        "entry_fills": sum(row["entry_fills"] for row in daily_rows),
        "first_fills": sum(row["first_fills"] for row in daily_rows),
        "add_fills": sum(row["add_fills"] for row in daily_rows),
        "closes": len(closed_trades),
        "total_pnl": total_pnl,
        "actual_pnl": actual_total,
        "estimated_pnl": estimated_total,
        "win_rate": round(len(wins) / len(actual_records) * 100, 2) if actual_records else 0.0,
        "avg_pnl": round(total_pnl / len(actual_records), 4) if actual_records else 0.0,
    }
    return {
        "summary": summary,
        "daily": daily_rows,
        "trades": list(reversed(trades[-200:])),
        "events": list(reversed(events[-300:])),
    }


def _parse_history_log(path: Path, limit: int) -> dict:
    """Parse a log file into sampled chart points and summary stats."""
    paths = _selected_log_paths(path.name)
    signature = tuple(
        (log_path.name, log_path.stat().st_size, log_path.stat().st_mtime_ns)
        for log_path in paths
    )
    cache_key = (path.name, limit)
    cached = HISTORY_CACHE.get(cache_key)
    if cached:
        cached_at, cached_signature, cached_data = cached
        if cached_signature == signature or monotonic() - cached_at <= HISTORY_CACHE_TTL_SECONDS:
            return copy.deepcopy(cached_data)

    sampled = []
    total = 0
    sample_stride = 1
    start_ts = ""
    end_ts = ""
    min_price = None
    max_price = None
    first_equity = 0.0
    last_equity = 0.0
    position_ticks = 0

    def add_sample(point: dict) -> None:
        nonlocal sample_stride, sampled
        sampled.append(point)
        if len(sampled) > limit * 2:
            sample_stride *= 2
            sampled = sampled[::2]

    for log_path in paths:
        with log_path.open("r", encoding="utf-8", errors="ignore") as file:
            for line in file:
                match = TICK_RE.search(line)
                if not match:
                    continue
                point = {
                    "ts": match.group("ts")[:19],
                    "price": float(match.group("price")),
                    "lower": float(match.group("lower")),
                    "mid": float(match.group("mid")),
                    "upper": float(match.group("upper")),
                    "direction": match.group("direction"),
                    "equity": float(match.group("equity")),
                }
                total += 1
                if not start_ts:
                    start_ts = point["ts"]
                    first_equity = point["equity"]
                end_ts = point["ts"]
                last_equity = point["equity"]
                min_price = point["price"] if min_price is None else min(min_price, point["price"])
                max_price = point["price"] if max_price is None else max(max_price, point["price"])
                if point["direction"] != "none":
                    position_ticks += 1
                if total % sample_stride == 0:
                    add_sample(point)

    if len(sampled) > limit:
        step = max(1, len(sampled) // limit)
        sampled = sampled[::step][:limit]

    summary = {
        "file": "全部日志" if path.name == "__all__" else path.name,
        "total_ticks": total,
        "sampled_ticks": len(sampled),
        "start": start_ts,
        "end": end_ts,
        "min_price": min_price if min_price is not None else 0.0,
        "max_price": max_price if max_price is not None else 0.0,
        "first_equity": first_equity,
        "last_equity": last_equity,
        "position_ticks": position_ticks,
    }
    result = {"summary": summary, "points": sampled}
    HISTORY_CACHE[cache_key] = (monotonic(), signature, copy.deepcopy(result))
    return result


_DESIGN_HTML = """<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>BARS Strategy Dashboard</title>
<link rel="icon" type="image/png" href="/assets/bars-favicon.png">
<style>
  :root {
    --bg: #070a12;
    --panel: #101420;
    --panel-2: #151927;
    --panel-3: #0d111c;
    --line: #253044;
    --line-hot: #0bb7d6;
    --text: #f2f7ff;
    --muted: #8490a5;
    --cyan: #10d7ff;
    --cyan-soft: rgba(16, 215, 255, .16);
    --green: #21e68a;
    --red: #ff4f78;
    --yellow: #ffc21a;
    --blue: #5b8cff;
    --shadow: 0 18px 48px rgba(0, 0, 0, .38);
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    min-height: 100vh;
    background:
      radial-gradient(circle at 76% 18%, rgba(16, 215, 255, .10), transparent 28%),
      linear-gradient(135deg, #060914 0%, #090d18 50%, #050812 100%);
    color: var(--text);
    font-family: "Segoe UI", "Microsoft YaHei", Arial, sans-serif;
    font-size: 14px;
  }
  .shell { display: grid; grid-template-columns: 156px minmax(0, 1fr); min-height: 100vh; }
  .side {
    margin: 20px 0 20px 22px;
    border: 1px solid rgba(16, 215, 255, .35);
    border-radius: 26px;
    background: rgba(11, 15, 27, .82);
    box-shadow: var(--shadow), inset 0 0 32px rgba(16, 215, 255, .05);
    padding: 18px 10px;
  }
  .brand { display: flex; align-items: center; justify-content: center; padding: 0 10px 18px; }
  .brand-logo {
    width: 58px;
    height: 58px;
    object-fit: contain;
    filter: drop-shadow(0 0 18px rgba(16, 215, 255, .45));
  }
  .nav button {
    width: 100%;
    display: flex; align-items: center; gap: 10px;
    border: 0;
    color: var(--muted);
    background: transparent;
    border-radius: 0;
    padding: 14px 14px;
    cursor: pointer;
    text-align: left;
    font-weight: 650;
  }
  .nav button.active {
    color: #fff;
    background: linear-gradient(90deg, rgba(16, 215, 255, .25), rgba(16, 215, 255, .05));
    border-left: 4px solid var(--cyan);
    box-shadow: inset 0 0 24px rgba(16, 215, 255, .08);
  }
  .nav-icon { width: 22px; text-align: center; color: var(--cyan); }
  .main { min-width: 0; padding: 28px 34px 34px; }
  .topbar {
    display: grid;
    grid-template-columns: minmax(260px, 1fr) auto;
    gap: 16px;
    align-items: center;
    margin-bottom: 18px;
  }
  h1 { margin: 0; font-size: 24px; letter-spacing: 0; }
  .sub { margin-top: 6px; color: var(--muted); font-size: 12px; }
  .status-pill {
    display: flex; gap: 10px; align-items: center;
    border: 1px solid var(--line);
    background: rgba(15, 19, 31, .78);
    border-radius: 16px;
    padding: 12px 14px;
    color: var(--muted);
    min-width: 260px;
    justify-content: flex-end;
  }
  .dot { width: 10px; height: 10px; border-radius: 50%; background: var(--green); box-shadow: 0 0 16px var(--green); }
  .status-pill.stale .dot { background: var(--yellow); box-shadow: 0 0 16px var(--yellow); }
  .status-pill.offline .dot { background: var(--red); box-shadow: 0 0 16px var(--red); }
  .hidden { display: none; }
  .grid { display: grid; grid-template-columns: repeat(12, minmax(0, 1fr)); gap: 14px; }
  .span-3 { grid-column: span 3; }
  .span-4 { grid-column: span 4; }
  .span-5 { grid-column: span 5; }
  .span-6 { grid-column: span 6; }
  .span-7 { grid-column: span 7; }
  .span-8 { grid-column: span 8; }
  .span-12 { grid-column: 1 / -1; }
  .panel {
    min-width: 0;
    background: linear-gradient(180deg, rgba(18, 23, 36, .94), rgba(12, 16, 27, .94));
    border: 1px solid var(--line);
    border-radius: 12px;
    box-shadow: var(--shadow);
    padding: 18px;
  }
  .panel.hot { border-color: rgba(16, 215, 255, .45); box-shadow: 0 0 0 1px rgba(16, 215, 255, .10), var(--shadow); }
  .trade-strip {
    display: grid;
    grid-template-columns: repeat(8, minmax(0, 1fr));
    gap: 10px;
    padding: 14px;
    background: linear-gradient(180deg, rgba(15, 20, 32, .96), rgba(10, 14, 24, .96));
  }
  .trade-card {
    min-width: 0;
    border: 1px solid rgba(132,144,165,.20);
    border-radius: 10px;
    background: rgba(8, 12, 22, .72);
    padding: 12px;
  }
  .trade-card .label { display: block; font-size: 12px; margin-bottom: 8px; }
  .trade-card .value { display: block; text-align: left; font-size: 19px; white-space: normal; }
  .trade-card.primary { grid-column: span 2; }
  .trade-card.primary .value { font-size: 18px; white-space: nowrap; }
  .trade-card.primary { border-color: rgba(16,215,255,.38); background: rgba(16,215,255,.08); }
  .trade-card.attention { border-color: rgba(33,230,138,.28); }
  .trade-card.attention.warn { border-color: rgba(255,194,26,.44); background: rgba(255,194,26,.08); }
  .trade-card.attention.danger { border-color: rgba(255,79,120,.48); background: rgba(255,79,120,.08); }
  .panel h2, .section-title {
    margin: 0 0 14px;
    color: #dce8ff;
    font-size: 13px;
    letter-spacing: 0;
    font-weight: 800;
  }
  .eyebrow { color: var(--cyan); font-size: 12px; font-weight: 800; margin-bottom: 8px; }
  .metric { display: flex; justify-content: space-between; gap: 14px; padding: 9px 0; border-bottom: 1px solid rgba(255,255,255,.055); }
  .metric:last-child { border-bottom: 0; }
  .label { color: var(--muted); }
  .value { font-weight: 800; text-align: right; white-space: nowrap; font-variant-numeric: tabular-nums; }
  .hero-price { font-size: 42px; line-height: 1; font-weight: 900; color: #fff; text-shadow: 0 0 30px rgba(16, 215, 255, .34); }
  .green { color: var(--green); }
  .red { color: var(--red); }
  .yellow { color: var(--yellow); }
  .cyan { color: var(--cyan); }
  .muted { color: var(--muted); }
  .band {
    position: relative;
    height: 12px;
    background: #202635;
    border-radius: 999px;
    margin: 22px 2px 10px;
    overflow: visible;
  }
  .band .inside {
    height: 100%;
    border-radius: inherit;
    background: linear-gradient(90deg, var(--green), var(--yellow), var(--red));
    opacity: .9;
  }
  .band .marker {
    position: absolute;
    top: -7px;
    width: 26px;
    height: 26px;
    border-radius: 50%;
    background: var(--cyan);
    border: 5px solid var(--panel);
    transform: translateX(-50%);
    box-shadow: 0 0 20px rgba(16, 215, 255, .55);
  }
  .risk-ring {
    width: 190px;
    height: 190px;
    border-radius: 50%;
    margin: 12px auto 8px;
    display: grid;
    place-items: center;
    background:
      radial-gradient(circle at center, #101724 52%, transparent 54%),
      conic-gradient(var(--risk-color, var(--cyan)) var(--risk-pct, 70%), rgba(255,255,255,.08) 0);
    box-shadow: 0 0 34px rgba(16, 215, 255, .18);
  }
  .risk-ring strong { display: block; font-size: 42px; line-height: 1; text-align: center; }
  .risk-ring span { display: block; color: var(--cyan); margin-top: 6px; font-weight: 800; text-align: center; }
  .history-message {
    margin: 10px 0 0;
    min-height: 18px;
    color: var(--muted);
    font-size: 12px;
  }
  table { width: 100%; border-collapse: collapse; table-layout: auto; }
  th, td {
    padding: 10px 10px;
    border-bottom: 1px solid rgba(255,255,255,.06);
    text-align: left;
    vertical-align: middle;
    white-space: nowrap;
  }
  th { color: var(--muted); font-size: 12px; font-weight: 750; background: rgba(255,255,255,.025); }
  td:last-child, th:last-child { text-align: right; }
  .num { text-align: right; font-variant-numeric: tabular-nums; }
  .time-cell { min-width: 150px; }
  .note-cell { min-width: 180px; white-space: normal; color: var(--muted); }
  .badge {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 4px 9px;
    border-radius: 999px;
    font-size: 12px;
    background: rgba(132,144,165,.14);
    color: var(--muted);
  }
  .badge.fill { background: rgba(33,230,138,.14); color: var(--green); }
  .badge.wait { background: rgba(255,194,26,.15); color: var(--yellow); }
  .toolbar { display: flex; gap: 12px; align-items: center; justify-content: space-between; margin-bottom: 14px; }
  .toolbar-actions { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; justify-content: flex-end; }
  select, button {
    background: #111827;
    color: var(--text);
    border: 1px solid var(--line);
    border-radius: 10px;
    padding: 10px 12px;
    font: inherit;
  }
  button { cursor: pointer; font-weight: 800; }
  button:hover { border-color: var(--cyan); box-shadow: 0 0 0 3px rgba(16,215,255,.08); }
  .primary { background: linear-gradient(135deg, #10d7ff, #087a98); color: #031018; border-color: transparent; }
  .history-card { position: relative; }
  .chart-wrap { position: relative; }
  canvas {
    width: 100%;
    height: 560px;
    display: block;
    border: 1px solid rgba(16, 215, 255, .18);
    border-radius: 12px;
    background: #090d16;
  }
  .tooltip {
    position: absolute;
    pointer-events: none;
    min-width: 210px;
    padding: 10px 12px;
    border-radius: 10px;
    border: 1px solid rgba(16,215,255,.34);
    background: rgba(8, 12, 22, .94);
    box-shadow: 0 12px 34px rgba(0,0,0,.45);
    color: var(--text);
    font-size: 12px;
    display: none;
    z-index: 4;
  }
  .legend { display: flex; flex-wrap: wrap; gap: 12px; margin-top: 10px; color: var(--muted); font-size: 12px; }
  .legend span { display: inline-flex; align-items: center; gap: 6px; }
  .key { width: 18px; height: 3px; border-radius: 999px; background: var(--cyan); }
  .key.upper { background: var(--red); }
  .key.mid { background: var(--yellow); }
  .key.lower { background: var(--green); }
  .key.fill { background: var(--cyan); height: 10px; opacity: .35; }
  .history-stats { display: grid; grid-template-columns: repeat(8, minmax(0, 1fr)); gap: 10px; margin: 14px 0; }
  .mini {
    background: rgba(14, 19, 31, .88);
    border: 1px solid var(--line);
    border-radius: 10px;
    padding: 12px;
    min-height: 74px;
  }
  .mini .label { font-size: 12px; margin-bottom: 8px; }
  .mini .value { text-align: left; font-size: 18px; }
  .history-grid { display: grid; grid-template-columns: minmax(0, .95fr) minmax(0, 1.35fr); gap: 14px; margin-top: 14px; }
  .table-scroll { max-height: 360px; overflow: auto; border: 1px solid var(--line); border-radius: 10px; background: rgba(9,13,22,.68); }
  .table-scroll thead th { position: sticky; top: 0; z-index: 1; background: #111827; }
  @media (max-width: 1180px) {
    .shell { grid-template-columns: 1fr; }
    .side { margin: 12px; border-radius: 18px; }
    .brand { padding: 0 8px 10px; }
    .brand-logo { width: 44px; height: 44px; }
    .nav { display: flex; }
    .nav button { justify-content: center; }
    .main { padding: 12px; }
    .span-3, .span-4, .span-5, .span-6, .span-7, .span-8 { grid-column: 1 / -1; }
    .trade-strip { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    .trade-card.primary { grid-column: span 2; }
    .history-stats { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    .history-grid { grid-template-columns: 1fr; }
    .topbar { grid-template-columns: 1fr; }
  }
  @media (max-width: 680px) {
    .trade-strip { grid-template-columns: 1fr; }
    .trade-card.primary { grid-column: auto; }
    .status-pill { min-width: 0; justify-content: flex-start; }
    .toolbar { display: block; }
    .toolbar-actions { justify-content: flex-start; margin-top: 12px; }
    .toolbar-actions select { width: 100%; }
    .toolbar-actions button { width: 100%; }
    canvas { height: 360px; }
  }
</style>
</head>
<body>
<div class="shell">
  <aside class="side">
    <div class="brand"><img class="brand-logo" src="/assets/bars-favicon.png" alt="BARS logo"></div>
    <div class="nav">
      <button id="tab-live" class="active" onclick="showTab('live')"><span class="nav-icon">⌂</span>实时</button>
      <button id="tab-history" onclick="showTab('history')"><span class="nav-icon">▧</span>历史日志</button>
    </div>
  </aside>
  <main class="main">
    <div class="topbar">
      <div>
        <h1>BARS 策略看板</h1>
        <div class="sub">Bollinger Adaptive Reversion Strategy · ETH-USDT-SWAP</div>
      </div>
      <div class="status-pill offline" id="connection-status"><span class="dot"></span><span id="updated">等待策略数据</span></div>
    </div>

    <section id="live">
      <div class="grid">
        <div class="panel trade-strip span-12">
          <div class="trade-card attention" id="summary-attention-card">
            <span class="label">需要关注</span>
            <span class="value" id="summary-attention">--</span>
          </div>
          <div class="trade-card primary">
            <span class="label">当前持仓</span>
            <span class="value" id="summary-position">--</span>
          </div>
          <div class="trade-card">
            <span class="label">浮盈亏</span>
            <span class="value" id="summary-upnl">--</span>
          </div>
          <div class="trade-card">
            <span class="label">止盈距离</span>
            <span class="value" id="summary-tp-distance">--</span>
          </div>
          <div class="trade-card">
            <span class="label">强平缓冲</span>
            <span class="value" id="summary-liq-buffer">--</span>
          </div>
          <div class="trade-card">
            <span class="label">今日收益</span>
            <span class="value" id="summary-today-pnl">--</span>
          </div>
          <div class="trade-card">
            <span class="label">数据延迟</span>
            <span class="value" id="summary-data-age">--</span>
          </div>
        </div>
        <div class="panel hot span-5">
          <div class="eyebrow">MARKET</div>
          <div class="metric"><span class="label">标记价格</span><span class="value hero-price" id="mark-price">--</span></div>
          <div class="band"><div class="inside"></div><div class="marker" id="band-marker" style="left:50%"></div></div>
          <div class="metric"><span class="label">下轨 / 中轨 / 上轨</span><span class="value" id="bands">--</span></div>
          <div class="metric"><span class="label">布林宽度</span><span class="value cyan" id="band-width">--</span></div>
        </div>
        <div class="panel span-3">
          <h2>账户</h2>
          <div class="metric"><span class="label">权益</span><span class="value" id="equity">--</span></div>
          <div class="metric"><span class="label">峰值</span><span class="value" id="peak">--</span></div>
          <div class="metric"><span class="label">回撤</span><span class="value" id="drawdown">--</span></div>
          <div class="metric"><span class="label">今日收益</span><span class="value" id="today-pnl">--</span></div>
        </div>
        <div class="panel span-4">
          <h2>风险状态</h2>
          <div class="risk-ring" id="risk-ring"><div><strong id="risk-score">--</strong><span id="risk-label">等待数据</span></div></div>
          <div class="metric"><span class="label">强平价</span><span class="value red" id="liq-price">--</span></div>
        </div>
        <div class="panel span-4">
          <h2>持仓</h2>
          <div class="metric"><span class="label">方向</span><span class="value" id="direction">--</span></div>
          <div class="metric"><span class="label">均价</span><span class="value" id="avg-entry">--</span></div>
          <div class="metric"><span class="label">张数</span><span class="value" id="total-size">--</span></div>
          <div class="metric"><span class="label">浮盈亏</span><span class="value" id="upnl">--</span></div>
        </div>
        <div class="panel span-4">
          <h2>退出订单</h2>
          <div class="metric"><span class="label">止盈价</span><span class="value green" id="tp-price">--</span></div>
          <div class="metric"><span class="label">止损价</span><span class="value red" id="sl-price">--</span></div>
          <div class="metric"><span class="label">止损模式</span><span class="value" id="sl-mode">--</span></div>
          <div class="metric"><span class="label">强平线</span><span class="value red" id="liq-price-copy">--</span></div>
          <div class="metric"><span class="label">成交流水</span><span class="value" id="trade-count">--</span></div>
        </div>
        <div class="panel span-4">
          <h2>策略状态</h2>
          <div class="metric"><span class="label">布林位置</span><span class="value" id="band-zone">--</span></div>
          <div class="metric"><span class="label">持仓模式</span><span class="value" id="position-mode">--</span></div>
          <div class="metric"><span class="label">更新时间</span><span class="value" id="last-tick">--</span></div>
        </div>
        <div class="panel span-6">
          <h2>批次</h2>
          <div class="table-scroll"><table><thead><tr><th>批次</th><th>价格</th><th>张数</th><th>状态</th></tr></thead><tbody id="batch-body"></tbody></table></div>
        </div>
        <div class="panel span-6">
          <h2>最近成交</h2>
          <div class="table-scroll"><table><thead><tr><th>时间</th><th>操作</th><th>价格</th><th>张数</th><th>收益</th></tr></thead><tbody id="trade-body"></tbody></table></div>
        </div>
      </div>
    </section>

    <section id="history" class="hidden">
      <div class="panel history-card span-12">
        <div class="toolbar">
          <div>
            <div class="eyebrow">HISTORICAL REPLAY</div>
            <h2 style="margin-bottom:4px">历史日志复盘</h2>
            <div class="muted" id="history-range">选择日志后查看价格、布林带、进场、补仓和平仓</div>
          </div>
          <div class="toolbar-actions">
            <select id="log-select"></select>
            <select id="event-filter" onchange="renderHistoryFromCache()">
              <option value="all">全部事件</option>
              <option value="entry">只看开/补仓</option>
              <option value="close">只看平仓/划转</option>
              <option value="loss">只看亏损交易</option>
            </select>
            <button class="primary" id="history-load" onclick="loadHistory()">加载</button>
          </div>
        </div>
        <div class="history-message" id="history-message"></div>
        <div class="chart-wrap">
          <canvas id="history-chart"></canvas>
          <div class="tooltip" id="chart-tooltip"></div>
        </div>
        <div class="legend">
          <span><i class="key"></i>价格</span>
          <span><i class="key upper"></i>上轨</span>
          <span><i class="key mid"></i>中轨</span>
          <span><i class="key lower"></i>下轨</span>
          <span><i class="key fill"></i>布林带区域</span>
          <span class="green">● 头仓</span>
          <span class="yellow">● 补仓</span>
          <span class="red">◆ 平仓/划转</span>
        </div>
        <div class="history-stats" id="history-stats"></div>
      </div>
      <div class="history-grid">
        <div class="panel">
          <h2>单日实际收益</h2>
          <div class="table-scroll">
            <table>
              <thead><tr><th>日期</th><th>开单</th><th>头仓</th><th>补仓</th><th>平仓</th><th>实际收益</th><th>累计</th></tr></thead>
              <tbody id="daily-body"></tbody>
            </table>
          </div>
        </div>
        <div class="panel">
          <h2>单笔交易明细</h2>
          <div class="table-scroll">
            <table>
              <thead><tr><th>开仓时间</th><th>平仓时间</th><th>方向</th><th>均价</th><th>平仓价</th><th>张数</th><th>批次</th><th>实际收益</th><th>备注</th></tr></thead>
              <tbody id="history-trade-body"></tbody>
            </table>
          </div>
        </div>
      </div>
      <div class="panel span-12" style="margin-top:14px">
        <h2>动作流水</h2>
        <div class="table-scroll">
          <table>
            <thead><tr><th>时间</th><th>类型</th><th>方向</th><th>价格</th><th>张数</th><th>收益</th><th>说明</th></tr></thead>
            <tbody id="event-body"></tbody>
          </table>
        </div>
      </div>
    </section>
  </main>
</div>

<script>
const fmt = (v, d = 2) => {
  const n = Number(v);
  return Number.isFinite(n) ? n.toFixed(d) : '--';
};
const money = v => `${fmt(v)} USDT`;
const pnlText = v => `${Number(v || 0) >= 0 ? '+' : ''}${fmt(v)} USDT`;
const pnlClass = v => Number(v || 0) >= 0 ? 'green' : 'red';
const RISK_THRESHOLDS = { danger: 8, warning: 18, stale: 10, offline: 30 };
const escapeHtml = value => String(value ?? '').replace(/[&<>"']/g, ch => ({
  '&': '&amp;',
  '<': '&lt;',
  '>': '&gt;',
  '"': '&quot;',
  "'": '&#39;'
}[ch]));
let historyChartState = null;
let currentHistoryData = null;
let currentTradeData = null;

function showTab(name) {
  document.getElementById('live').classList.toggle('hidden', name !== 'live');
  document.getElementById('history').classList.toggle('hidden', name !== 'history');
  document.getElementById('tab-live').classList.toggle('active', name === 'live');
  document.getElementById('tab-history').classList.toggle('active', name === 'history');
  if (name === 'history') loadLogs();
}

function parseUpdatedAt(value) {
  if (!value) return null;
  const parsed = new Date(String(value).replace(' ', 'T'));
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function dataAgeSeconds(value) {
  const parsed = parseUpdatedAt(value);
  if (!parsed) return null;
  return Math.max(0, Math.round((Date.now() - parsed.getTime()) / 1000));
}

function setLiveStatus(status, text) {
  const pill = document.getElementById('connection-status');
  pill.className = `status-pill ${status}`;
  document.getElementById('updated').textContent = text;
}

function distanceText(from, to, direction, favorableForLong = true) {
  if (!from || !to) return '--';
  const raw = favorableForLong
    ? (direction === 'long' ? to - from : from - to)
    : (direction === 'long' ? from - to : to - from);
  const pct = raw / from * 100;
  return `${fmt(raw)} (${fmt(pct)}%)`;
}

function riskScore(d) {
  if (!d.total_sz || !d.liq_price || !d.mark_price) {
    return { score: '--', label: '空仓', pct: 0, color: 'rgba(132,144,165,.42)', level: 'muted' };
  }
  const side = d.direction;
  const distance = side === 'long'
    ? (d.mark_price - d.liq_price) / d.mark_price
    : (d.liq_price - d.mark_price) / d.mark_price;
  const pctValue = distance * 100;
  const color = pctValue < RISK_THRESHOLDS.danger ? 'var(--red)' : pctValue < RISK_THRESHOLDS.warning ? 'var(--yellow)' : 'var(--green)';
  return {
    score: fmt(pctValue, 2),
    label: pctValue < RISK_THRESHOLDS.danger ? '危险缓冲%' : pctValue < RISK_THRESHOLDS.warning ? '注意缓冲%' : '安全缓冲%',
    pct: Math.max(8, Math.min(92, pctValue * 5)),
    color,
    level: pctValue < RISK_THRESHOLDS.danger ? 'red' : pctValue < RISK_THRESHOLDS.warning ? 'yellow' : 'green'
  };
}

function updateAttention(d, age, risk) {
  const card = document.getElementById('summary-attention-card');
  const text = document.getElementById('summary-attention');
  const hasPosition = Number(d.total_sz || 0) > 0;
  let level = 'ok';
  let label = '正常巡航';
  if (!d.updated_at || age === null || age > RISK_THRESHOLDS.offline) {
    level = 'danger';
    label = '数据中断';
  } else if (age > RISK_THRESHOLDS.stale) {
    level = 'warn';
    label = '数据延迟';
  } else if (hasPosition && risk.level === 'red') {
    level = 'danger';
    label = '接近强平';
  } else if (hasPosition && !Number(d.tp_price || 0)) {
    level = 'warn';
    label = '止盈缺失';
  } else if (hasPosition && Number(d.unrealized_pnl || 0) < 0) {
    level = 'warn';
    label = '持仓浮亏';
  }
  card.className = `trade-card attention ${level === 'danger' ? 'danger' : level === 'warn' ? 'warn' : ''}`;
  text.textContent = label;
  text.className = `value ${level === 'danger' ? 'red' : level === 'warn' ? 'yellow' : 'green'}`;
}

function updateSummary(d, age) {
  const hasPosition = Number(d.total_sz || 0) > 0;
  const dirMap = {long: 'LONG 做多', short: 'SHORT 做空', none: '空仓'};
  const positionText = hasPosition
    ? `${dirMap[d.direction] || d.direction} · ${d.total_sz} 张`
    : '空仓，等待信号';
  const summaryPosition = document.getElementById('summary-position');
  summaryPosition.textContent = positionText;
  summaryPosition.className = `value ${hasPosition ? (d.direction === 'long' ? 'green' : 'red') : 'muted'}`;

  const summaryUpnl = document.getElementById('summary-upnl');
  summaryUpnl.textContent = hasPosition ? pnlText(d.unrealized_pnl) : '--';
  summaryUpnl.className = `value ${hasPosition ? pnlClass(d.unrealized_pnl) : 'muted'}`;

  document.getElementById('summary-tp-distance').textContent = hasPosition
    ? distanceText(d.mark_price, d.tp_price, d.direction, true)
    : '--';
  const r = riskScore(d);
  updateAttention(d, age, r);
  const liqBuffer = document.getElementById('summary-liq-buffer');
  liqBuffer.textContent = hasPosition ? `${r.score}%` : '--';
  liqBuffer.className = `value ${r.level}`;

  const summaryToday = document.getElementById('summary-today-pnl');
  summaryToday.textContent = pnlText(d.today_pnl);
  summaryToday.className = `value ${pnlClass(d.today_pnl)}`;
  document.getElementById('summary-data-age').textContent = age === null ? '--' : `${age} 秒`;
}

async function refreshLive() {
  let d;
  try {
    const res = await fetch('/api/state');
    if (!res.ok) throw new Error(`state ${res.status}`);
    d = await res.json();
  } catch (err) {
    setLiveStatus('offline', '无法连接策略数据');
    return;
  }
  const age = dataAgeSeconds(d.updated_at);
  if (!d.updated_at) {
    setLiveStatus('offline', '等待策略数据');
  } else if (age !== null && age > RISK_THRESHOLDS.offline) {
    setLiveStatus('offline', `数据中断 ${age} 秒`);
  } else if (age !== null && age > RISK_THRESHOLDS.stale) {
    setLiveStatus('stale', `数据延迟 ${age} 秒`);
  } else {
    setLiveStatus('', `更新于 ${d.updated_at}`);
  }
  updateSummary(d, age);
  document.getElementById('last-tick').textContent = d.updated_at || '--';
  document.getElementById('mark-price').textContent = money(d.mark_price);
  document.getElementById('bands').textContent = `${fmt(d.boll_lower)} / ${fmt(d.boll_mid)} / ${fmt(d.boll_upper)}`;
  const width = d.boll_upper - d.boll_lower;
  document.getElementById('band-width').textContent = width > 0 ? `${fmt(width)} (${fmt(width / d.mark_price * 100)}%)` : '--';
  const pct = width > 0 ? Math.max(0, Math.min(100, (d.mark_price - d.boll_lower) / width * 100)) : 50;
  document.getElementById('band-marker').style.left = `${pct}%`;
  document.getElementById('band-zone').textContent = pct >= 100 ? '上轨外' : pct <= 0 ? '下轨外' : `${fmt(pct, 1)}%`;

  document.getElementById('equity').textContent = money(d.equity);
  document.getElementById('peak').textContent = money(d.peak_equity);
  const dd = d.peak_equity > 0 ? (d.peak_equity - d.equity) / d.peak_equity * 100 : 0;
  const ddEl = document.getElementById('drawdown');
  ddEl.textContent = `${fmt(dd)}%`;
  ddEl.className = `value ${dd > 5 ? 'red' : 'green'}`;
  const pnlEl = document.getElementById('today-pnl');
  pnlEl.textContent = pnlText(d.today_pnl);
  pnlEl.className = `value ${pnlClass(d.today_pnl)}`;

  const dirMap = {long: ['LONG 做多', 'green'], short: ['SHORT 做空', 'red'], none: ['空仓', 'muted']};
  const dir = dirMap[d.direction] || dirMap.none;
  const dirEl = document.getElementById('direction');
  dirEl.textContent = dir[0];
  dirEl.className = `value ${dir[1]}`;
  document.getElementById('position-mode').textContent = d.total_sz > 0 ? '持仓监控' : '等待信号';
  document.getElementById('avg-entry').textContent = d.avg_entry > 0 ? fmt(d.avg_entry) : '--';
  document.getElementById('total-size').textContent = d.total_sz > 0 ? `${d.total_sz} 张` : '--';
  const upnlEl = document.getElementById('upnl');
  upnlEl.textContent = d.total_sz > 0 ? pnlText(d.unrealized_pnl) : '--';
  upnlEl.className = `value ${pnlClass(d.unrealized_pnl)}`;
  document.getElementById('tp-price').textContent = d.tp_price > 0 ? fmt(d.tp_price) : '--';
  document.getElementById('sl-price').textContent = d.sl_price > 0 ? fmt(d.sl_price) : '--';
  document.getElementById('sl-mode').textContent = d.sl_mode || '--';
  document.getElementById('liq-price').textContent = d.liq_price > 0 ? fmt(d.liq_price) : '--';
  document.getElementById('liq-price-copy').textContent = d.liq_price > 0 ? fmt(d.liq_price) : '--';
  document.getElementById('trade-count').textContent = `${d.total_trades || 0} 次`;
  const r = riskScore(d);
  document.getElementById('risk-score').textContent = r.score;
  document.getElementById('risk-label').textContent = r.label;
  document.getElementById('risk-ring').style.setProperty('--risk-pct', `${r.pct}%`);
  document.getElementById('risk-ring').style.setProperty('--risk-color', r.color);

  const batches = d.batches || [];
  document.getElementById('batch-body').innerHTML = batches.length ? batches.map(b => `
    <tr><td>第 ${Number(b.batch_idx || 0) + 1} 批</td><td class="num">${fmt(b.price)}</td><td class="num">${escapeHtml(b.sz)}</td>
    <td><span class="badge ${b.filled ? 'fill' : 'wait'}">${b.filled ? '已成交' : '挂单中'}</span></td></tr>
  `).join('') : '<tr><td colspan="4" class="muted">暂无批次</td></tr>';

  const trades = d.trade_history || [];
  document.getElementById('trade-body').innerHTML = trades.length ? trades.map(t => `
    <tr><td>${escapeHtml(t.time)}</td><td>${escapeHtml(t.action)}</td><td class="num">${fmt(t.price)}</td><td class="num">${escapeHtml(t.sz)}</td>
    <td class="num ${pnlClass(t.pnl)}">${t.pnl ? pnlText(t.pnl) : '--'}</td></tr>
  `).join('') : '<tr><td colspan="5" class="muted">暂无成交</td></tr>';
}

async function loadLogs() {
  const res = await fetch('/api/logs');
  const logs = await res.json();
  const select = document.getElementById('log-select');
  if (!select.options.length) {
    select.innerHTML = logs.map(log => `<option value="${escapeHtml(log.name)}">${escapeHtml(log.label || log.name)}</option>`).join('');
    const latest = logs.find(log => log.name !== '__all__');
    if (latest) select.value = latest.name;
    if (latest) loadHistory();
  }
}

async function loadHistory() {
  const select = document.getElementById('log-select');
  if (!select.value) return;
  const button = document.getElementById('history-load');
  const message = document.getElementById('history-message');
  const selectedLabel = select.options[select.selectedIndex]?.textContent || select.value;
  button.disabled = true;
  button.textContent = '加载中';
  message.textContent = select.value === '__all__'
    ? '正在解析全部日志，数据量较大，可能需要更久。'
    : `正在加载 ${selectedLabel}`;
  try {
    const res = await fetch(`/api/history?file=${encodeURIComponent(select.value)}&limit=1800`);
    if (!res.ok) throw new Error(`history ${res.status}`);
    const data = await res.json();
    const tradeRes = await fetch(`/api/history/trades?file=${encodeURIComponent(select.value)}`);
    if (!tradeRes.ok) throw new Error(`trades ${tradeRes.status}`);
    const tradeData = await tradeRes.json();
    currentHistoryData = data;
    currentTradeData = tradeData;
    renderHistoryStats(data.summary || {});
    renderHistoryTrades(tradeData || {});
    drawHistory(data.points || [], filteredEvents(tradeData.events || []));
    message.textContent = `已加载 ${selectedLabel}`;
  } catch (err) {
    message.textContent = '日志加载失败，请换一个单日日志重试。';
    drawHistory([], []);
  } finally {
    button.disabled = false;
    button.textContent = '加载';
  }
}

function renderHistoryFromCache() {
  if (!currentHistoryData || !currentTradeData) return;
  renderHistoryStats(currentHistoryData.summary || {});
  renderHistoryTrades(currentTradeData || {});
  drawHistory(currentHistoryData.points || [], filteredEvents(currentTradeData.events || []));
}

function renderHistoryStats(s) {
  document.getElementById('history-range').textContent = s.start ? `${s.file}: ${s.start} → ${s.end}` : '没有可解析数据';
  const items = [
    ['Tick', s.total_ticks || 0],
    ['采样点', s.sampled_ticks || 0],
    ['价格区间', `${fmt(s.min_price)} - ${fmt(s.max_price)}`],
  ];
  document.getElementById('history-stats').innerHTML = items.map(([label, value]) => `
    <div class="mini"><div class="label">${escapeHtml(label)}</div><div class="value">${escapeHtml(value)}</div></div>
  `).join('');
}

function eventTypeLabel(type) {
  const map = {
    signal: '信号',
    entry_order: '入场挂单',
    exit_order: '止盈挂单',
    close_summary: '平仓摘要',
    first_fill: '头仓成交',
    add_fill: '补仓成交',
    close: '平仓',
    capital_profit: '固本收益',
    capital_loss: '固本补亏'
  };
  return map[type] || type || '--';
}

function directionLabel(direction) {
  if (direction === 'long') return '多';
  if (direction === 'short') return '空';
  return direction || '--';
}

function cleanNote(e) {
  if (e.type === 'first_fill') return '头仓成交';
  if (e.type === 'add_fill') return '补仓成交';
  if (e.type === 'capital_profit') return '实际收益';
  if (e.type === 'capital_loss') return '亏损补充';
  if (e.type === 'close_summary') return '平仓摘要';
  if (e.type === 'entry_order') return '策略挂单';
  return e.note || '';
}

function historyFilterValue() {
  return document.getElementById('event-filter')?.value || 'all';
}

function filteredEvents(events) {
  const filter = historyFilterValue();
  if (filter === 'entry') return events.filter(e => ['first_fill', 'add_fill', 'entry_order', 'signal'].includes(e.type));
  if (filter === 'close') return events.filter(e => ['close', 'close_summary', 'exit_order', 'capital_profit', 'capital_loss'].includes(e.type));
  if (filter === 'loss') return events.filter(e => Number(e.pnl || 0) < 0);
  return events;
}

function renderHistoryTrades(data) {
  const summary = data.summary || {};
  const currentStats = document.getElementById('history-stats').innerHTML;
  const tradeStats = [
    ['实际收益', pnlText(summary.total_pnl || 0)],
    ['平仓次数', summary.closes || 0],
    ['胜率', `${fmt(summary.win_rate || 0)}%`],
    ['头仓/补仓', `${summary.first_fills || 0} / ${summary.add_fills || 0}`],
    ['平均实际', pnlText(summary.avg_pnl || 0)],
  ].map(([label, value]) => `
    <div class="mini"><div class="label">${escapeHtml(label)}</div><div class="value ${String(value).startsWith('-') ? 'red' : ''}">${escapeHtml(value)}</div></div>
  `).join('');
  document.getElementById('history-stats').innerHTML = currentStats + tradeStats;

  const daily = data.daily || [];
  document.getElementById('daily-body').innerHTML = daily.length ? daily.map(row => `
    <tr>
      <td>${escapeHtml(row.date)}</td>
      <td class="num">${row.orders}</td>
      <td class="num">${row.first_fills}</td>
      <td class="num">${row.add_fills}</td>
      <td class="num">${row.closes}</td>
      <td class="num ${pnlClass(row.actual_pnl)}">${pnlText(row.actual_pnl)}</td>
      <td class="num ${pnlClass(row.cum_pnl)}">${pnlText(row.cum_pnl)}</td>
    </tr>
  `).join('') : '<tr><td colspan="7" class="muted">暂无可解析交易</td></tr>';

  const filter = historyFilterValue();
  const trades = (data.trades || []).filter(t => filter !== 'loss' || Number(t.actual_pnl || t.pnl || 0) < 0);
  document.getElementById('history-trade-body').innerHTML = trades.length ? trades.map(t => {
    if (t.record_type === 'settlement') {
      return `
        <tr>
          <td class="time-cell">${escapeHtml(t.exit_time || '--')}</td>
          <td colspan="6" class="note-cell muted">跨日固本划转，当前日志缺少对应开仓和平仓明细</td>
          <td class="num ${pnlClass(t.actual_pnl || t.pnl)}">${pnlText(t.actual_pnl || t.pnl)}</td>
          <td class="note-cell">实际收益</td>
        </tr>
      `;
    }
    return `
      <tr>
        <td class="time-cell">${escapeHtml(t.entry_time || '--')}</td>
        <td class="time-cell">${escapeHtml(t.exit_time || '--')}</td>
        <td>${escapeHtml(directionLabel(t.direction))}</td>
        <td class="num">${fmt(t.avg_entry)}</td>
        <td class="num">${fmt(t.exit_price)}</td>
        <td class="num">${escapeHtml(t.sz || '--')}</td>
        <td class="num">${t.batches || 0}</td>
        <td class="num ${pnlClass(t.actual_pnl || t.pnl)}">${pnlText(t.actual_pnl || t.pnl)}</td>
        <td class="note-cell">${t.pnl_source === 'capital' ? '实际收益' : ''}</td>
      </tr>
    `;
  }).join('') : '<tr><td colspan="9" class="muted">暂无平仓交易</td></tr>';

  const events = filteredEvents(data.events || []);
  document.getElementById('event-body').innerHTML = events.length ? events.map(e => `
    <tr>
      <td>${escapeHtml(e.time)}</td>
      <td>${escapeHtml(eventTypeLabel(e.type))}</td>
      <td>${escapeHtml(directionLabel(e.direction))}</td>
      <td class="num">${e.price ? fmt(e.price) : '--'}</td>
      <td class="num">${escapeHtml(e.sz || '--')}</td>
      <td class="num ${pnlClass(e.pnl)}">${e.pnl ? pnlText(e.pnl) : '--'}</td>
      <td class="note-cell">${escapeHtml(cleanNote(e))}</td>
    </tr>
  `).join('') : '<tr><td colspan="7" class="muted">暂无动作流水</td></tr>';
}

function drawHistory(points, events = []) {
  const canvas = document.getElementById('history-chart');
  const rect = canvas.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  canvas.width = Math.max(280 * dpr, Math.floor(rect.width * dpr));
  canvas.height = Math.floor(rect.height * dpr);
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  const w = canvas.width / dpr;
  const h = canvas.height / dpr;
  ctx.clearRect(0, 0, w, h);
  const bg = ctx.createLinearGradient(0, 0, 0, h);
  bg.addColorStop(0, '#0b101c');
  bg.addColorStop(1, '#070b13');
  ctx.fillStyle = bg;
  ctx.fillRect(0, 0, w, h);
  if (!points.length) {
    ctx.fillStyle = '#8490a5';
    ctx.font = '14px "Microsoft YaHei", Segoe UI';
    ctx.fillText('没有历史数据', 28, 44);
    historyChartState = null;
    return;
  }

  const left = 70, right = 82, top = 34, bottom = 58;
  const plotW = w - left - right;
  const plotH = h - top - bottom;
  const all = points.flatMap(p => [p.price, p.lower, p.mid, p.upper]).filter(Number.isFinite);
  const min = Math.min(...all), max = Math.max(...all);
  const pad = Math.max((max - min) * 0.10, 1);
  const lo = min - pad, hi = max + pad;
  const x = i => left + i / Math.max(points.length - 1, 1) * plotW;
  const y = v => top + (hi - v) / (hi - lo) * plotH;

  ctx.strokeStyle = 'rgba(132,144,165,.22)';
  ctx.lineWidth = 1;
  ctx.font = '12px "Segoe UI", "Microsoft YaHei"';
  ctx.textBaseline = 'middle';
  for (let i = 0; i < 6; i++) {
    const yy = top + i * plotH / 5;
    ctx.beginPath(); ctx.moveTo(left, yy); ctx.lineTo(w - right, yy); ctx.stroke();
    const labelValue = hi - i * (hi - lo) / 5;
    ctx.fillStyle = '#8490a5';
    ctx.textAlign = 'right';
    ctx.fillText(fmt(labelValue), left - 12, yy);
  }
  for (let i = 0; i < 7; i++) {
    const xx = left + i * plotW / 6;
    ctx.strokeStyle = 'rgba(132,144,165,.10)';
    ctx.beginPath(); ctx.moveTo(xx, top); ctx.lineTo(xx, top + plotH); ctx.stroke();
  }

  ctx.beginPath();
  points.forEach((p, i) => i ? ctx.lineTo(x(i), y(p.upper)) : ctx.moveTo(x(i), y(p.upper)));
  for (let i = points.length - 1; i >= 0; i--) ctx.lineTo(x(i), y(points[i].lower));
  ctx.closePath();
  const fill = ctx.createLinearGradient(0, top, 0, top + plotH);
  fill.addColorStop(0, 'rgba(255,79,120,.10)');
  fill.addColorStop(.55, 'rgba(16,215,255,.06)');
  fill.addColorStop(1, 'rgba(33,230,138,.10)');
  ctx.fillStyle = fill;
  ctx.fill();

  function line(key, color, width = 1.5, glow = false) {
    ctx.save();
    ctx.strokeStyle = color;
    ctx.lineWidth = width;
    if (glow) { ctx.shadowColor = color; ctx.shadowBlur = 12; }
    ctx.beginPath();
    points.forEach((p, i) => i ? ctx.lineTo(x(i), y(p[key])) : ctx.moveTo(x(i), y(p[key])));
    ctx.stroke();
    ctx.restore();
  }
  line('upper', 'rgba(255,79,120,.80)', 1.2);
  line('mid', 'rgba(255,194,26,.78)', 1.1);
  line('lower', 'rgba(33,230,138,.80)', 1.2);
  line('price', '#10d7ff', 2.4, true);

  const pointTimes = points.map(p => new Date(p.ts.replace(' ', 'T')).getTime());
  function nearestIndex(ts) {
    const target = new Date(String(ts).replace(' ', 'T')).getTime();
    if (!Number.isFinite(target)) return -1;
    let best = -1, bestGap = Infinity;
    pointTimes.forEach((value, idx) => {
      const gap = Math.abs(value - target);
      if (gap < bestGap) { bestGap = gap; best = idx; }
    });
    return best;
  }

  const markerTypes = new Set(['first_fill', 'add_fill', 'close', 'capital_profit', 'capital_loss']);
  let addCount = 0;
  const markers = (events || []).filter(e => markerTypes.has(e.type)).map(e => {
    const idx = nearestIndex(e.time);
    if (idx < 0) return null;
    const isAdd = e.type === 'add_fill';
    const isFirst = e.type === 'first_fill';
    if (isAdd) addCount += 1;
    const style = isFirst
      ? { color: '#21e68a', label: '头', shape: 'circle' }
      : isAdd
        ? { color: '#ffc21a', label: `补${addCount}`, shape: 'circle' }
        : { color: '#ff4f78', label: Number(e.pnl || 0) >= 0 ? '盈' : '平', shape: 'diamond' };
    return { event: e, idx, x: x(idx), y: y(e.price || points[idx].price), style };
  }).filter(Boolean);

  markers.forEach(m => {
    const { x: xx, y: yy, style } = m;
    ctx.save();
    ctx.strokeStyle = style.color;
    ctx.fillStyle = '#080c16';
    ctx.lineWidth = 2;
    ctx.shadowColor = style.color;
    ctx.shadowBlur = 14;
    if (style.shape === 'diamond') {
      ctx.beginPath();
      ctx.moveTo(xx, yy - 8); ctx.lineTo(xx + 8, yy); ctx.lineTo(xx, yy + 8); ctx.lineTo(xx - 8, yy); ctx.closePath();
      ctx.fill(); ctx.stroke();
    } else {
      ctx.beginPath(); ctx.arc(xx, yy, 7, 0, Math.PI * 2); ctx.fill(); ctx.stroke();
    }
    ctx.shadowBlur = 0;
    ctx.font = 'bold 12px "Microsoft YaHei", Segoe UI';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'bottom';
    ctx.fillStyle = style.color;
    ctx.fillText(style.label, xx, yy - 12);
    ctx.restore();
  });

  ctx.fillStyle = '#8490a5';
  ctx.font = '12px Segoe UI';
  ctx.textAlign = 'left';
  ctx.textBaseline = 'alphabetic';
  ctx.fillText(points[0].ts, left, h - 18);
  ctx.textAlign = 'right';
  ctx.fillText(points[points.length - 1].ts, w - right, h - 18);

  historyChartState = { points, markers, x, y, left, right, top, bottom, w, h };
}

function attachChartTooltip() {
  const canvas = document.getElementById('history-chart');
  const tooltip = document.getElementById('chart-tooltip');
  canvas.addEventListener('mousemove', ev => {
    if (!historyChartState) return;
    const rect = canvas.getBoundingClientRect();
    const mx = ev.clientX - rect.left;
    const my = ev.clientY - rect.top;
    const { points, markers, left, right, w } = historyChartState;
    if (mx < left || mx > w - right) { tooltip.style.display = 'none'; return; }
    const idx = Math.max(0, Math.min(points.length - 1, Math.round((mx - left) / (w - left - right) * (points.length - 1))));
    const p = points[idx];
    const near = markers.find(m => Math.abs(m.x - mx) < 14 && Math.abs(m.y - my) < 20);
    const eventHtml = near ? `<div style="margin-top:8px;color:${near.style.color}">${escapeHtml(eventTypeLabel(near.event.type))} ${near.event.price ? fmt(near.event.price) : ''} ${near.event.pnl ? pnlText(near.event.pnl) : ''}</div>` : '';
    tooltip.innerHTML = `
      <div class="muted">${escapeHtml(p.ts)}</div>
      <div>价格 <b class="cyan">${fmt(p.price)}</b></div>
      <div>布林 <span class="green">${fmt(p.lower)}</span> / <span class="yellow">${fmt(p.mid)}</span> / <span class="red">${fmt(p.upper)}</span></div>
      <div>宽度 <b>${fmt(p.upper - p.lower)}</b></div>
      ${eventHtml}
    `;
    tooltip.style.left = `${Math.min(rect.width - 230, mx + 16)}px`;
    tooltip.style.top = `${Math.max(12, my - 30)}px`;
    tooltip.style.display = 'block';
  });
  canvas.addEventListener('mouseleave', () => { tooltip.style.display = 'none'; });
}

attachChartTooltip();
refreshLive();
setInterval(refreshLive, 3000);
window.addEventListener('resize', () => {
  if (historyChartState) drawHistory(historyChartState.points, historyChartState.markers.map(m => m.event));
});
</script>
</body>
</html>"""


async def _handle_index(request):
    """Return the dashboard HTML page."""
    return web.Response(text=_DESIGN_HTML, content_type="text/html", charset="utf-8")


async def _handle_asset(request):
    """Return whitelisted dashboard image assets."""
    filename = Path(request.match_info["filename"]).name
    if filename not in {"bars-logo-transparent.png", "bars-favicon.png"}:
        raise web.HTTPNotFound()
    path = ASSET_DIR / filename
    if not path.exists():
        raise web.HTTPNotFound()
    return web.FileResponse(path)


async def _handle_state(request):
    """Return the current dashboard state as JSON."""
    data = asdict(state)
    return web.Response(text=json.dumps(data, ensure_ascii=False), content_type="application/json")


async def _handle_logs(request):
    """Return available historical log files."""
    logs = _list_log_files()
    if logs:
        total_size = sum(item["size"] for item in logs)
        logs.append({
            "name": "__all__",
            "size": total_size,
            "modified": logs[0]["modified"],
            "label": "全部日志",
        })
    return web.json_response(logs)


async def _handle_history(request):
    """Return sampled historical log data."""
    raw_file = request.query.get("file", "")
    filename = "__all__" if raw_file == "__all__" else Path(raw_file).name
    limit = int(request.query.get("limit", "1200"))
    data = _parse_history_log(LOG_DIR / filename, max(100, min(limit, 5000)))
    return web.json_response(data)


async def _handle_history_trades(request):
    """Return parsed daily trade and PnL statistics from historical logs."""
    raw_file = request.query.get("file", "")
    filename = "__all__" if raw_file == "__all__" else Path(raw_file).name
    data = _parse_trade_history(_selected_log_paths(filename))
    data["file"] = "全部日志" if filename == "__all__" else filename
    return web.json_response(data)


def create_dashboard_app() -> web.Application:
    """Create the local dashboard aiohttp app."""
    app = web.Application()
    app.router.add_get("/", _handle_index)
    app.router.add_get("/assets/{filename}", _handle_asset)
    app.router.add_get("/api/state", _handle_state)
    app.router.add_get("/api/logs", _handle_logs)
    app.router.add_get("/api/history", _handle_history)
    app.router.add_get("/api/history/trades", _handle_history_trades)
    return app


async def start_dashboard():
    """Start the dashboard server in the current event loop."""
    app = create_dashboard_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, WEB_HOST, WEB_PORT)
    await site.start()
    logger.info(f"看板已启动 → http://localhost:{WEB_PORT}")
