"""Local aiohttp dashboard for live strategy state and historical logs."""
import json
import re
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import List

from aiohttp import web
from loguru import logger

from src.config import WEB_HOST, WEB_PORT


LOG_DIR = Path("logs")
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


def _parse_trade_history(paths: list[Path]) -> dict:
    """Parse strategy-owned entries, closes, and realized PnL from logs."""
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

                close_summary_match = CLOSE_SUMMARY_RE.search(line)
                if close_summary_match:
                    current["close_summary"] = {
                        "time": ts,
                        "direction": close_summary_match.group("direction"),
                        "avg_entry": float(close_summary_match.group("avg")),
                        "exit_price": float(close_summary_match.group("exit")),
                        "sz": float(close_summary_match.group("sz")),
                        "estimated_pnl": float(close_summary_match.group("pnl")),
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
                    row = day_row(ts)
                    if last_closed_trade is None or _date_key(last_closed_trade.get("exit_time", "")) != _date_key(ts):
                        row["closes"] += 1
                        last_closed_trade = {
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
                        }
                        trades.append(last_closed_trade)
                    row["actual_pnl"] += actual_pnl
                    row["source"] = "capital"
                    if last_closed_trade is not None and not last_closed_trade.get("actual_locked"):
                        last_closed_trade["actual_pnl"] = round(actual_pnl, 4)
                        last_closed_trade["pnl"] = round(actual_pnl, 4)
                        last_closed_trade["pnl_source"] = "capital"
                        last_closed_trade["actual_locked"] = True
                    events.append({
                        "time": ts,
                        "type": "capital_profit" if actual_pnl >= 0 else "capital_loss",
                        "direction": "",
                        "price": 0.0,
                        "sz": 0.0,
                        "pnl": round(actual_pnl, 4),
                        "note": "固本实际收益",
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
                    if not fills and not current.get("close_summary") and current["direction"] == "none":
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
                    row = day_row(ts)
                    row["closes"] += 1
                    row["estimated_pnl"] += estimated_pnl
                    trade = {
                        "entry_time": current["entry_time"],
                        "exit_time": ts,
                        "direction": current["direction"],
                        "avg_entry": round(avg_entry, 4),
                        "exit_price": round(exit_price, 4),
                        "sz": round(total_sz, 8),
                        "batches": len(fills),
                        "estimated_pnl": round(estimated_pnl, 4),
                        "actual_pnl": 0.0,
                        "pnl": round(estimated_pnl, 4),
                        "pnl_source": "estimate",
                    }
                    trades.append(trade)
                    last_closed_trade = trade
                    events.append({
                        "time": ts,
                        "type": "close",
                        "direction": current["direction"],
                        "price": round(exit_price, 4),
                        "sz": round(total_sz, 8),
                        "pnl": round(estimated_pnl, 4),
                        "note": "平仓",
                    })
                    current = {"direction": "none", "fills": [], "entry_time": "", "tp_price": 0.0, "close_summary": None}

    cum = 0.0
    daily_rows = []
    for day in sorted(daily):
        row = daily[day]
        row["actual_pnl"] = round(row["actual_pnl"], 4)
        row["estimated_pnl"] = round(row["estimated_pnl"], 4)
        row["pnl"] = row["actual_pnl"] if row["source"] == "capital" else row["estimated_pnl"]
        row["pnl"] = round(row["pnl"], 4)
        cum += row["pnl"]
        row["cum_pnl"] = round(cum, 4)
        daily_rows.append(row)

    for trade in trades:
        trade.pop("actual_locked", None)
    wins = [trade for trade in trades if trade["pnl"] > 0]
    total_pnl = round(sum(row["pnl"] for row in daily_rows), 4)
    estimated_total = round(sum(row["estimated_pnl"] for row in daily_rows), 4)
    actual_total = round(sum(row["actual_pnl"] for row in daily_rows), 4)
    summary = {
        "orders": sum(row["orders"] for row in daily_rows),
        "entry_fills": sum(row["entry_fills"] for row in daily_rows),
        "first_fills": sum(row["first_fills"] for row in daily_rows),
        "add_fills": sum(row["add_fills"] for row in daily_rows),
        "closes": len(trades),
        "total_pnl": total_pnl,
        "actual_pnl": actual_total,
        "estimated_pnl": estimated_total,
        "win_rate": round(len(wins) / len(trades) * 100, 2) if trades else 0.0,
        "avg_pnl": round(total_pnl / len(trades), 4) if trades else 0.0,
    }
    return {
        "summary": summary,
        "daily": daily_rows,
        "trades": list(reversed(trades[-200:])),
        "events": list(reversed(events[-300:])),
    }


def _parse_history_log(path: Path, limit: int) -> dict:
    """Parse a log file into sampled chart points and summary stats."""
    points = []
    paths = _selected_log_paths(path.name)

    for log_path in paths:
        with log_path.open("r", encoding="utf-8", errors="ignore") as file:
            for line in file:
                match = TICK_RE.search(line)
                if not match:
                    continue
                points.append({
                    "ts": match.group("ts")[:19],
                    "price": float(match.group("price")),
                    "lower": float(match.group("lower")),
                    "mid": float(match.group("mid")),
                    "upper": float(match.group("upper")),
                    "direction": match.group("direction"),
                    "equity": float(match.group("equity")),
                })

    total = len(points)
    if total > limit:
        step = max(1, total // limit)
        sampled = points[::step][:limit]
    else:
        sampled = points

    prices = [p["price"] for p in points]
    equities = [p["equity"] for p in points]
    summary = {
        "file": "全部日志" if path.name == "__all__" else path.name,
        "total_ticks": total,
        "sampled_ticks": len(sampled),
        "start": points[0]["ts"] if points else "",
        "end": points[-1]["ts"] if points else "",
        "min_price": min(prices) if prices else 0.0,
        "max_price": max(prices) if prices else 0.0,
        "first_equity": equities[0] if equities else 0.0,
        "last_equity": equities[-1] if equities else 0.0,
        "position_ticks": sum(1 for p in points if p["direction"] != "none"),
    }
    return {"summary": summary, "points": sampled}


_HTML = """<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>OKX Strategy Dashboard</title>
<style>
  :root {
    --bg: #0b0e11;
    --panel: #12171d;
    --panel-2: #171d24;
    --line: #26313d;
    --text: #e8eef5;
    --muted: #8a97a6;
    --green: #27c483;
    --red: #ff5f66;
    --yellow: #e5b454;
    --blue: #5aa7ff;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    background: var(--bg);
    color: var(--text);
    font-family: "Segoe UI", "Microsoft YaHei", Arial, sans-serif;
    font-size: 14px;
  }
  header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 16px;
    padding: 18px 24px;
    border-bottom: 1px solid var(--line);
    background: #0f1419;
  }
  h1 { margin: 0; font-size: 18px; font-weight: 650; letter-spacing: 0; }
  .sub { color: var(--muted); font-size: 12px; margin-top: 4px; }
  .status { display: flex; gap: 8px; align-items: center; color: var(--muted); }
  .dot { width: 9px; height: 9px; border-radius: 50%; background: var(--green); box-shadow: 0 0 10px var(--green); }
  main { padding: 18px 24px 28px; max-width: 1500px; margin: 0 auto; }
  .tabs { display: inline-flex; border: 1px solid var(--line); background: var(--panel); margin-bottom: 16px; }
  .tabs button {
    min-width: 96px;
    border: 0;
    color: var(--muted);
    background: transparent;
    padding: 9px 14px;
    cursor: pointer;
  }
  .tabs button.active { background: var(--blue); color: #06111f; font-weight: 700; }
  .grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; }
  .wide { grid-column: span 2; }
  .full { grid-column: 1 / -1; }
  .panel {
    background: var(--panel);
    border: 1px solid var(--line);
    border-radius: 6px;
    padding: 14px;
    min-width: 0;
  }
  .panel h2 { margin: 0 0 12px; font-size: 12px; color: var(--muted); font-weight: 650; text-transform: uppercase; }
  .metric { display: flex; justify-content: space-between; gap: 12px; padding: 7px 0; border-bottom: 1px solid rgba(255,255,255,.04); }
  .metric:last-child { border-bottom: 0; }
  .label { color: var(--muted); }
  .value { font-weight: 650; text-align: right; white-space: nowrap; }
  .big { font-size: 26px; line-height: 1.1; }
  .green { color: var(--green); }
  .red { color: var(--red); }
  .yellow { color: var(--yellow); }
  .muted { color: var(--muted); }
  .band {
    position: relative;
    height: 10px;
    background: #202832;
    border-radius: 999px;
    margin: 14px 2px 4px;
  }
  .band .inside {
    height: 100%;
    border-radius: inherit;
    background: linear-gradient(90deg, var(--red), var(--yellow), var(--green));
    opacity: .85;
  }
  .band .marker {
    position: absolute;
    top: -5px;
    width: 20px;
    height: 20px;
    border-radius: 50%;
    background: var(--blue);
    border: 3px solid var(--panel);
    transform: translateX(-50%);
  }
  table { width: 100%; border-collapse: collapse; }
  th, td { padding: 9px 8px; border-bottom: 1px solid rgba(255,255,255,.06); text-align: left; }
  th { color: var(--muted); font-size: 12px; font-weight: 600; }
  td:last-child, th:last-child { text-align: right; }
  .badge { display: inline-block; padding: 3px 8px; border-radius: 999px; font-size: 12px; background: #24303b; color: var(--muted); }
  .badge.fill { background: rgba(39,196,131,.14); color: var(--green); }
  .badge.wait { background: rgba(229,180,84,.14); color: var(--yellow); }
  .toolbar { display: flex; gap: 10px; align-items: center; justify-content: space-between; margin-bottom: 12px; }
  select, button {
    background: var(--panel-2);
    color: var(--text);
    border: 1px solid var(--line);
    border-radius: 4px;
    padding: 8px 10px;
  }
  button { cursor: pointer; }
  button:hover { border-color: var(--blue); }
  canvas { width: 100%; height: 360px; background: #0e1318; border: 1px solid var(--line); border-radius: 6px; display: block; }
  .history-stats { display: grid; grid-template-columns: repeat(5, 1fr); gap: 10px; margin-top: 10px; }
  .mini { background: var(--panel-2); border: 1px solid var(--line); border-radius: 5px; padding: 10px; }
  .mini .label { font-size: 12px; margin-bottom: 5px; }
  .mini .value { text-align: left; }
  .history-grid { display: grid; grid-template-columns: 1fr 1.35fr; gap: 12px; margin-top: 12px; }
  .table-scroll { max-height: 340px; overflow: auto; border: 1px solid var(--line); border-radius: 6px; }
  .table-scroll table { background: var(--panel); }
  .table-scroll thead th { position: sticky; top: 0; background: var(--panel-2); z-index: 1; }
  .hidden { display: none; }
  @media (max-width: 980px) {
    header { align-items: flex-start; flex-direction: column; }
    .grid { grid-template-columns: 1fr; }
    .wide { grid-column: auto; }
    .history-stats { grid-template-columns: 1fr 1fr; }
    .history-grid { grid-template-columns: 1fr; }
  }
</style>
</head>
<body>
<header>
  <div>
    <h1>OKX ETH-USDT-SWAP Strategy</h1>
    <div class="sub">Live monitor and historical log playback</div>
  </div>
  <div class="status"><span class="dot"></span><span id="updated">waiting for data</span></div>
</header>
<main>
  <div class="tabs">
    <button id="tab-live" class="active" onclick="showTab('live')">实时</button>
    <button id="tab-history" onclick="showTab('history')">历史日志</button>
  </div>

  <section id="live">
    <div class="grid">
      <div class="panel wide">
        <h2>Market</h2>
        <div class="metric"><span class="label">标记价格</span><span class="value big" id="mark-price">--</span></div>
        <div class="band"><div class="inside"></div><div class="marker" id="band-marker" style="left:50%"></div></div>
        <div class="metric"><span class="label">下轨 / 中轨 / 上轨</span><span class="value" id="bands">--</span></div>
        <div class="metric"><span class="label">布林宽度</span><span class="value" id="band-width">--</span></div>
      </div>
      <div class="panel">
        <h2>Account</h2>
        <div class="metric"><span class="label">权益</span><span class="value" id="equity">--</span></div>
        <div class="metric"><span class="label">峰值</span><span class="value" id="peak">--</span></div>
        <div class="metric"><span class="label">回撤</span><span class="value" id="drawdown">--</span></div>
        <div class="metric"><span class="label">今日盈亏</span><span class="value" id="today-pnl">--</span></div>
      </div>
      <div class="panel">
        <h2>Position</h2>
        <div class="metric"><span class="label">方向</span><span class="value" id="direction">--</span></div>
        <div class="metric"><span class="label">均价</span><span class="value" id="avg-entry">--</span></div>
        <div class="metric"><span class="label">张数</span><span class="value" id="total-size">--</span></div>
        <div class="metric"><span class="label">浮盈亏</span><span class="value" id="upnl">--</span></div>
      </div>
      <div class="panel wide">
        <h2>Exit Orders</h2>
        <div class="metric"><span class="label">止盈价</span><span class="value green" id="tp-price">--</span></div>
        <div class="metric"><span class="label">强平线</span><span class="value red" id="liq-price">--</span></div>
        <div class="metric"><span class="label">总成交次数</span><span class="value" id="trade-count">--</span></div>
      </div>
      <div class="panel wide">
        <h2>Batches</h2>
        <table><thead><tr><th>批次</th><th>价格</th><th>张数</th><th>状态</th></tr></thead><tbody id="batch-body"></tbody></table>
      </div>
      <div class="panel wide">
        <h2>Recent Trades</h2>
        <table><thead><tr><th>时间</th><th>操作</th><th>价格</th><th>张数</th><th>盈亏</th></tr></thead><tbody id="trade-body"></tbody></table>
      </div>
    </div>
  </section>

  <section id="history" class="hidden">
    <div class="panel full">
      <div class="toolbar">
        <div>
          <h2 style="margin-bottom:4px">Historical Logs</h2>
          <div class="muted" id="history-range">选择一个日志文件查看价格和布林带变化</div>
        </div>
        <div>
          <select id="log-select"></select>
          <button onclick="loadHistory()">加载</button>
        </div>
      </div>
      <canvas id="history-chart" width="1200" height="360"></canvas>
      <div class="history-stats" id="history-stats"></div>
      <div class="history-grid">
        <div>
          <h2 style="margin:14px 0 8px">单日统计</h2>
          <div class="table-scroll">
            <table>
              <thead><tr><th>日期</th><th>开单</th><th>头仓</th><th>补仓</th><th>平仓</th><th>实际收益</th><th>估算收益</th><th>累计</th><th>来源</th></tr></thead>
              <tbody id="daily-body"></tbody>
            </table>
          </div>
        </div>
        <div>
          <h2 style="margin:14px 0 8px">交易明细</h2>
          <div class="table-scroll">
            <table>
              <thead><tr><th>开仓时间</th><th>平仓时间</th><th>方向</th><th>均价</th><th>平仓价</th><th>张数</th><th>批次</th><th>实际收益</th><th>估算收益</th><th>来源</th></tr></thead>
              <tbody id="history-trade-body"></tbody>
            </table>
          </div>
        </div>
      </div>
      <h2 style="margin:14px 0 8px">动作流水</h2>
      <div class="table-scroll">
        <table>
          <thead><tr><th>时间</th><th>类型</th><th>方向</th><th>价格</th><th>张数</th><th>收益</th><th>说明</th></tr></thead>
          <tbody id="event-body"></tbody>
        </table>
      </div>
    </div>
  </section>
</main>

<script>
const fmt = (v, d = 2) => Number(v || 0).toFixed(d);
const money = v => `${fmt(v)} USDT`;
const pnlText = v => `${v >= 0 ? '+' : ''}${fmt(v)} USDT`;
const pnlClass = v => v >= 0 ? 'green' : 'red';

function showTab(name) {
  document.getElementById('live').classList.toggle('hidden', name !== 'live');
  document.getElementById('history').classList.toggle('hidden', name !== 'history');
  document.getElementById('tab-live').classList.toggle('active', name === 'live');
  document.getElementById('tab-history').classList.toggle('active', name === 'history');
  if (name === 'history') loadLogs();
}

async function refreshLive() {
  const res = await fetch('/api/state');
  const d = await res.json();
  document.getElementById('updated').textContent = d.updated_at ? `更新于 ${d.updated_at}` : '等待策略数据';
  document.getElementById('mark-price').textContent = money(d.mark_price);
  document.getElementById('bands').textContent = `${fmt(d.boll_lower)} / ${fmt(d.boll_mid)} / ${fmt(d.boll_upper)}`;
  const width = d.boll_upper - d.boll_lower;
  document.getElementById('band-width').textContent = width > 0 ? `${fmt(width)} (${fmt(width / d.mark_price * 100)}%)` : '--';
  const pct = width > 0 ? Math.max(0, Math.min(100, (d.mark_price - d.boll_lower) / width * 100)) : 50;
  document.getElementById('band-marker').style.left = `${pct}%`;

  document.getElementById('equity').textContent = money(d.equity);
  document.getElementById('peak').textContent = money(d.peak_equity);
  const dd = d.peak_equity > 0 ? (d.peak_equity - d.equity) / d.peak_equity * 100 : 0;
  const ddEl = document.getElementById('drawdown');
  ddEl.textContent = `${fmt(dd)}%`;
  ddEl.className = `value ${dd > 5 ? 'red' : 'green'}`;
  const pnlEl = document.getElementById('today-pnl');
  pnlEl.textContent = pnlText(d.today_pnl);
  pnlEl.className = `value ${pnlClass(d.today_pnl)}`;

  const dirEl = document.getElementById('direction');
  const dirMap = {long: ['LONG 做多', 'green'], short: ['SHORT 做空', 'red'], none: ['空仓', 'muted']};
  const dir = dirMap[d.direction] || dirMap.none;
  dirEl.textContent = dir[0];
  dirEl.className = `value ${dir[1]}`;
  document.getElementById('avg-entry').textContent = d.avg_entry > 0 ? fmt(d.avg_entry) : '--';
  document.getElementById('total-size').textContent = d.total_sz > 0 ? `${d.total_sz} 张` : '--';
  const upnlEl = document.getElementById('upnl');
  upnlEl.textContent = d.total_sz > 0 ? pnlText(d.unrealized_pnl) : '--';
  upnlEl.className = `value ${pnlClass(d.unrealized_pnl)}`;
  document.getElementById('tp-price').textContent = d.tp_price > 0 ? fmt(d.tp_price) : '--';
  document.getElementById('liq-price').textContent = d.liq_price > 0 ? fmt(d.liq_price) : '--';
  document.getElementById('trade-count').textContent = `${d.total_trades} 次`;

  const batches = d.batches || [];
  document.getElementById('batch-body').innerHTML = batches.length ? batches.map(b => `
    <tr><td>第 ${b.batch_idx + 1} 批</td><td>${fmt(b.price)}</td><td>${b.sz}</td>
    <td><span class="badge ${b.filled ? 'fill' : 'wait'}">${b.filled ? '已成交' : '挂单中'}</span></td></tr>
  `).join('') : '<tr><td colspan="4" class="muted">暂无批次</td></tr>';

  const trades = d.trade_history || [];
  document.getElementById('trade-body').innerHTML = trades.length ? trades.map(t => `
    <tr><td>${t.time}</td><td>${t.action}</td><td>${fmt(t.price)}</td><td>${t.sz}</td>
    <td class="${pnlClass(t.pnl)}">${t.pnl ? pnlText(t.pnl) : '--'}</td></tr>
  `).join('') : '<tr><td colspan="5" class="muted">暂无成交</td></tr>';
}

async function loadLogs() {
  const res = await fetch('/api/logs');
  const logs = await res.json();
  const select = document.getElementById('log-select');
  if (!select.options.length) {
    select.innerHTML = logs.map(log => `<option value="${log.name}">${log.label || log.name}</option>`).join('');
    if (logs.length) loadHistory();
  }
}

async function loadHistory() {
  const select = document.getElementById('log-select');
  if (!select.value) return;
  const res = await fetch(`/api/history?file=${encodeURIComponent(select.value)}&limit=1200`);
  const data = await res.json();
  const tradeRes = await fetch(`/api/history/trades?file=${encodeURIComponent(select.value)}`);
  const tradeData = await tradeRes.json();
  drawHistory(data.points || [], tradeData.events || []);
  renderHistoryStats(data.summary || {});
  renderHistoryTrades(tradeData || {});
}

function renderHistoryStats(s) {
  document.getElementById('history-range').textContent = s.start ? `${s.file}: ${s.start} -> ${s.end}` : '没有可解析数据';
  const items = [
    ['Tick', s.total_ticks || 0],
    ['价格区间', `${fmt(s.min_price)} - ${fmt(s.max_price)}`],
    ['权益变化', `${fmt(s.first_equity)} -> ${fmt(s.last_equity)}`],
    ['持仓占比', s.total_ticks ? `${fmt(s.position_ticks / s.total_ticks * 100)}%` : '--'],
    ['采样点', s.sampled_ticks || 0],
  ];
  document.getElementById('history-stats').innerHTML = items.map(([label, value]) => `
    <div class="mini"><div class="label">${label}</div><div class="value">${value}</div></div>
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

function renderHistoryTrades(data) {
  const summary = data.summary || {};
  const currentStats = document.getElementById('history-stats').innerHTML;
  const tradeStats = [
    ['实际收益', pnlText(summary.total_pnl || 0)],
    ['估算收益', pnlText(summary.estimated_pnl || 0)],
    ['平仓次数', summary.closes || 0],
    ['胜率', `${fmt(summary.win_rate || 0)}%`],
    ['头仓/补仓', `${summary.first_fills || 0} / ${summary.add_fills || 0}`],
    ['平均实际', pnlText(summary.avg_pnl || 0)],
  ].map(([label, value]) => `
    <div class="mini"><div class="label">${label}</div><div class="value">${value}</div></div>
  `).join('');
  document.getElementById('history-stats').innerHTML = currentStats + tradeStats;

  const daily = data.daily || [];
  document.getElementById('daily-body').innerHTML = daily.length ? daily.map(row => `
    <tr>
      <td>${row.date}</td>
      <td>${row.orders}</td>
      <td>${row.first_fills}</td>
      <td>${row.add_fills}</td>
      <td>${row.closes}</td>
      <td class="${pnlClass(row.pnl)}">${pnlText(row.pnl)}</td>
      <td class="${pnlClass(row.estimated_pnl)}">${pnlText(row.estimated_pnl)}</td>
      <td class="${pnlClass(row.cum_pnl)}">${pnlText(row.cum_pnl)}</td>
      <td>${row.source === 'capital' ? '固本划转' : '估算'}</td>
    </tr>
  `).join('') : '<tr><td colspan="9" class="muted">暂无可解析交易</td></tr>';

  const trades = data.trades || [];
  document.getElementById('history-trade-body').innerHTML = trades.length ? trades.map(t => `
    <tr>
      <td>${t.entry_time || '--'}</td>
      <td>${t.exit_time || '--'}</td>
      <td>${directionLabel(t.direction)}</td>
      <td>${fmt(t.avg_entry)}</td>
      <td>${fmt(t.exit_price)}</td>
      <td>${t.sz || '--'}</td>
      <td>${t.batches || 0}</td>
      <td class="${pnlClass(t.pnl)}">${pnlText(t.pnl)}</td>
      <td class="${pnlClass(t.estimated_pnl)}">${pnlText(t.estimated_pnl || 0)}</td>
      <td>${t.pnl_source === 'capital' ? '固本划转' : '估算'}</td>
    </tr>
  `).join('') : '<tr><td colspan="10" class="muted">暂无平仓交易</td></tr>';

  const events = data.events || [];
  document.getElementById('event-body').innerHTML = events.length ? events.map(e => `
    <tr>
      <td>${e.time}</td>
      <td>${eventTypeLabel(e.type)}</td>
      <td>${directionLabel(e.direction)}</td>
      <td>${e.price ? fmt(e.price) : '--'}</td>
      <td>${e.sz || '--'}</td>
      <td class="${pnlClass(e.pnl)}">${e.pnl ? pnlText(e.pnl) : '--'}</td>
      <td>${e.note || ''}</td>
    </tr>
  `).join('') : '<tr><td colspan="7" class="muted">暂无动作流水</td></tr>';
}

function drawHistory(points, events = []) {
  const canvas = document.getElementById('history-chart');
  const ctx = canvas.getContext('2d');
  const w = canvas.width, h = canvas.height;
  ctx.clearRect(0, 0, w, h);
  ctx.fillStyle = '#0e1318';
  ctx.fillRect(0, 0, w, h);
  if (!points.length) {
    ctx.fillStyle = '#8a97a6';
    ctx.fillText('没有历史数据', 24, 40);
    return;
  }
  const all = points.flatMap(p => [p.price, p.lower, p.mid, p.upper]);
  const min = Math.min(...all), max = Math.max(...all);
  const pad = Math.max((max - min) * 0.08, 1);
  const lo = min - pad, hi = max + pad;
  const x = i => 48 + i / Math.max(points.length - 1, 1) * (w - 78);
  const y = v => 24 + (hi - v) / (hi - lo) * (h - 58);

  ctx.strokeStyle = '#26313d';
  ctx.lineWidth = 1;
  for (let i = 0; i < 5; i++) {
    const yy = 24 + i * (h - 58) / 4;
    ctx.beginPath(); ctx.moveTo(48, yy); ctx.lineTo(w - 30, yy); ctx.stroke();
    const labelValue = hi - i * (hi - lo) / 4;
    ctx.fillStyle = '#8a97a6';
    ctx.font = '11px Segoe UI';
    ctx.fillText(fmt(labelValue), 8, yy + 4);
  }
  for (let i = 0; i < 6; i++) {
    const xx = 48 + i * (w - 78) / 5;
    ctx.strokeStyle = '#18202a';
    ctx.beginPath(); ctx.moveTo(xx, 24); ctx.lineTo(xx, h - 34); ctx.stroke();
  }

  function line(key, color, width = 1.5) {
    ctx.strokeStyle = color; ctx.lineWidth = width; ctx.beginPath();
    points.forEach((p, i) => i ? ctx.lineTo(x(i), y(p[key])) : ctx.moveTo(x(i), y(p[key])));
    ctx.stroke();
  }
  line('upper', '#ff5f66', 1);
  line('mid', '#e5b454', 1);
  line('lower', '#27c483', 1);
  line('price', '#5aa7ff', 2);

  const pointTimes = points.map(p => new Date(p.ts.replace(' ', 'T')).getTime());
  const markerTypes = new Set(['first_fill', 'add_fill', 'close', 'capital_profit', 'capital_loss']);
  const markers = (events || []).filter(e => markerTypes.has(e.type));
  function nearestIndex(ts) {
    const target = new Date(ts.replace(' ', 'T')).getTime();
    if (!Number.isFinite(target)) return -1;
    let best = -1, bestGap = Infinity;
    pointTimes.forEach((value, idx) => {
      const gap = Math.abs(value - target);
      if (gap < bestGap) { bestGap = gap; best = idx; }
    });
    return best;
  }
  const markerStyle = {
    first_fill: ['#27c483', 'H'],
    add_fill: ['#e5b454', 'A'],
    close: ['#ff5f66', 'C'],
    capital_profit: ['#ff5f66', 'C'],
    capital_loss: ['#ff5f66', 'C'],
  };
  markers.forEach(e => {
    const idx = nearestIndex(e.time);
    if (idx < 0) return;
    const [color, label] = markerStyle[e.type] || ['#ffffff', '?'];
    const xx = x(idx);
    const yy = y(e.price || points[idx].price);
    ctx.fillStyle = color;
    ctx.beginPath();
    ctx.arc(xx, yy, 5, 0, Math.PI * 2);
    ctx.fill();
    ctx.fillStyle = '#0b0e11';
    ctx.font = 'bold 8px Segoe UI';
    ctx.textAlign = 'center';
    ctx.fillText(label, xx, yy + 3);
    ctx.textAlign = 'left';
  });

  ctx.fillStyle = '#8a97a6';
  ctx.font = '12px Segoe UI';
  ctx.fillText(points[0].ts, 48, h - 10);
  ctx.textAlign = 'right';
  ctx.fillText(points[points.length - 1].ts, w - 30, h - 10);
  ctx.textAlign = 'left';
  ctx.fillStyle = '#5aa7ff'; ctx.fillText('Price', w - 210, 22);
  ctx.fillStyle = '#ff5f66'; ctx.fillText('Upper', w - 160, 22);
  ctx.fillStyle = '#e5b454'; ctx.fillText('Mid', w - 108, 22);
  ctx.fillStyle = '#27c483'; ctx.fillText('Lower', w - 70, 22);
  ctx.fillStyle = '#27c483'; ctx.fillText('H 头仓', 56, 22);
  ctx.fillStyle = '#e5b454'; ctx.fillText('A 补仓', 112, 22);
  ctx.fillStyle = '#ff5f66'; ctx.fillText('C 平仓', 168, 22);
}

refreshLive();
setInterval(refreshLive, 3000);
</script>
</body>
</html>"""


async def _handle_index(request):
    """Return the dashboard HTML page."""
    return web.Response(text=_HTML, content_type="text/html")


async def _handle_state(request):
    """Return the current dashboard state as JSON."""
    data = asdict(state)
    return web.Response(text=json.dumps(data, ensure_ascii=False), content_type="application/json")


async def _handle_logs(request):
    """Return available historical log files."""
    logs = _list_log_files()
    if logs:
        total_size = sum(item["size"] for item in logs)
        logs.insert(0, {
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


async def start_dashboard():
    """Start the dashboard server in the current event loop."""
    app = web.Application()
    app.router.add_get("/", _handle_index)
    app.router.add_get("/api/state", _handle_state)
    app.router.add_get("/api/logs", _handle_logs)
    app.router.add_get("/api/history", _handle_history)
    app.router.add_get("/api/history/trades", _handle_history_trades)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, WEB_HOST, WEB_PORT)
    await site.start()
    logger.info(f"看板已启动 → http://localhost:{WEB_PORT}")
