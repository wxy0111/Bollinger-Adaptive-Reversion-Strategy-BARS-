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
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+).*?"
    r"价格=(?P<price>\d+(?:\.\d+)?)\s+"
    r"布林\[(?P<lower>\d+(?:\.\d+)?)\s+\|\s+"
    r"(?P<mid>\d+(?:\.\d+)?)\s+\|\s+"
    r"(?P<upper>\d+(?:\.\d+)?)\]\s+"
    r"持仓=(?P<direction>\w+)\s+权益=(?P<equity>-?\d+(?:\.\d+)?)"
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


def _parse_history_log(path: Path, limit: int) -> dict:
    """Parse a log file into sampled chart points and summary stats."""
    points = []
    if not path.exists() or path.parent.resolve() != LOG_DIR.resolve():
        raise web.HTTPNotFound(text="log file not found")

    with path.open("r", encoding="utf-8", errors="ignore") as file:
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
        "file": path.name,
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
  .hidden { display: none; }
  @media (max-width: 980px) {
    header { align-items: flex-start; flex-direction: column; }
    .grid { grid-template-columns: 1fr; }
    .wide { grid-column: auto; }
    .history-stats { grid-template-columns: 1fr 1fr; }
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
    select.innerHTML = logs.map(log => `<option value="${log.name}">${log.name}</option>`).join('');
    if (logs.length) loadHistory();
  }
}

async function loadHistory() {
  const select = document.getElementById('log-select');
  if (!select.value) return;
  const res = await fetch(`/api/history?file=${encodeURIComponent(select.value)}&limit=1200`);
  const data = await res.json();
  drawHistory(data.points || []);
  renderHistoryStats(data.summary || {});
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

function drawHistory(points) {
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

  ctx.fillStyle = '#8a97a6';
  ctx.font = '12px Segoe UI';
  ctx.fillText(fmt(hi), 8, 30);
  ctx.fillText(fmt(lo), 8, h - 32);
  ctx.fillText(points[0].ts, 48, h - 10);
  ctx.textAlign = 'right';
  ctx.fillText(points[points.length - 1].ts, w - 30, h - 10);
  ctx.textAlign = 'left';
  ctx.fillStyle = '#5aa7ff'; ctx.fillText('Price', w - 210, 22);
  ctx.fillStyle = '#ff5f66'; ctx.fillText('Upper', w - 160, 22);
  ctx.fillStyle = '#e5b454'; ctx.fillText('Mid', w - 108, 22);
  ctx.fillStyle = '#27c483'; ctx.fillText('Lower', w - 70, 22);
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
    return web.json_response(_list_log_files())


async def _handle_history(request):
    """Return sampled historical log data."""
    filename = Path(request.query.get("file", "")).name
    limit = int(request.query.get("limit", "1200"))
    data = _parse_history_log(LOG_DIR / filename, max(100, min(limit, 5000)))
    return web.json_response(data)


async def start_dashboard():
    """Start the dashboard server in the current event loop."""
    app = web.Application()
    app.router.add_get("/", _handle_index)
    app.router.add_get("/api/state", _handle_state)
    app.router.add_get("/api/logs", _handle_logs)
    app.router.add_get("/api/history", _handle_history)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, WEB_HOST, WEB_PORT)
    await site.start()
    logger.info(f"看板已启动 → http://localhost:{WEB_PORT}")
