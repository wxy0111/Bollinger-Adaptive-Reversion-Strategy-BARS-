"""Local aiohttp dashboard for strategy state.

The strategy updates the module-level ``state`` object on every tick. The web
server exposes a simple HTML page and a JSON endpoint for that state.
"""
import asyncio
import json
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import List, Optional
from aiohttp import web
from loguru import logger

from src.config import WEB_HOST, WEB_PORT


@dataclass
class TradeRecord:
    """One dashboard trade-history row."""

    time: str
    action: str       # "开多" | "开空" | "平仓"
    price: float
    sz: float
    pnl: float = 0.0


@dataclass
class DashboardState:
    """Mutable state displayed by the local dashboard."""

    # 行情
    mark_price: float = 0.0
    boll_lower: float = 0.0
    boll_mid:   float = 0.0
    boll_upper: float = 0.0

    # 账户
    equity:     float = 0.0
    peak_equity: float = 0.0

    # 持仓
    direction:  str   = "none"
    avg_entry:  float = 0.0
    total_sz:   float = 0.0
    tp_price:   float = 0.0
    liq_price:  float = 0.0
    unrealized_pnl: float = 0.0

    # 批次
    batches: List[dict] = field(default_factory=list)

    # 今日统计
    today_pnl:   float = 0.0
    total_trades: int  = 0

    # 成交记录（最近20条）
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


# 全局状态单例，由策略主循环写入
state = DashboardState()

_HTML = """<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="5">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ETH 永续 策略看板</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: #0d1117; color: #e6edf3; font-family: 'Segoe UI', sans-serif; padding: 20px; }
  h1 { font-size: 18px; color: #58a6ff; margin-bottom: 16px; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 12px; margin-bottom: 16px; }
  .card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; }
  .card h2 { font-size: 13px; color: #8b949e; margin-bottom: 12px; text-transform: uppercase; letter-spacing: .5px; }
  .stat { display: flex; justify-content: space-between; margin-bottom: 8px; font-size: 14px; }
  .stat .label { color: #8b949e; }
  .stat .value { font-weight: 600; }
  .pos { color: #3fb950; }
  .neg { color: #f85149; }
  .neutral { color: #e6edf3; }
  .dir-long  { color: #3fb950; font-weight: 700; }
  .dir-short { color: #f85149; font-weight: 700; }
  .dir-none  { color: #8b949e; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th { color: #8b949e; font-weight: 500; text-align: left; padding: 6px 8px; border-bottom: 1px solid #30363d; }
  td { padding: 6px 8px; border-bottom: 1px solid #21262d; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 12px; }
  .badge-filled  { background: #1f4e2b; color: #3fb950; }
  .badge-pending { background: #2d2208; color: #d29922; }
  .updated { color: #484f58; font-size: 12px; margin-top: 12px; }
  .boll-bar { display: flex; align-items: center; gap: 8px; margin-top: 8px; }
  .boll-track { flex: 1; height: 6px; background: #30363d; border-radius: 3px; position: relative; }
  .boll-fill { height: 100%; border-radius: 3px; background: linear-gradient(90deg,#f85149,#3fb950); }
  .boll-dot { position: absolute; top: -3px; width: 12px; height: 12px; border-radius: 50%; background: #58a6ff; transform: translateX(-50%); }
</style>
</head>
<body>
<h1>⚡ ETH-USDT-SWAP 策略看板</h1>
<div class="grid">

  <!-- 行情 -->
  <div class="card">
    <h2>实时行情</h2>
    <div class="stat"><span class="label">标记价格</span><span class="value neutral" id="mp">--</span></div>
    <div class="stat"><span class="label">布林上轨</span><span class="value" id="bu">--</span></div>
    <div class="stat"><span class="label">布林中轨</span><span class="value" id="bm">--</span></div>
    <div class="stat"><span class="label">布林下轨</span><span class="value" id="bl">--</span></div>
    <div class="boll-bar">
      <span style="font-size:11px;color:#8b949e" id="bl2">--</span>
      <div class="boll-track">
        <div class="boll-fill" id="boll-fill" style="width:100%"></div>
        <div class="boll-dot" id="boll-dot" style="left:50%"></div>
      </div>
      <span style="font-size:11px;color:#8b949e" id="bu2">--</span>
    </div>
  </div>

  <!-- 账户 -->
  <div class="card">
    <h2>账户状态</h2>
    <div class="stat"><span class="label">可用权益</span><span class="value neutral" id="eq">--</span></div>
    <div class="stat"><span class="label">历史峰值</span><span class="value" id="pk">--</span></div>
    <div class="stat"><span class="label">当前回撤</span><span class="value" id="dd">--</span></div>
    <div class="stat"><span class="label">今日盈亏</span><span class="value" id="tp">--</span></div>
    <div class="stat"><span class="label">总成交次数</span><span class="value neutral" id="tt">--</span></div>
  </div>

  <!-- 持仓 -->
  <div class="card">
    <h2>当前持仓</h2>
    <div class="stat"><span class="label">方向</span><span class="value" id="dir">--</span></div>
    <div class="stat"><span class="label">均价</span><span class="value neutral" id="ae">--</span></div>
    <div class="stat"><span class="label">总张数</span><span class="value neutral" id="sz">--</span></div>
    <div class="stat"><span class="label">止盈价</span><span class="value pos" id="tpp">--</span></div>
    <div class="stat"><span class="label">估算强平</span><span class="value neg" id="liq">--</span></div>
    <div class="stat"><span class="label">浮动盈亏</span><span class="value" id="upnl">--</span></div>
  </div>

</div>

<!-- 批次 -->
<div class="card" style="margin-bottom:12px">
  <h2>分批挂单</h2>
  <table>
    <thead><tr><th>批次</th><th>挂单价</th><th>张数</th><th>状态</th></tr></thead>
    <tbody id="batches"><tr><td colspan="4" style="color:#484f58">暂无挂单</td></tr></tbody>
  </table>
</div>

<!-- 成交记录 -->
<div class="card">
  <h2>最近成交</h2>
  <table>
    <thead><tr><th>时间</th><th>操作</th><th>价格</th><th>张数</th><th>盈亏</th></tr></thead>
    <tbody id="history"><tr><td colspan="5" style="color:#484f58">暂无记录</td></tr></tbody>
  </table>
</div>

<div class="updated" id="upd">更新于 --</div>

<script>
async function refresh() {
  const r = await fetch('/api/state');
  const d = await r.json();

  const fmt = v => v.toFixed(2);
  const pnlClass = v => v >= 0 ? 'pos' : 'neg';
  const pnlStr = v => (v >= 0 ? '+' : '') + v.toFixed(2) + ' USDT';

  document.getElementById('mp').textContent = fmt(d.mark_price) + ' USDT';
  document.getElementById('bu').textContent = fmt(d.boll_upper);
  document.getElementById('bm').textContent = fmt(d.boll_mid);
  document.getElementById('bl').textContent = fmt(d.boll_lower);
  document.getElementById('bu2').textContent = fmt(d.boll_upper);
  document.getElementById('bl2').textContent = fmt(d.boll_lower);

  // 布林带位置指示
  const range = d.boll_upper - d.boll_lower;
  const pct = range > 0 ? Math.max(0, Math.min(100, (d.mark_price - d.boll_lower) / range * 100)) : 50;
  document.getElementById('boll-dot').style.left = pct + '%';

  document.getElementById('eq').textContent = fmt(d.equity) + ' USDT';
  document.getElementById('pk').textContent = fmt(d.peak_equity) + ' USDT';
  const dd = d.peak_equity > 0 ? (d.peak_equity - d.equity) / d.peak_equity * 100 : 0;
  const ddEl = document.getElementById('dd');
  ddEl.textContent = dd.toFixed(2) + '%';
  ddEl.className = 'value ' + (dd > 10 ? 'neg' : dd > 5 ? '' : 'pos');

  const tpEl = document.getElementById('tp');
  tpEl.textContent = pnlStr(d.today_pnl);
  tpEl.className = 'value ' + pnlClass(d.today_pnl);

  document.getElementById('tt').textContent = d.total_trades + ' 次';

  const dirEl = document.getElementById('dir');
  if (d.direction === 'long')       { dirEl.textContent = '做多 LONG'; dirEl.className = 'value dir-long'; }
  else if (d.direction === 'short') { dirEl.textContent = '做空 SHORT'; dirEl.className = 'value dir-short'; }
  else                              { dirEl.textContent = '空仓'; dirEl.className = 'value dir-none'; }

  document.getElementById('ae').textContent  = d.avg_entry > 0 ? fmt(d.avg_entry) : '--';
  document.getElementById('sz').textContent  = d.total_sz > 0 ? d.total_sz + ' 张' : '--';
  document.getElementById('tpp').textContent = d.tp_price > 0 ? fmt(d.tp_price) : '--';
  document.getElementById('liq').textContent = d.liq_price > 0 ? fmt(d.liq_price) : '--';

  const upnlEl = document.getElementById('upnl');
  upnlEl.textContent = d.total_sz > 0 ? pnlStr(d.unrealized_pnl) : '--';
  if (d.total_sz > 0) upnlEl.className = 'value ' + pnlClass(d.unrealized_pnl);

  // 批次表格
  const tbody = document.getElementById('batches');
  if (d.batches && d.batches.length > 0) {
    tbody.innerHTML = d.batches.map(b =>
      `<tr>
        <td>第 ${b.batch_idx + 1} 批</td>
        <td>${fmt(b.price)}</td>
        <td>${b.sz} 张</td>
        <td><span class="badge ${b.filled ? 'badge-filled' : 'badge-pending'}">${b.filled ? '已成交' : '挂单中'}</span></td>
      </tr>`
    ).join('');
  } else {
    tbody.innerHTML = '<tr><td colspan="4" style="color:#484f58">暂无挂单</td></tr>';
  }

  // 成交记录
  const hbody = document.getElementById('history');
  if (d.trade_history && d.trade_history.length > 0) {
    hbody.innerHTML = d.trade_history.map(t =>
      `<tr>
        <td>${t.time}</td>
        <td>${t.action}</td>
        <td>${fmt(t.price)}</td>
        <td>${t.sz} 张</td>
        <td class="${pnlClass(t.pnl)}">${t.pnl !== 0 ? pnlStr(t.pnl) : '--'}</td>
      </tr>`
    ).join('');
  } else {
    hbody.innerHTML = '<tr><td colspan="5" style="color:#484f58">暂无记录</td></tr>';
  }

  document.getElementById('upd').textContent = '更新于 ' + d.updated_at;
}
refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>"""


async def _handle_index(request):
    """Return the dashboard HTML page."""
    return web.Response(text=_HTML, content_type="text/html")


async def _handle_state(request):
    """Return the current dashboard state as JSON."""
    data = asdict(state)
    return web.Response(text=json.dumps(data, ensure_ascii=False),
                        content_type="application/json")


async def start_dashboard():
    """Start the dashboard server in the current event loop."""
    app = web.Application()
    app.router.add_get("/", _handle_index)
    app.router.add_get("/api/state", _handle_state)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, WEB_HOST, WEB_PORT)
    await site.start()
    logger.info(f"看板已启动 → http://localhost:{WEB_PORT}")
