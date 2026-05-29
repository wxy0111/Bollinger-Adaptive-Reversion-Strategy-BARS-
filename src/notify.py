"""微信推送（Server酱）。"""
import aiohttp
from loguru import logger
from src.config import SERVERCHAN_KEY

_URL = f"https://sctapi.ftqq.com/{SERVERCHAN_KEY}.send"


async def wx_push(title: str, content: str = "") -> None:
    if not SERVERCHAN_KEY:
        return
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(_URL, data={"title": title[:32], "desp": content})
    except Exception as e:
        logger.warning(f"微信推送失败: {e}")


# ── 预设消息模板 ──────────────────────────────────────────────

async def notify_open(direction: str, avg_entry: float, sz: int,
                      tp: float, liq: float, batch: int, total: int):
    title = f"{'🟢做多' if direction == 'long' else '🔴做空'} ETH 第{batch}/{total}批成交"
    content = (
        f"**方向**：{direction.upper()}\n\n"
        f"**均价**：{avg_entry:.2f} USDT\n\n"
        f"**总张数**：{sz}\n\n"
        f"**止盈**：{tp:.2f}\n\n"
        f"**估算强平**：{liq:.2f}"
    )
    await wx_push(title, content)


async def notify_close(direction: str, avg_entry: float, close_price: float,
                       pnl: float, sz: int):
    emoji = "✅" if pnl >= 0 else "❌"
    title = f"{emoji} ETH 平仓  {'盈利' if pnl >= 0 else '亏损'} {pnl:+.2f} USDT"
    content = (
        f"**方向**：{direction.upper()}\n\n"
        f"**均价**：{avg_entry:.2f}\n\n"
        f"**平仓价**：{close_price:.2f}\n\n"
        f"**张数**：{sz}\n\n"
        f"**盈亏**：{pnl:+.2f} USDT"
    )
    await wx_push(title, content)


async def notify_liq_warning(direction: str, mark_price: float, liq_price: float, gap_pct: float):
    title = f"⚠️ ETH 强平预警  距强平还剩 {gap_pct:.1f}%"
    content = (
        f"**方向**：{direction.upper()}\n\n"
        f"**当前价**：{mark_price:.2f}\n\n"
        f"**强平价**：{liq_price:.2f}\n\n"
        f"**距离**：{gap_pct:.2f}%"
    )
    await wx_push(title, content)


async def notify_drawdown(current: float, peak: float, dd: float):
    title = f"🚨 ETH 策略回撤预警 {dd:.1%}"
    content = (
        f"**当前权益**：{current:.2f} USDT\n\n"
        f"**历史峰值**：{peak:.2f} USDT\n\n"
        f"**回撤幅度**：{dd:.2%}"
    )
    await wx_push(title, content)
