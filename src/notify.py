"""ServerChan notification helpers."""
import aiohttp
from loguru import logger
from src.config import SERVERCHAN_KEY

_URL = f"https://sctapi.ftqq.com/{SERVERCHAN_KEY}.send"


async def wx_push(title: str, content: str = "") -> None:
    """Send a ServerChan notification when a key is configured.

    Args:
        title: Notification title.
        content: Markdown notification body.
    """
    if not SERVERCHAN_KEY:
        return
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(_URL, data={"title": title[:32], "desp": content})
    except Exception as e:
        logger.warning(f"微信推送失败: {e}")

async def notify_entry_order(direction: str, price: float, sz: float,
                             batch: int, total: int, ord_id: str):
    """Notify that an entry or add-on order has been submitted."""
    action = "开仓挂单" if batch == 1 else "加仓挂单"
    side_text = "做多" if direction == "long" else "做空"
    title = f"ETH {action} 第{batch}/{total}批"
    content = (
        f"**方向**：{side_text}\n\n"
        f"**挂单价**：{price:.2f} USDT\n\n"
        f"**张数**：{sz}\n\n"
        f"**订单ID**：{ord_id}"
    )
    await wx_push(title, content)


async def notify_capital_shortage(trading_balance: float, target: float, funding_balance: float, top_up: float):
    """Notify that the funding account cannot fully restore trading capital."""
    title = "ETH 资金不足提醒"
    content = (
        f"**交易账户可用**：{trading_balance:.2f} USDT\n\n"
        f"**目标额度**：{target:.2f} USDT\n\n"
        f"**资金账户可用**：{funding_balance:.2f} USDT\n\n"
        f"**本次补充**：{top_up:.2f} USDT\n\n"
        "资金账户不足以补满目标额度，策略会按当前可用额度继续运行。"
    )
    await wx_push(title, content)


async def notify_capital_restored(trading_balance: float, target: float):
    """Notify that trading capital has recovered to the configured target."""
    title = "ETH 资金已补足"
    content = (
        f"**交易账户可用**：{trading_balance:.2f} USDT\n\n"
        f"**目标额度**：{target:.2f} USDT\n\n"
        "交易账户可用额度已达到目标，策略继续按目标资金运行。"
    )
    await wx_push(title, content)


async def notify_open(direction: str, avg_entry: float, sz: float,
                      tp: float, liq: float, batch: int, total: int):
    """Notify that an entry batch has filled."""
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
                       pnl: float, sz: float):
    """Notify that a position has closed."""
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


async def notify_liq_warning(
    direction: str,
    mark_price: float,
    liq_price: float,
    gap_pct: float,
    gap_usd: float | None = None,
):
    """Notify when mark price is close to liquidation price."""
    distance = f"{gap_usd:.2f} USDT / " if gap_usd is not None else ""
    title = f"ETH liquidation warning: {distance}{gap_pct:.1f}% left"
    content = (
        f"**Direction**: {direction.upper()}\n\n"
        f"**Mark price**: {mark_price:.2f}\n\n"
        f"**Liquidation price**: {liq_price:.2f}\n\n"
        f"**Distance**: {distance}{gap_pct:.2f}%"
    )
    await wx_push(title, content)


async def notify_drawdown(current: float, peak: float, dd: float):
    """Notify when account drawdown reaches the configured stop level."""
    title = f"🚨 ETH 策略回撤预警 {dd:.1%}"
    content = (
        f"**当前权益**：{current:.2f} USDT\n\n"
        f"**历史峰值**：{peak:.2f} USDT\n\n"
        f"**回撤幅度**：{dd:.2%}"
    )
    await wx_push(title, content)
