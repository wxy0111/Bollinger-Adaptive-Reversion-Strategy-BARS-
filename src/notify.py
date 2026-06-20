"""Notification helpers with provider routing and severity filtering."""

from __future__ import annotations

import aiohttp
from loguru import logger

from src.config import (
    NOTIFY_MIN_LEVEL,
    NOTIFY_PROVIDER,
    SERVERCHAN_KEY,
    WXPUSHER_APP_TOKEN,
    WXPUSHER_TOPIC_IDS,
    WXPUSHER_UIDS,
)


SERVERCHAN_URL = f"https://sctapi.ftqq.com/{SERVERCHAN_KEY}.send"
WXPUSHER_URL = "https://wxpusher.zjiecode.com/api/send/message"
LEVEL_ORDER = {
    "info": 10,
    "trade": 20,
    "critical": 30,
}


def _level_value(level: str) -> int:
    """Return numeric severity for a configured notification level."""
    return LEVEL_ORDER.get((level or "trade").lower(), LEVEL_ORDER["trade"])


def _should_notify(level: str) -> bool:
    """Return whether a message should be sent at the configured threshold."""
    return _level_value(level) >= _level_value(NOTIFY_MIN_LEVEL)


def _split_csv(text: str) -> list[str]:
    """Return non-empty comma-separated values."""
    return [item.strip() for item in (text or "").split(",") if item.strip()]


def _wxpusher_topic_ids() -> list[int]:
    """Return WxPusher topic ids parsed from config."""
    topic_ids = []
    for item in _split_csv(WXPUSHER_TOPIC_IDS):
        try:
            topic_ids.append(int(item))
        except ValueError:
            logger.warning(f"Invalid WxPusher topic id ignored: {item}")
    return topic_ids


async def _send_serverchan(title: str, content: str) -> None:
    """Send one ServerChan notification."""
    if not SERVERCHAN_KEY:
        return
    async with aiohttp.ClientSession() as session:
        response = await session.post(
            SERVERCHAN_URL,
            data={"title": title[:32], "desp": content},
        )
        if response.status >= 400:
            body = await response.text()
            raise RuntimeError(f"ServerChan HTTP {response.status}: {body[:200]}")


async def _send_wxpusher(title: str, content: str) -> None:
    """Send one WxPusher notification."""
    uids = _split_csv(WXPUSHER_UIDS)
    topic_ids = _wxpusher_topic_ids()
    if not WXPUSHER_APP_TOKEN:
        return
    if not uids and not topic_ids:
        logger.warning("WxPusher configured without WXPUSHER_UIDS or WXPUSHER_TOPIC_IDS")
        return

    payload = {
        "appToken": WXPUSHER_APP_TOKEN,
        "summary": title[:100],
        "content": f"## {title}\n\n{content}" if content else title,
        "contentType": 3,
    }
    if uids:
        payload["uids"] = uids
    if topic_ids:
        payload["topicIds"] = topic_ids

    async with aiohttp.ClientSession() as session:
        response = await session.post(WXPUSHER_URL, json=payload)
        data = await response.json(content_type=None)
        if response.status >= 400 or data.get("code") not in (1000, "1000", 0, "0"):
            raise RuntimeError(f"WxPusher HTTP {response.status}: {data}")


async def wx_push(title: str, content: str = "", level: str = "trade") -> None:
    """Send a notification through the configured provider.

    Levels:
        info: routine messages, such as submitted entry orders.
        trade: fills, closes, capital restoration, and normal trade events.
        critical: liquidation warnings, risk guards, shortage, and protection stops.
    """
    if not _should_notify(level):
        logger.debug(f"Notification skipped by level filter level={level} title={title}")
        return

    provider = (NOTIFY_PROVIDER or "serverchan").lower()
    try:
        if provider == "wxpusher":
            await _send_wxpusher(title, content)
        elif provider == "serverchan":
            await _send_serverchan(title, content)
        elif provider in ("none", "off", "disabled"):
            return
        else:
            logger.warning(f"Unknown NOTIFY_PROVIDER={NOTIFY_PROVIDER}; skip notification")
    except Exception as exc:
        logger.warning(f"WeChat notification failed provider={provider}: {exc}")


async def notify_entry_order(direction: str, price: float, sz: float, batch: int, total: int, ord_id: str):
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
    await wx_push(title, content, level="info")


async def notify_cross_copy_protect(
    account_equity: float,
    protected_equity: float,
    direction: str,
    total_sz: float,
) -> None:
    """Notify when cross-margin copy-protection stops the strategy."""
    title = "ETH 全仓带单保护触发"
    content = (
        f"**账户总权益**：{account_equity:.2f} USDT\n\n"
        f"**保护权益**：{protected_equity:.2f} USDT\n\n"
        f"**当前方向**：{direction.upper()}\n\n"
        f"**当前张数**：{total_sz}\n\n"
        "账户权益已触及保护线，程序将撤单并停止运行。"
    )
    await wx_push(title, content, level="critical")


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
    await wx_push(title, content, level="critical")


async def notify_capital_restored(trading_balance: float, target: float):
    """Notify that trading capital has recovered to the configured target."""
    title = "ETH 资金已补足"
    content = (
        f"**交易账户可用**：{trading_balance:.2f} USDT\n\n"
        f"**目标额度**：{target:.2f} USDT\n\n"
        "交易账户可用额度已达到目标，策略继续按目标资金运行。"
    )
    await wx_push(title, content, level="trade")


async def notify_open(direction: str, avg_entry: float, sz: float, tp: float, liq: float, batch: int, total: int):
    """Notify that an entry batch has filled."""
    side_text = "做多" if direction == "long" else "做空"
    title = f"ETH {side_text} 第{batch}/{total}批成交"
    content = (
        f"**方向**：{direction.upper()}\n\n"
        f"**均价**：{avg_entry:.2f} USDT\n\n"
        f"**总张数**：{sz}\n\n"
        f"**止盈**：{tp:.2f}\n\n"
        f"**估算强平**：{liq:.2f}"
    )
    await wx_push(title, content, level="trade")


async def notify_close(direction: str, avg_entry: float, close_price: float, pnl: float, sz: float):
    """Notify that a position has closed."""
    result = "盈利" if pnl >= 0 else "亏损"
    title = f"ETH 平仓 {result} {pnl:+.2f} USDT"
    content = (
        f"**方向**：{direction.upper()}\n\n"
        f"**均价**：{avg_entry:.2f}\n\n"
        f"**平仓价**：{close_price:.2f}\n\n"
        f"**张数**：{sz}\n\n"
        f"**盈亏**：{pnl:+.2f} USDT"
    )
    await wx_push(title, content, level="trade")


async def notify_liq_warning(
    direction: str,
    mark_price: float,
    liq_price: float,
    gap_pct: float,
    gap_usd: float | None = None,
):
    """Notify when mark price is close to liquidation price."""
    distance = f"{gap_usd:.2f} USDT / " if gap_usd is not None else ""
    title = f"ETH 强平风险预警：剩余 {distance}{gap_pct:.1f}%"
    content = (
        f"**方向**：{direction.upper()}\n\n"
        f"**标记价格**：{mark_price:.2f}\n\n"
        f"**强平价格**：{liq_price:.2f}\n\n"
        f"**距离强平**：{distance}{gap_pct:.2f}%"
    )
    await wx_push(title, content, level="critical")


async def notify_trend_risk_guard(
    direction: str,
    mark_price: float,
    head_price: float,
    adverse_pct: float,
    score: int,
    reasons: list[str],
    width_expand: float,
    width_pct: float,
    mid_slope: float,
    lower_slope: float,
    upper_slope: float,
    close_enabled: bool,
) -> None:
    """Notify when the trend-risk guard reaches its score threshold."""
    title = "ETH 趋势风险保护触发"
    content = (
        f"**方向**：{direction.upper()}\n\n"
        f"**标记价格**：{mark_price:.2f}\n\n"
        f"**首笔入场**：{head_price:.2f}\n\n"
        f"**首笔逆向幅度**：{adverse_pct:.2%}\n\n"
        f"**风险分数**：{score}\n\n"
        f"**触发原因**：{', '.join(reasons)}\n\n"
        f"**布林宽度扩张**：{width_expand:.3f}x\n\n"
        f"**布林宽度占比**：{width_pct:.2%}\n\n"
        f"**中轨斜率**：{mid_slope:.3f}%/h\n\n"
        f"**下轨斜率**：{lower_slope:.3f}%/h\n\n"
        f"**上轨斜率**：{upper_slope:.3f}%/h\n\n"
        f"**动作**：{'平掉当前仓位' if close_enabled else '仅通知，继续持仓'}"
    )
    await wx_push(title, content, level="critical")


async def notify_drawdown(current: float, peak: float, dd: float):
    """Notify when account drawdown reaches the configured stop level."""
    title = f"ETH 策略回撤预警 {dd:.1%}"
    content = (
        f"**当前权益**：{current:.2f} USDT\n\n"
        f"**历史峰值**：{peak:.2f} USDT\n\n"
        f"**回撤幅度**：{dd:.2%}"
    )
    await wx_push(title, content, level="critical")
