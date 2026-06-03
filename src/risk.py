"""Risk and batch-plan helpers.

This module builds the theoretical batch-entry plan used by the live strategy.
The current implementation estimates liquidation price and take-profit price,
but it does not yet enforce reserved controls such as liquidation buffer or
maximum total margin.
"""
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from typing import List
from loguru import logger

from src.config import (
    CT_VAL, LEVER,
    BATCH_COUNT, BATCH_SIZE_RATIO, BATCH_SPACING, STRATEGY_EQUITY_CAP_USDT,
    MIN_ORDER_CONTRACTS, CONTRACT_STEP,
    TP_PROFIT_USD,
)


@dataclass
class BatchOrder:
    """One planned entry batch.

    Attributes:
        batch_idx: Zero-based batch index.
        price: Limit order price.
        sz: Order size in contracts.
        notional: Notional value in USDT.
        margin: Estimated margin usage in USDT.
    """

    batch_idx: int
    price: float
    sz: float
    notional: float
    margin: float


@dataclass
class BatchPlan:
    """A generated batch-entry plan.

    Attributes:
        direction: Position side, either ``"long"`` or ``"short"``.
        orders: Batch orders included in the plan.
        avg_entry: Weighted average entry if all plan orders fill.
        liq_price: Simplified estimated liquidation price.
        sl_price: Display stop price. Currently equal to ``liq_price``.
        tp_price: Take-profit price based on average entry.
        total_margin: Estimated total margin in USDT.
        safe: Whether the plan passed current checks.
    """

    direction: str
    orders: List[BatchOrder]
    avg_entry: float
    liq_price: float
    sl_price: float
    tp_price: float
    total_margin: float
    safe: bool


def _floor_to_step(value: float, step: float) -> float:
    """Round a numeric value down to the exchange step size."""
    value_dec = Decimal(str(value))
    step_dec = Decimal(str(step))
    return float((value_dec / step_dec).to_integral_value(rounding=ROUND_DOWN) * step_dec)


def _liq_price_estimate(
    direction: str,
    avg_entry: float,
    total_sz: float,
    total_margin: float,
) -> float:
    """Estimate liquidation price with a simplified cross-margin formula.

    Args:
        direction: Position side, either ``"long"`` or ``"short"``.
        avg_entry: Weighted average entry price.
        total_sz: Total position size in contracts.
        total_margin: Estimated margin allocated to the position.

    Returns:
        Estimated liquidation price. Returns 0 when ``total_sz`` is 0.
    """
    if total_sz == 0:
        return 0.0
    margin_per_eth = (total_margin * 0.9) / (total_sz * CT_VAL)
    if direction == "long":
        return avg_entry - margin_per_eth
    else:
        return avg_entry + margin_per_eth


def build_batch_plan(
    direction: str,
    first_price: float,     # 首批入场价（当前市价 / 收盘价）
    boll_width: float,
    boll_mid: float,
    boll_lower: float,
    boll_upper: float,
    boll_std: float,        # 当前布林带标准差值
    equity: float,
    max_batch_idx: int | None = None,
    fixed_batch_sizes: list[float] | None = None,
) -> BatchPlan:
    """Build a batch-entry plan for a new or next batch.

    Args:
        direction: Position side, either ``"long"`` or ``"short"``.
        first_price: Reference price for the first batch.
        boll_width: Current Bollinger-band width.
        boll_mid: Current Bollinger middle band. Kept for future checks.
        boll_lower: Current Bollinger lower band. Kept for future checks.
        boll_upper: Current Bollinger upper band. Kept for future checks.
        boll_std: Current Bollinger standard deviation. Kept for future checks.
        equity: Available account balance used for sizing when no fixed sizes
            are provided.
        max_batch_idx: Optional maximum batch index to include.
        fixed_batch_sizes: Optional fixed contract sizes calculated at runtime.

    Returns:
        A ``BatchPlan``. ``safe`` is false only when no valid orders are built.
    """
    orders: List[BatchOrder] = []
    total_notional = 0.0
    total_margin   = 0.0
    total_sz       = 0.0
    weighted_sum   = 0.0
    effective_equity = min(equity, STRATEGY_EQUITY_CAP_USDT) if STRATEGY_EQUITY_CAP_USDT > 0 else equity

    batch_count = BATCH_COUNT if max_batch_idx is None else max_batch_idx + 1
    for i in range(batch_count):
        spacing_ratio = BATCH_SPACING[i] if i < len(BATCH_SPACING) else 0.0
        spacing = boll_width * spacing_ratio
        if direction == "long":
            price = first_price - spacing
        else:
            price = first_price + spacing

        price = round(price, 2)
        if price <= 0:
            break

        if fixed_batch_sizes is not None:
            sz = fixed_batch_sizes[i] if i < len(fixed_batch_sizes) else 0.0
        else:
            # Size by configured margin allocation, then round to exchange step.
            ratio = BATCH_SIZE_RATIO[i] if i < len(BATCH_SIZE_RATIO) else BATCH_SIZE_RATIO[-1]
            margin_budget = effective_equity * ratio
            notional_budget = margin_budget * LEVER
            raw_sz = notional_budget / (price * CT_VAL)
            sz = _floor_to_step(raw_sz, CONTRACT_STEP)
        if sz < MIN_ORDER_CONTRACTS:
            logger.warning(f"第{i+1}批张数不足{MIN_ORDER_CONTRACTS}，跳过")
            continue

        notional = sz * CT_VAL * price
        margin   = notional / LEVER

        orders.append(BatchOrder(
            batch_idx=i,
            price=price,
            sz=sz,
            notional=notional,
            margin=margin,
        ))
        total_notional += notional
        total_margin   += margin
        total_sz       += sz
        weighted_sum   += price * sz

    if not orders:
        return BatchPlan(
            direction=direction, orders=[], avg_entry=0, liq_price=0,
            sl_price=0, tp_price=0, total_margin=0, safe=False,
        )

    avg_entry = weighted_sum / total_sz
    liq_price = _liq_price_estimate(direction, avg_entry, total_sz, total_margin)

    # The live strategy recalculates take profit from the exchange average
    # entry after each fill. This value is a plan-time placeholder.
    if direction == "long":
        tp_price = round(avg_entry + TP_PROFIT_USD, 2)
    else:
        tp_price = round(avg_entry - TP_PROFIT_USD, 2)

    return BatchPlan(
        direction=direction,
        orders=orders,
        avg_entry=round(avg_entry, 2),
        liq_price=round(liq_price, 2),
        sl_price=round(liq_price, 2),
        tp_price=tp_price,
        total_margin=round(total_margin, 2),
        safe=True,
    )


def check_drawdown(current_equity: float, peak_equity: float, max_dd: float) -> bool:
    """Return whether the account drawdown has reached the configured limit."""
    if max_dd <= 0 or max_dd >= 1.0 or peak_equity <= 0:
        return False
    dd = (peak_equity - current_equity) / peak_equity
    if dd >= max_dd:
        logger.error(f"最大回撤触发！当前={current_equity:.2f} 峰值={peak_equity:.2f} 回撤={dd:.1%}")
        return True
    return False
