"""
风控模块：
  - 分批建仓计划生成
  - 强平价预估（50x 全仓）
  - 开仓前安全验证
"""
import math
from dataclasses import dataclass
from typing import List, Optional
from loguru import logger

from src.config import (
    CT_VAL, LEVER, MGN_MODE,
    BATCH_COUNT, BATCH_SIZE_RATIO, BATCH_SPACING,
    SL_BEYOND_MULT, BOLL_STD,
    LIQ_BUFFER, MAX_TOTAL_MARGIN,
    TP_PROFIT_USD,
)


@dataclass
class BatchOrder:
    batch_idx: int          # 第几批 (0-based)
    price: float            # 限价挂单价格
    sz: int                 # 张数
    notional: float         # 名义价值 USDT
    margin: float           # 占用保证金 USDT


@dataclass
class BatchPlan:
    direction: str          # "long" | "short"
    orders: List[BatchOrder]
    avg_entry: float        # 全部批次加权均价
    liq_price: float        # 估算强平价
    sl_price: float         # 止损价（布林外 SL_BEYOND_MULT 倍标准差）
    tp_price: float         # 止盈价（布林中轨）
    total_margin: float     # 合计保证金
    safe: bool              # 是否通过风控


def _liq_price_estimate(
    direction: str,
    avg_entry: float,
    total_sz: int,
    total_margin: float,
) -> float:
    """
    全仓模式强平价简化估算。
    多头：liq ≈ avg_entry - (total_margin * 0.9) / (total_sz * CT_VAL)
    空头：liq ≈ avg_entry + (total_margin * 0.9) / (total_sz * CT_VAL)
    0.9 是因为全仓维持保证金约占10%。
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
) -> BatchPlan:
    """
    生成分批建仓计划：
      - 做多：批次价格依次往下，间距为 boll_width * BATCH_SPACING[i]
      - 做空：批次价格依次往上
    """
    orders: List[BatchOrder] = []
    total_notional = 0.0
    total_margin   = 0.0
    total_sz       = 0
    weighted_sum   = 0.0

    for i in range(BATCH_COUNT):
        spacing = boll_width * BATCH_SPACING[i]
        if direction == "long":
            price = first_price - spacing
        else:
            price = first_price + spacing

        price = round(price, 2)
        if price <= 0:
            break

        # 该批次保证金额度
        margin_budget = equity * BATCH_SIZE_RATIO[i]
        # 该批次最大名义价值
        notional_budget = margin_budget * LEVER
        # 张数
        sz = math.floor(notional_budget / (price * CT_VAL))
        if sz < 1:
            logger.warning(f"第{i+1}批张数不足1，跳过")
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

    # 止盈：均价 ± 10 USDT（多头加，空头减）
    # 此处先用首批价格做占位，实际止盈在每次批次成交后由 strategy 动态更新
    if direction == "long":
        tp_price = round(avg_entry + TP_PROFIT_USD, 2)
    else:
        tp_price = round(avg_entry - TP_PROFIT_USD, 2)

    # ── 安全校验 ──────────────────────────────────────────────
    safe = True

    # 1. 强平价必须距最远批次价格有 LIQ_BUFFER 安全垫（这是唯一止损防线）
    farthest_price = orders[-1].price
    if direction == "long":
        liq_buffer_ok = (farthest_price - liq_price) / farthest_price >= LIQ_BUFFER
    else:
        liq_buffer_ok = (liq_price - farthest_price) / farthest_price >= LIQ_BUFFER

    if not liq_buffer_ok:
        logger.warning(
            f"强平价过近！估算强平价={liq_price:.2f}  最远批次={farthest_price:.2f}"
            f"  安全垫不足 {LIQ_BUFFER:.0%}，放弃信号"
        )
        safe = False

    # 2. 保证金总量不超过账户权益上限
    if total_margin / equity > MAX_TOTAL_MARGIN:
        logger.warning(f"保证金占用 {total_margin/equity:.1%} 超过上限 {MAX_TOTAL_MARGIN:.0%}")
        safe = False

    return BatchPlan(
        direction=direction,
        orders=orders,
        avg_entry=round(avg_entry, 2),
        liq_price=round(liq_price, 2),
        sl_price=round(liq_price, 2),   # sl_price 即强平价，仅用于日志展示
        tp_price=tp_price,
        total_margin=round(total_margin, 2),
        safe=safe,
    )


def check_drawdown(current_equity: float, peak_equity: float, max_dd: float) -> bool:
    if peak_equity <= 0:
        return False
    dd = (peak_equity - current_equity) / peak_equity
    if dd >= max_dd:
        logger.error(f"最大回撤触发！当前={current_equity:.2f} 峰值={peak_equity:.2f} 回撤={dd:.1%}")
        return True
    return False
