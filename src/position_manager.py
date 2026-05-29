"""
持仓状态管理：跟踪已开批次、挂单、止盈止损订单。
在程序重启时通过 OKX API 重建状态。
"""
from dataclasses import dataclass, field
from typing import List, Optional, Dict
from loguru import logger


@dataclass
class OpenBatch:
    batch_idx: int
    ord_id: str
    price: float
    sz: int
    filled: bool = False


@dataclass
class PositionState:
    direction: str                          # "long" | "short" | "none"
    batches: List[OpenBatch] = field(default_factory=list)
    tp_ord_id: Optional[str] = None
    sl_ord_id: Optional[str] = None
    plan_liq_price: float = 0.0
    plan_sl_price: float  = 0.0
    plan_tp_price: float  = 0.0
    total_sz: int = 0

    def is_active(self) -> bool:
        return self.direction != "none" and self.total_sz > 0

    def next_batch_idx(self) -> int:
        if not self.batches:
            return 0
        return max(b.batch_idx for b in self.batches) + 1

    def filled_batches(self) -> List[OpenBatch]:
        return [b for b in self.batches if b.filled]

    def add_batch(self, batch: OpenBatch):
        self.batches.append(batch)

    def mark_filled(self, ord_id: str, sz: int):
        for b in self.batches:
            if b.ord_id == ord_id:
                b.filled = True
                self.total_sz += sz
                logger.info(f"第{b.batch_idx+1}批成交 sz={sz} 累计持仓={self.total_sz}张")
                return

    def reset(self):
        self.direction = "none"
        self.batches.clear()
        self.tp_ord_id = None
        self.sl_ord_id = None
        self.plan_liq_price = 0.0
        self.plan_sl_price  = 0.0
        self.plan_tp_price  = 0.0
        self.total_sz = 0
        logger.info("持仓状态已重置")
