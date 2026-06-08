"""Local position state for the live strategy.

The exchange remains the source of truth for real position size and average
entry. This module stores the strategy's local view of planned batches and
exit-order ids so the main loop can reconcile them on every tick or restart.
"""
from dataclasses import dataclass, field
from typing import List, Optional
from loguru import logger


@dataclass
class OpenBatch:
    """One local entry-order batch.

    Attributes:
        batch_idx: Zero-based batch index.
        ord_id: OKX order id.
        price: Planned or submitted limit price.
        sz: Planned contract size.
        filled: Whether the batch is fully filled.
    """

    batch_idx: int
    ord_id: str
    price: float
    sz: float
    filled: bool = False


@dataclass
class PositionState:
    """Mutable local strategy state.

    Attributes:
        direction: Position side, either ``"long"``, ``"short"``, or
            ``"none"``.
        batches: Known entry batches.
        tp_ord_id: Active take-profit order id.
        sl_ord_id: Active stop-loss algo order id.
        plan_liq_price: Latest liquidation price from OKX or plan estimate.
        plan_sl_price: Latest stop trigger price.
        plan_tp_price: Latest take-profit price.
        avg_entry: Latest average entry price.
        total_sz: Latest position size in contracts.
        remaining_batches_placed: Whether no more batch entries should be
            placed for the current plan.
        cycle_start_account_value: Trading-account equity before this cycle.
        cycle_start_ts: Timestamp when the cycle baseline was recorded.
    """

    direction: str
    batches: List[OpenBatch] = field(default_factory=list)
    tp_ord_id: Optional[str] = None
    sl_ord_id: Optional[str] = None
    plan_liq_price: float = 0.0
    plan_sl_price: float  = 0.0
    plan_tp_price: float  = 0.0
    avg_entry: float = 0.0
    total_sz: float = 0.0
    remaining_batches_placed: bool = False
    cycle_start_account_value: float = 0.0
    cycle_start_ts: str = ""

    def is_active(self) -> bool:
        """Return whether the local state has an active position."""
        return self.direction != "none" and self.total_sz > 0

    def has_working_plan(self) -> bool:
        """Return whether there is an active position or pending plan."""
        return self.direction != "none" or bool(self.batches)

    def next_batch_idx(self) -> int:
        """Return the next batch index after known local batches."""
        if not self.batches:
            return 0
        return max(b.batch_idx for b in self.batches) + 1

    def last_batch(self) -> Optional[OpenBatch]:
        """Return the highest-index known batch."""
        if not self.batches:
            return None
        return max(self.batches, key=lambda b: b.batch_idx)

    def filled_batches(self) -> List[OpenBatch]:
        """Return batches marked as fully filled."""
        return [b for b in self.batches if b.filled]

    def last_filled_batch(self) -> Optional[OpenBatch]:
        """Return the highest-index fully filled batch."""
        filled = self.filled_batches()
        if not filled:
            return None
        return max(filled, key=lambda b: b.batch_idx)

    def pending_batch(self) -> Optional[OpenBatch]:
        """Return the highest-index local batch that is not fully filled."""
        pending = [b for b in self.batches if not b.filled]
        if not pending:
            return None
        return max(pending, key=lambda b: b.batch_idx)

    def add_batch(self, batch: OpenBatch):
        """Append a local batch record."""
        self.batches.append(batch)

    def remove_batch(self, ord_id: str):
        """Remove a local batch by OKX order id."""
        self.batches = [b for b in self.batches if b.ord_id != ord_id]

    def mark_filled(self, ord_id: str, sz: float, fill_price: Optional[float] = None):
        """Mark a local batch as fully filled and add its real filled size."""
        for b in self.batches:
            if b.ord_id == ord_id:
                if b.filled:
                    return
                if fill_price and fill_price > 0:
                    b.price = fill_price
                b.sz = sz
                b.filled = True
                self.total_sz += sz
                logger.info(
                    f"第{b.batch_idx+1}批成交 price={b.price:.2f} "
                    f"sz={sz} 累计持仓={self.total_sz}张"
                )
                return

    def update_position(self, sz: float, avg_entry: float, liq_price: float):
        """Update local position fields from the exchange position snapshot."""
        self.total_sz = sz
        self.avg_entry = avg_entry
        if liq_price > 0:
            self.plan_liq_price = liq_price

    def reset(self):
        """Clear all local position and order state."""
        self.direction = "none"
        self.batches.clear()
        self.tp_ord_id = None
        self.sl_ord_id = None
        self.plan_liq_price = 0.0
        self.plan_sl_price  = 0.0
        self.plan_tp_price  = 0.0
        self.avg_entry = 0.0
        self.total_sz = 0.0
        self.remaining_batches_placed = False
        self.cycle_start_account_value = 0.0
        self.cycle_start_ts = ""
        logger.info("持仓状态已重置")
