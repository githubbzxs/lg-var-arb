from decimal import Decimal
from typing import Optional


class PnLTracker:
    def __init__(self) -> None:
        self.position = Decimal("0")
        self.avg_price = Decimal("0")
        self.realized = Decimal("0")

    def update(self, delta: Decimal, price: Optional[Decimal]) -> Decimal:
        """Update position and realized PnL using the trade price."""
        if not delta:
            return Decimal("0")

        if price is None or price <= 0:
            self.position += delta
            if self.position == 0:
                self.avg_price = Decimal("0")
            return Decimal("0")

        if self.position == 0 or (self.position > 0 and delta > 0) or (
            self.position < 0 and delta < 0
        ):
            new_pos = self.position + delta
            total_cost = abs(self.position) * self.avg_price + abs(delta) * price
            self.position = new_pos
            if self.position == 0:
                self.avg_price = Decimal("0")
            else:
                self.avg_price = total_cost / abs(self.position)
            return Decimal("0")

        close_qty = min(abs(self.position), abs(delta))
        realized = close_qty * (price - self.avg_price) * (
            Decimal("1") if self.position > 0 else Decimal("-1")
        )
        self.realized += realized

        new_pos = self.position + delta
        if new_pos == 0:
            self.position = new_pos
            self.avg_price = Decimal("0")
        elif (self.position > 0 and new_pos > 0) or (self.position < 0 and new_pos < 0):
            self.position = new_pos
        else:
            self.position = new_pos
            self.avg_price = price
        return realized

    def unrealized(self, mark_price: Optional[Decimal]) -> Decimal:
        if mark_price is None or mark_price <= 0 or self.position == 0:
            return Decimal("0")
        return (mark_price - self.avg_price) * self.position
