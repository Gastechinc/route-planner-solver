"""StockSnapshot — service-side wrapper over the public.stock data the
web app sends in the request payload.

Mirrors the desktop's API (quantity / has_part) so the ported solver code
keeps its existing call sites unchanged.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class StockSnapshot:
    """Parsed stock state at a point in time.

    `by_location` maps each location (van reg or warehouse name, uppercased)
    to a `{stock_code_upper: quantity}` map. Zero-quantity entries are kept
    so we can still show "location carries this part, but is out of stock".
    """

    loaded_at: datetime
    by_location: dict[str, dict[str, float]] = field(default_factory=dict)

    @classmethod
    def from_request(cls, by_location: dict[str, dict[str, float]]) -> "StockSnapshot":
        # Normalise keys to uppercase + trim — same convention as web/parser
        norm: dict[str, dict[str, float]] = {}
        for loc, codes in by_location.items():
            loc_n = loc.strip().upper()
            if not loc_n:
                continue
            norm[loc_n] = {}
            for code, qty in codes.items():
                code_n = str(code).strip().upper()
                if not code_n:
                    continue
                try:
                    norm[loc_n][code_n] = float(qty)
                except (TypeError, ValueError):
                    norm[loc_n][code_n] = 0.0
        return cls(loaded_at=datetime.utcnow(), by_location=norm)

    @property
    def locations(self) -> list[str]:
        return sorted(self.by_location.keys())

    def quantity(self, location: str, stock_code: str) -> float:
        loc = location.strip().upper()
        code = stock_code.strip().upper()
        return self.by_location.get(loc, {}).get(code, 0.0)

    def has_part(self, location: str, stock_code: str) -> bool:
        return self.quantity(location, stock_code) > 0.0
