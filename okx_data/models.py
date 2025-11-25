from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional


OrderBookSide = List[Dict[str, Optional[float]]]


@dataclass(slots=True)
class AggregatedPoint:
    instrument_id: str
    product_id: str
    exchange_id: str
    trading_day: str
    open_interest: Optional[float]
    turnover: float
    update_millisec: int
    update_time: str
    last_price: Optional[float]
    volume: float
    local_cpu_time: str
    bids: OrderBookSide
    asks: OrderBookSide

    def _format_numeric(self, value: Optional[float]) -> str:
        if value is None:
            return ""
        return f"{value:.2f}"

    def to_csv_row(self) -> Dict[str, Any]:
        row: Dict[str, Any] = {
            "InstrumentID": self.instrument_id,
            "ProductID": self.product_id,
            "ExchangeID": self.exchange_id,
            "TradingDay": self.trading_day,
            "OpenInterest": self._format_numeric(self.open_interest),
            "Turnover": self._format_numeric(self.turnover),
            "UpdateMillisec": self.update_millisec,
            "UpdateTime": self.update_time,
            "LastPrice": self._format_numeric(self.last_price),
            "Volume": self._format_numeric(self.volume),
            "LocalCPUTime": self.local_cpu_time,
        }
        for idx in range(5):
            bid_entry = self.bids[idx] if idx < len(self.bids) else {}
            ask_entry = self.asks[idx] if idx < len(self.asks) else {}
            row[f"BidPrice{idx + 1}"] = self._format_numeric(bid_entry.get("price"))
            row[f"BidVolume{idx + 1}"] = self._format_numeric(bid_entry.get("size"))
            row[f"AskPrice{idx + 1}"] = self._format_numeric(ask_entry.get("price"))
            row[f"AskVolume{idx + 1}"] = self._format_numeric(ask_entry.get("size"))
        return row

