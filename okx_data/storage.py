from __future__ import annotations

import csv
from datetime import date
from pathlib import Path
from typing import Optional

from config.settings import CSV_FILE_TEMPLATE, OUTPUT_DIR
from okx_data.models import AggregatedPoint

CSV_HEADER = [
    "InstrumentID",
    "ProductID",
    "ExchangeID",
    "TradingDay",
    "OpenInterest",
    "Turnover",
    "UpdateMillisec",
    "UpdateTime",
    "BidPrice1",
    "BidVolume1",
    "BidPrice2",
    "BidVolume2",
    "BidPrice3",
    "BidVolume3",
    "BidPrice4",
    "BidVolume4",
    "BidPrice5",
    "BidVolume5",
    "AskPrice1",
    "AskVolume1",
    "AskPrice2",
    "AskVolume2",
    "AskPrice3",
    "AskVolume3",
    "AskPrice4",
    "AskVolume4",
    "AskPrice5",
    "AskVolume5",
    "LastPrice",
    "Volume",
    "LocalCPUTime",
]


class CsvWriter:
    def __init__(
        self,
        output_dir: Path = OUTPUT_DIR,
        file_template: str = CSV_FILE_TEMPLATE,
        instrument_id: Optional[str] = None,
    ) -> None:
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.file_template = file_template
        self.instrument_id = instrument_id
        self.current_path: Optional[Path] = None

    def _resolve_path(self, current_date: date) -> Path:
        inst_id = self.instrument_id or ""
        return self.output_dir / self.file_template.format(
            inst_id=inst_id, date=current_date.strftime("%Y%m%d")
        )

    def rotate(self, current_date: date) -> None:
        self.current_path = self._resolve_path(current_date)
        if not self.current_path.exists():
            self._write_header()

    def write(self, point: AggregatedPoint, current_date: date) -> None:
        if self.instrument_id is None:
            self.instrument_id = point.instrument_id
        if not self.current_path or self.current_path.parent.name != current_date.strftime(
            "%Y%m%d"
        ):
            self.rotate(current_date)
        row = point.to_csv_row()
        with self.current_path.open("a", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=CSV_HEADER)
            if file.tell() == 0:
                writer.writeheader()
            writer.writerow(row)

    def _write_header(self) -> None:
        if not self.current_path:
            return
        self.current_path.parent.mkdir(parents=True, exist_ok=True)
        with self.current_path.open("w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=CSV_HEADER)
            writer.writeheader()

