from __future__ import annotations

import csv
import queue
import threading
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
        async_write: bool = True,
    ) -> None:
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.file_template = file_template
        self.instrument_id = instrument_id
        self.current_path: Optional[Path] = None
        
        # 异步写入相关
        self.async_write = async_write
        self.write_queue: Optional[queue.Queue] = None
        self.write_thread: Optional[threading.Thread] = None
        self.write_running = False
        self.batch_size = 10  # 批量写入大小，减少I/O次数
        self.batch_buffer: list = []  # 批量写入缓冲区
        self.batch_lock = threading.Lock()  # 批量缓冲区锁
        
        if self.async_write:
            self.write_queue = queue.Queue(maxsize=1000)  # 缓冲队列，最多1000条
            self.write_running = True
            self.write_thread = threading.Thread(
                target=self._write_worker, daemon=True, name="csv_writer"
            )
            self.write_thread.start()

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
        """写入数据点（同步或异步）"""
        if self.instrument_id is None:
            self.instrument_id = point.instrument_id
        if not self.current_path or self.current_path.parent.name != current_date.strftime(
            "%Y%m%d"
        ):
            self.rotate(current_date)
        
        row = point.to_csv_row()
        
        if self.async_write and self.write_queue is not None:
            # 异步写入：放入队列，不阻塞
            try:
                self.write_queue.put_nowait((self.current_path, row))
            except queue.Full:
                # 队列满时，降级为同步写入（避免丢失数据）
                self._write_row_sync(self.current_path, row)
        else:
            # 同步写入
            self._write_row_sync(self.current_path, row)
    
    def _write_row_sync(self, file_path: Path, row: dict) -> None:
        """同步写入单行数据"""
        # 确保目录存在
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with file_path.open("a", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=CSV_HEADER)
            if file.tell() == 0:
                writer.writeheader()
            writer.writerow(row)
    
    def _write_worker(self) -> None:
        """后台写入线程：从队列中取出数据并批量写入文件（优化I/O性能）"""
        import time
        batch = []
        last_flush = time.time()
        flush_interval = 0.1  # 100ms 强制刷新一次
        
        while self.write_running:
            try:
                # 从队列中获取数据，超时0.1秒
                file_path, row = self.write_queue.get(timeout=0.1)
                batch.append((file_path, row))
                self.write_queue.task_done()
                
                # 批量写入：达到批量大小或超时则写入
                now = time.time()
                if len(batch) >= self.batch_size or (now - last_flush) >= flush_interval:
                    self._write_batch(batch)
                    batch.clear()
                    last_flush = now
            except queue.Empty:
                # 队列为空时，写入剩余的批量数据
                if batch:
                    self._write_batch(batch)
                    batch.clear()
                    last_flush = time.time()
                continue
            except Exception as exc:
                # 写入错误不应该影响主程序
                import logging
                logger = logging.getLogger(__name__)
                logger.error("CSV写入错误: %s", exc)
                batch.clear()  # 清空批量缓冲区，避免重复错误
        
        # 退出前写入剩余数据
        if batch:
            self._write_batch(batch)
    
    def _write_batch(self, batch: list) -> None:
        """批量写入多行数据（优化I/O性能）"""
        if not batch:
            return
        
        # 按文件路径分组
        file_groups: dict = {}
        for file_path, row in batch:
            if file_path not in file_groups:
                file_groups[file_path] = []
            file_groups[file_path].append(row)
        
        # 对每个文件批量写入
        for file_path, rows in file_groups.items():
            try:
                # 确保目录存在
                file_path.parent.mkdir(parents=True, exist_ok=True)
                with file_path.open("a", newline="", encoding="utf-8") as file:
                    writer = csv.DictWriter(file, fieldnames=CSV_HEADER)
                    # 检查文件是否为空（需要写入表头）
                    if file.tell() == 0:
                        writer.writeheader()
                    # 批量写入多行
                    writer.writerows(rows)
            except Exception as exc:
                import logging
                logger = logging.getLogger(__name__)
                logger.error("批量写入CSV错误: %s, 文件: %s", exc, file_path)
    
    def flush(self) -> None:
        """等待所有待写入数据完成"""
        if self.write_queue is not None:
            self.write_queue.join()
    
    def stop(self) -> None:
        """停止异步写入线程"""
        if self.async_write:
            self.write_running = False
            if self.write_thread and self.write_thread.is_alive():
                self.flush()  # 等待队列中的数据写入完成
                self.write_thread.join(timeout=5.0)

    def _write_header(self) -> None:
        if not self.current_path:
            return
        self.current_path.parent.mkdir(parents=True, exist_ok=True)
        with self.current_path.open("w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=CSV_HEADER)
            writer.writeheader()

