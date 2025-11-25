from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from config import settings
from okx_data.storage import CsvWriter
from okx_data.streams import TradeAggregator


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="OKX成交数据250ms聚合任务")
    parser.add_argument(
        "--inst-id",
        default=settings.INSTRUMENT_ID,
        help="OKX合约ID，默认BTC-USDT-SWAP",
    )
    parser.add_argument(
        "--inst-type",
        default=settings.INSTRUMENT_TYPE,
        help="合约类型，默认SWAP",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=settings.SAMPLE_INTERVAL_SECONDS,
        help="聚合间隔(秒)，默认0.25",
    )
    parser.add_argument(
        "--output",
        default=str(settings.OUTPUT_DIR),
        help="CSV输出目录，默认项目output文件夹",
    )
    return parser.parse_args()


def configure_logging() -> None:
    """配置日志系统，分别输出到log/warning/error文件夹"""
    log_file = settings.LOG_DIR / f"okx_collector_{datetime.now().strftime('%Y%m%d')}.log"
    warning_file = settings.WARNING_DIR / f"warning_{datetime.now().strftime('%Y%m%d')}.log"
    error_file = settings.ERROR_DIR / f"error_{datetime.now().strftime('%Y%m%d')}.log"
    
    # 配置根日志
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO))
    root_logger.handlers.clear()
    
    # 通用日志格式
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    
    # 控制台输出
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # 文件输出 - 所有日志
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    
    # 警告日志文件
    warning_handler = logging.FileHandler(warning_file, encoding="utf-8")
    warning_handler.setLevel(logging.WARNING)
    warning_handler.setFormatter(formatter)
    root_logger.addHandler(warning_handler)
    
    # 错误日志文件
    error_handler = logging.FileHandler(error_file, encoding="utf-8")
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    root_logger.addHandler(error_handler)


def main() -> None:
    configure_logging()
    args = parse_args()
    writer = CsvWriter(output_dir=Path(args.output), instrument_id=args.inst_id)
    aggregator = TradeAggregator(
        instrument_id=args.inst_id,
        inst_type=args.inst_type,
        sample_interval=args.interval,
        storage=writer,
    )
    aggregator.run()


if __name__ == "__main__":
    main()

