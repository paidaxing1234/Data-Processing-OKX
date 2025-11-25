from __future__ import annotations

import os
from pathlib import Path
from typing import Final

BASE_DIR: Final[Path] = Path(__file__).resolve().parents[1]
OUTPUT_DIR: Final[Path] = Path(
    os.getenv("OKX_OUTPUT_DIR", BASE_DIR / "output")
).resolve()

# Log directories
LOG_DIR: Final[Path] = BASE_DIR / "log"
WARNING_DIR: Final[Path] = BASE_DIR / "warning"
ERROR_DIR: Final[Path] = BASE_DIR / "error"

# Create log directories
LOG_DIR.mkdir(exist_ok=True)
WARNING_DIR.mkdir(exist_ok=True)
ERROR_DIR.mkdir(exist_ok=True)

# Instruments
INSTRUMENT_ID: Final[str] = os.getenv("OKX_INST_ID", "BTC-USDT-SWAP")
INSTRUMENT_TYPE: Final[str] = os.getenv("OKX_INST_TYPE", "SWAP")
PRODUCT_ID: Final[str] = os.getenv("OKX_PRODUCT_ID", INSTRUMENT_ID)
EXCHANGE_ID: Final[str] = os.getenv("OKX_EXCHANGE_ID", "OKX")

# Sampling configuration
SAMPLE_INTERVAL_SECONDS: Final[float] = float(
    os.getenv("OKX_SAMPLE_INTERVAL", "0.25")
)

# WebSocket endpoint provided by OKX SDK
OKX_WS_PUBLIC_URL: Final[str] = os.getenv(
    "OKX_WS_PUBLIC_URL", "wss://ws.okx.com:8443/ws/v5/public"
)

CSV_FILE_TEMPLATE: Final[str] = os.getenv(
    "OKX_CSV_TEMPLATE", "{date}/{inst_id}.csv"
)

LOG_LEVEL: Final[str] = os.getenv("OKX_LOG_LEVEL", "INFO")

