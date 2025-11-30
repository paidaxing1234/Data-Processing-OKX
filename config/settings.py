from __future__ import annotations

import os
from pathlib import Path
from typing import Final

BASE_DIR: Final[Path] = Path(__file__).resolve().parents[1]
OUTPUT_DIR: Final[Path] = Path(
    os.getenv("OKX_OUTPUT_DIR", BASE_DIR / "output")
).resolve()

# Log directories
LOG_DIR: Final[Path] = BASE_DIR / "logs" / "log"
WARNING_DIR: Final[Path] = BASE_DIR / "logs" / "warning"
ERROR_DIR: Final[Path] = BASE_DIR / "logs" / "error"

# Create log directories (with parents=True to create parent directories)
LOG_DIR.mkdir(parents=True, exist_ok=True)
WARNING_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

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
OKX_WS_BUSINESS_URL: Final[str] = os.getenv(
    "OKX_WS_BUSINESS_URL", "wss://ws.okx.com:8443/ws/v5/business"
)

# WebSocket 登录配置（用于订阅 books50-l2-tbt 频道）
OKX_API_KEY: Final[str] = os.getenv("OKX_API_KEY", "")
OKX_SECRET_KEY: Final[str] = os.getenv("OKX_SECRET_KEY", "")
OKX_PASSPHRASE: Final[str] = os.getenv("OKX_PASSPHRASE", "")

CSV_FILE_TEMPLATE: Final[str] = os.getenv(
    "OKX_CSV_TEMPLATE", "{date}/{inst_id}.csv"
)

LOG_LEVEL: Final[str] = os.getenv("OKX_LOG_LEVEL", "INFO")

# 代理配置（如需要）
# 使用方式：
# 1. 直接修改配置文件中的 enabled 和地址
# 2. 通过命令行参数 --proxy 和 --proxy-url
# 3. 通过环境变量 OKX_PROXY_URL
PROXY_CONFIG: Final[dict] = {
    "enabled": False,  # 默认不启用代理
    "http": "socks5h://127.0.0.1:7890",  # HTTP代理地址（SOCKS5示例）
    "https": "socks5h://127.0.0.1:7890",  # HTTPS代理地址
}

# 如果设置了环境变量，则覆盖配置
if os.getenv("OKX_PROXY_URL"):
    PROXY_CONFIG["enabled"] = True
    proxy_url = os.getenv("OKX_PROXY_URL")
    PROXY_CONFIG["http"] = proxy_url
    PROXY_CONFIG["https"] = proxy_url

