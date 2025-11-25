# Data-Processing-OKX

基于 OKX 官方 Python SDK（REST API）的高频成交数据采集与 250ms 聚合工具。

## 功能概览

- 通过 REST API 轮询 `Trades` / `Books` / `Open Interest`，保障 250ms 级别的同步采样。
- 每 250ms（可配置）聚合一次成交量与成交额，并从 0 点开始全日累加。
- 成交额 = 成交量 × 成交价 × 合约乘数；合约乘数 `ctMult` 自动来自 `GET /api/v5/public/instruments`。
- 同步记录 `BidPrice1-5 / BidVolume1-5 / AskPrice1-5 / AskVolume1-5` 的瞬时盘口、最新成交价以及持仓量。
- 以「日期/合约ID.csv」的形式按日写入本地 CSV，方便后续分析或回测。
- 模块化脚本结构，可扩展为多合约或多频率任务。

## 依赖安装

```bash
cd Data-Processing-OKX
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## 快速使用

```bash
python scripts/run_trade_collector.py \
  --inst-id BTC-USDT-SWAP \
  --inst-type SWAP \
  --interval 0.25 \
  --output ./output
```

> 默认配置定义在 `config/settings.py`，也可通过环境变量覆盖：

- `OKX_INST_ID`：合约 ID
- `OKX_INST_TYPE`：合约类型（`SWAP` / `FUTURES` 等）
- `OKX_SAMPLE_INTERVAL`：聚合间隔，单位秒
- `OKX_OUTPUT_DIR`：CSV 输出目录

## 输出字段

CSV 包含时间戳、成交统计、累积统计、盘口 5 档、持仓量等字段。文件位于 `output/日期/合约ID.csv`。
