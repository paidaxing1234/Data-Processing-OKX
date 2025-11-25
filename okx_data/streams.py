from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

from okx.MarketData import MarketAPI
from okx.api.public import Public as PublicAPI
from okx.websocket.WsPublicAsync import WsPublicAsync

from config.settings import (
    EXCHANGE_ID,
    INSTRUMENT_ID,
    INSTRUMENT_TYPE,
    OKX_WS_PUBLIC_URL,
    PRODUCT_ID,
    SAMPLE_INTERVAL_SECONDS,
)
from okx_data.models import AggregatedPoint
from okx_data.storage import CsvWriter

logger = logging.getLogger(__name__)


class TradeAggregator:
    def __init__(
        self,
        instrument_id: str = INSTRUMENT_ID,
        inst_type: str = INSTRUMENT_TYPE,
        sample_interval: float = SAMPLE_INTERVAL_SECONDS,
        storage: Optional[CsvWriter] = None,
        product_id: str = PRODUCT_ID,
        exchange_id: str = EXCHANGE_ID,
    ) -> None:
        self.instrument_id = instrument_id
        self.inst_type = inst_type
        self.sample_interval = sample_interval
        self.product_id = product_id or instrument_id
        self.exchange_id = exchange_id
        self.market_api = MarketAPI()
        self.public_api = PublicAPI()
        self.storage = storage or CsvWriter(instrument_id=instrument_id)
        
        # WebSocket 连接
        self.ws = WsPublicAsync(OKX_WS_PUBLIC_URL)
        self.ws_loop: Optional[asyncio.AbstractEventLoop] = None
        self.ws_thread: Optional[threading.Thread] = None

        self.contract_multiplier: float = 1.0
        self.last_trade_id: Optional[str] = None
        self.last_trade_price: Optional[float] = None
        self.last_open_interest: Optional[float] = None
        self.daily_volume = 0.0
        self.daily_amount = 0.0
        self.current_day = datetime.now(timezone.utc).date()
        
        # WebSocket 成交数据采集相关变量
        self.seen_trade_ids: Set[str] = set()  # 已处理的成交ID集合，用于去重
        self.collected_trades: deque = deque()  # 当前周期内收集的成交数据
        self.collector_lock = threading.Lock()
        self.ws_running = False
        self.ws_ready = threading.Event()  # WebSocket连接就绪事件
        self.executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="okx_api")  # 用于并行API调用
        self.last_api_call_time = 0.0
        self.api_call_interval = 0.1  # API调用最小间隔100ms，避免过快

    def run(self) -> None:
        # 1. 首先获取合约乘数（只获取一次）
        self._fetch_contract_multiplier_once()
        logger.info(
            "WebSocket采集启动: instId=%s, instType=%s, interval=%ss, 合约乘数=%s",
            self.instrument_id,
            self.inst_type,
            self.sample_interval,
            self.contract_multiplier,
        )
        # 2. 启动 WebSocket 连接和订阅，等待连接成功
        self._start_websocket()
        # 3. 等待WebSocket连接成功后再进入聚合循环
        if not self.ws_ready.wait(timeout=10.0):
            logger.error("WebSocket连接超时，退出")
            return
        logger.info("WebSocket已就绪，开始250ms聚合循环")
        try:
            while True:
                cycle_start = time.perf_counter()
                try:
                    self._collect_once()
                except Exception as exc:  # noqa: BLE001
                    logger.exception("采集周期失败: %s", exc)
                elapsed = time.perf_counter() - cycle_start
                sleep_time = max(self.sample_interval - elapsed, 0.0)
                if sleep_time > 0:
                    time.sleep(sleep_time)
        finally:
            self._stop_websocket()
            self.executor.shutdown(wait=True)

    def _fetch_contract_multiplier_once(self) -> None:
        """初始化时只获取一次合约乘数"""
        multiplier = self._fetch_contract_multiplier_with_retry()
        if multiplier is not None:
            self.contract_multiplier = multiplier
            logger.info("合约乘数初始化: %s", self.contract_multiplier)
        else:
            logger.warning("合约乘数获取失败，使用默认值1.0")

    def _collect_once(self) -> None:
        """250ms周期聚合：汇总本周期内持续采集的成交数据"""
        local_now = datetime.now(timezone.utc).astimezone()
        current_date = local_now.date()
        self._check_day_rollover(current_date)
        self._refresh_contract_multiplier()

        # 获取本周期内收集的所有新成交（已去重）
        with self.collector_lock:
            trades = list(self.collected_trades)
            self.collected_trades.clear()
        
        # 计算本周期内的成交量和成交额
        interval_volume = sum(abs(float(t["sz"])) for t in trades)
        interval_amount = sum(
            abs(float(t["sz"])) * float(t["px"]) * self.contract_multiplier
            for t in trades
        )
        self.daily_volume += interval_volume
        self.daily_amount += interval_amount

        # 时间戳格式
        trading_day_ts = int(local_now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
        update_ts = int(local_now.timestamp() * 1000)
        local_cpu_ts = update_ts

        book_data = self._fetch_orderbook()
        bids, asks = self._extract_orderbook_sides(book_data)
        self.last_open_interest = self._fetch_open_interest()

        point = AggregatedPoint(
            instrument_id=self.instrument_id,
            product_id=self.product_id,
            exchange_id=self.exchange_id,
            trading_day=str(trading_day_ts),
            open_interest=self.last_open_interest,
            turnover=self.daily_amount,
            update_millisec=update_ts,
            update_time=str(update_ts),
            last_price=self.last_trade_price,
            volume=self.daily_volume,
            local_cpu_time=str(local_cpu_ts),
            bids=bids,
            asks=asks,
        )
        self.storage.write(point, current_date)

    def _start_websocket(self) -> None:
        """启动 WebSocket 连接并订阅 trades-all 频道"""
        self.ws_running = True
        self.ws_loop = asyncio.new_event_loop()
        self.ws_thread = threading.Thread(
            target=self._run_websocket_loop, daemon=True, args=(self.ws_loop,)
        )
        self.ws_thread.start()
        # 等待 WebSocket 连接建立
        time.sleep(1.0)
        logger.info("WebSocket 连接已启动，已订阅 trades-all 频道")

    def _run_websocket_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """在新线程中运行 WebSocket 事件循环"""
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._websocket_main())

    async def _websocket_main(self) -> None:
        """WebSocket 主循环"""
        try:
            # 启动 WebSocket（会自动连接并创建 consume 任务）
            await self.ws.start()
            # 订阅 trades-all 频道
            params = [
                {
                    "channel": "trades-all",
                    "instId": self.instrument_id,
                }
            ]
            await self.ws.subscribe(params, self._on_websocket_message)
            logger.info("已订阅 trades-all 频道: instId=%s", self.instrument_id)
            # 保持连接运行
            while self.ws_running:
                await asyncio.sleep(1.0)
        except Exception as exc:  # noqa: BLE001
            logger.error("WebSocket 连接异常: %s", exc)
        finally:
            self.ws_running = False
            try:
                await self.ws.stop()
            except Exception:  # noqa: BLE001
                pass

    def _on_websocket_message(self, payload: str) -> None:
        """WebSocket 消息回调：处理成交数据"""
        try:
            message = json.loads(payload)
        except json.JSONDecodeError:
            logger.warning("WebSocket JSON解析失败: %s", payload)
            return

        # 处理订阅确认消息
        if message.get("event"):
            event = message.get("event")
            if event == "subscribe":
                logger.info("WebSocket 订阅成功: %s", message.get("arg"))
            elif event == "error":
                logger.error("WebSocket 订阅失败: %s", message)
            return

        # 处理成交数据推送
        arg = message.get("arg", {})
        channel = arg.get("channel")
        if channel == "trades-all":
            data = message.get("data") or []
            for trade in data:
                self._process_trade(trade)

    def _process_trade(self, trade: Dict[str, str]) -> None:
        """处理单笔成交数据：去重并加入队列"""
        trade_id = trade.get("tradeId")
        if not trade_id:
            return

        # 去重检查
        if trade_id in self.seen_trade_ids:
            return

        # 记录已处理的成交ID
        self.seen_trade_ids.add(trade_id)

        # 更新最新成交价
        px = trade.get("px")
        if px is not None:
            try:
                self.last_trade_price = float(px)
            except ValueError:
                pass

        # 加入当前周期队列
        with self.collector_lock:
            self.collected_trades.append(trade)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "WS Trade captured: id=%s px=%s sz=%s",
                trade_id,
                trade.get("px"),
                trade.get("sz"),
            )

        # 清理过期的trade_id（保留最近10000个）
        if len(self.seen_trade_ids) > 10000:
            recent_ids = set(list(self.seen_trade_ids)[-5000:])
            self.seen_trade_ids = recent_ids

    def _stop_websocket(self) -> None:
        """停止 WebSocket 连接"""
        self.ws_running = False
        if self.ws_loop and self.ws_loop.is_running():
            self.ws_loop.call_soon_threadsafe(self.ws_loop.stop)
        if self.ws_thread:
            self.ws_thread.join(timeout=2.0)
        logger.info("WebSocket 连接已停止")

    def _fetch_orderbook(self) -> Optional[Dict]:
        try:
            result = self.market_api.get_orderbook(self.instrument_id, sz="5")
            if result.get("code") == "0" and result.get("data"):
                return result["data"][0]
            logger.warning("获取订单簿失败: %s", result.get("msg", "未知错误"))
            return None
        except Exception as exc:  # noqa: BLE001
            logger.error("调用订单簿API异常: %s", exc)
            return None

    def _extract_orderbook_sides(
        self, book_data: Optional[Dict]
    ) -> Tuple[List[Dict[str, float]], List[Dict[str, float]]]:
        bids: List[Dict[str, float]] = []
        asks: List[Dict[str, float]] = []

        if not book_data:
            for _ in range(5):
                bids.append({"price": None, "size": None})
                asks.append({"price": None, "size": None})
            return bids, asks

        bid_entries = book_data.get("bids", [])
        for raw in bid_entries[:5]:
            try:
                price = float(raw[0])
                size = float(raw[1])
            except (IndexError, ValueError, TypeError):
                price = None
                size = None
            bids.append({"price": price, "size": size})
        while len(bids) < 5:
            bids.append({"price": None, "size": None})

        ask_entries = book_data.get("asks", [])
        for raw in ask_entries[:5]:
            try:
                price = float(raw[0])
                size = float(raw[1])
            except (IndexError, ValueError, TypeError):
                price = None
                size = None
            asks.append({"price": price, "size": size})
        while len(asks) < 5:
            asks.append({"price": None, "size": None})

        return bids, asks

    def _fetch_open_interest(self) -> Optional[float]:
        try:
            response = self.public_api.get_open_interest(
                instType=self.inst_type, instId=self.instrument_id
            )
            data = response.get("data") or []
            if data:
                return float(data[0].get("oi") or 0)
        except Exception as exc:  # noqa: BLE001
            logger.error("获取持仓量失败: %s", exc)
        return self.last_open_interest

    def _refresh_contract_multiplier(self, initial: bool = False) -> None:
        multiplier = self._fetch_contract_multiplier()
        if multiplier is None:
            return
        changed = abs(multiplier - self.contract_multiplier) > 1e-12
        if initial or changed:
            self.contract_multiplier = multiplier
            logger.info(
                "合约乘数%s: %s",
                "初始化" if initial else "更新",
                self.contract_multiplier,
            )

    def _fetch_contract_multiplier(self) -> Optional[float]:
        try:
            response = self.public_api.get_instruments(
                instType=self.inst_type, instId=self.instrument_id
            )
            data = response.get("data") or []
            for item in data:
                if item.get("instId") == self.instrument_id:
                    value = item.get("ctMult")
                    return float(value) if value not in (None, "") else None
        except Exception as exc:  # noqa: BLE001
            logger.error("获取合约乘数失败: %s", exc)
        return None


    def _check_day_rollover(self, today: date) -> None:
        if today == self.current_day:
            return
        logger.info("检测到新的一天，重置累计指标: %s -> %s", self.current_day, today)
        self.current_day = today
        self.daily_volume = 0.0
        self.daily_amount = 0.0
        # 清空已处理的成交ID集合，新的一天重新开始
        self.seen_trade_ids.clear()
        with self.collector_lock:
            self.collected_trades.clear()
        self.storage.rotate(today)


