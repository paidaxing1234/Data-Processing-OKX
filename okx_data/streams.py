from __future__ import annotations

import asyncio
import functools
import json
import logging
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

from okx.MarketData import MarketAPI
from okx.api.public import Public as PublicAPI
from okx.websocket.WsPublicAsync import WsPublicAsync

from config.settings import (
    EXCHANGE_ID,
    INSTRUMENT_ID,
    INSTRUMENT_TYPE,
    OKX_WS_BUSINESS_URL,
    OKX_WS_PUBLIC_URL,
    PRODUCT_ID,
    PROXY_CONFIG,
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
        use_proxy: bool = None,
        proxy_url: str = None,
    ) -> None:
        self.instrument_id = instrument_id
        self.inst_type = inst_type
        self.sample_interval = sample_interval
        self.product_id = product_id or instrument_id
        self.exchange_id = exchange_id
        
        # 代理配置（支持通过参数覆盖配置文件）
        self.use_proxy = use_proxy if use_proxy is not None else PROXY_CONFIG.get("enabled", False)
        self.proxy_url = proxy_url or PROXY_CONFIG.get("http") or PROXY_CONFIG.get("https")
        
        # 为 REST API 设置代理（requests 库会自动使用环境变量或 session.proxies）
        if self.use_proxy and self.proxy_url:
            import os
            # 设置环境变量，让 requests 库使用代理
            os.environ.setdefault("HTTP_PROXY", self.proxy_url)
            os.environ.setdefault("HTTPS_PROXY", self.proxy_url)
            os.environ.setdefault("http_proxy", self.proxy_url)
            os.environ.setdefault("https_proxy", self.proxy_url)
            logger.info("已启用代理: %s", self.proxy_url)
        
        self.market_api = MarketAPI()
        self.public_api = PublicAPI()
        self.storage = storage or CsvWriter(instrument_id=instrument_id)
        
        # trades-all 使用 business 端点（不需要登录）
        self.ws_trades = WsPublicAsync(OKX_WS_BUSINESS_URL)
        # books5 使用 public 端点（不需要登录）
        self.ws_books = WsPublicAsync(OKX_WS_PUBLIC_URL)
        self.ws_loop: Optional[asyncio.AbstractEventLoop] = None

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
        
        # WebSocket 订单簿数据（books5 频道）
        # 使用时间戳缓冲区，存储最近的books5数据，用于找到最接近聚合点的数据
        self.book_buffer: deque = deque(maxlen=100)  # 最多保留100条数据
        self.book_lock = threading.Lock()  # 订单簿数据锁
        
        # 心跳机制相关变量
        self.last_message_time: Dict[str, float] = {}  # 每个 WebSocket 连接的最后消息时间
        self.ping_tasks: Dict[str, asyncio.Task] = {}  # 每个 WebSocket 连接的心跳任务
        
        # 多线程执行器和API限流
        self.executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="okx_api")
        self.last_api_call_time = 0.0
        self.api_call_interval = 0.1  # API调用最小间隔100ms（限速20次/2秒=每100ms一次）
        self.rate_limit_lock = threading.Lock()  # API限流锁

    def run(self) -> None:
        """在主线程中运行，使用 asyncio 事件循环同时处理 WebSocket 和聚合循环"""
        # 1. 初始化：获取合约乘数（只获取一次）
        self._initialize_contract_multiplier()
        logger.info(
            "WebSocket采集启动: instId=%s, instType=%s, interval=%ss, 合约乘数=%s",
            self.instrument_id,
            self.inst_type,
            self.sample_interval,
            self.contract_multiplier,
        )
        logger.info(
            "数据获取方式: 成交数据(trades-all)和订单簿(books5)通过WebSocket实时推送，"
            "持仓量通过REST API每250ms获取一次（使用oiUsd字段）"
        )
        # 2. 在主线程的事件循环中运行 WebSocket 和聚合循环
        try:
            # 检查是否已有事件循环
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            self.ws_loop = loop
            self.ws_running = True
            self.ws_ready.clear()
            
            # 在主事件循环中同时运行 WebSocket 和聚合循环
            loop.run_until_complete(self._run_main_loop())
        except KeyboardInterrupt:
            logger.info("收到中断信号，正在停止...")
        finally:
            self._stop_websocket()
            self.executor.shutdown(wait=True)
            # 等待所有CSV写入完成
            if hasattr(self.storage, 'stop'):
                self.storage.stop()
    
    async def _run_main_loop(self) -> None:
        """主循环：同时运行 WebSocket 和聚合循环"""
        websocket_task = None
        aggregation_task = None
        
        try:
            # 启动 WebSocket 连接和订阅
            websocket_task = asyncio.create_task(self._websocket_main())
            
            # 等待 WebSocket 连接成功（轮询检查）
            timeout = 30.0  # 增加超时时间，因为可能需要重连
            start_time = time.time()
            while not self.ws_ready.is_set() and self.ws_running:
                if time.time() - start_time > timeout:
                    logger.error("WebSocket连接超时，退出")
                    self.ws_running = False
                    if websocket_task and not websocket_task.done():
                        websocket_task.cancel()
                        try:
                            await websocket_task
                        except (asyncio.CancelledError, Exception):  # noqa: BLE001
                            pass
                    return
                await asyncio.sleep(0.1)
            
            if not self.ws_running:
                return
            
            logger.info("WebSocket已就绪，开始250ms聚合循环")
            
            # 启动聚合循环任务（在后台线程中运行，避免阻塞事件循环）
            aggregation_task = asyncio.create_task(self._aggregation_loop())
            
            # 等待任务完成（通常不会完成，除非出错）
            # 使用 return_exceptions=True 确保所有异常都被捕获
            try:
                results = await asyncio.gather(
                    websocket_task, 
                    aggregation_task,
                    return_exceptions=True
                )
                # 检查是否有异常
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        task_name = "websocket" if i == 0 else "aggregation"
                        logger.error("%s任务异常: %s", task_name, result, exc_info=result)
            except Exception as exc:
                logger.error("主循环异常: %s", exc, exc_info=True)
        except asyncio.CancelledError:
            logger.info("主循环被取消")
        finally:
            # 确保所有任务都被正确取消
            if websocket_task and not websocket_task.done():
                websocket_task.cancel()
                try:
                    await websocket_task
                except (asyncio.CancelledError, Exception):  # noqa: BLE001
                    pass
            
            if aggregation_task and not aggregation_task.done():
                aggregation_task.cancel()
                try:
                    await aggregation_task
                except (asyncio.CancelledError, Exception):  # noqa: BLE001
                    pass
    
    async def _aggregation_loop(self) -> None:
        """聚合循环：每 250ms 执行一次数据聚合"""
        try:
            while self.ws_running:
                cycle_start = time.perf_counter()
                try:
                    # 在后台线程中运行同步的聚合逻辑
                    await asyncio.to_thread(self._collect_once)
                except Exception as exc:  # noqa: BLE001
                    logger.exception("采集周期失败: %s", exc)
                elapsed = time.perf_counter() - cycle_start
                sleep_time = max(self.sample_interval - elapsed, 0.0)
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
        except asyncio.CancelledError:
            logger.info("聚合循环已取消")
        except Exception as exc:
            logger.error("聚合循环异常: %s", exc, exc_info=True)

    def _initialize_contract_multiplier(self) -> None:
        """初始化时只获取一次合约乘数"""
        multiplier = self._fetch_contract_multiplier_with_retry()
        if multiplier is not None:
            self.contract_multiplier = multiplier
            logger.info("合约乘数初始化: %s", self.contract_multiplier)
        else:
            logger.warning("合约乘数获取失败，使用默认值1.0")

    def _collect_once(self) -> None:
        """
        250ms周期聚合：汇总本周期内持续采集的成交数据，并行获取订单簿、持仓量等
        优化：使用快速锁获取数据，减少锁持有时间，CSV写入改为异步
        """
        local_now = datetime.now(timezone.utc).astimezone()
        current_date = local_now.date()
        self._check_day_rollover(current_date)

        # 快速获取本周期内收集的所有新成交（最小化锁持有时间）
        with self.collector_lock:
            if self.collected_trades:
                trades = list(self.collected_trades)
                self.collected_trades.clear()
            else:
                trades = []
        
        # 计算本周期内的成交量和成交额（CPU密集操作，无锁）
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

        # 获取聚合时间点（毫秒时间戳）
        aggregation_ts = update_ts
        
        # 从缓冲区中找到最接近聚合点时间（但不超过）的订单簿数据
        book_data = self._get_book_data_at_time(aggregation_ts)
        if book_data is None:
            logger.warning("订单簿数据为空，缓冲区大小: %d", len(self.book_buffer) if hasattr(self, 'book_buffer') else 0)
        
        # 每250ms通过REST API获取持仓量（使用oiUsd字段）
        open_interest = self._fetch_open_interest_with_retry(max_retries=1)
        if open_interest is not None:
            self.last_open_interest = open_interest

        # 提取订单簿数据（CPU操作，无锁）
        bids, asks = self._extract_orderbook_sides(book_data)
        
        # 调试日志：检查成交数据
        if len(trades) == 0:
            logger.debug("本周期内无成交数据")
        else:
            logger.debug("本周期内成交数据: %d条, 成交量=%.2f, 成交额=%.2f", len(trades), interval_volume, interval_amount)

        # 构建数据点
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
        
        # 异步写入CSV（不阻塞主线程）
        self.storage.write(point, current_date)


    async def _websocket_main(self) -> None:
        """WebSocket 主循环，带自动重连机制"""
        max_reconnect_attempts = 10
        reconnect_delay = 5.0  # 重连延迟（秒）
        reconnect_attempts = 0
        
        try:
            while self.ws_running:
                try:
                    # 直接使用实例方法作为回调，与测试脚本完全一致
                    # 测试脚本中：await self.ws_trades.subscribe(params, self.on_message)
                    # 这里使用相同的方式：self._on_websocket_message
                    
                    # 启动 trades-all WebSocket（business 端点，不需要登录）
                    await self.ws_trades.start()
                    
                    # 检查连接是否真正建立
                    if not hasattr(self.ws_trades, 'websocket') or not self.ws_trades.websocket:
                        raise ConnectionError("trades WebSocket连接失败：websocket对象为None")
                    
                    logger.info("trades-all WebSocket连接成功")
                    
                    params_trades = [
                        {
                            "channel": "trades-all",
                            "instId": self.instrument_id,
                        }
                    ]
                    await self.ws_trades.subscribe(params_trades, self._on_websocket_message)
                    logger.info("已订阅 trades-all 频道: instId=%s", self.instrument_id)
                    # 等待订阅确认消息被处理
                    await asyncio.sleep(1.0)
                    
                    # 启动 books5 WebSocket（public 端点，不需要登录）
                    await self.ws_books.start()
                    
                    # 检查连接是否真正建立
                    if not hasattr(self.ws_books, 'websocket') or not self.ws_books.websocket:
                        raise ConnectionError("books WebSocket连接失败：websocket对象为None")
                    
                    logger.info("books5 WebSocket连接成功")
                    
                    params_book = [
                        {
                            "channel": "books5",
                            "instId": self.instrument_id,
                        }
                    ]
                    await self.ws_books.subscribe(params_book, self._on_websocket_message)
                    logger.info("已订阅 books5 频道: instId=%s", self.instrument_id)
                    # 等待订阅确认
                    await asyncio.sleep(1.0)
                    
                    # 启动心跳任务（根据 OKX 文档要求）
                    self._start_heartbeat("trades")
                    self._start_heartbeat("books")
                    
                    # 标记WebSocket已就绪
                    self.ws_ready.set()
                    logger.info("WebSocket已就绪，等待数据推送...")
                    
                    # 保持连接运行，监控连接状态
                    while self.ws_running:
                        await asyncio.sleep(1.0)
                        # 检查连接状态（安全地检查，因为不同版本的websocket库可能有不同的属性）
                        trades_closed = self._is_websocket_closed(self.ws_trades)
                        books_closed = self._is_websocket_closed(self.ws_books)
                        
                        if trades_closed or books_closed:
                            logger.warning(
                                "检测到WebSocket连接断开: trades=%s, books=%s",
                                trades_closed, books_closed
                            )
                            break
                    
                    # 如果内层循环正常退出（连接断开），重置重连计数器
                    reconnect_attempts = 0
                        
                except asyncio.CancelledError:
                    logger.info("WebSocket任务被取消")
                    break
                except Exception as exc:  # noqa: BLE001
                    logger.error("WebSocket 连接异常: %s", exc, exc_info=True)
                    reconnect_attempts += 1
                    
                    if reconnect_attempts >= max_reconnect_attempts:
                        logger.error("WebSocket重连次数超过限制(%d次)，停止重连", max_reconnect_attempts)
                        self.ws_running = False
                        break
                    
                    logger.info("等待 %d 秒后尝试重连 (第 %d/%d 次)...", 
                               int(reconnect_delay), reconnect_attempts, max_reconnect_attempts)
                    
                    # 清理现有连接
                    try:
                        await self._cleanup_websocket_connections()
                    except Exception as cleanup_exc:  # noqa: BLE001
                        logger.debug("清理WebSocket连接时出错: %s", cleanup_exc)
                    
                    # 重置就绪状态
                    self.ws_ready.clear()
                    
                    # 等待后重连
                    await asyncio.sleep(reconnect_delay)
                    # 指数退避，但不超过60秒
                    reconnect_delay = min(reconnect_delay * 1.5, 60.0)
        finally:
            self.ws_running = False
            await self._cleanup_websocket_connections()
    
    async def _cleanup_websocket_connections(self) -> None:
        """清理WebSocket连接"""
        # 取消心跳任务
        for task_name, task in list(self.ping_tasks.items()):
            if not task.done():
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):  # noqa: BLE001
                    pass
        self.ping_tasks.clear()
        
        # 停止WebSocket连接
        try:
            if hasattr(self.ws_trades, 'stop'):
                await self.ws_trades.stop()
        except Exception as e:  # noqa: BLE001
            logger.debug("停止trades WebSocket时出错: %s", e)
        
        try:
            if hasattr(self.ws_books, 'stop'):
                await self.ws_books.stop()
        except Exception as e:  # noqa: BLE001
            logger.debug("停止books WebSocket时出错: %s", e)
        
        # 重新创建WebSocket客户端（为下次重连做准备）
        self.ws_trades = WsPublicAsync(OKX_WS_BUSINESS_URL)
        self.ws_books = WsPublicAsync(OKX_WS_PUBLIC_URL)

    def _on_websocket_message(self, payload: str) -> None:
        """WebSocket 消息回调：处理成交数据和订单簿数据"""
        try:
            message = json.loads(payload)
        except json.JSONDecodeError:
            logger.warning("WebSocket JSON解析失败: %s", payload[:200])
            return

        # 处理 pong 响应（心跳响应）
        if payload == "pong":
            return

        event = message.get("event")
        arg = message.get("arg", {})
        channel = arg.get("channel") if arg else None
        data = message.get("data")
        
        # 更新最后消息时间（用于心跳机制）
        # 根据 channel 判断是哪个 WebSocket 连接
        if channel == "trades-all":
            self.last_message_time["trades"] = time.time()
        elif channel == "books5":
            self.last_message_time["books"] = time.time()
        elif event == "subscribe":
            # 订阅确认消息，根据 arg 判断是哪个连接
            if "trades-all" in str(arg):
                self.last_message_time["trades"] = time.time()
            elif "books5" in str(arg):
                self.last_message_time["books"] = time.time()

        # 处理订阅确认消息
        if event:
            if event == "subscribe":
                logger.debug("WebSocket 订阅成功: channel=%s, instId=%s", arg.get("channel"), arg.get("instId"))
            elif event == "error":
                logger.error("WebSocket 订阅失败: %s", message.get("msg", message))
            return

        # 处理数据推送（没有event字段的消息）
        if not channel:
            logger.debug("收到无channel的消息: %s", json.dumps(message)[:200])
            return
            
        data_list = data if isinstance(data, list) else []
        
        if channel == "trades-all":
            # 处理成交数据推送
            if data_list:
                logger.debug("收到trades-all数据: %d条成交", len(data_list))
                for trade in data_list:
                    self._process_trade(trade)
        elif channel == "books5":
            # 处理订单簿数据推送（每100ms推送一次，当5档数据变化时）
            if data_list:
                for book_item in data_list:
                    # 获取数据中的时间戳，如果没有则使用当前时间
                    ts_str = book_item.get("ts", "")
                    if ts_str:
                        try:
                            book_ts = int(ts_str)
                        except (ValueError, TypeError):
                            book_ts = int(time.time() * 1000)
                    else:
                        book_ts = int(time.time() * 1000)
                    
                    # 存储到缓冲区（带时间戳）
                    with self.book_lock:
                        self.book_buffer.append({
                            "data": book_item,
                            "ts": book_ts,  # 毫秒时间戳
                        })

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
        sz = trade.get("sz")
        if px is not None:
            try:
                self.last_trade_price = float(px)
            except ValueError:
                pass

        # 加入当前周期队列
        with self.collector_lock:
            self.collected_trades.append(trade)

        # 清理过期的trade_id（保留最近10000个）
        if len(self.seen_trade_ids) > 10000:
            recent_ids = set(list(self.seen_trade_ids)[-5000:])
            self.seen_trade_ids = recent_ids

    def _is_websocket_closed(self, ws_client: WsPublicAsync) -> bool:
        """安全地检查WebSocket连接是否已关闭"""
        try:
            if not hasattr(ws_client, 'websocket') or not ws_client.websocket:
                return True
            
            # 尝试不同的方式检查连接状态
            websocket = ws_client.websocket
            
            # 方法1: 检查closed属性（如果存在）
            if hasattr(websocket, 'closed'):
                return websocket.closed
            
            # 方法2: 检查close_code（如果存在且不为None，说明已关闭）
            if hasattr(websocket, 'close_code') and websocket.close_code is not None:
                return True
            
            # 方法3: 检查状态字符串
            if hasattr(websocket, 'state'):
                state = str(websocket.state).lower()
                if 'closed' in state or 'closing' in state:
                    return True
            
            # 如果都检查不到，假设连接正常
            return False
        except Exception as e:  # noqa: BLE001
            # 如果检查过程中出错，假设连接已关闭
            logger.debug("检查WebSocket状态时出错: %s", e)
            return True
    
    async def _heartbeat_task(self, ws_name: str, ws_client: WsPublicAsync) -> None:
        """心跳任务：根据 OKX 文档要求，如果 N 秒内没有收到新消息，发送 'ping'"""
        ping_interval = 20.0  # 20 秒，小于 30 秒
        try:
            while self.ws_running:
                try:
                    await asyncio.sleep(1.0)  # 每秒检查一次
                    
                    # 检查 WebSocket 连接状态
                    if self._is_websocket_closed(ws_client):
                        logger.debug("WebSocket (%s) 连接已关闭，停止心跳", ws_name)
                        break
                    
                    last_time = self.last_message_time.get(ws_name, 0)
                    elapsed = time.time() - last_time
                    
                    # 如果超过 ping_interval 秒没有收到消息，发送 ping
                    if elapsed >= ping_interval and last_time > 0:
                        try:
                            # 再次检查连接状态
                            if self._is_websocket_closed(ws_client):
                                logger.debug("WebSocket (%s) 连接已关闭，停止心跳", ws_name)
                                break
                            
                            # 发送 ping（根据 OKX 文档，发送字符串 'ping'）
                            await ws_client.websocket.send("ping")
                            logger.debug("发送 ping 到 %s WebSocket", ws_name)
                            # 重置时间，等待 pong 响应
                            self.last_message_time[ws_name] = time.time()
                        except (ConnectionError, OSError, AttributeError) as e:
                            # 连接已关闭，停止心跳任务
                            logger.debug("WebSocket (%s) 连接已断开，停止心跳: %s", ws_name, e)
                            break
                        except Exception as e:  # noqa: BLE001
                            # 其他异常，记录但不中断
                            logger.debug("发送 ping 失败 (%s): %s", ws_name, e)
                            await asyncio.sleep(1.0)  # 短暂等待后继续
                except asyncio.CancelledError:
                    logger.debug("心跳任务 (%s) 被取消", ws_name)
                    break
                except Exception as e:  # noqa: BLE001
                    logger.debug("心跳任务异常 (%s): %s", ws_name, e)
                    await asyncio.sleep(5.0)  # 出错后等待 5 秒再继续
        except asyncio.CancelledError:
            logger.debug("心跳任务 (%s) 被取消", ws_name)
        except Exception as e:  # noqa: BLE001
            logger.warning("心跳任务 (%s) 发生未捕获异常: %s", ws_name, e, exc_info=True)
    
    def _start_heartbeat(self, ws_name: str) -> None:
        """启动心跳任务"""
        if self.ws_loop and self.ws_loop.is_running():
            if ws_name == "trades":
                task = self.ws_loop.create_task(self._heartbeat_task("trades", self.ws_trades))
                self.ping_tasks["trades"] = task
            elif ws_name == "books":
                task = self.ws_loop.create_task(self._heartbeat_task("books", self.ws_books))
                self.ping_tasks["books"] = task
            logger.info("已启动 %s WebSocket 心跳任务", ws_name)

    def _stop_websocket(self) -> None:
        """停止 WebSocket 连接"""
        self.ws_running = False
        # 取消心跳任务
        if self.ws_loop and self.ws_loop.is_running():
            for task in self.ping_tasks.values():
                if not task.done():
                    task.cancel()
            self.ping_tasks.clear()
            # 不再需要停止事件循环，因为它在主线程中
        logger.info("WebSocket 连接已停止")

    def _rate_limit(self) -> None:
        """API调用限流：确保调用间隔，避免SSL错误（线程安全）"""
        with self.rate_limit_lock:
            now = time.perf_counter()
            elapsed = now - self.last_api_call_time
            if elapsed < self.api_call_interval:
                time.sleep(self.api_call_interval - elapsed)
            self.last_api_call_time = time.perf_counter()
    
    def _get_book_data_at_time(self, target_ts: int) -> Optional[Dict]:
        """
        从缓冲区中找到最接近目标时间点（但不超过）的订单簿数据
        返回聚合点之前最接近的数据
        
        Args:
            target_ts: 目标时间戳（毫秒），聚合点的时间戳
            
        Returns:
            最接近聚合点时间（但不超过）的订单簿数据，如果没有则返回最新的数据
        """
        with self.book_lock:
            if not self.book_buffer:
                logger.debug("订单簿缓冲区为空，无法获取数据")
                return None
            
            # 从后往前查找（最新的数据在最后）
            best_match = None
            best_ts = 0
            time_diff = float('inf')  # 时间差，用于找到最接近的数据
            
            for item in reversed(self.book_buffer):
                item_ts = item["ts"]
                # 找到不超过目标时间点的最大时间戳（最接近聚合点之前的数据）
                if item_ts <= target_ts:
                    diff = target_ts - item_ts
                    if diff < time_diff:
                        best_match = item["data"]
                        best_ts = item_ts
                        time_diff = diff
            
            # 如果没找到不超过目标时间的数据，返回最新的数据（作为备用）
            if best_match is None:
                logger.debug(
                    "未找到聚合点之前的数据（目标时间: %d），使用最新数据（时间: %d）",
                    target_ts,
                    self.book_buffer[-1]["ts"] if self.book_buffer else 0
                )
                best_match = self.book_buffer[-1]["data"]
            else:
                logger.debug(
                    "找到最接近的订单簿数据: 目标时间=%d, 数据时间=%d, 时间差=%dms",
                    target_ts, best_ts, time_diff
                )
            
            return best_match

    def _extract_orderbook_sides(
        self, book_data: Optional[Dict]
    ) -> Tuple[List[Dict[str, float]], List[Dict[str, float]]]:
        bids: List[Dict[str, float]] = []
        asks: List[Dict[str, float]] = []

        if not book_data:
            logger.debug("订单簿数据为空，返回空值")
            for _ in range(5):
                bids.append({"price": None, "size": None})
                asks.append({"price": None, "size": None})
            return bids, asks

        bid_entries = book_data.get("bids", [])
        if not bid_entries:
            logger.warning("订单簿数据中没有bids字段或bids为空: %s", list(book_data.keys()))
        
        for raw in bid_entries[:5]:
            try:
                # books5的数据格式是 [price, size, ...] 的数组
                price = float(raw[0])
                size = float(raw[1])
            except (IndexError, ValueError, TypeError) as e:
                logger.warning("解析订单簿bid数据失败: raw=%s, error=%s", raw, e)
                price = None
                size = None
            bids.append({"price": price, "size": size})
        while len(bids) < 5:
            bids.append({"price": None, "size": None})

        ask_entries = book_data.get("asks", [])
        if not ask_entries:
            logger.warning("订单簿数据中没有asks字段或asks为空: %s", list(book_data.keys()))
        
        for raw in ask_entries[:5]:
            try:
                # books5的数据格式是 [price, size, ...] 的数组
                price = float(raw[0])
                size = float(raw[1])
            except (IndexError, ValueError, TypeError) as e:
                logger.warning("解析订单簿ask数据失败: raw=%s, error=%s", raw, e)
                price = None
                size = None
            asks.append({"price": price, "size": size})
        while len(asks) < 5:
            asks.append({"price": None, "size": None})

        return bids, asks

    def _fetch_open_interest_with_retry(self, max_retries: int = 3) -> Optional[float]:
        """
        带重试的持仓量获取（REST API方式）
        每250ms调用一次，使用oiUsd字段（以USD计算的持仓量）
        API限速：20次/2秒 = 每100ms一次，250ms调用一次是安全的
        """
        self._rate_limit()
        for attempt in range(max_retries):
            try:
                response = self.public_api.get_open_interest(
                    instType=self.inst_type, instId=self.instrument_id
                )
                data = response.get("data") or []
                if data:
                    # 使用oiUsd字段（以USD计算的持仓量），更准确
                    oi_usd = data[0].get("oiUsd")
                    if oi_usd:
                        return float(oi_usd)
                    # 如果oiUsd不存在，回退到oi字段
                    oi = data[0].get("oi")
                    if oi:
                        return float(oi)
                if attempt < max_retries - 1:
                    time.sleep(0.1 * (attempt + 1))
            except Exception as exc:  # noqa: BLE001
                if attempt < max_retries - 1:
                    logger.warning("获取持仓量失败(重试 %d/%d): %s", attempt + 1, max_retries, exc)
                    time.sleep(0.1 * (attempt + 1))
                else:
                    logger.error("获取持仓量最终失败: %s", exc)
        return self.last_open_interest

    def _fetch_contract_multiplier_with_retry(self, max_retries: int = 3) -> Optional[float]:
        """带重试的合约乘数获取"""
        for attempt in range(max_retries):
            try:
                response = self.public_api.get_instruments(
                    instType=self.inst_type, instId=self.instrument_id
                )
                data = response.get("data") or []
                for item in data:
                    if item.get("instId") == self.instrument_id:
                        value = item.get("ctMult")
                        return float(value) if value not in (None, "") else None
                if attempt < max_retries - 1:
                    time.sleep(0.2 * (attempt + 1))
            except Exception as exc:  # noqa: BLE001
                if attempt < max_retries - 1:
                    logger.warning("获取合约乘数失败(重试 %d/%d): %s", attempt + 1, max_retries, exc)
                    time.sleep(0.2 * (attempt + 1))
                else:
                    logger.error("获取合约乘数最终失败: %s", exc)
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


