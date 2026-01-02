"""WebSocket management for Lighter exchange."""
import asyncio
import json
import logging
import time
import traceback
import websockets
from typing import Callable, Optional


class WebSocketManagerWrapper:
    """Manages WebSocket connections for Lighter."""

    def __init__(self, order_book_manager, logger: logging.Logger):
        self.order_book_manager = order_book_manager
        self.logger = logger
        self.stop_flag = False

        self.lighter_ws_task: Optional[asyncio.Task] = None
        self.lighter_client = None
        self.lighter_market_index: Optional[int] = None
        self.account_index: Optional[int] = None

        self.on_lighter_order_filled: Optional[Callable] = None
        self._resync_requested = False
        self._max_reconnect_delay = 30
        self._max_idle_seconds = 30

    def set_lighter_config(self, client, market_index: int, account_index: int):
        self.lighter_client = client
        self.lighter_market_index = market_index
        self.account_index = account_index

    def set_callbacks(self, on_lighter_order_filled: Callable = None):
        self.on_lighter_order_filled = on_lighter_order_filled

    def request_resync(self):
        self._resync_requested = True

    async def request_fresh_snapshot(self, ws):
        await ws.send(json.dumps({
            "type": "subscribe",
            "channel": f"order_book/{self.lighter_market_index}"
        }))

    async def handle_lighter_ws(self):
        url = "wss://mainnet.zklighter.elliot.ai/stream"
        cleanup_counter = 0
        reconnect_delay = 1

        while not self.stop_flag:
            timeout_count = 0
            try:
                await self.order_book_manager.reset_lighter_order_book()

                async with websockets.connect(url) as ws:
                    await ws.send(json.dumps({
                        "type": "subscribe",
                        "channel": f"order_book/{self.lighter_market_index}"
                    }))

                    account_orders_channel = (
                        f"account_orders/{self.lighter_market_index}/{self.account_index}"
                    )

                    try:
                        ten_minutes_deadline = int(time.time() + 10 * 60)
                        auth_token, err = self.lighter_client.create_auth_token_with_expiry(
                            ten_minutes_deadline)
                        if err is not None:
                            self.logger.warning(f"创建认证令牌失败: {err}")
                        else:
                            auth_message = {
                                "type": "subscribe",
                                "channel": account_orders_channel,
                                "auth": auth_token
                            }
                            await ws.send(json.dumps(auth_message))
                            self.logger.info(
                                "已使用认证令牌订阅账户订单（10分钟后过期）")
                    except Exception as e:
                        self.logger.warning(f"创建认证令牌时出错: {e}")

                    while not self.stop_flag:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=1)

                            try:
                                data = json.loads(msg)
                            except json.JSONDecodeError as e:
                                self.logger.warning(f"JSON 解析错误: {e}")
                                continue

                            timeout_count = 0
                            if reconnect_delay != 1:
                                reconnect_delay = 1

                            async with self.order_book_manager.lighter_order_book_lock:
                                if data.get("type") == "subscribed/order_book":
                                    self.order_book_manager.lighter_order_book["bids"].clear()
                                    self.order_book_manager.lighter_order_book["asks"].clear()

                                    order_book = data.get("order_book", {})
                                    if order_book and "offset" in order_book:
                                        self.order_book_manager.lighter_order_book_offset = order_book["offset"]
                                        self.logger.info(
                                            "初始订单簿 offset: "
                                            f"{self.order_book_manager.lighter_order_book_offset}")

                                    bids = order_book.get("bids", [])
                                    asks = order_book.get("asks", [])

                                    self.order_book_manager.update_lighter_order_book("bids", bids)
                                    self.order_book_manager.update_lighter_order_book("asks", asks)
                                    self.order_book_manager.lighter_snapshot_loaded = True
                                    self.order_book_manager.lighter_order_book_ready = True
                                    self.order_book_manager.update_lighter_bbo()
                                    self.order_book_manager.mark_lighter_snapshot()

                                    self.logger.info(
                                        "Lighter 订单簿快照加载完成，"
                                        f"买单 {len(self.order_book_manager.lighter_order_book['bids'])}，"
                                        f"卖单 {len(self.order_book_manager.lighter_order_book['asks'])}")

                                elif (data.get("type") == "update/order_book" and
                                      self.order_book_manager.lighter_snapshot_loaded):
                                    order_book = data.get("order_book", {})
                                    if not order_book or "offset" not in order_book:
                                        self.logger.warning("订单簿更新缺少 offset，已跳过")
                                        continue

                                    new_offset = order_book["offset"]

                                    if not self.order_book_manager.validate_order_book_offset(new_offset):
                                        self.order_book_manager.lighter_order_book_sequence_gap = True
                                        break

                                    self.order_book_manager.update_lighter_order_book(
                                        "bids", order_book.get("bids", []))
                                    self.order_book_manager.update_lighter_order_book(
                                        "asks", order_book.get("asks", []))

                                    if not self.order_book_manager.validate_order_book_integrity():
                                        self.logger.warning(
                                            "订单簿完整性校验失败，正在请求新快照")
                                        break

                                    self.order_book_manager.update_lighter_bbo()
                                    self.order_book_manager.mark_lighter_update()

                                elif data.get("type") == "ping":
                                    await ws.send(json.dumps({"type": "pong"}))

                                elif data.get("type") == "update/account_orders":
                                    orders = data.get("orders", {}).get(str(self.lighter_market_index), [])
                                    for order in orders:
                                        status = str(order.get("status", "")).lower()
                                        if status == "filled" and self.on_lighter_order_filled:
                                            self.on_lighter_order_filled(order)

                                elif (data.get("type") == "update/order_book" and
                                      not self.order_book_manager.lighter_snapshot_loaded):
                                    continue

                            cleanup_counter += 1
                            if cleanup_counter >= 1000:
                                cleanup_counter = 0

                            if self.order_book_manager.lighter_order_book_sequence_gap:
                                try:
                                    await self.request_fresh_snapshot(ws)
                                    self.order_book_manager.lighter_order_book_sequence_gap = False
                                except Exception as e:
                                    self.logger.error(f"请求新快照失败: {e}")
                                    break
                            if self._resync_requested:
                                try:
                                    await self.request_fresh_snapshot(ws)
                                    self._resync_requested = False
                                except Exception as e:
                                    self.logger.error(f"请求新快照失败: {e}")
                                    break

                        except asyncio.TimeoutError:
                            timeout_count += 1
                            if timeout_count % 3 == 0:
                                self.logger.warning(
                                    f"Lighter WebSocket {timeout_count} 秒未收到消息")
                            if timeout_count >= self._max_idle_seconds:
                                self.logger.warning(
                                    f"Lighter WebSocket {timeout_count} 秒未收到消息，正在重连")
                                break
                            continue
                        except websockets.exceptions.ConnectionClosed as e:
                            self.logger.warning(f"Lighter WebSocket 连接已关闭: {e}")
                            break
                        except websockets.exceptions.WebSocketException as e:
                            self.logger.warning(f"Lighter WebSocket 错误: {e}")
                            break
                        except Exception as e:
                            self.logger.error(f"Lighter WebSocket 发生错误: {e}")
                            self.logger.error(f"完整堆栈: {traceback.format_exc()}")
                            break
            except Exception as e:
                self.logger.error(f"连接 Lighter WebSocket 失败: {e}")

            if self.stop_flag:
                break

            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, self._max_reconnect_delay)

    def start_lighter_websocket(self):
        if self.lighter_ws_task is None or self.lighter_ws_task.done():
            self.lighter_ws_task = asyncio.create_task(self.handle_lighter_ws())
            self.logger.info("Lighter WebSocket 任务已启动")

    def shutdown(self):
        if self.lighter_ws_task and not self.lighter_ws_task.done():
            try:
                self.lighter_ws_task.cancel()
                self.logger.info("Lighter WebSocket 任务已取消")
            except Exception as e:
                self.logger.error(f"取消 Lighter WebSocket 任务失败: {e}")

        self.stop_flag = True
