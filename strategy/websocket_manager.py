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

    def set_lighter_config(self, client, market_index: int, account_index: int):
        self.lighter_client = client
        self.lighter_market_index = market_index
        self.account_index = account_index

    def set_callbacks(self, on_lighter_order_filled: Callable = None):
        self.on_lighter_order_filled = on_lighter_order_filled

    async def request_fresh_snapshot(self, ws):
        await ws.send(json.dumps({
            "type": "subscribe",
            "channel": f"order_book/{self.lighter_market_index}"
        }))

    async def handle_lighter_ws(self):
        url = "wss://mainnet.zklighter.elliot.ai/stream"
        cleanup_counter = 0

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
                            self.logger.warning(f"Failed to create auth token: {err}")
                        else:
                            auth_message = {
                                "type": "subscribe",
                                "channel": account_orders_channel,
                                "auth": auth_token
                            }
                            await ws.send(json.dumps(auth_message))
                            self.logger.info(
                                "Subscribed to account orders with auth token (expires in 10 minutes)")
                    except Exception as e:
                        self.logger.warning(f"Error creating auth token: {e}")

                    while not self.stop_flag:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=1)

                            try:
                                data = json.loads(msg)
                            except json.JSONDecodeError as e:
                                self.logger.warning(f"JSON parsing error: {e}")
                                continue

                            timeout_count = 0

                            async with self.order_book_manager.lighter_order_book_lock:
                                if data.get("type") == "subscribed/order_book":
                                    self.order_book_manager.lighter_order_book["bids"].clear()
                                    self.order_book_manager.lighter_order_book["asks"].clear()

                                    order_book = data.get("order_book", {})
                                    if order_book and "offset" in order_book:
                                        self.order_book_manager.lighter_order_book_offset = order_book["offset"]
                                        self.logger.info(
                                            "Initial order book offset: "
                                            f"{self.order_book_manager.lighter_order_book_offset}")

                                    bids = order_book.get("bids", [])
                                    asks = order_book.get("asks", [])

                                    self.order_book_manager.update_lighter_order_book("bids", bids)
                                    self.order_book_manager.update_lighter_order_book("asks", asks)
                                    self.order_book_manager.lighter_snapshot_loaded = True
                                    self.order_book_manager.lighter_order_book_ready = True
                                    self.order_book_manager.update_lighter_bbo()

                                    self.logger.info(
                                        "Lighter order book snapshot loaded with "
                                        f"{len(self.order_book_manager.lighter_order_book['bids'])} bids and "
                                        f"{len(self.order_book_manager.lighter_order_book['asks'])} asks")

                                elif (data.get("type") == "update/order_book" and
                                      self.order_book_manager.lighter_snapshot_loaded):
                                    order_book = data.get("order_book", {})
                                    if not order_book or "offset" not in order_book:
                                        self.logger.warning("Order book update missing offset, skipping")
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
                                            "Order book integrity check failed, requesting fresh snapshot...")
                                        break

                                    self.order_book_manager.update_lighter_bbo()

                                elif data.get("type") == "ping":
                                    await ws.send(json.dumps({"type": "pong"}))

                                elif data.get("type") == "update/account_orders":
                                    orders = data.get("orders", {}).get(str(self.lighter_market_index), [])
                                    for order in orders:
                                        if order.get("status") == "filled" and self.on_lighter_order_filled:
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
                                    self.logger.error(f"Failed to request fresh snapshot: {e}")
                                    break

                        except asyncio.TimeoutError:
                            timeout_count += 1
                            if timeout_count % 3 == 0:
                                self.logger.warning(
                                    f"No message from Lighter websocket for {timeout_count} seconds")
                            continue
                        except websockets.exceptions.ConnectionClosed as e:
                            self.logger.warning(f"Lighter websocket connection closed: {e}")
                            break
                        except websockets.exceptions.WebSocketException as e:
                            self.logger.warning(f"Lighter websocket error: {e}")
                            break
                        except Exception as e:
                            self.logger.error(f"Error in Lighter websocket: {e}")
                            self.logger.error(f"Full traceback: {traceback.format_exc()}")
                            break
            except Exception as e:
                self.logger.error(f"Failed to connect to Lighter websocket: {e}")

            await asyncio.sleep(2)

    def start_lighter_websocket(self):
        if self.lighter_ws_task is None or self.lighter_ws_task.done():
            self.lighter_ws_task = asyncio.create_task(self.handle_lighter_ws())
            self.logger.info("Lighter WebSocket task started")

    def shutdown(self):
        if self.lighter_ws_task and not self.lighter_ws_task.done():
            try:
                self.lighter_ws_task.cancel()
                self.logger.info("Lighter WebSocket task cancelled")
            except Exception as e:
                self.logger.error(f"Error cancelling Lighter WebSocket task: {e}")

        self.stop_flag = True
