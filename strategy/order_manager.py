"""Order placement and monitoring for Paradex and Lighter exchanges."""
import asyncio
import logging
import os
import time
from decimal import Decimal
from typing import Optional, Tuple

from lighter.signer_client import SignerClient
from paradex_py.common.order import Order, OrderSide, OrderType


class OrderManager:
    """Manages order placement and monitoring for both exchanges."""

    def __init__(self, order_book_manager, logger: logging.Logger):
        self.order_book_manager = order_book_manager
        self.logger = logger

        self.paradex_client = None
        self.paradex_market: Optional[str] = None

        self.lighter_client: Optional[SignerClient] = None
        self.lighter_market_index: Optional[int] = None
        self.base_amount_multiplier: Optional[int] = None
        self.price_multiplier: Optional[int] = None
        self.tick_size: Optional[Decimal] = None

        self.lighter_order_filled = False
        self.lighter_order_price: Optional[Decimal] = None
        self.lighter_order_side: Optional[str] = None
        self.lighter_order_size: Optional[Decimal] = None

        self.order_execution_complete = False
        self.waiting_for_lighter_fill = False

        self.on_order_filled: Optional[callable] = None
        self.lighter_fill_event = asyncio.Event()
        self.lighter_position_fetcher: Optional[callable] = None
        self._lighter_position_baseline: Optional[Decimal] = None
        self.log_decimals = int(os.getenv("LOG_DECIMALS", "4"))
        self.log_decimals = max(0, min(self.log_decimals, 8))
        self.lighter_taker_slippage = Decimal(os.getenv("LIGHTER_TAKER_SLIPPAGE", "0.0002"))
        self.lighter_use_market_order = os.getenv("LIGHTER_TAKER_USE_MARKET", "1").lower() in (
            "1", "true", "yes"
        )
        self.lighter_force_ioc = os.getenv("LIGHTER_TAKER_FORCE_IOC", "1").lower() in (
            "1", "true", "yes"
        )

    def set_paradex_config(self, client, market: str):
        self.paradex_client = client
        self.paradex_market = market

    def set_lighter_config(self, client: SignerClient, market_index: int,
                           base_amount_multiplier: int, price_multiplier: int, tick_size: Decimal):
        self.lighter_client = client
        self.lighter_market_index = market_index
        self.base_amount_multiplier = base_amount_multiplier
        self.price_multiplier = price_multiplier
        self.tick_size = tick_size

    def set_callbacks(self, on_order_filled: callable = None):
        self.on_order_filled = on_order_filled

    def set_lighter_position_fetcher(self, fetcher: callable = None):
        self.lighter_position_fetcher = fetcher

    async def _run_paradex(self, func, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: func(*args, **kwargs))

    def _normalize_decimal(self, value) -> Optional[Decimal]:
        if value is None:
            return None
        try:
            return Decimal(str(value))
        except Exception:
            return None

    def _extract_price_from_level(self, level) -> Optional[Decimal]:
        if isinstance(level, (list, tuple)) and level:
            return self._normalize_decimal(level[0])
        if isinstance(level, dict):
            for key in ("price", "px", "rate", "value"):
                price = self._normalize_decimal(level.get(key))
                if price is not None:
                    return price
        return None

    def _extract_bbo(self, payload) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        if not isinstance(payload, dict):
            return None, None

        data = payload.get("data", payload)
        if isinstance(data, dict) and "bbo" in data and isinstance(data["bbo"], dict):
            data = data["bbo"]

        bid = self._normalize_decimal(
            data.get("bid") or data.get("best_bid") or data.get("bid_price") or data.get("bid_px")
        )
        ask = self._normalize_decimal(
            data.get("ask") or data.get("best_ask") or data.get("ask_price") or data.get("ask_px")
        )

        if bid is None and isinstance(data.get("bid"), dict):
            bid = self._extract_price_from_level(data.get("bid"))
        if ask is None and isinstance(data.get("ask"), dict):
            ask = self._extract_price_from_level(data.get("ask"))

        if bid is None or ask is None:
            bids = data.get("bids") or data.get("bid_levels") or data.get("levels_bids")
            asks = data.get("asks") or data.get("ask_levels") or data.get("levels_asks")
            if bids:
                bid = self._extract_price_from_level(bids[0])
            if asks:
                ask = self._extract_price_from_level(asks[0])

        return bid, ask

    def _extract_order_price(self, payload) -> Optional[Decimal]:
        if not isinstance(payload, dict):
            return None
        data = payload.get("result") or payload.get("order") or payload
        for key in ("avg_price", "avg_fill_price", "filled_avg_price", "fill_price", "price"):
            price = self._normalize_decimal(data.get(key))
            if price is not None:
                return price
        return None

    def _extract_order_id(self, payload) -> Optional[str]:
        if not isinstance(payload, dict):
            return None
        data = payload.get("result") or payload.get("order") or payload
        for key in ("id", "order_id"):
            value = data.get(key)
            if value:
                return str(value)
        return None

    def _format_value(self, value) -> str:
        if value is None:
            return "N/A"
        try:
            return f"{float(value):.{self.log_decimals}f}"
        except (TypeError, ValueError):
            return str(value)

    async def fetch_paradex_mid_price(self) -> Decimal:
        if not self.paradex_client or not self.paradex_market:
            raise Exception("Paradex 客户端未初始化")

        bbo_data = await self._run_paradex(
            self.paradex_client.api_client.fetch_bbo,
            market=self.paradex_market,
        )
        bid, ask = self._extract_bbo(bbo_data)

        if bid is None or ask is None:
            orderbook = await self._run_paradex(
                self.paradex_client.api_client.fetch_orderbook,
                market=self.paradex_market,
                params={"depth": 1},
            )
            bid, ask = self._extract_bbo(orderbook)

        if bid is None or ask is None:
            raise Exception("Paradex BBO 不可用")
        if bid <= 0 or ask <= 0 or bid >= ask:
            raise Exception("Paradex 买卖价无效")

        return (bid + ask) / Decimal('2')

    async def place_paradex_market_order(self, side: str, quantity: Decimal) -> Optional[Decimal]:
        if not self.paradex_client or not self.paradex_market:
            raise Exception("Paradex 客户端未初始化")

        order_side = OrderSide.Buy if side.lower() == 'buy' else OrderSide.Sell
        client_order_id = f"arb_{int(time.time() * 1000)}"
        order = Order(
            market=self.paradex_market,
            order_type=OrderType.Market,
            order_side=order_side,
            size=quantity,
            client_id=client_order_id,
        )

        try:
            response = await self._run_paradex(
                self.paradex_client.api_client.submit_order,
                order=order,
            )
            order_id = self._extract_order_id(response)
            price = self._extract_order_price(response)
            if price is None and order_id:
                order_info = await self._run_paradex(
                    self.paradex_client.api_client.fetch_order,
                    order_id=order_id,
                )
                price = self._extract_order_price(order_info)

            if price is None:
                price = Decimal('0')

            self.logger.info(
                f"[{client_order_id}] [Paradex] [{side.upper()}] [开仓]: "
                f"{self._format_value(quantity)}"
            )
            return price
        except Exception as e:
            self.logger.error(f"下 Paradex 订单失败: {e}")
            return None

    async def place_lighter_market_order(self, lighter_side: str, quantity: Decimal,
                                         price: Decimal, stop_flag) -> Optional[str]:
        if not self.lighter_client:
            raise Exception("Lighter 客户端未初始化")

        best_bid, best_ask = self.order_book_manager.get_lighter_best_levels()
        if not best_bid or not best_ask:
            raise Exception("Lighter 订单簿未就绪")

        if lighter_side.lower() == 'buy':
            order_type = "CLOSE"
            is_ask = False
            slippage = self.lighter_taker_slippage
            if slippage < 0:
                slippage = Decimal("0")
            if slippage >= Decimal("1"):
                self.logger.warning("LIGHTER_TAKER_SLIPPAGE >= 1，已限制为 0.1")
                slippage = Decimal("0.1")
            price = best_ask[0] * (Decimal("1") + slippage)
        else:
            order_type = "OPEN"
            is_ask = True
            slippage = self.lighter_taker_slippage
            if slippage < 0:
                slippage = Decimal("0")
            if slippage >= Decimal("1"):
                self.logger.warning("LIGHTER_TAKER_SLIPPAGE >= 1，已限制为 0.1")
                slippage = Decimal("0.1")
            price = best_bid[0] * (Decimal("1") - slippage)

        self.lighter_order_filled = False
        self.lighter_order_price = price
        self.lighter_order_side = lighter_side
        self.lighter_order_size = quantity
        self.waiting_for_lighter_fill = True
        self.lighter_fill_event.clear()
        self._lighter_position_baseline = None
        if self.lighter_position_fetcher:
            try:
                self._lighter_position_baseline = await self.lighter_position_fetcher()
            except Exception as e:
                self.logger.warning(f"获取 Lighter 仓位失败: {e}")

        try:
            client_order_index = int(time.time() * 1000)
            order_type_value = getattr(self.lighter_client, "ORDER_TYPE_LIMIT", None)
            market_type = getattr(self.lighter_client, "ORDER_TYPE_MARKET", None)
            if self.lighter_use_market_order:
                if market_type is not None:
                    order_type_value = market_type
                else:
                    self.logger.warning("无法使用市价单类型，改用限价单")

            time_in_force_value = self.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME
            force_ioc = self.lighter_force_ioc or (
                market_type is not None and order_type_value == market_type
            )
            if force_ioc:
                ioc_value = getattr(
                    self.lighter_client, "ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL", None
                )
                if ioc_value is None:
                    ioc_value = getattr(self.lighter_client, "ORDER_TIME_IN_FORCE_IOC", None)
                if ioc_value is not None:
                    time_in_force_value = ioc_value
                else:
                    self.logger.warning("无法使用 IOC 时效，改用 GTT")

            tx_info, error = self.lighter_client.sign_create_order(
                market_index=self.lighter_market_index,
                client_order_index=client_order_index,
                base_amount=int(quantity * self.base_amount_multiplier),
                price=int(price * self.price_multiplier),
                is_ask=is_ask,
                order_type=order_type_value,
                time_in_force=time_in_force_value,
                reduce_only=False,
                trigger_price=0,
            )
            if error is not None:
                raise Exception(f"签名失败: {error}")

            tx_hash = await self.lighter_client.send_tx(
                tx_type=self.lighter_client.TX_TYPE_CREATE_ORDER,
                tx_info=tx_info
            )

            self.logger.info(
                f"[{client_order_index}] [{order_type}] [Lighter] [开仓]: "
                f"{self._format_value(quantity)}"
            )

            await self.monitor_lighter_order(client_order_index, stop_flag)

            return tx_hash
        except Exception as e:
            self.logger.error(f"下 Lighter 订单失败: {e}")
            self.waiting_for_lighter_fill = False
            return None

    async def monitor_lighter_order(self, client_order_index: int, stop_flag):
        if stop_flag:
            return

        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline and not stop_flag:
                remaining = max(0.0, deadline - time.monotonic())
                try:
                    await asyncio.wait_for(
                        self.lighter_fill_event.wait(),
                        timeout=min(2.0, remaining)
                    )
                    self.waiting_for_lighter_fill = False
                    return
                except asyncio.TimeoutError:
                    if await self._confirm_fill_by_position():
                        return
                    continue
        except Exception as e:
            self.logger.warning(f"检查 Lighter 订单状态失败: {e}")

        self.logger.error("等待 Lighter 订单成交超时（30秒）")
        self.logger.warning("使用降级处理：标记订单已成交以继续交易")
        self.mark_lighter_order_filled()

    async def _confirm_fill_by_position(self) -> bool:
        if not self.lighter_position_fetcher:
            return False
        if self._lighter_position_baseline is None:
            return False
        if self.lighter_order_size is None:
            return False
        try:
            current_pos = await self.lighter_position_fetcher()
        except Exception as e:
            self.logger.warning(f"获取 Lighter 仓位失败: {e}")
            return False
        if current_pos is None:
            return False
        delta = abs(current_pos - self._lighter_position_baseline)
        if delta >= self.lighter_order_size * Decimal("0.5"):
            self.logger.info("检测到 Lighter 仓位变化，认为订单已成交")
            self.mark_lighter_order_filled()
            return True
        return False

    def mark_lighter_order_filled(self):
        self.lighter_order_filled = True
        self.waiting_for_lighter_fill = False
        self.order_execution_complete = True
        self.lighter_fill_event.set()

    def handle_lighter_order_filled(self, order_data: dict):
        try:
            order_data["avg_filled_price"] = (
                Decimal(order_data["filled_quote_amount"]) /
                Decimal(order_data["filled_base_amount"])
            )
            if order_data["is_ask"]:
                order_data["side"] = "SHORT"
                order_type = "OPEN"
            else:
                order_data["side"] = "LONG"
                order_type = "CLOSE"

            client_order_index = order_data["client_order_id"]

            self.logger.info(
                f"[{client_order_index}] [{order_type}] [Lighter] [已成交]: "
                f"{self._format_value(order_data['filled_base_amount'])} @ "
                f"{self._format_value(order_data['avg_filled_price'])}")

            if self.on_order_filled:
                self.on_order_filled(order_data)

            self.mark_lighter_order_filled()

        except Exception as e:
            self.logger.error(f"处理 Lighter 订单结果失败: {e}")
