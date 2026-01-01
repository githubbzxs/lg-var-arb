"""Order placement and monitoring for Variational and Lighter exchanges."""
import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional, Tuple

from lighter.signer_client import SignerClient
from variational import Client as VariationalClient
from variational.models import TradeSide


class OrderManager:
    """Manages order placement and monitoring for both exchanges."""

    def __init__(self, order_book_manager, logger: logging.Logger):
        self.order_book_manager = order_book_manager
        self.logger = logger

        self.variational_client: Optional[VariationalClient] = None
        self.variational_instrument: Optional[dict] = None
        self.variational_target_companies: list[str] = []
        self.variational_rfq_expires_seconds: int = 5

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

    def set_variational_config(self, client: VariationalClient, instrument: dict,
                               target_companies: list[str], rfq_expires_seconds: int = 5):
        self.variational_client = client
        self.variational_instrument = instrument
        self.variational_target_companies = target_companies
        self.variational_rfq_expires_seconds = rfq_expires_seconds

    def set_lighter_config(self, client: SignerClient, market_index: int,
                           base_amount_multiplier: int, price_multiplier: int, tick_size: Decimal):
        self.lighter_client = client
        self.lighter_market_index = market_index
        self.base_amount_multiplier = base_amount_multiplier
        self.price_multiplier = price_multiplier
        self.tick_size = tick_size

    def set_callbacks(self, on_order_filled: callable = None):
        self.on_order_filled = on_order_filled

    async def _run_variational(self, func, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: func(*args, **kwargs))

    def _rfq_expiry(self) -> str:
        expires_at = datetime.now(timezone.utc) + timedelta(
            seconds=self.variational_rfq_expires_seconds)
        return expires_at.isoformat().replace("+00:00", "Z")

    async def fetch_variational_mid_price(self) -> Decimal:
        if not self.variational_client or not self.variational_instrument:
            raise Exception("Variational client not initialized")

        resp = await self._run_variational(
            self.variational_client.price_instrument,
            self.variational_instrument,
        )
        return Decimal(resp.result["price"])

    async def request_variational_quote(self, side: TradeSide, quantity: Decimal
                                        ) -> Optional[Tuple[Decimal, str, str]]:
        if not self.variational_client or not self.variational_instrument:
            raise Exception("Variational client not initialized")
        if not self.variational_target_companies:
            raise Exception("Variational target companies not configured")

        structure = {
            "legs": [{
                "side": side,
                "ratio": 1,
                "instrument": self.variational_instrument,
            }]
        }

        resp = await self._run_variational(
            self.variational_client.create_rfq,
            structure=structure,
            qty=str(quantity),
            expires_at=self._rfq_expiry(),
            target_companies=self.variational_target_companies,
        )
        rfq_id = resp.result.get("rfq_id")
        if not rfq_id:
            self.logger.error("Variational RFQ creation failed: missing rfq_id")
            return None

        quotes = await self._wait_for_rfq_quotes(rfq_id, side)
        if not quotes:
            return None

        quote_price, parent_quote_id = quotes
        return quote_price, rfq_id, parent_quote_id

    async def _wait_for_rfq_quotes(self, rfq_id: str, side: TradeSide
                                   ) -> Optional[Tuple[Decimal, str]]:
        deadline = time.time() + self.variational_rfq_expires_seconds
        while time.time() < deadline:
            resp = await self._run_variational(
                self.variational_client.get_rfqs_sent,
                id=rfq_id,
                price=True,
            )
            if resp.result:
                rfq = resp.result[0]
                quotes = rfq.get("asks" if side == TradeSide.BUY else "bids", [])
                if quotes:
                    if side == TradeSide.BUY:
                        best = min(quotes, key=lambda q: Decimal(q["quote_price"]))
                    else:
                        best = max(quotes, key=lambda q: Decimal(q["quote_price"]))
                    return Decimal(best["quote_price"]), best["parent_quote_id"]
            await asyncio.sleep(0.2)
        return None

    async def accept_variational_quote(self, rfq_id: str, parent_quote_id: str, side: TradeSide):
        if not self.variational_client:
            raise Exception("Variational client not initialized")

        await self._run_variational(
            self.variational_client.accept_quote,
            rfq_id=rfq_id,
            parent_quote_id=parent_quote_id,
            side=side,
        )

    async def cancel_variational_rfq(self, rfq_id: str):
        if not self.variational_client:
            return
        await self._run_variational(self.variational_client.cancel_rfq, rfq_id)

    async def place_lighter_market_order(self, lighter_side: str, quantity: Decimal,
                                         price: Decimal, stop_flag) -> Optional[str]:
        if not self.lighter_client:
            raise Exception("Lighter client not initialized")

        best_bid, best_ask = self.order_book_manager.get_lighter_best_levels()
        if not best_bid or not best_ask:
            raise Exception("Lighter order book not ready")

        if lighter_side.lower() == 'buy':
            order_type = "CLOSE"
            is_ask = False
            price = best_ask[0] * Decimal('1.002')
        else:
            order_type = "OPEN"
            is_ask = True
            price = best_bid[0] * Decimal('0.998')

        self.lighter_order_filled = False
        self.lighter_order_price = price
        self.lighter_order_side = lighter_side
        self.lighter_order_size = quantity

        try:
            client_order_index = int(time.time() * 1000)
            tx_info, error = self.lighter_client.sign_create_order(
                market_index=self.lighter_market_index,
                client_order_index=client_order_index,
                base_amount=int(quantity * self.base_amount_multiplier),
                price=int(price * self.price_multiplier),
                is_ask=is_ask,
                order_type=self.lighter_client.ORDER_TYPE_LIMIT,
                time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                reduce_only=False,
                trigger_price=0,
            )
            if error is not None:
                raise Exception(f"Sign error: {error}")

            tx_hash = await self.lighter_client.send_tx(
                tx_type=self.lighter_client.TX_TYPE_CREATE_ORDER,
                tx_info=tx_info
            )

            self.logger.info(f"[{client_order_index}] [{order_type}] [Lighter] [OPEN]: {quantity}")

            await self.monitor_lighter_order(client_order_index, stop_flag)

            return tx_hash
        except Exception as e:
            self.logger.error(f"Error placing Lighter order: {e}")
            return None

    async def monitor_lighter_order(self, client_order_index: int, stop_flag):
        start_time = time.time()
        while not self.lighter_order_filled and not stop_flag:
            if time.time() - start_time > 30:
                self.logger.error(
                    f"Timeout waiting for Lighter order fill after {time.time() - start_time:.1f}s")
                self.logger.warning("Using fallback - marking order as filled to continue trading")
                self.lighter_order_filled = True
                self.waiting_for_lighter_fill = False
                self.order_execution_complete = True
                break

            await asyncio.sleep(0.1)

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
                f"[{client_order_index}] [{order_type}] [Lighter] [FILLED]: "
                f"{order_data['filled_base_amount']} @ {order_data['avg_filled_price']}")

            if self.on_order_filled:
                self.on_order_filled(order_data)

            self.lighter_order_filled = True
            self.order_execution_complete = True

        except Exception as e:
            self.logger.error(f"Error handling Lighter order result: {e}")
