"""Main arbitrage trading bot for Variational and Lighter exchanges."""
import asyncio
import logging
import os
import signal
import sys
import time
import requests
import traceback
from decimal import Decimal

from variational import Client as VariationalClient, MAINNET
from variational.models import InstrumentType, TradeSide
from lighter.signer_client import SignerClient

from .data_logger import DataLogger
from .order_book_manager import OrderBookManager
from .websocket_manager import WebSocketManagerWrapper
from .order_manager import OrderManager
from .position_tracker import PositionTracker


class VariationalArb:
    """Arbitrage trading bot: taker orders on Variational and Lighter."""

    def __init__(self, ticker: str, order_quantity: Decimal,
                 fill_timeout: int = 5, max_position: Decimal = Decimal('0'),
                 long_ex_threshold: Decimal = Decimal('10'),
                 short_ex_threshold: Decimal = Decimal('10')):
        self.ticker = ticker
        self.order_quantity = order_quantity
        self.fill_timeout = fill_timeout
        self.max_position = max_position
        self.stop_flag = False
        self._cleanup_done = False

        self.long_ex_threshold = long_ex_threshold
        self.short_ex_threshold = short_ex_threshold

        self._setup_logger()

        self.data_logger = DataLogger(exchange="variational", ticker=ticker, logger=self.logger)
        self.order_book_manager = OrderBookManager(self.logger)
        self.ws_manager = WebSocketManagerWrapper(self.order_book_manager, self.logger)
        self.order_manager = OrderManager(self.order_book_manager, self.logger)

        self.variational_client = None
        self.lighter_client = None

        self.variational_base_url = os.getenv("VARIATIONAL_BASE_URL", MAINNET)
        self.variational_api_key = os.getenv("VARIATIONAL_API_KEY")
        self.variational_api_secret = os.getenv("VARIATIONAL_API_SECRET")
        self.variational_settlement_asset = os.getenv("VARIATIONAL_SETTLEMENT_ASSET", "USDC")
        self.variational_rfq_expires_seconds = int(
            os.getenv("VARIATIONAL_RFQ_EXPIRES_SECONDS", "5")
        )

        self.variational_instrument = None
        self.variational_target_companies = []

        self.lighter_base_url = "https://mainnet.zklighter.elliot.ai"
        self.account_index = int(os.getenv('LIGHTER_ACCOUNT_INDEX'))
        self.api_key_index = int(os.getenv('LIGHTER_API_KEY_INDEX'))

        self.lighter_market_index = None
        self.base_amount_multiplier = None
        self.price_multiplier = None
        self.tick_size = None

        self.position_tracker = None

        self._setup_callbacks()

    def _setup_logger(self):
        os.makedirs("logs", exist_ok=True)
        self.log_filename = f"logs/variational_{self.ticker}_log.txt"

        self.logger = logging.getLogger(f"arbitrage_bot_{self.ticker}")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()

        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('requests').setLevel(logging.WARNING)
        logging.getLogger('websockets').setLevel(logging.WARNING)

        file_handler = logging.FileHandler(self.log_filename)
        file_handler.setLevel(logging.INFO)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)

        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')

        file_handler.setFormatter(file_formatter)
        console_handler.setFormatter(console_formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        self.logger.propagate = False

    def _setup_callbacks(self):
        self.ws_manager.set_callbacks(
            on_lighter_order_filled=self._handle_lighter_order_filled
        )
        self.order_manager.set_callbacks(
            on_order_filled=self._handle_lighter_order_filled
        )

    def _handle_lighter_order_filled(self, order_data: dict):
        try:
            order_data["avg_filled_price"] = (
                Decimal(order_data["filled_quote_amount"]) /
                Decimal(order_data["filled_base_amount"])
            )
            if order_data["is_ask"]:
                order_data["side"] = "SHORT"
                order_type = "OPEN"
                if self.position_tracker:
                    self.position_tracker.update_lighter_position(
                        -Decimal(order_data["filled_base_amount"]))
            else:
                order_data["side"] = "LONG"
                order_type = "CLOSE"
                if self.position_tracker:
                    self.position_tracker.update_lighter_position(
                        Decimal(order_data["filled_base_amount"]))

            client_order_index = order_data["client_order_id"]
            self.logger.info(
                f"[{client_order_index}] [{order_type}] [Lighter] [FILLED]: "
                f"{order_data['filled_base_amount']} @ {order_data['avg_filled_price']}")

            self.data_logger.log_trade_to_csv(
                exchange='lighter',
                side=order_data['side'],
                price=str(order_data['avg_filled_price']),
                quantity=str(order_data['filled_base_amount'])
            )

            self.order_manager.lighter_order_filled = True
            self.order_manager.order_execution_complete = True

        except Exception as e:
            self.logger.error(f"Error handling Lighter order result: {e}")

    def shutdown(self, signum=None, frame=None):
        if self.stop_flag:
            return

        self.stop_flag = True

        if signum is not None:
            self.logger.info("\nStopping...")
        else:
            self.logger.info("Stopping...")

        try:
            if self.ws_manager:
                self.ws_manager.shutdown()
        except Exception as e:
            self.logger.error(f"Error shutting down WebSocket manager: {e}")

        try:
            if self.data_logger:
                self.data_logger.close()
        except Exception as e:
            self.logger.error(f"Error closing data logger: {e}")

        for handler in self.logger.handlers[:]:
            try:
                handler.close()
                self.logger.removeHandler(handler)
            except Exception:
                pass

    async def _async_cleanup(self):
        if self._cleanup_done:
            return

        self._cleanup_done = True

        try:
            if self.variational_client and getattr(self.variational_client, "sesh", None):
                self.variational_client.sesh.close()
                self.logger.info("Variational client closed")
        except Exception as e:
            self.logger.error(f"Error closing Variational client: {e}")

    def setup_signal_handlers(self):
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def initialize_lighter_client(self):
        if self.lighter_client is None:
            api_key_private_key = os.getenv('API_KEY_PRIVATE_KEY')
            if not api_key_private_key:
                raise Exception("API_KEY_PRIVATE_KEY environment variable not set")

            self.lighter_client = SignerClient(
                url=self.lighter_base_url,
                private_key=api_key_private_key,
                account_index=self.account_index,
                api_key_index=self.api_key_index,
            )

            err = self.lighter_client.check_client()
            if err is not None:
                raise Exception(f"CheckClient error: {err}")

            self.logger.info("Lighter client initialized successfully")
        return self.lighter_client

    def initialize_variational_client(self):
        if not self.variational_api_key or not self.variational_api_secret:
            raise ValueError(
                "VARIATIONAL_API_KEY and VARIATIONAL_API_SECRET must be set in environment variables")

        request_timeout = os.getenv("VARIATIONAL_REQUEST_TIMEOUT")
        timeout_value = float(request_timeout) if request_timeout else None
        self.variational_client = VariationalClient(
            self.variational_api_key,
            self.variational_api_secret,
            base_url=self.variational_base_url,
            request_timeout=timeout_value,
        )

        self.logger.info("Variational client initialized successfully")
        return self.variational_client

    def _build_variational_instrument(self) -> dict:
        return {
            "instrument_type": InstrumentType.PERPETUAL_FUTURE,
            "underlying": self.ticker,
            "settlement_asset": self.variational_settlement_asset,
            "funding_interval_s": 3600,
            "dex_token_details": None,
        }

    async def _load_variational_target_companies(self) -> list[str]:
        target_companies = []
        env_value = os.getenv("VARIATIONAL_TARGET_COMPANY_IDS", "").strip()
        if env_value:
            target_companies = [item.strip() for item in env_value.split(",") if item.strip()]
            return target_companies

        if not self.variational_client:
            raise Exception("Variational client not initialized")

        def _fetch_companies():
            companies = []
            page = None
            while True:
                resp = self.variational_client.get_companies(page=page)
                companies.extend(resp.result)
                next_page = resp.pagination.next_page
                if not next_page:
                    break
                page = next_page
            return [c["id"] for c in companies if c.get("is_lp")]

        loop = asyncio.get_running_loop()
        target_companies = await loop.run_in_executor(None, _fetch_companies)
        return target_companies

    def get_lighter_market_config(self) -> tuple[int, int, int, Decimal]:
        url = f"{self.lighter_base_url}/api/v1/orderBooks"
        headers = {"accept": "application/json"}

        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        if not response.text.strip():
            raise Exception("Empty response from Lighter API")

        data = response.json()

        if "order_books" not in data:
            raise Exception("Unexpected response format")

        for market in data["order_books"]:
            if market["symbol"] == self.ticker:
                price_multiplier = pow(10, market["supported_price_decimals"])
                return (market["market_id"],
                        pow(10, market["supported_size_decimals"]),
                        price_multiplier,
                        Decimal("1") / (Decimal("10") ** market["supported_price_decimals"]))
        raise Exception(f"Ticker {self.ticker} not found")

    async def trading_loop(self):
        self.logger.info(f"Starting arbitrage bot for {self.ticker}")

        try:
            self.initialize_lighter_client()
            self.initialize_variational_client()

            self.variational_instrument = self._build_variational_instrument()
            self.variational_target_companies = await self._load_variational_target_companies()
            if not self.variational_target_companies:
                self.logger.error("No Variational target companies found")
                return

            (self.lighter_market_index, self.base_amount_multiplier,
             self.price_multiplier, self.tick_size) = self.get_lighter_market_config()

            self.logger.info(
                f"Market config loaded - Lighter: {self.lighter_market_index}, "
                f"Variational target companies: {len(self.variational_target_companies)}")

        except Exception as e:
            self.logger.error(f"Failed to initialize: {e}")
            return

        self.position_tracker = PositionTracker(
            self.ticker,
            self.variational_client,
            self.variational_instrument,
            self.lighter_base_url,
            self.account_index,
            self.logger
        )

        self.order_manager.set_variational_config(
            client=self.variational_client,
            instrument=self.variational_instrument,
            target_companies=self.variational_target_companies,
            rfq_expires_seconds=self.variational_rfq_expires_seconds
        )
        self.order_manager.set_lighter_config(
            self.lighter_client, self.lighter_market_index,
            self.base_amount_multiplier, self.price_multiplier, self.tick_size)

        self.ws_manager.set_lighter_config(
            self.lighter_client, self.lighter_market_index, self.account_index)

        try:
            self.ws_manager.start_lighter_websocket()
            self.logger.info("Lighter WebSocket task started")

            self.logger.info("Waiting for initial Lighter order book data...")
            timeout = 10
            start_time = time.time()
            while (not self.order_book_manager.lighter_order_book_ready and
                   not self.stop_flag):
                if time.time() - start_time > timeout:
                    self.logger.warning(
                        f"Timeout waiting for Lighter order book data after {timeout}s")
                    break
                await asyncio.sleep(0.5)

            if self.order_book_manager.lighter_order_book_ready:
                self.logger.info("Lighter order book data received")
            else:
                self.logger.warning("Lighter order book not ready")

        except Exception as e:
            self.logger.error(f"Failed to setup Lighter websocket: {e}")
            return

        await asyncio.sleep(2)

        self.position_tracker.variational_position = await self.position_tracker.get_variational_position()
        self.position_tracker.lighter_position = await self.position_tracker.get_lighter_position()

        while not self.stop_flag:
            try:
                variational_mid = await self.order_manager.fetch_variational_mid_price()
            except Exception as e:
                self.logger.error(f"Error fetching Variational price: {e}")
                await asyncio.sleep(0.5)
                continue

            lighter_bid, lighter_ask = self.order_book_manager.get_lighter_bbo()

            long_ex = False
            short_ex = False
            if lighter_bid and variational_mid and lighter_bid - variational_mid > self.long_ex_threshold:
                long_ex = True
            elif lighter_ask and variational_mid and variational_mid - lighter_ask > self.short_ex_threshold:
                short_ex = True

            self.data_logger.log_bbo_to_csv(
                maker_bid=variational_mid if variational_mid else Decimal('0'),
                maker_ask=variational_mid if variational_mid else Decimal('0'),
                lighter_bid=lighter_bid if lighter_bid else Decimal('0'),
                lighter_ask=lighter_ask if lighter_ask else Decimal('0'),
                long_maker=long_ex,
                short_maker=short_ex,
                long_maker_threshold=self.long_ex_threshold,
                short_maker_threshold=self.short_ex_threshold
            )

            if self.stop_flag:
                break

            if (self.position_tracker.get_current_variational_position() < self.max_position and
                    long_ex):
                await self._execute_long_trade()
            elif (self.position_tracker.get_current_variational_position() > -1 * self.max_position and
                  short_ex):
                await self._execute_short_trade()
            else:
                await asyncio.sleep(0.05)

    async def _execute_long_trade(self):
        if self.stop_flag:
            return

        try:
            self.position_tracker.variational_position = await asyncio.wait_for(
                self.position_tracker.get_variational_position(),
                timeout=5.0
            )
            if self.stop_flag:
                return
            self.position_tracker.lighter_position = await asyncio.wait_for(
                self.position_tracker.get_lighter_position(),
                timeout=5.0
            )
        except asyncio.TimeoutError:
            if self.stop_flag:
                return
            self.logger.warning("Timeout getting positions")
            return
        except Exception as e:
            if self.stop_flag:
                return
            self.logger.error(f"Error getting positions: {e}")
            return

        if abs(self.position_tracker.get_net_position()) > self.order_quantity * 2:
            self.logger.error(
                f"Position diff is too large: {self.position_tracker.get_net_position()}")
            sys.exit(1)

        try:
            quote = await self.order_manager.request_variational_quote(
                side=TradeSide.BUY,
                quantity=self.order_quantity,
            )
            if not quote:
                return
            quote_price, rfq_id, parent_quote_id = quote

            lighter_bid, _ = self.order_book_manager.get_lighter_bbo()
            if not lighter_bid or lighter_bid - quote_price <= self.long_ex_threshold:
                await self.order_manager.cancel_variational_rfq(rfq_id)
                return

            await self.order_manager.accept_variational_quote(
                rfq_id=rfq_id,
                parent_quote_id=parent_quote_id,
                side=TradeSide.BUY,
            )

            self.position_tracker.update_variational_position(self.order_quantity)
            self.data_logger.log_trade_to_csv(
                exchange='variational',
                side='LONG',
                price=str(quote_price),
                quantity=str(self.order_quantity)
            )

            await self.order_manager.place_lighter_market_order(
                'sell',
                self.order_quantity,
                quote_price,
                self.stop_flag
            )

        except Exception as e:
            if self.stop_flag:
                return
            self.logger.error(f"Error executing long trade: {e}")
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
            sys.exit(1)

    async def _execute_short_trade(self):
        if self.stop_flag:
            return

        try:
            self.position_tracker.variational_position = await asyncio.wait_for(
                self.position_tracker.get_variational_position(),
                timeout=5.0
            )
            if self.stop_flag:
                return
            self.position_tracker.lighter_position = await asyncio.wait_for(
                self.position_tracker.get_lighter_position(),
                timeout=5.0
            )
        except asyncio.TimeoutError:
            if self.stop_flag:
                return
            self.logger.warning("Timeout getting positions")
            return
        except Exception as e:
            if self.stop_flag:
                return
            self.logger.error(f"Error getting positions: {e}")
            return

        if abs(self.position_tracker.get_net_position()) > self.order_quantity * 2:
            self.logger.error(
                f"Position diff is too large: {self.position_tracker.get_net_position()}")
            sys.exit(1)

        try:
            quote = await self.order_manager.request_variational_quote(
                side=TradeSide.SELL,
                quantity=self.order_quantity,
            )
            if not quote:
                return
            quote_price, rfq_id, parent_quote_id = quote

            _, lighter_ask = self.order_book_manager.get_lighter_bbo()
            if not lighter_ask or quote_price - lighter_ask <= self.short_ex_threshold:
                await self.order_manager.cancel_variational_rfq(rfq_id)
                return

            await self.order_manager.accept_variational_quote(
                rfq_id=rfq_id,
                parent_quote_id=parent_quote_id,
                side=TradeSide.SELL,
            )

            self.position_tracker.update_variational_position(-self.order_quantity)
            self.data_logger.log_trade_to_csv(
                exchange='variational',
                side='SHORT',
                price=str(quote_price),
                quantity=str(self.order_quantity)
            )

            await self.order_manager.place_lighter_market_order(
                'buy',
                self.order_quantity,
                quote_price,
                self.stop_flag
            )

        except Exception as e:
            if self.stop_flag:
                return
            self.logger.error(f"Error executing short trade: {e}")
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
            sys.exit(1)

    async def run(self):
        self.setup_signal_handlers()

        try:
            await self.trading_loop()
        except KeyboardInterrupt:
            self.logger.info("\nReceived interrupt signal...")
        except asyncio.CancelledError:
            self.logger.info("\nTask cancelled...")
        finally:
            self.logger.info("Cleaning up...")
            self.shutdown()
            try:
                await asyncio.wait_for(self._async_cleanup(), timeout=5.0)
            except asyncio.TimeoutError:
                self.logger.warning("Cleanup timeout, forcing exit")
            except Exception as e:
                self.logger.error(f"Error during cleanup: {e}")
