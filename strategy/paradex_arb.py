"""Main arbitrage trading bot for Paradex and Lighter exchanges."""
import asyncio
import logging
import math
import os
import signal
import sys
import time
import traceback
from collections import deque
from decimal import Decimal
from typing import Optional

import requests
from lighter.signer_client import SignerClient
from paradex_py import Paradex, ParadexSubkey

try:
    from paradex_py import environment as paradex_env
except Exception:
    paradex_env = None


def _get_paradex_env(name: str):
    if paradex_env is None:
        raise ImportError("paradex_py.environment 不可用")

    env_name = (name or "").lower()
    if hasattr(paradex_env, "Environment"):
        env_cls = paradex_env.Environment
        if env_name in ("testnet", "test") and hasattr(env_cls, "TESTNET"):
            return env_cls.TESTNET
        for attr in ("PROD", "PRODUCTION", "MAINNET"):
            if hasattr(env_cls, attr):
                return getattr(env_cls, attr)

    if env_name in ("testnet", "test") and hasattr(paradex_env, "TESTNET"):
        return paradex_env.TESTNET
    for attr in ("PROD", "PRODUCTION", "MAINNET"):
        if hasattr(paradex_env, attr):
            return getattr(paradex_env, attr)

    raise ValueError(f"不支持的 Paradex 环境: {name}")


class RollingStats:
    """Track rolling mean/std for spread values."""

    def __init__(self, window: int):
        self.window = max(1, window)
        self.values = deque()
        self.sum = 0.0
        self.sum_sq = 0.0

    def add(self, value: Decimal):
        v = float(value)
        self.values.append(v)
        self.sum += v
        self.sum_sq += v * v
        if len(self.values) > self.window:
            old = self.values.popleft()
            self.sum -= old
            self.sum_sq -= old * old

    def mean(self):
        if not self.values:
            return None
        return self.sum / len(self.values)

    def std(self):
        if len(self.values) < 2:
            return None
        mean = self.mean()
        variance = max(self.sum_sq / len(self.values) - mean * mean, 0.0)
        return math.sqrt(variance)

    def count(self):
        return len(self.values)

from .data_logger import DataLogger
from .order_book_manager import OrderBookManager
from .websocket_manager import WebSocketManagerWrapper
from .order_manager import OrderManager
from .position_tracker import PositionTracker
from .pnl_tracker import PnLTracker
from .telegram_notifier import TelegramNotifier


class ParadexArb:
    """Arbitrage trading bot: taker orders on Paradex and Lighter."""

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
        self.spread_window = int(os.getenv("SPREAD_WINDOW", "120"))
        self.spread_min_samples = int(os.getenv("SPREAD_MIN_SAMPLES", "20"))
        self.spread_min_samples = min(self.spread_min_samples, self.spread_window)
        self.spread_entry_std = float(os.getenv("SPREAD_ENTRY_STD", "1.5"))
        self.spread_exit_std = float(os.getenv("SPREAD_EXIT_STD", "0.5"))
        self.spread_tier_step_std = float(os.getenv("SPREAD_TIER_STEP_STD", "0.5"))
        self.spread_max_tiers = int(os.getenv("SPREAD_MAX_TIERS", "3"))
        self.min_spread_std = float(os.getenv("SPREAD_MIN_STD", "0.1"))
        self.min_tier_step = float(os.getenv("SPREAD_MIN_TIER_STEP", "0.5"))
        self.log_decimals = int(os.getenv("LOG_DECIMALS", "4"))
        self.log_decimals = max(0, min(self.log_decimals, 8))
        self.trading_enabled = os.getenv("TRADING_ENABLED", "1").lower() in (
            "1",
            "true",
            "yes",
            "y",
            "on",
        )
        self.paradex_mid_refresh_interval = float(os.getenv("PARADEX_BBO_MIN_INTERVAL", "0.2"))
        self.position_refresh_interval = float(os.getenv("POSITION_REFRESH_INTERVAL", "1.5"))
        self.position_min_request_interval = float(os.getenv("POSITION_MIN_REQUEST_INTERVAL", "0.5"))
        self.lighter_book_stale_after = float(os.getenv("LIGHTER_BOOK_STALE_AFTER", "3.0"))
        self.lighter_rest_check_interval = float(os.getenv("LIGHTER_REST_BBO_CHECK_INTERVAL", "5.0"))
        self.lighter_rest_timeout = float(os.getenv("LIGHTER_REST_TIMEOUT", "5.0"))
        self.loop_sleep = float(os.getenv("TRADING_LOOP_SLEEP", "0.02"))
        self.lighter_bbo_tolerance = Decimal(os.getenv("LIGHTER_BBO_TOLERANCE", "0"))
        imbalance_multiplier = Decimal(os.getenv("POSITION_IMBALANCE_MULTIPLIER", "2"))
        self.position_imbalance_limit = self.order_quantity * imbalance_multiplier
        self.long_spread_stats = RollingStats(self.spread_window)
        self.short_spread_stats = RollingStats(self.spread_window)
        self._last_paradex_mid_ts = 0.0
        self._paradex_mid_cache = None
        self._last_rest_check_ts = 0.0
        self._last_stale_log_ts = 0.0
        self._last_consistency_log_ts = 0.0
        self._last_imbalance_log_ts = 0.0
        self._last_status_log_ts = 0.0
        self._last_no_signal_log_ts = 0.0
        self._last_max_position_log_ts = 0.0
        self.status_log_interval = 10.0
        self.no_signal_log_interval = 0.0
        self._max_position_warned = False
        self._last_paradex_mid = None
        self._last_lighter_bid = None
        self._last_lighter_ask = None
        self._last_lighter_mid = None
        self._last_long_spread = None
        self._last_short_spread = None
        self._last_long_entry = None
        self._last_short_entry = None
        self._last_long_exit = None
        self._last_short_exit = None
        self.paradex_pnl = PnLTracker()
        self.lighter_pnl = PnLTracker()
        self._pending_trade = None

        self._setup_logger()

        self.telegram_notifier = TelegramNotifier(self.logger)
        self.telegram_status_interval = float(os.getenv("TELEGRAM_STATUS_INTERVAL", "0"))
        self.telegram_poll_interval = float(os.getenv("TELEGRAM_POLL_INTERVAL", "2"))
        self._last_telegram_status_ts = 0.0
        self._last_telegram_poll_ts = 0.0

        self.data_logger = DataLogger(exchange="paradex", ticker=ticker, logger=self.logger)
        self.order_book_manager = OrderBookManager(self.logger)
        self.ws_manager = WebSocketManagerWrapper(self.order_book_manager, self.logger)
        self.order_manager = OrderManager(self.order_book_manager, self.logger)

        self.paradex_client = None
        self.lighter_client = None

        self.paradex_env_name = os.getenv("PARADEX_ENV", "prod")
        self.paradex_market = os.getenv("PARADEX_MARKET") or f"{self.ticker}-USD-PERP"
        self.paradex_l1_address = os.getenv("PARADEX_L1_ADDRESS")
        self.paradex_l1_private_key = os.getenv("PARADEX_L1_PRIVATE_KEY")
        self.paradex_l2_private_key = os.getenv("PARADEX_L2_PRIVATE_KEY")
        self.paradex_l2_address = os.getenv("PARADEX_L2_ADDRESS")

        self.lighter_base_url = "https://mainnet.zklighter.elliot.ai"
        self.account_index = int(os.getenv('LIGHTER_ACCOUNT_INDEX'))
        self.api_key_index = int(os.getenv('LIGHTER_API_KEY_INDEX'))

        self.lighter_market_index = None
        self.base_amount_multiplier = None
        self.price_multiplier = None
        self.tick_size = None

        self.position_tracker = None
        self.latest_paradex_mid = None

        self._setup_callbacks()

    def _setup_logger(self):
        os.makedirs("logs", exist_ok=True)
        self.log_filename = f"logs/paradex_{self.ticker}_log.txt"

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

    async def _run_blocking(self, func, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: func(*args, **kwargs))

    async def _fetch_lighter_position_for_fill(self):
        try:
            await self.position_tracker.refresh_positions(force=True)
        except Exception as e:
            self.logger.warning(f"获取仓位失败: {e}")
            return None
        return self.position_tracker.get_current_lighter_position()

    def _update_spread_stats(self, long_spread: Optional[Decimal], short_spread: Optional[Decimal]):
        if long_spread is not None:
            self.long_spread_stats.add(long_spread)
        if short_spread is not None:
            self.short_spread_stats.add(short_spread)

    def _calc_arb_position(self, paradex_pos: Decimal, lighter_pos: Decimal) -> Decimal:
        if paradex_pos == 0 or lighter_pos == 0:
            return paradex_pos + lighter_pos
        if paradex_pos > 0 and lighter_pos < 0:
            return min(abs(paradex_pos), abs(lighter_pos))
        if paradex_pos < 0 and lighter_pos > 0:
            return -min(abs(paradex_pos), abs(lighter_pos))
        return paradex_pos + lighter_pos

    def _format_optional(self, value, decimals: Optional[int] = None) -> str:
        if value is None:
            return "无"
        if isinstance(value, bool):
            return str(value)
        if isinstance(value, int):
            return str(value)
        if decimals is None:
            decimals = self.log_decimals
        try:
            return f"{float(value):.{decimals}f}"
        except (TypeError, ValueError):
            return str(value)

    def _format_trade_action(self, action: Optional[str]) -> str:
        if not action:
            return "交易"
        mapping = {
            "OPEN_LONG": "开多",
            "OPEN_SHORT": "开空",
            "CLOSE_LONG": "平多",
            "CLOSE_SHORT": "平空",
        }
        return mapping.get(action, action)

    def _get_decimal_value(self, data: dict, *keys: str) -> Optional[Decimal]:
        for key in keys:
            if key not in data:
                continue
            value = data.get(key)
            if value is None or value == "":
                continue
            try:
                return Decimal(str(value))
            except Exception:
                continue
        return None

    def _get_bool_value(self, data: dict, *keys: str) -> Optional[bool]:
        for key in keys:
            if key not in data:
                continue
            value = data.get(key)
            if isinstance(value, bool):
                return value
            if isinstance(value, (int, float)):
                return value != 0
            if isinstance(value, str):
                normalized = value.strip().lower()
                if normalized in ("true", "1", "yes", "y", "on"):
                    return True
                if normalized in ("false", "0", "no", "n", "off"):
                    return False
        return None

    def _log_status(self, now: float) -> None:
        if self.status_log_interval <= 0:
            return
        if now - self._last_status_log_ts < self.status_log_interval:
            return
        self._last_status_log_ts = now

        if self.position_tracker:
            paradex_pos = self.position_tracker.get_current_paradex_position()
            lighter_pos = self.position_tracker.get_current_lighter_position()
        else:
            paradex_pos = Decimal("0")
            lighter_pos = Decimal("0")
        net_pos = paradex_pos + lighter_pos

        last_update_age = None
        if self.order_book_manager.lighter_last_update_ts is not None:
            last_update_age = now - self.order_book_manager.lighter_last_update_ts

        self.logger.info(
            "运行状态: "
            f"总仓位={self._format_optional(net_pos)} "
            f"Paradex仓位={self._format_optional(paradex_pos)} "
            f"Lighter仓位={self._format_optional(lighter_pos)} "
            f"最大仓位={self._format_optional(self.max_position)} "
            f"交易开关={'开' if self.trading_enabled else '停'} "
            f"等待成交={self.order_manager.waiting_for_lighter_fill} "
            f"行情准备好={self.order_book_manager.lighter_order_book_ready} "
            f"行情过期={self.order_book_manager.is_lighter_order_book_stale(self.lighter_book_stale_after)} "
            f"数据缺口={self.order_book_manager.lighter_order_book_sequence_gap} "
            f"距上次更新(秒)={self._format_optional(last_update_age)} "
            f"Paradex中间价={self._format_optional(self._last_paradex_mid)} "
            f"Lighter买价={self._format_optional(self._last_lighter_bid)} "
            f"Lighter卖价={self._format_optional(self._last_lighter_ask)} "
            f"做多差价={self._format_optional(self._last_long_spread)} "
            f"做多触发线={self._format_optional(self._last_long_entry)} "
            f"做多退出线={self._format_optional(self._last_long_exit)} "
            f"做空差价={self._format_optional(self._last_short_spread)} "
            f"做空触发线={self._format_optional(self._last_short_entry)} "
            f"做空退出线={self._format_optional(self._last_short_exit)} "
            f"样本数={self.long_spread_stats.count()}/{self.short_spread_stats.count()}"
        )

    def _build_status_message(self) -> str:
        if self.position_tracker:
            paradex_pos = self.position_tracker.get_current_paradex_position()
            lighter_pos = self.position_tracker.get_current_lighter_position()
        else:
            paradex_pos = Decimal("0")
            lighter_pos = Decimal("0")
        net_pos = paradex_pos + lighter_pos
        arb_pos = self._calc_arb_position(paradex_pos, lighter_pos)

        paradex_mid = self.latest_paradex_mid or self._last_paradex_mid
        lighter_mid = self._last_lighter_mid

        paradex_unreal = self.paradex_pnl.unrealized(paradex_mid)
        lighter_unreal = self.lighter_pnl.unrealized(lighter_mid)
        paradex_total = self.paradex_pnl.realized + paradex_unreal
        lighter_total = self.lighter_pnl.realized + lighter_unreal
        total_pnl = paradex_total + lighter_total

        return (
            f"{self.ticker} 状态\n"
            f"交易开关={'开' if self.trading_enabled else '停'}\n"
            f"净仓={self._format_optional(net_pos)} 对冲仓={self._format_optional(arb_pos)}\n"
            f"Paradex 仓位={self._format_optional(paradex_pos)} "
            f"均价={self._format_optional(self.paradex_pnl.avg_price)} "
            f"已实现={self._format_optional(self.paradex_pnl.realized)} "
            f"未实现={self._format_optional(paradex_unreal)}\n"
            f"Lighter 仓位={self._format_optional(lighter_pos)} "
            f"均价={self._format_optional(self.lighter_pnl.avg_price)} "
            f"已实现={self._format_optional(self.lighter_pnl.realized)} "
            f"未实现={self._format_optional(lighter_unreal)}\n"
            f"总 PnL={self._format_optional(total_pnl)}\n"
            f"中间价 Paradex={self._format_optional(paradex_mid)} "
            f"Lighter={self._format_optional(lighter_mid)}"
        )

    def _queue_telegram_message(self, message: str) -> None:
        if not self.telegram_notifier or not self.telegram_notifier.enabled:
            return
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.telegram_notifier.send(message))
        except RuntimeError:
            self.telegram_notifier.send_sync(message)

    async def _maybe_handle_telegram(self, now: float) -> None:
        if not self.telegram_notifier or not self.telegram_notifier.enabled:
            return
        try:
            if (
                self.telegram_poll_interval > 0
                and now - self._last_telegram_poll_ts >= self.telegram_poll_interval
            ):
                self._last_telegram_poll_ts = now
                commands = await self.telegram_notifier.fetch_commands()
                for cmd in commands:
                    command = cmd.strip().lower()
                    if command in ("/start", "start"):
                        if not self.trading_enabled:
                            self.trading_enabled = True
                            self.logger.info("Telegram: 交易已开启")
                            await self.telegram_notifier.send("交易已开启")
                        else:
                            await self.telegram_notifier.send("交易已处于开启状态")
                    elif command in ("/stop", "stop"):
                        if self.trading_enabled:
                            self.trading_enabled = False
                            self.logger.info("Telegram: 交易已停止")
                            await self.telegram_notifier.send("交易已停止（仅允许平仓）")
                        else:
                            await self.telegram_notifier.send("交易已处于停止状态")
                    elif command in ("/status", "status", "/pnl", "pnl"):
                        await self.telegram_notifier.send(self._build_status_message())

            if (
                self.telegram_status_interval > 0
                and now - self._last_telegram_status_ts >= self.telegram_status_interval
            ):
                self._last_telegram_status_ts = now
                await self.telegram_notifier.send(self._build_status_message())
        except Exception as exc:
            self.logger.warning(f"Telegram handler error: {exc}")

    def _cap_trade_qty(
        self,
        trade_qty: Decimal,
        delta: Decimal,
        paradex_pos: Decimal,
        lighter_pos: Decimal,
    ) -> Decimal:
        if trade_qty <= 0:
            return Decimal("0")
        if self.max_position <= 0:
            return Decimal("0")
        if delta > 0:
            max_qty_paradex = self.max_position - paradex_pos
            max_qty_lighter = self.max_position + lighter_pos
        else:
            max_qty_paradex = self.max_position + paradex_pos
            max_qty_lighter = self.max_position - lighter_pos
        max_allowed = min(max_qty_paradex, max_qty_lighter)
        if max_allowed <= 0:
            return Decimal("0")
        return min(trade_qty, max_allowed)

    def _dynamic_threshold(self, stats: RollingStats, fallback: Decimal,
                           std_multiplier: float, floor: Decimal):
        if stats.count() < self.spread_min_samples:
            return fallback, None
        mean = stats.mean()
        std = stats.std() or 0.0
        std = max(std, self.min_spread_std)
        threshold = Decimal(str(mean + std_multiplier * std))
        if threshold < floor:
            threshold = floor
        return threshold, std

    def _compute_target_size(self, spread: Decimal, entry_threshold: Decimal,
                             entry_std: Optional[float]) -> Decimal:
        if spread <= entry_threshold:
            return Decimal("0")
        tiers = 1
        if entry_std:
            tier_step = max(entry_std * self.spread_tier_step_std, self.min_tier_step)
            tiers = 1 + int(float(spread - entry_threshold) / tier_step)
            tiers = min(tiers, self.spread_max_tiers)
        size = self.order_quantity * Decimal(tiers)
        if self.max_position > 0:
            size = min(size, self.max_position)
        return size

    def _determine_target_position(self, current_net: Decimal, long_spread: Decimal,
                                   short_spread: Decimal, long_entry: Decimal, long_exit: Decimal,
                                   short_entry: Decimal, short_exit: Decimal,
                                   long_std: Optional[float], short_std: Optional[float]) -> Decimal:
        target = current_net
        long_excess = long_spread - long_entry
        short_excess = short_spread - short_entry

        if long_excess > 0 and long_excess >= short_excess:
            target = self._compute_target_size(long_spread, long_entry, long_std)
        elif short_excess > 0 and short_excess > long_excess:
            target = -self._compute_target_size(short_spread, short_entry, short_std)
        else:
            if current_net > 0 and long_spread < long_exit:
                target = Decimal("0")
            elif current_net < 0 and short_spread < short_exit:
                target = Decimal("0")

        if self.max_position > 0:
            target = max(min(target, self.max_position), -self.max_position)
        return target

    async def _fetch_paradex_mid_cached(self):
        now = time.monotonic()
        if (self._paradex_mid_cache is not None and
                (now - self._last_paradex_mid_ts) < self.paradex_mid_refresh_interval):
            return self._paradex_mid_cache

        paradex_mid = await self.order_manager.fetch_paradex_mid_price()
        self._paradex_mid_cache = paradex_mid
        self._last_paradex_mid_ts = time.monotonic()
        return paradex_mid

    def _extract_bbo_from_market(self, market: dict):
        if not isinstance(market, dict):
            return None, None

        bid = None
        ask = None
        for key in ("best_bid", "bestBid", "bid", "bid_price", "bidPrice"):
            if market.get(key) is not None:
                try:
                    bid = Decimal(str(market.get(key)))
                    break
                except Exception:
                    bid = None

        for key in ("best_ask", "bestAsk", "ask", "ask_price", "askPrice"):
            if market.get(key) is not None:
                try:
                    ask = Decimal(str(market.get(key)))
                    break
                except Exception:
                    ask = None

        if bid is None or ask is None:
            order_book = market.get("order_book") or market.get("orderBook") or market
            bids = order_book.get("bids") if isinstance(order_book, dict) else None
            asks = order_book.get("asks") if isinstance(order_book, dict) else None
            if bid is None and bids:
                try:
                    if isinstance(bids[0], dict):
                        bid = Decimal(str(bids[0].get("price") or bids[0].get("px")))
                    else:
                        bid = Decimal(str(bids[0][0]))
                except Exception:
                    bid = None
            if ask is None and asks:
                try:
                    if isinstance(asks[0], dict):
                        ask = Decimal(str(asks[0].get("price") or asks[0].get("px")))
                    else:
                        ask = Decimal(str(asks[0][0]))
                except Exception:
                    ask = None

        return bid, ask

    async def _fetch_lighter_bbo_rest(self):
        url = f"{self.lighter_base_url}/api/v1/orderBooks"
        headers = {"accept": "application/json"}
        response = await self._run_blocking(
            requests.get,
            url,
            headers=headers,
            timeout=self.lighter_rest_timeout,
        )
        response.raise_for_status()
        data = response.json()
        order_books = data.get("order_books") or data.get("orderBooks") or data.get("results") or []
        for market in order_books:
            symbol = market.get("symbol") if isinstance(market, dict) else None
            market_id = market.get("market_id") if isinstance(market, dict) else None
            if symbol == self.ticker or market_id == self.lighter_market_index:
                return self._extract_bbo_from_market(market)
        return None, None

    async def _check_lighter_rest_consistency(self, ws_bid: Optional[Decimal], ws_ask: Optional[Decimal]):
        if (not ws_bid or not ws_ask or
                (time.monotonic() - self._last_rest_check_ts) < self.lighter_rest_check_interval):
            return

        self._last_rest_check_ts = time.monotonic()
        try:
            rest_bid, rest_ask = await self._fetch_lighter_bbo_rest()
        except Exception as e:
            self.logger.warning(f"Lighter REST BBO 校验失败: {e}")
            return

        if not rest_bid or not rest_ask:
            return

        if rest_bid <= 0 or rest_ask <= 0 or rest_bid >= rest_ask:
            self.logger.warning("Lighter REST BBO 无效，跳过一致性校验")
            return

        tolerance = self.lighter_bbo_tolerance
        if tolerance <= 0 and self.tick_size:
            tolerance = self.tick_size * Decimal("2")

        bid_diff = abs(ws_bid - rest_bid)
        ask_diff = abs(ws_ask - rest_ask)
        if bid_diff > tolerance or ask_diff > tolerance:
            now = time.monotonic()
            if now - self._last_consistency_log_ts > 5:
                self._last_consistency_log_ts = now
                self.logger.warning(
                    "Lighter BBO 不一致（ws 与 rest）"
                    f"bid {ws_bid}/{rest_bid} ask {ws_ask}/{rest_ask}，正在重同步")
            self.ws_manager.request_resync()

    def _handle_lighter_order_filled(self, order_data: dict):
        try:
            is_ask = self._get_bool_value(order_data, "is_ask", "isAsk")
            if is_ask is None:
                side_hint = str(order_data.get("side", "")).lower()
                if side_hint in ("sell", "short", "ask"):
                    is_ask = True
                elif side_hint in ("buy", "long", "bid"):
                    is_ask = False
                else:
                    fallback_side = getattr(self.order_manager, "lighter_order_side", None)
                    if fallback_side:
                        is_ask = str(fallback_side).lower() == "sell"
            if is_ask is None:
                is_ask = False
            order_data["is_ask"] = is_ask

            filled_qty = self._get_decimal_value(
                order_data,
                "filled_base_amount",
                "filledBaseAmount",
                "filled_size",
                "filledSize",
            )
            if filled_qty is None or filled_qty <= 0:
                filled_qty = self._get_decimal_value(
                    order_data,
                    "initial_base_amount",
                    "initialBaseAmount",
                )
            if filled_qty is None or filled_qty <= 0:
                fallback_qty = getattr(self.order_manager, "lighter_order_size", None)
                if fallback_qty is not None:
                    filled_qty = Decimal(str(fallback_qty))
            if filled_qty is None or filled_qty <= 0:
                self.logger.warning("Lighter 成交数量缺失，无法更新 PnL")
                return

            filled_quote = self._get_decimal_value(
                order_data,
                "filled_quote_amount",
                "filledQuoteAmount",
            )
            avg_filled_price = self._get_decimal_value(
                order_data,
                "avg_filled_price",
                "avgFilledPrice",
            )
            order_price = self._get_decimal_value(order_data, "price")
            if avg_filled_price is None or avg_filled_price <= 0:
                if filled_quote is not None and filled_quote > 0:
                    avg_filled_price = filled_quote / filled_qty
                elif order_price is not None and order_price > 0:
                    avg_filled_price = order_price
                else:
                    fallback_price = getattr(self.order_manager, "lighter_order_price", None)
                    if fallback_price is not None:
                        avg_filled_price = Decimal(str(fallback_price))
                    else:
                        avg_filled_price = self._last_lighter_mid or Decimal("0")
            order_data["avg_filled_price"] = avg_filled_price

            if order_data["is_ask"]:
                order_data["side"] = "SHORT"
                order_type = "OPEN"
                if self.position_tracker:
                    self.position_tracker.update_lighter_position(
                        -filled_qty)
            else:
                order_data["side"] = "LONG"
                order_type = "CLOSE"
                if self.position_tracker:
                    self.position_tracker.update_lighter_position(
                        filled_qty)

            filled_price = order_data["avg_filled_price"]
            pnl_delta = -filled_qty if order_data["is_ask"] else filled_qty
            self.lighter_pnl.update(pnl_delta, filled_price)

            client_order_index = order_data["client_order_id"]
            self.logger.info(
                f"[{client_order_index}] [{order_type}] [Lighter] [已成交]: "
                f"{self._format_optional(filled_qty)} @ "
                f"{self._format_optional(filled_price)}")

            self.data_logger.log_trade_to_csv(
                exchange='lighter',
                side=order_data['side'],
                price=str(order_data['avg_filled_price']),
                quantity=str(order_data['filled_base_amount'])
            )

            pending = self._pending_trade
            message = None
            if pending:
                age = time.time() - pending.get("ts", 0)
                if age < 120:
                    action_label = self._format_trade_action(pending.get("action"))
                    message = (
                        f"{self.ticker} {action_label} 已成交\n"
                        f"Paradex {pending.get('paradex_side', '')} "
                        f"{self._format_optional(pending.get('quantity'))} @ "
                        f"{self._format_optional(pending.get('paradex_price'))}\n"
                        f"Lighter {order_data['side']} "
                        f"{self._format_optional(filled_qty)} @ "
                        f"{self._format_optional(filled_price)}\n"
                        f"{self._build_status_message()}"
                    )
                self._pending_trade = None
            if message is None:
                message = (
                    f"{self.ticker} Lighter 成交\n"
                    f"Lighter {order_data['side']} "
                    f"{self._format_optional(filled_qty)} @ "
                    f"{self._format_optional(filled_price)}\n"
                    f"{self._build_status_message()}"
                )
            self._queue_telegram_message(message)

            self.order_manager.mark_lighter_order_filled()

        except Exception as e:
            self.logger.error(f"处理 Lighter 订单结果失败: {e}")
            self._pending_trade = None

    def shutdown(self, signum=None, frame=None):
        if self.stop_flag:
            return

        self.stop_flag = True

        if signum is not None:
            self.logger.info("\n正在停止...")
        else:
            self.logger.info("正在停止...")

        try:
            if self.ws_manager:
                self.ws_manager.shutdown()
        except Exception as e:
            self.logger.error(f"关闭 WebSocket 管理器失败: {e}")

        try:
            if self.data_logger:
                self.data_logger.close()
        except Exception as e:
            self.logger.error(f"关闭数据记录器失败: {e}")

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
            if self.paradex_client and hasattr(self.paradex_client, "close"):
                close_fn = self.paradex_client.close
                if asyncio.iscoroutinefunction(close_fn):
                    await close_fn()
                else:
                    close_fn()
                self.logger.info("Paradex 客户端已关闭")
        except Exception as e:
            self.logger.error(f"关闭 Paradex 客户端失败: {e}")

    def setup_signal_handlers(self):
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def initialize_lighter_client(self):
        if self.lighter_client is None:
            api_key_private_key = os.getenv('API_KEY_PRIVATE_KEY')
            if not api_key_private_key:
                raise Exception("未设置 API_KEY_PRIVATE_KEY 环境变量")

            self.lighter_client = SignerClient(
                url=self.lighter_base_url,
                private_key=api_key_private_key,
                account_index=self.account_index,
                api_key_index=self.api_key_index,
            )

            err = self.lighter_client.check_client()
            if err is not None:
                raise Exception(f"CheckClient 错误: {err}")

            self.logger.info("Lighter 客户端初始化成功")
        return self.lighter_client

    def initialize_paradex_client(self):
        if self.paradex_client is None:
            env = _get_paradex_env(self.paradex_env_name)

            if self.paradex_l2_private_key and self.paradex_l2_address:
                if self.paradex_l1_address and self.paradex_l1_private_key:
                    self.paradex_client = Paradex(
                        env=env,
                        l1_address=self.paradex_l1_address,
                        l1_private_key=self.paradex_l1_private_key,
                        l2_private_key=self.paradex_l2_private_key,
                    )
                else:
                    self.paradex_client = ParadexSubkey(
                        env=env,
                        l2_private_key=self.paradex_l2_private_key,
                        l2_address=self.paradex_l2_address,
                    )
            elif self.paradex_l1_address and self.paradex_l1_private_key:
                self.paradex_client = Paradex(
                    env=env,
                    l1_address=self.paradex_l1_address,
                    l1_private_key=self.paradex_l1_private_key,
                )
            else:
                raise ValueError(
                    "缺少 Paradex 凭证。请设置 PARADEX_L2_PRIVATE_KEY 和 "
                    "PARADEX_L2_ADDRESS，或提供 PARADEX_L1_ADDRESS 和 "
                    "PARADEX_L1_PRIVATE_KEY。"
                )

            self.logger.info("Paradex 客户端初始化成功")
        return self.paradex_client

    def get_lighter_market_config(self) -> tuple[int, int, int, Decimal]:
        url = f"{self.lighter_base_url}/api/v1/orderBooks"
        headers = {"accept": "application/json"}

        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        if not response.text.strip():
            raise Exception("Lighter API 返回空响应")

        data = response.json()

        if "order_books" not in data:
            raise Exception("响应格式异常")

        for market in data["order_books"]:
            if market["symbol"] == self.ticker:
                price_multiplier = pow(10, market["supported_price_decimals"])
                return (market["market_id"],
                        pow(10, market["supported_size_decimals"]),
                        price_multiplier,
                        Decimal("1") / (Decimal("10") ** market["supported_price_decimals"]))
        raise Exception(f"未找到交易对 {self.ticker}")

    async def trading_loop(self):
        self.logger.info(f"启动套利机器人: {self.ticker}")

        try:
            self.initialize_lighter_client()
            self.initialize_paradex_client()

            (self.lighter_market_index, self.base_amount_multiplier,
             self.price_multiplier, self.tick_size) = self.get_lighter_market_config()

            self.logger.info(
                f"市场配置已加载 - Lighter: {self.lighter_market_index}, "
                f"Paradex 市场: {self.paradex_market}")
            if self.lighter_bbo_tolerance <= 0 and self.tick_size:
                self.lighter_bbo_tolerance = self.tick_size * Decimal("2")

        except Exception as e:
            self.logger.error(f"初始化失败: {e}")
            return

        self.position_tracker = PositionTracker(
            self.ticker,
            self.paradex_client,
            self.paradex_market,
            self.lighter_base_url,
            self.account_index,
            self.logger
        )
        self.position_tracker.set_refresh_config(
            self.position_refresh_interval,
            self.position_min_request_interval
        )

        self.order_manager.set_paradex_config(
            client=self.paradex_client,
            market=self.paradex_market
        )
        self.order_manager.set_lighter_config(
            self.lighter_client, self.lighter_market_index,
            self.base_amount_multiplier, self.price_multiplier, self.tick_size)
        self.order_manager.set_lighter_position_fetcher(self._fetch_lighter_position_for_fill)

        self.ws_manager.set_lighter_config(
            self.lighter_client, self.lighter_market_index, self.account_index)

        try:
            self.ws_manager.start_lighter_websocket()
            self.logger.info("Lighter WebSocket 任务已启动")

            self.logger.info("等待 Lighter 初始订单簿数据...")
            timeout = 10
            ready = await self.order_book_manager.wait_for_lighter_ready(timeout=timeout)
            if ready and self.order_book_manager.lighter_order_book_ready:
                self.logger.info("已收到 Lighter 订单簿数据")
            else:
                self.logger.warning("Lighter 订单簿未就绪")

        except Exception as e:
            self.logger.error(f"启动 Lighter WebSocket 失败: {e}")
            return

        await self.position_tracker.refresh_positions(force=True)

        while not self.stop_flag:
            now = time.monotonic()
            self._log_status(now)
            await self._maybe_handle_telegram(now)
            if self.max_position <= 0:
                if not self._max_position_warned:
                    self._max_position_warned = True
                    self.logger.warning("max_position 为 0，不会下单")
                await asyncio.sleep(self.loop_sleep)
                continue

            if self.order_book_manager.is_lighter_order_book_stale(self.lighter_book_stale_after):
                if now - self._last_stale_log_ts > 2:
                    self._last_stale_log_ts = now
                    self.logger.warning("Lighter 订单簿已过期，等待更新")
                self.ws_manager.request_resync()
                await self.order_book_manager.wait_for_lighter_update(timeout=self.loop_sleep)
                continue

            try:
                paradex_mid = await self._fetch_paradex_mid_cached()
                self.latest_paradex_mid = paradex_mid
                self._last_paradex_mid = paradex_mid
            except Exception as e:
                self.logger.error(f"获取 Paradex 价格失败: {e}")
                await asyncio.sleep(self.loop_sleep)
                continue

            lighter_bid, lighter_ask = self.order_book_manager.get_lighter_bbo()
            self._last_lighter_bid = lighter_bid
            self._last_lighter_ask = lighter_ask
            if lighter_bid and lighter_ask:
                self._last_lighter_mid = (lighter_bid + lighter_ask) / Decimal("2")
            if not lighter_bid or not lighter_ask:
                await self.order_book_manager.wait_for_lighter_update(timeout=self.loop_sleep)
                continue

            await self._check_lighter_rest_consistency(lighter_bid, lighter_ask)

            long_spread = lighter_bid - paradex_mid
            short_spread = paradex_mid - lighter_ask
            self._last_long_spread = long_spread
            self._last_short_spread = short_spread
            self._update_spread_stats(long_spread, short_spread)

            long_entry, long_std = self._dynamic_threshold(
                self.long_spread_stats, self.long_ex_threshold,
                self.spread_entry_std, self.long_ex_threshold)
            long_exit, _ = self._dynamic_threshold(
                self.long_spread_stats, self.long_ex_threshold,
                self.spread_exit_std, Decimal("0"))
            if self.long_spread_stats.count() < self.spread_min_samples:
                long_exit = self.long_ex_threshold * Decimal("0.5")
            long_exit = min(long_entry, long_exit)

            short_entry, short_std = self._dynamic_threshold(
                self.short_spread_stats, self.short_ex_threshold,
                self.spread_entry_std, self.short_ex_threshold)
            short_exit, _ = self._dynamic_threshold(
                self.short_spread_stats, self.short_ex_threshold,
                self.spread_exit_std, Decimal("0"))
            if self.short_spread_stats.count() < self.spread_min_samples:
                short_exit = self.short_ex_threshold * Decimal("0.5")
            short_exit = min(short_entry, short_exit)
            self._last_long_entry = long_entry
            self._last_short_entry = short_entry
            self._last_long_exit = long_exit
            self._last_short_exit = short_exit

            long_ex = long_spread > long_entry
            short_ex = short_spread > short_entry

            if self.no_signal_log_interval > 0 and not long_ex and not short_ex:
                if now - self._last_no_signal_log_ts > self.no_signal_log_interval:
                    self._last_no_signal_log_ts = now
                    self.logger.info(
                        "未触发交易: "
                        f"做多差价={self._format_optional(long_spread)} "
                        f"做多触发线={self._format_optional(long_entry)} "
                        f"做空差价={self._format_optional(short_spread)} "
                        f"做空触发线={self._format_optional(short_entry)} "
                        f"做多退出线={self._format_optional(long_exit)} "
                        f"做空退出线={self._format_optional(short_exit)} "
                        f"样本数={self.long_spread_stats.count()}/{self.short_spread_stats.count()}"
                    )

            self.data_logger.log_bbo_to_csv(
                maker_bid=paradex_mid if paradex_mid else Decimal('0'),
                maker_ask=paradex_mid if paradex_mid else Decimal('0'),
                lighter_bid=lighter_bid if lighter_bid else Decimal('0'),
                lighter_ask=lighter_ask if lighter_ask else Decimal('0'),
                long_maker=long_ex,
                short_maker=short_ex,
                long_maker_threshold=long_entry,
                short_maker_threshold=short_entry
            )

            if self.stop_flag:
                break

            try:
                await self.position_tracker.refresh_positions()
            except Exception as e:
                self.logger.error(f"刷新仓位失败: {e}")
                await asyncio.sleep(self.loop_sleep)
                continue
            paradex_pos = self.position_tracker.get_current_paradex_position()
            lighter_pos = self.position_tracker.get_current_lighter_position()
            current_net = paradex_pos + lighter_pos
            arb_position = self._calc_arb_position(paradex_pos, lighter_pos)
            if abs(current_net) > self.position_imbalance_limit:
                if now - self._last_imbalance_log_ts > 2:
                    self._last_imbalance_log_ts = now
                    self.logger.warning(
                        f"仓位不平衡 {current_net}，跳过新交易")
                try:
                    await self.position_tracker.refresh_positions(force=True)
                except Exception as e:
                    self.logger.error(f"刷新仓位失败: {e}")
                await self.order_book_manager.wait_for_lighter_update(timeout=self.loop_sleep)
                continue

            if self.order_manager.waiting_for_lighter_fill:
                await self.order_book_manager.wait_for_lighter_update(timeout=self.loop_sleep)
                continue

            target_position = self._determine_target_position(
                arb_position, long_spread, short_spread,
                long_entry, long_exit, short_entry, short_exit,
                long_std, short_std
            )
            if not self.trading_enabled:
                if arb_position > 0:
                    target_position = max(Decimal("0"), min(target_position, arb_position))
                elif arb_position < 0:
                    target_position = min(Decimal("0"), max(target_position, arb_position))
                else:
                    target_position = Decimal("0")
            delta = target_position - arb_position
            max_trade = self.order_quantity * Decimal(self.spread_max_tiers)
            trade_qty = min(abs(delta), max_trade)
            capped_qty = self._cap_trade_qty(trade_qty, delta, paradex_pos, lighter_pos)
            if trade_qty > 0 and capped_qty <= 0:
                if now - self._last_max_position_log_ts > 2:
                    self._last_max_position_log_ts = now
                    self.logger.warning("已到最大仓位，暂停开新仓")
                await self.order_book_manager.wait_for_lighter_update(timeout=self.loop_sleep)
                continue
            trade_qty = capped_qty

            if delta > 0 and trade_qty > 0:
                await self._execute_long_trade(trade_qty)
            elif delta < 0 and trade_qty > 0:
                await self._execute_short_trade(trade_qty)
            else:
                await self.order_book_manager.wait_for_lighter_update(timeout=self.loop_sleep)

    async def _execute_long_trade(self, quantity: Decimal):
        if self.stop_flag:
            return
        if quantity <= 0:
            return

        try:
            await asyncio.wait_for(self.position_tracker.refresh_positions(), timeout=5.0)
        except asyncio.TimeoutError:
            if self.stop_flag:
                return
            self.logger.warning("获取仓位超时")
            return
        except Exception as e:
            if self.stop_flag:
                return
            self.logger.error(f"获取仓位失败: {e}")
            return

        paradex_pos = self.position_tracker.get_current_paradex_position()
        lighter_pos = self.position_tracker.get_current_lighter_position()
        current_net = paradex_pos + lighter_pos
        arb_before = self._calc_arb_position(paradex_pos, lighter_pos)
        if abs(current_net) > self.position_imbalance_limit:
            self.logger.error(f"仓位差过大: {current_net}")
            return

        action = "CLOSE_SHORT" if arb_before < 0 else "OPEN_LONG"

        try:
            paradex_price = await self.order_manager.place_paradex_market_order(
                'buy',
                quantity,
            )
            if paradex_price is None:
                return

            log_price = paradex_price
            if log_price <= 0:
                log_price = self.latest_paradex_mid or Decimal('0')

            self.position_tracker.update_paradex_position(quantity)
            self.paradex_pnl.update(quantity, log_price)
            self._pending_trade = {
                "action": action,
                "paradex_side": "BUY",
                "quantity": quantity,
                "paradex_price": log_price,
                "ts": time.time(),
            }
            self.data_logger.log_trade_to_csv(
                exchange='paradex',
                side='LONG',
                price=str(log_price),
                quantity=str(quantity)
            )

            tx_hash = await self.order_manager.place_lighter_market_order(
                'sell',
                quantity,
                log_price,
                self.stop_flag
            )
            if tx_hash:
                action_label = self._format_trade_action(action)
                message = (
                    f"{self.ticker} {action_label} 已提交\n"
                    f"Paradex BUY {self._format_optional(quantity)} @ "
                    f"{self._format_optional(log_price)}\n"
                    f"Lighter sell {self._format_optional(quantity)} @ "
                    f"{self._format_optional(log_price)}\n"
                    f"{self._build_status_message()}"
                )
                self._queue_telegram_message(message)

        except Exception as e:
            if self.stop_flag:
                return
            self.logger.error(f"执行做多交易失败: {e}")
            self.logger.error(f"完整堆栈: {traceback.format_exc()}")
            self._pending_trade = None
            return

    async def _execute_short_trade(self, quantity: Decimal):
        if self.stop_flag:
            return
        if quantity <= 0:
            return

        try:
            await asyncio.wait_for(self.position_tracker.refresh_positions(), timeout=5.0)
        except asyncio.TimeoutError:
            if self.stop_flag:
                return
            self.logger.warning("获取仓位超时")
            return
        except Exception as e:
            if self.stop_flag:
                return
            self.logger.error(f"获取仓位失败: {e}")
            return

        paradex_pos = self.position_tracker.get_current_paradex_position()
        lighter_pos = self.position_tracker.get_current_lighter_position()
        current_net = paradex_pos + lighter_pos
        arb_before = self._calc_arb_position(paradex_pos, lighter_pos)
        if abs(current_net) > self.position_imbalance_limit:
            self.logger.error(f"仓位差过大: {current_net}")
            return

        action = "CLOSE_LONG" if arb_before > 0 else "OPEN_SHORT"

        try:
            paradex_price = await self.order_manager.place_paradex_market_order(
                'sell',
                quantity,
            )
            if paradex_price is None:
                return

            log_price = paradex_price
            if log_price <= 0:
                log_price = self.latest_paradex_mid or Decimal('0')

            self.position_tracker.update_paradex_position(-quantity)
            self.paradex_pnl.update(-quantity, log_price)
            self._pending_trade = {
                "action": action,
                "paradex_side": "SELL",
                "quantity": quantity,
                "paradex_price": log_price,
                "ts": time.time(),
            }
            self.data_logger.log_trade_to_csv(
                exchange='paradex',
                side='SHORT',
                price=str(log_price),
                quantity=str(quantity)
            )

            tx_hash = await self.order_manager.place_lighter_market_order(
                'buy',
                quantity,
                log_price,
                self.stop_flag
            )
            if tx_hash:
                action_label = self._format_trade_action(action)
                message = (
                    f"{self.ticker} {action_label} 已提交\n"
                    f"Paradex SELL {self._format_optional(quantity)} @ "
                    f"{self._format_optional(log_price)}\n"
                    f"Lighter buy {self._format_optional(quantity)} @ "
                    f"{self._format_optional(log_price)}\n"
                    f"{self._build_status_message()}"
                )
                self._queue_telegram_message(message)

        except Exception as e:
            if self.stop_flag:
                return
            self.logger.error(f"执行做空交易失败: {e}")
            self.logger.error(f"完整堆栈: {traceback.format_exc()}")
            self._pending_trade = None
            return

    async def run(self):
        self.setup_signal_handlers()

        try:
            await self.trading_loop()
        except KeyboardInterrupt:
            self.logger.info("\n收到中断信号...")
        except asyncio.CancelledError:
            self.logger.info("\n任务已取消...")
        finally:
            self.logger.info("正在清理...")
            self.shutdown()
            try:
                await asyncio.wait_for(self._async_cleanup(), timeout=5.0)
            except asyncio.TimeoutError:
                self.logger.warning("清理超时，强制退出")
            except Exception as e:
                self.logger.error(f"清理时出错: {e}")
