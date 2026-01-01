"""Position tracking for Variational and Lighter exchanges."""
import asyncio
import json
import logging
import requests
import sys
from decimal import Decimal

from variational.models import InstrumentType


class PositionTracker:
    """Tracks positions on both exchanges."""

    def __init__(self, ticker: str, variational_client, variational_instrument: dict,
                 lighter_base_url: str, account_index: int, logger: logging.Logger):
        self.ticker = ticker
        self.variational_client = variational_client
        self.variational_instrument = variational_instrument
        self.lighter_base_url = lighter_base_url
        self.account_index = account_index
        self.logger = logger

        self.variational_position = Decimal('0')
        self.lighter_position = Decimal('0')

    async def _run_variational(self, func, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: func(*args, **kwargs))

    def _matches_variational_instrument(self, instrument: dict) -> bool:
        if not instrument:
            return False
        inst_type = instrument.get("instrument_type")
        inst_type_value = str(inst_type)
        if inst_type_value != str(InstrumentType.PERPETUAL_FUTURE):
            return False
        if instrument.get("underlying") != self.ticker:
            return False
        return True

    async def get_variational_position(self) -> Decimal:
        if not self.variational_client:
            raise Exception("Variational client not initialized")

        def _fetch_positions():
            positions = []
            page = None
            while True:
                resp = self.variational_client.get_portfolio_positions(page=page)
                positions.extend(resp.result)
                next_page = resp.pagination.next_page
                if not next_page:
                    break
                page = next_page
            return positions

        positions = await self._run_variational(_fetch_positions)
        total = Decimal('0')
        for position in positions:
            instrument = position.get("instrument", {})
            if self._matches_variational_instrument(instrument):
                total += Decimal(position.get("qty", "0"))

        return total

    async def get_lighter_position(self) -> Decimal:
        url = f"{self.lighter_base_url}/api/v1/account"
        headers = {"accept": "application/json"}

        current_position = None
        parameters = {"by": "index", "value": self.account_index}
        attempts = 0
        while current_position is None and attempts < 10:
            try:
                response = requests.get(url, headers=headers, params=parameters, timeout=10)
                response.raise_for_status()

                if not response.text.strip():
                    self.logger.warning("Empty response from Lighter API for position check")
                    return self.lighter_position

                data = response.json()

                if 'accounts' not in data or not data['accounts']:
                    self.logger.warning(f"Unexpected response format from Lighter API: {data}")
                    return self.lighter_position

                positions = data['accounts'][0].get('positions', [])
                for position in positions:
                    if position.get('symbol') == self.ticker:
                        current_position = Decimal(position['position']) * position['sign']
                        break
                if current_position is None:
                    current_position = 0

            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Network error getting position: {e}")
            except json.JSONDecodeError as e:
                self.logger.warning(f"JSON parsing error in position response: {e}")
                self.logger.warning(f"Response text: {response.text[:200]}...")
            except Exception as e:
                self.logger.warning(f"Unexpected error getting position: {e}")
            finally:
                attempts += 1
                await asyncio.sleep(1)

        if current_position is None:
            self.logger.error(f"Failed to get Lighter position after {attempts} attempts")
            sys.exit(1)

        return current_position

    def update_variational_position(self, delta: Decimal):
        self.variational_position += delta

    def update_lighter_position(self, delta: Decimal):
        self.lighter_position += delta

    def get_current_variational_position(self) -> Decimal:
        return self.variational_position

    def get_current_lighter_position(self) -> Decimal:
        return self.lighter_position

    def get_net_position(self) -> Decimal:
        return self.variational_position + self.lighter_position
