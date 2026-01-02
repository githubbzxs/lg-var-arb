import asyncio
import logging
import os
from typing import List

import requests


class TelegramNotifier:
    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger
        self.enabled = os.getenv("TELEGRAM_ENABLED", "0").lower() in (
            "1",
            "true",
            "yes",
            "y",
            "on",
        )
        self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        self.timeout = float(os.getenv("TELEGRAM_TIMEOUT", "5"))
        self._last_update_id = None

    def _ready(self) -> bool:
        return self.enabled and self.bot_token and self.chat_id

    def _send_sync(self, message: str) -> bool:
        if not self._ready():
            return False
        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            resp = requests.post(
                url,
                data={"chat_id": self.chat_id, "text": message},
                timeout=self.timeout,
            )
            resp.raise_for_status()
            return True
        except Exception as exc:
            self.logger.warning(f"Telegram send failed: {exc}")
            return False

    def send_sync(self, message: str) -> bool:
        return self._send_sync(message)

    async def send(self, message: str) -> bool:
        if not self._ready():
            return False
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._send_sync, message)

    def _fetch_updates_sync(self) -> List[str]:
        if not self._ready():
            return []
        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/getUpdates"
            params = {"timeout": 0}
            if self._last_update_id is not None:
                params["offset"] = self._last_update_id + 1
            resp = requests.get(url, params=params, timeout=self.timeout)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            self.logger.warning(f"Telegram poll failed: {exc}")
            return []

        updates = []
        for item in data.get("result", []):
            update_id = item.get("update_id")
            if update_id is not None:
                if self._last_update_id is None or update_id > self._last_update_id:
                    self._last_update_id = update_id
            message = item.get("message") or {}
            chat_id = str((message.get("chat") or {}).get("id", ""))
            text = message.get("text")
            if chat_id == self.chat_id and text:
                updates.append(text.strip())
        return updates

    async def fetch_commands(self) -> List[str]:
        if not self._ready():
            return []
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._fetch_updates_sync)
