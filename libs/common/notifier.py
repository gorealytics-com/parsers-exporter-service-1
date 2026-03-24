"""Telegram Notifier — async version for transition period.

Ported from: aragog_exporter_service/utils/notifier.py

In the target architecture, this is replaced by:
  Prometheus Alertmanager → Slack/Telegram webhook

During transition, services can optionally import and use this
for critical error notifications.
"""

import io
import os
from typing import Optional

import aiohttp
from loguru import logger


class TelegramNotifier:
    """Async Telegram notifier. Non-blocking, fire-and-forget."""

    def __init__(
        self,
        bot_token: Optional[str] = None,
        chat_id: Optional[str] = None,
    ) -> None:
        self.bot_token = bot_token or os.environ.get("TELEGRAM_BOT_TOKEN", "")
        self.chat_id = chat_id or os.environ.get("TELEGRAM_CHAT_ID", "")
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}/"
        self.env = os.environ.get("ENVIRONMENT", "")
        self._enabled = bool(self.bot_token and self.chat_id)

        if not self._enabled:
            logger.info("TelegramNotifier disabled: missing bot_token or chat_id")

    async def send_message(self, text: str) -> None:
        """Send a text message to the configured Telegram chat."""
        if not self._enabled:
            return

        if self.env == "staging":
            text = f"🧪 [STAGING] {text}"

        try:
            async with aiohttp.ClientSession() as session:
                await session.post(
                    self.base_url + "sendMessage",
                    json={
                        "chat_id": self.chat_id,
                        "text": text,
                        "parse_mode": "HTML",
                    },
                    timeout=aiohttp.ClientTimeout(total=5),
                )
        except Exception as e:
            logger.warning(f"Telegram send_message failed: {e}")

    async def send_document(
        self, data: bytes, filename: str, caption: str
    ) -> None:
        """Send a file document to the configured Telegram chat."""
        if not self._enabled:
            return

        if self.env == "staging":
            caption = f"🧪 [STAGING] {caption}"

        try:
            form = aiohttp.FormData()
            form.add_field("chat_id", self.chat_id)
            form.add_field("caption", caption)
            form.add_field(
                "document",
                io.BytesIO(data),
                filename=filename,
                content_type="application/json",
            )
            async with aiohttp.ClientSession() as session:
                await session.post(
                    self.base_url + "sendDocument",
                    data=form,
                    timeout=aiohttp.ClientTimeout(total=10),
                )
        except Exception as e:
            logger.warning(f"Telegram send_document failed: {e}")
