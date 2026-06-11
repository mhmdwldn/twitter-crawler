"""
Telegram notifier — async crawl alerts via the Telegram Bot API.

Alerts are best-effort: failures are logged and never propagate, so a
Telegram outage cannot break a crawl. When the bot token or chat ID is
unconfigured the notifier silently no-ops.
"""

from __future__ import annotations

import logging
from pathlib import Path

import httpx

from library.config import TelegramSettings

logger = logging.getLogger(__name__)


class TelegramNotifier:
    """Send crawl lifecycle alerts to a Telegram chat.

    Example::

        notifier = TelegramNotifier(settings.telegram)
        await notifier.send_message("Crawl started")
    """

    def __init__(self, settings: TelegramSettings) -> None:
        self._settings = settings

    @property
    def enabled(self) -> bool:
        """True when both bot token and chat ID are configured."""
        return bool(self._settings.bot_token and self._settings.chat_id)

    async def send_message(self, text: str) -> bool:
        """Send a plain-text message. Returns True when delivered."""
        if not self.enabled:
            return False
        url = self._api_url("sendMessage")
        try:
            async with httpx.AsyncClient(timeout=self._settings.request_timeout) as client:
                resp = await client.post(
                    url,
                    json={
                        "chat_id": self._settings.chat_id,
                        "text": text,
                        "disable_web_page_preview": True,
                    },
                )
                resp.raise_for_status()
            return True
        except httpx.HTTPError as exc:
            logger.warning("Telegram message failed: %s", exc)
            return False

    async def send_document(self, path: Path, caption: str = "") -> bool:
        """Upload a file as a document. Returns True when delivered."""
        if not self.enabled:
            return False
        url = self._api_url("sendDocument")
        try:
            content = Path(path).read_bytes()
            async with httpx.AsyncClient(timeout=self._settings.request_timeout * 4) as client:
                resp = await client.post(
                    url,
                    data={"chat_id": self._settings.chat_id, "caption": caption},
                    files={"document": (Path(path).name, content, "application/json")},
                )
                resp.raise_for_status()
            return True
        except (OSError, httpx.HTTPError) as exc:
            logger.warning("Telegram document upload failed: %s", exc)
            return False

    def _api_url(self, method: str) -> str:
        """Build a Bot API method URL."""
        base = self._settings.api_base_url.rstrip("/")
        return f"{base}/bot{self._settings.bot_token}/{method}"
