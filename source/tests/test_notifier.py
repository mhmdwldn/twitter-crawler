"""Tests for library/notifier.py — TelegramNotifier."""

from __future__ import annotations

import pytest
from pytest_mock import MockerFixture

from library.config import TelegramSettings
from library.notifier import TelegramNotifier


class TestTelegramNotifier:
    def test_disabled_without_credentials(self) -> None:
        notifier = TelegramNotifier(TelegramSettings(bot_token=None, chat_id=None))
        assert notifier.enabled is False

    def test_enabled_with_credentials(self, telegram_settings: TelegramSettings) -> None:
        notifier = TelegramNotifier(telegram_settings)
        assert notifier.enabled is True

    @pytest.mark.asyncio
    async def test_send_message_noop_when_disabled(self, mocker: MockerFixture) -> None:
        notifier = TelegramNotifier(TelegramSettings())
        client_cls = mocker.patch("library.notifier.httpx.AsyncClient")
        assert await notifier.send_message("hello") is False
        client_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_send_message_posts_to_bot_api(
        self, mocker: MockerFixture, telegram_settings: TelegramSettings
    ) -> None:
        notifier = TelegramNotifier(telegram_settings)

        mock_resp = mocker.MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_client = mocker.AsyncMock()
        mock_client.post.return_value = mock_resp
        mock_client.__aenter__.return_value = mock_client
        mocker.patch("library.notifier.httpx.AsyncClient", return_value=mock_client)

        assert await notifier.send_message("crawl done") is True

        url = mock_client.post.call_args.args[0]
        assert url.endswith("/sendMessage")
        assert telegram_settings.bot_token in url
        payload = mock_client.post.call_args.kwargs["json"]
        assert payload["chat_id"] == telegram_settings.chat_id
        assert payload["text"] == "crawl done"

    @pytest.mark.asyncio
    async def test_send_message_swallows_http_errors(
        self, mocker: MockerFixture, telegram_settings: TelegramSettings
    ) -> None:
        import httpx

        notifier = TelegramNotifier(telegram_settings)

        mock_client = mocker.AsyncMock()
        mock_client.post.side_effect = httpx.ConnectError("no network")
        mock_client.__aenter__.return_value = mock_client
        mocker.patch("library.notifier.httpx.AsyncClient", return_value=mock_client)

        # Must not raise — alerts are best-effort
        assert await notifier.send_message("crawl done") is False
