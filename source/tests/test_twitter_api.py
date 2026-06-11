"""Tests for library/twitter_api.py — TwitterAPI client and HTML parsing."""

from __future__ import annotations

from datetime import date

import pytest
from pytest_mock import MockerFixture

from exception.exception import MirrorsExhaustedException, PageFetchException
from library.schemas import KafkaEvent
from library.twitter_api import (
    TwitterAPI,
    is_anubis_challenge,
    parse_date_from_title,
    parse_search_page,
)

MIRROR = "https://nitter.test"
SEARCH_URL = "https://nitter.test/search?f=tweets&q=openclaw"


class TestParsing:
    def test_parse_search_page(self, sample_search_html: str) -> None:
        tweets, cursor = parse_search_page(sample_search_html, MIRROR, SEARCH_URL)
        assert len(tweets) == 2
        assert tweets[0].tweet_id == "1923456789012345678"
        assert tweets[0].username == "someuser"
        assert tweets[0].content == "openclaw keeps crashing again"
        assert tweets[0].tweet_date == date(2026, 6, 9)
        assert tweets[0].tweet_url == f"{MIRROR}/someuser/status/1923456789012345678"
        assert cursor == "NEXT_CURSOR_TOKEN"

    def test_parse_last_page_no_cursor(self, sample_last_page_html: str) -> None:
        tweets, cursor = parse_search_page(sample_last_page_html, MIRROR, SEARCH_URL)
        assert len(tweets) == 1
        assert cursor is None

    def test_date_filter_since(self, sample_search_html: str) -> None:
        tweets, _ = parse_search_page(
            sample_search_html, MIRROR, SEARCH_URL, since=date(2026, 6, 1)
        )
        assert len(tweets) == 1
        assert tweets[0].tweet_date == date(2026, 6, 9)

    def test_date_filter_until(self, sample_search_html: str) -> None:
        tweets, _ = parse_search_page(
            sample_search_html, MIRROR, SEARCH_URL, until=date(2026, 5, 31)
        )
        assert len(tweets) == 1
        assert tweets[0].tweet_date == date(2026, 5, 1)

    def test_parse_date_from_title(self) -> None:
        assert parse_date_from_title("Jun 9, 2026 · 10:15 PM UTC") == date(2026, 6, 9)
        assert parse_date_from_title("9 Jun 2026") == date(2026, 6, 9)
        assert parse_date_from_title("") is None
        assert parse_date_from_title("not a date") is None

    def test_is_anubis_challenge(self) -> None:
        challenge_html = (
            '<script id="preact_info">{"anubis_challenge": "x", '
            '"connection_security_message": "y"}</script>'
        )
        assert is_anubis_challenge(challenge_html) is True
        assert is_anubis_challenge("<html>regular page</html>") is False


class TestTwitterAPI:
    @pytest.mark.asyncio
    async def test_start_creates_client(self, crawler_settings) -> None:
        api = TwitterAPI(crawler_settings)
        await api.start()
        assert api._client is not None
        await api.stop()

    @pytest.mark.asyncio
    async def test_async_context_manager(self, crawler_settings) -> None:
        async with TwitterAPI(crawler_settings) as api:
            assert api._client is not None
        assert api._client is None

    @pytest.mark.asyncio
    async def test_search_yields_kafka_events(
        self, mocker: MockerFixture, crawler_settings, sample_last_page_html: str
    ) -> None:
        api = TwitterAPI(crawler_settings)
        mocker.patch.object(
            api, "_fetch_html", new=mocker.AsyncMock(return_value=sample_last_page_html)
        )

        events = [e async for e in api.search_tweets(query="openclaw", max_pages=3)]
        assert len(events) == 1
        assert isinstance(events[0], KafkaEvent)
        assert events[0].payload.tweet_id == "1923456789012345680"
        assert events[0].metadata["query"] == "openclaw"

    @pytest.mark.asyncio
    async def test_search_paginates_until_cursor_exhausted(
        self,
        mocker: MockerFixture,
        crawler_settings,
        sample_search_html: str,
        sample_last_page_html: str,
    ) -> None:
        api = TwitterAPI(crawler_settings)
        fetch = mocker.AsyncMock(side_effect=[sample_search_html, sample_last_page_html])
        mocker.patch.object(api, "_fetch_html", new=fetch)

        events = [e async for e in api.search_tweets(query="openclaw", max_pages=10)]
        # page 1 yields 2 tweets + cursor, page 2 yields 1 tweet, no cursor
        assert len(events) == 3
        assert fetch.call_count == 2
        # second request must carry the cursor from page 1
        assert "cursor=NEXT_CURSOR_TOKEN" in fetch.call_args_list[1].args[0]

    @pytest.mark.asyncio
    async def test_search_respects_max_pages(
        self, mocker: MockerFixture, crawler_settings, sample_search_html: str
    ) -> None:
        api = TwitterAPI(crawler_settings)
        fetch = mocker.AsyncMock(return_value=sample_search_html)
        mocker.patch.object(api, "_fetch_html", new=fetch)

        events = [e async for e in api.search_tweets(query="openclaw", max_pages=2)]
        assert len(events) == 4  # 2 tweets per page x 2 pages
        assert fetch.call_count == 2

    @pytest.mark.asyncio
    async def test_mirror_rotation_on_failure(
        self, mocker: MockerFixture, sample_last_page_html: str
    ) -> None:
        from library.config import TwitterCrawlerSettings

        settings = TwitterCrawlerSettings(
            mirrors=["https://bad.mirror", "https://good.mirror"],
            rate_limit_rps=1000.0,
            max_retries=1,
            max_mirror_rotations=1,
        )
        api = TwitterAPI(settings)

        async def fetch(url: str) -> str:
            if "bad.mirror" in url:
                raise PageFetchException(f"HTTP 503 for {url}")
            return sample_last_page_html

        mocker.patch.object(api, "_fetch_html", new=mocker.AsyncMock(side_effect=fetch))

        events = [e async for e in api.search_tweets(query="openclaw", max_pages=1)]
        assert len(events) == 1
        assert events[0].payload.mirror == "https://good.mirror"

    @pytest.mark.asyncio
    async def test_mirrors_exhausted_raises(
        self, mocker: MockerFixture, crawler_settings
    ) -> None:
        api = TwitterAPI(crawler_settings)
        mocker.patch.object(
            api,
            "_fetch_html",
            new=mocker.AsyncMock(side_effect=PageFetchException("HTTP 503")),
        )

        with pytest.raises(MirrorsExhaustedException):
            async for _ in api.search_tweets(query="openclaw", max_pages=1):
                pass

    def test_build_search_url(self, crawler_settings) -> None:
        from library.schemas import TweetSearchRequest

        api = TwitterAPI(crawler_settings)
        url = api._build_search_url(
            "https://nitter.test", TweetSearchRequest(query="openclaw bug")
        )
        assert url.startswith("https://nitter.test/search?")
        assert "f=tweets" in url

    def test_default_headers_match_browser_fingerprint(self, crawler_settings) -> None:
        headers = TwitterAPI(crawler_settings)._default_headers()
        assert headers["accept"].startswith("text/html,application/xhtml+xml")
        assert headers["accept-language"] == crawler_settings.accept_language
        assert headers["cache-control"] == "no-cache"
        assert headers["pragma"] == "no-cache"
        assert headers["priority"] == "u=0, i"
        assert headers["sec-ch-ua"] == (
            '"Google Chrome";v="149", "Chromium";v="149", "Not)A;Brand";v="24"'
        )
        assert headers["sec-ch-ua-mobile"] == "?0"
        assert headers["sec-ch-ua-platform"] == '"Windows"'
        assert headers["sec-fetch-dest"] == "document"
        assert headers["sec-fetch-mode"] == "navigate"
        assert headers["sec-fetch-site"] == "same-origin"
        assert headers["sec-fetch-user"] == "?1"
        assert headers["upgrade-insecure-requests"] == "1"
        assert headers["user-agent"] == crawler_settings.user_agent
        # Referer is per-mirror, never a static default header
        assert "referer" not in {k.lower() for k in headers}

    def test_referer_is_mirror_root(self) -> None:
        referer = TwitterAPI._referer_for(
            "https://nitter.net/search?f=tweets&q=openclaw&cursor=ABC"
        )
        assert referer == "https://nitter.net/"

    @pytest.mark.asyncio
    async def test_fetch_sends_browser_headers(
        self, mocker: MockerFixture, crawler_settings, sample_last_page_html: str
    ) -> None:
        """Real request path must carry the fingerprint + per-mirror referer."""
        api = TwitterAPI(crawler_settings)
        await api.start()
        try:
            mock_resp = mocker.MagicMock()
            mock_resp.text = sample_last_page_html
            mock_resp.status_code = 200
            get = mocker.patch.object(
                api._client, "get", new=mocker.AsyncMock(return_value=mock_resp)
            )

            await api._fetch_html("https://nitter.test/search?f=tweets&q=openclaw")

            # Per-request referer points at the mirror root
            assert get.call_args.kwargs["headers"]["referer"] == "https://nitter.test/"
            # Fingerprint headers live on the client itself
            assert api._client.headers["sec-fetch-mode"] == "navigate"
            assert api._client.headers["user-agent"] == crawler_settings.user_agent
        finally:
            await api.stop()
