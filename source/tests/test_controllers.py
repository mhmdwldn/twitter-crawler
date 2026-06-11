"""Tests for controllers/twitter/search_tweets.py — TwitterSearchTweets controller."""

from __future__ import annotations

import pytest
from pytest_mock import MockerFixture

from controllers.twitter.search_tweets import TwitterSearchTweets
from library.schemas import KafkaEvent, Tweet


def _make_event(tweet_id: str) -> KafkaEvent:
    return KafkaEvent(
        payload=Tweet(
            tweet_id=tweet_id,
            tweet_url=f"https://nitter.test/u/status/{tweet_id}",
            username="someuser",
            content="openclaw keeps crashing",
        ),
        metadata={"query": "openclaw"},
    )


def _patch_api(mocker: MockerFixture, events: list[KafkaEvent]) -> None:
    """Patch TwitterAPI lifecycle and search with canned events."""

    async def mock_search(*args, **kwargs):
        for event in events:
            yield event

    mocker.patch(
        "controllers.twitter.TwitterAPI.search_tweets",
        side_effect=mock_search,
    )
    mocker.patch("controllers.twitter.TwitterAPI.start", new_callable=mocker.AsyncMock)
    mocker.patch("controllers.twitter.TwitterAPI.stop", new_callable=mocker.AsyncMock)


class TestTwitterSearchTweetsController:
    @pytest.mark.asyncio
    async def test_scrape_to_json(self, mocker: MockerFixture) -> None:
        """scrape_to_json() should return a list of raw tweet dicts."""
        _patch_api(mocker, [_make_event("111"), _make_event("222")])

        ctl = TwitterSearchTweets(query="openclaw", target=10, max_pages=1)
        tweets = await ctl.scrape_to_json({"query": "openclaw"})

        assert len(tweets) == 2
        assert tweets[0]["tweet_id"] == "111"

    @pytest.mark.asyncio
    async def test_scrape_deduplicates(self, mocker: MockerFixture) -> None:
        """Duplicate tweet IDs across pages are collected only once."""
        _patch_api(mocker, [_make_event("111"), _make_event("111"), _make_event("222")])

        ctl = TwitterSearchTweets(query="openclaw", target=10, max_pages=1)
        tweets = await ctl.scrape_to_json({"query": "openclaw"})

        assert len(tweets) == 2

    @pytest.mark.asyncio
    async def test_scrape_stops_at_target(self, mocker: MockerFixture) -> None:
        """Collection stops once the target count is reached."""
        _patch_api(mocker, [_make_event(str(i)) for i in range(10)])

        ctl = TwitterSearchTweets(query="openclaw", target=3, max_pages=1)
        tweets = await ctl.scrape_to_json({"query": "openclaw"})

        assert len(tweets) == 3

    @pytest.mark.asyncio
    async def test_handler_sends_output(self, mocker: MockerFixture) -> None:
        """handler() should call send_output for each unique tweet."""
        _patch_api(mocker, [_make_event("111"), _make_event("222")])
        send_spy = mocker.patch.object(
            TwitterSearchTweets, "send_output", new_callable=mocker.AsyncMock
        )

        ctl = TwitterSearchTweets(query="openclaw", target=10, max_pages=1)
        await ctl.handler({"query": "openclaw"})

        assert send_spy.call_count == 2

    @pytest.mark.asyncio
    async def test_handler_notifies_on_failure(self, mocker: MockerFixture) -> None:
        """A crawl failure triggers a Telegram alert and re-raises."""

        async def failing_search(*args, **kwargs):
            raise RuntimeError("boom")
            yield  # pragma: no cover — makes this an async generator

        mocker.patch(
            "controllers.twitter.TwitterAPI.search_tweets",
            side_effect=failing_search,
        )
        mocker.patch("controllers.twitter.TwitterAPI.start", new_callable=mocker.AsyncMock)
        mocker.patch("controllers.twitter.TwitterAPI.stop", new_callable=mocker.AsyncMock)

        ctl = TwitterSearchTweets(query="openclaw", target=10, max_pages=1)
        notify_spy = mocker.patch.object(
            ctl.notifier, "send_message", new_callable=mocker.AsyncMock
        )

        with pytest.raises(RuntimeError):
            await ctl.handler({"query": "openclaw"})

        sent_texts = [call.args[0] for call in notify_spy.call_args_list]
        assert any("failed" in text.lower() for text in sent_texts)

    def test_parse_job_query_cli_precedence(self, mocker: MockerFixture) -> None:
        ctl = TwitterSearchTweets(query="cli-query")
        assert ctl.parse_job_query({"query": "job-query"}) == "cli-query"

    def test_parse_job_date(self) -> None:
        from datetime import date

        ctl = TwitterSearchTweets(query="x", since="2026-01-15")
        assert ctl.parse_job_date({}, "since") == date(2026, 1, 15)
        assert ctl.parse_job_date({}, "until") is None
