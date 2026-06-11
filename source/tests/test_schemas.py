"""Tests for library/schemas.py — Pydantic v2 models."""

from __future__ import annotations

from datetime import date

import pytest
from pydantic import ValidationError

from library.schemas import KafkaEvent, Tweet, TweetSearchRequest


class TestTweetSearchRequest:
    def test_valid_request(self, sample_search_request: TweetSearchRequest) -> None:
        assert sample_search_request.query == "openclaw bug"
        assert sample_search_request.cursor is None

    def test_missing_query_raises(self) -> None:
        with pytest.raises(ValidationError):
            TweetSearchRequest()

    def test_blank_query_raises(self) -> None:
        with pytest.raises(ValidationError):
            TweetSearchRequest(query="")

    def test_to_query_params_basic(self) -> None:
        req = TweetSearchRequest(query="openclaw")
        assert req.to_query_params() == {"f": "tweets", "q": "openclaw"}

    def test_to_query_params_with_dates(self) -> None:
        req = TweetSearchRequest(
            query="openclaw",
            since=date(2026, 1, 1),
            until=date(2026, 6, 1),
        )
        params = req.to_query_params()
        assert params["q"] == "openclaw since:2026-01-01 until:2026-06-01"

    def test_to_query_params_with_cursor(self) -> None:
        req = TweetSearchRequest(query="openclaw", cursor="ABC123")
        assert req.to_query_params()["cursor"] == "ABC123"


class TestTweet:
    def test_parse_from_dict(self, sample_tweet_dict: dict) -> None:
        tweet = Tweet.model_validate(sample_tweet_dict)
        assert tweet.tweet_id == "1923456789012345678"
        assert tweet.username == "someuser"
        assert tweet.tweet_date == date(2026, 6, 9)

    def test_minimal_tweet(self) -> None:
        tweet = Tweet.model_validate(
            {"tweet_id": "123", "tweet_url": "https://nitter.net/u/status/123"}
        )
        assert tweet.tweet_id == "123"
        assert tweet.tweet_date is None

    def test_missing_tweet_id_raises(self) -> None:
        with pytest.raises(ValidationError):
            Tweet.model_validate({"tweet_url": "https://nitter.net/u/status/123"})

    def test_json_round_trip(self, sample_tweet: Tweet) -> None:
        dumped = sample_tweet.model_dump_json(exclude_none=True)
        restored = Tweet.model_validate_json(dumped)
        assert restored.tweet_id == sample_tweet.tweet_id
        assert restored.tweet_date == sample_tweet.tweet_date


class TestKafkaEvent:
    def test_create_event(self, sample_tweet: Tweet) -> None:
        event = KafkaEvent(payload=sample_tweet, metadata={"query": "openclaw"})
        assert event.event_type == "twitter.tweet.scraped"
        assert event.source == "twitter-crawler"
        assert len(event.event_id) == 32
        assert event.payload == sample_tweet

    def test_extra_fields_forbidden(self, sample_tweet: Tweet) -> None:
        with pytest.raises(ValidationError):
            KafkaEvent(payload=sample_tweet, unknown_field="oops")
