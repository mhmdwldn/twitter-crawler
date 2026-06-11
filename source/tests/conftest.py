"""Shared pytest fixtures for the Twitter crawler test suite."""

from __future__ import annotations

import pytest

from library.config import (
    ElasticsearchSettings,
    KafkaSettings,
    TelegramSettings,
    TwitterCrawlerSettings,
)
from library.schemas import KafkaEvent, Tweet, TweetSearchRequest


# ---------------------------------------------------------------------------
# Sample data builders
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_search_request() -> TweetSearchRequest:
    return TweetSearchRequest(query="openclaw bug")


@pytest.fixture
def sample_tweet_dict() -> dict:
    return {
        "tweet_id": "1923456789012345678",
        "tweet_url": "https://nitter.net/someuser/status/1923456789012345678",
        "search_url": "https://nitter.net/search?f=tweets&q=openclaw",
        "mirror": "https://nitter.net",
        "username": "someuser",
        "display_name": "Some User",
        "content": "openclaw keeps crashing again",
        "tweet_date": "2026-06-09",
        "tweet_date_title": "Jun 9, 2026 · 10:15 PM UTC",
        "relative_time": "2h",
    }


@pytest.fixture
def sample_tweet(sample_tweet_dict: dict) -> Tweet:
    return Tweet.model_validate(sample_tweet_dict)


@pytest.fixture
def sample_kafka_event(sample_tweet: Tweet) -> KafkaEvent:
    return KafkaEvent(
        event_type="twitter.tweet.scraped",
        payload=sample_tweet,
        metadata={"query": "openclaw"},
    )


@pytest.fixture
def sample_search_html() -> str:
    """Minimal Nitter-style search-results page with two tweets and a cursor."""
    return """
    <html><body>
      <div class="timeline">
        <div class="timeline-item" data-username="someuser">
          <a class="tweet-link" href="/someuser/status/1923456789012345678#m"></a>
          <div class="fullname-and-username">
            <a class="fullname">Some User</a>
            <a class="username">@someuser</a>
          </div>
          <span class="tweet-date">
            <a title="Jun 9, 2026 · 10:15 PM UTC">Jun 9</a>
          </span>
          <div class="tweet-content">openclaw keeps crashing again</div>
        </div>
        <div class="timeline-item" data-username="otheruser">
          <a class="tweet-link" href="/otheruser/status/1923456789012345679#m"></a>
          <div class="fullname-and-username">
            <a class="fullname">Other User</a>
            <a class="username">@otheruser</a>
          </div>
          <span class="tweet-date">
            <a title="May 1, 2026 · 08:00 AM UTC">May 1</a>
          </span>
          <div class="tweet-content">openclaw is stuck in a loop</div>
        </div>
        <div class="show-more">
          <a href="?f=tweets&amp;q=openclaw&amp;cursor=NEXT_CURSOR_TOKEN">Load more</a>
        </div>
      </div>
    </body></html>
    """


@pytest.fixture
def sample_last_page_html() -> str:
    """Search-results page with one tweet and no pagination cursor."""
    return """
    <html><body>
      <div class="timeline">
        <div class="timeline-item" data-username="lastuser">
          <a class="tweet-link" href="/lastuser/status/1923456789012345680#m"></a>
          <a class="username">@lastuser</a>
          <span class="tweet-date"><a title="Jun 1, 2026 · 09:00 AM UTC">Jun 1</a></span>
          <div class="tweet-content">final openclaw tweet</div>
        </div>
      </div>
    </body></html>
    """


# ---------------------------------------------------------------------------
# Settings fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def kafka_settings() -> KafkaSettings:
    return KafkaSettings(
        bootstrap_servers="test-kafka:9092",
        topic="test.topic",
    )


@pytest.fixture
def es_settings() -> ElasticsearchSettings:
    return ElasticsearchSettings(
        hosts=["http://test-es:9200"],
        index_name="test_index",
    )


@pytest.fixture
def telegram_settings() -> TelegramSettings:
    return TelegramSettings(
        bot_token="123456:test-token",
        chat_id="-100123456",
    )


@pytest.fixture
def crawler_settings() -> TwitterCrawlerSettings:
    return TwitterCrawlerSettings(
        mirrors=["https://nitter.test"],
        rate_limit_rps=1000.0,
        request_timeout=5.0,
        max_retries=1,
        max_mirror_rotations=1,
    )
