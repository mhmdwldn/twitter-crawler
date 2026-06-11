"""Tests for library/config.py — Pydantic BaseSettings."""

from __future__ import annotations

import pytest

from library.config import (
    ElasticsearchSettings,
    KafkaSettings,
    Settings,
    TelegramSettings,
    TwitterCrawlerSettings,
)


class TestKafkaSettings:
    def test_defaults(self) -> None:
        ks = KafkaSettings()
        assert ks.bootstrap_servers == "localhost:9092"
        assert ks.topic == "twitter.tweets.raw"

    def test_override_via_init(self) -> None:
        ks = KafkaSettings(bootstrap_servers="kafka:29092", topic="custom.topic")
        assert ks.bootstrap_servers == "kafka:29092"
        assert ks.topic == "custom.topic"


class TestElasticsearchSettings:
    def test_defaults(self) -> None:
        es = ElasticsearchSettings()
        assert es.hosts == ["http://localhost:9200"]
        assert es.index_name == "twitter_tweets"
        assert es.api_key is None


class TestTelegramSettings:
    def test_disabled_by_default(self) -> None:
        tg = TelegramSettings()
        assert tg.bot_token is None
        assert tg.chat_id is None

    def test_env_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BOT_TOKEN", "abc:123")
        tg = TelegramSettings()
        assert tg.bot_token == "abc:123"


class TestTwitterCrawlerSettings:
    def test_defaults(self) -> None:
        cs = TwitterCrawlerSettings()
        assert len(cs.mirrors) >= 1
        assert cs.search_path == "/search"
        assert cs.rate_limit_rps == 2.0

    def test_target_count_bounds(self) -> None:
        with pytest.raises(Exception):
            TwitterCrawlerSettings(default_target_count=0)


class TestRootSettings:
    def test_nested_settings_created(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("library.config.Path.exists", lambda _self: False)
        settings = Settings()
        assert isinstance(settings.kafka, KafkaSettings)
        assert isinstance(settings.elasticsearch, ElasticsearchSettings)
        assert isinstance(settings.telegram, TelegramSettings)
        assert isinstance(settings.crawler, TwitterCrawlerSettings)

    def test_nested_env_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("library.config.Path.exists", lambda _self: False)
        monkeypatch.setenv("TWITTER_KAFKA__TOPIC", "env.topic")
        monkeypatch.setenv("TWITTER_CRAWLER__RATE_LIMIT_RPS", "9.5")
        settings = Settings()
        assert settings.kafka.topic == "env.topic"
        assert settings.crawler.rate_limit_rps == 9.5
