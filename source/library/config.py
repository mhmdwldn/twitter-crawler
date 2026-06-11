"""
Configuration module for the Twitter scraper pipeline.

All settings are loaded via Pydantic BaseSettings, supporting:
  - Environment variables (prefixed with TWITTER_, nested with __)
  - YAML configuration file (config.yaml, section ``twitter_crawler``)
  - Direct initialisation overrides

Zero hardcoded values — every tunable is defined here.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic_settings.sources import YamlConfigSettingsSource


class KafkaSettings(BaseSettings):
    """Apache Kafka connection and producer configuration."""

    bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Comma-separated list of Kafka broker addresses",
    )
    topic: str = Field(
        default="twitter.tweets.raw",
        description="Default Kafka topic for scraped tweets",
    )
    client_id: str = Field(
        default="twitter-crawler",
        description="Kafka client identifier",
    )
    acks: str = Field(
        default="all",
        description="Producer acknowledgment level: 0, 1, or 'all'",
    )
    compression_type: Optional[str] = Field(
        default="gzip",
        description="Compression codec: gzip, snappy, lz4, zstd, or None",
    )
    max_request_size: int = Field(
        default=1_048_576,
        description="Maximum request size in bytes (default 1 MB)",
    )
    linger_ms: int = Field(
        default=10,
        description="Artificial delay in ms to batch outgoing messages",
    )
    request_timeout_ms: int = Field(
        default=30_000,
        description="Kafka producer request timeout in ms",
    )


class ElasticsearchSettings(BaseSettings):
    """Elasticsearch connection and indexing configuration."""

    hosts: list[str] = Field(
        default=["http://localhost:9200"],
        description="List of Elasticsearch node URLs",
    )
    index_name: str = Field(
        default="twitter_tweets",
        description="Target Elasticsearch index",
    )
    api_key: Optional[str] = Field(
        default=None,
        description="Optional API key for Elasticsearch authentication",
    )
    username: Optional[str] = Field(
        default=None,
        description="Basic-auth username",
    )
    password: Optional[str] = Field(
        default=None,
        description="Basic-auth password",
    )
    request_timeout: int = Field(
        default=30,
        description="ES client request timeout in seconds",
    )
    max_retries: int = Field(
        default=3,
        description="Number of retries on transient failures",
    )


class TelegramSettings(BaseSettings):
    """Telegram alerting configuration.

    Alerts are optional — when ``bot_token`` or ``chat_id`` is unset the
    notifier silently no-ops. Both values are secrets and must only be
    provided via environment variables (TWITTER_TELEGRAM__BOT_TOKEN,
    TWITTER_TELEGRAM__CHAT_ID), never committed to YAML or source.
    """

    api_base_url: str = Field(
        default="https://api.telegram.org",
        description="Telegram Bot API base URL",
    )
    bot_token: Optional[str] = Field(
        default=None,
        description="Telegram bot token (secret — env var only)",
    )
    chat_id: Optional[str] = Field(
        default=None,
        description="Telegram chat ID receiving crawl alerts",
    )
    request_timeout: float = Field(
        default=30.0,
        description="HTTP timeout for Telegram API calls in seconds",
    )


class CrawlerSettings(BaseSettings):
    """Generic crawler HTTP configuration."""

    request_timeout: float = Field(
        default=45.0,
        description="HTTP request timeout in seconds",
    )
    max_retries: int = Field(
        default=4,
        description="Maximum retry attempts on transient HTTP errors",
    )
    retry_backoff: float = Field(
        default=2.0,
        description="Exponential backoff multiplier for retries",
    )
    user_agent: str = Field(
        default=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/149.0.0.0 Safari/537.36"
        ),
        description="Default User-Agent header for HTTP requests "
                    "(keep in sync with the sec-ch-ua browser fingerprint)",
    )
    accept_language: str = Field(
        default="en-US,en;q=0.9,id;q=0.8",
        description="Accept-Language header for HTTP requests",
    )
    rate_limit_rps: float = Field(
        default=2.0,
        description="Maximum requests per second (per crawler instance)",
    )
    proxy_url: Optional[str] = Field(
        default=None,
        description="Optional HTTP/SOCKS proxy URL",
    )


class TwitterCrawlerSettings(CrawlerSettings):
    """Twitter-specific crawler configuration (Nitter-style mirrors)."""

    mirrors: list[str] = Field(
        default=[
            "https://nitter.privacyredirect.com",
            "https://nitter.net",
            "https://nitter.tiekoetter.com",
        ],
        description="Nitter-style mirror base URLs, tried in order",
    )
    search_path: str = Field(
        default="/search",
        description="Search endpoint path on each mirror",
    )
    default_query: str = Field(
        default="",
        description="Default search query when none is supplied via CLI/job",
    )
    default_target_count: int = Field(
        default=100,
        ge=1,
        description="Default number of unique tweets to collect",
    )
    max_mirror_rotations: int = Field(
        default=3,
        ge=1,
        description="Full passes over the mirror list before giving up",
    )


class Settings(BaseSettings):
    """Root settings aggregating all sub-configurations."""

    model_config = SettingsConfigDict(
        env_prefix="TWITTER_",
        env_nested_delimiter="__",
        yaml_file="../config.yaml",
        yaml_config_section="twitter_crawler",
        case_sensitive=False,
    )

    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    elasticsearch: ElasticsearchSettings = Field(default_factory=ElasticsearchSettings)
    telegram: TelegramSettings = Field(default_factory=TelegramSettings)
    crawler: TwitterCrawlerSettings = Field(default_factory=TwitterCrawlerSettings)

    environment: str = Field(default="development")
    log_level: str = Field(default="INFO")

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings,
        env_settings,
        dotenv_settings,
        file_secret_settings,
    ):
        """Customise the settings source priority.

        Priority (highest first):
          1. Constructor / init kwargs
          2. Environment variables
          3. YAML config file (if present)
          4. .env / dotenv files
          5. File secrets
        """
        yaml_path = cls._resolve_yaml_path(settings_cls)
        sources = [
            init_settings,
            env_settings,
        ]
        if yaml_path and yaml_path.exists():
            section = settings_cls.model_config.get("yaml_config_section")
            sources.append(
                YamlConfigSettingsSource(
                    settings_cls,
                    yaml_file=str(yaml_path),
                    yaml_config_section=section,
                )
            )
        sources.extend([dotenv_settings, file_secret_settings])
        return tuple(sources)

    @staticmethod
    def _resolve_yaml_path(settings_cls: type[BaseSettings]) -> Path | None:
        """Resolve the YAML config path — checks multiple locations."""
        yaml_file = settings_cls.model_config.get("yaml_file", "../config.yaml")
        candidates = [
            Path(yaml_file),                          # relative to CWD
            Path(__file__).resolve().parent.parent.parent / "config.yaml",  # project root
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate
        return candidates[0]  # return primary path even if missing (will be skipped)


# Singleton settings instance — import this throughout the application.
settings = Settings()
