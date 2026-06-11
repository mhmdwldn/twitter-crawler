"""
Pydantic v2 data schemas for the Twitter crawler pipeline.

Defines:
  - TweetSearchRequest:  query parameters for a mirror search page
  - Tweet:               parsed tweet data extracted from search results
  - KafkaEvent:          event envelope published to Kafka
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Optional
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Request schema
# ---------------------------------------------------------------------------


class TweetSearchRequest(BaseModel):
    """Search request for a Nitter-style mirror's ``/search`` page.

    The ``since``/``until`` bounds are encoded into the query string using
    Twitter's native ``since:``/``until:`` search operators.
    """

    query: str = Field(
        ...,
        min_length=1,
        max_length=1000,
        description="Twitter search query (supports native search operators)",
    )
    cursor: Optional[str] = Field(
        default=None,
        description="Pagination cursor extracted from the previous page",
    )
    since: Optional[date] = Field(
        default=None,
        description="Lower bound tweet date (inclusive)",
    )
    until: Optional[date] = Field(
        default=None,
        description="Upper bound tweet date (inclusive)",
    )

    def to_query_params(self) -> dict[str, str]:
        """Encode the request as URL query parameters for the search page."""
        query_parts = [self.query]
        if self.since:
            query_parts.append(f"since:{self.since.isoformat()}")
        if self.until:
            query_parts.append(f"until:{self.until.isoformat()}")

        params = {"f": "tweets", "q": " ".join(query_parts)}
        if self.cursor:
            params["cursor"] = self.cursor
        return params


# ---------------------------------------------------------------------------
# Tweet schema
# ---------------------------------------------------------------------------


class Tweet(BaseModel):
    """A single tweet parsed from a mirror's search-results page."""

    tweet_id: str = Field(..., min_length=1, description="Numeric tweet/status ID")
    tweet_url: str = Field(..., description="Canonical URL of the tweet on the mirror")
    search_url: str = Field(default="", description="Search-page URL the tweet was found on")
    mirror: str = Field(default="", description="Mirror base URL that served the tweet")
    username: str = Field(default="", description="Author handle without the @ prefix")
    display_name: str = Field(default="", description="Author display name")
    content: str = Field(default="", description="Tweet text content")
    tweet_date: Optional[date] = Field(
        default=None,
        description="Tweet date parsed from the timestamp tooltip",
    )
    tweet_date_title: str = Field(
        default="",
        description="Raw timestamp tooltip text (e.g. 'Jun 9, 2026 · 10:15 PM UTC')",
    )
    relative_time: str = Field(default="", description="Relative time label (e.g. '2h')")
    raw_html: Optional[str] = Field(
        default=None,
        description="Raw HTML of the timeline card, for reprocessing",
    )

    model_config = ConfigDict(extra="allow", populate_by_name=True)


# ---------------------------------------------------------------------------
# Kafka event envelope
# ---------------------------------------------------------------------------


class KafkaEvent(BaseModel):
    """Standardised event envelope for Kafka messages."""

    event_id: str = Field(default_factory=lambda: uuid4().hex)
    event_type: str = Field(default="twitter.tweet.scraped")
    source: str = Field(default="twitter-crawler")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(tz=timezone.utc))
    payload: Tweet = Field(...)
    metadata: dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="forbid", populate_by_name=True)
