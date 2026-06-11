"""
Twitter search client for Nitter-style mirrors.

Provides async methods to paginate tweet search results, transparently
solving Anubis anti-bot challenges and rotating across mirrors on failure.
Used by controllers as the HTTP data-access layer.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from datetime import date, datetime
from typing import Any, AsyncIterator, Optional
from urllib.parse import parse_qs, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from exception.exception import (
    AnubisChallengeException,
    MirrorsExhaustedException,
    PageFetchException,
)
from library.config import TwitterCrawlerSettings
from library.schemas import KafkaEvent, Tweet, TweetSearchRequest

logger = logging.getLogger(__name__)

_STATUS_ID_RE = re.compile(r"/status/(\d+)")
_TWEET_DATE_FORMATS = ("%b %d, %Y", "%d %b %Y")


def is_anubis_challenge(html: str) -> bool:
    """Return True when *html* is an Anubis anti-bot challenge page."""
    return (
        'id="preact_info"' in html
        and "anubis_challenge" in html
        and "connection_security_message" in html
    )


def parse_date_from_title(title: str) -> Optional[date]:
    """Parse a tweet date from the timestamp tooltip (e.g. 'Jun 9, 2026 · ...')."""
    if not title:
        return None
    left = title.split("·", 1)[0].strip()
    for fmt in _TWEET_DATE_FORMATS:
        try:
            return datetime.strptime(left, fmt).date()
        except ValueError:
            continue
    return None


def parse_search_page(
    html: str,
    mirror: str,
    search_url: str,
    since: Optional[date] = None,
    until: Optional[date] = None,
) -> tuple[list[Tweet], Optional[str]]:
    """Extract tweets and the next-page cursor from a search-results page.

    Args:
        html: Raw HTML of the search page.
        mirror: Mirror base URL used to resolve relative links.
        search_url: Full URL the page was fetched from (kept for provenance).
        since: Skip tweets dated before this bound (inclusive).
        until: Skip tweets dated after this bound (inclusive).

    Returns:
        A ``(tweets, cursor)`` tuple — ``cursor`` is None on the last page.
    """
    soup = BeautifulSoup(html, "html.parser")
    tweets: list[Tweet] = []

    for card in soup.select(".timeline-item"):
        link = card.select_one('a.tweet-link[href*="/status/"]')
        if not link:
            continue

        href = link.get("href", "")
        match = _STATUS_ID_RE.search(href)
        if not match:
            continue

        date_el = card.select_one(".tweet-date a")
        title = date_el.get("title", "").strip() if date_el else ""
        tweet_date = parse_date_from_title(title)
        if tweet_date and since and tweet_date < since:
            continue
        if tweet_date and until and tweet_date > until:
            continue

        username = str(card.get("data-username") or "")
        username_el = card.select_one(".username")
        if username_el and username_el.get_text(strip=True):
            username = username_el.get_text(strip=True)

        fullname_el = card.select_one(".fullname")
        content_el = card.select_one(".tweet-content")

        tweets.append(
            Tweet(
                tweet_id=match.group(1),
                tweet_url=urljoin(mirror, href.split("#", 1)[0]),
                search_url=search_url,
                mirror=mirror,
                username=username.lstrip("@"),
                display_name=fullname_el.get_text(" ", strip=True) if fullname_el else "",
                content=content_el.get_text("\n", strip=True) if content_el else "",
                tweet_date=tweet_date,
                tweet_date_title=title,
                relative_time=date_el.get_text(" ", strip=True) if date_el else "",
                raw_html=str(card),
            )
        )

    cursor: Optional[str] = None
    load_more = soup.select_one('a[href*="cursor="]')
    if load_more and load_more.get("href"):
        parsed = urlparse(load_more["href"])
        cursor = parse_qs(parsed.query).get("cursor", [None])[0]
    return tweets, cursor


class TwitterAPI:
    """Async HTTP client for searching tweets via Nitter-style mirrors.

    Handles rate limiting, retries, Anubis challenge solving, and
    automatic mirror rotation.

    Example::

        api = TwitterAPI(settings.crawler)
        await api.start()
        async for event in api.search_tweets("openclaw bug"):
            print(event.payload.content)
        await api.stop()
    """

    def __init__(self, settings: TwitterCrawlerSettings) -> None:
        self._settings = settings
        self._client: Optional[httpx.AsyncClient] = None
        self._mirror_index: int = 0
        self._rate_delay: float = (
            1.0 / settings.rate_limit_rps if settings.rate_limit_rps > 0 else 0.0
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Create the async HTTP client."""
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self._settings.request_timeout),
            headers=self._default_headers(),
            follow_redirects=True,
            proxy=self._settings.proxy_url,
        )
        logger.info("TwitterAPI client created (%d mirrors)", len(self._settings.mirrors))

    async def stop(self) -> None:
        """Close the HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        logger.info("TwitterAPI client stopped")

    async def __aenter__(self) -> TwitterAPI:
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.stop()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def search_tweets(
        self,
        query: str,
        max_pages: int = 1,
        since: Optional[date] = None,
        until: Optional[date] = None,
        **kwargs: Any,
    ) -> AsyncIterator[KafkaEvent]:
        """Paginate through search results for *query* across mirrors.

        Pages are fetched sequentially following the mirror's pagination
        cursor. When a page fails the next mirror is tried with the same
        cursor; after ``max_mirror_rotations`` full passes over the mirror
        list, :class:`MirrorsExhaustedException` is raised.

        Args:
            query: Twitter search query (native search operators supported).
            max_pages: Maximum number of result pages to fetch.
            since: Lower bound tweet date (inclusive).
            until: Upper bound tweet date (inclusive).

        Yields:
            :class:`KafkaEvent` per tweet found.
        """
        cursor: Optional[str] = kwargs.get("cursor")
        pages_fetched = 0
        failures = 0
        max_failures = len(self._settings.mirrors) * self._settings.max_mirror_rotations

        while pages_fetched < max_pages:
            mirror = self._current_mirror()
            request = TweetSearchRequest(query=query, cursor=cursor, since=since, until=until)
            search_url = self._build_search_url(mirror, request)

            try:
                html = await self._fetch_html(search_url)
                tweets, next_cursor = parse_search_page(
                    html, mirror, search_url, since=since, until=until
                )
            except (httpx.HTTPError, PageFetchException, AnubisChallengeException) as exc:
                failures += 1
                logger.warning(
                    "Page failed on mirror=%s (%d/%d failures): %s",
                    mirror, failures, max_failures, exc,
                )
                if failures >= max_failures:
                    raise MirrorsExhaustedException(
                        f"All mirrors failed after {failures} attempts: {exc}"
                    ) from exc
                self._rotate_mirror()
                continue

            logger.info(
                "Fetched %d tweets (page=%d mirror=%s cursor=%s)",
                len(tweets), pages_fetched + 1, mirror, "yes" if cursor else "no",
            )
            for tweet in tweets:
                yield self._tweet_to_event(tweet, metadata={"query": query, "page": pages_fetched})

            pages_fetched += 1
            failures = 0

            if not next_cursor:
                logger.info("Search exhausted after %d page(s)", pages_fetched)
                break
            cursor = next_cursor

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _current_mirror(self) -> str:
        """Return the mirror currently in rotation."""
        mirrors = self._settings.mirrors
        return mirrors[self._mirror_index % len(mirrors)]

    def _rotate_mirror(self) -> None:
        """Advance to the next mirror in the list."""
        self._mirror_index += 1

    def _build_search_url(self, mirror: str, request: TweetSearchRequest) -> str:
        """Build the full search URL for *request* on *mirror*."""
        url = httpx.URL(urljoin(mirror, self._settings.search_path))
        return str(url.copy_merge_params(request.to_query_params()))

    def _default_headers(self) -> dict[str, str]:
        """Return browser-like default headers matching a Chrome navigation.

        ``Referer`` is intentionally absent — it is mirror-specific and set
        per request (see :meth:`_referer_for`).
        """
        return {
            "accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "image/avif,image/webp,image/apng,*/*;q=0.8,"
                "application/signed-exchange;v=b3;q=0.7"
            ),
            "accept-language": self._settings.accept_language,
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "priority": "u=0, i",
            "sec-ch-ua": '"Google Chrome";v="149", "Chromium";v="149", "Not)A;Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": self._settings.user_agent,
        }

    @staticmethod
    def _referer_for(url: str) -> str:
        """Build the mirror-root Referer for *url* (e.g. 'https://nitter.net/')."""
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}/"

    async def _fetch_html(self, url: str) -> str:
        """GET *url* with retries, solving Anubis challenges when detected."""
        assert self._client is not None, "HTTP client not initialised — call start()"

        max_retries = max(self._settings.max_retries, 1)
        last_exc: Optional[Exception] = None
        for attempt in range(1, max_retries + 1):
            try:
                await self._throttle()
                logger.debug("GET %s (attempt %d/%d)", url, attempt, max_retries)
                headers = {"referer": self._referer_for(url)}
                resp = await self._client.get(url, headers=headers)
                html = resp.text or ""

                if is_anubis_challenge(html):
                    logger.info("Anubis challenge detected, solving for %s", url)
                    await self._solve_anubis_challenge(str(resp.url), html)
                    resp = await self._client.get(url, headers=headers)
                    html = resp.text or ""
                    if is_anubis_challenge(html):
                        raise AnubisChallengeException(
                            f"Challenge still present after solving on {url}"
                        )

                if resp.status_code >= 400:
                    raise PageFetchException(f"HTTP {resp.status_code} for {url}")
                return html
            except (httpx.HTTPError, PageFetchException, AnubisChallengeException) as exc:
                last_exc = exc
                if attempt == max_retries:
                    break
                wait = min(self._settings.retry_backoff ** attempt, 10.0)
                logger.warning(
                    "Fetch attempt %d/%d failed for %s: %s. Retrying in %.1fs ...",
                    attempt, max_retries, url, exc, wait,
                )
                await asyncio.sleep(wait)

        raise PageFetchException(f"Failed to fetch {url}: {last_exc}") from last_exc

    async def _solve_anubis_challenge(self, base_url: str, html: str) -> None:
        """Solve the SHA-256 proof-of-work Anubis challenge and store cookies."""
        assert self._client is not None

        soup = BeautifulSoup(html, "html.parser")
        script = soup.find("script", id="preact_info")
        if script is None or not script.string:
            raise AnubisChallengeException(
                "Anubis challenge detected but preact_info payload was missing"
            )

        info = json.loads(script.string)
        challenge = info["challenge"]
        # Anubis' "preact" challenge: the answer is sha256(challenge) — no
        # proof-of-work nonce. The redir already carries ?id=...&redir=...,
        # so merge the result param in (a plain params= would overwrite the
        # query string and drop id/redir, yielding HTTP 400).
        result = hashlib.sha256(challenge.encode("utf-8")).hexdigest()
        redir = httpx.URL(urljoin(base_url, info["redir"])).copy_merge_params(
            {"result": result}
        )
        logger.debug("Solving Anubis challenge for %s", base_url)

        resp = await self._client.get(
            redir,
            headers={"referer": self._referer_for(base_url)},
            follow_redirects=False,
        )
        if resp.status_code not in {302, 303, 307, 308}:
            raise AnubisChallengeException(
                f"Challenge solve failed with HTTP {resp.status_code} at {redir}"
            )

        # Follow the redirect once to allow auth cookies to settle.
        location = resp.headers.get("location")
        if location:
            await self._client.get(urljoin(str(resp.url), location))

    async def _throttle(self) -> None:
        """Enforce rate limit."""
        if self._rate_delay > 0:
            await asyncio.sleep(self._rate_delay)

    @staticmethod
    def _tweet_to_event(
        tweet: Tweet, metadata: Optional[dict[str, Any]] = None
    ) -> KafkaEvent:
        """Wrap a parsed Tweet in a KafkaEvent envelope."""
        return KafkaEvent(
            event_type="twitter.tweet.scraped",
            payload=tweet,
            metadata=metadata or {},
        )
