"""Custom exceptions for the Twitter crawler pipeline."""


class TwitterCrawlerException(Exception):
    """Base class for all crawler-specific errors."""


class AnubisChallengeException(TwitterCrawlerException):
    """Raised when an Anubis anti-bot challenge cannot be solved."""

    def __str__(self) -> str:
        return super().__str__() or "Anubis challenge could not be solved"


class MirrorsExhaustedException(TwitterCrawlerException):
    """Raised when every configured mirror has failed repeatedly."""

    def __str__(self) -> str:
        return super().__str__() or "All mirrors exhausted"


class PageFetchException(TwitterCrawlerException):
    """Raised when a search page cannot be fetched after all retries."""

    def __str__(self) -> str:
        return super().__str__() or "Page fetch failed"


class RateLimitExceeded(TwitterCrawlerException):
    """Raised when a mirror responds with HTTP 429 / Too Many Requests."""

    def __str__(self) -> str:
        return super().__str__() or "Too Many Requests"


class OutputDriverNotRecognizeException(Exception):
    """Raised by the output driver factory for unknown destinations."""

    def __str__(self) -> str:
        return super().__str__() or "Destination not recognized"
