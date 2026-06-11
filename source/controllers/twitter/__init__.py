"""Twitter controllers — shared base for all Twitter-related handlers."""

import json
import logging
import os
from datetime import date, datetime

from controllers import Controllers
from library.notifier import TelegramNotifier
from library.twitter_api import TwitterAPI


class TwitterControllers(Controllers):
    """Shared base for Twitter crawler controllers.

    Sets up the TwitterAPI client and Telegram notifier, and provides
    helper methods for parsing jobs and saving intermediate results.
    """

    log = logging.getLogger("twitter.controller")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Load settings from library
        from library.config import settings

        self.settings = settings
        self.notifier = TelegramNotifier(settings.telegram)
        self.api: TwitterAPI | None = None

    async def _ensure_api(self):
        """Lazily initialise the TwitterAPI client."""
        if self.api is None:
            crawler_settings = self.settings.crawler
            mirrors = self.args.get("mirrors")
            if mirrors:
                crawler_settings = crawler_settings.model_copy(
                    update={"mirrors": list(mirrors)}
                )
            self.api = TwitterAPI(crawler_settings)
            await self.api.start()

    async def _close_api(self):
        """Tear down the TwitterAPI client."""
        if self.api is not None:
            await self.api.stop()
            self.api = None

    # ------------------------------------------------------------------
    # Job helpers
    # ------------------------------------------------------------------

    def parse_job_query(self, job: dict, default: str | None = None) -> str:
        """Extract the search query from job dict, CLI args, or settings."""
        query = job.get("query") or default or self.settings.crawler.default_query
        if self.args.get("query"):
            query = self.args["query"]
        return str(query).strip('"').strip("'")

    def parse_job_target(self, job: dict) -> int:
        """Extract the target tweet count from job dict or CLI args."""
        if self.args.get("target"):
            return int(self.args["target"])
        return int(job.get("target", self.settings.crawler.default_target_count))

    def parse_job_max_pages(self, job: dict, default: int = 1) -> int:
        """Extract max_pages from job dict or CLI args."""
        if self.args.get("max_pages"):
            return int(self.args["max_pages"])
        return int(job.get("max_pages", default))

    def parse_job_date(self, job: dict, key: str) -> date | None:
        """Extract an ISO date bound (``since``/``until``) from job or CLI args."""
        value = self.args.get(key) or job.get(key)
        if not value:
            return None
        if isinstance(value, date):
            return value
        return datetime.strptime(str(value), "%Y-%m-%d").date()

    def save_to_file(self, data, path: str):
        """Save JSON-serialisable *data* to a local file."""
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
        self.log.info("Saved to %s", path)
