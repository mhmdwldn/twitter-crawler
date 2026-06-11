"""Twitter search-tweets controller — the main crawling handler."""

import logging

from controllers.twitter import TwitterControllers


class TwitterSearchTweets(TwitterControllers):
    """Handler that executes a tweet search and sends results to output.

    Deduplicates tweets by ID across pages and stops once the target
    count is reached. Sends optional Telegram alerts on start, completion,
    and failure.

    Job dict fields (also overridable via CLI):
        - ``query``: Twitter search query (native operators supported)
        - ``target``: number of unique tweets to collect
        - ``max_pages``: maximum result pages to crawl (default 1)
        - ``since`` / ``until``: ISO date bounds (YYYY-MM-DD)
        - ``output_file``: optional path to save raw JSON results
    """

    log = logging.getLogger("twitter.search_tweets")

    async def handler(self, job: dict):
        """Execute the search and pipe results to the output driver."""
        query = self.parse_job_query(job)
        target = self.parse_job_target(job)
        max_pages = self.parse_job_max_pages(job)
        since = self.parse_job_date(job, "since")
        until = self.parse_job_date(job, "until")
        output_file = self.args.get("output_file") or job.get("output_file")

        self.log.info(
            "Searching Twitter: query=%r  target=%d  max_pages=%d  range=%s..%s",
            query, target, max_pages, since or "all-time", until or "all-time",
        )
        await self.notifier.send_message(
            f"Crawl started.\nQuery: {query}\nTarget: {target}\n"
            f"Range: {since or 'all-time'} to {until or 'all-time'}"
        )

        await self._ensure_api()

        try:
            # Only accumulate the full list when we'll need to save to file
            tweets_data: list[dict] | None = [] if output_file else None
            seen_ids: set[str] = set()

            async for event in self.api.search_tweets(
                query=query,
                max_pages=max_pages,
                since=since,
                until=until,
            ):
                tweet = event.payload
                if tweet.tweet_id in seen_ids:
                    continue
                seen_ids.add(tweet.tweet_id)

                # Serialize once to JSON string for the output driver
                tweet_json = tweet.model_dump_json(exclude_none=True)
                if tweets_data is not None:
                    tweets_data.append(
                        tweet.model_dump(mode="json", exclude_none=True)
                    )

                self.log.info(
                    "  [tweet_id=%s] @%s: %s",
                    tweet.tweet_id, tweet.username, tweet.content[:80],
                )
                await self.send_output(tweet_json)

                if len(seen_ids) >= target:
                    self.log.info("Target of %d tweets reached", target)
                    break

            if output_file and tweets_data:
                self.save_to_file(tweets_data, output_file)

            self.log.info(
                "Search complete — %d unique tweets scraped for %r",
                len(seen_ids), query,
            )
            await self.notifier.send_message(
                f"Crawl finished.\nQuery: {query}\n"
                f"Collected: {len(seen_ids)}/{target} unique tweets"
            )

        except Exception as exc:
            await self.notifier.send_message(f"Crawl failed.\nQuery: {query}\nError: {exc}")
            raise
        finally:
            await self._close_api()

    # ------------------------------------------------------------------
    # Convenience: scrape without an output driver
    # ------------------------------------------------------------------

    async def scrape_to_json(self, job: dict) -> list[dict]:
        """Scrape and return raw dicts — no output driver involved.

        Useful when called programmatically or in ``--mode scrape``.
        """
        query = self.parse_job_query(job)
        target = self.parse_job_target(job)
        max_pages = self.parse_job_max_pages(job)
        since = self.parse_job_date(job, "since")
        until = self.parse_job_date(job, "until")

        tweets: list[dict] = []
        seen_ids: set[str] = set()
        await self._ensure_api()
        try:
            async for event in self.api.search_tweets(
                query=query,
                max_pages=max_pages,
                since=since,
                until=until,
            ):
                tweet = event.payload
                if tweet.tweet_id in seen_ids:
                    continue
                seen_ids.add(tweet.tweet_id)
                tweets.append(tweet.model_dump(mode="json", exclude_none=True))
                if len(tweets) >= target:
                    break
        finally:
            await self._close_api()

        return tweets
