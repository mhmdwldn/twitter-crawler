#!/usr/bin/env python3
"""
Twitter End-to-End Crawler — CLI Entry Point
=============================================

Follows the template-crawler pattern: argparse CLI -> Controllers -> Input/Output drivers.

Usage:
    # Scrape-only (prints JSON to stdout)
    python main.py crawler --mode scrape --query "openclaw bug" --target 50

    # Scrape + save to file
    python main.py crawler --mode scrape --query "openclaw" --max-pages 5 -o results.json

    # Full pipeline: scrape + publish to Kafka
    python main.py crawler --mode full --query "openclaw" -d kafka -o twitter.tweets.raw --bootstrap-servers localhost:9092

    # Scrape + index to Elasticsearch
    python main.py crawler --mode full --query "openclaw" -d elasticsearch -o twitter_tweets --elasticsearch-hosts http://localhost:9200
"""

import argparse
import asyncio
import json
import logging
import os
import sys

if __name__ == "__main__":
    argp = argparse.ArgumentParser(
        description="Twitter End-to-End Crawler",
    )

    argp.add_argument("-c", "--config", dest="config", type=str, default="config.yaml")
    argp.add_argument("-s", "--source", dest="source", type=str, default=None)
    argp.add_argument("-d", "--destination", dest="destination", type=str, default=None)
    argp.add_argument("-i", "--input", dest="input", type=str, default=None)
    argp.add_argument("-o", "--output", dest="output", type=str, default=None)

    # Kafka
    argp.add_argument("--bootstrap-servers", dest="bootstrap_servers", type=str, default=None)
    # Elasticsearch
    argp.add_argument("--elasticsearch-hosts", dest="elasticsearch_hosts", type=str, default=None)

    # --- Subcommands ---
    argp_sub = argp.add_subparsers(title="action", dest="which", help="-h / --help to see usage")

    argp_crawler = argp_sub.add_parser("crawler", help="Run the Twitter crawler")
    argp_crawler.add_argument("--mode", dest="mode", type=str, default="scrape",
                              choices=["scrape", "full"],
                              help="scrape: JSON only | full: crawl + output driver")
    argp_crawler.add_argument("--type", dest="type", type=str, default="search",
                              choices=["search"],
                              help="search: tweet search by query")
    argp_crawler.add_argument("--query", dest="query", type=str, default=None,
                              help="Twitter search query (native operators supported)")
    argp_crawler.add_argument("--target", dest="target", type=int, default=None,
                              help="Number of unique tweets to collect")
    argp_crawler.add_argument("--max-pages", dest="max_pages", type=int, default=None,
                              help="Maximum result pages to crawl")
    argp_crawler.add_argument("--since", dest="since", type=str, default=None,
                              help="Lower bound date YYYY-MM-DD (omit for all-time)")
    argp_crawler.add_argument("--until", dest="until", type=str, default=None,
                              help="Upper bound date YYYY-MM-DD (omit for all-time)")
    argp_crawler.add_argument("--mirrors", dest="mirrors", nargs="+", default=None,
                              help="Override mirror base URLs from config")
    argp_crawler.add_argument("-o", "--output", dest="output_file", type=str, default=None,
                              help="Save JSON to file (scrape mode) or output destination name (full mode)")
    argp_crawler.add_argument("--pretty", action="store_true", default=False,
                              help="Pretty-print JSON output")
    argp_crawler.add_argument("--log-level", dest="log_level", type=str, default="INFO")
    # Output-driver flags (duplicated from parent so they work after 'crawler' too)
    argp_crawler.add_argument("-d", "--destination", dest="destination_crawler", type=str, default=None,
                              help="Output driver: kafka | elasticsearch | file | std")
    argp_crawler.add_argument("--bootstrap-servers", dest="bootstrap_servers_crawler", type=str, default=None,
                              help="Kafka broker list")
    argp_crawler.add_argument("--elasticsearch-hosts", dest="elasticsearch_hosts_crawler", type=str, default=None,
                              help="ES host URL")

    args = argp.parse_args()

    # Merge: crawler subparser values take precedence over parent parser defaults
    if getattr(args, "bootstrap_servers_crawler", None):
        args.bootstrap_servers = args.bootstrap_servers_crawler
    if getattr(args, "elasticsearch_hosts_crawler", None):
        args.elasticsearch_hosts = args.elasticsearch_hosts_crawler
    if getattr(args, "destination_crawler", None):
        args.destination = args.destination_crawler

    # --- Setup logging ---
    log_level = getattr(args, "log_level", "INFO")
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    log = logging.getLogger("main")

    if args.which != "crawler":
        argp.print_help()
        sys.exit(1)

    from controllers.twitter.search_tweets import TwitterSearchTweets

    # ================================================================
    # Mode: scrape (no output driver — just JSON to stdout/file)
    # ================================================================
    if args.mode == "scrape":
        output_path = args.output_file or args.output
        indent = 2 if args.pretty else None

        kwargs = dict(
            query=args.query,
            target=args.target,
            max_pages=args.max_pages,
            since=args.since,
            until=args.until,
            mirrors=args.mirrors,
        )
        ctl = TwitterSearchTweets(**kwargs)
        tweets = asyncio.run(ctl.scrape_to_json({"query": args.query or ""}))

        if output_path:
            os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(tweets, f, ensure_ascii=False, indent=indent, default=str)
            log.info("Saved %d tweets -> %s", len(tweets), output_path)
            print(f"Saved {len(tweets)} tweets to {output_path}")
        else:
            text = json.dumps(tweets, ensure_ascii=False, indent=indent, default=str)
            try:
                print(text)
            except UnicodeEncodeError:
                print(json.dumps(tweets, ensure_ascii=True, indent=indent, default=str))

        log.info("Scraped %d tweets (type=%s)", len(tweets), args.type)

    # ================================================================
    # Mode: full (crawl + output driver: Kafka / ES / file / std)
    # ================================================================
    elif args.mode == "full":
        if not args.destination:
            log.error("--destination / -d is required for full mode")
            sys.exit(1)

        # Merge: crawler subparser -o takes precedence over parent -o.
        # In full mode -o names the destination (topic/index), not a file —
        # clear output_file so the handler doesn't also write a local file.
        if args.output_file and not args.output:
            args.output = args.output_file
        args.output_file = None

        ctl = TwitterSearchTweets(**vars(args))
        asyncio.run(ctl.main())
