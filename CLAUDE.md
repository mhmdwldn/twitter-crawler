# CLAUDE.md

Entry point for AI agents (and humans) working on this project. Read this
before making changes.

## Project Overview

**Twitter End-to-End Crawler** — a config-driven, fully async scraper
pipeline that searches tweets through Nitter-style mirror sites and ships
them to Kafka, Elasticsearch, a file, or stdout. Built as a portfolio-grade
example of a production data-engineering pipeline.

What it does per crawl:

1. Builds a search URL from a Twitter query (native `since:`/`until:`
   operators supported) for the current mirror.
2. Fetches the page with `httpx`, transparently solving Anubis anti-bot
   challenges (SHA-256 proof-of-work) and rotating to the next mirror on
   failure.
3. Parses tweets out of the HTML with BeautifulSoup into Pydantic `Tweet`
   models, wraps each in a `KafkaEvent` envelope.
4. Deduplicates by tweet ID, follows the pagination cursor, and streams
   each tweet to the configured output driver until the target count or
   page limit is reached.
5. Optionally sends Telegram alerts on crawl start / completion / failure
   (best-effort, never breaks the crawl).

The architecture mirrors the reference project
`D:\Kerjaan\project\portofolio\tiktok-end-to-end-crawler` (same
Controllers / helpers / library layering), with one deliberate upgrade:
the output-driver interface is **async-native** (`async def put/close`)
instead of sync-with-background-threads.

## Tech Stack

| Layer            | Library                              | Role |
|------------------|--------------------------------------|------|
| Runtime          | Python 3.10+, `asyncio`              | Single event loop for the whole pipeline |
| Config           | `pydantic-settings` (`BaseSettings`) | Env vars + YAML, zero hardcoded values |
| Validation       | Pydantic v2 (`BaseModel`)            | `Tweet`, `TweetSearchRequest`, `KafkaEvent` data contracts |
| HTTP client      | `httpx` (async)                      | Mirror fetching, Anubis solve, Telegram alerts |
| HTML parsing     | `beautifulsoup4`                     | Tweet extraction from search pages |
| Message queue    | `aiokafka`                           | `AIOKafkaProducer` output driver + `AIOKafkaConsumer` (Kafka→ES) + admin in `setup_infra.py` |
| Search / storage | `elasticsearch[async]` (pinned `<9`) | `AsyncElasticsearch` output driver (needs `aiohttp`; client major must match the ES server, so pinned to 8.x for an ES 8.x cluster) |
| Testing          | `pytest` + `pytest-asyncio` + `pytest-mock` | Fully mocked, no live services |

## Project Structure

```
twitter-crawler/
├── CLAUDE.md                          # This file
├── README.md                          # Human-facing docs
├── Dockerfile                         # python:3.11-slim image, ENTRYPOINT main.py
├── config.yaml                        # Sample YAML config (NO secrets)
├── requirements.txt                   # Pointer -> source/requirements.txt
├── LICENSE                            # MIT
├── skills/                            # Vendored Claude Code skill packs (dev tooling,
│                                      #   not runtime code; see skills/exploration.md)
└── source/                            # Application root — run everything from here
    ├── main.py                        # argparse CLI: crawler --mode scrape|full
    ├── requirements.txt               # Runtime + test dependencies
    ├── .gitignore / .dockerignore
    ├── controllers/
    │   ├── __init__.py                # Controllers(ABC): job loop, output dispatch, exceptions
    │   └── twitter/
    │       ├── __init__.py            # TwitterControllers: API lifecycle, job parsing helpers
    │       └── search_tweets.py       # TwitterSearchTweets: search handler + scrape_to_json
    ├── exception/
    │   ├── __init__.py
    │   └── exception.py               # TwitterCrawlerException hierarchy + factory error
    ├── helpers/
    │   ├── __init__.py
    │   ├── input/
    │   │   ├── __init__.py            # Input facade
    │   │   └── driver/
    │   │       ├── __init__.py        # InputDriver(ABC)
    │   │       ├── std.py             # StdInputDriver (JSON job file or single empty job)
    │   │       └── factory/__init__.py
    │   └── output/
    │       ├── __init__.py            # Output facade (async put/close)
    │       └── driver/
    │           ├── __init__.py        # OutputDriver(ABC) — async interface
    │           ├── kafka.py           # KafkaOutputDriver (lazy AIOKafkaProducer)
    │           ├── elasticsearch.py   # ElasticsearchOutputDriver (AsyncElasticsearch)
    │           ├── file.py            # FileOutputDriver (JSON Lines append)
    │           ├── std.py             # StdOutputDriver (stdout)
    │           └── factory/__init__.py# OutputDriverFactory (registry pattern)
    ├── library/
    │   ├── __init__.py
    │   ├── config.py                  # Settings (BaseSettings, TWITTER_ env prefix, YAML)
    │   ├── schemas.py                 # Tweet, TweetSearchRequest, KafkaEvent (Pydantic v2)
    │   ├── twitter_api.py             # TwitterAPI client + parse_search_page + Anubis solver
    │   ├── notifier.py                # TelegramNotifier (async, best-effort)
    │   ├── consumer.py                # KafkaSinkConsumer + run_kafka_to_elasticsearch
    │   └── setup_infra.py             # Create Kafka topic + ES index (async, CLI script)
    ├── deployment/
    │   ├── compose.yaml               # Local Kafka + ES + Kibana
    │   ├── 01-configmap.yaml          # K8s ConfigMap (config.yaml)
    │   └── 02-deployment.yaml         # K8s Deployment (secrets via secretKeyRef)
    └── tests/                         # pytest suite (see "How to test")
```

## Architecture Patterns

- **Config-driven (zero hardcoding).** Every tunable — mirrors, query
  defaults, Kafka topic, ES index, timeouts, rate limits, Telegram
  credentials — lives in `library/config.py` as a `BaseSettings` field.
  Source priority: constructor kwargs → `TWITTER_*` env vars →
  `config.yaml` (section `twitter_crawler`) → `.env`. Never add a literal
  URL/topic/index/token to code; add a settings field instead.
- **Async-first.** All network I/O (HTTP, Kafka, ES, Telegram) is async on
  one event loop. No threads, no private loops. Output drivers expose
  `async def put()` / `async def close()`.
- **SOLID / Open-Closed.** The engine (`Controllers.main`, `Output`,
  `Input`, factories) is closed for modification. New crawler types and
  drivers plug in via subclassing + registry (see extension guide below).
- **Schema-first contracts.** Anything crossing a process boundary is a
  Pydantic v2 model: `TweetSearchRequest` (outbound query),
  `Tweet` (parsed data), `KafkaEvent` (envelope, `extra="forbid"`).
- **Best-effort side channels.** Telegram alerting and ES indexing log
  errors instead of raising — a broken side channel must not kill a crawl.
  Mirror failures, by contrast, raise `MirrorsExhaustedException` after
  `max_mirror_rotations` full passes.

## How to Run

```bash
# Setup (Python 3.10+)
python -m venv .venv && .venv\Scripts\activate     # Windows
pip install -r source/requirements.txt

# Optional local infra (Kafka + ES + Kibana)
docker compose -f source/deployment/compose.yaml up -d
cd source && python library/setup_infra.py          # create topic + index

# Run the crawler (always from source/)
cd source
python main.py crawler --mode scrape --query "openclaw bug" --target 20 --pretty
python main.py crawler --mode full --query "openclaw" -d kafka -o twitter.tweets.raw
python main.py crawler --mode full --query "openclaw" -d elasticsearch -o twitter_tweets

# End-to-end through Kafka: produce, then consume into Elasticsearch
python main.py crawler --mode full --query "openclaw" -d kafka -o twitter.tweets.raw
python main.py consumer --topic twitter.tweets.raw --index twitter_tweets --idle-timeout 8
```

The `consumer` subcommand reads `library/consumer.py`'s `KafkaSinkConsumer`,
which forwards each Kafka message to an injected output sink (the ES driver by
default). `--idle-timeout N` drains the topic then exits; omit it to run
forever. Docs are upserted by `tweet_id`, so reprocessing never duplicates.

Env configuration: export `TWITTER_*` variables (see reference below) or
edit `config.yaml`. Secrets go in env vars only.

## How to Test

```bash
cd source
python -m pytest tests/ -v
```

- Runner: `pytest`; async tests use `pytest-asyncio` in strict mode —
  decorate every async test with `@pytest.mark.asyncio`.
- Mocking: `pytest-mock` (`mocker` fixture). All HTTP/Kafka/ES calls are
  mocked; tests never hit the network.
- Layout: `source/tests/`, one file per module
  (`test_config.py`, `test_schemas.py`, `test_twitter_api.py`,
  `test_controllers.py`, `test_output_drivers.py`, `test_notifier.py`).
  Shared fixtures (sample tweets, HTML pages, settings) live in
  `tests/conftest.py`.
- Run from `source/` so `library/`, `controllers/`, `helpers/` are
  importable (the `tests/` package makes pytest add `source/` to
  `sys.path`).

## Crawler Extension Guide

Add a new crawler type (e.g. user-timeline) **without touching the
engine**:

1. **Schema** — add a request model to `library/schemas.py`
   (e.g. `UserTimelineRequest(BaseModel)`) with a `to_query_params()`
   method. Reuse `Tweet`/`KafkaEvent` for output.
2. **Client method** — add `async def user_timeline(...) ->
   AsyncIterator[KafkaEvent]` to `TwitterAPI` in `library/twitter_api.py`,
   reusing `_fetch_html` (retries + Anubis + rate limit) and
   `parse_search_page` or a new parser.
3. **Controller** — create `controllers/twitter/user_timeline.py` with
   `class TwitterUserTimeline(TwitterControllers)` implementing
   `async def handler(self, job: dict)` (and optionally
   `scrape_to_json`). Follow `search_tweets.py` as the template:
   dedupe set, `await self.send_output(...)`, Telegram notify, `finally:
   await self._close_api()`.
4. **CLI** — register the new `--type` choice in `source/main.py` and
   dispatch to the new controller class.
5. **Settings** — if the new type needs tunables (endpoint path, limits),
   add fields to `TwitterCrawlerSettings` in `library/config.py` and
   mirror them in `config.yaml`.
6. **Tests** — add `tests/test_user_timeline.py` mocking
   `TwitterAPI` exactly like `tests/test_controllers.py`.

Add a new **output destination** the same way: subclass `OutputDriver`
in `helpers/output/driver/<name>.py` and register it in the `_DRIVERS`
dict in `helpers/output/driver/factory/__init__.py`.

## Environment Variables Reference

Prefix `TWITTER_`, nesting delimiter `__`. Complex types (lists) are JSON.

| Variable | Type | Description | Example |
|---|---|---|---|
| `TWITTER_ENVIRONMENT` | str | Deployment environment label | `production` |
| `TWITTER_LOG_LEVEL` | str | Root log level | `INFO` |
| `TWITTER_KAFKA__BOOTSTRAP_SERVERS` | str | Comma-separated broker list | `kafka01:9092,kafka02:9092` |
| `TWITTER_KAFKA__TOPIC` | str | Topic for scraped tweets | `twitter.tweets.raw` |
| `TWITTER_KAFKA__CLIENT_ID` | str | Kafka client ID | `twitter-crawler` |
| `TWITTER_KAFKA__ACKS` | str | Producer acks: `0`, `1`, `all` | `all` |
| `TWITTER_KAFKA__COMPRESSION_TYPE` | str | gzip/snappy/lz4/zstd | `gzip` |
| `TWITTER_ELASTICSEARCH__HOSTS` | list[str] (JSON) | ES node URLs | `["http://es01:9200"]` |
| `TWITTER_ELASTICSEARCH__INDEX_NAME` | str | Target index | `twitter_tweets` |
| `TWITTER_ELASTICSEARCH__API_KEY` | str (secret) | ES API key auth | `bXktYXBpLWtleQ==` |
| `TWITTER_ELASTICSEARCH__USERNAME` | str (secret) | ES basic-auth user | `elastic` |
| `TWITTER_ELASTICSEARCH__PASSWORD` | str (secret) | ES basic-auth password | `changeme` |
| `TWITTER_TELEGRAM__BOT_TOKEN` | str (secret) | Telegram bot token; unset = alerts off | `123456:ABC-DEF...` |
| `TWITTER_TELEGRAM__CHAT_ID` | str (secret) | Telegram chat receiving alerts | `-1001234567890` |
| `TWITTER_CRAWLER__MIRRORS` | list[str] (JSON) | Mirror base URLs in priority order | `["https://nitter.net"]` |
| `TWITTER_CRAWLER__SEARCH_PATH` | str | Search endpoint path | `/search` |
| `TWITTER_CRAWLER__DEFAULT_QUERY` | str | Fallback query | `openclaw bug` |
| `TWITTER_CRAWLER__DEFAULT_TARGET_COUNT` | int ≥1 | Default unique-tweet target | `100` |
| `TWITTER_CRAWLER__MAX_MIRROR_ROTATIONS` | int ≥1 | Mirror-list passes before giving up | `3` |
| `TWITTER_CRAWLER__REQUEST_TIMEOUT` | float | HTTP timeout (s) | `45.0` |
| `TWITTER_CRAWLER__MAX_RETRIES` | int | Retries per page fetch | `4` |
| `TWITTER_CRAWLER__RETRY_BACKOFF` | float | Exponential backoff base | `2.0` |
| `TWITTER_CRAWLER__RATE_LIMIT_RPS` | float | Max requests/second | `2.0` |
| `TWITTER_CRAWLER__USER_AGENT` | str | User-Agent header | `Mozilla/5.0 ...` |
| `TWITTER_CRAWLER__PROXY_URL` | str (secret if authed) | Optional HTTP/SOCKS proxy | `http://proxy:8080` |

## Git Hygiene

**Never commit:**
- Secrets of any kind — Telegram bot tokens/chat IDs, ES API keys or
  passwords, authenticated proxy URLs. They exist only as env vars
  (locally) or Kubernetes Secrets (`secretKeyRef` in
  `deployment/02-deployment.yaml`).
- `.env` / `.env.local` / `*.local.yaml` files.
- Crawl output: `output/`, `results*.json`, `*.jsonl`, `*.log`.
- Virtualenvs, `__pycache__/`, `.pytest_cache/`, IDE state.

**`.gitignore` covers:** the full GitHub Python template (byte-code,
build, venvs, test caches, type-checker caches) plus project-specific
sections for runtime crawl outputs, local config overrides, env files,
Docker overrides, and `.claude/` tooling state. `source/.gitignore` and
`source/.dockerignore` add source-level guards.

The committed `config.yaml` is a sample with **no secrets** — keep it
that way; commented placeholders point to the env vars instead.
