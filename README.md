# Twitter End-to-End Crawler

<div align="center">

**Config-driven, event-driven scraper pipeline for Twitter data via Nitter-style mirrors**

[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Pydantic](https://img.shields.io/badge/pydantic-v2-e92063.svg)](https://docs.pydantic.dev/latest/)
[![Docker](https://img.shields.io/badge/docker-ready-2496ED.svg)](https://www.docker.com/)

</div>

---

## Overview

A production-grade, fully async scraper pipeline that searches tweets through
Nitter-style mirror sites and ships them to Kafka, Elasticsearch, a file, or
stdout. It transparently solves Anubis anti-bot challenges, rotates across
mirrors on failure, deduplicates tweets, and (optionally) sends Telegram
alerts on crawl start/finish/failure.

### Operation Modes

| Mode | `--type` | Description |
|------|----------|-------------|
| `scrape` | `search` | Search by query → JSON to stdout or file |
| `full` | `search` | Crawl + publish to an output driver (Kafka/ES/file/std) |

### Design Principles

| Principle | Implementation |
|-----------|---------------|
| **Zero hardcoding** | Pydantic `BaseSettings` — env vars (`TWITTER_*`), YAML, or constructor overrides |
| **Open/Closed** | `Controllers(ABC)` + `OutputDriver(ABC)` + `InputDriver(ABC)` — extend without touching the engine |
| **Factory pattern** | `OutputDriverFactory` / `InputDriverFactory` — destination resolved at runtime |
| **Fully async** | `httpx` for HTTP, `aiokafka` for Kafka, `AsyncElasticsearch` for ES — one event loop, no threads |
| **Schema-first** | Pydantic v2 `BaseModel` for requests, tweets, and the Kafka event envelope |
| **Best-effort alerting** | Telegram failures are logged, never break a crawl |

---

## Project Structure

```
twitter-crawler/
├── Dockerfile                         # Docker image (python:3.11-slim)
├── config.yaml                        # YAML configuration (no secrets)
├── CLAUDE.md                          # AI-agent entry point / project guide
├── README.md
├── requirements.txt                   # -> source/requirements.txt
│
└── source/
    ├── main.py                        # CLI entry point (argparse)
    ├── requirements.txt               # Python dependencies
    │
    ├── controllers/
    │   ├── __init__.py                #   Controllers(ABC) — main loop, input/output, exceptions
    │   └── twitter/
    │       ├── __init__.py            #   TwitterControllers — API client lifecycle, job helpers
    │       └── search_tweets.py       #   TwitterSearchTweets — tweet-search handler
    │
    ├── exception/
    │   └── exception.py               #   Crawler exception hierarchy
    │
    ├── helpers/
    │   ├── input/                     #   Input facade + StdInputDriver (+ factory)
    │   └── output/                    #   Output facade + Kafka/ES/file/std drivers (+ factory)
    │
    ├── library/
    │   ├── config.py                  #   Pydantic v2 BaseSettings (TWITTER_* env + YAML)
    │   ├── schemas.py                 #   Pydantic v2 models (Tweet, TweetSearchRequest, KafkaEvent)
    │   ├── twitter_api.py             #   TwitterAPI — async mirror client, Anubis solver, parser
    │   ├── notifier.py                #   TelegramNotifier — async best-effort alerts
    │   └── setup_infra.py             #   Kafka topic + ES index creation (async)
    │
    ├── deployment/
    │   ├── compose.yaml               #   Kafka + ES + Kibana for local dev
    │   ├── 01-configmap.yaml          #   Kubernetes ConfigMap
    │   └── 02-deployment.yaml         #   Kubernetes Deployment (secrets via Secret refs)
    │
    └── tests/                         #   pytest + pytest-asyncio suite
```

---

## Quick Start

```bash
# 1. Install dependencies
pip install -r source/requirements.txt

# 2. (optional) Start local Kafka + Elasticsearch
docker compose -f source/deployment/compose.yaml up -d

# 3. (optional) Create topic + index
cd source && python library/setup_infra.py

# 4. Scrape to stdout
cd source
python main.py crawler --mode scrape --query "openclaw bug" --target 20 --max-pages 2 --pretty
```

### CLI Examples

```bash
# Scrape + save to file
python main.py crawler --mode scrape --query "openclaw" --target 100 --max-pages 10 -o results.json

# Date-bounded search
python main.py crawler --mode scrape --query "openclaw" --since 2026-01-01 --until 2026-06-01

# Full pipeline -> Kafka
python main.py crawler --mode full --query "openclaw" -d kafka -o twitter.tweets.raw --bootstrap-servers localhost:9092

# Full pipeline -> Elasticsearch
python main.py crawler --mode full --query "openclaw" -d elasticsearch -o twitter_tweets --elasticsearch-hosts http://localhost:9200

# Custom mirrors
python main.py crawler --mode scrape --query "openclaw" --mirrors https://nitter.net https://nitter.tiekoetter.com
```

---

## Configuration

Configuration priority (highest first): **constructor kwargs → env vars → `config.yaml` → `.env`**.

All env vars use the `TWITTER_` prefix with `__` as the nesting delimiter.
See [CLAUDE.md](CLAUDE.md#environment-variables-reference) for the full
reference table. Secrets (Telegram token, ES credentials) must be provided
via env vars only:

```bash
export TWITTER_TELEGRAM__BOT_TOKEN="123456:ABC..."   # enables Telegram alerts
export TWITTER_TELEGRAM__CHAT_ID="-100123456"
export TWITTER_ELASTICSEARCH__API_KEY="..."
```

---

## Testing

```bash
cd source
python -m pytest tests/ -v
```

The suite covers settings, schemas, HTML parsing, pagination, mirror
rotation, controllers, output drivers, and the Telegram notifier. All
network I/O is mocked — no live services needed.

---

## Docker

```bash
docker build -t twitter-crawler .
docker run --rm twitter-crawler crawler --mode scrape --query "openclaw" --target 10
```

Kubernetes manifests live in [source/deployment/](source/deployment/).
Secrets are injected via `secretKeyRef` — never baked into images.

---

## Extending

New crawler types and output destinations are added without modifying the
engine — see the step-by-step guide in
[CLAUDE.md](CLAUDE.md#crawler-extension-guide).

---

## License

[MIT](LICENSE)
