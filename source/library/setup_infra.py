#!/usr/bin/env python3
"""
Infrastructure setup — create Kafka topics & Elasticsearch indices.

Usage:
    python library/setup_infra.py                  # create default topic + index
    python library/setup_infra.py --dry-run        # show what would be created
    python library/setup_infra.py --delete         # delete and recreate
    python library/setup_infra.py --health         # connectivity check only

Reads configuration from ``config.yaml`` (or TWITTER_* env vars) via
``library.config.settings``. Fully async — aiokafka admin + httpx.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

import httpx
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import KafkaError, TopicAlreadyExistsError

from library.config import settings

logger = logging.getLogger("setup_infra")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)


# ============================================================================
# Kafka
# ============================================================================

async def create_kafka_topic(
    topic: str | None = None,
    num_partitions: int = 3,
    replication_factor: int = 1,
    dry_run: bool = False,
    delete_first: bool = False,
) -> bool:
    """Create a Kafka topic.

    Args:
        topic: Topic name (defaults to ``settings.kafka.topic``).
        num_partitions: Number of partitions.
        replication_factor: Replication factor.
        dry_run: If True, only print what would be done.
        delete_first: If True, delete the topic before recreating.

    Returns:
        True on success.
    """
    topic = topic or settings.kafka.topic
    broker = settings.kafka.bootstrap_servers

    logger.info("Connecting to Kafka broker: %s", broker)

    admin = AIOKafkaAdminClient(bootstrap_servers=broker, client_id="setup-infra")
    try:
        await admin.start()
    except KafkaError as e:
        logger.error("Failed to connect to Kafka: %s", e)
        return False

    try:
        existing = await admin.list_topics()
        logger.info("Existing topics: %s", existing)

        if topic in existing:
            if delete_first:
                logger.info("Deleting existing topic: %s", topic)
                if not dry_run:
                    await admin.delete_topics([topic])
                    await asyncio.sleep(1)
            else:
                logger.info("Topic '%s' already exists — skipping", topic)
                return True

        if dry_run:
            logger.info(
                "[DRY-RUN] Would create topic: %s (partitions=%d, rf=%d)",
                topic, num_partitions, replication_factor,
            )
            return True

        try:
            await admin.create_topics([
                NewTopic(
                    name=topic,
                    num_partitions=num_partitions,
                    replication_factor=replication_factor,
                )
            ])
            logger.info("[OK] Kafka topic created: %s (partitions=%d)", topic, num_partitions)
        except TopicAlreadyExistsError:
            logger.info("Topic '%s' already exists", topic)
        except KafkaError as e:
            logger.error("Failed to create topic: %s", e)
            return False
        return True
    finally:
        await admin.close()


# ============================================================================
# Elasticsearch
# ============================================================================

ES_INDEX_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
    },
    "mappings": {
        "properties": {
            "tweet_id": {"type": "keyword"},
            "tweet_url": {"type": "keyword"},
            "search_url": {"type": "keyword"},
            "mirror": {"type": "keyword"},
            "username": {"type": "keyword"},
            "display_name": {"type": "text"},
            "content": {"type": "text", "analyzer": "standard"},
            "tweet_date": {"type": "date", "format": "strict_date"},
            "tweet_date_title": {"type": "keyword"},
            "relative_time": {"type": "keyword"},
            "raw_html": {"type": "text", "index": False},
        }
    },
}


def _es_url(path: str = "") -> str:
    """Build a full Elasticsearch REST URL."""
    host = settings.elasticsearch.hosts[0].rstrip("/")
    return f"{host}/{path}"


async def _es_request(
    client: httpx.AsyncClient,
    method: str,
    path: str,
    json_body: dict | None = None,
) -> httpx.Response:
    """Make a request to the Elasticsearch REST API."""
    return await client.request(method, _es_url(path), json=json_body)


async def create_elasticsearch_index(
    index: str | None = None,
    dry_run: bool = False,
    delete_first: bool = False,
) -> bool:
    """Create an Elasticsearch index with optimised mappings.

    Args:
        index: Index name (defaults to ``settings.elasticsearch.index_name``).
        dry_run: If True, only print what would be done.
        delete_first: If True, delete the index before recreating.

    Returns:
        True on success.
    """
    index = index or settings.elasticsearch.index_name

    logger.info("Connecting to Elasticsearch: %s", settings.elasticsearch.hosts)

    async with httpx.AsyncClient(
        timeout=settings.elasticsearch.request_timeout,
        headers={"Content-Type": "application/json"},
    ) as client:
        try:
            resp = await _es_request(client, "GET", "")
            info = resp.json()
            logger.info(
                "ES cluster: %s (version %s)",
                info["cluster_name"], info["version"]["number"],
            )
        except (httpx.HTTPError, KeyError, ValueError) as e:
            logger.error("Failed to connect to Elasticsearch: %s", e)
            return False

        exists = await _es_request(client, "HEAD", index)
        if exists.status_code == 200:
            if delete_first:
                logger.info("Deleting existing index: %s", index)
                if not dry_run:
                    await _es_request(client, "DELETE", index)
            else:
                logger.info("Index '%s' already exists -- skipping", index)
                return True

        if dry_run:
            logger.info("[DRY-RUN] Would create index: %s", index)
            return True

        try:
            resp = await _es_request(client, "PUT", index, json_body=ES_INDEX_MAPPING)
            if resp.status_code in (200, 201):
                logger.info("[OK] Elasticsearch index created: %s", index)
                return True
            logger.error("Failed to create index: %s", resp.text)
            return False
        except httpx.HTTPError as e:
            logger.error("Failed to create index: %s", e)
            return False


# ============================================================================
# Health check
# ============================================================================

async def health_check() -> dict[str, str]:
    """Quick connectivity check for Kafka + Elasticsearch."""
    status: dict[str, str] = {"kafka": "...", "elasticsearch": "..."}

    # Kafka
    admin = AIOKafkaAdminClient(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        client_id="health-check",
        request_timeout_ms=5000,
    )
    try:
        await admin.start()
        topics = await admin.list_topics()
        status["kafka"] = f"[OK] connected ({len(topics)} topics)"
    except Exception as e:
        status["kafka"] = f"[FAIL] {e}"
    finally:
        try:
            await admin.close()
        except Exception:
            pass

    # Elasticsearch
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await _es_request(client, "GET", "")
            info = resp.json()
            status["elasticsearch"] = f"[OK] connected (v{info['version']['number']})"
    except Exception as e:
        status["elasticsearch"] = f"[FAIL] {e}"

    return status


# ============================================================================
# CLI
# ============================================================================

async def _main(args: argparse.Namespace) -> int:
    """Run the requested infrastructure actions."""
    if args.health:
        report = await health_check()
        for svc, stat in report.items():
            print(f"  {svc}: {stat}")
        return 0

    print("=" * 60)
    print("Twitter Crawler — Infrastructure Setup")
    print("=" * 60)

    report = await health_check()
    for svc, stat in report.items():
        print(f"  {svc}: {stat}")

    if "[FAIL]" in report["kafka"] or "[FAIL]" in report["elasticsearch"]:
        logger.error("One or more services are unreachable. Aborting.")
        return 1

    print()
    ok_kafka = await create_kafka_topic(
        topic=args.topic, dry_run=args.dry_run, delete_first=args.delete,
    )
    ok_es = await create_elasticsearch_index(
        index=args.index, dry_run=args.dry_run, delete_first=args.delete,
    )

    print()
    if ok_kafka and ok_es:
        print("[OK] All infrastructure ready.")
        return 0
    print("[WARN] Some steps failed — check logs above.")
    return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create Kafka topics & Elasticsearch indices")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    parser.add_argument("--delete", action="store_true", help="Delete and recreate")
    parser.add_argument("--topic", type=str, default=None, help="Kafka topic name")
    parser.add_argument("--index", type=str, default=None, help="ES index name")
    parser.add_argument("--health", action="store_true", help="Only health check")
    sys.exit(asyncio.run(_main(parser.parse_args())))
