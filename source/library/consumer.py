"""
Kafka -> sink consumer.

Reads scraped-tweet messages off a Kafka topic and forwards each one to an
output sink (any :class:`~helpers.output.driver.OutputDriver` — typically the
Elasticsearch driver). Decoupling the sink keeps the consumer open for
extension: the same loop can fan tweets out to ES, a file, or stdout without
modification.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

from aiokafka import AIOKafkaConsumer

from helpers.output.driver import OutputDriver
from library.config import KafkaSettings

logger = logging.getLogger(__name__)


class KafkaSinkConsumer:
    """Consume tweet messages from Kafka and write them to an output sink.

    Args:
        kafka_settings: Broker/connection configuration.
        topic: Source topic to subscribe to.
        sink: Output driver that receives each message's raw JSON value.
        group_id: Kafka consumer group ID (for offset tracking).
        max_messages: Stop after consuming this many messages (``None`` =
            run until idle-stopped or cancelled). Useful for batch/one-shot runs.
        idle_timeout: Stop after this many seconds without new messages
            (``0`` = never stop on idle; run forever). Useful so a one-shot
            drain exits once the topic is caught up.
        batch_timeout_ms: Max time to wait for a poll batch.
        max_records: Max records to pull per poll.
    """

    def __init__(
        self,
        kafka_settings: KafkaSettings,
        topic: str,
        sink: OutputDriver,
        *,
        group_id: str = "twitter-es-indexer",
        max_messages: Optional[int] = None,
        idle_timeout: float = 0.0,
        batch_timeout_ms: int = 1000,
        max_records: int = 500,
    ) -> None:
        self._settings = kafka_settings
        self._topic = topic
        self._sink = sink
        self._group_id = group_id
        self._max_messages = max_messages
        self._idle_timeout = idle_timeout
        self._batch_timeout_ms = batch_timeout_ms
        self._max_records = max_records
        self._consumer: Optional[AIOKafkaConsumer] = None

    async def run(self) -> int:
        """Consume until the stop condition is met. Returns messages processed."""
        self._consumer = AIOKafkaConsumer(
            self._topic,
            bootstrap_servers=self._settings.bootstrap_servers,
            client_id=self._settings.client_id,
            group_id=self._group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        await self._consumer.start()
        logger.info(
            "KafkaSinkConsumer started (topic=%s, group=%s, sink=%s)",
            self._topic, self._group_id, getattr(self._sink, "name", "?"),
        )

        processed = 0
        last_message_at = time.monotonic()
        try:
            while True:
                batch = await self._consumer.getmany(
                    timeout_ms=self._batch_timeout_ms,
                    max_records=self._max_records,
                )

                if not batch:
                    if self._idle_timeout and (
                        time.monotonic() - last_message_at >= self._idle_timeout
                    ):
                        logger.info(
                            "No messages for %.1fs — stopping (processed=%d)",
                            self._idle_timeout, processed,
                        )
                        break
                    continue

                for _tp, messages in batch.items():
                    for message in messages:
                        value = message.value
                        if isinstance(value, bytes):
                            value = value.decode("utf-8")
                        await self._sink.put(value)
                        processed += 1

                last_message_at = time.monotonic()
                logger.info("Indexed %d message(s) so far", processed)

                if self._max_messages is not None and processed >= self._max_messages:
                    logger.info("Reached max_messages=%d — stopping", self._max_messages)
                    break
        finally:
            await self._consumer.stop()
            await self._sink.close()
            logger.info("KafkaSinkConsumer stopped (processed=%d)", processed)

        return processed

    async def stop(self) -> None:
        """Stop the underlying consumer early (e.g. on signal)."""
        if self._consumer is not None:
            await self._consumer.stop()


async def run_kafka_to_elasticsearch(
    *,
    topic: Optional[str] = None,
    index: Optional[str] = None,
    bootstrap_servers: Optional[str] = None,
    elasticsearch_hosts: Optional[str] = None,
    group_id: str = "twitter-es-indexer",
    max_messages: Optional[int] = None,
    idle_timeout: float = 0.0,
) -> int:
    """Wire a Kafka source to the Elasticsearch sink and run until stopped.

    Topic, broker list, and index default to the configured values. Returns
    the number of messages indexed.
    """
    from library.config import settings
    from helpers.output.driver.factory import OutputDriverFactory

    kafka_settings = settings.kafka
    if bootstrap_servers:
        kafka_settings = kafka_settings.model_copy(
            update={"bootstrap_servers": bootstrap_servers}
        )

    sink = OutputDriverFactory.create_output_driver(
        destination="elasticsearch",
        output=index or settings.elasticsearch.index_name,
        elasticsearch_hosts=elasticsearch_hosts,
    )
    consumer = KafkaSinkConsumer(
        kafka_settings,
        topic=topic or settings.kafka.topic,
        sink=sink,
        group_id=group_id,
        max_messages=max_messages,
        idle_timeout=idle_timeout,
    )
    return await consumer.run()
