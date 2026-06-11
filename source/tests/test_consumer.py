"""Tests for library/consumer.py — KafkaSinkConsumer (Kafka -> sink)."""

from __future__ import annotations

import pytest
from pytest_mock import MockerFixture

from helpers.output.driver import OutputDriver
from library.config import KafkaSettings
from library.consumer import KafkaSinkConsumer


class RecordingSink(OutputDriver):
    """In-memory sink that records every value it receives."""

    name = "recording"

    def __init__(self):
        super().__init__()
        self.values: list[str] = []
        self.closed = False

    async def put(self, output: str, **kwargs):
        self.values.append(output)

    async def close(self):
        self.closed = True


class _FakeMessage:
    def __init__(self, value: bytes):
        self.value = value


def _patch_consumer(mocker: MockerFixture, batches: list[dict]):
    """Patch AIOKafkaConsumer so getmany() returns the given batches in order.

    Each batch is a ``{TopicPartition: [messages]}`` dict; an empty ``{}``
    simulates an idle poll. After the supplied batches are exhausted,
    getmany() returns ``{}`` (idle) indefinitely so the idle-timeout path can
    drive termination without raising StopIteration.
    """
    queue = list(batches)

    async def _getmany(*_args, **_kwargs):
        return queue.pop(0) if queue else {}

    fake = mocker.AsyncMock()
    fake.start = mocker.AsyncMock()
    fake.stop = mocker.AsyncMock()
    fake.getmany = mocker.AsyncMock(side_effect=_getmany)
    mocker.patch("library.consumer.AIOKafkaConsumer", return_value=fake)
    return fake


@pytest.fixture
def kafka_settings() -> KafkaSettings:
    return KafkaSettings(bootstrap_servers="test:9092", topic="twitter.tweets.raw")


class TestKafkaSinkConsumer:
    @pytest.mark.asyncio
    async def test_forwards_messages_to_sink(
        self, mocker: MockerFixture, kafka_settings: KafkaSettings
    ) -> None:
        msgs = [_FakeMessage(b'{"tweet_id":"1"}'), _FakeMessage(b'{"tweet_id":"2"}')]
        _patch_consumer(mocker, [{"tp": msgs}])  # one batch, then idle forever
        sink = RecordingSink()

        consumer = KafkaSinkConsumer(
            kafka_settings, topic="twitter.tweets.raw", sink=sink, idle_timeout=0.01
        )
        processed = await consumer.run()

        assert processed == 2
        assert sink.values == ['{"tweet_id":"1"}', '{"tweet_id":"2"}']
        assert sink.closed is True  # sink is closed on shutdown

    @pytest.mark.asyncio
    async def test_stops_at_max_messages(
        self, mocker: MockerFixture, kafka_settings: KafkaSettings
    ) -> None:
        msgs = [_FakeMessage(b'{"tweet_id":"%d"}' % i) for i in range(5)]
        _patch_consumer(mocker, [{"tp": msgs}])
        sink = RecordingSink()

        consumer = KafkaSinkConsumer(
            kafka_settings, topic="t", sink=sink, max_messages=3
        )
        processed = await consumer.run()

        # The whole batch is drained, then the max_messages gate stops the loop
        assert processed == 5
        assert len(sink.values) == 5

    @pytest.mark.asyncio
    async def test_idle_timeout_stops_when_no_messages(
        self, mocker: MockerFixture, kafka_settings: KafkaSettings
    ) -> None:
        _patch_consumer(mocker, [])  # always idle
        sink = RecordingSink()

        consumer = KafkaSinkConsumer(
            kafka_settings, topic="t", sink=sink, idle_timeout=0.01
        )
        processed = await consumer.run()

        assert processed == 0
        assert sink.closed is True

    @pytest.mark.asyncio
    async def test_decodes_bytes_values(
        self, mocker: MockerFixture, kafka_settings: KafkaSettings
    ) -> None:
        _patch_consumer(mocker, [{"tp": [_FakeMessage(b'{"tweet_id":"x"}')]}])
        sink = RecordingSink()

        consumer = KafkaSinkConsumer(
            kafka_settings, topic="t", sink=sink, idle_timeout=0.01
        )
        await consumer.run()

        assert sink.values[0] == '{"tweet_id":"x"}'
        assert isinstance(sink.values[0], str)
