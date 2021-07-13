# -*- coding: utf-8 -*-
from unittest.mock import Mock

import pytest
from kombu.messaging import Exchange, Queue
from nameko.messaging import Publisher, consume
from nameko.testing.services import entrypoint_waiter
from nameko.testing.utils import get_extension
from opentelemetry.trace import SpanKind

from nameko_opentelemetry import active_tracer
from nameko_opentelemetry.scrubbers import SCRUBBED


exchange = Exchange(name="test")
queue = Queue(name="test", exchange=exchange)


class TestCaptureIncomingContext:
    @pytest.fixture
    def container(self, container_factory, rabbit_config):
        class Service:
            name = "service"

            publish = Publisher(exchange)

            @consume(queue)
            def handle(self, payload):
                return payload

        container = container_factory(Service)
        container.start()

        return container

    @pytest.fixture
    def publish(self, rabbit_config, container):
        dp = get_extension(container, Publisher)
        return dp.get_dependency(Mock(context_data={}))

    def test_incoming_context(self, container, publish, memory_exporter):

        payload = "payload"
        with entrypoint_waiter(container, "handle") as result:
            publish(payload)
        assert result.get() == payload

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        client_span = list(filter(lambda span: span.kind == SpanKind.PRODUCER, spans))[
            0
        ]
        server_span = list(filter(lambda span: span.kind == SpanKind.CONSUMER, spans))[
            0
        ]

        assert client_span.parent is None
        assert server_span.parent.span_id == client_span.get_span_context().span_id


class TestServerAttributes:
    @pytest.fixture
    def container(self, container_factory, rabbit_config):
        class Service:
            name = "service"

            publish = Publisher(exchange)

            @consume(queue, requeue_on_error=True)
            def handle(self, payload):
                return payload

        container = container_factory(Service)
        container.start()

        return container

    @pytest.fixture
    def publish(self, rabbit_config, container):
        dp = get_extension(container, Publisher)
        return dp.get_dependency(Mock(context_data={}))

    def test_messaging_attributes(self, container, publish, memory_exporter):

        payload = "payload"
        with entrypoint_waiter(container, "handle") as result:
            publish(payload)
        assert result.get() == payload

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        server_span = list(filter(lambda span: span.kind == SpanKind.CONSUMER, spans))[
            0
        ]

        attributes = server_span.attributes
        assert attributes["nameko.messaging.requeue_on_error"] == "True"

    def test_consumer_attributes(self, container, publish, memory_exporter):

        payload = "payload"
        with entrypoint_waiter(container, "handle") as result:
            publish(payload)
        assert result.get() == payload

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        server_span = list(filter(lambda span: span.kind == SpanKind.CONSUMER, spans))[
            0
        ]

        attributes = server_span.attributes
        assert attributes["nameko.amqp.prefetch_count"] == "10"
        assert attributes["nameko.amqp.heartbeat"] == "60"
        # no need to test all


class TestClientAttributes:
    @pytest.fixture(
        params=[True, False], ids=["send_request_payloads", "no_send_request_payloads"]
    )
    def send_request_payloads(self, request):
        return request.param

    @pytest.fixture
    def config(self, config, send_request_payloads):
        # disable request payloads based on param
        config["send_request_payloads"] = send_request_payloads
        return config

    @pytest.fixture
    def container(self, container_factory, rabbit_config):
        class Service:
            name = "service"

            publish = Publisher(exchange)

            @consume(queue)
            def handle(self, payload):
                return payload

        container = container_factory(Service)
        container.start()

        return container

    @pytest.fixture
    def publish(self, rabbit_config, container):
        dp = get_extension(container, Publisher)
        return dp.get_dependency(Mock(context_data={}))

    def test_event_attributes(
        self, container, publish, memory_exporter, send_request_payloads
    ):

        payload = "payload"
        with entrypoint_waiter(container, "handle") as result:
            publish(payload)
        assert result.get() == payload

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        client_span = list(filter(lambda span: span.kind == SpanKind.PRODUCER, spans))[
            0
        ]

        attributes = client_span.attributes
        assert attributes["nameko.messaging.exchange"] == exchange.name

        if send_request_payloads:
            assert attributes["nameko.messaging.payload"] == "payload"
            assert attributes["nameko.messaging.payload_truncated"] == "False"
        else:
            assert "nameko.messaging.payload" not in attributes
            assert "nameko.messaging.payload_truncated" not in attributes

    def test_publisher_attributes(self, container, publish, memory_exporter):

        payload = "payload"
        with entrypoint_waiter(container, "handle") as result:
            publish(payload, expiration=10)
        assert result.get() == payload

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        client_span = list(filter(lambda span: span.kind == SpanKind.PRODUCER, spans))[
            0
        ]

        attributes = client_span.attributes
        assert attributes["nameko.amqp.mandatory"] == "False"
        assert attributes["nameko.amqp.expiration"] == "10"
        # no need to test all


class TestAdditionalSpans:
    @pytest.fixture
    def container(self, container_factory, rabbit_config):
        class Service:
            name = "service"

            publish = Publisher(exchange)

            @consume(queue)
            def handle(self, payload):
                with active_tracer().start_as_current_span(
                    "foobar", attributes={"foo": "bar"}
                ):
                    return payload

        container = container_factory(Service)
        container.start()

        return container

    @pytest.fixture
    def publish(self, rabbit_config, container):
        dp = get_extension(container, Publisher)
        return dp.get_dependency(Mock(context_data={}))

    def test_internal_span(self, container, publish, memory_exporter):

        payload = "payload"
        with entrypoint_waiter(container, "handle") as result:
            publish(payload)
        assert result.get() == payload

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 3

        internal_span = list(
            filter(lambda span: span.kind == SpanKind.INTERNAL, spans)
        )[0]

        assert internal_span.name == "foobar"


class TestScrubbing:
    @pytest.fixture
    def container(self, container_factory, rabbit_config):
        class Service:
            name = "service"

            publish = Publisher(exchange)

            @consume(queue)
            def handle(self, payload):
                return payload

        container = container_factory(Service)
        container.start()

        return container

    @pytest.fixture
    def publish(self, rabbit_config, container):
        dp = get_extension(container, Publisher)
        return dp.get_dependency(Mock(context_data={}))

    def test_payload_scrubber(self, container, publish, memory_exporter):

        payload = {"auth": "token"}
        with entrypoint_waiter(container, "handle") as result:
            publish(payload)
        assert result.get() == payload

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        client_span = list(filter(lambda span: span.kind == SpanKind.PRODUCER, spans))[
            0
        ]

        attributes = client_span.attributes
        assert attributes["nameko.messaging.payload"] == f"{{'auth': '{SCRUBBED}'}}"

    def test_call_args_scrubber(self, container, publish, memory_exporter):

        payload = {"auth": "token"}
        with entrypoint_waiter(container, "handle") as result:
            publish(payload)
        assert result.get() == payload

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        server_span = list(filter(lambda span: span.kind == SpanKind.CONSUMER, spans))[
            0
        ]

        attributes = server_span.attributes
        assert attributes["call_args"] == f"{{'payload': {{'auth': '{SCRUBBED}'}}}}"

    def test_header_scrubber(self, container, publish, memory_exporter):

        payload = {"auth": "token"}
        with entrypoint_waiter(container, "handle") as result:
            publish(payload, headers={"password": "secret"})
        assert result.get() == payload

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        client_span = list(filter(lambda span: span.kind == SpanKind.PRODUCER, spans))[
            0
        ]
        server_span = list(filter(lambda span: span.kind == SpanKind.CONSUMER, spans))[
            0
        ]

        # headers scrubbed at client
        assert (
            f"'password': '{SCRUBBED}'" in client_span.attributes["nameko.amqp.headers"]
        )

        # context data scrubbed at server
        assert f"'password': '{SCRUBBED}'" in server_span.attributes["context_data"]
