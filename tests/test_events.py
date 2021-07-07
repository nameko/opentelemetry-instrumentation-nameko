# -*- coding: utf-8 -*-
from unittest.mock import Mock

import nameko.standalone.events
import pytest
from nameko.events import EventDispatcher, event_handler
from nameko.testing.services import entrypoint_waiter
from nameko.testing.utils import get_extension
from opentelemetry.trace import SpanKind

from nameko_opentelemetry import active_tracer
from nameko_opentelemetry.scrubbers import SCRUBBED


class TestCaptureIncomingContext:
    @pytest.fixture
    def container(self, container_factory, rabbit_config):
        class Service:
            name = "service"

            dispatch = EventDispatcher()

            @event_handler("service", "example")
            def handle(self, payload):
                return payload

        container = container_factory(Service)
        container.start()

        return container

    @pytest.fixture(params=["standalone", "dependency_provider"])
    def dispatch(self, rabbit_config, request, container):
        if request.param == "standalone":
            dispatch = nameko.standalone.events.event_dispatcher()
            yield lambda event_type, payload: dispatch("service", event_type, payload)

        if request.param == "dependency_provider":
            dp = get_extension(container, EventDispatcher)
            yield dp.get_dependency(Mock(context_data={}))

    def test_incoming_context(self, container, dispatch, memory_exporter):

        payload = "payload"
        with entrypoint_waiter(container, "handle") as result:
            dispatch("example", payload)
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

            @event_handler("service", "example", requeue_on_error=True)
            def handle(self, payload):
                return payload

        container = container_factory(Service)
        container.start()

        return container

    @pytest.fixture
    def dispatch(self, rabbit_config):
        dispatch = nameko.standalone.events.event_dispatcher()
        yield lambda event_type, payload: dispatch("service", event_type, payload)

    def test_event_attributes(self, container, dispatch, memory_exporter):

        payload = "payload"
        with entrypoint_waiter(container, "handle") as result:
            dispatch("example", payload)
        assert result.get() == payload

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        server_span = list(filter(lambda span: span.kind == SpanKind.CONSUMER, spans))[
            0
        ]

        attributes = server_span.attributes
        assert attributes["nameko.events.handler_type"] == "service_pool"
        assert attributes["nameko.events.reliable_delivery"] == "True"
        assert attributes["nameko.events.requeue_on_error"] == "True"

    def test_consumer_attributes(self, container, dispatch, memory_exporter):

        payload = "payload"
        with entrypoint_waiter(container, "handle") as result:
            dispatch("example", payload)
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

            dispatch = EventDispatcher(expiration=10)

            @event_handler("service", "example")
            def handle(self, payload):
                return payload

        container = container_factory(Service)
        container.start()

        return container

    @pytest.fixture(params=["standalone", "dependency_provider"])
    def dispatch(self, rabbit_config, request, container):
        if request.param == "standalone":
            dispatch = nameko.standalone.events.event_dispatcher(expiration=10)
            yield lambda event_type, payload: dispatch("service", event_type, payload)

        if request.param == "dependency_provider":
            dp = get_extension(container, EventDispatcher)
            yield dp.get_dependency(Mock(context_data={}))

    def test_event_attributes(
        self, container, dispatch, memory_exporter, send_request_payloads
    ):

        payload = "payload"
        with entrypoint_waiter(container, "handle") as result:
            dispatch("example", payload)
        assert result.get() == payload

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        client_span = list(filter(lambda span: span.kind == SpanKind.PRODUCER, spans))[
            0
        ]

        attributes = client_span.attributes
        assert attributes["nameko.events.exchange"] == "service.events"
        assert attributes["nameko.events.event_type"] == "example"

        if send_request_payloads:
            assert attributes["nameko.events.event_data"] == "payload"
            assert attributes["nameko.events.event_data_truncated"] == "False"
        else:
            assert "nameko.events.event_data" not in attributes
            assert "nameko.events.event_data_truncated" not in attributes

    def test_publisher_attributes(self, container, dispatch, memory_exporter):

        payload = "payload"
        with entrypoint_waiter(container, "handle") as result:
            dispatch("example", payload)
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

            @event_handler("service", "example")
            def handle(self, payload):
                with active_tracer().start_as_current_span(
                    "foobar", attributes={"foo": "bar"}
                ):
                    return payload

        container = container_factory(Service)
        container.start()

        return container

    @pytest.fixture
    def dispatch(self, rabbit_config):
        dispatch = nameko.standalone.events.event_dispatcher()
        yield lambda event_type, payload: dispatch("service", event_type, payload)

    def test_internal_span(self, container, dispatch, memory_exporter):

        payload = "payload"
        with entrypoint_waiter(container, "handle") as result:
            dispatch("example", payload)
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

            dispatch = EventDispatcher(expiration=10, headers={"password": "secret"})

            @event_handler("service", "example")
            def handle(self, payload):
                return payload

        container = container_factory(Service)
        container.start()

        return container

    @pytest.fixture(params=["standalone", "dependency_provider"])
    def dispatch(self, rabbit_config, request, container):
        if request.param == "standalone":
            dispatch = nameko.standalone.events.event_dispatcher(
                expiration=10, headers={"password": "secret"}
            )
            yield lambda event_type, payload: dispatch("service", event_type, payload)

        if request.param == "dependency_provider":
            dp = get_extension(container, EventDispatcher)
            yield dp.get_dependency(Mock(context_data={}))

    def test_payload_scrubber(self, container, dispatch, memory_exporter):

        payload = {"auth": "token"}
        with entrypoint_waiter(container, "handle") as result:
            dispatch("example", payload)
        assert result.get() == payload

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        client_span = list(filter(lambda span: span.kind == SpanKind.PRODUCER, spans))[
            0
        ]

        attributes = client_span.attributes
        assert attributes["nameko.events.event_data"] == f"{{'auth': '{SCRUBBED}'}}"

    def test_call_args_scrubber(self, container, dispatch, memory_exporter):

        payload = {"auth": "token"}
        with entrypoint_waiter(container, "handle") as result:
            dispatch("example", payload)
        assert result.get() == payload

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        server_span = list(filter(lambda span: span.kind == SpanKind.CONSUMER, spans))[
            0
        ]

        attributes = server_span.attributes
        assert attributes["call_args"] == f"{{'payload': {{'auth': '{SCRUBBED}'}}}}"

    def test_header_scrubber(self, container, dispatch, memory_exporter):

        payload = {"auth": "token"}
        with entrypoint_waiter(container, "handle") as result:
            dispatch("example", payload)
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
            client_span.attributes["nameko.amqp.headers"]
            == f"{{'password': '{SCRUBBED}'}}"
        )
        # context data scrubbed at server
        assert f"'password': '{SCRUBBED}'" in server_span.attributes["context_data"]
