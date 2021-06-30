from unittest.mock import Mock

import nameko.standalone.events
import pytest
from nameko.events import EventDispatcher, event_handler
from nameko.testing.services import entrypoint_waiter
from nameko.testing.utils import get_extension
from opentelemetry.trace import SpanKind


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
            return lambda event_type, payload: dispatch("service", event_type, payload)

        if request.param == "dependency_provider":
            dp = get_extension(container, EventDispatcher)
            return dp.get_dependency(Mock(context_data={}))

    def test_incoming_context(self, container, dispatch, memory_exporter):

        payload = "payload"
        with entrypoint_waiter(container, "handle") as result:
            dispatch("example", payload)
        assert result.get() == payload

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        client_span = list(filter(lambda span: span.kind == SpanKind.CLIENT, spans))[0]
        server_span = list(filter(lambda span: span.kind == SpanKind.SERVER, spans))[0]

        assert client_span.parent is None
        assert server_span.parent.span_id == client_span.get_span_context().span_id
