from unittest.mock import Mock

import nameko
import nameko.amqp.publish
import pytest
from kombu.messaging import Exchange, Queue
from nameko.messaging import Publisher, consume
from nameko.testing.services import entrypoint_waiter
from nameko.testing.utils import get_extension
from opentelemetry import trace
from opentelemetry.propagate import inject
from opentelemetry.trace import SpanKind

from nameko_opentelemetry.version import __version__


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
    def standalone_publisher(self):
        """ Nameko doesn't include a standalone publisher equivalent to the one in
        `nameko.standalone.events`, so we create one here, ensuring that a span is
        active and the headers are included in published messages.
        """
        headers = {}
        publisher = nameko.amqp.publish.Publisher(
            nameko.config["AMQP_URI"], exchange=exchange, headers=headers
        )

        tracer_provider = trace.get_tracer_provider()
        tracer = trace.get_tracer("nameko", __version__, tracer_provider)

        def publish(*args, **kwargs):
            exchange = getattr(publisher, "exchange", kwargs.get("exchange"))

            with tracer.start_as_current_span(
                f"Publish to {exchange and exchange.name or 'default-exchange'}",
                kind=SpanKind.CLIENT,
            ):
                inject(headers)
                return publisher.publish(*args, **kwargs)

        return publish

    @pytest.fixture(params=["standalone", "dependency_provider"])
    def publish(self, rabbit_config, request, container, standalone_publisher):
        if request.param == "standalone":
            return standalone_publisher

        if request.param == "dependency_provider":
            dp = get_extension(container, Publisher)
            return dp.get_dependency(Mock(context_data={}))

    def test_incoming_context(self, container, publish, memory_exporter):

        payload = "payload"
        with entrypoint_waiter(container, "handle") as result:
            publish(payload)
        assert result.get() == payload

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        client_span = list(filter(lambda span: span.kind == SpanKind.CLIENT, spans))[0]
        server_span = list(filter(lambda span: span.kind == SpanKind.SERVER, spans))[0]

        assert client_span.parent is None
        assert server_span.parent.span_id == client_span.get_span_context().span_id
