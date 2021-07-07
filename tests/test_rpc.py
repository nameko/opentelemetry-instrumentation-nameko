# -*- coding: utf-8 -*-
import uuid
from unittest.mock import Mock

import pytest
from nameko.exceptions import IncorrectSignature, MethodNotFound
from nameko.rpc import ServiceRpc, rpc
from nameko.standalone.rpc import ServiceRpcClient
from nameko.testing.services import entrypoint_waiter
from nameko.testing.utils import get_extension
from opentelemetry.trace import SpanKind
from opentelemetry.trace.status import StatusCode

from nameko_opentelemetry import active_tracer
from nameko_opentelemetry.scrubbers import SCRUBBED


class TestCaptureIncomingContext:
    @pytest.fixture
    def container(self, container_factory, rabbit_config):
        class Service:
            name = "service"

            self_rpc = ServiceRpc("service")

            @rpc
            def method(self, arg, kwarg=None):
                return "OK"

        container = container_factory(Service)
        container.start()

        return container

    @pytest.fixture(params=["standalone", "dependency_provider"])
    def client(self, rabbit_config, request, container):
        if request.param == "standalone":
            with ServiceRpcClient("service") as client:
                yield client
        if request.param == "dependency_provider":
            dp = get_extension(container, ServiceRpc)
            yield dp.get_dependency(
                Mock(context_data={"call_id": f"service.method.{uuid.uuid4()}"})
            )

    def test_incoming_context(self, container, client, memory_exporter):

        with entrypoint_waiter(container, "method"):
            assert client.method("arg", kwarg="kwarg") == "OK"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        client_span = list(filter(lambda span: span.kind == SpanKind.CLIENT, spans))[0]
        server_span = list(filter(lambda span: span.kind == SpanKind.SERVER, spans))[0]

        assert client_span.parent is None
        assert server_span.parent.span_id == client_span.get_span_context().span_id


class TestNoEntrypointFired:
    """ Test cases where the request is aborted before finding an entrypoint
    """

    @pytest.fixture
    def container(self, container_factory, rabbit_config):
        class Service:
            name = "service"

            @rpc
            def method(self, arg, kwarg=None):
                return "OK"  # pragma: no cover

        container = container_factory(Service)
        container.start()

        return container

    @pytest.fixture
    def client(self, rabbit_config):
        with ServiceRpcClient("service") as client:
            yield client

    def test_method_not_found(self, container, client, memory_exporter):

        with pytest.raises(MethodNotFound):
            client.not_a_method()

        container.stop()

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        server_span = list(filter(lambda span: span.kind == SpanKind.SERVER, spans))[0]

        assert not server_span.status.is_ok
        assert server_span.status.status_code == StatusCode.ERROR
        assert server_span.status.description == "MethodNotFound: not_a_method"

    def test_incorrect_signature(self, container, client, memory_exporter):
        with pytest.raises(IncorrectSignature):
            client.method("foo", "bar", "baz")

        container.stop()

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        server_span = list(filter(lambda span: span.kind == SpanKind.SERVER, spans))[0]

        assert not server_span.status.is_ok
        assert server_span.status.status_code == StatusCode.ERROR
        assert "IncorrectSignature" in server_span.status.description


class TestServerAttributes:
    @pytest.fixture
    def container(self, container_factory, rabbit_config):
        class Service:
            name = "service"

            @rpc
            def method(self, arg, kwarg=None):
                return "OK"

        container = container_factory(Service)
        container.start()

        return container

    @pytest.fixture
    def client(self, rabbit_config):
        with ServiceRpcClient("service") as client:
            yield client

    def test_consumer_attributes(self, container, client, memory_exporter):

        with entrypoint_waiter(container, "method"):
            assert client.method("arg", kwarg="kwarg") == "OK"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        server_span = list(filter(lambda span: span.kind == SpanKind.SERVER, spans))[0]

        attributes = server_span.attributes
        assert attributes["nameko.amqp.prefetch_count"] == "10"
        assert attributes["nameko.amqp.heartbeat"] == "60"
        # no need to test all


class TestClientAttributes:
    @pytest.fixture
    def container(self, container_factory, rabbit_config):
        class Service:
            name = "service"

            self_rpc = ServiceRpc("service")

            @rpc
            def method(self, arg, kwarg=None):
                return "OK"

        container = container_factory(Service)
        container.start()

        return container

    @pytest.fixture(params=["standalone", "dependency_provider"])
    def client(self, rabbit_config, request, container):
        if request.param == "standalone":
            with ServiceRpcClient("service") as client:
                yield client
        if request.param == "dependency_provider":
            dp = get_extension(container, ServiceRpc)
            yield dp.get_dependency(
                Mock(context_data={"call_id": f"service.method.{uuid.uuid4()}"})
            )

    def test_rpc_attributes(self, container, client, memory_exporter):

        with entrypoint_waiter(container, "method"):
            assert client.method("arg", kwarg="kwarg") == "OK"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        client_span = list(filter(lambda span: span.kind == SpanKind.CLIENT, spans))[0]

        attributes = client_span.attributes
        assert attributes["nameko.rpc.target_service"] == "service"
        assert attributes["nameko.rpc.target_method"] == "method"

    def test_publisher_attributes(self, container, client, memory_exporter):

        with entrypoint_waiter(container, "method"):
            assert client.method("arg", kwarg="kwarg") == "OK"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        client_span = list(filter(lambda span: span.kind == SpanKind.CLIENT, spans))[0]

        attributes = client_span.attributes
        assert attributes["nameko.amqp.mandatory"] == "True"
        assert attributes["nameko.amqp.retry"] == "True"
        # no need to test all


class TestAdditionalSpans:
    @pytest.fixture
    def container(self, container_factory, rabbit_config):
        class Service:
            name = "service"

            @rpc
            def method(self, arg, kwarg=None):
                with active_tracer().start_as_current_span(
                    "foobar", attributes={"foo": "bar"}
                ):
                    return "OK"

        container = container_factory(Service)
        container.start()

        return container

    @pytest.fixture
    def client(self, rabbit_config):
        with ServiceRpcClient("service") as client:
            yield client

    def test_internal_span(self, container, client, memory_exporter):

        with entrypoint_waiter(container, "method"):
            assert client.method("arg", kwarg="kwarg") == "OK"

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

            self_rpc = ServiceRpc("service")

            @rpc
            def method(self, arg, kwarg=None):
                return "OK"

        container = container_factory(Service)
        container.start()

        return container

    @pytest.fixture(params=["standalone", "dependency_provider"])
    def client(self, rabbit_config, request, container):
        if request.param == "standalone":
            with ServiceRpcClient("service", context_data={"auth": "token"}) as client:
                yield client
        if request.param == "dependency_provider":
            dp = get_extension(container, ServiceRpc)
            yield dp.get_dependency(Mock(context_data={"auth": "token"}))

    def test_header_scrubber(self, container, client, memory_exporter):

        with entrypoint_waiter(container, "method"):
            assert client.method("arg", kwarg="kwarg") == "OK"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        client_span = list(filter(lambda span: span.kind == SpanKind.CLIENT, spans))[0]

        assert (
            f"'nameko.auth': '{SCRUBBED}'"
            in client_span.attributes["nameko.amqp.headers"]
        )
