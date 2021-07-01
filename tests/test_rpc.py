from unittest.mock import Mock

import pytest
from nameko.exceptions import IncorrectSignature, MethodNotFound
from nameko.rpc import ServiceRpc, rpc
from nameko.standalone.rpc import ServiceRpcClient
from nameko.testing.services import entrypoint_waiter
from nameko.testing.utils import get_extension
from opentelemetry.trace import SpanKind
from opentelemetry.trace.status import StatusCode


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
            yield dp.get_dependency(Mock(context_data={}))

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
                return "OK"

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


# 4. think about then implement attribute customization for rpc, events, messaging, timer
