from unittest.mock import Mock

import pytest
from nameko.rpc import ServiceRpc, rpc
from nameko.standalone.rpc import ServiceRpcClient
from nameko.testing.services import entrypoint_waiter
from nameko.testing.utils import get_extension
from opentelemetry.trace import SpanKind


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

    pass


# 1. add this for rpc, messaging, events
# 2. include addtl patches for not found in rpc case
# 3. commit, along with existing impls (without attribute customization)
# 4. think about then implement attribute customization for rpc, events, messaging, timer
