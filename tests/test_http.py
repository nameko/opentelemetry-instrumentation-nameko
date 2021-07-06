# -*- coding: utf-8 -*-
import socket

import pytest
from nameko.testing.services import entrypoint_waiter
from nameko.web.handlers import Response, http
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import SpanKind
from opentelemetry.trace.status import StatusCode


class TestCaptureIncomingContext:
    @pytest.fixture
    def container(self, container_factory, web_config):
        class Service:
            name = "service"

            @http("GET", "/resource")
            def get_resource(self, request):
                return "OK"

        container = container_factory(Service)
        container.start()

        return container

    @pytest.fixture
    def instrument_requests(self):
        instrumentor = RequestsInstrumentor()
        instrumentor.instrument()
        yield
        instrumentor.uninstrument()

    def test_incoming_context(
        self, container, web_session, memory_exporter, instrument_requests
    ):
        with entrypoint_waiter(container, "get_resource"):
            resp = web_session.get("/resource")

        assert resp.status_code == 200

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        client_span = list(filter(lambda span: span.kind == SpanKind.CLIENT, spans))[0]
        server_span = list(filter(lambda span: span.kind == SpanKind.SERVER, spans))[0]

        assert client_span.parent is None
        assert server_span.parent.span_id == client_span.get_span_context().span_id


class TestSpanName:
    @pytest.fixture
    def container(self, container_factory, web_config):
        class Service:
            name = "service"

            @http("GET", "/resource/<int:value>")
            def get_resource(self, request, value):
                return f"OK {value}"

        container = container_factory(Service)
        container.start()

        return container

    def test_match(self, container, web_session, memory_exporter):

        with entrypoint_waiter(container, "get_resource"):
            resp = web_session.get("/resource/1")

        assert resp.status_code == 200

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        assert spans[0].name == "/resource/<int:value>"

    def test_no_match(self, container, web_session, memory_exporter):

        resp = web_session.get("/missing")
        assert resp.status_code == 404

        container.stop()

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        assert spans[0].name == "HTTP GET"


class TestNoEntrypointFired:
    """ Test cases where the request is aborted before finding an entrypoint
    """

    @pytest.fixture
    def container(self, container_factory, web_config):
        class Service:
            name = "service"

            @http("GET", "/resource")
            def get_resource(self, request):
                return "OK"  # pragma: no cover

        container = container_factory(Service)
        container.start()

        return container

    def test_method_not_found(self, container, web_session, memory_exporter):

        resp = web_session.get("/missing")
        assert resp.status_code == 404

        container.stop()

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        span = spans[0]

        assert spans[0].name == "HTTP GET"

        assert not span.status.is_ok
        assert span.status.status_code == StatusCode.ERROR
        assert "404 Not Found" in span.status.description

    def test_method_not_allowed(self, container, web_session, memory_exporter):

        resp = web_session.post("/resource")
        assert resp.status_code == 405

        container.stop()

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        span = spans[0]

        assert spans[0].name == "HTTP POST"

        assert not span.status.is_ok
        assert span.status.status_code == StatusCode.ERROR
        assert "405 Method Not Allowed" in span.status.description


class TestSpanAttributes:
    @pytest.fixture
    def container(self, container_factory, web_config):
        class Service:
            name = "service"

            @http("GET", "/resource")
            def get_resource(self, request):
                return "OK"

        container = container_factory(Service)
        container.start()

        return container

    def test_common(self, container, web_session, memory_exporter):

        with entrypoint_waiter(container, "get_resource"):
            resp = web_session.get("/resource")

        assert resp.status_code == 200

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        attributes = spans[0].attributes
        assert attributes["hostname"] == socket.gethostname()
        assert attributes["service_name"] == "service"
        assert attributes["entrypoint_type"] == "HttpRequestHandler"
        assert attributes["method_name"] == "get_resource"


class TestCallArgs:
    @pytest.fixture(params=[True, False], ids=["send_headers", "no_send_headers"])
    def send_headers(self, request):
        return request.param

    @pytest.fixture(
        params=[True, False], ids=["send_request_payloads", "no_send_request_payloads"]
    )
    def send_request_payloads(self, request):
        return request.param

    @pytest.fixture
    def config(self, config, send_headers, send_request_payloads):
        # disable headers based on param
        config["send_headers"] = send_headers
        # disable request payload based on param
        config["send_request_payloads"] = send_request_payloads
        return config

    @pytest.fixture
    def container(self, container_factory, web_config):
        class Service:
            name = "service"

            @http("GET", "/resource")
            def get_resource(self, request):
                return "OK"

        container = container_factory(Service)
        container.start()

        return container

    def test_wsgi_common(self, container, web_session, memory_exporter):
        """ These are determined by the Opentelemetry WSGI middleware module
        """
        with entrypoint_waiter(container, "get_resource"):
            resp = web_session.get("/resource")

        assert resp.status_code == 200

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        attributes = spans[0].attributes
        assert attributes[SpanAttributes.HTTP_METHOD] == "GET"
        assert attributes[SpanAttributes.HTTP_SCHEME] == "http"
        # no need to test them exhaustively

    def test_request_data(
        self, container, web_session, memory_exporter, send_request_payloads
    ):
        with entrypoint_waiter(container, "get_resource"):
            resp = web_session.get("/resource", data="foobar")

        assert resp.status_code == 200

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        attributes = spans[0].attributes

        if send_request_payloads:
            assert attributes["request.data"] == "foobar"
        else:
            assert "request.data" not in attributes

    def test_request_headers(
        self, container, web_session, memory_exporter, send_headers
    ):

        with entrypoint_waiter(container, "get_resource"):
            resp = web_session.get(
                "/resource", headers={"auth": "should-be-secret"}
            )  # XXX

        assert resp.status_code == 200

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        attributes = spans[0].attributes
        if send_headers:
            assert "['auth', 'should-be-secret']" in attributes["request.headers"]
        else:
            assert "request.headers" not in attributes


class TestExceptions:
    @pytest.fixture
    def container(self, container_factory, web_config):
        class Service:
            name = "service"

            class Error(Exception):
                pass

            @http("POST", "/resource")
            def raises(self, request):
                raise self.Error("boom")

            @http("DELETE", "/resource", expected_exceptions=Error)
            def raises_expected(self, request):
                raise self.Error("boom")

        container = container_factory(Service)
        container.start()

        return container

    def test_exception(self, container, web_session, memory_exporter):

        with entrypoint_waiter(container, "raises"):
            resp = web_session.post("/resource")

        assert resp.status_code == 500

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        span = spans[0]
        assert len(span.events) == 1

        event = span.events[0]
        assert event.name == "exception"
        assert event.attributes["exception.type"] == "Error"
        assert event.attributes["exception.message"] == "boom"
        assert 'raise self.Error("boom")' in event.attributes["exception.stacktrace"]
        assert event.attributes["exception.escaped"] == "True"
        # extra attributes
        assert event.attributes["exception.expected"] == "False"

    def test_expected_exception(self, container, web_session, memory_exporter):

        with entrypoint_waiter(container, "raises_expected"):
            resp = web_session.delete("/resource")

        assert resp.status_code == 400

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        span = spans[0]
        assert len(span.events) == 1

        event = span.events[0]
        assert event.name == "exception"
        assert event.attributes["exception.type"] == "Error"
        assert event.attributes["exception.message"] == "boom"
        assert 'raise self.Error("boom")' in event.attributes["exception.stacktrace"]
        assert event.attributes["exception.escaped"] == "True"
        # extra attributes
        assert event.attributes["exception.expected"] == "True"


class TestResult:
    @pytest.fixture(
        params=[True, False],
        ids=["send_response_payloads", "no_send_response_payloads"],
    )
    def send_response_payloads(self, request):
        return request.param

    @pytest.fixture
    def config(self, config, send_response_payloads):
        # override default truncation length
        config["truncate_max_length"] = 300
        # disable response payload based on param
        config["send_response_payloads"] = send_response_payloads
        return config

    @pytest.fixture
    def container(self, container_factory, web_config):
        class Service:
            name = "service"

            @http("GET", "/simple")
            def simple_result(self, request):
                return "OK"

            @http("GET", "/tuple")
            def tuple_result(self, request):
                return (
                    401,
                    {"Content-Type": "application/json"},
                    '{"authorized": false}',
                )

            @http("GET", "/response")
            def response_result(self, request):
                return Response(
                    "Permission denied", status=403, content_type="text/plain"
                )

            @http("GET", "/big")
            def truncate_result(self, request):
                return "x" * 1000

        container = container_factory(Service)
        container.start()

        return container

    def test_simple_result(
        self, container, web_session, memory_exporter, send_response_payloads
    ):

        with entrypoint_waiter(container, "simple_result"):
            resp = web_session.get("/simple")

        assert resp.status_code == 200

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        attributes = spans[0].attributes
        assert attributes["response.content_type"] == "text/plain; charset=utf-8"
        assert attributes[SpanAttributes.HTTP_RESPONSE_CONTENT_LENGTH] == 2
        assert attributes[SpanAttributes.HTTP_STATUS_CODE] == 200

        if send_response_payloads:
            assert attributes["response.data"] == "OK"
            assert attributes["response.data_truncated"] == "False"
        else:
            assert "response.data" not in attributes
            assert "response.data_truncated" not in attributes

    def test_tuple_result(
        self, container, web_session, memory_exporter, send_response_payloads
    ):

        with entrypoint_waiter(container, "tuple_result"):
            resp = web_session.get("/tuple")

        assert resp.status_code == 401

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        attributes = spans[0].attributes
        assert attributes["response.content_type"] == "application/json"
        assert attributes[SpanAttributes.HTTP_RESPONSE_CONTENT_LENGTH] == 21
        assert attributes[SpanAttributes.HTTP_STATUS_CODE] == 401

        if send_response_payloads:
            assert attributes["response.data"] == '{"authorized": false}'
            assert attributes["response.data_truncated"] == "False"
        else:
            assert "response.data" not in attributes
            assert "response.data_truncated" not in attributes

    def test_response_result(
        self, container, web_session, memory_exporter, send_response_payloads
    ):

        with entrypoint_waiter(container, "response_result"):
            resp = web_session.get("/response")

        assert resp.status_code == 403

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        attributes = spans[0].attributes
        assert attributes["response.content_type"] == "text/plain"
        assert attributes[SpanAttributes.HTTP_RESPONSE_CONTENT_LENGTH] == 17
        assert attributes[SpanAttributes.HTTP_STATUS_CODE] == 403

        if send_response_payloads:
            assert attributes["response.data"] == "Permission denied"
            assert attributes["response.data_truncated"] == "False"
        else:
            assert "response.data" not in attributes
            assert "response.data_truncated" not in attributes

    def test_truncated_result(
        self, container, web_session, memory_exporter, send_response_payloads
    ):

        with entrypoint_waiter(container, "truncate_result"):
            resp = web_session.get("/big")

        assert resp.status_code == 200

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        attributes = spans[0].attributes
        assert attributes["response.content_type"] == "text/plain; charset=utf-8"
        assert attributes[SpanAttributes.HTTP_RESPONSE_CONTENT_LENGTH] == 1000
        assert attributes[SpanAttributes.HTTP_STATUS_CODE] == 200

        if send_response_payloads:
            assert attributes["response.data"] == "x" * 300
            assert attributes["response.data_truncated"] == "True"
        else:
            assert "response.data" not in attributes
            assert "response.data_truncated" not in attributes


class TestStatus:
    """
    Spec says status code MUST be set for 4xx and 5xx errors, and MUST NOT be set for
    1xx, 2xx and 3xx responses. See
    https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/http.md#status
    """

    @pytest.fixture
    def container(self, container_factory, web_config):
        class Service:
            name = "service"

            class Error(Exception):
                pass

            @http("GET", "/resource")
            def get_resource(self, request):
                return "OK"

            @http("GET", "/redirect")
            def get_redirect(self, request):
                return 302, "https://example.org"

            @http("GET", "/unauthorized")
            def get_status_code(self, request):
                return 401, "Unauthorized"

            @http("GET", "/response")
            def get_response(self, request):
                return Response("Permission denied", status=403)

            @http("POST", "/resource")
            def raises(self, request):
                raise self.Error("boom")

            @http("DELETE", "/resource", expected_exceptions=Error)
            def raises_expected(self, request):
                raise self.Error("boom")

        container = container_factory(Service)
        container.start()

        return container

    def test_success(self, container, web_session, memory_exporter):

        with entrypoint_waiter(container, "get_resource"):
            resp = web_session.get("/resource")

        assert resp.status_code == 200

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        span = spans[0]
        assert span.status.is_ok
        assert span.status.status_code == StatusCode.UNSET

    def test_redirect(self, container, web_session, memory_exporter):

        with entrypoint_waiter(container, "get_redirect"):
            resp = web_session.get("/redirect")

        assert resp.status_code == 302

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        span = spans[0]
        assert span.status.is_ok
        assert span.status.status_code == StatusCode.UNSET

    def test_status_code(self, container, web_session, memory_exporter):

        with entrypoint_waiter(container, "get_status_code"):
            resp = web_session.get("/unauthorized")

        assert resp.status_code == 401

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        span = spans[0]
        assert not span.status.is_ok
        assert span.status.status_code == StatusCode.ERROR

    def test_response_object(self, container, web_session, memory_exporter):

        with entrypoint_waiter(container, "get_response"):
            resp = web_session.get("/response")

        assert resp.status_code == 403

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        span = spans[0]
        assert not span.status.is_ok
        assert span.status.status_code == StatusCode.ERROR

    def test_exception(self, container, web_session, memory_exporter):

        with entrypoint_waiter(container, "raises"):
            resp = web_session.post("/resource")

        assert resp.status_code == 500

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        span = spans[0]
        assert not span.status.is_ok
        assert span.status.status_code == StatusCode.ERROR
        assert span.status.description == "Error: boom"

    def test_expected_exception(self, container, web_session, memory_exporter):

        with entrypoint_waiter(container, "raises_expected"):
            resp = web_session.delete("/resource")

        assert resp.status_code == 400

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        span = spans[0]
        assert not span.status.is_ok
        assert span.status.status_code == StatusCode.ERROR
        assert span.status.description == "Error: boom"
