# -*- coding: utf-8 -*-
import gc
import socket
import uuid
from unittest.mock import Mock, patch
from weakref import WeakKeyDictionary

import pytest
from nameko.extensions import DependencyProvider
from nameko.rpc import ServiceRpc, rpc
from nameko.standalone.rpc import ServiceRpcClient
from nameko.testing.services import dummy, entrypoint_hook, entrypoint_waiter
from nameko.testing.utils import get_extension
from nameko.utils import REDACTED
from opentelemetry import trace
from opentelemetry.trace import SpanKind
from opentelemetry.trace.status import StatusCode

from nameko_opentelemetry.entrypoints import EntrypointAdapter


class TestWrappedMethods:
    @pytest.fixture
    def track_worker_setup(self):
        return Mock()

    @pytest.fixture
    def track_worker_result(self):
        return Mock()

    @pytest.fixture
    def container(self, container_factory, track_worker_setup, track_worker_result):
        class Tracker(DependencyProvider):
            def worker_setup(self, worker_ctx):
                track_worker_setup(worker_ctx)

            def worker_result(self, worker_ctx, result, exc_info):
                track_worker_result(worker_ctx)

        class Service:
            name = "service"

            tracker = Tracker()

            @rpc
            def method(self, arg, kwarg=None):
                return "OK"

        container = container_factory(Service)
        container.start()

        return container

    def test_wrapped_methods_called(
        self, container, memory_exporter, track_worker_setup, track_worker_result
    ):

        with entrypoint_hook(container, "method") as hook:
            assert hook("arg", kwarg="kwarg") == "OK"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        assert len(track_worker_setup.call_args_list) == 1
        assert len(track_worker_result.call_args_list) == 1


class TestSpanAttributes:
    @pytest.fixture(params=[True, False], ids=["send_headers", "no_send_headers"])
    def send_headers(self, request):
        return request.param

    @pytest.fixture
    def config(self, config, send_headers):
        # disable headers based on param
        config["send_headers"] = send_headers
        return config

    @pytest.fixture
    def container(self, container_factory):
        class Service:
            name = "service"

            @rpc
            def method(self, arg, kwarg=None):
                return "OK"

        container = container_factory(Service)
        container.start()

        return container

    def test_common(self, container, memory_exporter, send_headers):

        with entrypoint_hook(container, "method") as hook:
            assert hook("arg", kwarg="kwarg") == "OK"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        assert spans[0].name == "service.method"

        attributes = spans[0].attributes
        assert attributes["hostname"] == socket.gethostname()
        assert attributes["service_name"] == "service"
        assert attributes["entrypoint_type"] == "Rpc"
        assert attributes["method_name"] == "method"
        assert attributes["active_workers"] == 1
        assert attributes["available_workers"] == 9

        if send_headers:
            assert "call_id_stack" in attributes["context_data"]
        else:
            assert "call_id_stack" not in attributes


class TestNoTracer:
    @pytest.fixture
    def container(self, container_factory):
        class Service:
            name = "service"

            @rpc
            def method(self, arg, kwarg=None):
                return "OK"

        container = container_factory(Service)
        container.start()

        return container

    @pytest.fixture
    def trace_provider(self):
        """ Temporarily replace the configured trace provider with the default
        provider that would be used if no SDK was in use.
        """
        with patch("nameko_opentelemetry.trace") as patched:
            patched.get_tracer.return_value = trace.get_tracer(
                __name__, "", trace._DefaultTracerProvider()
            )
            yield

    def test_not_recording(self, trace_provider, container, memory_exporter):
        with entrypoint_hook(container, "method") as hook:
            assert hook("arg", kwarg="kwarg") == "OK"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 0


class CustomAdapter(EntrypointAdapter):
    def get_span_name(self, worker_ctx):
        return "custom_span_name"


class TestCustomAdapter:
    @pytest.fixture
    def container(self, container_factory):
        class Service:
            name = "service"

            @rpc
            def method(self, arg, kwarg=None):
                return "OK"

        container = container_factory(Service)
        container.start()

        return container

    @pytest.fixture
    def config(self, config):
        config["entrypoint_adapters"] = {
            "nameko.rpc.Rpc": "test_entrypoints.CustomAdapter"
        }
        return config

    def test_custom_adapter(self, container, memory_exporter):

        with entrypoint_hook(container, "method") as hook:
            assert hook("arg", kwarg="kwarg") == "OK"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        assert spans[0].name == "custom_span_name"


class TestResultAttributes:
    @pytest.fixture(
        params=[True, False], ids=["send_response_payloads", "no_response_payloads"]
    )
    def send_response_payloads(self, request):
        return request.param

    @pytest.fixture
    def config(self, config, send_response_payloads):
        # disable headers based on param
        config["send_response_payloads"] = send_response_payloads
        return config

    @pytest.fixture
    def unserializable_object(self):
        return object()

    @pytest.fixture
    def container(self, container_factory, unserializable_object):
        class Service:
            name = "service"

            class Error(Exception):
                pass

            @rpc
            def method(self, arg, kwarg=None):
                return "OK"

            @dummy
            def unserializable(self, arg, kwarg=None):
                return unserializable_object

            @rpc
            def raises(self, arg, kwarg=None):
                raise self.Error("boom")

        container = container_factory(Service)
        container.start()

        return container

    def test_simple(self, container, memory_exporter, send_response_payloads):

        with entrypoint_hook(container, "method") as hook:
            assert hook("arg", kwarg="kwarg") == "OK"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        attributes = spans[0].attributes
        assert attributes["method_name"] == "method"

        if send_response_payloads:
            assert attributes["result"] == "OK"
        else:
            assert "result" not in attributes

    def test_exception(self, container, memory_exporter, send_response_payloads):

        with entrypoint_hook(container, "raises") as hook:
            with pytest.raises(Exception):
                hook("arg", kwarg="kwarg")

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        attributes = spans[0].attributes
        assert attributes["method_name"] == "raises"

        if send_response_payloads:
            assert attributes.get("result") is None
        else:
            assert "result" not in attributes

    def test_unserializable_result(
        self, container, memory_exporter, unserializable_object, send_response_payloads
    ):

        with entrypoint_hook(container, "unserializable") as hook:
            hook("arg", kwarg="kwarg")

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        attributes = spans[0].attributes

        if send_response_payloads:
            assert attributes["method_name"] == "unserializable"
            assert attributes["result"] == str(unserializable_object)
        else:
            assert "result" not in attributes


class TestExceptions:
    @pytest.fixture
    def container(self, container_factory):
        class Service:
            name = "service"

            class Error(Exception):
                pass

            @rpc
            def method(self, arg, kwarg=None):
                return "OK"

            @rpc
            def raises(self, arg, kwarg=None):
                raise self.Error("boom")

            @rpc(expected_exceptions=Error)
            def raises_expected(self, arg, kwarg=None):
                raise self.Error("boom")

        container = container_factory(Service)
        container.start()

        return container

    def test_success(self, container, memory_exporter):

        with entrypoint_hook(container, "method") as hook:
            assert hook("arg", kwarg="kwarg") == "OK"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        span = spans[0]
        assert len(span.events) == 0

    def test_exception(self, container, memory_exporter):

        with entrypoint_hook(container, "raises") as hook:
            with pytest.raises(Exception):
                hook("arg", kwarg="kwarg")

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

    def test_expected_exception(self, container, memory_exporter):

        with entrypoint_hook(container, "raises_expected") as hook:
            with pytest.raises(Exception):
                hook("arg", kwarg="kwarg")

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

    @patch(
        "nameko_opentelemetry.entrypoints.format_exception",
        side_effect=Exception("boom"),
    )
    def test_unserializable_exception(
        self, format_exccepion, container, memory_exporter
    ):

        with entrypoint_hook(container, "raises_expected") as hook:
            with pytest.raises(Exception):
                hook("arg", kwarg="kwarg")

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        span = spans[0]
        assert len(span.events) == 1

        event = span.events[0]
        assert event.name == "exception"
        assert event.attributes["exception.type"] == "Error"
        assert event.attributes["exception.message"] == "boom"
        assert (
            "Exception occurred on stacktrace formatting"
            in event.attributes["exception.stacktrace"]
        )
        assert event.attributes["exception.escaped"] == "True"
        # extra attributes
        assert event.attributes["exception.expected"] == "True"


class TestCallArgs:
    @pytest.fixture(
        params=[True, False], ids=["send_request_payloads", "no_request_payloads"]
    )
    def send_request_payloads(self, request):
        return request.param

    @pytest.fixture
    def config(self, config, send_request_payloads):
        # override default truncation length
        config["truncate_max_length"] = 300
        # disable call args based on param
        config["send_request_payloads"] = send_request_payloads
        return config

    @pytest.fixture
    def container(self, container_factory):
        class Service:
            name = "service"

            @rpc(sensitive_arguments="kwarg.foo")
            def method(self, arg, kwarg=None):
                return "OK"

        container = container_factory(Service)
        container.start()

        return container

    def test_call_args(self, container, memory_exporter, send_request_payloads):

        with entrypoint_hook(container, "method") as hook:
            assert hook("arg", kwarg="kwarg") == "OK"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        attributes = spans[0].attributes

        if send_request_payloads:
            assert attributes["call_args"] == "{'arg': 'arg', 'kwarg': 'kwarg'}"
        else:
            assert "call_args" not in attributes

    def test_call_args_truncation(
        self, container, memory_exporter, send_request_payloads
    ):

        with entrypoint_hook(container, "method") as hook:
            assert hook("arg" * 1000) == "OK"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        attributes = spans[0].attributes

        if send_request_payloads:
            assert len(attributes["call_args"]) == 300
            assert attributes["call_args_truncated"] == "True"
        else:
            assert "call_args_truncated" not in attributes

    def test_call_args_redaction(
        self, container, memory_exporter, send_request_payloads
    ):

        with entrypoint_hook(container, "method") as hook:
            assert hook("arg", kwarg={"foo": "FOO", "bar": "BAR"}) == "OK"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        attributes = spans[0].attributes

        if send_request_payloads:
            assert attributes["call_args"] == (
                f"{{'arg': 'arg', 'kwarg': {{'foo': '{REDACTED}', 'bar': 'BAR'}}}}"
            )
            assert attributes["call_args_redacted"] == "True"
        else:
            assert "call_args_redacted" not in attributes


class TestStatus:
    @pytest.fixture
    def container(self, container_factory):
        class Service:
            name = "service"

            class Error(Exception):
                pass

            @rpc
            def method(self, arg, kwarg=None):
                return "OK"

            @rpc
            def raises(self, arg, kwarg=None):
                raise self.Error("boom")

            @rpc(expected_exceptions=Error)
            def raises_expected(self, arg, kwarg=None):
                raise self.Error("boom")

        container = container_factory(Service)
        container.start()

        return container

    def test_success(self, container, memory_exporter):

        with entrypoint_hook(container, "method") as hook:
            assert hook("arg", kwarg="kwarg") == "OK"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        span = spans[0]
        assert span.status.is_ok
        assert span.status.status_code == StatusCode.OK

    def test_exception(self, container, memory_exporter):

        with entrypoint_hook(container, "raises") as hook:
            with pytest.raises(Exception):
                hook("arg", kwarg="kwarg")

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        span = spans[0]
        assert not span.status.is_ok
        assert span.status.status_code == StatusCode.ERROR
        assert span.status.description == "Error: boom"

    def test_expected_exception(self, container, memory_exporter):

        with entrypoint_hook(container, "raises_expected") as hook:
            with pytest.raises(Exception):
                hook("arg", kwarg="kwarg")

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        span = spans[0]
        assert span.status.is_ok
        assert span.status.status_code == StatusCode.OK


class TestPartialSpan:
    @pytest.fixture
    def container(self, container_factory):
        class Service:
            name = "service"

            @rpc
            def method(self, arg, kwarg=None):
                return "OK"

        container = container_factory(Service)
        container.start()

        return container

    @pytest.fixture
    def active_spans(self):

        replacement = WeakKeyDictionary()
        replacement.pop = lambda key, default: default  # always return default

        with patch("nameko_opentelemetry.entrypoints.active_spans", new=replacement):
            yield replacement

    def test_span_not_started(self, active_spans, container, memory_exporter):

        with pytest.warns(UserWarning) as warnings:
            with entrypoint_hook(container, "method") as hook:
                assert hook("arg", kwarg="kwarg") == "OK"

        assert "no active span" in str(warnings[0].message)

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 0

    def test_memory_leak(self, active_spans, container, memory_exporter):
        """ Regression test for a memory leak in version 0.2.0.

        `nameko_opentelemetry.entrypoints.active_spans` accumulated items even after the
        workers terminated, because the values stored in the dictionary contained
        a reference to the worker context, which is used as the key.
        """
        for _ in range(10):
            with entrypoint_hook(container, "method") as hook:
                assert hook("arg", kwarg="kwarg") == "OK"

        gc.collect()  # force gc to remove any newly out-of-scope objects
        assert len(active_spans) == 0


class TestScrubbing:
    @pytest.fixture
    def container(self, container_factory, rabbit_config):
        class Service:
            name = "service"

            self_rpc = ServiceRpc("service")

            @rpc
            def method(self, arg, password=None):
                return {"token": "should-be-secret"}

        container = container_factory(Service)
        container.start()

        return container

    @pytest.fixture(params=["standalone", "dependency_provider"])
    def client(self, rabbit_config, request, container):

        context_data = {
            "call_id": f"service.method.{uuid.uuid4()}",
            "token": "should-be-secret",
        }

        if request.param == "standalone":
            with ServiceRpcClient("service", context_data=context_data) as client:
                yield client
        if request.param == "dependency_provider":
            dp = get_extension(container, ServiceRpc)
            yield dp.get_dependency(Mock(context_data=context_data))

    def test_response_scrubber(self, container, client, memory_exporter):

        with entrypoint_waiter(container, "method"):
            assert client.method("arg", password="password") == {
                "token": "should-be-secret"
            }

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        server_span = list(filter(lambda span: span.kind == SpanKind.SERVER, spans))[0]

        assert server_span.attributes["result"] == "{'token': 'scrubbed'}"

    def test_call_args_scrubber(self, container, client, memory_exporter):

        with entrypoint_waiter(container, "method"):
            assert client.method("arg", password="password") == {
                "token": "should-be-secret"
            }

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        server_span = list(filter(lambda span: span.kind == SpanKind.SERVER, spans))[0]

        assert (
            server_span.attributes["call_args"]
            == "{'arg': 'arg', 'password': 'scrubbed'}"
        )

    def test_context_data_scrubber(self, container, client, memory_exporter):

        with entrypoint_waiter(container, "method"):
            assert client.method("arg", password="password") == {
                "token": "should-be-secret"
            }

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        server_span = list(filter(lambda span: span.kind == SpanKind.SERVER, spans))[0]

        assert "'token': 'scrubbed'" in server_span.attributes["context_data"]
