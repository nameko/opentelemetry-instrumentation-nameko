# -*- coding: utf-8 -*-
import socket
from unittest.mock import patch

import pytest
from nameko.rpc import rpc
from nameko.testing.services import dummy, entrypoint_hook
from nameko.utils import REDACTED
from opentelemetry import trace
from opentelemetry.trace.status import StatusCode


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

    @patch("nameko_opentelemetry.entrypoints.active_spans")
    def test_span_not_started(self, active_spans, container, memory_exporter):

        # fake a missing span
        active_spans.get.return_value = None

        with pytest.warns(UserWarning) as warnings:
            with entrypoint_hook(container, "method") as hook:
                assert hook("arg", kwarg="kwarg") == "OK"

        assert "no active span" in str(warnings[0].message)

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 0
