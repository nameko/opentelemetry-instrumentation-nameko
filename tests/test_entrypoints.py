import json
import socket

import pytest
from nameko.rpc import rpc
from nameko.testing.services import dummy, entrypoint_hook
from nameko.utils import REDACTED
from opentelemetry.trace.status import StatusCode

from nameko_opentelemetry.entrypoints import TRUNCATE_MAX_LENGTH


class TestSpanAttributes:
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

    def test_common(self, container, memory_exporter):

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


class TestResultAttributes:
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

    def test_simple(self, container, memory_exporter):

        with entrypoint_hook(container, "method") as hook:
            assert hook("arg", kwarg="kwarg") == "OK"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        attributes = spans[0].attributes
        assert attributes["method_name"] == "method"
        assert attributes["result"] == "OK"

    def test_exception(self, container, memory_exporter):

        with entrypoint_hook(container, "raises") as hook:
            with pytest.raises(Exception):
                hook("arg", kwarg="kwarg")

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        attributes = spans[0].attributes
        assert attributes["method_name"] == "raises"
        assert attributes.get("result") is None

    def test_unserializable_result(
        self, container, memory_exporter, unserializable_object
    ):

        with entrypoint_hook(container, "unserializable") as hook:
            hook("arg", kwarg="kwarg")

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        attributes = spans[0].attributes
        assert attributes["method_name"] == "unserializable"
        assert attributes["result"] == str(unserializable_object)


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


class TestCallArgs:
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

    def test_call_args(self, container, memory_exporter):

        with entrypoint_hook(container, "method") as hook:
            assert hook("arg", kwarg="kwarg") == "OK"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        attributes = spans[0].attributes
        assert attributes["call_args"] == json.dumps({"arg": "arg", "kwarg": "kwarg"})

    def test_call_args_truncation(self, container, memory_exporter):

        with entrypoint_hook(container, "method") as hook:
            assert hook("arg" * 1000) == "OK"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        attributes = spans[0].attributes
        assert len(attributes["call_args"]) == TRUNCATE_MAX_LENGTH
        assert attributes["call_args_truncated"] is True

    def test_call_args_redaction(self, container, memory_exporter):

        with entrypoint_hook(container, "method") as hook:
            assert hook("arg", kwarg={"foo": "FOO", "bar": "BAR"}) == "OK"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        attributes = spans[0].attributes
        assert attributes["call_args"] == json.dumps(
            {"arg": "arg", "kwarg": {"foo": REDACTED, "bar": "BAR"}}
        )
        assert attributes["call_args_redacted"] is True


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
