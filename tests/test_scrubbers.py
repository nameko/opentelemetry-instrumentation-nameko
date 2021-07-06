# -*- coding: utf-8 -*-
import uuid
from unittest.mock import Mock

import pytest
from nameko.rpc import ServiceRpc, rpc
from nameko.standalone.rpc import ServiceRpcClient
from nameko.testing.services import entrypoint_waiter
from nameko.testing.utils import get_extension
from opentelemetry.trace import SpanKind

from nameko_opentelemetry.scrubbers import SCRUBBED, DefaultScrubber, scrub


class TestDefaultScubber:
    @pytest.fixture
    def scrubber(self):
        return DefaultScrubber(config={})

    @pytest.mark.parametrize(
        "value",
        ["value", 1, 1.0, True, [1, 2, 3], dict(foo="bar"), (1, 2, 3), object()],
    )
    def test_innocuous(self, value, scrubber):
        assert scrubber.scrub(value) == value

    def test_sensitive_scalar(self, scrubber):
        assert scrubber.scrub("matt@pacerevenue.com") == SCRUBBED

    def test_list_with_sensitive_item(self, scrubber):
        data = ["foo", "matt@pacerevenue.com", "bar"]
        assert scrubber.scrub(data) == ["foo", SCRUBBED, "bar"]

        # source data not modified
        assert data == ["foo", "matt@pacerevenue.com", "bar"]

    def test_tuple_with_sensitive_item(self, scrubber):
        data = ("foo", "matt@pacerevenue.com", "bar")
        assert scrubber.scrub(data) == ("foo", SCRUBBED, "bar")

        # source data not modified
        assert data == ("foo", "matt@pacerevenue.com", "bar")

    def test_generator_with_sensitive_item(self, scrubber):
        def items():
            yield "foo"
            yield "matt@pacerevenue.com"
            yield "bar"

        data = items()
        assert scrubber.scrub(data) == ["foo", SCRUBBED, "bar"]

    def test_dict_with_sensitive_key(self, scrubber):
        data = {"password": "foobar"}
        assert scrubber.scrub(data) == {"password": SCRUBBED}

        # source data not modified
        assert data == {"password": "foobar"}

    def test_dict_with_sensitive_value(self, scrubber):
        data = {"email": "matt@pacerevenue.com"}
        assert scrubber.scrub(data) == {"email": SCRUBBED}

        # source data not modified
        assert data == {"email": "matt@pacerevenue.com"}

    def test_dict_with_sensitive_value_for_key(self, scrubber):
        data = {"matt@pacerevenue.com": "me"}
        assert scrubber.scrub(data) == {SCRUBBED: "me"}

        # source data not modified
        assert data == {"matt@pacerevenue.com": "me"}

    def test_dict_with_sensitive_sub_key(self, scrubber):
        data = {"innocuous": {"email": "matt@pacerevenue.com"}}
        assert scrubber.scrub(data) == {"innocuous": {"email": SCRUBBED}}

        # source data not modified
        assert data == {"innocuous": {"email": "matt@pacerevenue.com"}}

    def test_dict_with_nested_list_containing_sensitive_value(self, scrubber):
        data = {"innocuous": ["foo", "matt@pacerevenue.com", "bar"]}
        assert scrubber.scrub(data) == {"innocuous": ["foo", SCRUBBED, "bar"]}

        # source data not modified
        assert data == {"innocuous": ["foo", "matt@pacerevenue.com", "bar"]}

    def test_list_with_nested_dict_containing_sensitive_data(self, scrubber):
        data = ["foo", {"email": "matt@pacerevenue.com", "secret": "shh"}, "bar"]
        assert scrubber.scrub(data) == [
            "foo",
            {"email": SCRUBBED, "secret": SCRUBBED},
            "bar",
        ]

        # source data not modified
        assert data == [
            "foo",
            {"email": "matt@pacerevenue.com", "secret": "shh"},
            "bar",
        ]


class CustomScrubber(DefaultScrubber):
    SENSITIVE_KEYS = ("name",)
    REPLACEMENT = "***"


class TestScrub:
    @pytest.fixture
    def config(self):
        return {
            "scrubbers": (
                "test_scrubbers.CustomScrubber",
                "nameko_opentelemetry.scrubbers.DefaultScrubber",
            )
        }

    def test_register_new_scrubber(self, config):
        data = {"name": "Matt", "email": "matt@pacerevenue.com"}
        assert scrub(data, config) == {"name": "***", "email": "***"}


class XTestScrubbers:
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

        assert server_span.attributes["result"] == '{"token": "scrubbed"}'

    def test_call_args_scrubber(self, container, client, memory_exporter):

        with entrypoint_waiter(container, "method"):
            assert client.method("arg", password="password") == {
                "token": "should-be-secret"
            }

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        client_span = list(filter(lambda span: span.kind == SpanKind.CLIENT, spans))[0]
        server_span = list(filter(lambda span: span.kind == SpanKind.SERVER, spans))[0]

        assert (
            client_span.attributes["call_args"]
            == '{"arg": "arg", "password": "scrubbed"}'
        )
        assert server_span.attributes["result"] == '{"token": "scrubbed"}'

    def test_context_data_scrubber(self, container, client, memory_exporter):

        with entrypoint_waiter(container, "method"):
            assert client.method("arg", password="password") == {
                "token": "should-be-secret"
            }

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        client_span = list(filter(lambda span: span.kind == SpanKind.CLIENT, spans))[0]
        server_span = list(filter(lambda span: span.kind == SpanKind.SERVER, spans))[0]

        assert '"token": "scrubbed"' in client_span.attributes.context_data
        assert '"token": "scrubbed"' in server_span.attributes.context_data
